package scaleway

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// Target identifies one validated zonal Scaleway Instance.
type Target struct {
	Zone     string
	ServerID string
}

// ParseNodeID parses the exact CSI <zone>/<serverID> contract.
func ParseNodeID(nodeID string) (Target, error) {
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return Target{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	parts := strings.Split(nodeID, "/")
	return Target{Zone: parts[0], ServerID: parts[1]}, nil
}

// AttachConfig defines bounded readiness polling.
type AttachConfig struct {
	Deadline       time.Duration
	InitialBackoff time.Duration
	MaximumBackoff time.Duration
}

// Jitter returns one bounded polling delay for an attempt.
type Jitter interface {
	Delay(base time.Duration, attempt uint32) time.Duration
}

// AttachRequest contains every identity and exclusivity input required before
// the shared workload/controller attachment path may call the provider.
type AttachRequest struct {
	Region              string
	ProjectID           string
	FilesystemID        string
	Target              Target
	ConfiguredParentIDs map[string]struct{}
	// KnownInstances includes exact existing Kubernetes Node/CSINode provider
	// identities, including cordoned nodes that may retain warm attachments.
	KnownInstances map[string]Target
	// EligibleInstanceIDs is the stricter current publish target set produced by
	// the homogeneous Ready node rollout preflight.
	EligibleInstanceIDs      map[string]struct{}
	QualifiedCommercialTypes map[string]struct{}
}

// AttachmentManager owns the single v1 attach and readiness state machine.
type AttachmentManager struct {
	api    API
	clock  clock.Clock
	jitter Jitter
	config AttachConfig
}

// NewAttachmentManager validates bounded polling dependencies.
func NewAttachmentManager(api API, operationClock clock.Clock, jitter Jitter, config AttachConfig) (*AttachmentManager, error) {
	if api == nil || operationClock == nil || jitter == nil {
		return nil, fmt.Errorf("attachment manager dependency is nil")
	}
	if config.Deadline <= 0 || config.InitialBackoff <= 0 || config.MaximumBackoff < config.InitialBackoff {
		return nil, fmt.Errorf("invalid attachment polling deadline or backoff")
	}
	return &AttachmentManager{api: api, clock: operationClock, jitter: jitter, config: config}, nil
}

// EnsureAttached makes at most one attach call and returns only after regional
// and Instance inventories agree that the target parent is available.
func (manager *AttachmentManager) EnsureAttached(ctx context.Context, request AttachRequest) error {
	if err := validateAttachRequest(request); err != nil {
		return err
	}
	deadline := manager.clock.Now().Add(manager.config.Deadline)
	backoff := manager.config.InitialBackoff
	attachIssued := false
	var ambiguousAttachErr error
	var lastObservationErr error
	for attempt := uint32(0); ; attempt++ {
		observation, observeErr := manager.observe(ctx, request)
		if observeErr != nil {
			if !providerRetryable(observeErr) {
				return errors.Join(ambiguousAttachErr, observeErr)
			}
			lastObservationErr = observeErr
		} else {
			lastObservationErr = nil
			switch observation.state {
			case ServerFilesystemAvailable:
				return nil
			case ServerFilesystemDetaching:
				return errors.Join(ambiguousAttachErr, fmt.Errorf("parent %q is detaching from server %q: %w", request.FilesystemID, request.Target.ServerID, ErrUnavailable))
			case ServerFilesystemUnknown:
				if !attachIssued {
					attachIssued = true
					attachErr := manager.api.AttachServerFilesystem(ctx, request.Target.Zone, request.Target.ServerID, request.FilesystemID)
					if attachErr != nil {
						if !providerRetryable(attachErr) {
							return attachErr
						}
						ambiguousAttachErr = attachErr
					}
					// One immediate authoritative reread resolves the common committed
					// response-loss case without sleeping. Subsequent absence is polled
					// and never causes a second Attach call in this operation.
					continue
				}
			case ServerFilesystemAttaching:
				// Poll below until both provider inventory surfaces report available.
			default:
				return errors.Join(ambiguousAttachErr, fmt.Errorf("parent %q has unknown attachment state: %w", request.FilesystemID, ErrUnavailable))
			}
		}
		if !manager.clock.Now().Before(deadline) {
			deadlineErr := fmt.Errorf("wait for parent %q attachment on server %q: %w", request.FilesystemID, request.Target.ServerID, ErrDeadlineExceeded)
			return errors.Join(ambiguousAttachErr, lastObservationErr, deadlineErr)
		}
		delay := manager.jitter.Delay(backoff, attempt)
		remaining := deadline.Sub(manager.clock.Now())
		if delay <= 0 || delay > remaining {
			delay = remaining
		}
		if err := wait(ctx, manager.clock, delay); err != nil {
			return err
		}
		if backoff < manager.config.MaximumBackoff {
			backoff = nextBackoff(backoff, manager.config.MaximumBackoff)
		}
	}
}

func providerRetryable(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrDeadlineExceeded) || errors.Is(err, ErrConflict)
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum-current {
		return maximum
	}
	return current * 2
}

type attachObservation struct {
	state ServerFilesystemState
}

func (manager *AttachmentManager) observe(ctx context.Context, request AttachRequest) (attachObservation, error) {
	filesystem, err := manager.api.GetFilesystem(ctx, request.Region, request.FilesystemID)
	if err != nil {
		return attachObservation{}, fmt.Errorf("get parent %q metadata: %w", request.FilesystemID, err)
	}
	if filesystem.ID != request.FilesystemID || filesystem.ProjectID != request.ProjectID || filesystem.Region != request.Region {
		return attachObservation{}, fmt.Errorf("parent %q project or region identity mismatch: %w", request.FilesystemID, ErrFailedPrecondition)
	}
	if err := filesystem.Status.PermitNewMutation(); err != nil {
		return attachObservation{}, err
	}
	inventory, err := ListRegionalInventory(ctx, manager.api, filesystem)
	if err != nil {
		return attachObservation{}, err
	}
	if err := ValidateAuthorizedAttachments(inventory, request.KnownInstances); err != nil {
		return attachObservation{}, err
	}
	instanceIDs := make([]string, 0, len(request.KnownInstances))
	for instanceID := range request.KnownInstances {
		instanceIDs = append(instanceIDs, instanceID)
	}
	slices.Sort(instanceIDs)
	servers := make(map[string]Server, len(instanceIDs))
	for _, instanceID := range instanceIDs {
		target := request.KnownInstances[instanceID]
		server, err := manager.api.GetServer(ctx, target.Zone, target.ServerID)
		if err != nil {
			return attachObservation{}, fmt.Errorf("get known server %q: %w", target.ServerID, err)
		}
		if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != request.Region || server.ProjectID != request.ProjectID {
			return attachObservation{}, fmt.Errorf("known server %q identity mismatch: %w", target.ServerID, ErrFailedPrecondition)
		}
		if err := ValidateExclusiveServerInventory(server, request.ConfiguredParentIDs); err != nil {
			return attachObservation{}, err
		}
		servers[instanceID] = server
	}
	if err := ValidateAttachmentInventoryAgreement(inventory, request.KnownInstances, servers); err != nil {
		return attachObservation{}, fmt.Errorf("reconcile complete pre-attach inventory: %w: %w", err, ErrUnavailable)
	}
	server := servers[request.Target.ServerID]
	if err := server.State.PermitNewAttachment(); err != nil {
		return attachObservation{}, err
	}
	if _, qualified := request.QualifiedCommercialTypes[server.CommercialType]; !qualified {
		return attachObservation{}, fmt.Errorf("server commercial type %q is not release-qualified: %w", server.CommercialType, ErrFailedPrecondition)
	}
	if err := ValidatePostAttachBudget(server, request.ConfiguredParentIDs); err != nil {
		return attachObservation{}, err
	}
	attachments, err := ServerAttachmentMap(server)
	if err != nil {
		return attachObservation{}, err
	}
	state, onServer := attachments[request.FilesystemID]
	regionalCount := 0
	for _, attachment := range inventory.Attachments {
		if attachment.ResourceID == request.Target.ServerID && attachment.Zone == request.Target.Zone {
			regionalCount++
		}
	}
	if regionalCount > 1 || onServer != (regionalCount == 1) {
		return attachObservation{}, fmt.Errorf("regional and Instance attachment inventories disagree for parent %q and server %q: %w: %w", request.FilesystemID, request.Target.ServerID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
	}
	if !onServer {
		return attachObservation{state: ServerFilesystemUnknown}, nil
	}
	return attachObservation{state: state}, nil
}

func validateAttachRequest(request AttachRequest) error {
	if err := validateProviderScope(request.Region, request.ProjectID); err != nil {
		return fmt.Errorf("attach request scope: %v: %w", err, ErrInvalidArgument)
	}
	if err := validateTargetInRegion(request.Target, request.Region); err != nil {
		return fmt.Errorf("attach request target: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(request.FilesystemID); err != nil {
		return fmt.Errorf("%v: %w", err, ErrInvalidArgument)
	}
	if _, configured := request.ConfiguredParentIDs[request.FilesystemID]; !configured {
		return fmt.Errorf("target parent is not configured: %w", ErrFailedPrecondition)
	}
	if _, authorized := request.EligibleInstanceIDs[request.Target.ServerID]; !authorized {
		return fmt.Errorf("target server is not authorized by Kubernetes node evidence: %w", ErrFailedPrecondition)
	}
	if target, known := request.KnownInstances[request.Target.ServerID]; !known || target != request.Target {
		return fmt.Errorf("eligible target server is absent from known Kubernetes node evidence: %w", ErrFailedPrecondition)
	}
	for instanceID, target := range request.KnownInstances {
		if instanceID != target.ServerID {
			return fmt.Errorf("known Instance map key %q differs from target server %q: %w", instanceID, target.ServerID, ErrFailedPrecondition)
		}
		if err := validateTargetInRegion(target, request.Region); err != nil {
			return fmt.Errorf("known Instance %q: %v: %w", instanceID, err, ErrFailedPrecondition)
		}
	}
	if len(request.QualifiedCommercialTypes) == 0 {
		return fmt.Errorf("release-qualified commercial type allowlist is empty: %w", ErrFailedPrecondition)
	}
	return nil
}

func wait(ctx context.Context, operationClock clock.Clock, duration time.Duration) error {
	timer := operationClock.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

// RandomJitter varies a base delay within [80%,120%] using crypto/rand. Failure
// to obtain randomness safely falls back to the bounded base delay.
type RandomJitter struct{}

// Delay returns a bounded random duration.
func (RandomJitter) Delay(base time.Duration, _ uint32) time.Duration {
	if base <= 0 {
		return base
	}
	var buffer [8]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return base
	}
	// Pick one of 401 integer permille factors from 800 through 1200.
	factor := uint64(800) + binary.LittleEndian.Uint64(buffer[:])%401
	return scaleJitter(base, factor)
}

func scaleJitter(base time.Duration, factor uint64) time.Duration {
	if base <= 0 || factor < 800 || factor > 1200 {
		return base
	}
	const maximumDuration = uint64(1<<63 - 1)
	nanoseconds := uint64(base)
	whole := nanoseconds / 1000
	fractional := (nanoseconds % 1000) * factor / 1000
	if whole > maximumDuration/factor {
		return time.Duration(maximumDuration)
	}
	scaled := whole * factor
	if scaled > maximumDuration-fractional {
		return time.Duration(maximumDuration)
	}
	scaled += fractional
	if scaled == 0 {
		return time.Nanosecond
	}
	return time.Duration(scaled)
}
