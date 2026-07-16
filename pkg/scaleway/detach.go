package scaleway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// DetachRequest is the exact offline scope approved after mount and reference
// validation. Targets must enumerate every installation Instance that may
// carry this parent; any other regional attachment fails closed.
type DetachRequest struct {
	Region       string
	ProjectID    string
	FilesystemID string
	Targets      []Target
}

// DetachmentManager owns the only normal production detach state machine. CSI
// unpublish paths must never use it.
type DetachmentManager struct {
	api    API
	clock  clock.Clock
	jitter Jitter
	config AttachConfig
}

// NewDetachmentManager validates the bounded polling dependencies. Detach uses
// the same deadline/backoff shape as attach but a separate manager instance so
// authorization cannot be confused at call sites.
func NewDetachmentManager(api API, operationClock clock.Clock, jitter Jitter, config AttachConfig) (*DetachmentManager, error) {
	if api == nil || operationClock == nil || jitter == nil {
		return nil, fmt.Errorf("detachment manager dependency is nil")
	}
	if config.Deadline <= 0 || config.InitialBackoff <= 0 || config.MaximumBackoff < config.InitialBackoff {
		return nil, fmt.Errorf("invalid detachment polling deadline or backoff")
	}
	return &DetachmentManager{api: api, clock: operationClock, jitter: jitter, config: config}, nil
}

// EnsureDetached detaches only the exact authorized targets and returns after
// complete regional and per-Instance inventories agree on absence. An
// ambiguous detach result is resolved by a mandatory reread before returning.
func (manager *DetachmentManager) EnsureDetached(ctx context.Context, request DetachRequest) error {
	targets, err := validateDetachRequest(request)
	if err != nil {
		return err
	}
	deadline := manager.clock.Now().Add(manager.config.Deadline)
	backoff := manager.config.InitialBackoff
	var present map[Target]struct{}
	for attempt := uint32(0); ; attempt++ {
		present, err = manager.observeDetach(ctx, request, targets)
		if err == nil {
			break
		}
		if !providerRetryable(err) {
			return err
		}
		if !manager.clock.Now().Before(deadline) {
			return errors.Join(err, fmt.Errorf("wait for initial parent %q detach inventory: %w", request.FilesystemID, ErrDeadlineExceeded))
		}
		delay := manager.jitter.Delay(backoff, attempt)
		remaining := deadline.Sub(manager.clock.Now())
		if delay <= 0 || delay > remaining {
			delay = remaining
		}
		if waitErr := wait(ctx, manager.clock, delay); waitErr != nil {
			return errors.Join(err, waitErr)
		}
		if backoff < manager.config.MaximumBackoff {
			backoff = nextBackoff(backoff, manager.config.MaximumBackoff)
		}
	}
	if len(present) == 0 {
		return nil
	}

	var detachErrors []error
	definiteDetachFailure := false
	for _, target := range targets {
		if _, attached := present[target]; !attached {
			continue
		}
		if err := manager.api.DetachServerFilesystem(ctx, target.Zone, target.ServerID, request.FilesystemID); err != nil {
			detachErrors = append(detachErrors, fmt.Errorf("detach parent %q from Instance %q: %w", request.FilesystemID, target.ServerID, err))
			if !providerRetryable(err) {
				definiteDetachFailure = true
			}
		}
	}
	// Always reread, even after every detach returned an error. A provider may
	// have committed the mutation before its response became ambiguous.
	present, observeErr := manager.observeDetach(ctx, request, targets)
	if observeErr != nil {
		if !providerRetryable(observeErr) {
			return errors.Join(append(detachErrors, observeErr)...)
		}
	} else if len(present) == 0 {
		return nil
	}
	if definiteDetachFailure {
		return errors.Join(detachErrors...)
	}

	backoff = manager.config.InitialBackoff
	lastObservationErr := observeErr
	for attempt := uint32(0); ; attempt++ {
		if !manager.clock.Now().Before(deadline) {
			deadlineErr := fmt.Errorf("wait for parent %q detachment: %w", request.FilesystemID, ErrDeadlineExceeded)
			return errors.Join(append(detachErrors, lastObservationErr, deadlineErr)...)
		}
		delay := manager.jitter.Delay(backoff, attempt)
		remaining := deadline.Sub(manager.clock.Now())
		if delay <= 0 || delay > remaining {
			delay = remaining
		}
		if err := wait(ctx, manager.clock, delay); err != nil {
			return errors.Join(append(detachErrors, err)...)
		}
		present, err = manager.observeDetach(ctx, request, targets)
		if err != nil {
			if !providerRetryable(err) {
				return errors.Join(append(detachErrors, err)...)
			}
			lastObservationErr = err
		} else {
			lastObservationErr = nil
			if len(present) == 0 {
				return nil
			}
		}
		if backoff < manager.config.MaximumBackoff {
			backoff = nextBackoff(backoff, manager.config.MaximumBackoff)
		}
	}
}

func (manager *DetachmentManager) observeDetach(ctx context.Context, request DetachRequest, targets []Target) (map[Target]struct{}, error) {
	filesystem, err := manager.api.GetFilesystem(ctx, request.Region, request.FilesystemID)
	if err != nil {
		return nil, fmt.Errorf("read parent %q metadata for detach: %w", request.FilesystemID, err)
	}
	if filesystem.ID != request.FilesystemID || filesystem.ProjectID != request.ProjectID || filesystem.Region != request.Region {
		return nil, fmt.Errorf("parent identity mismatch during detach: %w", ErrFailedPrecondition)
	}
	if err := filesystem.Status.PermitNewMutation(); err != nil {
		return nil, fmt.Errorf("parent %q is not available for detach: %w", request.FilesystemID, err)
	}
	inventory, err := ListRegionalInventory(ctx, manager.api, filesystem)
	if err != nil {
		return nil, err
	}
	authorized := make(map[string]Target, len(targets))
	for _, target := range targets {
		authorized[target.ServerID] = target
	}
	regional := make(map[Target]int, len(inventory.Attachments))
	for _, attachment := range inventory.Attachments {
		target, present := authorized[attachment.ResourceID]
		if !present || attachment.Zone != target.Zone {
			return nil, fmt.Errorf("parent %q has unauthorized attachment to Instance %q in zone %q: %w", request.FilesystemID, attachment.ResourceID, attachment.Zone, ErrFailedPrecondition)
		}
		regional[target]++
		if regional[target] != 1 {
			return nil, fmt.Errorf("parent %q has duplicate attachments to Instance %q: %w", request.FilesystemID, target.ServerID, ErrUnavailable)
		}
	}

	present := make(map[Target]struct{}, len(regional))
	for _, target := range targets {
		server, serverErr := manager.api.GetServer(ctx, target.Zone, target.ServerID)
		if errors.Is(serverErr, ErrNotFound) {
			if regional[target] != 0 {
				return nil, fmt.Errorf("deleted Instance %q retains regional parent attachment: %w", target.ServerID, ErrFailedPrecondition)
			}
			continue
		}
		if serverErr != nil {
			return nil, fmt.Errorf("read detach target Instance %q: %w", target.ServerID, serverErr)
		}
		if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != request.Region || server.ProjectID != request.ProjectID {
			return nil, fmt.Errorf("detach target Instance identity mismatch: %w", ErrFailedPrecondition)
		}
		if err := server.State.PermitOfflineDetach(); err != nil {
			return nil, err
		}
		serverFilesystems, err := ServerAttachmentMap(server)
		if err != nil {
			return nil, err
		}
		_, instancePresent := serverFilesystems[request.FilesystemID]
		regionalPresent := regional[target] == 1
		if instancePresent != regionalPresent {
			return nil, fmt.Errorf("regional and Instance inventories disagree for detach target %q: %w: %w", target.ServerID, ErrAttachmentInventoryDisagreement, ErrUnavailable)
		}
		if instancePresent {
			present[target] = struct{}{}
		}
	}
	return present, nil
}

func validateDetachRequest(request DetachRequest) ([]Target, error) {
	if err := validateProviderScope(request.Region, request.ProjectID); err != nil {
		return nil, fmt.Errorf("detach request scope: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(request.FilesystemID); err != nil {
		return nil, fmt.Errorf("detach parent ID: %w: %w", err, ErrInvalidArgument)
	}
	if len(request.Targets) == 0 {
		return nil, fmt.Errorf("detach request must contain at least one exact target: %w", ErrInvalidArgument)
	}
	targets := slices.Clone(request.Targets)
	slices.SortFunc(targets, func(left, right Target) int {
		if compared := strings.Compare(left.Zone, right.Zone); compared != 0 {
			return compared
		}
		return strings.Compare(left.ServerID, right.ServerID)
	})
	serverIDs := make(map[string]struct{}, len(targets))
	for index, target := range targets {
		if err := validateTargetInRegion(target, request.Region); err != nil {
			return nil, fmt.Errorf("detach target %d is invalid or outside region %q: %w", index, request.Region, ErrInvalidArgument)
		}
		if _, duplicate := serverIDs[target.ServerID]; duplicate {
			return nil, fmt.Errorf("detach target Instance %q is duplicated: %w", target.ServerID, ErrInvalidArgument)
		}
		serverIDs[target.ServerID] = struct{}{}
	}
	return targets, nil
}
