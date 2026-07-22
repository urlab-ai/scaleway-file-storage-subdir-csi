package driverapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const (
	freshDiscoveryInitialBackoff = time.Second
	freshDiscoveryMaximumBackoff = 15 * time.Second
)

// freshInstallationDiscovery is the only provisional-Lease path allowed to
// attach parents. It proves global Kubernetes absence, an initially empty
// provider inventory, literal parent-root emptiness, and parent-claim absence.
// Its observations are process-local so a crash can never turn an ambiguous
// pre-existing attachment into first-claim authority.
type freshInstallationDiscovery struct {
	manager     *parentBootstrapManager
	allocations parentBootstrapAllocationLister
	pvs         parentBootstrapPVLister
	journals    freshReservationJournalBootstrap
	poolNames   []string
	clusterUID  string
	retry       freshDiscoveryRetry

	gate     chan struct{}
	mu       sync.Mutex
	observed map[string]time.Time
}

type freshDiscoveryRetry struct {
	deadline time.Duration
	jitter   scaleway.Jitter
}

type freshReservationJournalBootstrap interface {
	BootstrapFresh(ctx context.Context, pools []string, clusterUID string) error
}

func newFreshInstallationDiscovery(manager *parentBootstrapManager, allocations parentBootstrapAllocationLister, pvs parentBootstrapPVLister, journals freshReservationJournalBootstrap, poolNames []string, clusterUID string, retryDeadline time.Duration, retryJitter scaleway.Jitter) (*freshInstallationDiscovery, error) {
	if manager == nil || allocations == nil || pvs == nil || journals == nil || retryJitter == nil {
		return nil, fmt.Errorf("fresh installation discovery dependency is nil")
	}
	if len(poolNames) == 0 || clusterUID == "" {
		return nil, fmt.Errorf("fresh installation journal scope is incomplete")
	}
	if retryDeadline <= 0 {
		return nil, fmt.Errorf("fresh installation retry deadline must be positive")
	}
	return &freshInstallationDiscovery{
		manager: manager, allocations: allocations, pvs: pvs, journals: journals,
		poolNames: slices.Clone(poolNames), clusterUID: clusterUID,
		retry: freshDiscoveryRetry{deadline: retryDeadline, jitter: retryJitter},
		gate:  make(chan struct{}, 1), observed: make(map[string]time.Time),
	}, nil
}

// VerifyFreshInstallation repeats the complete absence proof immediately
// before Lease promotion. An early Kubernetes check avoids attaching parents
// once durable recovery state is already visible; the final check closes the
// discovery window before the caller drains renewal and performs its CAS.
func (discovery *freshInstallationDiscovery) VerifyFreshInstallation(ctx context.Context) error {
	if err := discovery.lock(ctx); err != nil {
		return err
	}
	defer discovery.unlock()
	if err := discovery.requireKubernetesEmpty(ctx); err != nil {
		return err
	}
	// Commit the complete permanent journal set before provider attachment or
	// parent-root mutation. A crash can therefore resume an Initializing set
	// while the fresh proof is still valid; after Ready, operational startup
	// treats any missing committed journal as corruption.
	if err := discovery.journals.BootstrapFresh(ctx, discovery.poolNames, discovery.clusterUID); err != nil {
		return fmt.Errorf("bootstrap fresh reservation journals: %w", err)
	}

	parentIDs := make([]string, 0, len(discovery.manager.parents))
	for parentID := range discovery.manager.parents {
		parentIDs = append(parentIDs, parentID)
	}
	slices.Sort(parentIDs)
	retryCtx, cancelRetry := context.WithTimeout(ctx, discovery.retry.deadline)
	defer cancelRetry()
	deadline := discovery.manager.operationClock.Now().Add(discovery.retry.deadline)
	backoff := freshDiscoveryInitialBackoff
	for attempt := uint32(0); ; attempt++ {
		verificationErr := discovery.verifyParentsAndFinalize(retryCtx, parentIDs)
		if verificationErr == nil {
			return nil
		}
		if !freshDiscoveryRetryable(verificationErr) {
			return verificationErr
		}
		if err := retryCtx.Err(); err != nil {
			return err
		}
		remaining := deadline.Sub(discovery.manager.operationClock.Now())
		if remaining <= 0 {
			return errors.Join(verificationErr, fmt.Errorf("fresh installation discovery retry deadline expired: %w", scaleway.ErrDeadlineExceeded))
		}
		delay := discovery.retry.jitter.Delay(backoff, attempt)
		if delay <= 0 || delay > remaining {
			delay = remaining
		}
		if err := discovery.wait(retryCtx, delay); err != nil {
			return err
		}
		backoff = nextFreshDiscoveryBackoff(backoff)
	}
}

func (discovery *freshInstallationDiscovery) verifyParentsAndFinalize(ctx context.Context, parentIDs []string) error {
	for _, parentID := range parentIDs {
		if err := discovery.inspectParent(ctx, parentID); err != nil {
			return err
		}
	}
	if err := discovery.requireKubernetesEmpty(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return discovery.manager.authorizeFreshBootstrap(discovery.observedSnapshot())
}

func freshDiscoveryRetryable(err error) bool {
	// Some provider inventory failures deliberately carry ErrUnavailable in
	// addition to a stronger safety classification. Never let the retry marker
	// hide a conclusive foreign attachment, identity, authorization, or
	// precondition failure.
	if errors.Is(err, scaleway.ErrInvalidArgument) || errors.Is(err, scaleway.ErrNotFound) ||
		errors.Is(err, scaleway.ErrPermissionDenied) || errors.Is(err, scaleway.ErrResourceExhausted) ||
		errors.Is(err, scaleway.ErrFailedPrecondition) || errors.Is(err, scaleway.ErrUnknownAttachmentNode) ||
		errors.Is(err, scaleway.ErrForeignAttachmentType) {
		return false
	}
	return errors.Is(err, scaleway.ErrUnavailable) || errors.Is(err, mount.ErrMountUnavailable) || errors.Is(err, k8s.ErrUnavailable)
}

func nextFreshDiscoveryBackoff(current time.Duration) time.Duration {
	if current >= freshDiscoveryMaximumBackoff || current > freshDiscoveryMaximumBackoff-current {
		return freshDiscoveryMaximumBackoff
	}
	return current * 2
}

func (discovery *freshInstallationDiscovery) wait(ctx context.Context, delay time.Duration) error {
	timer := discovery.manager.operationClock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

func (discovery *freshInstallationDiscovery) inspectParent(ctx context.Context, parentID string) error {
	observation, err := discovery.manager.observeProvider(ctx, parentID)
	if err != nil {
		return fmt.Errorf("observe fresh parent %q provider inventory: %w", parentID, err)
	}
	discovery.mu.Lock()
	_, observedBefore := discovery.observed[parentID]
	discovery.mu.Unlock()
	if !observedBefore {
		if !observation.emptyFor(discovery.manager.localTarget) {
			return fmt.Errorf("fresh parent %q had a pre-existing provider attachment", parentID)
		}
		observedAt := discovery.manager.operationClock.Now()
		if observedAt.IsZero() {
			return fmt.Errorf("fresh parent %q empty-inventory observation time is zero", parentID)
		}
		// Record before attach. If the attach response is ambiguous, only this
		// same verifier instance may recognize the exact resulting attachment.
		discovery.mu.Lock()
		discovery.observed[parentID] = observedAt
		discovery.mu.Unlock()
	} else if !observation.emptyFor(discovery.manager.localTarget) {
		if err := observation.requireCurrentControllerOnly(discovery.manager.localTarget); err != nil {
			return fmt.Errorf("reobserve same-process fresh parent %q: %w", parentID, err)
		}
	}

	root, err := discovery.manager.access.EnsureMounted(ctx, parentID)
	if err != nil {
		return fmt.Errorf("attach and mount fresh parent %q: %w", parentID, err)
	}
	attached, err := discovery.manager.observeProvider(ctx, parentID)
	if err != nil {
		return fmt.Errorf("reobserve mounted fresh parent %q: %w", parentID, err)
	}
	if err := attached.requireCurrentControllerOnly(discovery.manager.localTarget); err != nil {
		return fmt.Errorf("validate mounted fresh parent %q attachment: %w", parentID, err)
	}
	if err := discovery.inspectFreshRoot(ctx, parentID, root); err != nil {
		return err
	}
	return nil
}

func (discovery *freshInstallationDiscovery) inspectFreshRoot(ctx context.Context, parentID, root string) (returnErr error) {
	filesystem, err := discovery.manager.openFilesystem(root)
	if err != nil {
		return fmt.Errorf("open fresh parent %q filesystem: %w", parentID, err)
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	_, claimPresent, err := filesystem.ReadParentClaim(ctx)
	if err != nil {
		return fmt.Errorf("read fresh parent %q claim: %w", parentID, err)
	}
	if claimPresent {
		return fmt.Errorf("fresh parent %q already has an immutable owner claim", parentID)
	}
	if err := filesystem.InspectFreshRoot(ctx); err != nil {
		return fmt.Errorf("inspect fresh parent %q root: %w", parentID, err)
	}
	return nil
}

func (discovery *freshInstallationDiscovery) requireKubernetesEmpty(ctx context.Context) error {
	allocations, err := discovery.allocations.List(ctx)
	if err != nil {
		return fmt.Errorf("list fresh-installation allocations: %w", err)
	}
	if len(allocations) != 0 {
		return fmt.Errorf("fresh installation has %d durable allocation records", len(allocations))
	}
	persistentVolumes, err := discovery.pvs.DriverPersistentVolumes(ctx)
	if err != nil {
		return fmt.Errorf("list fresh-installation PersistentVolumes: %w", err)
	}
	if len(persistentVolumes) != 0 {
		return fmt.Errorf("fresh installation has %d driver PersistentVolumes", len(persistentVolumes))
	}
	return nil
}

func (discovery *freshInstallationDiscovery) observedSnapshot() map[string]time.Time {
	discovery.mu.Lock()
	defer discovery.mu.Unlock()
	result := make(map[string]time.Time, len(discovery.observed))
	for parentID, observedAt := range discovery.observed {
		result[parentID] = observedAt
	}
	return result
}

func (discovery *freshInstallationDiscovery) lock(ctx context.Context) error {
	select {
	case discovery.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (discovery *freshInstallationDiscovery) unlock() { <-discovery.gate }

var _ interface {
	VerifyFreshInstallation(context.Context) error
} = (*freshInstallationDiscovery)(nil)
