package driverapp

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type controllerUninstallAvailability interface {
	BeginUninstall() error
}

type controllerUninstallCleanup interface {
	CleanupController(ctx context.Context, requestID string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error)
}

type controllerUninstallLease interface {
	ReleaseGracefully(ctx context.Context, requestID string, gate *coordination.MutationGate, checkpointActive bool) (coordination.LeaseSnapshot, error)
}

type controllerUninstallLeadership interface {
	RequireActiveLeadership(ctx context.Context) error
	Context() context.Context
}

type controllerUninstallJournalBarrier interface {
	RequireAllIdle(ctx context.Context) error
}

// controllerUninstallWorkflow retains the request-bound target inventory
// captured before the operator stops node Pods. No later phase accepts caller-
// supplied Instance IDs, parent IDs, or paths.
type controllerUninstallWorkflow struct {
	mu sync.Mutex

	gate               *coordination.MutationGate
	availability       controllerUninstallAvailability
	leadership         controllerUninstallLeadership
	journals           controllerUninstallJournalBarrier
	inventory          controllerInstallationInventory
	cleanup            controllerUninstallCleanup
	lease              controllerUninstallLease
	releaseDisposition func(success bool)

	requestID string
	targets   []scaleway.Target
	cleaned   *admin.ControllerCleanupEvidence
	released  *coordination.LeaseSnapshot
}

func newControllerUninstallWorkflow(gate *coordination.MutationGate, availability controllerUninstallAvailability, leadership controllerUninstallLeadership, journals controllerUninstallJournalBarrier, inventory controllerInstallationInventory, cleanup controllerUninstallCleanup, lease controllerUninstallLease, releaseDisposition func(bool)) (*controllerUninstallWorkflow, error) {
	if gate == nil || availability == nil || leadership == nil || journals == nil || inventory == nil || cleanup == nil || lease == nil || releaseDisposition == nil {
		return nil, fmt.Errorf("controller uninstall workflow dependency is nil")
	}
	return &controllerUninstallWorkflow{
		gate: gate, availability: availability, leadership: leadership, journals: journals,
		inventory: inventory, cleanup: cleanup, lease: lease, releaseDisposition: releaseDisposition,
	}, nil
}

func (workflow *controllerUninstallWorkflow) Quiesce(ctx context.Context, requestID string) error {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if workflow.requestID != "" {
		if workflow.requestID != requestID {
			return coordination.ErrQuiesceConflict
		}
		if workflow.gate.QuiesceRequestID() != requestID {
			return fmt.Errorf("uninstall workflow and mutation gate state disagree")
		}
		return nil
	}
	if err := workflow.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	if err := workflow.availability.BeginUninstall(); err != nil {
		return err
	}
	if err := workflow.gate.BeginQuiesce(ctx, requestID); err != nil {
		return err
	}
	if err := workflow.journals.RequireAllIdle(ctx); err != nil {
		return fmt.Errorf("safe-uninstall reservation journals: %w", err)
	}
	refresh, err := workflow.inventory.ValidateInstallationInventory(ctx)
	if err != nil {
		return fmt.Errorf("capture quiesced safe-uninstall node and attachment inventory: %w", err)
	}
	targets, err := uninstallTargets(refresh)
	if err != nil {
		return err
	}
	workflow.requestID = requestID
	workflow.targets = targets
	return nil
}

func (workflow *controllerUninstallWorkflow) Cleanup(ctx context.Context, requestID string) (admin.ControllerCleanupEvidence, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if err := workflow.requireActiveRequest(ctx, requestID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if workflow.cleaned != nil {
		return cloneControllerCleanupEvidence(*workflow.cleaned), nil
	}
	evidence, err := workflow.cleanup.CleanupController(ctx, requestID, slices.Clone(workflow.targets))
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	stored := cloneControllerCleanupEvidence(evidence)
	workflow.cleaned = &stored
	return cloneControllerCleanupEvidence(stored), nil
}

func (workflow *controllerUninstallWorkflow) Release(ctx context.Context, requestID string) (coordination.LeaseSnapshot, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if workflow.released != nil {
		if workflow.requestID != requestID {
			return coordination.LeaseSnapshot{}, coordination.ErrQuiesceConflict
		}
		return cloneLeaseSnapshotForUninstall(*workflow.released), nil
	}
	if err := workflow.requireActiveRequest(ctx, requestID); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	if workflow.cleaned == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("controller cleanup has not completed for uninstall request %q", requestID)
	}
	released, err := workflow.lease.ReleaseGracefully(ctx, requestID, workflow.gate, false)
	if err != nil {
		if workflow.leadership.Context().Err() != nil {
			workflow.releaseDisposition(false)
		}
		return coordination.LeaseSnapshot{}, err
	}
	workflow.releaseDisposition(true)
	stored := cloneLeaseSnapshotForUninstall(released)
	workflow.released = &stored
	return cloneLeaseSnapshotForUninstall(stored), nil
}

func (workflow *controllerUninstallWorkflow) requireActiveRequest(ctx context.Context, requestID string) error {
	if workflow.requestID == "" || workflow.requestID != requestID || workflow.gate.QuiesceRequestID() != requestID {
		return coordination.ErrQuiesceConflict
	}
	return workflow.leadership.RequireActiveLeadership(ctx)
}

func uninstallTargets(refresh controllerNodeAuthorizationRefresh, requiredParents ...string) ([]scaleway.Target, error) {
	if len(requiredParents) == 0 {
		requiredParents = make([]string, 0, len(refresh.ParentDegradations))
		for parentID := range refresh.ParentDegradations {
			requiredParents = append(requiredParents, parentID)
		}
	}
	slices.Sort(requiredParents)
	for _, parentID := range requiredParents {
		if degradationErr := refresh.ParentDegradations[parentID]; degradationErr != nil {
			return nil, fmt.Errorf("provider inventory for parent %q is degraded: %w", parentID, degradationErr)
		}
	}
	targets := make([]scaleway.Target, 0, len(refresh.KnownInstanceIDs))
	for instanceID := range refresh.KnownInstanceIDs {
		server, present := refresh.Servers[instanceID]
		if !present || server.ID != instanceID {
			return nil, fmt.Errorf("safe-uninstall known Instance %q has no exact provider observation", instanceID)
		}
		target := scaleway.Target{Zone: server.Zone, ServerID: server.ID}
		if _, err := scaleway.ParseNodeID(target.Zone + "/" + target.ServerID); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	slices.SortFunc(targets, func(left, right scaleway.Target) int {
		if compared := strings.Compare(left.Zone, right.Zone); compared != 0 {
			return compared
		}
		return strings.Compare(left.ServerID, right.ServerID)
	})
	if len(targets) == 0 {
		return nil, fmt.Errorf("safe-uninstall target Instance set is empty")
	}
	return targets, nil
}

func cloneControllerCleanupEvidence(evidence admin.ControllerCleanupEvidence) admin.ControllerCleanupEvidence {
	evidence.UnmountedParents = slices.Clone(evidence.UnmountedParents)
	evidence.DetachedParentFilesystemIDs = slices.Clone(evidence.DetachedParentFilesystemIDs)
	evidence.CheckedInstanceIDs = slices.Clone(evidence.CheckedInstanceIDs)
	evidence.RegionalAttachmentIDs = slices.Clone(evidence.RegionalAttachmentIDs)
	evidence.InstanceAttachmentIDs = slices.Clone(evidence.InstanceAttachmentIDs)
	evidence.RemainingControllerMountPaths = slices.Clone(evidence.RemainingControllerMountPaths)
	return evidence
}

func cloneLeaseSnapshotForUninstall(snapshot coordination.LeaseSnapshot) coordination.LeaseSnapshot {
	clone := snapshot
	clone.Annotations = make(map[string]string, len(snapshot.Annotations))
	for key, value := range snapshot.Annotations {
		clone.Annotations[key] = value
	}
	return clone
}

var _ admin.ControllerUninstallWorkflow = (*controllerUninstallWorkflow)(nil)
