package driverapp

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type controllerDecommissionAvailability interface {
	BeginDecommission() error
}

type controllerDecommissionCleanup interface {
	CleanupParent(ctx context.Context, requestID, parentID string, targets []scaleway.Target) (admin.ControllerCleanupEvidence, error)
}

type controllerDecommissionJournalBarrier interface {
	InspectParentClear(ctx context.Context, parentID string) error
	RequireParentClear(ctx context.Context, parentID string) error
}

// controllerDecommissionWorkflow owns the target-bound barrier, the last
// online allocation/ownership validation, exact provider targets, controller
// cleanup, and graceful release. Stopping node/controller Pods and changing
// Helm values remain outside the runtime ServiceAccount.
type controllerDecommissionWorkflow struct {
	mu sync.Mutex

	gate               *coordination.MutationGate
	availability       controllerDecommissionAvailability
	leadership         controllerUninstallLeadership
	journals           controllerDecommissionJournalBarrier
	records            checkpointInventoryReader
	installation       controllerInstallationInventory
	cleanup            controllerDecommissionCleanup
	lease              controllerUninstallLease
	releaseDisposition func(bool)
	parentStates       map[string]pool.ParentState

	requestID       string
	parentID        string
	targets         []scaleway.Target
	validated       bool
	availabilitySet bool
	cleaned         *admin.ControllerCleanupEvidence
	released        *coordination.LeaseSnapshot
}

// InspectParent reads the complete controller-authoritative record and
// provider-target inventory without changing readiness, mutation admission, or
// durable state. Execute repeats the same proof under the quiesce barrier; this
// first pass exists so dry-run never presents an allocation-only approximation
// as a complete decommission plan.
func (workflow *controllerDecommissionWorkflow) InspectParent(ctx context.Context, requestID, parentID string) (admin.ControllerDecommissionInspection, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	if err := volume.ValidateParentFilesystemID(parentID); err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	state, configured := workflow.parentStates[parentID]
	if !configured {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("decommission parent %q is not configured", parentID)
	}
	if state != pool.ParentDraining {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("decommission parent %q must be draining", parentID)
	}
	if workflow.requestID != "" {
		if workflow.requestID != requestID || workflow.parentID != parentID || workflow.gate.QuiesceRequestID() != requestID {
			return admin.ControllerDecommissionInspection{}, coordination.ErrQuiesceConflict
		}
		if err := workflow.requireActiveRequest(ctx, requestID, parentID); err != nil {
			return admin.ControllerDecommissionInspection{}, err
		}
	} else if err := workflow.leadership.RequireActiveLeadership(ctx); err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	if err := workflow.journals.InspectParentClear(ctx, parentID); err != nil {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("inspect parent reservation journal: %w", err)
	}
	snapshot, err := workflow.records.Read(ctx)
	if err != nil {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("read parent decommission inspection inventory: %w", err)
	}
	if _, err := recovery.BuildStartupInventoryPlan(snapshot); err != nil {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("validate parent decommission inspection inventory: %w", err)
	}
	recordSnapshot, err := decommissionRecordSnapshot(snapshot, parentID, state)
	if err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	blockers, err := pool.DecommissionRecordBlockers(recordSnapshot)
	if err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	refresh, err := workflow.installation.ValidateInstallationInventory(ctx)
	if err != nil {
		return admin.ControllerDecommissionInspection{}, fmt.Errorf("inspect parent decommission Instance inventory: %w", err)
	}
	targets, err := uninstallTargets(refresh, parentID)
	if err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	instances := make([]string, 0, len(targets))
	for _, target := range targets {
		instances = append(instances, target.ServerID)
	}
	slices.Sort(instances)
	inspection := admin.ControllerDecommissionInspection{
		RequestID: requestID, ParentFilesystemID: parentID, ParentState: state,
		Blockers: slices.Clone(blockers), CheckedInstanceIDs: instances,
	}
	if err := inspection.Validate(); err != nil {
		return admin.ControllerDecommissionInspection{}, err
	}
	return inspection, nil
}

func newControllerDecommissionWorkflow(gate *coordination.MutationGate, availability controllerDecommissionAvailability, leadership controllerUninstallLeadership, journals controllerDecommissionJournalBarrier, records checkpointInventoryReader, installation controllerInstallationInventory, cleanup controllerDecommissionCleanup, lease controllerUninstallLease, releaseDisposition func(bool), pools []pool.Config) (*controllerDecommissionWorkflow, error) {
	if gate == nil || availability == nil || leadership == nil || journals == nil || records == nil || installation == nil || cleanup == nil || lease == nil || releaseDisposition == nil {
		return nil, fmt.Errorf("controller decommission workflow dependency is nil")
	}
	if err := pool.ValidateConfigs(pools); err != nil {
		return nil, err
	}
	states := make(map[string]pool.ParentState)
	for _, configuredPool := range pools {
		for _, parent := range configuredPool.Filesystems {
			states[parent.ID] = parent.State
		}
	}
	return &controllerDecommissionWorkflow{
		gate: gate, availability: availability, leadership: leadership, journals: journals,
		records: records, installation: installation, cleanup: cleanup,
		lease: lease, releaseDisposition: releaseDisposition, parentStates: states,
	}, nil
}

func (workflow *controllerDecommissionWorkflow) QuiesceParent(ctx context.Context, requestID, parentID string) error {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(parentID); err != nil {
		return err
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	state, configured := workflow.parentStates[parentID]
	if !configured {
		return fmt.Errorf("decommission parent %q is not configured", parentID)
	}
	if state != pool.ParentDraining {
		return fmt.Errorf("decommission parent %q must be draining", parentID)
	}
	if workflow.requestID != "" {
		if workflow.requestID != requestID || workflow.parentID != parentID || workflow.gate.QuiesceRequestID() != requestID {
			return coordination.ErrQuiesceConflict
		}
		if workflow.validated {
			return nil
		}
		if !workflow.availabilitySet {
			if err := workflow.availability.BeginDecommission(); err != nil {
				return err
			}
			workflow.availabilitySet = true
		}
		return workflow.validateQuiescedParent(ctx)
	}
	if err := workflow.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	if err := workflow.gate.BeginQuiesce(ctx, requestID); err != nil {
		return err
	}
	workflow.requestID = requestID
	workflow.parentID = parentID
	if err := workflow.availability.BeginDecommission(); err != nil {
		return err
	}
	workflow.availabilitySet = true
	return workflow.validateQuiescedParent(ctx)
}

func (workflow *controllerDecommissionWorkflow) validateQuiescedParent(ctx context.Context) error {
	if err := workflow.requireActiveRequest(ctx, workflow.requestID, workflow.parentID); err != nil {
		return err
	}
	if err := workflow.journals.RequireParentClear(ctx, workflow.parentID); err != nil {
		return fmt.Errorf("validate quiesced parent reservation journal: %w", err)
	}
	snapshot, err := workflow.records.Read(ctx)
	if err != nil {
		return fmt.Errorf("read quiesced parent decommission inventory: %w", err)
	}
	if _, err := recovery.BuildStartupInventoryPlan(snapshot); err != nil {
		return fmt.Errorf("validate quiesced parent decommission inventory: %w", err)
	}
	recordSnapshot, err := decommissionRecordSnapshot(snapshot, workflow.parentID, workflow.parentStates[workflow.parentID])
	if err != nil {
		return err
	}
	blockers, err := pool.DecommissionRecordBlockers(recordSnapshot)
	if err != nil {
		return err
	}
	if len(blockers) != 0 {
		return fmt.Errorf("parent %q has %d quiesced decommission blocker(s); first is %s", workflow.parentID, len(blockers), blockers[0])
	}
	refresh, err := workflow.installation.ValidateInstallationInventory(ctx)
	if err != nil {
		return fmt.Errorf("capture parent decommission Instance inventory: %w", err)
	}
	targets, err := uninstallTargets(refresh, workflow.parentID)
	if err != nil {
		return err
	}
	workflow.targets = targets
	workflow.validated = true
	return nil
}

func decommissionRecordSnapshot(snapshot recovery.StartupInventorySnapshot, parentID string, state pool.ParentState) (pool.DecommissionRecordSnapshot, error) {
	records := pool.DecommissionRecordSnapshot{
		ParentFilesystemID: parentID, ParentState: state,
		Allocations: make([]volume.AllocationRecord, 0, len(snapshot.Allocations)),
	}
	for _, stored := range snapshot.Allocations {
		records.Allocations = append(records.Allocations, stored.Record)
	}
	parentFound := false
	for _, parent := range snapshot.Parents {
		if parent.ParentFilesystemID == parentID {
			if parentFound {
				return pool.DecommissionRecordSnapshot{}, fmt.Errorf("decommission parent %q inventory is duplicated", parentID)
			}
			parentFound = true
			records.Ownerships = slices.Clone(parent.Ownerships)
		}
	}
	if !parentFound {
		return pool.DecommissionRecordSnapshot{}, fmt.Errorf("decommission parent %q has no complete ownership inventory", parentID)
	}
	for _, persistentVolume := range snapshot.PersistentVolumes {
		immutable, err := persistentVolume.Validate()
		if err != nil {
			return pool.DecommissionRecordSnapshot{}, err
		}
		if immutable.ParentFilesystemID == parentID {
			records.References.PersistentVolumes = append(records.References.PersistentVolumes, persistentVolume.Name)
		}
	}
	return records, nil
}

func (workflow *controllerDecommissionWorkflow) CleanupParent(ctx context.Context, requestID, parentID string) (admin.ControllerCleanupEvidence, error) {
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if err := workflow.requireActiveRequest(ctx, requestID, parentID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if !workflow.validated {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("parent decommission quiesced validation is incomplete")
	}
	if workflow.cleaned != nil {
		return cloneControllerCleanupEvidence(*workflow.cleaned), nil
	}
	evidence, err := workflow.cleanup.CleanupParent(ctx, requestID, parentID, slices.Clone(workflow.targets))
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if len(evidence.UnmountedParents) != 1 || evidence.UnmountedParents[0].ParentFilesystemID != parentID {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("parent decommission cleanup returned another parent set")
	}
	stored := cloneControllerCleanupEvidence(evidence)
	workflow.cleaned = &stored
	return cloneControllerCleanupEvidence(stored), nil
}

func (workflow *controllerDecommissionWorkflow) ReleaseAfterParentCleanup(ctx context.Context, requestID, parentID string) (coordination.LeaseSnapshot, error) {
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if workflow.released != nil {
		if workflow.requestID != requestID || workflow.parentID != parentID {
			return coordination.LeaseSnapshot{}, coordination.ErrQuiesceConflict
		}
		return cloneLeaseSnapshotForUninstall(*workflow.released), nil
	}
	if err := workflow.requireActiveRequest(ctx, requestID, parentID); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	if workflow.cleaned == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("parent decommission cleanup has not completed")
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

func (workflow *controllerDecommissionWorkflow) requireActiveRequest(ctx context.Context, requestID, parentID string) error {
	if workflow.requestID == "" || workflow.requestID != requestID || workflow.parentID != parentID || workflow.gate.QuiesceRequestID() != requestID {
		return coordination.ErrQuiesceConflict
	}
	return workflow.leadership.RequireActiveLeadership(ctx)
}

var _ admin.ControllerDecommissionWorkflow = (*controllerDecommissionWorkflow)(nil)
