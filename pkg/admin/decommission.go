package admin

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// DecommissionMode selects a read-only plan or the complete offline sequence.
type DecommissionMode string

const (
	// DecommissionDryRun performs complete online inventory without mutation.
	DecommissionDryRun DecommissionMode = "dry-run"
	// DecommissionExecute runs the target-bound stop, unmount, and detach flow.
	DecommissionExecute DecommissionMode = "execute"
)

// DecommissionInventory is one complete operator view for a still-configured
// parent. Blockers are already validated stable diagnostic identities.
type DecommissionInventory struct {
	Complete                  bool
	ParentFilesystemID        string
	ParentState               pool.ParentState
	Blockers                  []string
	NodeTargets               []UninstallNodeTarget
	NodeParentMountRoot       string
	ControllerParentMountRoot string
	ChartVersion              string
	DriverVersion             string
}

// DecommissionOperatorBackend owns caller-authorized Kubernetes and local
// admin operations. Every method must be idempotent for one request ID.
type DecommissionOperatorBackend interface {
	ReadDecommissionInventory(ctx context.Context, request MutationRequest, parentFilesystemID string) (DecommissionInventory, error)
	QuiesceParent(ctx context.Context, requestID, parentFilesystemID string) error
	UnmountNodeParent(ctx context.Context, requestID, parentFilesystemID string, target UninstallNodeTarget) (NodeDecommissionUnmountResult, error)
	DeleteNodePlugin(ctx context.Context, requestID string) error
	WaitNodePluginStopped(ctx context.Context, requestID string) error
	CleanupControllerParent(ctx context.Context, requestID, parentFilesystemID string) (ControllerCleanupEvidence, error)
	ReleaseControllerAfterDecommission(ctx context.Context, requestID, parentFilesystemID string) (coordination.LeaseSnapshot, error)
	ScaleControllerToZero(ctx context.Context, requestID string) error
	WaitControllerStopped(ctx context.Context, requestID string) (time.Time, error)
}

// DecommissionPlan is the immutable normalized plan shown before mutation.
type DecommissionPlan struct {
	ChartVersion              string                `json:"chartVersion"`
	DriverVersion             string                `json:"driverVersion"`
	AdminVersion              string                `json:"adminVersion"`
	LeaseName                 string                `json:"leaseName"`
	ParentFilesystemID        string                `json:"parentFilesystemID"`
	NodeTargets               []UninstallNodeTarget `json:"nodeTargets"`
	NodeParentMountRoot       string                `json:"nodeParentMountRoot"`
	ControllerParentMountRoot string                `json:"controllerParentMountRoot"`
}

// DecommissionAudit is the final evidence permitting the operator to remove
// exactly one parent from values and restart the release.
type DecommissionAudit struct {
	SchemaVersion             string                          `json:"schemaVersion"`
	RequestID                 string                          `json:"requestID"`
	ChartVersion              string                          `json:"chartVersion"`
	DriverVersion             string                          `json:"driverVersion"`
	AdminVersion              string                          `json:"adminVersion"`
	LeaseName                 string                          `json:"leaseName"`
	LeaseUID                  string                          `json:"leaseUID"`
	ParentFilesystemID        string                          `json:"parentFilesystemID"`
	NodeParentMountRoot       string                          `json:"nodeParentMountRoot"`
	ControllerParentMountRoot string                          `json:"controllerParentMountRoot"`
	CheckedNodeIDs            []string                        `json:"checkedNodeIDs"`
	CheckedInstanceIDs        []string                        `json:"checkedInstanceIDs"`
	NodeUnmounts              []NodeDecommissionUnmountResult `json:"nodeUnmounts"`
	ControllerUnmount         ParentUnmountEvidence           `json:"controllerUnmount"`
	Detached                  bool                            `json:"detached"`
	RegionalInventorySHA256   string                          `json:"regionalInventorySHA256"`
	InstanceInventorySHA256   string                          `json:"instanceInventorySHA256"`
	CompletedAt               string                          `json:"completedAt"`
}

// Validate checks the closed final audit independent of live cluster access.
func (audit DecommissionAudit) Validate() error {
	if audit.SchemaVersion != volume.SchemaVersionV1 {
		return fmt.Errorf("decommission audit schema %q is unsupported", audit.SchemaVersion)
	}
	if err := volume.ValidateOperationID(audit.RequestID); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(audit.ParentFilesystemID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"chart version": audit.ChartVersion, "driver version": audit.DriverVersion, "admin version": audit.AdminVersion,
	} {
		if err := validateAuditText(name, value); err != nil {
			return err
		}
	}
	if audit.LeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("decommission audit Lease name is not fixed")
	}
	if err := volume.ValidateOperationID(audit.LeaseUID); err != nil {
		return fmt.Errorf("decommission audit Lease UID: %w", err)
	}
	if err := validateMountRoot("node parent mount root", audit.NodeParentMountRoot); err != nil {
		return err
	}
	if err := validateMountRoot("controller parent mount root", audit.ControllerParentMountRoot); err != nil {
		return err
	}
	if err := validateSortedUniqueNodeIDs(audit.CheckedNodeIDs); err != nil || len(audit.CheckedNodeIDs) == 0 {
		return fmt.Errorf("decommission audit nodes: %w", firstError(err, fmt.Errorf("node set is empty")))
	}
	if err := validateSortedUniqueProviderIDs("Instance", audit.CheckedInstanceIDs, false); err != nil {
		return err
	}
	wantInstances := make([]string, 0, len(audit.CheckedNodeIDs))
	for _, nodeID := range audit.CheckedNodeIDs {
		wantInstances = append(wantInstances, strings.SplitN(nodeID, "/", 2)[1])
	}
	slices.Sort(wantInstances)
	if !slices.Equal(wantInstances, audit.CheckedInstanceIDs) {
		return fmt.Errorf("decommission audit checked Instances differ from nodes")
	}
	if len(audit.NodeUnmounts) != len(audit.CheckedNodeIDs) {
		return fmt.Errorf("decommission audit node evidence count differs from nodes")
	}
	for index, unmount := range audit.NodeUnmounts {
		if unmount.NodeID != audit.CheckedNodeIDs[index] || unmount.Unmounted.ParentFilesystemID != audit.ParentFilesystemID ||
			unmount.Unmounted.MountPath != audit.NodeParentMountRoot+"/"+audit.ParentFilesystemID ||
			len(unmount.RemainingStagingMountPaths) != 0 || len(unmount.RemainingWorkloadTargetPaths) != 0 {
			return fmt.Errorf("decommission audit node %q evidence is incomplete", unmount.NodeID)
		}
	}
	if audit.ControllerUnmount.ParentFilesystemID != audit.ParentFilesystemID || audit.ControllerUnmount.MountPath != audit.ControllerParentMountRoot+"/"+audit.ParentFilesystemID {
		return fmt.Errorf("decommission audit controller unmount differs from target parent")
	}
	if !sha256DigestPattern.MatchString(audit.RegionalInventorySHA256) || !sha256DigestPattern.MatchString(audit.InstanceInventorySHA256) {
		return fmt.Errorf("decommission audit inventory hashes must be lowercase SHA-256 digests")
	}
	completed, err := time.Parse(time.RFC3339Nano, audit.CompletedAt)
	if err != nil || completed.IsZero() || completed.Unix() < 0 || completed.Location() != time.UTC || completed.Format(time.RFC3339Nano) != audit.CompletedAt {
		return fmt.Errorf("decommission audit completion time must be canonical RFC 3339 UTC")
	}
	return nil
}

// DecommissionPrepareResult reports a dry-run plan, blockers, or final audit.
type DecommissionPrepareResult struct {
	RequestID string             `json:"requestID"`
	Mode      DecommissionMode   `json:"mode"`
	Ready     bool               `json:"ready"`
	Completed bool               `json:"completed"`
	Blockers  []string           `json:"blockers,omitempty"`
	Plan      DecommissionPlan   `json:"plan"`
	Audit     *DecommissionAudit `json:"audit,omitempty"`
}

// DecommissionCoordinator executes the explicit offline ordering.
type DecommissionCoordinator struct {
	backend DecommissionOperatorBackend
}

// NewDecommissionCoordinator validates the operator backend.
func NewDecommissionCoordinator(backend DecommissionOperatorBackend) (*DecommissionCoordinator, error) {
	if backend == nil {
		return nil, fmt.Errorf("decommission coordinator dependency is nil")
	}
	return &DecommissionCoordinator{backend: backend}, nil
}

// Prepare returns a read-only plan or executes the exact target-bound flow.
func (coordinator *DecommissionCoordinator) Prepare(ctx context.Context, request MutationRequest, parentFilesystemID string, mode DecommissionMode) (DecommissionPrepareResult, error) {
	if err := request.Validate(); err != nil {
		return DecommissionPrepareResult{}, err
	}
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return DecommissionPrepareResult{}, err
	}
	if mode != DecommissionDryRun && mode != DecommissionExecute {
		return DecommissionPrepareResult{}, fmt.Errorf("decommission mode %q is unsupported", mode)
	}
	inventory, err := coordinator.backend.ReadDecommissionInventory(ctx, request, parentFilesystemID)
	if err != nil {
		return DecommissionPrepareResult{}, fmt.Errorf("read parent decommission inventory: %w", err)
	}
	plan, blockers, err := validateDecommissionInventory(request, parentFilesystemID, inventory)
	result := DecommissionPrepareResult{RequestID: request.RequestID, Mode: mode, Plan: plan}
	if err != nil {
		return result, err
	}
	if len(blockers) != 0 {
		result.Blockers = blockers
		if mode == DecommissionDryRun {
			return result, nil
		}
		return result, fmt.Errorf("parent decommission has %d blocker(s); first is %s", len(blockers), blockers[0])
	}
	result.Ready = true
	if mode == DecommissionDryRun {
		return result, nil
	}
	if err := coordinator.backend.QuiesceParent(ctx, request.RequestID, parentFilesystemID); err != nil {
		return result, fmt.Errorf("quiesce parent decommission: %w", err)
	}
	quiesced, err := coordinator.backend.ReadDecommissionInventory(ctx, request, parentFilesystemID)
	if err != nil {
		return result, fmt.Errorf("reread parent decommission inventory while quiesced: %w", err)
	}
	quiescedPlan, racedBlockers, err := validateDecommissionInventory(request, parentFilesystemID, quiesced)
	if err != nil {
		return result, err
	}
	if !equalDecommissionPlans(plan, quiescedPlan) {
		return result, fmt.Errorf("parent decommission immutable plan changed after quiesce")
	}
	if len(racedBlockers) != 0 {
		return result, fmt.Errorf("parent decommission gained %d blocker(s) after quiesce; first is %s", len(racedBlockers), racedBlockers[0])
	}

	nodeEvidence := make([]NodeDecommissionUnmountResult, 0, len(plan.NodeTargets))
	for _, target := range plan.NodeTargets {
		evidence, err := coordinator.backend.UnmountNodeParent(ctx, request.RequestID, parentFilesystemID, target)
		if err != nil {
			return result, fmt.Errorf("unmount parent on node %q: %w", target.NodeID, err)
		}
		if err := ValidateNodeDecommissionUnmountEvidence(evidence, target.NodeID, parentFilesystemID, plan.NodeParentMountRoot); err != nil {
			return result, err
		}
		nodeEvidence = append(nodeEvidence, evidence)
	}
	if err := coordinator.backend.DeleteNodePlugin(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("delete node plugin for parent decommission: %w", err)
	}
	if err := coordinator.backend.WaitNodePluginStopped(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("wait for node plugin stop: %w", err)
	}
	cleanup, err := coordinator.backend.CleanupControllerParent(ctx, request.RequestID, parentFilesystemID)
	if err != nil {
		return result, fmt.Errorf("cleanup controller parent: %w", err)
	}
	if err := ValidateDecommissionCleanupEvidence(cleanup, parentFilesystemID, plan.NodeTargets, plan.ControllerParentMountRoot); err != nil {
		return result, err
	}
	lease, err := coordinator.backend.ReleaseControllerAfterDecommission(ctx, request.RequestID, parentFilesystemID)
	if err != nil {
		return result, fmt.Errorf("release controller after parent decommission: %w", err)
	}
	checkedNodes := make([]string, 0, len(plan.NodeTargets))
	convertedNodes := make([]NodeUnmountEvidence, 0, len(nodeEvidence))
	for _, evidence := range nodeEvidence {
		checkedNodes = append(checkedNodes, evidence.NodeID)
		convertedNodes = append(convertedNodes, NodeUnmountEvidence{
			NodeID: evidence.NodeID, UnmountedParents: []ParentUnmountEvidence{evidence.Unmounted},
			RemainingChildMountPaths: append(slices.Clone(evidence.RemainingStagingMountPaths), evidence.RemainingWorkloadTargetPaths...),
		})
	}
	completion := UninstallCompletionEvidence{
		RequestID: request.RequestID, ExpectedNodeIDs: checkedNodes,
		ExpectedParentFilesystemIDs: []string{parentFilesystemID}, Nodes: convertedNodes,
		ProviderInventoriesFresh: cleanup.ProviderInventoriesFresh,
		RegionalAttachmentIDs:    cleanup.RegionalAttachmentIDs, InstanceAttachmentIDs: cleanup.InstanceAttachmentIDs,
		ReleasedLease: lease,
	}
	if err := ValidateUninstallCompletion(completion); err != nil {
		return result, fmt.Errorf("validate parent decommission completion: %w", err)
	}
	if err := coordinator.backend.ScaleControllerToZero(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("scale controller to zero after parent decommission: %w", err)
	}
	completedAt, err := coordinator.backend.WaitControllerStopped(ctx, request.RequestID)
	if err != nil {
		return result, fmt.Errorf("wait for controller stop after parent decommission: %w", err)
	}
	slices.SortFunc(nodeEvidence, func(left, right NodeDecommissionUnmountResult) int { return strings.Compare(left.NodeID, right.NodeID) })
	checkedInstances := slices.Clone(cleanup.CheckedInstanceIDs)
	slices.Sort(checkedInstances)
	audit := &DecommissionAudit{
		SchemaVersion: volume.SchemaVersionV1, RequestID: request.RequestID,
		ChartVersion: plan.ChartVersion, DriverVersion: plan.DriverVersion, AdminVersion: plan.AdminVersion,
		LeaseName: volume.LeadershipLeaseNameV1, LeaseUID: lease.UID,
		ParentFilesystemID: parentFilesystemID, NodeParentMountRoot: plan.NodeParentMountRoot,
		ControllerParentMountRoot: plan.ControllerParentMountRoot, CheckedNodeIDs: checkedNodes,
		CheckedInstanceIDs: checkedInstances, NodeUnmounts: nodeEvidence,
		ControllerUnmount: cleanup.UnmountedParents[0], Detached: slices.Contains(cleanup.DetachedParentFilesystemIDs, parentFilesystemID),
		RegionalInventorySHA256: cleanup.RegionalInventorySHA256, InstanceInventorySHA256: cleanup.InstanceInventorySHA256,
		CompletedAt: completedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := audit.Validate(); err != nil {
		return result, err
	}
	result.Completed = true
	result.Audit = audit
	return result, nil
}

func validateDecommissionInventory(request MutationRequest, parentFilesystemID string, inventory DecommissionInventory) (DecommissionPlan, []string, error) {
	plan := DecommissionPlan{
		ChartVersion: inventory.ChartVersion, DriverVersion: inventory.DriverVersion,
		AdminVersion: request.AdminVersion, LeaseName: volume.LeadershipLeaseNameV1,
		ParentFilesystemID: parentFilesystemID, NodeTargets: slices.Clone(inventory.NodeTargets),
		NodeParentMountRoot: inventory.NodeParentMountRoot, ControllerParentMountRoot: inventory.ControllerParentMountRoot,
	}
	if !inventory.Complete || inventory.ParentFilesystemID != parentFilesystemID || inventory.ParentState != pool.ParentDraining {
		return plan, nil, fmt.Errorf("parent decommission inventory is incomplete, mismatched, or not draining")
	}
	if err := validateAuditText("chart version", inventory.ChartVersion); err != nil {
		return plan, nil, err
	}
	if err := validateAuditText("driver version", inventory.DriverVersion); err != nil {
		return plan, nil, err
	}
	if err := validateMountRoot("node parent mount root", inventory.NodeParentMountRoot); err != nil {
		return plan, nil, err
	}
	if err := validateMountRoot("controller parent mount root", inventory.ControllerParentMountRoot); err != nil {
		return plan, nil, err
	}
	slices.SortFunc(plan.NodeTargets, func(left, right UninstallNodeTarget) int { return strings.Compare(left.NodeID, right.NodeID) })
	if len(plan.NodeTargets) == 0 {
		return plan, nil, fmt.Errorf("parent decommission node target set is empty")
	}
	seenNodes := make(map[string]struct{}, len(plan.NodeTargets))
	seenPods := make(map[string]struct{}, len(plan.NodeTargets))
	for _, target := range plan.NodeTargets {
		if err := volume.ValidateNodeID(target.NodeID); err != nil {
			return plan, nil, err
		}
		if err := validateBlockerIdentity("node Pod", target.PodName); err != nil {
			return plan, nil, err
		}
		if _, duplicate := seenNodes[target.NodeID]; duplicate {
			return plan, nil, fmt.Errorf("parent decommission node %q is duplicated", target.NodeID)
		}
		if _, duplicate := seenPods[target.PodName]; duplicate {
			return plan, nil, fmt.Errorf("parent decommission node Pod %q is duplicated", target.PodName)
		}
		seenNodes[target.NodeID] = struct{}{}
		seenPods[target.PodName] = struct{}{}
	}
	blockers := slices.Clone(inventory.Blockers)
	for _, blocker := range blockers {
		if err := validateBlockerIdentity("decommission", blocker); err != nil {
			return plan, nil, err
		}
	}
	slices.Sort(blockers)
	if len(slices.Compact(slices.Clone(blockers))) != len(blockers) {
		return plan, nil, fmt.Errorf("parent decommission blocker set contains duplicates")
	}
	return plan, blockers, nil
}

func equalDecommissionPlans(left, right DecommissionPlan) bool {
	return left.ChartVersion == right.ChartVersion && left.DriverVersion == right.DriverVersion &&
		left.AdminVersion == right.AdminVersion && left.LeaseName == right.LeaseName &&
		left.ParentFilesystemID == right.ParentFilesystemID &&
		left.NodeParentMountRoot == right.NodeParentMountRoot && left.ControllerParentMountRoot == right.ControllerParentMountRoot &&
		slices.Equal(left.NodeTargets, right.NodeTargets)
}

// ValidateNodeDecommissionUnmountEvidence proves that a node-local result is
// bound to the exact frozen node, parent, and configured mount root.
func ValidateNodeDecommissionUnmountEvidence(evidence NodeDecommissionUnmountResult, nodeID, parentID, root string) error {
	if evidence.NodeID != nodeID || evidence.Unmounted.ParentFilesystemID != parentID || evidence.Unmounted.MountPath != root+"/"+parentID ||
		len(evidence.RemainingStagingMountPaths) != 0 || len(evidence.RemainingWorkloadTargetPaths) != 0 {
		return fmt.Errorf("node %q returned incomplete target-parent unmount evidence", nodeID)
	}
	return nil
}

// ValidateDecommissionCleanupEvidence proves target-only controller unmount,
// detach, and fresh dual-inventory absence for the frozen release node set.
func ValidateDecommissionCleanupEvidence(evidence ControllerCleanupEvidence, parentID string, nodeTargets []UninstallNodeTarget, controllerParentMountRoot string) error {
	if len(evidence.UnmountedParents) != 1 || evidence.UnmountedParents[0].ParentFilesystemID != parentID ||
		evidence.UnmountedParents[0].MountPath != controllerParentMountRoot+"/"+parentID ||
		len(evidence.RemainingControllerMountPaths) != 0 || !evidence.ProviderInventoriesFresh ||
		len(evidence.RegionalAttachmentIDs) != 0 || len(evidence.InstanceAttachmentIDs) != 0 {
		return fmt.Errorf("controller cleanup evidence differs from decommission target or retains live state")
	}
	if !sha256DigestPattern.MatchString(evidence.RegionalInventorySHA256) || !sha256DigestPattern.MatchString(evidence.InstanceInventorySHA256) {
		return fmt.Errorf("controller decommission cleanup inventory hashes are malformed")
	}
	if len(evidence.DetachedParentFilesystemIDs) > 1 {
		return fmt.Errorf("controller decommission cleanup detached parent evidence is duplicated")
	}
	for _, detached := range evidence.DetachedParentFilesystemIDs {
		if detached != parentID {
			return fmt.Errorf("controller decommission cleanup detached unrelated parent %q", detached)
		}
	}
	wantInstances := make([]string, 0, len(nodeTargets))
	for _, target := range nodeTargets {
		wantInstances = append(wantInstances, strings.SplitN(target.NodeID, "/", 2)[1])
	}
	slices.Sort(wantInstances)
	checked := slices.Clone(evidence.CheckedInstanceIDs)
	slices.Sort(checked)
	if !slices.Equal(checked, wantInstances) {
		return fmt.Errorf("controller decommission cleanup checked Instance set differs from node plan")
	}
	return nil
}
