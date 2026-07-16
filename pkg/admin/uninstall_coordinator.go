package admin

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const uninstallAuditSchemaV1 = "1"

var sha256DigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// UninstallMode selects a read-only plan or the complete ordered workflow.
type UninstallMode string

const (
	// UninstallDryRun inventories and validates blockers without mutation.
	UninstallDryRun UninstallMode = "dry-run"
	// UninstallExecute runs the request-bound idempotent cleanup sequence.
	UninstallExecute UninstallMode = "execute"
)

// UninstallNodeTarget binds one expected eligible node to the exact current
// node-plugin Pod reached with the operator's authorization.
type UninstallNodeTarget struct {
	NodeID  string `json:"nodeID"`
	PodName string `json:"podName"`
}

// UninstallInventory is one complete, pre-mutation operator inventory.
type UninstallInventory struct {
	Complete                  bool
	Preflight                 UninstallPreflightSnapshot
	ParentFilesystemIDs       []string
	NodeTargets               []UninstallNodeTarget
	NodeParentMountRoot       string
	ControllerParentMountRoot string
	ChartVersion              string
	DriverVersion             string
}

// ControllerCleanupEvidence records the controller-local unmount and provider
// fencing result before leadership release.
type ControllerCleanupEvidence struct {
	UnmountedParents              []ParentUnmountEvidence `json:"unmountedParents"`
	DetachedParentFilesystemIDs   []string                `json:"detachedParentFilesystemIDs"`
	CheckedInstanceIDs            []string                `json:"checkedInstanceIDs"`
	RegionalInventorySHA256       string                  `json:"regionalInventorySHA256"`
	InstanceInventorySHA256       string                  `json:"instanceInventorySHA256"`
	ProviderInventoriesFresh      bool                    `json:"providerInventoriesFresh"`
	RegionalAttachmentIDs         []string                `json:"regionalAttachmentIDs"`
	InstanceAttachmentIDs         []string                `json:"instanceAttachmentIDs"`
	RemainingControllerMountPaths []string                `json:"remainingControllerMountPaths"`
}

// UninstallOperatorBackend is the caller-authorized Kubernetes/admin boundary.
// Every mutating method must be idempotent for one request ID and derive paths,
// parents, workloads, and object names from fresh observed state rather than
// caller-provided arbitrary strings.
type UninstallOperatorBackend interface {
	ReadUninstallInventory(ctx context.Context, request MutationRequest) (UninstallInventory, error)
	QuiesceController(ctx context.Context, requestID string) error
	UnmountNodeParents(ctx context.Context, requestID string, target UninstallNodeTarget) (NodeUnmountEvidence, error)
	DeleteNodePlugin(ctx context.Context, requestID string) error
	WaitNodePluginStopped(ctx context.Context, requestID string) error
	CleanupControllerParents(ctx context.Context, requestID string) (ControllerCleanupEvidence, error)
	ReleaseController(ctx context.Context, requestID string) (coordination.LeaseSnapshot, error)
	ScaleControllerToZero(ctx context.Context, requestID string) error
	WaitControllerStopped(ctx context.Context, requestID string) (time.Time, error)
}

// UninstallPlan is the bounded read-only plan returned before any mutation.
type UninstallPlan struct {
	ChartVersion              string                `json:"chartVersion"`
	DriverVersion             string                `json:"driverVersion"`
	AdminVersion              string                `json:"adminVersion"`
	LeaseName                 string                `json:"leaseName"`
	ParentFilesystemIDs       []string              `json:"parentFilesystemIDs"`
	NodeTargets               []UninstallNodeTarget `json:"nodeTargets"`
	NodeParentMountRoot       string                `json:"nodeParentMountRoot"`
	ControllerParentMountRoot string                `json:"controllerParentMountRoot"`
}

// UninstallAudit is the final independently verifiable operator artifact.
type UninstallAudit struct {
	SchemaVersion               string                  `json:"schemaVersion"`
	RequestID                   string                  `json:"requestID"`
	ChartVersion                string                  `json:"chartVersion"`
	DriverVersion               string                  `json:"driverVersion"`
	AdminVersion                string                  `json:"adminVersion"`
	LeaseName                   string                  `json:"leaseName"`
	LeaseUID                    string                  `json:"leaseUID"`
	ParentFilesystemIDs         []string                `json:"parentFilesystemIDs"`
	NodeParentMountRoot         string                  `json:"nodeParentMountRoot"`
	ControllerParentMountRoot   string                  `json:"controllerParentMountRoot"`
	CheckedNodeIDs              []string                `json:"checkedNodeIDs"`
	CheckedInstanceIDs          []string                `json:"checkedInstanceIDs"`
	NodeUnmounts                []NodeUnmountEvidence   `json:"nodeUnmounts"`
	ControllerUnmountedParents  []ParentUnmountEvidence `json:"controllerUnmountedParents"`
	DetachedParentFilesystemIDs []string                `json:"detachedParentFilesystemIDs"`
	RegionalInventorySHA256     string                  `json:"regionalInventorySHA256"`
	InstanceInventorySHA256     string                  `json:"instanceInventorySHA256"`
	CompletedAt                 string                  `json:"completedAt"`
}

// Validate checks the final audit's closed identities, stable order, hashes,
// and completion time.
func (audit UninstallAudit) Validate() error {
	if audit.SchemaVersion != uninstallAuditSchemaV1 {
		return fmt.Errorf("uninstall audit schema %q is unsupported", audit.SchemaVersion)
	}
	if err := volume.ValidateOperationID(audit.RequestID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"chart version":  audit.ChartVersion,
		"driver version": audit.DriverVersion,
		"admin version":  audit.AdminVersion,
	} {
		if err := validateAuditText(name, value); err != nil {
			return err
		}
	}
	if audit.LeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("uninstall audit Lease name %q is not fixed v1 name", audit.LeaseName)
	}
	if err := volume.ValidateOperationID(audit.LeaseUID); err != nil {
		return fmt.Errorf("uninstall audit Lease UID: %w", err)
	}
	if err := validateSortedUniqueNodeIDs(audit.CheckedNodeIDs); err != nil {
		return fmt.Errorf("uninstall audit nodes: %w", err)
	}
	parents, err := normalizedParentIDs(audit.ParentFilesystemIDs)
	if err != nil || len(parents) == 0 || !slices.Equal(parents, audit.ParentFilesystemIDs) {
		return fmt.Errorf("uninstall audit parents must be non-empty, unique, and sorted: %w", firstError(err, fmt.Errorf("parent order mismatch")))
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
		return fmt.Errorf("uninstall audit checked Instances differ from checked nodes")
	}
	if err := validateMountRoot("node parent mount root", audit.NodeParentMountRoot); err != nil {
		return err
	}
	if err := validateMountRoot("controller parent mount root", audit.ControllerParentMountRoot); err != nil {
		return err
	}
	if len(audit.NodeUnmounts) != len(audit.CheckedNodeIDs) {
		return fmt.Errorf("uninstall audit node evidence count differs from checked nodes")
	}
	for index, node := range audit.NodeUnmounts {
		if node.NodeID != audit.CheckedNodeIDs[index] {
			return fmt.Errorf("uninstall audit node evidence is not sorted with checked nodes")
		}
		unmounted, unmountErr := validateParentUnmountEvidence(node.UnmountedParents)
		if unmountErr == nil {
			unmountErr = validateExactUnmountPaths(node.UnmountedParents, audit.NodeParentMountRoot)
		}
		if unmountErr != nil || !slices.Equal(unmounted, parents) || len(node.RemainingParentMountPaths) != 0 || len(node.RemainingChildMountPaths) != 0 {
			return fmt.Errorf("uninstall audit node %q has incomplete mount evidence: %w", node.NodeID, firstError(unmountErr, fmt.Errorf("parent or remaining-mount mismatch")))
		}
	}
	controllerParents, err := validateParentUnmountEvidence(audit.ControllerUnmountedParents)
	if err == nil {
		err = validateExactUnmountPaths(audit.ControllerUnmountedParents, audit.ControllerParentMountRoot)
	}
	if err != nil || !slices.Equal(controllerParents, parents) {
		return fmt.Errorf("uninstall audit controller mount evidence: %w", firstError(err, fmt.Errorf("parent set mismatch")))
	}
	if err := validateSortedUniqueProviderIDs("detached parent", audit.DetachedParentFilesystemIDs, true); err != nil {
		return err
	}
	for _, detached := range audit.DetachedParentFilesystemIDs {
		if !slices.Contains(parents, detached) {
			return fmt.Errorf("uninstall audit detached parent %q is outside configured set", detached)
		}
	}
	if !sha256DigestPattern.MatchString(audit.RegionalInventorySHA256) || !sha256DigestPattern.MatchString(audit.InstanceInventorySHA256) {
		return fmt.Errorf("uninstall audit inventory hashes must be lowercase SHA-256 digests")
	}
	completed, err := time.Parse(time.RFC3339Nano, audit.CompletedAt)
	if err != nil || completed.IsZero() || completed.Unix() < 0 || completed.Location() != time.UTC || completed.Format(time.RFC3339Nano) != audit.CompletedAt {
		return fmt.Errorf("uninstall audit completion time must be canonical RFC 3339 UTC")
	}
	return nil
}

// UninstallPrepareResult reports a dry-run plan, blocker, or completed audit.
type UninstallPrepareResult struct {
	RequestID string          `json:"requestID"`
	Mode      UninstallMode   `json:"mode"`
	Ready     bool            `json:"ready"`
	Completed bool            `json:"completed"`
	Blockers  []string        `json:"blockers,omitempty"`
	Plan      UninstallPlan   `json:"plan"`
	Audit     *UninstallAudit `json:"audit,omitempty"`
}

// UninstallCoordinator executes the explicit operator-authorized ordering. It
// never deletes workloads, PVCs, PVs, tombstones, claims, or Secrets.
type UninstallCoordinator struct {
	backend UninstallOperatorBackend
}

// NewUninstallCoordinator validates the operator backend.
func NewUninstallCoordinator(backend UninstallOperatorBackend) (*UninstallCoordinator, error) {
	if backend == nil {
		return nil, fmt.Errorf("uninstall coordinator dependency is nil")
	}
	return &UninstallCoordinator{backend: backend}, nil
}

// Prepare inventories and either returns a read-only plan or runs the complete
// state-driven safe-uninstall sequence. An execution error deliberately leaves
// the request-bound quiesce/observed state for an exact same-ID retry.
func (coordinator *UninstallCoordinator) Prepare(ctx context.Context, request MutationRequest, mode UninstallMode) (UninstallPrepareResult, error) {
	if err := request.Validate(); err != nil {
		return UninstallPrepareResult{}, err
	}
	if mode != UninstallDryRun && mode != UninstallExecute {
		return UninstallPrepareResult{}, fmt.Errorf("uninstall mode %q is unsupported", mode)
	}
	inventory, err := coordinator.backend.ReadUninstallInventory(ctx, request)
	if err != nil {
		return UninstallPrepareResult{}, fmt.Errorf("read safe-uninstall inventory: %w", err)
	}
	plan, targets, parents, validationErr := validateUninstallInventory(request, inventory)
	result := UninstallPrepareResult{RequestID: request.RequestID, Mode: mode, Plan: plan}
	if validationErr != nil {
		return result, validationErr
	}
	blockers, err := UninstallPreflightBlockers(inventory.Preflight)
	if err != nil {
		return result, err
	}
	if len(blockers) != 0 {
		result.Blockers = blockers
		if mode == UninstallDryRun {
			return result, nil
		}
		return result, fmt.Errorf("safe uninstall has %d blocker(s); first is %s", len(blockers), blockers[0])
	}
	result.Ready = true
	if mode == UninstallDryRun {
		return result, nil
	}

	if err := coordinator.backend.QuiesceController(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("quiesce controller for safe uninstall: %w", err)
	}
	// Re-read the complete inventory after the barrier closes. The first read is
	// operator guidance, not authority to unmount: a CreateVolume, publish, Pod,
	// or attachment could otherwise appear between preflight and quiesce.
	confirmed, err := coordinator.backend.ReadUninstallInventory(ctx, request)
	if err != nil {
		return result, fmt.Errorf("re-read safe-uninstall inventory after quiesce: %w", err)
	}
	confirmedPlan, confirmedTargets, confirmedParents, err := validateUninstallInventory(request, confirmed)
	if err != nil {
		return result, fmt.Errorf("validate safe-uninstall inventory after quiesce: %w", err)
	}
	if !sameUninstallPlan(plan, confirmedPlan) || !slices.EqualFunc(targets, confirmedTargets, func(left, right UninstallNodeTarget) bool {
		return left == right
	}) || !slices.Equal(parents, confirmedParents) {
		return result, fmt.Errorf("safe-uninstall identities changed while quiescing")
	}
	confirmedBlockers, err := UninstallPreflightBlockers(confirmed.Preflight)
	if err != nil {
		return result, fmt.Errorf("validate safe-uninstall blockers after quiesce: %w", err)
	}
	if len(confirmedBlockers) != 0 {
		return result, fmt.Errorf("safe uninstall gained %d blocker(s) while quiescing; first is %s", len(confirmedBlockers), confirmedBlockers[0])
	}
	inventory = confirmed
	result.Plan = confirmedPlan
	nodeEvidence := make([]NodeUnmountEvidence, 0, len(targets))
	for _, target := range targets {
		evidence, unmountErr := coordinator.backend.UnmountNodeParents(ctx, request.RequestID, target)
		if unmountErr != nil {
			return result, fmt.Errorf("unmount parents on node %q: %w", target.NodeID, unmountErr)
		}
		if evidence.NodeID != target.NodeID {
			return result, fmt.Errorf("node Pod %q returned evidence for %q, want %q", target.PodName, evidence.NodeID, target.NodeID)
		}
		unmounted, validateErr := validateParentUnmountEvidence(evidence.UnmountedParents)
		if validateErr == nil {
			validateErr = validateExactUnmountPaths(evidence.UnmountedParents, inventory.NodeParentMountRoot)
		}
		if validateErr != nil || !slices.Equal(unmounted, parents) || len(evidence.RemainingParentMountPaths) != 0 || len(evidence.RemainingChildMountPaths) != 0 {
			return result, fmt.Errorf("node %q returned incomplete unmount evidence: %w", target.NodeID, firstError(validateErr, fmt.Errorf("configured parent or remaining-mount mismatch")))
		}
		nodeEvidence = append(nodeEvidence, evidence)
	}
	if err := coordinator.backend.DeleteNodePlugin(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("delete exact node plugin DaemonSet: %w", err)
	}
	if err := coordinator.backend.WaitNodePluginStopped(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("wait for node plugin termination: %w", err)
	}
	cleanup, err := coordinator.backend.CleanupControllerParents(ctx, request.RequestID)
	if err != nil {
		return result, fmt.Errorf("clean controller parents and attachments: %w", err)
	}
	if err := validateControllerCleanup(cleanup, parents, targets, inventory.ControllerParentMountRoot); err != nil {
		return result, err
	}
	releasedLease, err := coordinator.backend.ReleaseController(ctx, request.RequestID)
	if err != nil {
		return result, fmt.Errorf("release controller leadership: %w", err)
	}
	if err := coordinator.backend.ScaleControllerToZero(ctx, request.RequestID); err != nil {
		return result, fmt.Errorf("scale controller to zero: %w", err)
	}
	completedAt, err := coordinator.backend.WaitControllerStopped(ctx, request.RequestID)
	if err != nil {
		return result, fmt.Errorf("wait for controller termination: %w", err)
	}
	completion := UninstallCompletionEvidence{
		RequestID: request.RequestID, ExpectedNodeIDs: nodeIDs(targets), ExpectedParentFilesystemIDs: parents,
		Nodes: nodeEvidence, ProviderInventoriesFresh: cleanup.ProviderInventoriesFresh,
		RegionalAttachmentIDs: cleanup.RegionalAttachmentIDs, InstanceAttachmentIDs: cleanup.InstanceAttachmentIDs,
		ControllerMountPaths: cleanup.RemainingControllerMountPaths, ReleasedLease: releasedLease,
	}
	if err := ValidateUninstallCompletion(completion); err != nil {
		return result, fmt.Errorf("validate final safe-uninstall evidence: %w", err)
	}
	audit, err := buildUninstallAudit(request, inventory, nodeEvidence, cleanup, releasedLease, completedAt)
	if err != nil {
		return result, err
	}
	result.Completed = true
	result.Audit = &audit
	return result, nil
}

func sameUninstallPlan(left, right UninstallPlan) bool {
	return left.ChartVersion == right.ChartVersion && left.DriverVersion == right.DriverVersion &&
		left.AdminVersion == right.AdminVersion && left.LeaseName == right.LeaseName &&
		left.NodeParentMountRoot == right.NodeParentMountRoot && left.ControllerParentMountRoot == right.ControllerParentMountRoot &&
		slices.Equal(left.ParentFilesystemIDs, right.ParentFilesystemIDs) &&
		slices.EqualFunc(left.NodeTargets, right.NodeTargets, func(leftTarget, rightTarget UninstallNodeTarget) bool {
			return leftTarget == rightTarget
		})
}

func validateUninstallInventory(request MutationRequest, inventory UninstallInventory) (UninstallPlan, []UninstallNodeTarget, []string, error) {
	plan := UninstallPlan{
		ChartVersion: inventory.ChartVersion, DriverVersion: inventory.DriverVersion,
		AdminVersion: request.AdminVersion, LeaseName: volume.LeadershipLeaseNameV1,
		ParentFilesystemIDs: slices.Clone(inventory.ParentFilesystemIDs), NodeTargets: slices.Clone(inventory.NodeTargets),
		NodeParentMountRoot: inventory.NodeParentMountRoot, ControllerParentMountRoot: inventory.ControllerParentMountRoot,
	}
	if !inventory.Complete {
		return plan, nil, nil, fmt.Errorf("safe-uninstall inventory is not fresh and complete")
	}
	if inventory.Preflight.Request != request {
		return plan, nil, nil, fmt.Errorf("safe-uninstall preflight request identity differs from caller")
	}
	if err := validateAuditText("chart version", inventory.ChartVersion); err != nil {
		return plan, nil, nil, err
	}
	if err := validateAuditText("driver version", inventory.DriverVersion); err != nil {
		return plan, nil, nil, err
	}
	if err := validateMountRoot("node parent mount root", inventory.NodeParentMountRoot); err != nil {
		return plan, nil, nil, err
	}
	if err := validateMountRoot("controller parent mount root", inventory.ControllerParentMountRoot); err != nil {
		return plan, nil, nil, err
	}
	parents, err := normalizedParentIDs(inventory.ParentFilesystemIDs)
	if err != nil || len(parents) == 0 {
		return plan, nil, nil, firstError(err, fmt.Errorf("safe uninstall requires configured parents"))
	}
	targets := slices.Clone(inventory.NodeTargets)
	slices.SortFunc(targets, func(left, right UninstallNodeTarget) int { return strings.Compare(left.NodeID, right.NodeID) })
	if len(targets) == 0 {
		return plan, nil, nil, fmt.Errorf("safe uninstall requires eligible node targets")
	}
	seenPods := make(map[string]struct{}, len(targets))
	for index, target := range targets {
		if err := volume.ValidateNodeID(target.NodeID); err != nil {
			return plan, nil, nil, fmt.Errorf("uninstall node target %d: %w", index, err)
		}
		if err := validateAuditText("node-plugin Pod name", target.PodName); err != nil {
			return plan, nil, nil, err
		}
		if index > 0 && targets[index-1].NodeID == target.NodeID {
			return plan, nil, nil, fmt.Errorf("uninstall node target %q is duplicated", target.NodeID)
		}
		if _, duplicate := seenPods[target.PodName]; duplicate {
			return plan, nil, nil, fmt.Errorf("uninstall node Pod %q is duplicated", target.PodName)
		}
		seenPods[target.PodName] = struct{}{}
	}
	plan.ParentFilesystemIDs = slices.Clone(parents)
	plan.NodeTargets = slices.Clone(targets)
	return plan, targets, parents, nil
}

func validateControllerCleanup(cleanup ControllerCleanupEvidence, parents []string, targets []UninstallNodeTarget, controllerRoot string) error {
	unmounted, err := validateParentUnmountEvidence(cleanup.UnmountedParents)
	if err == nil {
		err = validateExactUnmountPaths(cleanup.UnmountedParents, controllerRoot)
	}
	if err != nil || !slices.Equal(unmounted, parents) {
		return fmt.Errorf("controller did not prove every configured parent unmounted: %w", firstError(err, fmt.Errorf("parent set mismatch")))
	}
	if blocker, present := firstBoundedIdentity(cleanup.RemainingControllerMountPaths); present {
		return fmt.Errorf("controller retains mount %q", blocker)
	}
	if !cleanup.ProviderInventoriesFresh || len(cleanup.RegionalAttachmentIDs) != 0 || len(cleanup.InstanceAttachmentIDs) != 0 {
		return fmt.Errorf("controller did not prove fresh conclusive provider attachment absence")
	}
	detached, err := normalizedParentIDs(cleanup.DetachedParentFilesystemIDs)
	if err != nil {
		return fmt.Errorf("detached parent evidence: %w", err)
	}
	for _, parent := range detached {
		if !slices.Contains(parents, parent) {
			return fmt.Errorf("detached parent %q is outside configured set", parent)
		}
	}
	wantInstances := instanceIDs(targets)
	instances := slices.Clone(cleanup.CheckedInstanceIDs)
	slices.Sort(instances)
	if err := validateSortedUniqueProviderIDs("checked Instance", instances, false); err != nil {
		return err
	}
	if !slices.Equal(instances, wantInstances) {
		return fmt.Errorf("checked Instance inventory differs from eligible node set")
	}
	if !sha256DigestPattern.MatchString(cleanup.RegionalInventorySHA256) || !sha256DigestPattern.MatchString(cleanup.InstanceInventorySHA256) {
		return fmt.Errorf("controller cleanup inventory hashes are malformed")
	}
	return nil
}

func buildUninstallAudit(request MutationRequest, inventory UninstallInventory, nodes []NodeUnmountEvidence, cleanup ControllerCleanupEvidence, lease coordination.LeaseSnapshot, completedAt time.Time) (UninstallAudit, error) {
	slices.SortFunc(nodes, func(left, right NodeUnmountEvidence) int { return strings.Compare(left.NodeID, right.NodeID) })
	detached := slices.Clone(cleanup.DetachedParentFilesystemIDs)
	slices.Sort(detached)
	checkedInstances := slices.Clone(cleanup.CheckedInstanceIDs)
	slices.Sort(checkedInstances)
	parents := slices.Clone(inventory.ParentFilesystemIDs)
	slices.Sort(parents)
	audit := UninstallAudit{
		SchemaVersion: uninstallAuditSchemaV1, RequestID: request.RequestID,
		ChartVersion: inventory.ChartVersion, DriverVersion: inventory.DriverVersion, AdminVersion: request.AdminVersion,
		LeaseName: volume.LeadershipLeaseNameV1, LeaseUID: lease.UID,
		ParentFilesystemIDs: parents, NodeParentMountRoot: inventory.NodeParentMountRoot,
		ControllerParentMountRoot: inventory.ControllerParentMountRoot,
		CheckedNodeIDs:            nodeIDs(inventory.NodeTargets), CheckedInstanceIDs: checkedInstances,
		NodeUnmounts: slices.Clone(nodes), ControllerUnmountedParents: slices.Clone(cleanup.UnmountedParents),
		DetachedParentFilesystemIDs: detached,
		RegionalInventorySHA256:     cleanup.RegionalInventorySHA256, InstanceInventorySHA256: cleanup.InstanceInventorySHA256,
		CompletedAt: completedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := audit.Validate(); err != nil {
		return UninstallAudit{}, fmt.Errorf("validate safe-uninstall audit: %w", err)
	}
	return audit, nil
}

func nodeIDs(targets []UninstallNodeTarget) []string {
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		result = append(result, target.NodeID)
	}
	slices.Sort(result)
	return result
}

func instanceIDs(targets []UninstallNodeTarget) []string {
	result := make([]string, 0, len(targets))
	for _, target := range targets {
		result = append(result, strings.SplitN(target.NodeID, "/", 2)[1])
	}
	slices.Sort(result)
	return result
}

func validateAuditText(name, value string) error {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s must be single-line UTF-8 containing 1 to 128 bytes", name)
	}
	return nil
}

func validateMountRoot(name, value string) error {
	if value == "" || value == "/" || !strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return fmt.Errorf("%s %q must be absolute, normalized, and non-root", name, value)
	}
	if name == "node parent mount root" && value != config.FixedNodeParentMountRoot {
		return fmt.Errorf("node parent mount root must be fixed to %q", config.FixedNodeParentMountRoot)
	}
	return nil
}

func validateExactUnmountPaths(values []ParentUnmountEvidence, root string) error {
	for _, value := range values {
		want := path.Join(root, value.ParentFilesystemID)
		if value.MountPath != want {
			return fmt.Errorf("parent %q mount path %q differs from configured path %q", value.ParentFilesystemID, value.MountPath, want)
		}
	}
	return nil
}

func validateSortedUniqueNodeIDs(values []string) error {
	if len(values) == 0 || !slices.IsSorted(values) {
		return fmt.Errorf("node IDs must be non-empty and sorted")
	}
	for index, value := range values {
		if err := volume.ValidateNodeID(value); err != nil {
			return err
		}
		if index > 0 && values[index-1] == value {
			return fmt.Errorf("node ID %q is duplicated", value)
		}
	}
	return nil
}

func validateSortedUniqueProviderIDs(name string, values []string, allowEmpty bool) error {
	if !allowEmpty && len(values) == 0 {
		return fmt.Errorf("%s set is empty", name)
	}
	if !slices.IsSorted(values) {
		return fmt.Errorf("%s IDs are not sorted", name)
	}
	for index, value := range values {
		if err := volume.ValidateParentFilesystemID(value); err != nil {
			return fmt.Errorf("%s ID: %w", name, err)
		}
		if index > 0 && values[index-1] == value {
			return fmt.Errorf("%s ID %q is duplicated", name, value)
		}
	}
	return nil
}

func firstError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}
