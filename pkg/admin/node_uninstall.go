package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"sync"

	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// NodeUninstallCommandOperation is the node-local half of safe uninstall. It
// never detaches provider resources or removes directories. It unmounts only
// exact configured parent mounts after one coherent snapshot proves that no
// staging, workload, foreign, or stacked mount remains in the driver's roots.
type NodeUninstallCommandOperation struct {
	mu         sync.Mutex
	nodeID     string
	parentRoot string
	parentIDs  []string
	mounter    mount.Interface
	gate       *coordination.MutationGate
	beginStop  func(string) error
	stopID     string
}

// NodeUninstallInspection is a read-only coherent node mount inventory. Parent
// mounts are expected warm state and are validated but not returned as
// blockers. Any staging or publish target blocks both dry-run readiness and
// execution until Kubernetes has completed normal unpublish and unstage.
type NodeUninstallInspection struct {
	NodeID              string   `json:"nodeID"`
	StagingMountPaths   []string `json:"stagingMountPaths"`
	WorkloadTargetPaths []string `json:"workloadTargetPaths"`
}

// DecommissionParentPayload is the only caller-selected local decommission
// value. The operation additionally requires it to be in the frozen configured
// parent set before inspecting or unmounting anything.
type DecommissionParentPayload struct {
	ParentFilesystemID string `json:"parentFilesystemID"`
}

// Validate checks the provider parent identity syntax.
func (payload DecommissionParentPayload) Validate() error {
	return volume.ValidateParentFilesystemID(payload.ParentFilesystemID)
}

// NodeDecommissionInspection reports only mounts backed by the selected
// parent. Mounts for other configured parents remain validated but do not block
// this target-specific offline procedure.
type NodeDecommissionInspection struct {
	NodeID              string   `json:"nodeID"`
	ParentFilesystemID  string   `json:"parentFilesystemID"`
	ParentMountPath     string   `json:"parentMountPath"`
	ParentMounted       bool     `json:"parentMounted"`
	StagingMountPaths   []string `json:"stagingMountPaths"`
	WorkloadTargetPaths []string `json:"workloadTargetPaths"`
}

// NodeDecommissionUnmountResult proves exact target-parent absence after the
// node-local unmount phase.
type NodeDecommissionUnmountResult struct {
	NodeID                       string                `json:"nodeID"`
	Unmounted                    ParentUnmountEvidence `json:"unmounted"`
	RemainingStagingMountPaths   []string              `json:"remainingStagingMountPaths"`
	RemainingWorkloadTargetPaths []string              `json:"remainingWorkloadTargetPaths"`
}

// NewNodeUninstallCommandOperation validates and freezes the node-local mount
// authority. Parent IDs are sorted so retries emit stable audit evidence.
func NewNodeUninstallCommandOperation(nodeID, parentRoot string, parentIDs []string, mounter mount.Interface, gate *coordination.MutationGate, beginStop func(string) error) (*NodeUninstallCommandOperation, error) {
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if err := mount.ValidateAbsoluteNormalizedPath(parentRoot); err != nil {
		return nil, fmt.Errorf("node parent root: %w", err)
	}
	if mounter == nil || gate == nil || beginStop == nil {
		return nil, fmt.Errorf("node uninstall dependency is nil")
	}
	parents := slices.Clone(parentIDs)
	for index, parentID := range parents {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return nil, fmt.Errorf("node uninstall parent %d: %w", index, err)
		}
	}
	slices.Sort(parents)
	if len(parents) == 0 || len(slices.Compact(parents)) != len(parents) {
		return nil, fmt.Errorf("node uninstall parent set must be non-empty and unique")
	}
	return &NodeUninstallCommandOperation{
		nodeID: nodeID, parentRoot: parentRoot, parentIDs: parents, mounter: mounter,
		gate: gate, beginStop: beginStop,
	}, nil
}

// Commands returns the private inspect and exact-unmount routes used only by
// the operator-side multi-Pod orchestrator.
func (*NodeUninstallCommandOperation) Commands() []Command {
	return []Command{CommandUninstallInspect, CommandUninstallPrepare, CommandDecommissionInspect, CommandDecommissionPrepare}
}

// HandleCommand proves and removes exact warm parent mounts. A retry after a
// partial prior result treats conclusively absent configured targets as already
// unmounted, while every remaining mount must still pass full validation.
func (operation *NodeUninstallCommandOperation) HandleCommand(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	if command != CommandUninstallInspect && command != CommandUninstallPrepare && command != CommandDecommissionInspect && command != CommandDecommissionPrepare {
		return nil, fmt.Errorf("node uninstall operation received unowned route %q", command)
	}
	if err := request.Validate(); err != nil {
		return nil, NewOperationError(ErrorInvalidArgument, err)
	}
	operation.mu.Lock()
	defer operation.mu.Unlock()
	if command == CommandDecommissionInspect || command == CommandDecommissionPrepare {
		var selected DecommissionParentPayload
		if err := strictjson.Decode(payload, &selected); err != nil {
			return nil, NewOperationError(ErrorInvalidArgument, err)
		}
		if err := selected.Validate(); err != nil {
			return nil, NewOperationError(ErrorInvalidArgument, err)
		}
		if !slices.Contains(operation.parentIDs, selected.ParentFilesystemID) {
			return nil, NewOperationError(ErrorFailedPrecondition, fmt.Errorf("decommission parent %q is not configured", selected.ParentFilesystemID))
		}
		if command == CommandDecommissionInspect {
			inspection, err := operation.inspectDecommission(ctx, selected.ParentFilesystemID)
			if err != nil {
				return nil, commandWorkflowError(err)
			}
			return encodeCommandResult(inspection)
		}
		if err := operation.beginTerminalStop(ctx, request.RequestID); err != nil {
			return nil, commandWorkflowError(err)
		}
		result, err := operation.unmountDecommission(ctx, selected.ParentFilesystemID)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(result)
	}
	if len(payload) != 0 {
		return nil, NewOperationError(ErrorInvalidArgument, fmt.Errorf("node uninstall command does not accept a payload"))
	}
	if command == CommandUninstallInspect {
		inspection, err := operation.inspect(ctx)
		if err != nil {
			return nil, commandWorkflowError(err)
		}
		return encodeCommandResult(inspection)
	}
	if err := operation.beginTerminalStop(ctx, request.RequestID); err != nil {
		return nil, commandWorkflowError(err)
	}

	if err := operation.unmount(ctx); err != nil {
		return nil, commandWorkflowError(err)
	}
	evidence := NodeUnmountEvidence{NodeID: operation.nodeID}
	for _, parentID := range operation.parentIDs {
		evidence.UnmountedParents = append(evidence.UnmountedParents, ParentUnmountEvidence{
			ParentFilesystemID: parentID,
			MountPath:          path.Join(operation.parentRoot, parentID),
		})
	}
	return encodeCommandResult(evidence)
}

// beginTerminalStop removes readiness, closes Node mutation admission, and
// waits for every already-admitted Stage/Publish/Unpublish/Unstage operation
// before the first mount snapshot or unmount. The barrier is terminal for this
// process; normal operation resumes only through a fresh node-plugin Pod.
func (operation *NodeUninstallCommandOperation) beginTerminalStop(ctx context.Context, requestID string) error {
	if operation.stopID != "" && operation.stopID != requestID {
		return coordination.ErrQuiesceConflict
	}
	if operation.stopID == "" {
		if err := operation.beginStop(requestID); err != nil {
			return err
		}
		operation.stopID = requestID
	}
	if err := operation.gate.BeginQuiesce(ctx, requestID); err != nil {
		return err
	}
	return nil
}

func (operation *NodeUninstallCommandOperation) inspectDecommission(ctx context.Context, parentID string) (NodeDecommissionInspection, error) {
	table, err := operation.mounter.Snapshot(ctx)
	if err != nil {
		return NodeDecommissionInspection{}, fmt.Errorf("read node mount table for parent decommission: %w", err)
	}
	inspection := NodeDecommissionInspection{
		NodeID: operation.nodeID, ParentFilesystemID: parentID,
		ParentMountPath: path.Join(operation.parentRoot, parentID),
	}
	if err := operation.validateDecommissionMountTable(table, parentID, &inspection); err != nil {
		return NodeDecommissionInspection{}, err
	}
	slices.Sort(inspection.StagingMountPaths)
	slices.Sort(inspection.WorkloadTargetPaths)
	return inspection, nil
}

func (operation *NodeUninstallCommandOperation) unmountDecommission(ctx context.Context, parentID string) (NodeDecommissionUnmountResult, error) {
	// Inspect remains strictly read-only. The mutating retry path first resumes
	// any exact unmount that already moved into the private quarantine, then
	// rebuilds the complete mount inventory before making a new decision.
	if err := operation.mounter.ReconcileQuarantines(ctx); err != nil {
		return NodeDecommissionUnmountResult{}, fmt.Errorf("reconcile interrupted node unmount before parent decommission: %w", err)
	}
	inspection, err := operation.inspectDecommission(ctx, parentID)
	if err != nil {
		return NodeDecommissionUnmountResult{}, err
	}
	if len(inspection.StagingMountPaths) != 0 || len(inspection.WorkloadTargetPaths) != 0 {
		return NodeDecommissionUnmountResult{}, fmt.Errorf("parent %q still has staging or workload child mounts", parentID)
	}
	if inspection.ParentMounted {
		table, err := operation.mounter.Snapshot(ctx)
		if err != nil {
			return NodeDecommissionUnmountResult{}, err
		}
		entry, err := mount.ValidateParent(table, inspection.ParentMountPath, parentID)
		if err != nil {
			return NodeDecommissionUnmountResult{}, err
		}
		if _, err := operation.mounter.UnmountExact(ctx, inspection.ParentMountPath, entry.MountID); err != nil {
			return NodeDecommissionUnmountResult{}, fmt.Errorf("unmount decommission parent %q: %w", parentID, err)
		}
	}
	verified, err := operation.inspectDecommission(ctx, parentID)
	if err != nil {
		return NodeDecommissionUnmountResult{}, err
	}
	if verified.ParentMounted || len(verified.StagingMountPaths) != 0 || len(verified.WorkloadTargetPaths) != 0 {
		return NodeDecommissionUnmountResult{}, fmt.Errorf("parent %q mount graph remains after decommission unmount", parentID)
	}
	return NodeDecommissionUnmountResult{
		NodeID:                     operation.nodeID,
		Unmounted:                  ParentUnmountEvidence{ParentFilesystemID: parentID, MountPath: inspection.ParentMountPath},
		RemainingStagingMountPaths: []string{}, RemainingWorkloadTargetPaths: []string{},
	}, nil
}

func (operation *NodeUninstallCommandOperation) validateDecommissionMountTable(table mount.Table, selectedParentID string, inspection *NodeDecommissionInspection) error {
	configured := make(map[string]struct{}, len(operation.parentIDs))
	for _, parentID := range operation.parentIDs {
		configured[parentID] = struct{}{}
	}
	seenTargets := make(map[string]struct{}, len(table.Entries))
	for _, entry := range table.Entries {
		if _, duplicate := seenTargets[entry.Target]; duplicate {
			return fmt.Errorf("parent decommission mount target %q is stacked", entry.Target)
		}
		seenTargets[entry.Target] = struct{}{}
		if _, present := configured[entry.ParentFilesystemID]; !present {
			return fmt.Errorf("parent decommission found foreign mount %q", entry.Target)
		}
		switch entry.Kind {
		case mount.KindParent:
			expected := path.Join(operation.parentRoot, entry.ParentFilesystemID)
			if entry.Target != expected {
				return fmt.Errorf("parent decommission found aliased parent mount %q", entry.Target)
			}
			if _, err := mount.ValidateParent(table, expected, entry.ParentFilesystemID); err != nil {
				return err
			}
			if entry.ParentFilesystemID == selectedParentID {
				inspection.ParentMounted = true
			}
		case mount.KindStage:
			if entry.ParentFilesystemID == selectedParentID {
				inspection.StagingMountPaths = append(inspection.StagingMountPaths, entry.Target)
			}
		case mount.KindPublish:
			if entry.ParentFilesystemID == selectedParentID {
				inspection.WorkloadTargetPaths = append(inspection.WorkloadTargetPaths, entry.Target)
			}
		default:
			return fmt.Errorf("parent decommission found unknown mount kind %q at %q", entry.Kind, entry.Target)
		}
	}
	return nil
}

func (operation *NodeUninstallCommandOperation) inspect(ctx context.Context) (NodeUninstallInspection, error) {
	table, err := operation.mounter.Snapshot(ctx)
	if err != nil {
		return NodeUninstallInspection{}, fmt.Errorf("read node mount table for safe-uninstall inspection: %w", err)
	}
	configured := make(map[string]struct{}, len(operation.parentIDs))
	for _, parentID := range operation.parentIDs {
		configured[parentID] = struct{}{}
		target := path.Join(operation.parentRoot, parentID)
		if _, err := table.Exact(target); errors.Is(err, mount.ErrNotMounted) {
			continue
		} else if err != nil {
			return NodeUninstallInspection{}, fmt.Errorf("inspect node parent %q for safe uninstall: %w", parentID, err)
		}
		if _, err := mount.ValidateParent(table, target, parentID); err != nil {
			return NodeUninstallInspection{}, fmt.Errorf("validate node parent %q for safe uninstall: %w", parentID, err)
		}
	}
	inspection := NodeUninstallInspection{NodeID: operation.nodeID}
	seenTargets := make(map[string]struct{}, len(table.Entries))
	for _, entry := range table.Entries {
		if _, duplicate := seenTargets[entry.Target]; duplicate {
			return NodeUninstallInspection{}, fmt.Errorf("safe-uninstall mount target %q is stacked", entry.Target)
		}
		seenTargets[entry.Target] = struct{}{}
		switch entry.Kind {
		case mount.KindParent:
			if _, exists := configured[entry.ParentFilesystemID]; !exists || entry.Target != path.Join(operation.parentRoot, entry.ParentFilesystemID) {
				return NodeUninstallInspection{}, fmt.Errorf("safe uninstall found foreign parent mount %q", entry.Target)
			}
		case mount.KindStage:
			inspection.StagingMountPaths = append(inspection.StagingMountPaths, entry.Target)
		case mount.KindPublish:
			inspection.WorkloadTargetPaths = append(inspection.WorkloadTargetPaths, entry.Target)
		default:
			return NodeUninstallInspection{}, fmt.Errorf("safe uninstall found unknown mount kind %q at %q", entry.Kind, entry.Target)
		}
	}
	slices.Sort(inspection.StagingMountPaths)
	slices.Sort(inspection.WorkloadTargetPaths)
	return inspection, nil
}

func (operation *NodeUninstallCommandOperation) unmount(ctx context.Context) error {
	if err := operation.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted node unmount before safe uninstall: %w", err)
	}
	table, err := operation.mounter.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read node mount table for safe uninstall: %w", err)
	}
	if err := operation.rejectChildrenAndForeignParents(table); err != nil {
		return err
	}
	for _, parentID := range operation.parentIDs {
		target := path.Join(operation.parentRoot, parentID)
		entry, err := table.Exact(target)
		if errors.Is(err, mount.ErrNotMounted) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect node parent %q for safe uninstall: %w", parentID, err)
		}
		if _, err := mount.ValidateParent(table, target, parentID); err != nil {
			return fmt.Errorf("validate node parent %q for safe uninstall: %w", parentID, err)
		}
		if _, err := operation.mounter.UnmountExact(ctx, target, entry.MountID); err != nil {
			return fmt.Errorf("unmount node parent %q for safe uninstall: %w", parentID, err)
		}
		table, err = operation.mounter.Snapshot(ctx)
		if err != nil {
			return fmt.Errorf("verify node parent %q unmount: %w", parentID, err)
		}
		if _, err := table.Exact(target); !errors.Is(err, mount.ErrNotMounted) {
			if err == nil {
				return fmt.Errorf("node parent %q remains mounted after exact unmount", parentID)
			}
			return fmt.Errorf("verify node parent %q absence: %w", parentID, err)
		}
		if err := operation.rejectChildrenAndForeignParents(table); err != nil {
			return err
		}
	}
	return operation.rejectChildrenAndForeignParents(table)
}

func (operation *NodeUninstallCommandOperation) rejectChildrenAndForeignParents(table mount.Table) error {
	configured := make(map[string]struct{}, len(operation.parentIDs))
	for _, parentID := range operation.parentIDs {
		configured[parentID] = struct{}{}
	}
	for _, entry := range table.Entries {
		switch entry.Kind {
		case mount.KindStage, mount.KindPublish, mount.KindForeign, mount.KindQuarantine:
			return fmt.Errorf("safe uninstall is blocked by child mount %q", entry.Target)
		case mount.KindParent:
			if _, exists := configured[entry.ParentFilesystemID]; !exists || entry.Target != path.Join(operation.parentRoot, entry.ParentFilesystemID) {
				return fmt.Errorf("safe uninstall is blocked by foreign parent mount %q", entry.Target)
			}
		default:
			return fmt.Errorf("safe uninstall is blocked by unknown mount kind %q at %q", entry.Kind, entry.Target)
		}
	}
	return nil
}

var _ CommandOperation = (*NodeUninstallCommandOperation)(nil)
