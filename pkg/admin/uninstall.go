package admin

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// UninstallPreflightSnapshot is the complete blocker inventory captured before
// safe uninstall may quiesce the controller. The operator removes workloads
// through normal Kubernetes flows; this package never deletes them.
type UninstallPreflightSnapshot struct {
	Request                    MutationRequest
	Allocations                []volume.AllocationRecord
	PersistentVolumeNames      []string
	PersistentVolumeClaimNames []string
	VolumeAttachmentNames      []string
	WorkloadPodNames           []string
	StagingMounts              []NodeMountReference
	WorkloadTargets            []NodeMountReference
}

// NodeMountReference binds one live staging or workload mount to the exact CSI
// node that reported it. The same host path on two nodes remains two blockers.
type NodeMountReference struct {
	NodeID string `json:"nodeID"`
	Path   string `json:"path"`
}

// ValidateUninstallPreflight permits only permanent non-reserving Deleted
// allocation variants and no live Kubernetes or mount reference.
func ValidateUninstallPreflight(snapshot UninstallPreflightSnapshot) error {
	blockers, err := UninstallPreflightBlockers(snapshot)
	if err != nil {
		return err
	}
	if len(blockers) != 0 {
		return fmt.Errorf("safe uninstall has %d blocker(s); first is %s", len(blockers), blockers[0])
	}
	return nil
}

// UninstallPreflightBlockers validates the complete inventory and returns every
// live, stable-identity blocker in deterministic order. Corrupt durable state
// is an error rather than an operator-removable blocker.
func UninstallPreflightBlockers(snapshot UninstallPreflightSnapshot) ([]string, error) {
	if err := snapshot.Request.Validate(); err != nil {
		return nil, err
	}
	blockers := make([]string, 0)
	for kind, values := range map[string][]string{
		"PersistentVolume":      snapshot.PersistentVolumeNames,
		"PersistentVolumeClaim": snapshot.PersistentVolumeClaimNames,
		"VolumeAttachment":      snapshot.VolumeAttachmentNames,
		"workload Pod":          snapshot.WorkloadPodNames,
	} {
		for _, value := range values {
			if err := validateBlockerIdentity(kind, value); err != nil {
				return nil, err
			}
			blockers = append(blockers, fmt.Sprintf("%s %q", kind, value))
		}
	}
	for kind, values := range map[string][]NodeMountReference{
		"staging mount": snapshot.StagingMounts, "workload target": snapshot.WorkloadTargets,
	} {
		for _, value := range values {
			if err := volume.ValidateNodeID(value.NodeID); err != nil {
				return nil, fmt.Errorf("%s node: %w", kind, err)
			}
			if value.Path == "" || len(value.Path) > 512 || !utf8.ValidString(value.Path) || !strings.HasPrefix(value.Path, "/") || path.Clean(value.Path) != value.Path {
				return nil, fmt.Errorf("%s path is not bounded, absolute, and normalized", kind)
			}
			blockers = append(blockers, fmt.Sprintf("%s %q on node %q", kind, value.Path, value.NodeID))
		}
	}
	seen := make(map[string]struct{}, len(snapshot.Allocations))
	for index, allocation := range snapshot.Allocations {
		if allocation == nil {
			return nil, fmt.Errorf("uninstall allocation %d is nil", index)
		}
		if err := allocation.Validate(); err != nil {
			return nil, fmt.Errorf("uninstall allocation %d: %w", index, err)
		}
		if _, duplicate := seen[allocation.LogicalID()]; duplicate {
			return nil, fmt.Errorf("uninstall allocation %q is duplicated", allocation.LogicalID())
		}
		seen[allocation.LogicalID()] = struct{}{}
		switch record := allocation.(type) {
		case *volume.DetailedAllocationRecord:
			if _, err := volume.CompactDeletedProjection(record); err != nil {
				blockers = append(blockers, fmt.Sprintf("allocation %q in state %q", record.LogicalVolumeID, record.State))
				for _, nodeID := range record.PublishedNodeIDs {
					blockers = append(blockers, fmt.Sprintf("published-node fence for allocation %q on node %q", record.LogicalVolumeID, nodeID))
				}
			}
		case *volume.CompactDeletedAllocationRecord:
			if record.State != volume.StateDeleted || record.ReservesCapacity {
				return nil, fmt.Errorf("allocation %q compact tombstone is not terminal and non-reserving", record.LogicalVolumeID)
			}
		case *volume.DeletedUnknownAllocationRecord:
			// Its closed validator already proves terminal non-reserving state.
		default:
			return nil, fmt.Errorf("allocation %q has unsupported kind %q", allocation.LogicalID(), allocation.Kind())
		}
	}
	slices.Sort(blockers)
	return blockers, nil
}

func validateBlockerIdentity(kind, value string) error {
	if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return errors.New(kind + " blocker identity is not bounded single-line UTF-8")
	}
	return nil
}

// ParentUnmountEvidence binds one configured parent to the exact mount path
// that the node-admin or controller cleanup proved absent after exact unmount.
type ParentUnmountEvidence struct {
	ParentFilesystemID string `json:"parentFilesystemID"`
	MountPath          string `json:"mountPath"`
}

// NodeUnmountEvidence is the exact node-admin result retained after the node
// DaemonSet is scaled to zero.
type NodeUnmountEvidence struct {
	NodeID                    string                  `json:"nodeID"`
	UnmountedParents          []ParentUnmountEvidence `json:"unmountedParents"`
	RemainingParentMountPaths []string                `json:"remainingParentMountPaths"`
	RemainingChildMountPaths  []string                `json:"remainingChildMountPaths"`
}

// UninstallCompletionEvidence is the final audit boundary checked before the
// CLI permits Helm deletion. It is read-only and never removes retained data,
// claims, Secrets, or allocation tombstones.
type UninstallCompletionEvidence struct {
	RequestID                   string
	ExpectedNodeIDs             []string
	ExpectedParentFilesystemIDs []string
	Nodes                       []NodeUnmountEvidence
	NodePluginPodNames          []string
	ControllerPodNames          []string
	ControllerMountPaths        []string
	ProviderInventoriesFresh    bool
	RegionalAttachmentIDs       []string
	InstanceAttachmentIDs       []string
	ReleasedLease               coordination.LeaseSnapshot
}

// ValidateUninstallCompletion proves exact node/parent cleanup, conclusive
// provider absence, and graceful release of the immutable leadership Lease.
func ValidateUninstallCompletion(evidence UninstallCompletionEvidence) error {
	if err := volume.ValidateOperationID(evidence.RequestID); err != nil {
		return err
	}
	nodes, err := normalizedNodeIDs(evidence.ExpectedNodeIDs)
	if err != nil {
		return err
	}
	parents, err := normalizedParentIDs(evidence.ExpectedParentFilesystemIDs)
	if err != nil {
		return err
	}
	if len(nodes) == 0 || len(parents) == 0 {
		return fmt.Errorf("safe uninstall requires non-empty expected node and parent sets")
	}
	if blocker, kind := firstCompletionBlocker(evidence); blocker != "" {
		return fmt.Errorf("safe uninstall completion still has %s %q", kind, blocker)
	}
	if !evidence.ProviderInventoriesFresh {
		return fmt.Errorf("safe uninstall provider inventories are not fresh and complete")
	}

	reported := make(map[string]struct{}, len(evidence.Nodes))
	for index, node := range evidence.Nodes {
		if err := volume.ValidateNodeID(node.NodeID); err != nil {
			return fmt.Errorf("node unmount evidence %d: %w", index, err)
		}
		if _, duplicate := reported[node.NodeID]; duplicate {
			return fmt.Errorf("node unmount evidence %q is duplicated", node.NodeID)
		}
		if !slices.Contains(nodes, node.NodeID) {
			return fmt.Errorf("node unmount evidence %q is outside expected set", node.NodeID)
		}
		unmounted, err := validateParentUnmountEvidence(node.UnmountedParents)
		if err != nil {
			return fmt.Errorf("node %q unmounted parents: %w", node.NodeID, err)
		}
		if !slices.Equal(unmounted, parents) {
			return fmt.Errorf("node %q did not prove every exact configured parent unmounted", node.NodeID)
		}
		if blocker, present := firstBoundedIdentity(node.RemainingParentMountPaths); present {
			return fmt.Errorf("node %q retains parent mount %q", node.NodeID, blocker)
		}
		if blocker, present := firstBoundedIdentity(node.RemainingChildMountPaths); present {
			return fmt.Errorf("node %q retains child mount %q", node.NodeID, blocker)
		}
		reported[node.NodeID] = struct{}{}
	}
	if len(reported) != len(nodes) {
		return fmt.Errorf("safe uninstall has %d node unmount results, want %d", len(reported), len(nodes))
	}

	lease := evidence.ReleasedLease
	if err := volume.ValidateOperationID(lease.UID); err != nil {
		return fmt.Errorf("released Lease UID: %w", err)
	}
	if lease.ResourceVersion == "" || lease.Annotations == nil {
		return fmt.Errorf("released Lease lacks resourceVersion or explicit annotations")
	}
	if lease.HolderIdentity != "" {
		return fmt.Errorf("leadership Lease still has holder %q", lease.HolderIdentity)
	}
	holder, present, err := coordination.ParseHolderEvidence(lease.Annotations)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("released Lease lacks preserved holder evidence")
	}
	release, present, err := coordination.ParseGracefulRelease(lease.Annotations)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("released Lease lacks graceful-release marker")
	}
	if release.RequestID != evidence.RequestID {
		return fmt.Errorf("graceful-release request %q differs from uninstall request %q", release.RequestID, evidence.RequestID)
	}
	if err := release.ValidateHandoff(lease.UID, holder.InstallationID, holder.ActiveClusterUID, holder); err != nil {
		return err
	}
	return nil
}

func validateParentUnmountEvidence(values []ParentUnmountEvidence) ([]string, error) {
	parents := make([]string, 0, len(values))
	paths := make(map[string]struct{}, len(values))
	for index, value := range values {
		if err := volume.ValidateParentFilesystemID(value.ParentFilesystemID); err != nil {
			return nil, fmt.Errorf("entry %d parent: %w", index, err)
		}
		if value.MountPath == "" || len(value.MountPath) > 512 || !utf8.ValidString(value.MountPath) || !strings.HasPrefix(value.MountPath, "/") || value.MountPath == "/" || path.Clean(value.MountPath) != value.MountPath {
			return nil, fmt.Errorf("entry %d mount path %q is not absolute, normalized, and non-root", index, value.MountPath)
		}
		if path.Base(value.MountPath) != value.ParentFilesystemID {
			return nil, fmt.Errorf("entry %d mount path is not bound to parent %q", index, value.ParentFilesystemID)
		}
		if _, duplicate := paths[value.MountPath]; duplicate {
			return nil, fmt.Errorf("mount path %q is duplicated", value.MountPath)
		}
		paths[value.MountPath] = struct{}{}
		parents = append(parents, value.ParentFilesystemID)
	}
	slices.Sort(parents)
	if len(slices.Compact(parents)) != len(parents) {
		return nil, fmt.Errorf("parent unmount evidence contains duplicate parent IDs")
	}
	return parents, nil
}

func normalizedNodeIDs(values []string) ([]string, error) {
	result := slices.Clone(values)
	for index, value := range result {
		if err := volume.ValidateNodeID(value); err != nil {
			return nil, fmt.Errorf("expected node %d: %w", index, err)
		}
	}
	slices.Sort(result)
	if len(slices.Compact(result)) != len(result) {
		return nil, fmt.Errorf("expected node set contains duplicates")
	}
	return result, nil
}

func normalizedParentIDs(values []string) ([]string, error) {
	result := slices.Clone(values)
	for index, value := range result {
		if err := volume.ValidateParentFilesystemID(value); err != nil {
			return nil, fmt.Errorf("parent %d: %w", index, err)
		}
	}
	slices.Sort(result)
	if len(slices.Compact(result)) != len(result) {
		return nil, fmt.Errorf("parent set contains duplicates")
	}
	return result, nil
}

func firstCompletionBlocker(evidence UninstallCompletionEvidence) (string, string) {
	for _, candidate := range []struct {
		kind   string
		values []string
	}{
		{kind: "node-plugin Pod", values: evidence.NodePluginPodNames},
		{kind: "controller Pod", values: evidence.ControllerPodNames},
		{kind: "controller mount", values: evidence.ControllerMountPaths},
		{kind: "regional attachment", values: evidence.RegionalAttachmentIDs},
		{kind: "Instance attachment", values: evidence.InstanceAttachmentIDs},
	} {
		if blocker, present := firstBoundedIdentity(candidate.values); present {
			return blocker, candidate.kind
		}
	}
	return "", ""
}

func firstBoundedIdentity(values []string) (string, bool) {
	if len(values) == 0 {
		return "", false
	}
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	if ordered[0] == "" {
		return "<empty identity>", true
	}
	value := strings.ToValidUTF8(ordered[0], "?")
	if len(value) > 512 {
		value = strings.ToValidUTF8(value[:512], "?")
	}
	return value, true
}
