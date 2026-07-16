package pool

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// DecommissionReferences is the complete online Kubernetes and mount evidence
// already filtered for one parent. Values are exact resource or absolute mount
// identities used only for bounded diagnostics; any entry is a blocker.
type DecommissionReferences struct {
	PersistentVolumes   []string
	VolumeAttachments   []string
	StagingMountPaths   []string
	WorkloadTargetPaths []string
	ChildBindMountPaths []string
}

// DecommissionRecordSnapshot is the last online allocation/ownership view for
// one still-configured parent. The caller must list the complete installation
// allocation set and the complete ownership inventory for this parent.
type DecommissionRecordSnapshot struct {
	ParentFilesystemID string
	ParentState        ParentState
	Allocations        []volume.AllocationRecord
	Ownerships         []volume.OwnershipRecord
	References         DecommissionReferences
}

// ValidateDecommissionRecords proves that only exact paired historical Deleted
// tombstones remain. It performs no filesystem or provider mutation.
func ValidateDecommissionRecords(snapshot DecommissionRecordSnapshot) error {
	blockers, err := DecommissionRecordBlockers(snapshot)
	if err != nil {
		return err
	}
	if len(blockers) != 0 {
		return fmt.Errorf("parent %q has %d decommission blocker(s); first is %s", snapshot.ParentFilesystemID, len(blockers), blockers[0])
	}
	return nil
}

// DecommissionRecordBlockers validates the complete online snapshot and
// returns every operator-removable reference in deterministic order. Invalid,
// one-sided, or conflicting durable tombstone evidence remains an error rather
// than a blocker because removing workloads cannot repair it safely.
func DecommissionRecordBlockers(snapshot DecommissionRecordSnapshot) ([]string, error) {
	if err := volume.ValidateParentFilesystemID(snapshot.ParentFilesystemID); err != nil {
		return nil, err
	}
	if snapshot.ParentState != ParentDraining {
		return nil, fmt.Errorf("parent %q must be draining before offline decommission", snapshot.ParentFilesystemID)
	}
	blockers, err := decommissionReferenceBlockers(snapshot.References)
	if err != nil {
		return nil, err
	}

	allocations := make(map[string]volume.AllocationRecord)
	seenAllocations := make(map[string]struct{}, len(snapshot.Allocations))
	blockedAllocations := make(map[string]struct{})
	for index, allocation := range snapshot.Allocations {
		if allocation == nil {
			return nil, fmt.Errorf("decommission allocation %d is nil", index)
		}
		if err := allocation.Validate(); err != nil {
			return nil, fmt.Errorf("decommission allocation %d: %w", index, err)
		}
		if _, duplicate := seenAllocations[allocation.LogicalID()]; duplicate {
			return nil, fmt.Errorf("decommission allocation %q is duplicated", allocation.LogicalID())
		}
		seenAllocations[allocation.LogicalID()] = struct{}{}
		if allocationParentID(allocation) == snapshot.ParentFilesystemID {
			switch record := allocation.(type) {
			case *volume.DetailedAllocationRecord:
				if record.State != volume.StateDeleted || record.ReservesCapacity {
					blockers = append(blockers, fmt.Sprintf("allocation %q in state %q", record.LogicalVolumeID, record.State))
					blockedAllocations[record.LogicalVolumeID] = struct{}{}
				} else if len(record.PublishedNodeIDs) != 0 {
					for _, nodeID := range record.PublishedNodeIDs {
						blockers = append(blockers, fmt.Sprintf("published-node fence for allocation %q on node %q", record.LogicalVolumeID, nodeID))
					}
					blockedAllocations[record.LogicalVolumeID] = struct{}{}
				} else if _, err := volume.CompactDeletedProjection(record); err != nil {
					return nil, fmt.Errorf("parent allocation %q terminal projection: %w", record.LogicalVolumeID, err)
				}
			case *volume.CompactDeletedAllocationRecord:
				if record.State != volume.StateDeleted || record.ReservesCapacity {
					return nil, fmt.Errorf("parent allocation %q is not a non-reserving Deleted tombstone", record.LogicalVolumeID)
				}
			default:
				return nil, fmt.Errorf("parent allocation %q has unsupported kind %q", allocation.LogicalID(), allocation.Kind())
			}
			allocations[allocation.LogicalID()] = allocation
		}
	}

	paired := make(map[string]struct{}, len(snapshot.Ownerships))
	detailedOwnerships := make(map[string]struct{})
	seenOwnerships := make(map[string]struct{}, len(snapshot.Ownerships))
	for index, ownership := range snapshot.Ownerships {
		if ownership == nil {
			return nil, fmt.Errorf("decommission ownership %d is nil", index)
		}
		if err := ownership.Validate(); err != nil {
			return nil, fmt.Errorf("decommission ownership %d: %w", index, err)
		}
		if ownershipParentID(ownership) != snapshot.ParentFilesystemID {
			return nil, fmt.Errorf("decommission ownership %q belongs to another parent", ownership.LogicalID())
		}
		if _, duplicate := seenOwnerships[ownership.LogicalID()]; duplicate {
			return nil, fmt.Errorf("decommission ownership %q is duplicated", ownership.LogicalID())
		}
		seenOwnerships[ownership.LogicalID()] = struct{}{}
		compact, ok := ownership.(*volume.CompactDeletedOwnershipRecord)
		if !ok {
			detailedOwnerships[ownership.LogicalID()] = struct{}{}
			blockers = append(blockers, fmt.Sprintf("live detailed ownership %q", ownership.LogicalID()))
			continue
		}
		if _, duplicate := paired[compact.LogicalVolumeID]; duplicate {
			return nil, fmt.Errorf("decommission ownership %q is duplicated", compact.LogicalVolumeID)
		}
		allocation, present := allocations[compact.LogicalVolumeID]
		if !present {
			return nil, fmt.Errorf("compact ownership %q has no allocation tombstone", compact.LogicalVolumeID)
		}
		var projection *volume.CompactDeletedAllocationRecord
		switch record := allocation.(type) {
		case *volume.CompactDeletedAllocationRecord:
			projection = record
		case *volume.DetailedAllocationRecord:
			var err error
			projection, err = volume.CompactDeletedProjection(record)
			if err != nil {
				if _, blocked := blockedAllocations[record.LogicalVolumeID]; blocked {
					continue
				}
				return nil, err
			}
		default:
			return nil, fmt.Errorf("allocation %q cannot pair with compact ownership", allocation.LogicalID())
		}
		if err := volume.ValidateCompactPair(projection, compact); err != nil {
			return nil, fmt.Errorf("decommission tombstone pair %q: %w", compact.LogicalVolumeID, err)
		}
		paired[compact.LogicalVolumeID] = struct{}{}
	}
	for logicalID := range allocations {
		if _, present := paired[logicalID]; !present {
			if _, blocked := blockedAllocations[logicalID]; blocked {
				continue
			}
			if _, detailed := detailedOwnerships[logicalID]; detailed {
				continue
			}
			return nil, fmt.Errorf("allocation tombstone %q has no compact ownership pair", logicalID)
		}
	}
	slices.Sort(blockers)
	return blockers, nil
}

// DecommissionOfflineEvidence is the final post-stop proof collected before a
// parent may be removed from Helm values. InventoryFresh means both complete
// regional and per-Instance provider views were read successfully.
type DecommissionOfflineEvidence struct {
	DriverProcessesStopped bool
	InventoryFresh         bool
	ControllerMountPaths   []string
	NodeMountPaths         []string
	ChildBindMountPaths    []string
	RegionalAttachmentIDs  []string
	InstanceAttachmentIDs  []string
}

// ValidateDecommissionOfflineEvidence requires conclusive absence after exact
// unmount and detach. An unavailable inventory must never become absence.
func ValidateDecommissionOfflineEvidence(evidence DecommissionOfflineEvidence) error {
	if !evidence.DriverProcessesStopped {
		return fmt.Errorf("driver processes are not conclusively stopped")
	}
	if !evidence.InventoryFresh {
		return fmt.Errorf("provider attachment inventories are not fresh and complete")
	}
	for kind, values := range map[string][]string{
		"controller mount":    evidence.ControllerMountPaths,
		"node mount":          evidence.NodeMountPaths,
		"child bind mount":    evidence.ChildBindMountPaths,
		"regional attachment": evidence.RegionalAttachmentIDs,
		"Instance attachment": evidence.InstanceAttachmentIDs,
	} {
		if len(values) != 0 {
			ordered := slices.Clone(values)
			slices.Sort(ordered)
			return fmt.Errorf("offline decommission still has %s %q", kind, ordered[0])
		}
	}
	return nil
}

func decommissionReferenceBlockers(references DecommissionReferences) ([]string, error) {
	blockers := make([]string, 0)
	for _, candidate := range []struct {
		kind   string
		values []string
	}{
		{kind: "PersistentVolume", values: references.PersistentVolumes},
		{kind: "VolumeAttachment", values: references.VolumeAttachments},
		{kind: "staging mount", values: references.StagingMountPaths},
		{kind: "workload target", values: references.WorkloadTargetPaths},
		{kind: "child bind mount", values: references.ChildBindMountPaths},
	} {
		for _, value := range candidate.values {
			if value == "" || len(value) > 512 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
				return nil, fmt.Errorf("decommission %s identity is not bounded single-line UTF-8", candidate.kind)
			}
			blockers = append(blockers, fmt.Sprintf("%s %q", candidate.kind, value))
		}
	}
	slices.Sort(blockers)
	return blockers, nil
}

func allocationParentID(allocation volume.AllocationRecord) string {
	switch record := allocation.(type) {
	case *volume.DetailedAllocationRecord:
		return record.ParentFilesystemID
	case *volume.CompactDeletedAllocationRecord:
		return record.ParentFilesystemID
	case *volume.DeletedUnknownAllocationRecord:
		return ""
	default:
		return ""
	}
}

func ownershipParentID(ownership volume.OwnershipRecord) string {
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		return record.ParentFilesystemID
	case *volume.CompactDeletedOwnershipRecord:
		return record.ParentFilesystemID
	default:
		return ""
	}
}
