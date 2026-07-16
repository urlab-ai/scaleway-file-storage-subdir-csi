package pool

import (
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	decommissionParent    = "11111111-1111-4111-8111-111111111111"
	decommissionTimestamp = "2026-07-13T17:00:00Z"
)

func decommissionCompactPair(t *testing.T) (*volume.CompactDeletedAllocationRecord, *volume.CompactDeletedOwnershipRecord) {
	t.Helper()
	logicalID, err := volume.LogicalVolumeID("file-storage-subdir.csi.urlab.ai", "pvc-decommissioned")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	ownership, err := (volume.CompactDeletedOwnershipRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.OwnershipRecordCompactDeleted,
		Revision: 4, DriverName: "file-storage-subdir.csi.urlab.ai",
		InstallationID:   "22222222-2222-4222-8222-222222222222",
		ActiveClusterUID: "33333333-3333-4333-8333-333333333333",
		VolumeHandleHash: "vh-" + strings.Repeat("a", 32), LogicalVolumeID: logicalID,
		CreateVolumeRequestName: "pvc-decommissioned", MappingHash: "mh-" + strings.Repeat("b", 32),
		ParentFilesystemID: decommissionParent, BasePathHash: "bp-" + strings.Repeat("c", 32),
		DirectoryName: "tenant--decommissioned--0123456789ab", State: volume.StateDeleted,
		DeleteResult: "deleted", UpdatedAt: decommissionTimestamp, DeletedAt: decommissionTimestamp,
		DeleteOperation:   volume.DeleteOperationDelete,
		DeleteOperationID: "44444444-4444-4444-8444-444444444444",
		DeleteCompletedAt: decommissionTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	allocation, err := volume.ReconstructCompactAllocationFromOwnership(&ownership)
	if err != nil {
		t.Fatalf("ReconstructCompactAllocationFromOwnership() error = %v", err)
	}
	return allocation, &ownership
}

func TestValidateDecommissionRecordsAcceptsOnlyPairedHistoricalTombstones(t *testing.T) {
	allocation, ownership := decommissionCompactPair(t)
	snapshot := DecommissionRecordSnapshot{
		ParentFilesystemID: decommissionParent, ParentState: ParentDraining,
		Allocations: []volume.AllocationRecord{allocation}, Ownerships: []volume.OwnershipRecord{ownership},
	}
	if err := ValidateDecommissionRecords(snapshot); err != nil {
		t.Fatalf("ValidateDecommissionRecords() error = %v", err)
	}

	snapshot.ParentState = ParentActive
	if err := ValidateDecommissionRecords(snapshot); err == nil {
		t.Fatal("ValidateDecommissionRecords(active parent) error = nil")
	}
	snapshot.ParentState = ParentDraining
	snapshot.References.PersistentVolumes = []string{"pv-live"}
	if err := ValidateDecommissionRecords(snapshot); err == nil || !strings.Contains(err.Error(), "PersistentVolume") {
		t.Fatalf("ValidateDecommissionRecords(live PV) error = %v", err)
	}
}

func TestDecommissionRecordBlockersReturnsEveryReferenceDeterministically(t *testing.T) {
	allocation, ownership := decommissionCompactPair(t)
	snapshot := DecommissionRecordSnapshot{
		ParentFilesystemID: decommissionParent, ParentState: ParentDraining,
		Allocations: []volume.AllocationRecord{allocation}, Ownerships: []volume.OwnershipRecord{ownership},
		References: DecommissionReferences{
			PersistentVolumes: []string{"pv-z", "pv-a"}, VolumeAttachments: []string{"va-a"},
			StagingMountPaths: []string{"/staging/a"}, WorkloadTargetPaths: []string{"/target/a"},
			ChildBindMountPaths: []string{"/child/a"},
		},
	}
	blockers, err := DecommissionRecordBlockers(snapshot)
	if err != nil {
		t.Fatalf("DecommissionRecordBlockers() error = %v", err)
	}
	if len(blockers) != 6 || blockers[0] != `PersistentVolume "pv-a"` {
		t.Fatalf("decommission blockers = %#v", blockers)
	}
	if err := ValidateDecommissionRecords(snapshot); err == nil || !strings.Contains(err.Error(), "6 decommission blocker") {
		t.Fatalf("ValidateDecommissionRecords(blockers) error = %v", err)
	}

	snapshot.References.PersistentVolumes = []string{""}
	if _, err := DecommissionRecordBlockers(snapshot); err == nil {
		t.Fatal("DecommissionRecordBlockers(empty reference) error = nil")
	}
}

func TestDecommissionRecordBlockersValidatesDuplicateAllocationAcrossCompleteInventory(t *testing.T) {
	allocation, ownership := decommissionCompactPair(t)
	other := *allocation
	other.ParentFilesystemID = "99999999-9999-4999-8999-999999999999"
	snapshot := DecommissionRecordSnapshot{
		ParentFilesystemID: decommissionParent, ParentState: ParentDraining,
		Allocations: []volume.AllocationRecord{allocation, &other}, Ownerships: []volume.OwnershipRecord{ownership},
	}
	if _, err := DecommissionRecordBlockers(snapshot); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("DecommissionRecordBlockers(duplicate installation allocation) error = %v", err)
	}
}

func TestValidateDecommissionRecordsRejectsMissingOrConflictingTombstonePair(t *testing.T) {
	allocation, ownership := decommissionCompactPair(t)
	base := DecommissionRecordSnapshot{
		ParentFilesystemID: decommissionParent, ParentState: ParentDraining,
		Allocations: []volume.AllocationRecord{allocation}, Ownerships: []volume.OwnershipRecord{ownership},
	}
	missingOwnership := base
	missingOwnership.Ownerships = nil
	if err := ValidateDecommissionRecords(missingOwnership); err == nil || !strings.Contains(err.Error(), "no compact ownership pair") {
		t.Fatalf("ValidateDecommissionRecords(missing ownership) error = %v", err)
	}
	missingAllocation := base
	missingAllocation.Allocations = nil
	if err := ValidateDecommissionRecords(missingAllocation); err == nil || !strings.Contains(err.Error(), "no allocation tombstone") {
		t.Fatalf("ValidateDecommissionRecords(missing allocation) error = %v", err)
	}
	conflicting := *ownership
	conflicting.DeleteResult = "archived"
	sealed, err := conflicting.Seal()
	if err != nil {
		t.Fatalf("Seal(conflicting ownership) error = %v", err)
	}
	base.Ownerships = []volume.OwnershipRecord{&sealed}
	if err := ValidateDecommissionRecords(base); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("ValidateDecommissionRecords(conflicting pair) error = %v", err)
	}
}

func TestValidateDecommissionOfflineEvidenceRequiresConclusiveAbsence(t *testing.T) {
	evidence := DecommissionOfflineEvidence{DriverProcessesStopped: true, InventoryFresh: true}
	if err := ValidateDecommissionOfflineEvidence(evidence); err != nil {
		t.Fatalf("ValidateDecommissionOfflineEvidence() error = %v", err)
	}
	for name, mutate := range map[string]func(*DecommissionOfflineEvidence){
		"process running":     func(value *DecommissionOfflineEvidence) { value.DriverProcessesStopped = false },
		"stale inventory":     func(value *DecommissionOfflineEvidence) { value.InventoryFresh = false },
		"controller mount":    func(value *DecommissionOfflineEvidence) { value.ControllerMountPaths = []string{"/controller/parent"} },
		"node mount":          func(value *DecommissionOfflineEvidence) { value.NodeMountPaths = []string{"/node/parent"} },
		"child bind":          func(value *DecommissionOfflineEvidence) { value.ChildBindMountPaths = []string{"/node/parent/child"} },
		"regional attachment": func(value *DecommissionOfflineEvidence) { value.RegionalAttachmentIDs = []string{"attachment-a"} },
		"Instance attachment": func(value *DecommissionOfflineEvidence) { value.InstanceAttachmentIDs = []string{"instance-a/parent"} },
	} {
		t.Run(name, func(t *testing.T) {
			changed := evidence
			mutate(&changed)
			if err := ValidateDecommissionOfflineEvidence(changed); err == nil {
				t.Fatal("ValidateDecommissionOfflineEvidence(blocked) error = nil")
			}
		})
	}
}
