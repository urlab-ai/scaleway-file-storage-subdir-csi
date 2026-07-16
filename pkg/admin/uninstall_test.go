package admin

import (
	"slices"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const uninstallRequestID = "11111111-1111-4111-8111-111111111111"

func validMutationRequest() MutationRequest {
	return MutationRequest{
		RequestID: uninstallRequestID, AdminVersion: "1.0.0",
		Protocol: ProtocolVersion{Major: ProtocolMajorV1, Minor: ProtocolMinorV1},
	}
}

func validDeletedUnknown(t *testing.T) *volume.DeletedUnknownAllocationRecord {
	t.Helper()
	logicalID, err := volume.LogicalVolumeID("sfs-subdir.csi.example.com", "unknown-deleted")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	return &volume.DeletedUnknownAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDeletedUnknown,
		RecordRevision: 1, DriverName: "sfs-subdir.csi.example.com",
		InstallationID:   "22222222-2222-4222-8222-222222222222",
		ActiveClusterUID: "33333333-3333-4333-8333-333333333333",
		LogicalVolumeID:  logicalID, VolumeHandleHash: "vh-" + strings.Repeat("a", 32),
		MappingHash: "mh-" + strings.Repeat("b", 32), State: volume.StateDeleted,
		ReservesCapacity: false, AbsenceReason: "all authoritative sources conclusively absent",
		CreatedAt: "2026-07-13T17:00:00Z", UpdatedAt: "2026-07-13T17:00:00Z", DeletedAt: "2026-07-13T17:00:00Z",
	}
}

func TestValidateUninstallPreflightAllowsOnlyTerminalInventory(t *testing.T) {
	snapshot := UninstallPreflightSnapshot{
		Request:     validMutationRequest(),
		Allocations: []volume.AllocationRecord{validDeletedUnknown(t)},
	}
	if err := ValidateUninstallPreflight(snapshot); err != nil {
		t.Fatalf("ValidateUninstallPreflight() error = %v", err)
	}
	snapshot.PersistentVolumeNames = []string{"pv-live"}
	if err := ValidateUninstallPreflight(snapshot); err == nil || !strings.Contains(err.Error(), "PersistentVolume") {
		t.Fatalf("ValidateUninstallPreflight(live PV) error = %v", err)
	}
	snapshot.PersistentVolumeNames = nil
	invalid := *validDeletedUnknown(t)
	invalid.ReservesCapacity = true
	snapshot.Allocations = []volume.AllocationRecord{&invalid}
	if err := ValidateUninstallPreflight(snapshot); err == nil {
		t.Fatal("ValidateUninstallPreflight(non-terminal allocation) error = nil")
	}
}

func TestUninstallPreflightBlockersReportsEveryStableIdentity(t *testing.T) {
	snapshot := UninstallPreflightSnapshot{
		Request: validMutationRequest(), Allocations: []volume.AllocationRecord{validDeletedUnknown(t)},
		PersistentVolumeNames: []string{"pv-b", "pv-a"}, PersistentVolumeClaimNames: []string{"ns/claim"},
		VolumeAttachmentNames: []string{"attachment"}, WorkloadPodNames: []string{"ns/pod"},
		StagingMounts:   []NodeMountReference{{NodeID: "fr-par-1/55555555-5555-4555-8555-555555555555", Path: "/var/lib/kubelet/plugins/stage"}},
		WorkloadTargets: []NodeMountReference{{NodeID: "fr-par-1/55555555-5555-4555-8555-555555555555", Path: "/var/lib/kubelet/pods/target"}},
	}
	blockers, err := UninstallPreflightBlockers(snapshot)
	if err != nil {
		t.Fatalf("UninstallPreflightBlockers() error = %v", err)
	}
	if len(blockers) != 7 || !slices.IsSorted(blockers) || !strings.Contains(strings.Join(blockers, "\n"), "pv-a") || !strings.Contains(strings.Join(blockers, "\n"), "plugins/stage") {
		t.Fatalf("blockers = %#v", blockers)
	}
}

func releasedLeaseForUninstall(t *testing.T) coordination.LeaseSnapshot {
	t.Helper()
	holder, err := coordination.NewHolderEvidence(
		"44444444-4444-4444-8444-444444444444", "worker-a",
		"fr-par-1/55555555-5555-4555-8555-555555555555",
		"55555555-5555-4555-8555-555555555555", "fr-par-1",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("HolderEvidence.Annotations() error = %v", err)
	}
	current := coordination.LeaseSnapshot{
		UID: "66666666-6666-4666-8666-666666666666", ResourceVersion: "1",
		HolderIdentity: holder.PodUID, Annotations: annotations,
	}
	released, err := coordination.PlanGracefulRelease(
		current, holder, uninstallRequestID, time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC), 0, false,
	)
	if err != nil {
		t.Fatalf("PlanGracefulRelease() error = %v", err)
	}
	released.ResourceVersion = "2"
	return released
}

func validUninstallCompletion(t *testing.T) UninstallCompletionEvidence {
	t.Helper()
	parents := []string{
		"77777777-7777-4777-8777-777777777777",
		"88888888-8888-4888-8888-888888888888",
	}
	nodes := []string{
		"fr-par-1/99999999-9999-4999-8999-999999999999",
		"fr-par-2/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
	}
	return UninstallCompletionEvidence{
		RequestID: uninstallRequestID, ExpectedNodeIDs: nodes,
		ExpectedParentFilesystemIDs: parents, ProviderInventoriesFresh: true,
		Nodes: []NodeUnmountEvidence{
			{NodeID: nodes[1], UnmountedParents: []ParentUnmountEvidence{
				{ParentFilesystemID: parents[1], MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parents[1]},
				{ParentFilesystemID: parents[0], MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parents[0]},
			}},
			{NodeID: nodes[0], UnmountedParents: []ParentUnmountEvidence{
				{ParentFilesystemID: parents[0], MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parents[0]},
				{ParentFilesystemID: parents[1], MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parents[1]},
			}},
		},
		ReleasedLease: releasedLeaseForUninstall(t),
	}
}

func TestValidateUninstallCompletionRequiresExactCleanupAndRelease(t *testing.T) {
	evidence := validUninstallCompletion(t)
	if err := ValidateUninstallCompletion(evidence); err != nil {
		t.Fatalf("ValidateUninstallCompletion() error = %v", err)
	}

	tests := map[string]func(*UninstallCompletionEvidence){
		"missing node":          func(value *UninstallCompletionEvidence) { value.Nodes = value.Nodes[:1] },
		"remaining child mount": func(value *UninstallCompletionEvidence) { value.Nodes[0].RemainingChildMountPaths = []string{"/child"} },
		"node pod":              func(value *UninstallCompletionEvidence) { value.NodePluginPodNames = []string{"node-plugin-a"} },
		"controller pod":        func(value *UninstallCompletionEvidence) { value.ControllerPodNames = []string{"controller-a"} },
		"stale provider":        func(value *UninstallCompletionEvidence) { value.ProviderInventoriesFresh = false },
		"attachment":            func(value *UninstallCompletionEvidence) { value.RegionalAttachmentIDs = []string{"attachment-a"} },
		"held Lease":            func(value *UninstallCompletionEvidence) { value.ReleasedLease.HolderIdentity = "still-held" },
		"mismatched mount path": func(value *UninstallCompletionEvidence) {
			value.Nodes[0].UnmountedParents[0].MountPath = "/wrong/parent"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := validUninstallCompletion(t)
			mutate(&changed)
			if err := ValidateUninstallCompletion(changed); err == nil {
				t.Fatal("ValidateUninstallCompletion(blocked) error = nil")
			}
		})
	}
}

func TestValidateUninstallCompletionBindsGracefulMarkerToRequest(t *testing.T) {
	evidence := validUninstallCompletion(t)
	release, present, err := coordination.ParseGracefulRelease(evidence.ReleasedLease.Annotations)
	if err != nil || !present {
		t.Fatalf("ParseGracefulRelease() = %#v, %v, %v", release, present, err)
	}
	evidence.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	if err := ValidateUninstallCompletion(evidence); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("ValidateUninstallCompletion(other request) error = %v", err)
	}
}
