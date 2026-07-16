package volume

import (
	"strings"
	"testing"
)

func TestValidateAllocationUpdateAllowsClosedForwardTransition(t *testing.T) {
	current := validDetailedAllocation(t)
	next := *current
	next.RecordRevision++
	next.UpdatedAt = "2026-07-12T12:00:01Z"
	next.PublishedNodeIDs = []string{"fr-par-1/11111111-1111-4111-8111-111111111111"}
	if err := ValidateAllocationUpdate(current, &next); err != nil {
		t.Fatalf("ValidateAllocationUpdate(same-state publish) error = %v", err)
	}
}

func TestValidateAllocationUpdateRejectsImmutableMutationOrRevisionSkip(t *testing.T) {
	current := validDetailedAllocation(t)
	next := *current
	next.RecordRevision += 2
	if err := ValidateAllocationUpdate(current, &next); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("ValidateAllocationUpdate(revision skip) error = %v", err)
	}

	next = *current
	next.RecordRevision++
	next.DirectoryUID = 2000
	if err := ValidateAllocationUpdate(current, &next); err == nil {
		t.Fatal("ValidateAllocationUpdate(immutable mutation) error = nil")
	}
}

func TestValidateAllocationUpdateRejectsBackwardTransition(t *testing.T) {
	current := validDetailedAllocation(t)
	current.State = StateDeleting
	current.DeleteOperationID = "66666666-6666-4666-8666-666666666666"
	current.DeleteOperation = DeleteOperationArchive
	current.DeleteSourcePath = "/kubernetes-volumes/tenant--claim--0123456789ab"
	current.DeletePreparedAt = recordTimestamp
	var err error
	current.DeleteTargetPath, err = ManagedLifecycleTarget(current.BasePath, ".archived", current.DirectoryName, current.LogicalVolumeID, current.DeletePreparedAt, current.DeleteOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget() error = %v", err)
	}
	current.ArchivedPath = current.DeleteTargetPath
	next := *current
	next.RecordRevision++
	next.State = StateReady
	next.DeleteOperationID = ""
	next.DeleteOperation = ""
	next.DeleteSourcePath = ""
	next.DeleteTargetPath = ""
	next.DeletePreparedAt = ""
	next.ArchivedPath = ""
	if err := ValidateAllocationUpdate(current, &next); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("ValidateAllocationUpdate(backward) error = %v", err)
	}
}
