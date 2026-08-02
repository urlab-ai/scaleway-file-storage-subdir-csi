package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

const (
	checkpointTestProjectID    = "11111111-1111-4111-8111-111111111111"
	checkpointTestParentA      = "22222222-2222-4222-8222-222222222222"
	checkpointTestParentB      = "33333333-3333-4333-8333-333333333333"
	checkpointTestInstanceID   = "44444444-4444-4444-8444-444444444444"
	checkpointTestAttachmentID = "55555555-5555-4555-8555-555555555555"
	checkpointTestClusterID    = "66666666-6666-4666-8666-666666666666"
	checkpointTestPoolID       = "77777777-7777-4777-8777-777777777777"
	checkpointTestKapsuleNode  = "88888888-8888-4888-8888-888888888888"
)

func TestStableCheckpointProviderStateRequiresConsecutiveReads(t *testing.T) {
	observations := []bool{true, false, true, true}
	calls := 0
	err := waitForStableCheckpointProviderState(
		context.Background(), time.Second, time.Nanosecond, "test convergence",
		func(context.Context) (bool, error) {
			observation := observations[calls]
			calls++
			return observation, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if calls != len(observations) {
		t.Fatalf("observer calls = %d, want %d", calls, len(observations))
	}
}

func TestStableCheckpointProviderStateFailsImmediately(t *testing.T) {
	want := errors.New("foreign attachment")
	calls := 0
	err := waitForStableCheckpointProviderState(
		context.Background(), time.Second, time.Nanosecond, "test convergence",
		func(context.Context) (bool, error) {
			calls++
			return false, want
		},
	)
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("error = %v, calls = %d; want immediate foreign-state failure", err, calls)
	}
}

func TestCheckpointDetachedParentAllowsOnlyKnownTransitionalAttachments(t *testing.T) {
	plan := checkpointProviderPlan()
	known := map[string]struct{}{checkpointTestInstanceID: {}}
	exact := checkpointParentSnapshot(checkpointTestParentA, 0)
	converged, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, exact)
	if err != nil || !converged {
		t.Fatalf("exact detached snapshot = (%v, %v), want converged", converged, err)
	}

	transitional := checkpointParentSnapshot(checkpointTestParentA, 1)
	transitional.attachments.Attachments = []*fileapi.Attachment{
		checkpointAttachment(checkpointTestParentA, checkpointTestInstanceID),
	}
	converged, err = validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, transitional)
	if err != nil || converged {
		t.Fatalf("known transitional snapshot = (%v, %v), want pending", converged, err)
	}

	foreign := checkpointParentSnapshot(checkpointTestParentA, 1)
	foreign.attachments.Attachments = []*fileapi.Attachment{
		checkpointAttachment(checkpointTestParentA, "66666666-6666-4666-8666-666666666666"),
	}
	if _, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, foreign); err == nil {
		t.Fatal("unknown attachment was treated as eventual consistency")
	}
	wrongZone := checkpointParentSnapshot(checkpointTestParentA, 1)
	wrongZoneAttachment := checkpointAttachment(checkpointTestParentA, checkpointTestInstanceID)
	zone := scw.ZoneFrPar2
	wrongZoneAttachment.Zone = &zone
	wrongZone.attachments.Attachments = []*fileapi.Attachment{wrongZoneAttachment}
	if _, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, wrongZone); err == nil {
		t.Fatal("wrong-zone attachment was treated as eventual consistency")
	}
	historical := checkpointParentSnapshot(checkpointTestParentB, 1)
	historical.attachments.Attachments = []*fileapi.Attachment{
		checkpointAttachment(checkpointTestParentB, checkpointTestInstanceID),
	}
	if _, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentB, known, "fr-par-1", true, historical); err == nil {
		t.Fatal("historical parent reattachment was treated as eventual consistency")
	}
}

func TestCheckpointDetachedParentRequiresAvailableStableCount(t *testing.T) {
	plan := checkpointProviderPlan()
	known := map[string]struct{}{checkpointTestInstanceID: {}}
	snapshot := checkpointParentSnapshot(checkpointTestParentA, 1)
	converged, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, snapshot)
	if err != nil || converged {
		t.Fatalf("lagging File Storage count = (%v, %v), want pending", converged, err)
	}
	snapshot = checkpointParentSnapshot(checkpointTestParentA, 0)
	snapshot.filesystem.Status = fileapi.FileSystemStatusUpdating
	converged, err = validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, snapshot)
	if err != nil || converged {
		t.Fatalf("updating parent = (%v, %v), want pending", converged, err)
	}
	snapshot.filesystem.Status = fileapi.FileSystemStatusError
	if _, err := validateCheckpointDetachedParentSnapshot(plan, checkpointTestParentA, known, "fr-par-1", false, snapshot); err == nil {
		t.Fatal("unsafe parent status was treated as eventual consistency")
	}
}

func TestCheckpointProvisionalSnapshotAcceptsInstanceInventoryLag(t *testing.T) {
	plan := checkpointProviderPlan()
	snapshot := exactCheckpointProvisionalSnapshot()
	snapshot.server.Filesystems = nil
	converged, err := validateCheckpointProvisionalSnapshot(
		plan,
		[]string{checkpointTestParentA, checkpointTestParentB},
		checkpointTestInstanceID,
		"fr-par-1",
		snapshot,
	)
	if err != nil || converged {
		t.Fatalf("lagging Instance inventory = (%v, %v), want pending", converged, err)
	}
	snapshot.server.Filesystems = []*instanceapi.ServerFilesystem{{
		FilesystemID: checkpointTestParentA,
		State:        instanceapi.ServerFilesystemStateAttaching,
	}}
	converged, err = validateCheckpointProvisionalSnapshot(
		plan,
		[]string{checkpointTestParentA, checkpointTestParentB},
		checkpointTestInstanceID,
		"fr-par-1",
		snapshot,
	)
	if err != nil || converged {
		t.Fatalf("attaching Instance inventory = (%v, %v), want pending", converged, err)
	}
	snapshot.server.Filesystems[0].State = instanceapi.ServerFilesystemStateAvailable
	converged, err = validateCheckpointProvisionalSnapshot(
		plan,
		[]string{checkpointTestParentA, checkpointTestParentB},
		checkpointTestInstanceID,
		"fr-par-1",
		snapshot,
	)
	if err != nil || !converged {
		t.Fatalf("exact provisional snapshot = (%v, %v), want converged", converged, err)
	}
}

func TestCheckpointProvisionalSnapshotRejectsForeignState(t *testing.T) {
	plan := checkpointProviderPlan()
	tests := map[string]func(*checkpointProvisionalProviderSnapshot){
		"foreign attached Instance": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.attachedParent.attachments.Attachments[0].ResourceID = "66666666-6666-4666-8666-666666666666"
		},
		"reattached decommissioned parent": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.decommissionedParent.filesystem.NumberOfAttachments = 1
			snapshot.decommissionedParent.attachments.Attachments = []*fileapi.Attachment{
				checkpointAttachment(checkpointTestParentB, checkpointTestInstanceID),
			}
		},
		"foreign Instance filesystem": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.server.Filesystems[0].FilesystemID = checkpointTestParentB
		},
		"stopped Instance": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.server.State = instanceapi.ServerStateStopped
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := exactCheckpointProvisionalSnapshot()
			mutate(&snapshot)
			if _, err := validateCheckpointProvisionalSnapshot(
				plan,
				[]string{checkpointTestParentA, checkpointTestParentB},
				checkpointTestInstanceID,
				"fr-par-1",
				snapshot,
			); err == nil {
				t.Fatal("foreign or unsafe provisional state was accepted")
			}
		})
	}
}

func TestCheckpointParentSnapshotRejectsMalformedInventory(t *testing.T) {
	plan := checkpointProviderPlan()
	snapshot := checkpointParentSnapshot(checkpointTestParentA, 1)
	snapshot.attachments.Attachments = []*fileapi.Attachment{nil}
	if _, err := validateCheckpointParentSnapshotIdentity(plan, checkpointTestParentA, snapshot); err == nil ||
		!strings.Contains(err.Error(), "malformed") {
		t.Fatalf("nil attachment error = %v, want malformed failure", err)
	}
}

func TestInterruptedProvisionalAttachmentClassifiesOnlyExactReplayResidue(t *testing.T) {
	plan := checkpointProviderPlan()
	request := e2erunner.Request{Zone: "fr-par-1"}
	current := exactCheckpointReplacementNodes(plan)
	snapshot := exactCheckpointProvisionalSnapshot()

	attachment, pending, err := classifyInterruptedProvisionalAttachment(
		plan, request, checkpointTestClusterID, checkpointTestPoolID,
		[]string{checkpointTestParentA, checkpointTestParentB}, current,
		snapshot.attachedParent, snapshot.decommissionedParent, snapshot.server, true,
	)
	if err != nil || pending {
		t.Fatalf("exact replay residue = (%#v, %t, %v), want exact attachment", attachment, pending, err)
	}
	want := checkpointReplayAttachment{
		AttachmentID: checkpointTestAttachmentID, ParentID: checkpointTestParentA,
		InstanceID: checkpointTestInstanceID, KapsuleNodeID: checkpointTestKapsuleNode,
		Zone: "fr-par-1",
	}
	if attachment == nil || !sameCheckpointReplayAttachment(*attachment, want) {
		t.Fatalf("replay residue = %#v, want %#v", attachment, want)
	}
	attachment, pending, err = classifyInterruptedProvisionalAttachment(
		plan, request, checkpointTestClusterID, checkpointTestPoolID,
		[]string{checkpointTestParentA, checkpointTestParentB}, current,
		snapshot.attachedParent, snapshot.decommissionedParent, snapshot.server, false,
	)
	if err != nil || !pending || attachment != nil {
		t.Fatalf("second replacement still attached = (%#v, %t, %v), want pending", attachment, pending, err)
	}

	snapshot.server.Filesystems[0].State = instanceapi.ServerFilesystemStateAttaching
	attachment, pending, err = classifyInterruptedProvisionalAttachment(
		plan, request, checkpointTestClusterID, checkpointTestPoolID,
		[]string{checkpointTestParentA, checkpointTestParentB}, current,
		snapshot.attachedParent, snapshot.decommissionedParent, snapshot.server, true,
	)
	if err != nil || !pending || attachment != nil {
		t.Fatalf("transitional replay residue = (%#v, %t, %v), want pending", attachment, pending, err)
	}
}

func TestInterruptedProvisionalAttachmentRequiresTwoViewAbsence(t *testing.T) {
	plan := checkpointProviderPlan()
	request := e2erunner.Request{Zone: "fr-par-1"}
	current := exactCheckpointReplacementNodes(plan)
	parentIDs := []string{checkpointTestParentA, checkpointTestParentB}
	active := checkpointParentSnapshot(checkpointTestParentA, 0)
	decommissioned := checkpointParentSnapshot(checkpointTestParentB, 0)

	attachment, pending, err := classifyInterruptedProvisionalAttachment(
		plan, request, checkpointTestClusterID, checkpointTestPoolID, parentIDs, current,
		active, decommissioned, nil, false,
	)
	if err != nil || !pending || attachment != nil {
		t.Fatalf("regional-only absence = (%#v, %t, %v), want pending", attachment, pending, err)
	}
	attachment, pending, err = classifyInterruptedProvisionalAttachment(
		plan, request, checkpointTestClusterID, checkpointTestPoolID, parentIDs, current,
		active, decommissioned, nil, true,
	)
	if err != nil || pending || attachment != nil {
		t.Fatalf("stable two-view absence = (%#v, %t, %v), want absent", attachment, pending, err)
	}
}

func TestInterruptedProvisionalAttachmentFailsClosedOnForeignState(t *testing.T) {
	plan := checkpointProviderPlan()
	request := e2erunner.Request{Zone: "fr-par-1"}
	current := exactCheckpointReplacementNodes(plan)
	parentIDs := []string{checkpointTestParentA, checkpointTestParentB}
	tests := map[string]func(*checkpointProvisionalProviderSnapshot){
		"foreign replacement Instance": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.attachedParent.attachments.Attachments[0].ResourceID = "99999999-9999-4999-8999-999999999999"
		},
		"historical parent reattached": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.decommissionedParent.filesystem.NumberOfAttachments = 1
			snapshot.decommissionedParent.attachments.Attachments = []*fileapi.Attachment{
				checkpointAttachment(checkpointTestParentB, checkpointTestInstanceID),
			}
		},
		"foreign Instance filesystem": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.server.Filesystems[0].FilesystemID = checkpointTestParentB
		},
		"contradictory regional count": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.attachedParent.filesystem.NumberOfAttachments = 2
		},
		"extra Instance filesystem": func(snapshot *checkpointProvisionalProviderSnapshot) {
			snapshot.server.Filesystems = append(snapshot.server.Filesystems, &instanceapi.ServerFilesystem{
				FilesystemID: checkpointTestParentB,
				State:        instanceapi.ServerFilesystemStateAvailable,
			})
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := exactCheckpointProvisionalSnapshot()
			mutate(&snapshot)
			if _, _, err := classifyInterruptedProvisionalAttachment(
				plan, request, checkpointTestClusterID, checkpointTestPoolID, parentIDs, current,
				snapshot.attachedParent, snapshot.decommissionedParent, snapshot.server, false,
			); err == nil {
				t.Fatal("foreign replay state was accepted")
			}
		})
	}
}

func TestCheckpointReplayServerDetachmentRequiresExactEmptyInventory(t *testing.T) {
	plan := checkpointProviderPlan()
	request := e2erunner.Request{Zone: "fr-par-1"}
	parentIDs := []string{checkpointTestParentA, checkpointTestParentB}
	server := &instanceapi.Server{
		ID: checkpointTestInstanceID, Project: checkpointTestProjectID,
		Zone: scw.ZoneFrPar1, State: instanceapi.ServerStateRunning,
	}
	detached, err := validateCheckpointReplayServerDetached(
		plan, request, parentIDs, checkpointTestInstanceID, server,
	)
	if err != nil || !detached {
		t.Fatalf("empty replacement Instance = (%t, %v), want detached", detached, err)
	}
	server.Filesystems = []*instanceapi.ServerFilesystem{{
		FilesystemID: checkpointTestParentA, State: instanceapi.ServerFilesystemStateDetaching,
	}}
	detached, err = validateCheckpointReplayServerDetached(
		plan, request, parentIDs, checkpointTestInstanceID, server,
	)
	if err != nil || detached {
		t.Fatalf("known transitional filesystem = (%t, %v), want pending", detached, err)
	}
	server.Filesystems[0].FilesystemID = checkpointTestParentB
	if _, err := validateCheckpointReplayServerDetached(
		plan, request, parentIDs, checkpointTestInstanceID, server,
	); err == nil {
		t.Fatal("historical parent on replacement Instance was treated as provider lag")
	}
	server.Filesystems[0].FilesystemID = "99999999-9999-4999-8999-999999999999"
	if _, err := validateCheckpointReplayServerDetached(
		plan, request, parentIDs, checkpointTestInstanceID, server,
	); err == nil {
		t.Fatal("foreign replacement filesystem was accepted")
	}
	server.Filesystems = []*instanceapi.ServerFilesystem{
		{FilesystemID: checkpointTestParentA, State: instanceapi.ServerFilesystemStateAvailable},
		{FilesystemID: checkpointTestParentB, State: instanceapi.ServerFilesystemStateAvailable},
	}
	if _, err := validateCheckpointReplayServerDetached(
		plan, request, parentIDs, checkpointTestInstanceID, server,
	); err == nil {
		t.Fatal("extra replacement filesystem was accepted")
	}
}

func checkpointProviderPlan() e2eplan.Plan {
	return e2eplan.Plan{
		ProjectID: checkpointTestProjectID,
		Region:    "fr-par",
	}
}

func checkpointParentSnapshot(parentID string, reported uint32) checkpointParentProviderSnapshot {
	return checkpointParentProviderSnapshot{
		filesystem: &fileapi.FileSystem{
			ID: parentID, ProjectID: checkpointTestProjectID, Region: scw.RegionFrPar,
			Status: fileapi.FileSystemStatusAvailable, NumberOfAttachments: reported,
		},
		attachments: &fileapi.ListAttachmentsResponse{},
	}
}

func checkpointAttachment(parentID, instanceID string) *fileapi.Attachment {
	zone := scw.ZoneFrPar1
	return &fileapi.Attachment{
		ID: checkpointTestAttachmentID, FilesystemID: parentID, ResourceID: instanceID,
		ResourceType: fileapi.AttachmentResourceTypeInstanceServer, Zone: &zone,
	}
}

func exactCheckpointProvisionalSnapshot() checkpointProvisionalProviderSnapshot {
	attached := checkpointParentSnapshot(checkpointTestParentA, 1)
	attached.attachments.Attachments = []*fileapi.Attachment{
		checkpointAttachment(checkpointTestParentA, checkpointTestInstanceID),
	}
	return checkpointProvisionalProviderSnapshot{
		attachedParent:       attached,
		decommissionedParent: checkpointParentSnapshot(checkpointTestParentB, 0),
		server: &instanceapi.Server{
			ID: checkpointTestInstanceID, Project: checkpointTestProjectID, Zone: scw.ZoneFrPar1,
			State: instanceapi.ServerStateRunning,
			Filesystems: []*instanceapi.ServerFilesystem{{
				FilesystemID: checkpointTestParentA,
				State:        instanceapi.ServerFilesystemStateAvailable,
			}},
		},
	}
}

func exactCheckpointReplacementNodes(plan e2eplan.Plan) kapsuleNodeSet {
	node := recoveryKapsuleNode(
		plan, checkpointTestClusterID, checkpointTestPoolID,
		checkpointTestKapsuleNode, checkpointTestInstanceID,
	)
	return kapsuleNodeSet{
		Nodes: []*k8sapi.Node{node}, InstanceIDs: []string{checkpointTestInstanceID},
		NodeNames: []string{node.Name},
	}
}
