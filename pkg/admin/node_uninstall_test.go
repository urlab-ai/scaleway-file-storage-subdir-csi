package admin

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
)

const nodeUninstallRoot = "/var/lib/scaleway-sfs-subdir-csi/parents"

type reconcileCountingMounter struct {
	*mount.Fake
	reconcileCalls int
}

func newTestNodeUninstallCommandOperation(t *testing.T, nodeID, parentRoot string, parentIDs []string, mounter mount.Interface) (*NodeUninstallCommandOperation, error) {
	t.Helper()
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatal(err)
	}
	return NewNodeUninstallCommandOperation(nodeID, parentRoot, parentIDs, mounter, gate, func(string) error { return nil })
}

func (mounter *reconcileCountingMounter) ReconcileQuarantines(ctx context.Context) error {
	mounter.reconcileCalls++
	return mounter.Fake.ReconcileQuarantines(ctx)
}

func TestNodeUninstallCommandUnmountsExactParentsAndIsIdempotent(t *testing.T) {
	mounter := mount.NewFake()
	parents := []string{uninstallParentB, uninstallParentA}
	for _, parentID := range parents {
		if err := mounter.MountParent(context.Background(), parentID, nodeUninstallRoot+"/"+parentID); err != nil {
			t.Fatalf("MountParent() error = %v", err)
		}
	}
	operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, parents, mounter)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	request := validCommandMutationRequest()
	for attempt := 0; attempt < 2; attempt++ {
		encoded, err := operation.HandleCommand(context.Background(), CommandUninstallPrepare, request, nil)
		if err != nil {
			t.Fatalf("HandleCommand(attempt %d) error = %v", attempt, err)
		}
		var evidence NodeUnmountEvidence
		if err := json.Unmarshal(encoded, &evidence); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		if evidence.NodeID != uninstallNodeA || len(evidence.UnmountedParents) != 2 || evidence.UnmountedParents[0].ParentFilesystemID != uninstallParentA {
			t.Fatalf("evidence = %#v", evidence)
		}
	}
	if table, err := mounter.Snapshot(context.Background()); err != nil || len(table.Entries) != 0 {
		t.Fatalf("final mount table = %#v, %v", table, err)
	}
}

func TestNodeUninstallPrepareDrainsNodeMutationsBeforeUnmount(t *testing.T) {
	mounter := mount.NewFake()
	if err := mounter.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA); err != nil {
		t.Fatal(err)
	}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatal(err)
	}
	releaseMutation, err := gate.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	becameUnready := make(chan string, 1)
	operation, err := NewNodeUninstallCommandOperation(
		uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter, gate,
		func(requestID string) error {
			becameUnready <- requestID
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := validCommandMutationRequest()
	result := make(chan error, 1)
	go func() {
		_, handleErr := operation.HandleCommand(context.Background(), CommandUninstallPrepare, request, nil)
		result <- handleErr
	}()
	if got := <-becameUnready; got != request.RequestID {
		t.Fatalf("unready request ID = %q", got)
	}
	deadline := time.After(time.Second)
	for gate.QuiesceRequestID() != request.RequestID {
		select {
		case <-deadline:
			t.Fatalf("node mutation gate is not closed by %q", request.RequestID)
		default:
			runtime.Gosched()
		}
	}
	if table, err := mounter.Snapshot(context.Background()); err != nil || len(table.Entries) != 1 {
		t.Fatalf("parent was unmounted before admitted mutation drained: %#v, %v", table, err)
	}
	releaseMutation()
	if err := <-result; err != nil {
		t.Fatalf("HandleCommand(prepare) error = %v", err)
	}
	if table, err := mounter.Snapshot(context.Background()); err != nil || len(table.Entries) != 0 {
		t.Fatalf("parent remains after drained prepare: %#v, %v", table, err)
	}
}

func TestNodeUninstallInspectIsReadOnlyAndReportsChildTargets(t *testing.T) {
	mounter := mount.NewFake()
	if err := mounter.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	stage := "/var/lib/kubelet/plugins/kubernetes.io/csi/file-storage-subdir.csi.urlab.ai/volume/stage"
	publish := "/var/lib/kubelet/pods/pod/volumes/kubernetes.io~csi/volume/mount"
	mounter.Seed(mount.Entry{Kind: mount.KindPublish, Target: publish, ParentFilesystemID: uninstallParentA})
	mounter.Seed(mount.Entry{Kind: mount.KindStage, Target: stage, ParentFilesystemID: uninstallParentA})
	operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	encoded, err := operation.HandleCommand(context.Background(), CommandUninstallInspect, validCommandMutationRequest(), nil)
	if err != nil {
		t.Fatalf("HandleCommand(inspect) error = %v", err)
	}
	var inspection NodeUninstallInspection
	if err := json.Unmarshal(encoded, &inspection); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if inspection.NodeID != uninstallNodeA || len(inspection.StagingMountPaths) != 1 || inspection.StagingMountPaths[0] != stage || len(inspection.WorkloadTargetPaths) != 1 || inspection.WorkloadTargetPaths[0] != publish {
		t.Fatalf("inspection = %#v", inspection)
	}
	table, err := mounter.Snapshot(context.Background())
	if err != nil || len(table.Entries) != 3 {
		t.Fatalf("read-only inspection changed table = %#v, %v", table, err)
	}
}

func TestNodeUninstallCommandRejectsChildForeignAndStackedMounts(t *testing.T) {
	tests := []struct {
		name string
		seed func(*mount.Fake)
		want string
	}{
		{name: "child", seed: func(mounter *mount.Fake) {
			_ = mounter.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA)
			mounter.Seed(mount.Entry{Kind: mount.KindStage, Target: "/var/lib/kubelet/plugins/kubernetes.io/csi/test/stage", ParentFilesystemID: uninstallParentA})
		}, want: "child mount"},
		{name: "foreign", seed: func(mounter *mount.Fake) {
			foreign := "99999999-9999-4999-8999-999999999999"
			_ = mounter.MountParent(context.Background(), foreign, nodeUninstallRoot+"/"+foreign)
		}, want: "foreign parent"},
		{name: "stacked", seed: func(mounter *mount.Fake) {
			_ = mounter.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA)
			mounter.Seed(mount.Entry{Kind: mount.KindParent, Target: nodeUninstallRoot + "/" + uninstallParentA, FilesystemType: "virtiofs", FilesystemSource: uninstallParentA, ParentFilesystemID: uninstallParentA, BackingRelativePath: "/"})
		}, want: "mount layers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mounter := mount.NewFake()
			test.seed(mounter)
			operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter)
			if err != nil {
				t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
			}
			_, err = operation.HandleCommand(context.Background(), CommandUninstallPrepare, validCommandMutationRequest(), nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("HandleCommand() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestNodeUninstallCommandValidatesAuthority(t *testing.T) {
	mounter := mount.NewFake()
	if _, err := newTestNodeUninstallCommandOperation(t, "bad", nodeUninstallRoot, []string{uninstallParentA}, mounter); err == nil {
		t.Fatal("NewNodeUninstallCommandOperation(bad node) error = nil")
	}
	if _, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, "/", []string{uninstallParentA}, mounter); err == nil {
		t.Fatal("NewNodeUninstallCommandOperation(bad root) error = nil")
	}
	if _, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA, uninstallParentA}, mounter); err == nil {
		t.Fatal("NewNodeUninstallCommandOperation(duplicate parent) error = nil")
	}
	operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	invalid := validCommandMutationRequest()
	invalid.RequestID = "bad"
	if _, err := operation.HandleCommand(context.Background(), CommandUninstallPrepare, invalid, nil); err == nil {
		t.Fatal("HandleCommand(invalid request) error = nil")
	}
	if _, err := operation.HandleCommand(context.Background(), CommandUninstallPrepare, validCommandMutationRequest(), json.RawMessage(`{}`)); err == nil {
		t.Fatal("HandleCommand(payload) error = nil")
	}
}

func TestNodeDecommissionUnmountsOnlySelectedParentAndIgnoresOtherParentChildren(t *testing.T) {
	mounter := mount.NewFake()
	for _, parentID := range []string{uninstallParentA, uninstallParentB} {
		if err := mounter.MountParent(context.Background(), parentID, nodeUninstallRoot+"/"+parentID); err != nil {
			t.Fatalf("MountParent(%s) error = %v", parentID, err)
		}
	}
	otherStage := "/var/lib/kubelet/plugins/kubernetes.io/csi/other/stage"
	mounter.Seed(mount.Entry{Kind: mount.KindStage, Target: otherStage, ParentFilesystemID: uninstallParentB})
	operation, err := newTestNodeUninstallCommandOperation(t,
		uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA, uninstallParentB}, mounter,
	)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	payload, err := canonicaljson.Marshal(DecommissionParentPayload{ParentFilesystemID: uninstallParentA})
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	encoded, err := operation.HandleCommand(context.Background(), CommandDecommissionInspect, validCommandMutationRequest(), payload)
	if err != nil {
		t.Fatalf("HandleCommand(decommission.inspect) error = %v", err)
	}
	var inspection NodeDecommissionInspection
	if err := json.Unmarshal(encoded, &inspection); err != nil {
		t.Fatalf("json.Unmarshal(inspection) error = %v", err)
	}
	if !inspection.ParentMounted || len(inspection.StagingMountPaths) != 0 || inspection.ParentFilesystemID != uninstallParentA {
		t.Fatalf("decommission inspection = %#v", inspection)
	}
	encoded, err = operation.HandleCommand(context.Background(), CommandDecommissionPrepare, validCommandMutationRequest(), payload)
	if err != nil {
		t.Fatalf("HandleCommand(decommission.prepare) error = %v", err)
	}
	var result NodeDecommissionUnmountResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("json.Unmarshal(result) error = %v", err)
	}
	if result.Unmounted.ParentFilesystemID != uninstallParentA || result.Unmounted.MountPath != nodeUninstallRoot+"/"+uninstallParentA {
		t.Fatalf("decommission result = %#v", result)
	}
	table, err := mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if _, err := table.Exact(nodeUninstallRoot + "/" + uninstallParentA); err == nil {
		t.Fatal("selected parent remains mounted")
	}
	if _, err := table.Exact(nodeUninstallRoot + "/" + uninstallParentB); err != nil {
		t.Fatalf("other configured parent was unmounted: %v", err)
	}
	if _, err := table.Exact(otherStage); err != nil {
		t.Fatalf("other parent child was removed: %v", err)
	}
}

func TestNodeDecommissionRejectsSelectedParentChildrenAndUnconfiguredPayload(t *testing.T) {
	mounter := mount.NewFake()
	if err := mounter.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	mounter.Seed(mount.Entry{Kind: mount.KindStage, Target: "/var/lib/kubelet/plugins/target/stage", ParentFilesystemID: uninstallParentA})
	operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	payload, _ := canonicaljson.Marshal(DecommissionParentPayload{ParentFilesystemID: uninstallParentA})
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionPrepare, validCommandMutationRequest(), payload); err == nil || !strings.Contains(err.Error(), "child mounts") {
		t.Fatalf("HandleCommand(selected child) error = %v", err)
	}
	foreignPayload, _ := canonicaljson.Marshal(DecommissionParentPayload{ParentFilesystemID: uninstallParentB})
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionInspect, validCommandMutationRequest(), foreignPayload); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("HandleCommand(unconfigured parent) error = %v", err)
	}
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionInspect, validCommandMutationRequest(), json.RawMessage(`{"parentFilesystemID":"`+uninstallParentA+`","extra":true}`)); err == nil {
		t.Fatal("HandleCommand(unknown payload field) error = nil")
	}
}

func TestNodeDecommissionReconcilesInterruptedUnmountOnlyForPrepare(t *testing.T) {
	fake := mount.NewFake()
	if err := fake.MountParent(context.Background(), uninstallParentA, nodeUninstallRoot+"/"+uninstallParentA); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	mounter := &reconcileCountingMounter{Fake: fake}
	operation, err := newTestNodeUninstallCommandOperation(t, uninstallNodeA, nodeUninstallRoot, []string{uninstallParentA}, mounter)
	if err != nil {
		t.Fatalf("NewNodeUninstallCommandOperation() error = %v", err)
	}
	payload, _ := canonicaljson.Marshal(DecommissionParentPayload{ParentFilesystemID: uninstallParentA})
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionInspect, validCommandMutationRequest(), payload); err != nil {
		t.Fatalf("HandleCommand(inspect) error = %v", err)
	}
	if mounter.reconcileCalls != 0 {
		t.Fatalf("read-only inspect reconciled %d quarantine(s)", mounter.reconcileCalls)
	}
	if _, err := operation.HandleCommand(context.Background(), CommandDecommissionPrepare, validCommandMutationRequest(), payload); err != nil {
		t.Fatalf("HandleCommand(prepare) error = %v", err)
	}
	if mounter.reconcileCalls != 1 {
		t.Fatalf("mutating prepare reconciliation calls = %d, want 1", mounter.reconcileCalls)
	}
}
