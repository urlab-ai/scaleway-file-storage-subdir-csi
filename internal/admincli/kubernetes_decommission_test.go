package admincli

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
)

func newDecommissionBackendHarness(t *testing.T) (*kubernetesDecommissionBackend, *fake.Clientset, *fakePodExecutor) {
	t.Helper()
	objects, _ := operatorObjects(t)
	client := fake.NewClientset(objects...)
	installFakeScaleReactors(t, client)
	executor := &fakePodExecutor{t: t, released: operatorReleaseResult(t)}
	backend, err := newKubernetesDecommissionBackendForClient(client, executor, operatorDecommissionInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: testRequestID,
		parentFilesystemID: operatorParentID, mode: admin.DecommissionExecute,
	}, "1.0.0")
	if err != nil {
		t.Fatalf("newKubernetesDecommissionBackendForClient() error = %v", err)
	}
	backend.now = func() time.Time { return time.Date(2026, 7, 13, 21, 0, 0, 0, time.UTC) }
	return backend, client, executor
}

func TestKubernetesDecommissionBackendDryRunIsCompleteAndReadOnly(t *testing.T) {
	backend, client, executor := newDecommissionBackendHarness(t)
	inventory, err := backend.ReadDecommissionInventory(context.Background(), operatorMutationRequest(), operatorParentID)
	if err != nil {
		t.Fatalf("ReadDecommissionInventory() error = %v", err)
	}
	if !inventory.Complete || inventory.ParentFilesystemID != operatorParentID || len(inventory.Blockers) != 0 || len(inventory.NodeTargets) != 1 || inventory.NodeTargets[0].NodeID != operatorNodeID {
		t.Fatalf("decommission inventory = %#v", inventory)
	}
	if _, err := client.CoreV1().ConfigMaps(operatorNamespace).Get(context.Background(), decommissionProgressConfigMapName(operatorNamespace, operatorRelease, testRequestID), metav1.GetOptions{}); err == nil {
		t.Fatal("read-only decommission inventory created progress")
	}
	wantCalls := []string{
		"handshake:driver-system/driver-controller-pod",
		"handshake:driver-system/driver-node-pod", "inspect:driver-node-pod",
		"handshake:driver-system/driver-controller-pod", "decommission.inspect:driver-controller-pod",
		"handshake:driver-system/driver-node-pod", "decommission.inspect:driver-node-pod",
	}
	if !slices.Equal(executor.calls, wantCalls) {
		t.Fatalf("decommission dry-run calls = %#v, want %#v", executor.calls, wantCalls)
	}
}

func TestKubernetesDecommissionBackendPersistsPostQuiesceProofAndResumes(t *testing.T) {
	backend, client, executor := newDecommissionBackendHarness(t)
	ctx := context.Background()
	request := operatorMutationRequest()
	if _, err := backend.ReadDecommissionInventory(ctx, request, operatorParentID); err != nil {
		t.Fatalf("initial ReadDecommissionInventory() error = %v", err)
	}
	if err := backend.QuiesceParent(ctx, testRequestID, operatorParentID); err != nil {
		t.Fatalf("QuiesceParent() error = %v", err)
	}
	if backend.progress == nil || backend.progress.value.PostQuiesceValidated {
		t.Fatalf("progress immediately after quiesce = %#v", backend.progress)
	}
	if _, err := backend.ReadDecommissionInventory(ctx, request, operatorParentID); err != nil {
		t.Fatalf("post-quiesce ReadDecommissionInventory() error = %v", err)
	}
	if backend.progress == nil || !backend.progress.value.PostQuiesceValidated {
		t.Fatal("post-quiesce repeated inventory was not persisted")
	}
	target := admin.UninstallNodeTarget{NodeID: operatorNodeID, PodName: "driver-node-pod"}
	evidence, err := backend.UnmountNodeParent(ctx, testRequestID, operatorParentID, target)
	if err != nil || evidence.NodeID != operatorNodeID {
		t.Fatalf("UnmountNodeParent() evidence/error = %#v/%v", evidence, err)
	}
	if err := backend.DeleteNodePlugin(ctx, testRequestID); err != nil {
		t.Fatalf("DeleteNodePlugin() error = %v", err)
	}
	if err := client.CoreV1().Pods(operatorNamespace).Delete(ctx, "driver-node-pod", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete fake node Pod: %v", err)
	}
	if err := backend.WaitNodePluginStopped(ctx, testRequestID); err != nil {
		t.Fatalf("WaitNodePluginStopped() error = %v", err)
	}
	cleanup, err := backend.CleanupControllerParent(ctx, testRequestID, operatorParentID)
	if err != nil || !cleanup.ProviderInventoriesFresh || !slices.Equal(cleanup.DetachedParentFilesystemIDs, []string{operatorParentID}) {
		t.Fatalf("CleanupControllerParent() result/error = %#v/%v", cleanup, err)
	}
	released, err := backend.ReleaseControllerAfterDecommission(ctx, testRequestID, operatorParentID)
	if err != nil || released.UID != operatorLeaseUID {
		t.Fatalf("ReleaseControllerAfterDecommission() result/error = %#v/%v", released, err)
	}
	if err := backend.ScaleControllerToZero(ctx, testRequestID); err != nil {
		t.Fatalf("ScaleControllerToZero() error = %v", err)
	}
	if err := client.CoreV1().Pods(operatorNamespace).Delete(ctx, "driver-controller-pod", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete fake controller Pod: %v", err)
	}
	completed, err := backend.WaitControllerStopped(ctx, testRequestID)
	if err != nil || completed.Format(time.RFC3339Nano) != "2026-07-13T21:00:00Z" {
		t.Fatalf("WaitControllerStopped() time/error = %s/%v", completed, err)
	}

	resumedExecutor := &fakePodExecutor{t: t, released: operatorReleaseResult(t)}
	resumed, err := newKubernetesDecommissionBackendForClient(client, resumedExecutor, operatorDecommissionInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: testRequestID,
		parentFilesystemID: operatorParentID, mode: admin.DecommissionExecute,
	}, "1.0.0")
	if err != nil {
		t.Fatalf("new resumed decommission backend: %v", err)
	}
	if _, err := resumed.ReadDecommissionInventory(ctx, request, operatorParentID); err != nil {
		t.Fatalf("resumed ReadDecommissionInventory() error = %v", err)
	}
	if err := resumed.QuiesceParent(ctx, testRequestID, operatorParentID); err != nil {
		t.Fatalf("resumed QuiesceParent() error = %v", err)
	}
	if _, err := resumed.UnmountNodeParent(ctx, testRequestID, operatorParentID, target); err != nil {
		t.Fatalf("resumed UnmountNodeParent() error = %v", err)
	}
	if err := resumed.DeleteNodePlugin(ctx, testRequestID); err != nil {
		t.Fatalf("resumed DeleteNodePlugin() error = %v", err)
	}
	if err := resumed.WaitNodePluginStopped(ctx, testRequestID); err != nil {
		t.Fatalf("resumed WaitNodePluginStopped() error = %v", err)
	}
	if _, err := resumed.CleanupControllerParent(ctx, testRequestID, operatorParentID); err != nil {
		t.Fatalf("resumed CleanupControllerParent() error = %v", err)
	}
	if _, err := resumed.ReleaseControllerAfterDecommission(ctx, testRequestID, operatorParentID); err != nil {
		t.Fatalf("resumed ReleaseControllerAfterDecommission() error = %v", err)
	}
	if err := resumed.ScaleControllerToZero(ctx, testRequestID); err != nil {
		t.Fatalf("resumed ScaleControllerToZero() error = %v", err)
	}
	resumedCompletion, err := resumed.WaitControllerStopped(ctx, testRequestID)
	if err != nil || !resumedCompletion.Equal(completed) || len(resumedExecutor.calls) != 0 {
		t.Fatalf("resumed completion/calls/error = %s/%#v/%v", resumedCompletion, resumedExecutor.calls, err)
	}
	if len(executor.calls) < 18 {
		t.Fatalf("initial decommission executor calls are incomplete: %#v", executor.calls)
	}
}

func TestKubernetesDecommissionDryRunDoesNotAdvanceExistingExecuteProgress(t *testing.T) {
	execute, client, executor := newDecommissionBackendHarness(t)
	ctx := context.Background()
	request := operatorMutationRequest()
	if _, err := execute.ReadDecommissionInventory(ctx, request, operatorParentID); err != nil {
		t.Fatalf("initial ReadDecommissionInventory() error = %v", err)
	}
	if err := execute.QuiesceParent(ctx, testRequestID, operatorParentID); err != nil {
		t.Fatalf("QuiesceParent() error = %v", err)
	}
	name := decommissionProgressConfigMapName(operatorNamespace, operatorRelease, testRequestID)
	before, err := client.CoreV1().ConfigMaps(operatorNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get progress before dry-run: %v", err)
	}
	dryRun, err := newKubernetesDecommissionBackendForClient(client, executor, operatorDecommissionInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: testRequestID,
		parentFilesystemID: operatorParentID, mode: admin.DecommissionDryRun,
	}, "1.0.0")
	if err != nil {
		t.Fatalf("new dry-run backend: %v", err)
	}
	if _, err := dryRun.ReadDecommissionInventory(ctx, request, operatorParentID); err != nil {
		t.Fatalf("dry-run ReadDecommissionInventory(existing progress) error = %v", err)
	}
	after, err := client.CoreV1().ConfigMaps(operatorNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get progress after dry-run: %v", err)
	}
	if before.ResourceVersion != after.ResourceVersion || !maps.Equal(before.Data, after.Data) {
		t.Fatalf("dry-run mutated durable progress: before=%#v after=%#v", before.Data, after.Data)
	}
}

func TestParseOperatorDecommissionIsClosedAndBounded(t *testing.T) {
	parsed, err := parseOperatorDecommission([]string{
		"decommission", "prepare", "--namespace=" + operatorNamespace, "--release=" + operatorRelease,
		"--request-id=" + testRequestID, "--parent-filesystem-id=" + operatorParentID,
		"--mode=dry-run", "--timeout=10m",
	})
	if err != nil {
		t.Fatalf("parseOperatorDecommission() error = %v", err)
	}
	if parsed.parentFilesystemID != operatorParentID || parsed.mode != admin.DecommissionDryRun || parsed.timeout != 10*time.Minute {
		t.Fatalf("parsed decommission = %#v", parsed)
	}
	for _, args := range [][]string{
		{"decommission"},
		{"decommission", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--parent-filesystem-id=" + operatorParentID, "--mode=mutate"},
		{"decommission", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--parent-filesystem-id=bad/id", "--mode=dry-run"},
		{"decommission", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--parent-filesystem-id=" + operatorParentID, "--mode=dry-run", "--timeout=30s"},
	} {
		if _, err := parseOperatorDecommission(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorDecommission(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}
