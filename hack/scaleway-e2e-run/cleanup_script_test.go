package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
)

func TestCleanupScriptFailsClosedAndRequiresRunID(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is required for the checked-in cleanup script")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	temporary := t.TempDir()
	evidence := filepath.Join(temporary, "evidence")
	preconditions := filepath.Join(temporary, "preconditions.json")

	command := exec.Command(script, "cleanup", "--kubeconfig=/tmp/kubeconfig", "--namespace=driver-system", "--release=driver",
		"--admin=/tmp/csi-admin", "--evidence-dir="+evidence, "--preconditions="+preconditions)
	command.Env = append(os.Environ(), "JQ="+jq)
	if err := command.Run(); err == nil {
		t.Fatal("cleanup without run ID succeeded")
	}
	if _, err := os.Stat(preconditions); !os.IsNotExist(err) {
		t.Fatalf("cleanup without run ID wrote preconditions: %v", err)
	}

	helper := writeExecutable(t, temporary, "helm-error", "#!/bin/sh\nexit 1\n")
	kubectl := writeExecutable(t, temporary, "kubectl-unused", "#!/bin/sh\nexit 99\n")
	admin := writeExecutable(t, temporary, "admin-unused", "#!/bin/sh\nexit 99\n")
	command = exec.Command(script, "cleanup", "--kubeconfig=/tmp/kubeconfig", "--namespace=driver-system", "--release=driver",
		"--admin="+admin, "--validator="+admin,
		"--run-id=11111111-1111-4111-8111-111111111111",
		"--parent-a=77777777-7777-4777-8777-777777777777", "--parent-b=88888888-8888-4888-8888-888888888888",
		"--evidence-dir="+evidence, "--preconditions="+preconditions)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helper, "KUBECTL="+kubectl)
	if err := command.Run(); err == nil {
		t.Fatal("cleanup accepted unavailable Helm state")
	}
	if _, err := os.Stat(preconditions); !os.IsNotExist(err) {
		t.Fatalf("cleanup after Helm error wrote preconditions: %v", err)
	}
}

func TestCleanupScriptDerivesPreconditionsFromCompletedUninstall(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is required for the checked-in cleanup script")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	temporary := t.TempDir()
	helmState := filepath.Join(temporary, "helm-state")
	namespaceState := filepath.Join(temporary, "namespace-state")
	if err := os.WriteFile(helmState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(namespaceState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	helm := writeExecutable(t, temporary, "helm", `#!/bin/sh
case "$1" in
  list)
    if [ "$(sed -n '1p' "$HELM_STATE")" = present ]; then
      printf '%s\n' '[{"name":"driver"}]'
    else
      printf '%s\n' '[]'
    fi
    ;;
  status) printf '%s\n' '{"name":"driver"}' ;;
  uninstall) printf '%s\n' absent >"$HELM_STATE" ;;
  *) exit 91 ;;
esac
`)
	kubectl := writeExecutable(t, temporary, "kubectl", `#!/bin/sh
case "$*" in
  *"get pv -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get namespace"*"--ignore-not-found"*)
    if [ "$(sed -n '1p' "$NAMESPACE_STATE")" = present ]; then
      printf '%s\n' '{"metadata":{"labels":{"sfs-subdir-e2e-run":"11111111-1111-4111-8111-111111111111"}}}'
    fi
    ;;
  *"delete namespace"*) printf '%s\n' absent >"$NAMESPACE_STATE" ;;
  *"delete pod,pvc"*) ;;
  *) exit 92 ;;
esac
`)
	admin := writeExecutable(t, temporary, "csi-admin", `#!/bin/sh
case "$*" in
  *"--mode=dry-run"*) printf '%s\n' '{"ready":true,"completed":false,"blockers":[]}' ;;
  *"--mode=execute"*) printf '%s\n' '{"ready":true,"completed":true,"blockers":[],"audit":{}}' ;;
  *) exit 93 ;;
esac
`)
	validator := writeExecutable(t, temporary, "validator", `#!/bin/sh
case "$*" in
  *"validate-uninstall-result"*"--request-id=11111111-1111-4111-8111-111111111111"*"--parent-a=77777777-7777-4777-8777-777777777777"*"--parent-b=88888888-8888-4888-8888-888888888888"*) exit 0 ;;
  *) exit 94 ;;
esac
`)
	evidence := filepath.Join(temporary, "evidence")
	preconditions := filepath.Join(temporary, "preconditions.json")
	command := exec.Command(script, "cleanup", "--kubeconfig=/tmp/kubeconfig", "--namespace=driver-system", "--release=driver",
		"--admin="+admin, "--validator="+validator,
		"--run-id=11111111-1111-4111-8111-111111111111",
		"--parent-a=77777777-7777-4777-8777-777777777777", "--parent-b=88888888-8888-4888-8888-888888888888",
		"--evidence-dir="+evidence, "--preconditions="+preconditions)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState)
	if output, err := command.CombinedOutput(); err != nil {
		cleanupLog, _ := os.ReadFile(filepath.Join(evidence, "cleanup-kubernetes.log"))
		t.Fatalf("cleanup error = %v, output = %s, cleanup log = %s", err, output, cleanupLog)
	}
	encoded, err := os.ReadFile(preconditions)
	if err != nil {
		t.Fatal(err)
	}
	var observed map[string]bool
	if err := json.Unmarshal(encoded, &observed); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 13 {
		t.Fatalf("cleanup precondition count = %d, want 13", len(observed))
	}
	for name, value := range observed {
		if !value {
			t.Fatalf("cleanup precondition %q is false", name)
		}
	}
}

func TestProviderReviewMustBeFreshAtLivePreflight(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	review := e2eplan.ProviderReview{ObservedAt: now.Add(-time.Hour).Format(time.RFC3339Nano)}
	if err := validateProviderReviewFresh(review, now); err != nil {
		t.Fatalf("validateProviderReviewFresh() error = %v", err)
	}
	review.ObservedAt = now.Add(-maximumProviderReviewAge - time.Second).Format(time.RFC3339Nano)
	if err := validateProviderReviewFresh(review, now); err == nil {
		t.Fatal("validateProviderReviewFresh(stale) error = nil")
	}
	review.ObservedAt = now.Add(maximumProviderReviewFutureSkew + time.Second).Format(time.RFC3339Nano)
	if err := validateProviderReviewFresh(review, now); err == nil {
		t.Fatal("validateProviderReviewFresh(future) error = nil")
	}
}

func TestAttachmentCapacityMustCoverEveryParent(t *testing.T) {
	if err := validateAttachmentCapacity(2, 2); err != nil {
		t.Fatalf("validateAttachmentCapacity(2, 2) error = %v", err)
	}
	if err := validateAttachmentCapacity(1, 2); err == nil {
		t.Fatal("validateAttachmentCapacity(1, 2) error = nil")
	}
}

func TestRemoveRetainedKubeconfigPropagatesFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-kubeconfig")
	if err := removeRetainedKubeconfig(missing); err != nil {
		t.Fatalf("removeRetainedKubeconfig(missing) error = %v", err)
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "keep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeRetainedKubeconfig(directory); err == nil {
		t.Fatal("removeRetainedKubeconfig(directory) error = nil")
	}
}

func TestValidateUninstallResultRequiresTypedRunBoundAudit(t *testing.T) {
	const (
		requestID = "11111111-1111-4111-8111-111111111111"
		parentA   = "77777777-7777-4777-8777-777777777777"
		parentB   = "88888888-8888-4888-8888-888888888888"
		nodeID    = "fr-par-1/99999999-9999-4999-8999-999999999999"
	)
	nodeRoot := "/var/lib/scaleway-sfs-subdir-csi/parents"
	controllerRoot := "/var/lib/scaleway-sfs-subdir-csi/controller-parents"
	parents := []string{parentA, parentB}
	parentUnmounts := func(root string) []admin.ParentUnmountEvidence {
		return []admin.ParentUnmountEvidence{
			{ParentFilesystemID: parentA, MountPath: root + "/" + parentA},
			{ParentFilesystemID: parentB, MountPath: root + "/" + parentB},
		}
	}
	result := admin.UninstallPrepareResult{
		RequestID: requestID, Mode: admin.UninstallExecute, Ready: true, Completed: true,
		Plan: admin.UninstallPlan{
			ChartVersion: "1.0.0", DriverVersion: "1.0.0", AdminVersion: "1.0.0",
			LeaseName: "scaleway-sfs-subdir-csi-controller", ParentFilesystemIDs: parents,
			NodeTargets:         []admin.UninstallNodeTarget{{NodeID: nodeID, PodName: "driver-node-a"}},
			NodeParentMountRoot: nodeRoot, ControllerParentMountRoot: controllerRoot,
		},
		Audit: &admin.UninstallAudit{
			SchemaVersion: "1", RequestID: requestID,
			ChartVersion: "1.0.0", DriverVersion: "1.0.0", AdminVersion: "1.0.0",
			LeaseName: "scaleway-sfs-subdir-csi-controller", LeaseUID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			ParentFilesystemIDs: parents, NodeParentMountRoot: nodeRoot, ControllerParentMountRoot: controllerRoot,
			CheckedNodeIDs: []string{nodeID}, CheckedInstanceIDs: []string{"99999999-9999-4999-8999-999999999999"},
			NodeUnmounts:               []admin.NodeUnmountEvidence{{NodeID: nodeID, UnmountedParents: parentUnmounts(nodeRoot)}},
			ControllerUnmountedParents: parentUnmounts(controllerRoot), DetachedParentFilesystemIDs: parents,
			RegionalInventorySHA256: "sha256:" + strings.Repeat("a", 64),
			InstanceInventorySHA256: "sha256:" + strings.Repeat("b", 64),
			CompletedAt:             "2026-07-16T12:00:00Z",
		},
	}
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateUninstallResult(encoded, requestID, parents); err != nil {
		t.Fatalf("validateUninstallResult() error = %v", err)
	}
	if err := validateUninstallResult([]byte(`{"requestID":"11111111-1111-4111-8111-111111111111","mode":"execute","ready":true,"completed":true,"plan":{},"audit":{}}`), requestID, parents); err == nil {
		t.Fatal("validateUninstallResult() accepted an empty audit")
	}
	if err := validateUninstallResult(encoded, "22222222-2222-4222-8222-222222222222", parents); err == nil {
		t.Fatal("validateUninstallResult() accepted another request ID")
	}
}

func TestAmbiguousProviderCreateRequiresStableDiscovery(t *testing.T) {
	inventory := e2ecleanup.Inventory{PendingCreate: &e2ecleanup.CreateIntent{Kind: e2ecleanup.ResourceKindParent, Name: "late-parent"}}
	calls := 0
	reconcile := func(_ context.Context, current e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
		calls++
		if calls == 3 {
			current.Resources = append(current.Resources, e2ecleanup.Resource{
				Kind: e2ecleanup.ResourceKindParent, ID: "late-resource", Name: "late-parent",
				CreatedByRun: true, State: e2ecleanup.ResourceStatePresent,
			})
		}
		return current, nil
	}
	waits := 0
	wait := func(context.Context, time.Duration) error { waits++; return nil }
	observed, err := confirmStableProvisioningDiscovery(context.Background(), inventory, reconcile, wait)
	if err != nil {
		t.Fatalf("confirmStableProvisioningDiscovery() error = %v", err)
	}
	if calls != 7 || waits != 6 || len(observed.Resources) != 1 || observed.PendingCreate != nil {
		t.Fatalf("stable discovery calls/waits/resources = %d/%d/%d, want 7/6/1", calls, waits, len(observed.Resources))
	}
}

func TestAmbiguousProviderCreateCannotCompleteWhileIntentIsUnresolved(t *testing.T) {
	inventory := e2ecleanup.Inventory{PendingCreate: &e2ecleanup.CreateIntent{Kind: e2ecleanup.ResourceKindCluster, Name: "late-cluster"}}
	calls := 0
	waits := 0
	_, err := confirmStableProvisioningDiscovery(context.Background(), inventory,
		func(_ context.Context, current e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
			calls++
			return current, nil
		},
		func(context.Context, time.Duration) error { waits++; return nil },
	)
	if err == nil || !strings.Contains(err.Error(), "remains unresolved") {
		t.Fatalf("confirmStableProvisioningDiscovery() error = %v", err)
	}
	if calls != provisioningDiscoveryStableReads || waits != provisioningDiscoveryStableReads-1 {
		t.Fatalf("stable unresolved discovery calls/waits = %d/%d", calls, waits)
	}
}

func TestProviderDiscoveryStabilizesExactSetNotOnlyCardinality(t *testing.T) {
	calls := 0
	waits := 0
	observed, err := confirmStableProvisioningDiscovery(context.Background(), e2ecleanup.Inventory{},
		func(_ context.Context, current e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
			calls++
			id := "resource-a"
			if calls >= 2 {
				id = "resource-b"
			}
			current.Resources = []e2ecleanup.Resource{{Kind: e2ecleanup.ResourceKindCluster, ID: id, Name: "cluster", State: e2ecleanup.ResourceStatePresent}}
			return current, nil
		},
		func(context.Context, time.Duration) error { waits++; return nil },
	)
	if err != nil {
		t.Fatalf("confirmStableProvisioningDiscovery() error = %v", err)
	}
	if calls != 6 || waits != 5 || len(observed.Resources) != 1 || observed.Resources[0].ID != "resource-b" {
		t.Fatalf("exact-set discovery calls/waits/resources = %d/%d/%#v", calls, waits, observed.Resources)
	}
}

func writeExecutable(t *testing.T, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
