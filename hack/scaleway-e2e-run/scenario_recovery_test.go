package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

const recoveryTestRunID = "11111111-1111-4111-8111-111111111111"

func recoveryTestPlan(t *testing.T) e2eplan.Plan {
	t.Helper()
	evidence := filepath.Join(t.TempDir(), "evidence")
	return e2eplan.Plan{
		RunID: recoveryTestRunID, Region: "fr-par", Profile: e2eplan.ProfileReleaseCandidate,
		NodePool:             e2eplan.NodePoolPlan{Count: 2},
		CleanupInventoryPath: filepath.Join(evidence, "scaleway-e2e-inventory-"+recoveryTestRunID+".json"),
	}
}

func recoveryTestInventory() e2ecleanup.Inventory {
	return e2ecleanup.Inventory{Resources: []e2ecleanup.Resource{
		{Kind: e2ecleanup.ResourceKindCluster, ID: "22222222-2222-4222-8222-222222222222"},
		{Kind: e2ecleanup.ResourceKindNodePool, ID: "33333333-3333-4333-8333-333333333333"},
	}}
}

func TestControllerRecoveryJournalRejectsScopeDrift(t *testing.T) {
	plan := recoveryTestPlan(t)
	request := e2erunner.Request{Zone: "fr-par-1"}
	inventory := recoveryTestInventory()
	journal := controllerRecoveryJournal{
		SchemaVersion: controllerRecoverySchemaVersion, RunID: plan.RunID, Phase: controllerRecoveryPhaseStopped,
		ClusterID:        "22222222-2222-4222-8222-222222222222",
		PoolID:           "33333333-3333-4333-8333-333333333333",
		OldKapsuleNodeID: "44444444-4444-4444-8444-444444444444",
		OldControllerPod: "controller-pod", OldControllerPodUID: "55555555-5555-4555-8555-555555555555",
		OldNodeName: "worker-a", OldCSINodeID: "fr-par-1/66666666-6666-4666-8666-666666666666",
		OldServerID: "66666666-6666-4666-8666-666666666666", OldZone: "fr-par-1",
		LeaseUID:       "77777777-7777-4777-8777-777777777777",
		InstallationID: plan.RunID, ActiveClusterUID: "88888888-8888-4888-8888-888888888888",
	}
	if err := journal.validateForRequest(request, plan, inventory); err != nil {
		t.Fatalf("validateForRequest() error = %v", err)
	}
	journal.PoolID = "99999999-9999-4999-8999-999999999999"
	if err := journal.validateForRequest(request, plan, inventory); err == nil {
		t.Fatal("validateForRequest(wrong pool) error = nil")
	}
	journal.PoolID = "33333333-3333-4333-8333-333333333333"
	journal.OldCSINodeID = "fr-par-2/" + journal.OldServerID
	if err := journal.validateForRequest(request, plan, inventory); err == nil {
		t.Fatal("validateForRequest(wrong CSI zone) error = nil")
	}
}

func TestControllerFreezeRecoveryActionRequiresExactProviderFence(t *testing.T) {
	freeze := controllerProcessFreeze{
		InjectorPodName:  "e2e-controller-fault-11111111",
		ControllerPodUID: "55555555-5555-4555-8555-555555555555",
		CgroupPodUID:     "55555555_5555_4555_8555_555555555555",
		HostPID:          "1234",
	}
	journal := controllerRecoveryJournal{
		OldControllerPodUID: freeze.ControllerPodUID,
	}
	action, err := controllerFreezeRecoveryActionFor(&freeze, &journal, false)
	if err != nil || action != controllerFreezeResume {
		t.Fatalf("live Instance action = %q, %v; want exact resume", action, err)
	}
	action, err = controllerFreezeRecoveryActionFor(&freeze, &journal, true)
	if err != nil || action != controllerFreezeDeleteAfterFencing {
		t.Fatalf("stopped Instance action = %q, %v; want injector deletion", action, err)
	}
	journal.OldControllerPodUID = "99999999-9999-4999-8999-999999999999"
	if _, err := controllerFreezeRecoveryActionFor(&freeze, &journal, true); err == nil {
		t.Fatal("stopped Instance accepted a fault injector for another controller")
	}
}

func TestControllerFreezeRecoveryActionNeverDropsLiveJournalWithoutInjector(t *testing.T) {
	journal := controllerRecoveryJournal{
		OldControllerPodUID: "55555555-5555-4555-8555-555555555555",
	}
	if _, err := controllerFreezeRecoveryActionFor(nil, &journal, false); err == nil {
		t.Fatal("live journal without retained injector was accepted")
	}
	action, err := controllerFreezeRecoveryActionFor(nil, &journal, true)
	if err != nil || action != controllerFreezeNoop {
		t.Fatalf("stopped journal without injector action = %q, %v; want noop", action, err)
	}
	action, err = controllerFreezeRecoveryActionFor(nil, nil, false)
	if err != nil || action != controllerFreezeNoop {
		t.Fatalf("empty recovery action = %q, %v; want noop", action, err)
	}
}

func TestControllerRecoveryNodeIdentityMustBindPoolNodeAndServer(t *testing.T) {
	journal := controllerRecoveryJournal{
		ClusterID:        "22222222-2222-4222-8222-222222222222",
		PoolID:           "33333333-3333-4333-8333-333333333333",
		OldKapsuleNodeID: "44444444-4444-4444-8444-444444444444",
		OldNodeName:      "worker-a",
		OldServerID:      "66666666-6666-4666-8666-666666666666",
	}
	node := &k8sapi.Node{
		ID: journal.OldKapsuleNodeID, Name: journal.OldNodeName,
		ClusterID: journal.ClusterID, PoolID: journal.PoolID,
		ProviderID: "scaleway://fr-par-1/" + journal.OldServerID,
	}
	present, err := controllerRecoveryNodeIdentityPresent([]*k8sapi.Node{node}, journal)
	if err != nil || !present {
		t.Fatalf("exact recovery node = %t, %v; want present", present, err)
	}
	present, err = controllerRecoveryNodeIdentityPresent(nil, journal)
	if err != nil || present {
		t.Fatalf("absent recovery node = %t, %v; want conclusively absent", present, err)
	}
	drifted := *node
	drifted.ProviderID = "scaleway://fr-par-1/99999999-9999-4999-8999-999999999999"
	if _, err := controllerRecoveryNodeIdentityPresent([]*k8sapi.Node{&drifted}, journal); err == nil {
		t.Fatal("recovery node bound to another Instance was accepted")
	}
}

func TestCheckpointRecoveryJournalClosesEveryPhase(t *testing.T) {
	plan := recoveryTestPlan(t)
	journal := newCheckpointRecoveryJournal(plan)
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(workload creating) error = %v", err)
	}
	journal.Phase = checkpointPhaseWorkloadReady
	journal.PersistentVolume = "pv-checkpoint"
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(workload ready) error = %v", err)
	}
	journal.Phase = checkpointPhasePreparing
	journal.ValuesPath = filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "checkpoint-release-values-"+plan.RunID+".yaml")
	journal.ValuesSHA256 = "sha256:" + strings.Repeat("c", 64)
	journal.CheckpointRequestID = "99999999-9999-4999-8999-999999999999"
	journal.ArchivePath = filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "checkpoint-"+journal.CheckpointRequestID+".tar")
	journal.OldInstanceIDs = []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(preparing) error = %v", err)
	}
	journal.Phase = checkpointPhasePrepared
	journal.ArchiveSHA256 = "sha256:" + strings.Repeat("a", 64)
	journal.ManifestSHA256 = "sha256:" + strings.Repeat("b", 64)
	journal.ArchiveBytes = 4096
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(prepared) error = %v", err)
	}
	journal.ArchivePath = filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "..", "foreign.tar")
	if err := journal.validate(plan); err == nil {
		t.Fatal("validate(out-of-scope archive) error = nil")
	}
}

func TestCheckpointRecoveryReplayRequiresDurablyPreparedArchive(t *testing.T) {
	for _, phase := range []string{
		checkpointPhaseWorkloadCreating,
		checkpointPhaseWorkloadReady,
		checkpointPhasePreparing,
	} {
		if checkpointRecoveryCanReplay(phase) {
			t.Fatalf("checkpointRecoveryCanReplay(%q) = true before the archive is durably prepared", phase)
		}
	}
	for _, phase := range []string{
		checkpointPhasePrepared,
		checkpointPhaseNamespaceDeleted,
		checkpointPhaseControllerRestored,
	} {
		if !checkpointRecoveryCanReplay(phase) {
			t.Fatalf("checkpointRecoveryCanReplay(%q) = false after durable preparation", phase)
		}
	}
}

func TestCheckpointRecoveryArtifactsRejectReplacement(t *testing.T) {
	plan := recoveryTestPlan(t)
	journal := newCheckpointRecoveryJournal(plan)
	journal.Phase = checkpointPhasePrepared
	journal.PersistentVolume = "pv-checkpoint"
	journal.ValuesPath = filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "checkpoint-release-values-"+plan.RunID+".yaml")
	journal.CheckpointRequestID = "99999999-9999-4999-8999-999999999999"
	journal.ArchivePath = filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "checkpoint-"+journal.CheckpointRequestID+".tar")
	journal.OldInstanceIDs = []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
	if err := os.MkdirAll(filepath.Dir(plan.CleanupInventoryPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal.ValuesPath, []byte("driver:\n  name: exact\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := []byte("exact checkpoint archive")
	if err := os.WriteFile(journal.ArchivePath, archive, 0o600); err != nil {
		t.Fatal(err)
	}
	var err error
	journal.ValuesSHA256, err = fileSHA256(journal.ValuesPath)
	if err != nil {
		t.Fatal(err)
	}
	journal.ArchiveSHA256, err = fileSHA256(journal.ArchivePath)
	if err != nil {
		t.Fatal(err)
	}
	journal.ManifestSHA256 = "sha256:" + strings.Repeat("b", 64)
	journal.ArchiveBytes = uint64(len(archive))
	if err := validateCheckpointRecoveryArtifacts(journal); err != nil {
		t.Fatalf("validateCheckpointRecoveryArtifacts() error = %v", err)
	}
	if err := os.WriteFile(journal.ValuesPath, []byte("driver:\n  name: replaced\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateCheckpointRecoveryArtifacts(journal); err == nil {
		t.Fatal("replaced checkpoint Helm values were accepted")
	}
}
