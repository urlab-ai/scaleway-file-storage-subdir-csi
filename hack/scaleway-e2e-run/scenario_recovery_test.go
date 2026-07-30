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
		Phase:               controllerRecoveryPhaseFreezeReady,
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
	journal.Phase = controllerRecoveryPhaseArmed
	action, err = controllerFreezeRecoveryActionFor(nil, &journal, false)
	if err != nil || action != controllerFreezeNoop {
		t.Fatalf("pre-freeze journal without injector action = %q, %v; want noop", action, err)
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
	journal.Phase = checkpointPhaseWorkloadStopped
	journal.WorkloadStoppedBeforeNamespaceDeletion = true
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(workload stopped) error = %v", err)
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
	journal.NodeRetirements = []checkpointNodeRetirement{
		{
			KapsuleNodeID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
			NodeName:      "worker-a",
			InstanceID:    journal.OldInstanceIDs[0],
			RootVolumeID:  "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		},
		{
			KapsuleNodeID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
			NodeName:      "worker-b",
			InstanceID:    journal.OldInstanceIDs[1],
			RootVolumeID:  "ffffffff-ffff-4fff-8fff-ffffffffffff",
		},
	}
	if err := journal.validate(plan); err != nil {
		t.Fatalf("validate(preparing) error = %v", err)
	}
	currentJournalWithLegacyAbsence := journal
	currentJournalWithLegacyAbsence.NodeRetirements = append(
		[]checkpointNodeRetirement(nil), journal.NodeRetirements...,
	)
	currentJournalWithLegacyAbsence.NodeRetirements[0] = checkpointNodeRetirement{
		InstanceID:    journal.OldInstanceIDs[0],
		AlreadyAbsent: true,
	}
	if err := currentJournalWithLegacyAbsence.validate(plan); err == nil {
		t.Fatal("current checkpoint journal accepted a legacy already-absent retirement")
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
		checkpointPhaseWorkloadStopped,
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

func TestCheckpointRecoveryRestoresNodePluginsImmediatelyAfterApproval(t *testing.T) {
	tests := []struct {
		file     string
		function string
	}{
		{"checkpoint_recovery.go", "func (backend *scalewayBackend) runCheckpointRecoveryScenarios("},
		{"checkpoint_recovery_journal.go", "func (backend *scalewayBackend) replayCheckpointForCleanup("},
	}
	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			encoded, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			contents := string(encoded)
			start := strings.Index(contents, test.function)
			if start < 0 {
				t.Fatalf("%s is missing", test.function)
			}
			body := contents[start:]
			if next := strings.Index(body[len(test.function):], "\nfunc "); next >= 0 {
				body = body[:len(test.function)+next]
			}
			approval := strings.Index(body, "createMissingLeaseApproval(")
			fullRelease := strings.Index(body, "installFullRecoveredRelease(")
			controllerReady := strings.Index(body, `"rollout", "status", "deployment"`)
			if approval < 0 || fullRelease < 0 || controllerReady < 0 ||
				approval >= fullRelease || fullRelease >= controllerReady {
				t.Fatalf("recovery order must be approval, full release, then controller readiness")
			}
			if strings.Count(body, "installFullRecoveredRelease(") != 1 {
				t.Fatalf("recovery must restore the full release exactly once")
			}
		})
	}
}

func TestMissingLeaseApprovalRechecksNodeDaemonSetAbsenceAtCreationBoundary(t *testing.T) {
	encoded, err := os.ReadFile("checkpoint_recovery.go")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "func (backend *scalewayBackend) createMissingLeaseApproval(")
	if start < 0 {
		t.Fatal("createMissingLeaseApproval is missing")
	}
	body := contents[start:]
	if next := strings.Index(body, "\nfunc "); next >= 0 {
		body = body[:next]
	}
	absence := strings.Index(body, "requireRecoveryNodeDaemonSetAbsent(")
	approvalIdentity := strings.Index(body, "randomUUIDv4()")
	create := strings.Index(body, `"create", "-f", "-"`)
	if absence < 0 || approvalIdentity < 0 || create < 0 ||
		absence >= approvalIdentity || absence >= create {
		t.Fatal("missing-Lease approval does not prove node DaemonSet absence immediately before creation")
	}
}

func TestCheckpointNodeRetirementsCloseExactOldInstanceSet(t *testing.T) {
	oldInstances := []string{
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}
	retirements := []checkpointNodeRetirement{
		{
			KapsuleNodeID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
			NodeName:      "worker-a",
			InstanceID:    oldInstances[0],
			RootVolumeID:  "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		},
		{
			KapsuleNodeID: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
			NodeName:      "worker-b",
			InstanceID:    oldInstances[1],
			RootVolumeID:  "ffffffff-ffff-4fff-8fff-ffffffffffff",
		},
	}
	if err := validateCheckpointNodeRetirements(oldInstances, retirements); err != nil {
		t.Fatalf("validate exact checkpoint retirement set: %v", err)
	}
	if err := validateCheckpointNodeRetirements(oldInstances, nil); err != nil {
		t.Fatalf("legacy checkpoint journal without retirement records: %v", err)
	}
	legacyRetirements := append([]checkpointNodeRetirement(nil), retirements...)
	legacyRetirements[0] = checkpointNodeRetirement{
		InstanceID:    oldInstances[0],
		AlreadyAbsent: true,
	}
	if err := validateCheckpointNodeRetirements(oldInstances, legacyRetirements); err != nil {
		t.Fatalf("legacy checkpoint journal with conclusive absence: %v", err)
	}
	invalidLegacy := append([]checkpointNodeRetirement(nil), legacyRetirements...)
	invalidLegacy[0].RootVolumeID = "99999999-9999-4999-8999-999999999999"
	if err := validateCheckpointNodeRetirements(oldInstances, invalidLegacy); err == nil {
		t.Fatal("already-absent retirement with invented root authority was accepted")
	}

	tests := map[string]func([]checkpointNodeRetirement) []checkpointNodeRetirement{
		"missing record": func(items []checkpointNodeRetirement) []checkpointNodeRetirement {
			return items[:1]
		},
		"foreign Instance": func(items []checkpointNodeRetirement) []checkpointNodeRetirement {
			items[0].InstanceID = "99999999-9999-4999-8999-999999999999"
			return items
		},
		"duplicate Kapsule node": func(items []checkpointNodeRetirement) []checkpointNodeRetirement {
			items[1].KapsuleNodeID = items[0].KapsuleNodeID
			return items
		},
		"duplicate root volume": func(items []checkpointNodeRetirement) []checkpointNodeRetirement {
			items[1].RootVolumeID = items[0].RootVolumeID
			return items
		},
		"invalid node name": func(items []checkpointNodeRetirement) []checkpointNodeRetirement {
			items[0].NodeName = "worker/a"
			return items
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := append([]checkpointNodeRetirement(nil), retirements...)
			if err := validateCheckpointNodeRetirements(oldInstances, mutate(changed)); err == nil {
				t.Fatal("unsafe checkpoint retirement records were accepted")
			}
		})
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
