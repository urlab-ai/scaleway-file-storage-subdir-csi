package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestReadProviderBootstrapRestartProofRequiresExactRegularStrictJSON(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "provider-bootstrap-restart.json")
	want := e2erunner.ProviderBootstrapRestartProof{ParentFilesystemID: "11111111-1111-4111-8111-111111111111"}
	encoded, err := canonicaljson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readProviderBootstrapRestartProof(directory)
	if err != nil || got.ParentFilesystemID != want.ParentFilesystemID {
		t.Fatalf("readProviderBootstrapRestartProof() = %#v, %v", got, err)
	}

	if err := os.WriteFile(path, []byte(`{"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readProviderBootstrapRestartProof(directory); err == nil {
		t.Fatal("readProviderBootstrapRestartProof(unknown field) error = nil")
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "target.json")
	if err := os.WriteFile(target, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if _, err := readProviderBootstrapRestartProof(directory); err == nil {
		t.Fatal("readProviderBootstrapRestartProof(symlink) error = nil")
	}
}

func TestBootstrapRestartScenarioProvesFreshParentBeforeAndAfterControllerRestart(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "bootstrap_restart_add_parent() {")
	end := strings.Index(contents, "\nscenario_scale() {")
	if start < 0 || end <= start {
		t.Fatal("bootstrap restart function boundary is missing")
	}
	body := contents[start:end]
	steps := []string{
		`number_of_attachments == 0`,
		`bootstrap_lease_uid=`,
		`helm_candidate "$bootstrap_parents"`,
		`bootstrap_claim_before=`,
		`bootstrap_lease_ready=`,
		`bootstrap_server_before=`,
		`rollout restart "$bootstrap_deployment"`,
		`controller rollout did not leave exactly one active Ready Pod`,
		`bootstrap_claim_after=`,
		`[ "$bootstrap_claim_after" = "$bootstrap_claim_before" ]`,
		`bootstrap_lease_after=`,
		`bootstrap_server_after=`,
	}
	previous := -1
	for _, step := range steps {
		index := strings.Index(body, step)
		if index <= previous {
			t.Fatalf("bootstrap restart step %q is absent or out of order", step)
		}
		previous = index
	}
	for _, forbidden := range []string{"hostPID:", "privileged:", "kill -STOP", "kill -KILL", "bootstrap_fault_pod"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("bootstrap restart retains timing-sensitive fault injection %q", forbidden)
		}
	}
	if strings.Count(body, `s file attachment list`) != 3 ||
		strings.Count(body, `findmnt -n -t virtiofs`) != 2 {
		t.Fatal("bootstrap restart does not prove provider and mount state before and after restart")
	}

	scaleStart := end + 1
	scaleEnd := strings.Index(contents[scaleStart:], "\nscenario_controller_restart_smoke() {")
	if scaleEnd < 0 {
		t.Fatal("scale scenario boundary is missing")
	}
	scale := contents[scaleStart : scaleStart+scaleEnd]
	if !strings.Contains(scale, "bootstrap_restart_add_parent") {
		t.Fatal("100-PVC scenario does not hand the still-fresh second parent to the bootstrap restart scenario")
	}
}

func TestScaleSoakRequiresTwoReadWritePeersPerSampledPVC(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	soakStart := strings.Index(contents, "run_scale_soak() {")
	scaleStart := strings.Index(contents, "\nscenario_scale() {")
	scaleEnd := strings.Index(contents, "\nscenario_controller_restart_smoke() {")
	if soakStart < 0 || scaleStart <= soakStart || scaleEnd <= scaleStart {
		t.Fatal("scale soak or scenario function boundary is missing")
	}
	soak := contents[soakStart:scaleStart]
	scale := contents[scaleStart:scaleEnd]

	for _, required := range []string{
		`own_prefix=$2`,
		`peer_prefix=$3`,
		`own_record=$4`,
		`peer_record=$5`,
		`own_ready=$6`,
		`peer_ready=$7`,
		`start_record=$8`,
		`start_identity=$9`,
		`mv "$ready_temporary" "$own_ready"`,
		`peer_ready_value=$(cat "$peer_ready"`,
		`observed_start=$(cat "$start_record"`,
		`mv "$temporary" "$own_record"`,
		`peer_value=$(cat "$peer_record"`,
		`case "$peer_payload" in`,
		`peer_sequence=${peer_payload#"$peer_prefix"}`,
		`last_peer_sequence=`,
		`[ "$peer_sequence" -gt "$last_peer_sequence" ]`,
		`active_record="$own_ready.active"`,
		`scale_soak_record_a="/data/soak-$short_run-record-a"`,
		`scale_soak_record_b="/data/soak-$short_run-record-b"`,
		`scale_soak_same_node_writes=`,
		`scale_soak_same_node_cross_reads=`,
		`scale_soak_peer_node_writes=`,
		`scale_soak_peer_node_cross_reads=`,
		`scale_soak_require_running`,
		`scale_soak_require_restart_window`,
		`scale_soak_prove_post_restart_io controller`,
		`scale_soak_prove_post_restart_io node-plugin`,
		`controller soak rollout did not leave exactly one active Ready Pod`,
	} {
		if !strings.Contains(soak, required) {
			t.Fatalf("scale soak is missing multi-writer invariant %q", required)
		}
	}
	if strings.Count(soak, `sh -c "$scale_soak_peer_command"`) != 2 {
		t.Fatal("scale soak does not start exactly two symmetric peers per sampled PVC")
	}
	if strings.Contains(soak, `mv "$temporary" /data/soak-record`) {
		t.Fatal("scale soak still makes both nodes contend on one application record")
	}
	ordered := []string{
		`scale_soak_start=$(date +%s)`,
		`"active I/O"`,
		`rollout restart "$scale_soak_controller"`,
		`scale_soak_prove_post_restart_io controller`,
		`delete "$scale_soak_node_pod_before"`,
		`scale_soak_prove_post_restart_io node-plugin`,
	}
	previous := -1
	for _, required := range ordered {
		index := strings.Index(soak, required)
		if index <= previous {
			t.Fatalf("scale soak recovery step %q is absent or out of order", required)
		}
		previous = index
	}
	controllerRollout := strings.Index(soak, `rollout status "$scale_soak_controller"`)
	controllerUID := strings.Index(soak, `scale_soak_controller_uid_after=`)
	if controllerRollout < 0 || controllerUID <= controllerRollout {
		t.Fatal("scale soak controller rollout proof boundary is missing")
	}
	controllerSelection := soak[controllerRollout:controllerUID]
	for _, required := range []string{
		`.metadata.deletionTimestamp == null`,
		`.type == "Ready" and .status == "True"`,
		`if length == 1 then "pod/" + .[0].metadata.name`,
	} {
		if !strings.Contains(controllerSelection, required) {
			t.Fatalf("scale soak controller post-rollout selection is missing %q", required)
		}
	}

	for _, required := range []string{
		`.spec.accessModes == ["ReadWriteMany"]`,
		`.spec.nodeName == $node`,
		`(.persistentVolumeClaim.readOnly // false) == false`,
		`(.readOnly // false) == false`,
		`multiWriterPairCount:10`,
		`multiWriterActivePairCount:10`,
		`multiWriterMountsReadWrite:$mounts_read_write`,
		`successfulWriterCount:20,successfulReaderCount:20`,
		`soakSameNodeWrites:$same_node_writes`,
		`soakSameNodeCrossReads:$same_node_cross_reads`,
		`soakPeerNodeWrites:$peer_node_writes`,
		`soakPeerNodeCrossReads:$peer_node_cross_reads`,
		`soakControllerRecoveryOffsetSeconds:$controller_recovery_offset`,
		`soakNodePluginRecoveryOffsetSeconds:$node_recovery_offset`,
		`soakControllerPostRestartReadWrite:$controller_post_restart_rw`,
		`soakNodePluginPostRestartReadWrite:$node_post_restart_rw`,
	} {
		if !strings.Contains(scale, required) {
			t.Fatalf("scale scenario is missing multi-writer proof %q", required)
		}
	}
}

func TestScaleSoakPeerCommandRunsTwoConcurrentWritersAndCrossReaders(t *testing.T) {
	peerCommand := scaleSoakPeerCommand(t)

	directory := t.TempDir()
	recordA := filepath.Join(directory, "record-a")
	recordB := filepath.Join(directory, "record-b")
	readyA := filepath.Join(directory, "ready-a")
	readyB := filepath.Join(directory, "ready-b")
	startRecord := filepath.Join(directory, "start")
	startIdentity := "soak-start"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type peerProcess struct {
		command *exec.Cmd
		stdout  bytes.Buffer
		stderr  bytes.Buffer
	}
	newPeer := func(ownPrefix, peerPrefix, ownRecord, peerRecord, ownReady, peerReady string) *peerProcess {
		process := &peerProcess{}
		process.command = exec.CommandContext(ctx, "sh", "-c", peerCommand, "sh", "5", ownPrefix, peerPrefix, ownRecord, peerRecord, ownReady, peerReady, startRecord, startIdentity)
		process.command.Stdout = &process.stdout
		process.command.Stderr = &process.stderr
		return process
	}
	peerA := newPeer("peer-a-", "peer-b-", recordA, recordB, readyA, readyB)
	peerB := newPeer("peer-b-", "peer-a-", recordB, recordA, readyB, readyA)
	if err := peerA.command.Start(); err != nil {
		t.Fatal(err)
	}
	if err := peerB.command.Start(); err != nil {
		_ = peerA.command.Process.Kill()
		t.Fatal(err)
	}
	readyDeadline := time.Now().Add(5 * time.Second)
	for {
		observedA, _ := os.ReadFile(readyA)
		observedB, _ := os.ReadFile(readyB)
		if string(observedA) == "peer-a-\n" && string(observedB) == "peer-b-\n" {
			break
		}
		if time.Now().After(readyDeadline) {
			_ = peerA.command.Process.Kill()
			_ = peerB.command.Process.Kill()
			t.Fatalf("peer readiness barrier was not reached: A=%q B=%q", observedA, observedB)
		}
		time.Sleep(10 * time.Millisecond)
	}
	startTemporary := startRecord + ".tmp"
	if err := os.WriteFile(startTemporary, []byte(startIdentity+"\n"), 0o600); err != nil {
		_ = peerA.command.Process.Kill()
		_ = peerB.command.Process.Kill()
		t.Fatal(err)
	}
	if err := os.Rename(startTemporary, startRecord); err != nil {
		_ = peerA.command.Process.Kill()
		_ = peerB.command.Process.Kill()
		t.Fatal(err)
	}
	waitA := peerA.command.Wait()
	waitB := peerB.command.Wait()
	if waitA != nil || waitB != nil {
		t.Fatalf("peer commands failed: A=%v stderr=%q; B=%v stderr=%q", waitA, peerA.stderr.String(), waitB, peerB.stderr.String())
	}
	for path, expected := range map[string]string{readyA + ".active": "peer-a-\n", readyB + ".active": "peer-b-\n"} {
		observed, err := os.ReadFile(path)
		if err != nil || string(observed) != expected {
			t.Fatalf("active acknowledgement %q = %q, %v; want %q", path, observed, err, expected)
		}
	}
	for name, output := range map[string]string{"A": peerA.stdout.String(), "B": peerB.stdout.String()} {
		writes, crossReads := parseSoakPeerResult(t, output)
		if writes < 1 || crossReads < 1 {
			t.Fatalf("peer %s result %q does not prove positive writes and cross-reads", name, output)
		}
	}
}

func TestScaleSoakPeerCommandDoesNotCountOneStaleRecordRepeatedly(t *testing.T) {
	peerCommand := scaleSoakPeerCommand(t)
	directory := t.TempDir()
	ownRecord := filepath.Join(directory, "record-a")
	peerRecord := filepath.Join(directory, "record-b")
	ownReady := filepath.Join(directory, "ready-a")
	peerReady := filepath.Join(directory, "ready-b")
	startRecord := filepath.Join(directory, "start")
	peerPayload := "peer-b-1"
	digest := sha256.Sum256([]byte(peerPayload))
	for path, contents := range map[string]string{
		peerRecord:  hex.EncodeToString(digest[:]) + " " + peerPayload + "\n",
		peerReady:   "peer-b-\n",
		startRecord: "soak-start\n",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "sh", "-c", peerCommand, "sh", "5", "peer-a-", "peer-b-", ownRecord, peerRecord, ownReady, peerReady, startRecord, "soak-start")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("single peer command failed: %v stderr=%q", err, stderr.String())
	}
	writes, crossReads := parseSoakPeerResult(t, string(output))
	if writes < 2 || crossReads != 1 {
		t.Fatalf("stale peer result = %d writes, %d distinct reads; want multiple writes and exactly one distinct read", writes, crossReads)
	}
}

func scaleSoakPeerCommand(t *testing.T) string {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Clean(filepath.Join("..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	startMarker := "  scale_soak_peer_command='\n"
	endMarker := "\n  '\n  scale_soak_cleanup() {"
	start := strings.Index(contents, startMarker)
	if start < 0 {
		t.Fatal("scale soak peer command start is missing")
	}
	start += len(startMarker)
	end := strings.Index(contents[start:], endMarker)
	if end < 0 {
		t.Fatal("scale soak peer command end is missing")
	}
	return contents[start : start+end]
}

func parseSoakPeerResult(t *testing.T, output string) (int, int) {
	t.Helper()
	fields := strings.Fields(output)
	if len(fields) != 2 {
		t.Fatalf("soak peer result %q does not contain exactly two counters", output)
	}
	writes, err := strconv.Atoi(fields[0])
	if err != nil {
		t.Fatalf("parse soak peer writes %q: %v", fields[0], err)
	}
	reads, err := strconv.Atoi(fields[1])
	if err != nil {
		t.Fatalf("parse soak peer reads %q: %v", fields[1], err)
	}
	return writes, reads
}

func TestNMinusOneUpgradeLeavesSecondParentFreshForBootstrapRestart(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "prepare_n_minus_one_upgrade() {")
	end := strings.Index(contents, "\nscenario_artifact_and_install() {")
	if start < 0 || end <= start {
		t.Fatal("N-1 upgrade function boundary is missing")
	}
	body := contents[start:end]
	if strings.Contains(body, "$parent_b") {
		t.Fatal("N-1 upgrade consumes the second parent reserved for bootstrap recovery")
	}
	if !strings.Contains(body, `upgrade_parents="[{\"id\":\"$parent_a\"`) {
		t.Fatal("N-1 upgrade does not retain the first-parent-only topology")
	}
	armed := strings.Index(body, "arm_n_minus_one_recovery")
	firstMutation := strings.Index(body, "apply_upgrade_storage_class")
	proofCommit := strings.Index(body, `mv "$upgrade_prepared.tmp" "$upgrade_prepared"`)
	disarmed := strings.Index(body, "disarm_n_minus_one_recovery")
	if armed < 0 || firstMutation < 0 || proofCommit < 0 || disarmed < 0 ||
		armed >= firstMutation || disarmed <= proofCommit {
		t.Fatal("N-1 transition is not durably armed before mutation and disarmed only after completed proof")
	}

	cleanupStart := strings.Index(contents, "cleanup_cluster() {")
	cleanupEnd := strings.Index(contents[cleanupStart:], "\nvalidate_bootstrap_abort_evidence() {")
	if cleanupStart < 0 || cleanupEnd < 0 {
		t.Fatal("cleanup function boundary is missing")
	}
	cleanup := contents[cleanupStart : cleanupStart+cleanupEnd]
	recoverTransition := strings.Index(cleanup, "recover_n_minus_one_transition")
	removeWorkloads := strings.Index(cleanup, "remove_test_workloads")
	if recoverTransition < 0 || removeWorkloads < 0 || recoverTransition >= removeWorkloads {
		t.Fatal("cleanup does not converge an interrupted N-1 transition before workload removal")
	}
}

func TestCleanupRecoversDisposableAttachmentsBeforeKubernetesUninstall(t *testing.T) {
	encoded, err := os.ReadFile("backend.go")
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "func (backend *scalewayBackend) Cleanup(")
	if start < 0 {
		t.Fatal("Cleanup function is missing")
	}
	body := contents[start:]
	recoverAttachments := strings.Index(body, "recoverDisposableInstanceAttachments")
	recoverCheckpoint := strings.Index(body, "recoverInterruptedCheckpoint")
	runCleanup := strings.Index(body, `runScenarioCommand(ctx, "cleanup"`)
	if recoverAttachments < 0 || recoverCheckpoint < 0 || runCleanup < 0 ||
		recoverAttachments >= recoverCheckpoint || recoverAttachments >= runCleanup {
		t.Fatal("cleanup does not detach the exact disposable Instance before Kubernetes recovery and safe uninstall")
	}
}
