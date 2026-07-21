package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
		"--profile=base", "--region=fr-par",
		"--cluster-created-by-run=true",
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

func TestScenarioRunnerDoesNotSuppressShellErrexit(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	encoded, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	if strings.Contains(contents, `if ! "$scenario_runner_function"`) || !strings.Contains(contents, `"$scenario_runner_function" >"$scenario_runner_evidence" 2>&1`) {
		t.Fatal("scenario functions must run as simple commands so set -e remains effective inside them")
	}
}

func TestScenarioRunnerIdentitySurvivesScenarioAssignments(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is required for the checked-in scenario script")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	encoded, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "run_scenario() {")
	if start < 0 {
		t.Fatal("scenario runner function is missing")
	}
	end := strings.Index(contents[start:], "\n}\n\ncleanup_cluster()")
	if end < 0 {
		t.Fatal("scenario runner function boundary is missing")
	}
	runScenario := contents[start : start+end+2]

	temporary := t.TempDir()
	evidence := filepath.Join(temporary, "evidence")
	if err := os.Mkdir(evidence, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := filepath.Join(evidence, "results.ndjson")
	const digest = "0000000000000000000000000000000000000000000000000000000000000000"
	writeExecutable(t, temporary, "sha256sum", "#!/bin/sh\nprintf '%s  %s\\n' '"+digest+"' \"$1\"\n")
	harness := `set -eu
JQ=$1
evidence_dir=$2
entries=$3
` + runScenario + `
scenario_clobbers_generic_name() {
  name=resource-name
  printf '%s\n' scenario-proof
}
run_scenario expected-scenario scenario_clobbers_generic_name
`
	command := exec.Command("sh", "-c", harness, "scenario-runner-test", jq, evidence, entries)
	command.Env = append(os.Environ(), "PATH="+temporary+string(os.PathListSeparator)+os.Getenv("PATH"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("scenario runner failed: %v, output = %s", err, output)
	}

	encodedResult, err := os.ReadFile(entries)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Name         string `json:"name"`
		Succeeded    bool   `json:"succeeded"`
		EvidenceFile string `json:"evidenceFile"`
		EvidenceSHA  string `json:"evidenceSha256"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(encodedResult))), &result); err != nil {
		t.Fatalf("decode scenario result: %v", err)
	}
	if result.Name != "expected-scenario" || !result.Succeeded || result.EvidenceFile != "expected-scenario.log" || result.EvidenceSHA != "sha256:"+digest {
		t.Fatalf("scenario result = %+v", result)
	}
	proof, err := os.ReadFile(filepath.Join(evidence, "expected-scenario.log"))
	if err != nil || string(proof) != "scenario-proof\n" {
		t.Fatalf("scenario evidence = %q, %v", proof, err)
	}
}

func TestPVCCountFilterKeepsTheKubernetesListAsInput(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is required for the checked-in scenario script")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	encoded, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	const filter = `[ (.items | length), ([.items[] | select(.status.phase == "Bound")] | length) ] | @tsv`
	const brokenFilter = `[.items | length, [.items[] | select(.status.phase == "Bound")] | length] | @tsv`
	contents := string(encoded)
	if strings.Contains(contents, brokenFilter) {
		t.Fatal("scenario script still contains the broken PVC count filter")
	}
	if count := strings.Count(contents, `"$JQ" -r '`+filter+`'`); count != 2 {
		t.Fatalf("scenario script contains %d regression-tested PVC count filters, want 2", count)
	}
	fixture := `{"items":[{"status":{"phase":"Bound"}},{"status":{"phase":"Pending"}},{"status":{"phase":"Bound"}}]}`
	command := exec.Command(jq, "-r", filter)
	command.Stdin = strings.NewReader(fixture)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("PVC count filter error = %v, output = %s", err, output)
	}
	if string(output) != "3\t2\n" {
		t.Fatalf("PVC count output = %q, want %q", output, "3\\t2\\n")
	}
}

func TestE2ECleanupAllocationInventoryFailsClosed(t *testing.T) {
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Skip("jq is required for the checked-in cleanup script")
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	encoded, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "read_test_allocations() {")
	if start < 0 {
		t.Fatal("E2E allocation inventory function is missing")
	}
	end := strings.Index(contents[start:], "\n}\n\ngc_test_allocations()")
	if end < 0 {
		t.Fatal("E2E allocation inventory function boundary is missing")
	}
	readAllocations := contents[start : start+end+2]

	const (
		runID     = "11111111-1111-4111-8111-111111111111"
		logicalID = "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		parentA   = "77777777-7777-4777-8777-777777777777"
		parentB   = "88888888-8888-4888-8888-888888888888"
	)
	for _, test := range []struct {
		name           string
		installationID string
		state          string
		parentID       string
		wantSuccess    bool
	}{
		{name: "exact archived allocation", installationID: runID, state: "Archived", parentID: parentA, wantSuccess: true},
		{name: "foreign installation", installationID: "22222222-2222-4222-8222-222222222222", state: "Archived", parentID: parentA},
		{name: "active allocation", installationID: runID, state: "Ready", parentID: parentA},
		{name: "foreign parent", installationID: runID, state: "Archived", parentID: "99999999-9999-4999-8999-999999999999"},
	} {
		t.Run(test.name, func(t *testing.T) {
			record, err := json.Marshal(map[string]string{
				"logicalVolumeID": logicalID, "state": test.state, "parentFilesystemID": test.parentID,
				"createVolumeRequestName": "pvc-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			})
			if err != nil {
				t.Fatal(err)
			}
			fixture, err := json.Marshal(map[string]any{"items": []any{map[string]any{
				"metadata": map[string]any{"labels": map[string]string{
					"file-storage-subdir.csi.urlab.ai/installation-id":   test.installationID,
					"file-storage-subdir.csi.urlab.ai/logical-volume-id": logicalID,
				}},
				"data": map[string]string{"record.json": string(record)},
			}}})
			if err != nil {
				t.Fatal(err)
			}
			temporary := t.TempDir()
			fixturePath := filepath.Join(temporary, "configmaps.json")
			if err := os.WriteFile(fixturePath, fixture, 0o600); err != nil {
				t.Fatal(err)
			}
			harness := `set -eu
JQ=$1
fixture=$2
namespace=driver-system
run_id=` + runID + `
parent_a=` + parentA + `
parent_b=` + parentB + `
k() { cat "$fixture"; }
` + readAllocations + `
read_test_allocations
`
			command := exec.Command("sh", "-c", harness, "allocation-inventory-test", jq, fixturePath)
			output, runErr := command.CombinedOutput()
			if test.wantSuccess {
				if runErr != nil || !strings.Contains(string(output), logicalID) {
					t.Fatalf("valid inventory error = %v, output = %s", runErr, output)
				}
			} else if runErr == nil {
				t.Fatalf("unsafe inventory succeeded: %s", output)
			}
		})
	}
}

func TestScenarioCredentialSecretIsStreamedAndNotPersisted(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	temporary := t.TempDir()
	evidence := filepath.Join(temporary, "evidence")
	streamProof := filepath.Join(temporary, "stream-proof")
	argumentLeak := filepath.Join(temporary, "argument-leak")
	environmentLeak := filepath.Join(temporary, "environment-leak")

	kubectl := writeExecutable(t, temporary, "kubectl", `#!/bin/sh
for argument in "$@"; do
  case "$argument" in
    *SCWTESTACCESSFIXTURE*|*test-secret-fixture*) : >"$ARGUMENT_LEAK" ;;
  esac
done
if [ "${SCW_ACCESS_KEY+x}" = x ] || [ "${SCW_SECRET_KEY+x}" = x ]; then
  : >"$ENVIRONMENT_LEAK"
fi
case "$*" in
  "get namespace driver-system") exit 1 ;;
  "create namespace driver-system") exit 0 ;;
  "label namespace driver-system "*) exit 0 ;;
  *"create secret generic scaleway-sfs-subdir-csi-credentials --from-env-file=/dev/stdin --dry-run=client -o yaml")
    streamed=$(cat)
    expected='SCW_ACCESS_KEY=SCWTESTACCESSFIXTURE
SCW_SECRET_KEY=test-secret-fixture'
    [ "$streamed" = "$expected" ] || exit 91
    printf '%s\n' streamed >"$STREAM_PROOF"
    printf '%s\n' 'apiVersion: v1' 'kind: Secret' 'metadata:' '  name: scaleway-sfs-subdir-csi-credentials'
    ;;
  "apply -f -")
    applied=$(cat)
    case "$applied" in
      *"name: scaleway-sfs-subdir-csi-credentials"*) exit 0 ;;
      *) exit 92 ;;
    esac
    ;;
  *) exit 93 ;;
esac
`)
	admin := writeExecutable(t, temporary, "csi-admin", "#!/bin/sh\n[ \"$1\" = version ]\n")
	unused := writeExecutable(t, temporary, "unused", "#!/bin/sh\nexit 99\n")
	results := filepath.Join(temporary, "results.json")
	command := exec.Command(script, "run-smoke",
		"--kubeconfig=/tmp/kubeconfig", "--chart=/tmp/chart.tgz", "--values=/tmp/values.yaml",
		"--namespace=driver-system", "--release=driver", "--admin="+admin,
		"--workload-image=example.invalid/workload@sha256:"+strings.Repeat("a", 64), "--profile=base",
		"--project-id=22222222-2222-4222-8222-222222222222", "--region=fr-par",
		"--run-id=11111111-1111-4111-8111-111111111111", "--cluster-id=33333333-3333-4333-8333-333333333333",
		"--parent-a=77777777-7777-4777-8777-777777777777", "--parent-b=88888888-8888-4888-8888-888888888888",
		"--results="+results, "--evidence-dir="+evidence)
	command.Env = append(environmentWithoutScalewayCredentials(),
		"SCW_ACCESS_KEY=SCWTESTACCESSFIXTURE", // gitleaks:allow -- non-secret test fixture.
		"SCW_SECRET_KEY=test-secret-fixture",  // gitleaks:allow -- non-secret test fixture.
		"KUBECTL="+kubectl, "HELM="+unused, "JQ="+unused, "SCW="+unused,
		"STREAM_PROOF="+streamProof, "ARGUMENT_LEAK="+argumentLeak, "ENVIRONMENT_LEAK="+environmentLeak)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("scenario unexpectedly continued after the intentional identity-Secret failure: %s", output)
	}
	if proof, err := os.ReadFile(streamProof); err != nil || string(proof) != "streamed\n" {
		t.Fatalf("credential stdin proof = %q, %v", proof, err)
	}
	if _, err := os.Stat(argumentLeak); !os.IsNotExist(err) {
		t.Fatalf("credential appeared in a kubectl process argument: %v", err)
	}
	if _, err := os.Stat(environmentLeak); !os.IsNotExist(err) {
		t.Fatalf("kubectl inherited Scaleway credentials: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(evidence, ".credentials.*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("persistent credential files = %v, %v", matches, err)
	}
	err = filepath.WalkDir(evidence, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(contents), "SCWTESTACCESSFIXTURE") || strings.Contains(string(contents), "test-secret-fixture") {
			return fmt.Errorf("credential fixture persisted in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestChildToolEnvironmentOmitsScalewayCredentials(t *testing.T) {
	t.Setenv("SCW_ACCESS_KEY", "SCWTESTACCESSFIXTURE") // gitleaks:allow -- non-secret test fixture.
	t.Setenv("SCW_SECRET_KEY", "test-secret-fixture")  // gitleaks:allow -- non-secret test fixture.
	t.Setenv("SFS_SUBDIR_E2E_ENVIRONMENT_FIXTURE", "retained-fixture")

	retained := false
	for _, entry := range environmentWithoutScalewayCredentials() {
		name, value, _ := strings.Cut(entry, "=")
		if name == "SCW_ACCESS_KEY" || name == "SCW_SECRET_KEY" {
			t.Fatalf("credential environment entry survived filtering: %s", name)
		}
		if name == "SFS_SUBDIR_E2E_ENVIRONMENT_FIXTURE" && value == "retained-fixture" {
			retained = true
		}
	}
	if !retained {
		t.Fatal("environment filtering removed an unrelated entry")
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
	allocationState := filepath.Join(temporary, "allocation-state")
	if err := os.WriteFile(helmState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(namespaceState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(allocationState, []byte("Archived\n"), 0o600); err != nil {
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
  status) printf '%s\n' '{"name":"driver","info":{"status":"deployed"}}' ;;
  uninstall) printf '%s\n' absent >"$HELM_STATE" ;;
  *) exit 91 ;;
esac
`)
	kubectl := writeExecutable(t, temporary, "kubectl", `#!/bin/sh
case "$*" in
  "get namespace driver-system -o json") printf '%s\n' '{"metadata":{"labels":{"sfs-subdir-e2e-run":"11111111-1111-4111-8111-111111111111"}}}' ;;
  "-n driver-system get secret scaleway-sfs-subdir-csi-identity -o jsonpath={.data.installationID}") printf '%s' 'MTExMTExMTEtMTExMS00MTExLTgxMTEtMTExMTExMTExMTEx' ;;
  "-n driver-system get configmaps -l app.kubernetes.io/name=scaleway-sfs-subdir-csi -o json")
    state=$(sed -n '1p' "$ALLOCATION_STATE")
    printf '%s\n' "{\"items\":[{\"metadata\":{\"labels\":{\"file-storage-subdir.csi.urlab.ai/installation-id\":\"11111111-1111-4111-8111-111111111111\",\"file-storage-subdir.csi.urlab.ai/logical-volume-id\":\"lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}},\"data\":{\"record.json\":\"{\\\"logicalVolumeID\\\":\\\"lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\\",\\\"state\\\":\\\"$state\\\",\\\"parentFilesystemID\\\":\\\"77777777-7777-4777-8777-777777777777\\\",\\\"createVolumeRequestName\\\":\\\"pvc-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\\\"}\"}}]}"
    ;;
  *"get pods -l sfs-subdir-e2e-run=11111111-1111-4111-8111-111111111111 -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get pvc -o json"*) printf '%s\n' '{"items":[]}' ;;
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
case "$1" in
  gc)
    for argument in "$@"; do
      case "$argument" in
        --request-id=*) request=${argument#*=} ;;
        --logical-volume-id=*) logical=${argument#*=} ;;
        --mode=*) mode=${argument#*=} ;;
        --expected-state=*) expected=${argument#*=} ;;
      esac
    done
    if [ "$mode" = dry-run ]; then
      printf '{"requestID":"%s","mode":"dry-run","logicalVolumeID":"%s","parentFilesystemID":"77777777-7777-4777-8777-777777777777","previousState":"%s","finalState":"%s","targetPath":"/archive","completed":false}\n' "$request" "$logical" "$expected" "$expected"
    elif [ "$mode" = execute ]; then
      printf '%s\n' Deleted >"$ALLOCATION_STATE"
      printf '{"requestID":"%s","mode":"execute","logicalVolumeID":"%s","parentFilesystemID":"77777777-7777-4777-8777-777777777777","previousState":"%s","finalState":"Deleted","targetPath":"/archive","quarantinePath":"/quarantine","completed":true}\n' "$request" "$logical" "$expected"
    else
      exit 93
    fi
    ;;
  uninstall)
    case "$*" in
      *"--mode=dry-run"*) printf '%s\n' '{"ready":true,"completed":false,"blockers":[]}' ;;
      *"--mode=execute"*) printf '%s\n' '{"ready":true,"completed":true,"blockers":[],"audit":{}}' ;;
      *) exit 94 ;;
    esac
    ;;
  *) exit 95 ;;
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
		"--profile=base", "--region=fr-par",
		"--cluster-created-by-run=true",
		"--run-id=11111111-1111-4111-8111-111111111111",
		"--parent-a=77777777-7777-4777-8777-777777777777", "--parent-b=88888888-8888-4888-8888-888888888888",
		"--evidence-dir="+evidence, "--preconditions="+preconditions)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState, "ALLOCATION_STATE="+allocationState)
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
	if len(observed) != 14 {
		t.Fatalf("cleanup precondition count = %d, want 14", len(observed))
	}
	for name, value := range observed {
		if name == "bootstrapAbortComplete" {
			if value {
				t.Fatal("normal safe uninstall reported bootstrap-abort completion")
			}
			continue
		}
		if !value {
			t.Fatalf("cleanup precondition %q is false", name)
		}
	}
	if state, err := os.ReadFile(allocationState); err != nil || string(state) != "Deleted\n" {
		t.Fatalf("cleanup GC state = %q, %v", state, err)
	}
	for _, name := range []string{
		"cleanup-gc-plan-11111111-1111-4111-8111-111111111111.json",
		"gc-lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-dry-run.json",
		"gc-lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-execute.json",
	} {
		if info, err := os.Stat(filepath.Join(evidence, name)); err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("cleanup GC evidence %q info = %v, %v", name, info, err)
		}
	}
}

func TestCleanupScriptUsesBoundedBootstrapAbortAfterFailedFirstInstall(t *testing.T) {
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
	attachmentState := filepath.Join(temporary, "attachment-state")
	if err := os.WriteFile(attachmentState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pvcState := filepath.Join(temporary, "pvc-state")
	if err := os.WriteFile(pvcState, []byte("absent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	helm := writeExecutable(t, temporary, "helm", `#!/bin/sh
case "$1" in
  list)
    if [ "$(sed -n '1p' "$HELM_STATE")" = present ]; then
      printf '%s\n' '[{"name":"driver","status":"failed"}]'
    else
      printf '%s\n' '[]'
    fi
    ;;
  status) printf '%s\n' '{"info":{"status":"failed"}}' ;;
  get) printf '%s\n' '{"driver":{"name":"csi.example.test"}}' ;;
  uninstall) printf '%s\n' absent >"$HELM_STATE" ;;
  *) exit 91 ;;
esac
`)
	kubectl := writeExecutable(t, temporary, "kubectl", `#!/bin/sh
case "$*" in
  *"get namespace driver-system -o json"*) printf '%s\n' '{"metadata":{"labels":{"sfs-subdir-e2e-run":"11111111-1111-4111-8111-111111111111"}}}' ;;
  *"get namespace"*"--ignore-not-found"*) ;;
  *"get pods -l sfs-subdir-e2e-run=11111111-1111-4111-8111-111111111111 -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get pvc -o json"*)
    if [ "$(sed -n '1p' "$PVC_STATE")" = present ]; then printf '%s\n' '{"items":[{"metadata":{"name":"unexpected"}}]}'; else printf '%s\n' '{"items":[]}'; fi
    ;;
  *"get pv -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get volumeattachments -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get csinodes -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"get configmaps -o json"*) printf '%s\n' '{"items":[]}' ;;
  *"delete namespace"*) printf '%s\n' absent >"$NAMESPACE_STATE" ;;
  *"delete pod,pvc"*) printf '%s\n' absent >"$PVC_STATE" ;;
  *) exit 92 ;;
esac
`)
	admin := writeExecutable(t, temporary, "csi-admin-unavailable", "#!/bin/sh\nexit 1\n")
	validator := writeExecutable(t, temporary, "validator-unused", "#!/bin/sh\nexit 99\n")
	scw := writeExecutable(t, temporary, "scw", `#!/bin/sh
case "$*" in
  "file attachment list region=fr-par filesystem-id=77777777-7777-4777-8777-777777777777 -o json")
    if [ "$(sed -n '1p' "$ATTACHMENT_STATE")" = present ]; then printf '%s\n' '[{}]'; else printf '%s\n' '[]'; fi
    ;;
  "file attachment list region=fr-par filesystem-id=88888888-8888-4888-8888-888888888888 -o json") printf '%s\n' '[]' ;;
  "file filesystem get 77777777-7777-4777-8777-777777777777 region=fr-par -o json"|"file filesystem get 88888888-8888-4888-8888-888888888888 region=fr-par -o json") printf '%s\n' '{"number_of_attachments":0}' ;;
  *) exit 95 ;;
esac
`)
	evidence := filepath.Join(temporary, "evidence")
	if err := os.MkdirAll(evidence, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidence, ".scenario-results-run-smoke.ndjson"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	preconditions := filepath.Join(temporary, "preconditions.json")
	arguments := []string{"cleanup", "--kubeconfig=/tmp/kubeconfig", "--namespace=driver-system", "--release=driver",
		"--admin=" + admin, "--validator=" + validator, "--profile=base", "--region=fr-par", "--cluster-created-by-run=true",
		"--run-id=11111111-1111-4111-8111-111111111111",
		"--parent-a=77777777-7777-4777-8777-777777777777", "--parent-b=88888888-8888-4888-8888-888888888888",
		"--evidence-dir=" + evidence, "--preconditions=" + preconditions}
	reusedArguments := slices.Clone(arguments)
	for index := range reusedArguments {
		if reusedArguments[index] == "--cluster-created-by-run=true" {
			reusedArguments[index] = "--cluster-created-by-run=false"
		}
	}
	command := exec.Command(script, reusedArguments...)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "SCW="+scw, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState, "ATTACHMENT_STATE="+attachmentState, "PVC_STATE="+pvcState)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("bootstrap cleanup accepted a reused cluster: %s", output)
	}
	if state, err := os.ReadFile(helmState); err != nil || string(state) != "present\n" {
		t.Fatalf("reused-cluster bootstrap cleanup changed Helm state: %q, %v", state, err)
	}
	command = exec.Command(script, arguments...)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "SCW="+scw, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState, "ATTACHMENT_STATE="+attachmentState, "PVC_STATE="+pvcState)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("bootstrap cleanup accepted a provider attachment: %s", output)
	}
	if _, err := os.Stat(preconditions); !os.IsNotExist(err) {
		t.Fatalf("blocked bootstrap cleanup wrote preconditions: %v", err)
	}
	if state, err := os.ReadFile(helmState); err != nil || string(state) != "present\n" {
		t.Fatalf("blocked bootstrap cleanup changed Helm state: %q, %v", state, err)
	}
	if err := os.WriteFile(attachmentState, []byte("absent\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pvcState, []byte("present\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command = exec.Command(script, arguments...)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "SCW="+scw, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState, "ATTACHMENT_STATE="+attachmentState, "PVC_STATE="+pvcState)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("bootstrap cleanup accepted a pre-cleanup PVC: %s", output)
	}
	if _, err := os.Stat(preconditions); !os.IsNotExist(err) {
		t.Fatalf("PVC-blocked bootstrap cleanup wrote preconditions: %v", err)
	}
	command = exec.Command(script, arguments...)
	command.Env = append(os.Environ(), "JQ="+jq, "HELM="+helm, "KUBECTL="+kubectl, "SCW="+scw, "HELM_STATE="+helmState, "NAMESPACE_STATE="+namespaceState, "ATTACHMENT_STATE="+attachmentState, "PVC_STATE="+pvcState)
	if output, err := command.CombinedOutput(); err != nil {
		cleanupLog, _ := os.ReadFile(filepath.Join(evidence, "cleanup-kubernetes.log"))
		bootstrapEvidence, _ := os.ReadFile(filepath.Join(evidence, "bootstrap-abort-cleanup-11111111-1111-4111-8111-111111111111.json"))
		files, _ := os.ReadDir(evidence)
		t.Fatalf("bootstrap cleanup error = %v, output = %s, cleanup log = %s, bootstrap evidence = %s, files = %v", err, output, cleanupLog, bootstrapEvidence, files)
	}
	encoded, err := os.ReadFile(preconditions)
	if err != nil {
		t.Fatal(err)
	}
	var observed e2ecleanup.Preconditions
	if err := json.Unmarshal(encoded, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.UninstallPrepareComplete || !observed.BootstrapAbortComplete || !observed.HelmUninstalled || !observed.ParentAttachmentsAbsent {
		t.Fatalf("bootstrap cleanup preconditions = %#v", observed)
	}
	if _, err := os.Stat(filepath.Join(evidence, "bootstrap-abort-cleanup-11111111-1111-4111-8111-111111111111.json")); err != nil {
		t.Fatalf("bootstrap cleanup evidence: %v", err)
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

func TestRealE2EClientDropsEnvironmentDefaultZone(t *testing.T) {
	projectID := "22222222-2222-4222-8222-222222222222"
	t.Setenv("SCW_ACCESS_KEY", "SCW1234567890ABCDEFG")                 // gitleaks:allow -- syntactically valid SDK fixture.
	t.Setenv("SCW_SECRET_KEY", "7363616c-6577-6573-6862-6f7579616161") // gitleaks:allow -- non-secret test fixture.
	t.Setenv("SCW_DEFAULT_PROJECT_ID", projectID)
	t.Setenv("SCW_DEFAULT_REGION", "fr-par")
	t.Setenv("SCW_DEFAULT_ZONE", "fr-par-2")

	client, err := newRegionalScalewayClientFromEnvironment(e2eplan.Plan{ProjectID: projectID, Region: "fr-par"})
	if err != nil {
		t.Fatalf("newRegionalScalewayClientFromEnvironment() error = %v", err)
	}
	if _, present := client.GetDefaultZone(); present {
		t.Fatal("regional E2E client inherited SCW_DEFAULT_ZONE")
	}
	if region, present := client.GetDefaultRegion(); !present || region.String() != "fr-par" {
		t.Fatalf("regional E2E client region = %q, %t", region, present)
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

func TestProviderAttachmentEvidenceIsBoundedByParentsAndNodes(t *testing.T) {
	instanceA := "11111111-1111-4111-8111-111111111111"
	instanceB := "22222222-2222-4222-8222-222222222222"
	evidence := providerAttachmentEvidence{PlannedNodeIDs: []string{"fr-par-1/" + instanceA, "fr-par-1/" + instanceB}, Parents: []providerParentAttachment{
		{FilesystemID: "parent-a", FilesystemStatus: "available", ReportedAttachments: 2,
			AttachmentIDs: []string{"attachment-a-1", "attachment-a-2"}, ResourceIDs: []string{instanceA, instanceB},
			ResourceTypes: []string{"instance_server", "instance_server"}, Zones: []string{"fr-par-1", "fr-par-1"}},
		{FilesystemID: "parent-b", FilesystemStatus: "available", ReportedAttachments: 2,
			AttachmentIDs: []string{"attachment-b-1", "attachment-b-2"}, ResourceIDs: []string{instanceA, instanceB},
			ResourceTypes: []string{"instance_server", "instance_server"}, Zones: []string{"fr-par-1", "fr-par-1"}},
	}}
	if err := validateProviderAttachmentEvidence(evidence, "fr-par-1", 2, true); err != nil {
		t.Fatalf("validateProviderAttachmentEvidence() error = %v", err)
	}

	duplicateResource := evidence
	duplicateResource.Parents = append([]providerParentAttachment(nil), evidence.Parents...)
	duplicateResource.Parents[0].ResourceIDs = []string{instanceA, instanceA}
	if err := validateProviderAttachmentEvidence(duplicateResource, "fr-par-1", 2, true); err == nil {
		t.Fatal("validateProviderAttachmentEvidence(duplicate parent attachment) error = nil")
	}

	wrongZone := evidence
	wrongZone.Parents = append([]providerParentAttachment(nil), evidence.Parents...)
	wrongZone.Parents[0].Zones = []string{"fr-par-2", "fr-par-1"}
	if err := validateProviderAttachmentEvidence(wrongZone, "fr-par-1", 2, true); err == nil {
		t.Fatal("validateProviderAttachmentEvidence(wrong zone) error = nil")
	}

	foreignResource := evidence
	foreignResource.Parents = append([]providerParentAttachment(nil), evidence.Parents...)
	foreignResource.Parents[0].ResourceIDs = []string{instanceA, "33333333-3333-4333-8333-333333333333"}
	if err := validateProviderAttachmentEvidence(foreignResource, "fr-par-1", 2, true); err == nil {
		t.Fatal("validateProviderAttachmentEvidence(foreign Instance) error = nil")
	}
}

func TestDecodePlannedCSINodeIDsBindsExactPoolNamesToDriver(t *testing.T) {
	driverObjects := []byte(`{"items":[{"metadata":{"name":"csi.example.test"}}]}`)
	csiNodeObjects := []byte(`{"items":[
		{"metadata":{"name":"planned-a"},"spec":{"drivers":[{"name":"official.example.test","nodeID":"foreign"},{"name":"csi.example.test","nodeID":"fr-par-1/11111111-1111-4111-8111-111111111111"}]}},
		{"metadata":{"name":"planned-b"},"spec":{"drivers":[{"name":"csi.example.test","nodeID":"fr-par-1/22222222-2222-4222-8222-222222222222"}]}},
		{"metadata":{"name":"other-pool"},"spec":{"drivers":[{"name":"csi.example.test","nodeID":"fr-par-1/33333333-3333-4333-8333-333333333333"}]}}
	]}`)
	plannedNames := map[string]struct{}{"planned-a": {}, "planned-b": {}}
	nodeIDs, err := decodePlannedCSINodeIDs(driverObjects, csiNodeObjects, plannedNames)
	if err != nil {
		t.Fatalf("decodePlannedCSINodeIDs() error = %v", err)
	}
	want := []string{
		"fr-par-1/11111111-1111-4111-8111-111111111111",
		"fr-par-1/22222222-2222-4222-8222-222222222222",
	}
	if !slices.Equal(nodeIDs, want) {
		t.Fatalf("decodePlannedCSINodeIDs() = %v, want %v", nodeIDs, want)
	}

	if _, err := decodePlannedCSINodeIDs(driverObjects, []byte(`{"items":[{"metadata":{"name":"planned-a"},"spec":{"drivers":[]}}]}`), plannedNames); err == nil {
		t.Fatal("decodePlannedCSINodeIDs(missing planned registration) error = nil")
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
