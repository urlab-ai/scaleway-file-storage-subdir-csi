package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestScenarioScriptAcceptsEveryBackendPhase(t *testing.T) {
	working, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh"))
	for _, phase := range []string{"run-smoke", "run-pre", "run-mid", "run-post", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			command := exec.Command(script, phase)
			output, runErr := command.CombinedOutput()
			if runErr == nil {
				t.Fatal("scenario script unexpectedly accepted missing closed inputs")
			}
			message := string(output)
			if strings.Contains(message, "usage:") || !strings.Contains(message, "required Kapsule E2E value kubeconfig is empty") {
				t.Fatalf("scenario script rejected backend phase before closed-input validation: %s", message)
			}
		})
	}
}

func TestReleaseScenarioShellEmitsProofsInExecutionOrder(t *testing.T) {
	source, err := os.ReadFile(filepath.Clean(filepath.Join("..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	ordered := []string{
		"run_scenario artifact-and-install-preflight scenario_artifact_and_install",
		"run_scenario n-minus-one-upgrade scenario_upgrade",
		"run_scenario virtiofs-mount-api scenario_virtiofs",
		"run_scenario single-node-writer-conflict scenario_single_node_writer",
		"run_scenario one-hundred-pvc-scale scenario_scale",
		"run_scenario parent-decommission scenario_decommission",
		"run_scenario official-csi-coexistence scenario_official_coexistence",
		"run_scenario safe-uninstall scenario_safe_uninstall",
	}
	previous := -1
	for _, statement := range ordered {
		index := strings.LastIndex(text, statement)
		if index <= previous {
			t.Fatalf("scenario statement %q is missing or out of order", statement)
		}
		previous = index
	}
}

func TestUpgradeProofAdmissionDoesNotDependOnLaterVirtioFSWorkload(t *testing.T) {
	source, err := os.ReadFile(filepath.Clean(filepath.Join("..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "scenario_upgrade() {")
	end := strings.Index(text, "\nremove_test_workloads() {")
	if start == -1 || end <= start {
		t.Fatal("scenario_upgrade function is absent or ambiguous")
	}
	body := text[start:end]
	if strings.Contains(body, "e2e-smoke-") {
		t.Fatal("N-1 proof admission depends on the later virtiofs smoke workload")
	}
	for _, required := range []string{
		`prepared="$evidence_dir/.n-minus-one-upgrade-prepared.json"`,
		`test -s "$prepared"`,
		`h history "$release" -n "$namespace" | grep -q deployed`,
		`cp "$prepared" "$proof.tmp"`,
	} {
		if !strings.Contains(body, required) {
			t.Fatalf("scenario_upgrade is missing required proof admission step %q", required)
		}
	}
}
