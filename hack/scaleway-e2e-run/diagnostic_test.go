package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestDiagnosticPhasesAreClosedAndOrdered(t *testing.T) {
	for _, phase := range []string{
		diagnosticPhaseDestructive,
		diagnosticPhaseMid,
		diagnosticPhaseRecovery,
		diagnosticPhasePost,
	} {
		if !validDiagnosticPhase(phase) {
			t.Fatalf("closed diagnostic phase %q was rejected", phase)
		}
	}
	for _, phase := range []string{"", "pre", "cleanup", "all", "destructive "} {
		if validDiagnosticPhase(phase) {
			t.Fatalf("unknown diagnostic phase %q was accepted", phase)
		}
	}
}

func TestDiagnosticPhaseRequiresExactScenarioNames(t *testing.T) {
	results := []e2erunner.ScenarioResult{
		{Name: "node-drain-and-replacement"},
		{Name: "controller-hard-failure"},
	}
	if err := validateDiagnosticScenarioNames(diagnosticPhaseDestructive, results); err != nil {
		t.Fatal(err)
	}
	results[0], results[1] = results[1], results[0]
	if err := validateDiagnosticScenarioNames(diagnosticPhaseDestructive, results); err == nil {
		t.Fatal("out-of-order diagnostic scenarios were accepted")
	}
	if err := validateDiagnosticScenarioNames(diagnosticPhaseMid, results[:1]); err == nil {
		t.Fatal("scenario from another diagnostic phase was accepted")
	}
}

func TestExactDiagnosticFilesRejectMissingOrAliasedEvidence(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"run-pre.json", "provider.json"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := requireExactDiagnosticFile(filepath.Join(directory, "run-pre.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(
		filepath.Join(directory, "provider.json"),
		filepath.Join(directory, "aliased.json"),
	); err != nil {
		t.Fatal(err)
	}
	if err := requireExactDiagnosticFile(filepath.Join(directory, "aliased.json")); err == nil {
		t.Fatal("aliased diagnostic prerequisite was accepted")
	}
	if err := requireExactDiagnosticFile(filepath.Join(directory, "missing.json")); err == nil {
		t.Fatal("missing diagnostic prerequisite was accepted")
	}
}

func TestDiagnosticFullRunPrefixRequiresExactGlobalOrder(t *testing.T) {
	results := make([]e2erunner.ScenarioResult, 10)
	for index := range results {
		results[index].Name = e2erunner.RequiredScenarios[index]
	}
	if err := validateDiagnosticFullRunPrefixNames(results, 10); err != nil {
		t.Fatal(err)
	}
	results[8], results[9] = results[9], results[8]
	if err := validateDiagnosticFullRunPrefixNames(results, 10); err == nil {
		t.Fatal("out-of-order full-run prefix was accepted")
	}
	if err := validateDiagnosticFullRunPrefixNames(results[:9], 10); err == nil {
		t.Fatal("incomplete full-run prefix was accepted")
	}
}

func TestLoadRetainedScenarioProofRehashesAndRejectsAlias(t *testing.T) {
	directory := t.TempDir()
	name := e2erunner.RequiredScenarios[0]
	path := filepath.Join(directory, name+".json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := loadRetainedScenarioProof(directory, name)
	if err != nil {
		t.Fatal(err)
	}
	if result.Name != name || result.EvidenceSHA == "" || len(result.Proof) == 0 || result.ProofSHA256 == "" {
		t.Fatalf("retained proof was not completely hydrated: %#v", result)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(directory, "target.json"), path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "target.json"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRetainedScenarioProof(directory, name); err == nil {
		t.Fatal("aliased retained scenario proof was accepted")
	}
}
