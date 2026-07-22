package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestReadProviderBootstrapCrashProofRequiresExactRegularStrictJSON(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "provider-bootstrap-crash.json")
	want := e2erunner.ProviderBootstrapCrashProof{ParentFilesystemID: "11111111-1111-4111-8111-111111111111"}
	encoded, err := canonicaljson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readProviderBootstrapCrashProof(directory)
	if err != nil || got.ParentFilesystemID != want.ParentFilesystemID {
		t.Fatalf("readProviderBootstrapCrashProof() = %#v, %v", got, err)
	}

	if err := os.WriteFile(path, []byte(`{"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readProviderBootstrapCrashProof(directory); err == nil {
		t.Fatal("readProviderBootstrapCrashProof(unknown field) error = nil")
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
	if _, err := readProviderBootstrapCrashProof(directory); err == nil {
		t.Fatal("readProviderBootstrapCrashProof(symlink) error = nil")
	}
}

func TestBootstrapCrashScenarioOrdersRealAttachBeforeSamePodRestart(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Clean(filepath.Join(working, "..", "run-kapsule-e2e.sh")))
	if err != nil {
		t.Fatal(err)
	}
	contents := string(encoded)
	start := strings.Index(contents, "bootstrap_crash_add_parent() {")
	end := strings.Index(contents, "\nscenario_scale() {")
	if start < 0 || end <= start {
		t.Fatal("bootstrap crash function boundary is missing")
	}
	body := contents[start:end]
	steps := []string{
		`number_of_attachments == 0`,
		`sfs-subdir-bootstrap-parent-filesystem-id`,
		`kill -STOP 1`,
		`parent owner claim existed before the injected controller crash`,
		`bootstrap_available=`,
		`kill -KILL 1`,
		`bootstrap_restart_after=`,
		`bootstrap_journal_count=`,
		`bootstrap_claim=`,
	}
	previous := -1
	for _, step := range steps {
		index := strings.Index(body, step)
		if index <= previous {
			t.Fatalf("bootstrap crash step %q is absent or out of order", step)
		}
		previous = index
	}

	scaleStart := end + 1
	scaleEnd := strings.Index(contents[scaleStart:], "\nscenario_controller_failure() {")
	if scaleEnd < 0 {
		t.Fatal("scale scenario boundary is missing")
	}
	scale := contents[scaleStart : scaleStart+scaleEnd]
	if !strings.Contains(scale, "bootstrap_crash_add_parent") {
		t.Fatal("100-PVC scenario does not hand the still-fresh second parent to the bootstrap crash scenario")
	}
}

func TestNMinusOneUpgradeLeavesSecondParentFreshForBootstrapCrash(t *testing.T) {
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
}
