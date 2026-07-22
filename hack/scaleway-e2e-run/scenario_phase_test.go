package main

import (
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
