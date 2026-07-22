package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpgradePostRendererChangesOnlyDaemonSetStrategy(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	postRenderer := filepath.Clean(filepath.Join(working, "..", "e2e-helm-ondelete-postrenderer.sh"))
	input := `apiVersion: apps/v1
kind: Deployment
spec:
  strategy:
    type: RollingUpdate
---
apiVersion: apps/v1
kind: DaemonSet
spec:
  updateStrategy:
    type: RollingUpdate
    rollingUpdate: {maxUnavailable: 1}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: retained
`
	command := exec.Command(postRenderer)
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("post-render candidate: %v: %s", err, output)
	}
	rendered := string(output)
	if strings.Count(rendered, "type: OnDelete") != 1 || strings.Count(rendered, "type: RollingUpdate") != 1 {
		t.Fatalf("post-rendered rollout strategies are not exact:\n%s", rendered)
	}
	if strings.Contains(rendered, "rollingUpdate: {maxUnavailable: 1}") || !strings.Contains(rendered, "name: retained") {
		t.Fatalf("post-renderer retained DaemonSet rolling options or changed unrelated objects:\n%s", rendered)
	}
}
