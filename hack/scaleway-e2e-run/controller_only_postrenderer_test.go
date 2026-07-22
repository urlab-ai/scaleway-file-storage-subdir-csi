package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestControllerOnlyPostRendererDropsOnlyDaemonSetDocuments(t *testing.T) {
	input := `---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: controller
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: node
---
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: example.csi
`
	command := exec.Command("../e2e-helm-controller-only-postrenderer.sh")
	command.Stdin = strings.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("controller-only post-renderer: %v: %s", err, output)
	}
	observed := string(output)
	if strings.Contains(observed, "kind: DaemonSet") || strings.Contains(observed, "name: node") ||
		!strings.Contains(observed, "kind: Deployment") || !strings.Contains(observed, "kind: CSIDriver") {
		t.Fatalf("controller-only rendering is incomplete:\n%s", observed)
	}
}
