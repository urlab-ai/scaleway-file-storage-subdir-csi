package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPromotesExactChartAndProjectsCommercialTypes(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatal(err)
	}
	chart := "apiVersion: v2\nname: test\nversion: 0.0.0-dev\nappVersion: 0.0.0-dev\nannotations:\n  scaleway-sfs-subdir-csi.io/release-status: development-only\n"
	if err := os.WriteFile(filepath.Join(source, "Chart.yaml"), []byte(chart), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "values.yaml"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("1", 64)
	configured := options{
		chartSource: source, chartOutput: filepath.Join(root, "promoted"), valuesOutput: filepath.Join(root, "release-values.yaml"),
		version: "1.2.3", releaseTag: "v1.2.3", driverName: "driver.example.org", imageRepository: "registry.example.org/driver",
		imageDigest: digest, commercialTypes: "TYPE-A,TYPE-B", provisionerDigest: digest,
		attacherDigest: digest, registrarDigest: digest, livenessDigest: digest,
	}
	if err := run(configured); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	promoted, err := os.ReadFile(filepath.Join(configured.chartOutput, "Chart.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(promoted), "development-only") || !strings.Contains(string(promoted), "release-status: release-candidate") {
		t.Fatalf("promoted Chart.yaml = %s", promoted)
	}
	values, err := os.ReadFile(configured.valuesOutput)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(values), "qualifiedCommercialTypes:\n    - TYPE-A\n    - TYPE-B") {
		t.Fatalf("release values = %s", values)
	}
}
