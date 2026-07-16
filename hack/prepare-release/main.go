// Command prepare-release creates the exact promoted chart copy and values used
// by release qualification and publication. The repository chart deliberately
// remains development-only; this command is the single audited promotion path.
package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/compatibility"
)

type options struct {
	chartSource, chartOutput, valuesOutput                        string
	version, releaseTag, driverName, imageRepository, imageDigest string
	commercialTypes                                               string
	provisionerDigest, attacherDigest, registrarDigest            string
	livenessDigest                                                string
}

func main() {
	var configured options
	flag.StringVar(&configured.chartSource, "chart-source", "", "source development chart directory")
	flag.StringVar(&configured.chartOutput, "chart-output", "", "new promoted chart directory")
	flag.StringVar(&configured.valuesOutput, "values-output", "", "release values output path")
	flag.StringVar(&configured.version, "version", "", "release version without v prefix")
	flag.StringVar(&configured.releaseTag, "release-tag", "", "release image tag")
	flag.StringVar(&configured.driverName, "driver-name", "", "immutable public CSI driver name")
	flag.StringVar(&configured.imageRepository, "image-repository", "", "driver image repository")
	flag.StringVar(&configured.imageDigest, "image-digest", "", "driver image sha256 digest")
	flag.StringVar(&configured.commercialTypes, "qualified-commercial-types", "", "canonical comma-separated commercial types")
	flag.StringVar(&configured.provisionerDigest, "provisioner-digest", "", "external-provisioner digest")
	flag.StringVar(&configured.attacherDigest, "attacher-digest", "", "external-attacher digest")
	flag.StringVar(&configured.registrarDigest, "registrar-digest", "", "node-driver-registrar digest")
	flag.StringVar(&configured.livenessDigest, "liveness-digest", "", "liveness-probe digest")
	flag.Parse()
	if err := run(configured); err != nil {
		fmt.Fprintln(os.Stderr, "prepare release:", err)
		os.Exit(1)
	}
}

func run(configured options) error {
	if configured.chartSource == "" || configured.chartOutput == "" || configured.valuesOutput == "" {
		return fmt.Errorf("chart-source, chart-output, and values-output are required")
	}
	if configured.version == "" || configured.releaseTag == "" || configured.driverName == "" || configured.imageRepository == "" {
		return fmt.Errorf("version, release-tag, driver-name, and image-repository are required")
	}
	commercialTypes, err := compatibility.ParseCommercialTypes(configured.commercialTypes)
	if err != nil {
		return err
	}
	for name, digest := range map[string]string{
		"driver": configured.imageDigest, "external-provisioner": configured.provisionerDigest,
		"external-attacher": configured.attacherDigest, "node-driver-registrar": configured.registrarDigest,
		"liveness-probe": configured.livenessDigest,
	} {
		if !validDigest(digest) {
			return fmt.Errorf("%s digest is not an immutable sha256", name)
		}
	}
	if _, err := os.Stat(configured.chartOutput); !os.IsNotExist(err) {
		return fmt.Errorf("chart output %q must not already exist", configured.chartOutput)
	}
	if err := copyTree(configured.chartSource, configured.chartOutput); err != nil {
		return err
	}
	chartPath := filepath.Join(configured.chartOutput, "Chart.yaml")
	chart, err := os.ReadFile(chartPath)
	if err != nil {
		return err
	}
	promoted, err := promoteChartMetadata(string(chart), configured.version)
	if err != nil {
		return err
	}
	if err := os.WriteFile(chartPath, []byte(promoted), 0o644); err != nil {
		return err
	}
	values := renderReleaseValues(configured, commercialTypes)
	if err := os.MkdirAll(filepath.Dir(configured.valuesOutput), 0o755); err != nil {
		return err
	}
	return os.WriteFile(configured.valuesOutput, []byte(values), 0o644)
}

func promoteChartMetadata(chart, version string) (string, error) {
	lines := strings.Split(chart, "\n")
	replacements := map[string]string{
		"version:":    "version: " + version,
		"appVersion:": "appVersion: " + version,
		"scaleway-sfs-subdir-csi.io/release-status:": "  scaleway-sfs-subdir-csi.io/release-status: release-candidate",
	}
	seen := make(map[string]int, len(replacements))
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		for prefix, replacement := range replacements {
			if strings.HasPrefix(trimmed, prefix) {
				lines[index] = replacement
				seen[prefix]++
			}
		}
	}
	for prefix := range replacements {
		if seen[prefix] != 1 {
			return "", fmt.Errorf("chart.yaml field %q occurred %d times", prefix, seen[prefix])
		}
	}
	return strings.Join(lines, "\n"), nil
}

func renderReleaseValues(configured options, commercialTypes []string) string {
	var result strings.Builder
	fmt.Fprintln(&result, "release:")
	fmt.Fprintln(&result, "  mode: production")
	fmt.Fprintln(&result, "driver:")
	fmt.Fprintf(&result, "  name: %s\n", configured.driverName)
	fmt.Fprintln(&result, "image:")
	fmt.Fprintf(&result, "  repository: %s\n  tag: %s\n  digest: %s\n", configured.imageRepository, configured.releaseTag, configured.imageDigest)
	fmt.Fprintln(&result, "compatibility:")
	fmt.Fprintln(&result, "  qualifiedCommercialTypes:")
	for _, commercialType := range commercialTypes {
		fmt.Fprintf(&result, "    - %s\n", commercialType)
	}
	fmt.Fprintln(&result, "sidecars:")
	fmt.Fprintf(&result, "  externalProvisioner:\n    tag: v6.3.0\n    digest: %s\n", configured.provisionerDigest)
	fmt.Fprintf(&result, "  externalAttacher:\n    tag: v4.12.0\n    digest: %s\n", configured.attacherDigest)
	fmt.Fprintf(&result, "  nodeDriverRegistrar:\n    tag: v2.17.0\n    digest: %s\n", configured.registrarDigest)
	fmt.Fprintf(&result, "  livenessProbe:\n    tag: v2.19.0\n    digest: %s\n", configured.livenessDigest)
	return result.String()
}

func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, current)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("chart source contains unsupported non-regular entry %q", current)
		}
		content, err := os.ReadFile(current)
		if err != nil {
			return err
		}
		return os.WriteFile(target, content, info.Mode().Perm())
	})
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range strings.TrimPrefix(value, "sha256:") {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
