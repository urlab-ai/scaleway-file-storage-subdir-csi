package driverapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	buildversion "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
)

func TestParseAcceptsClosedControllerAndNodeInvocations(t *testing.T) {
	controller, err := Parse([]string{
		"--mode=controller",
		"--endpoint=unix:///csi/csi.sock",
		"--admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock",
		"--config=/etc/scaleway-sfs-subdir-csi/config.json",
		"--live-address=:9810",
		"--metrics-address=:8080",
	})
	if err != nil {
		t.Fatalf("Parse(controller) error = %v", err)
	}
	if controller.Component != config.ComponentController || controller.CSIEndpointPath != "/csi/csi.sock" || controller.AdminEndpointPath != admin.DefaultUnixSocketPath || controller.MetricsAddress != ":8080" {
		t.Fatalf("Parse(controller) = %#v", controller)
	}

	node, err := Parse([]string{
		"--mode", "node",
		"--endpoint", "unix:///csi/csi.sock",
		"--admin-endpoint", "unix:///run/scaleway-sfs-subdir-csi/admin.sock",
		"--config", "/etc/scaleway-sfs-subdir-csi/config.json",
		"--live-address", "[::]:9811",
	})
	if err != nil {
		t.Fatalf("Parse(node) error = %v", err)
	}
	if node.Component != config.ComponentNode || node.LiveAddress != "[::]:9811" || node.MetricsAddress != "" {
		t.Fatalf("Parse(node) = %#v", node)
	}
}

func TestParseRejectsAmbiguousAndUnsafeInvocations(t *testing.T) {
	valid := []string{
		"--mode=controller",
		"--endpoint=unix:///csi/csi.sock",
		"--admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock",
		"--config=/etc/scaleway-sfs-subdir-csi/config.json",
		"--live-address=:9810",
	}
	tests := map[string][]string{
		"empty":             nil,
		"positional":        append(append([]string(nil), valid...), "extra"),
		"short flag":        replaceArgument(valid, 0, "-mode=controller"),
		"unknown flag":      append(append([]string(nil), valid...), "--future=true"),
		"duplicate flag":    append(append([]string(nil), valid...), "--mode=node"),
		"missing value":     replaceArgument(valid, 0, "--mode"),
		"invalid mode":      replaceArgument(valid, 0, "--mode=worker"),
		"TCP CSI":           replaceArgument(valid, 1, "--endpoint=tcp://127.0.0.1:9000"),
		"relative CSI":      replaceArgument(valid, 1, "--endpoint=unix://relative.sock"),
		"normalized CSI":    replaceArgument(valid, 1, "--endpoint=unix:///csi/../csi.sock"),
		"query CSI":         replaceArgument(valid, 1, "--endpoint=unix:///csi/csi.sock?x=1"),
		"long CSI":          replaceArgument(valid, 1, "--endpoint=unix:///"+strings.Repeat("a", maxUnixPathBytes)),
		"non-fixed admin":   replaceArgument(valid, 2, "--admin-endpoint=unix:///tmp/admin.sock"),
		"same endpoint dir": replaceArgument(valid, 1, "--endpoint=unix:///run/scaleway-sfs-subdir-csi/csi.sock"),
		"relative config":   replaceArgument(valid, 3, "--config=config.json"),
		"non-normal config": replaceArgument(valid, 3, "--config=/etc/../config.json"),
		"missing live port": replaceArgument(valid, 4, "--live-address=127.0.0.1"),
		"DNS live host":     replaceArgument(valid, 4, "--live-address=localhost:9810"),
		"zero live port":    replaceArgument(valid, 4, "--live-address=:0"),
		"noncanonical port": replaceArgument(valid, 4, "--live-address=:09810"),
		"empty metrics":     append(append([]string(nil), valid...), "--metrics-address="),
		"same metrics":      append(append([]string(nil), valid...), "--metrics-address=:9810"),
		"multi-line flag":   replaceArgument(valid, 3, "--config=/etc/config.json\nother"),
		"oversized flag":    replaceArgument(valid, 3, "--config=/"+strings.Repeat("x", maxFlagValueBytes)),
		"too many arguments": append(append([]string(nil), valid...),
			"--metrics-address=:8080", "--mode", "controller", "--config", "/tmp/config", "--endpoint", "unix:///tmp/csi.sock", "--live-address", ":1"),
	}
	for name, args := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(args)
			if err == nil || ExitCode(err) != 2 {
				t.Fatalf("Parse() error/exit = %v/%d", err, ExitCode(err))
			}
		})
	}
}

func TestUniqueFlagValuesReportsMissingFieldsDeterministically(t *testing.T) {
	_, err := uniqueFlagValues([]string{"--mode=node"})
	if err == nil || err.Error() != "required driver flag --endpoint is missing" {
		t.Fatalf("uniqueFlagValues() error = %v", err)
	}
}

func TestLoadRejectsContextAndConfigurationBeforeRuntimeConstruction(t *testing.T) {
	args := []string{
		"--mode=node",
		"--endpoint=unix:///csi/csi.sock",
		"--admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock",
		"--config=/does/not/exist/config.json",
		"--live-address=:9811",
	}
	lookup := func(string) (string, bool) { return "", false }
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if _, err := Load(nil, args, lookup); err == nil {
		t.Fatal("Load(nil context) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Load(canceled, args, lookup); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load(canceled) error = %v", err)
	}
	if _, err := Load(context.Background(), args, lookup); err == nil || !strings.Contains(err.Error(), "load node runtime") {
		t.Fatalf("Load(missing config) error = %v", err)
	}
}

func TestLoadRejectsInvalidLinkedBuildIdentityBeforeConfiguration(t *testing.T) {
	originalVersion, originalCommit, originalBuildDate, originalTypes := buildversion.Version, buildversion.Commit, buildversion.BuildDate, buildversion.QualifiedCommercialTypes
	t.Cleanup(func() {
		buildversion.Version, buildversion.Commit, buildversion.BuildDate, buildversion.QualifiedCommercialTypes = originalVersion, originalCommit, originalBuildDate, originalTypes
	})
	buildversion.Version = "v1.2.3"
	buildversion.Commit = strings.Repeat("a", 40)
	buildversion.BuildDate = "2026-07-13T12:34:56Z"
	_, err := Load(context.Background(), []string{"--mode=node"}, func(string) (string, bool) { return "", false })
	if err == nil || !strings.Contains(err.Error(), "validate driver build identity") || ExitCode(err) != 1 {
		t.Fatalf("Load(invalid build) error/exit = %v/%d", err, ExitCode(err))
	}
}

func TestValidateReleaseCompatibilityRequiresExactEmbeddedAllowlist(t *testing.T) {
	original := buildversion.QualifiedCommercialTypes
	t.Cleanup(func() { buildversion.QualifiedCommercialTypes = original })
	runtime := config.Runtime{
		Mode: config.ModeProduction,
		Compatibility: config.Compatibility{
			QualifiedCommercialTypes: []string{"TEST-TYPE-1"},
		},
	}
	buildversion.QualifiedCommercialTypes = "TEST-TYPE-1"
	if err := validateReleaseCompatibility(runtime); err != nil {
		t.Fatalf("validateReleaseCompatibility() error = %v", err)
	}
	buildversion.QualifiedCommercialTypes = "TEST-TYPE-2"
	if err := validateReleaseCompatibility(runtime); err == nil {
		t.Fatal("validateReleaseCompatibility(mismatch) error = nil")
	}
	buildversion.QualifiedCommercialTypes = ""
	if err := validateReleaseCompatibility(runtime); err == nil {
		t.Fatal("validateReleaseCompatibility(empty build) error = nil")
	}
	runtime.Mode = config.ModeDevelopment
	if err := validateReleaseCompatibility(runtime); err != nil {
		t.Fatalf("validateReleaseCompatibility(development) error = %v", err)
	}
}

func TestExitCodeAndUsage(t *testing.T) {
	if ExitCode(nil) != 0 || ExitCode(usage(errors.New("bad flags"))) != 2 || ExitCode(errors.New("startup failed")) != 1 {
		t.Fatalf("ExitCode values = %d/%d/%d", ExitCode(nil), ExitCode(usage(errors.New("bad flags"))), ExitCode(errors.New("startup failed")))
	}
	for _, value := range []string{"--mode=<controller|node>", "--admin-endpoint", "--metrics-address", "--version"} {
		if !strings.Contains(Usage(), value) {
			t.Errorf("Usage() does not contain %q", value)
		}
	}
}

func TestRenderedRuntimeConfigFromHelm(t *testing.T) {
	filename := os.Getenv("SFS_SUBDIR_TEST_RENDERED_CONFIG")
	if filename == "" {
		t.Skip("rendered runtime config is supplied by make helm-test")
	}
	baseArgs := []string{
		"--endpoint=unix:///csi/csi.sock",
		"--admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock",
		"--config=" + filepath.Clean(filename),
		"--live-address=:9810",
		"--metrics-address=:8080",
	}
	controllerEnvironment := map[string]string{
		"INSTALLATION_ID":        "11111111-1111-4111-8111-111111111111",
		"SCW_ACCESS_KEY":         "fixture-access",
		"SCW_SECRET_KEY":         "fixture-secret",
		"SCW_DEFAULT_PROJECT_ID": "00000000-0000-4000-8000-000000000000",
		"SCW_DEFAULT_REGION":     "fr-par",
		"SCW_DEFAULT_ZONE":       "fr-par-1",
	}
	lookup := func(values map[string]string) config.LookupEnv {
		return func(key string) (string, bool) {
			value, present := values[key]
			return value, present
		}
	}
	controllerArgs := append([]string{"--mode=controller"}, baseArgs...)
	controller, err := Load(context.Background(), controllerArgs, lookup(controllerEnvironment))
	if err != nil {
		t.Fatalf("Load(rendered controller) error = %v", err)
	}
	if controller.Options.Component != config.ComponentController || controller.Config.Runtime.DriverName == "" {
		t.Fatalf("rendered controller startup = %#v", controller)
	}

	nodeArgs := append([]string{"--mode=node"}, baseArgs...)
	node, err := Load(context.Background(), nodeArgs, lookup(map[string]string{
		"INSTALLATION_ID": controllerEnvironment["INSTALLATION_ID"],
	}))
	if err != nil {
		t.Fatalf("Load(rendered node) error = %v", err)
	}
	if node.Options.Component != config.ComponentNode || node.Config.NodeConfigGeneration != controller.Config.NodeConfigGeneration {
		t.Fatalf("rendered node/controller startup = %#v/%#v", node, controller)
	}
}

func replaceArgument(values []string, index int, replacement string) []string {
	result := append([]string(nil), values...)
	result[index] = replacement
	return result
}
