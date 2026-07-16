package config

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func validRuntimeFileFixture(t *testing.T) runtimeFile {
	t.Helper()
	runtime := validRuntime(t)
	generation, err := NodeConfigGeneration(
		runtime.DriverName, runtime.Provider.Region, runtime.Node.ParentMountRoot,
		runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools,
	)
	if err != nil {
		t.Fatalf("NodeConfigGeneration() error = %v", err)
	}
	return runtimeFile{
		SchemaVersion: RuntimeFileSchemaV1, Mode: runtime.Mode, DriverName: runtime.DriverName,
		LogLevel: "info", ControllerNamespace: "driver-system", HelmReleaseName: "driver",
		ChartVersion: "1.0.0",
		RenderedImages: []RenderedImage{
			{Name: "driver", Digest: "sha256:" + strings.Repeat("1", 64)},
			{Name: "external-attacher", Digest: "sha256:" + strings.Repeat("2", 64)},
			{Name: "external-provisioner", Digest: "sha256:" + strings.Repeat("3", 64)},
			{Name: "liveness-probe", Digest: "sha256:" + strings.Repeat("4", 64)},
			{Name: "node-driver-registrar", Digest: "sha256:" + strings.Repeat("5", 64)},
		},
		NodeConfigGeneration: generation,
		Installation: runtimeInstallationFile{
			ExistingSecretName:         runtime.Installation.ExistingSecretName,
			IDKey:                      runtime.Installation.IDKey,
			GenerateForDevelopmentOnly: runtime.Installation.GenerateForDevelopmentOnly,
		},
		Provider: runtimeProviderFile{
			Region: runtime.Provider.Region, DefaultZone: runtime.Provider.DefaultZone, ProjectID: runtime.Provider.ProjectID,
			Credentials: runtimeCredentialsFile{
				ExistingSecretName: runtime.Provider.CredentialsExistingSecretName,
				AccessKeyKey:       runtime.Provider.AccessKeyKey, SecretKeyKey: runtime.Provider.SecretKeyKey,
			},
		},
		Controller: runtimeControllerFile{
			Replicas: runtime.Controller.Replicas, UpdateStrategy: runtime.Controller.UpdateStrategy,
			MaxConcurrentMutations:            runtime.Controller.MaxConcurrentMutations,
			ShutdownDeadlineSeconds:           uint64(runtime.Controller.ShutdownDeadline / time.Second),
			TerminationGracePeriodSeconds:     uint64(runtime.Controller.TerminationGracePeriod / time.Second),
			ProgressDeadlineSeconds:           uint64(runtime.Controller.ProgressDeadline / time.Second),
			StartupProbeBudgetSeconds:         uint64(runtime.Controller.StartupProbeBudget / time.Second),
			AttachReadyDeadlineSeconds:        uint64(runtime.Controller.AttachReadyDeadline / time.Second),
			MetadataRefreshIntervalSeconds:    uint64(runtime.Controller.MetadataRefreshInterval / time.Second),
			DetailedTombstoneRetentionSeconds: uint64(runtime.Controller.DetailedTombstoneRetention / time.Second),
			ParentMountRoot:                   runtime.Controller.ParentMountRoot,
			Leadership: runtimeLeadershipFile{
				Enabled: runtime.Controller.Leadership.Enabled, LeaseName: volume.LeadershipLeaseNameV1,
				LeaseDurationSeconds: uint64(runtime.Controller.Leadership.LeaseDuration / time.Second),
				RenewDeadlineSeconds: uint64(runtime.Controller.Leadership.RenewDeadline / time.Second),
				RetryPeriodSeconds:   uint64(runtime.Controller.Leadership.RetryPeriod / time.Second),
			},
		},
		Node:       runtimeNodeFile{ParentMountRoot: runtime.Node.ParentMountRoot, KubeletPath: runtime.Node.KubeletPath},
		Scheduling: runtime.Scheduling,
		Compatibility: runtimeCompatibilityFile{
			QualifiedCommercialTypes: slices.Clone(runtime.Compatibility.QualifiedCommercialTypes),
		},
		Pools: map[string]runtimePoolFile{
			"standard": {
				BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
				MaxParentsPerEligibleNode: 2, MaxLogicalOvercommitRatio: "1.0",
				MinFreeBytes: 10 << 30, MinFreePercent: 5, DeletePolicy: volume.DeletePolicyArchive,
				DirectoryMode: "0770", DirectoryUID: "1000", DirectoryGID: "1000",
				Filesystems: []runtimeParentFile{{
					ID: "33333333-3333-4333-8333-333333333333", Name: "parent-a", State: pool.ParentActive,
				}},
			},
		},
		StorageClasses: []runtimeStorageClassFile{{
			Name: "sfs-subdir-rwx", PoolName: "standard", ReclaimPolicy: "Delete", VolumeBindingMode: "Immediate",
		}},
	}
}

func encodeRuntimeFileFixture(t *testing.T, file runtimeFile) []byte {
	t.Helper()
	data, err := canonicaljson.Marshal(file)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	return data
}

func controllerEnvironment() map[string]string {
	return map[string]string{
		"INSTALLATION_ID":        "11111111-1111-4111-8111-111111111111",
		"SCW_ACCESS_KEY":         "test-access-value",
		"SCW_SECRET_KEY":         "test-secret-value",
		"SCW_DEFAULT_PROJECT_ID": "22222222-2222-4222-8222-222222222222",
		"SCW_DEFAULT_REGION":     "fr-par",
		"SCW_DEFAULT_ZONE":       "fr-par-1",
	}
}

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, present := values[key]
		return value, present
	}
}

func TestDecodeRuntimeFileBuildsValidatedControllerWithoutRetainingSecrets(t *testing.T) {
	data := encodeRuntimeFileFixture(t, validRuntimeFileFixture(t))
	loaded, err := DecodeRuntimeFile(data, ComponentController, mapLookup(controllerEnvironment()))
	if err != nil {
		t.Fatalf("DecodeRuntimeFile() error = %v", err)
	}
	if err := loaded.Runtime.Validate(); err != nil {
		t.Fatalf("loaded Runtime.Validate() error = %v", err)
	}
	if loaded.Runtime.Installation.ID != "11111111-1111-4111-8111-111111111111" || loaded.ControllerNamespace != "driver-system" || loaded.LogLevel != "info" || loaded.ChartVersion != "1.0.0" || len(loaded.RenderedImages) != 5 {
		t.Fatalf("loaded identity/metadata = %#v", loaded)
	}
	if loaded.Runtime.Controller.ShutdownDeadline != 90*time.Second || loaded.Runtime.Controller.StartupProbeBudget != time.Hour {
		t.Fatalf("loaded controller durations = %#v", loaded.Runtime.Controller)
	}
	if strings.Contains(string(data), "test-access-value") || strings.Contains(string(data), "test-secret-value") {
		t.Fatal("runtime document retained credential values")
	}
	formatted := fmt.Sprintf("%#v", loaded)
	if strings.Contains(formatted, "test-access-value") || strings.Contains(formatted, "test-secret-value") {
		t.Fatal("loaded runtime retained credential value")
	}
}

func TestDecodeRuntimeFileEnforcesComponentCredentialBoundary(t *testing.T) {
	data := encodeRuntimeFileFixture(t, validRuntimeFileFixture(t))
	nodeEnvironment := map[string]string{"INSTALLATION_ID": "11111111-1111-4111-8111-111111111111"}
	if _, err := DecodeRuntimeFile(data, ComponentNode, mapLookup(nodeEnvironment)); err != nil {
		t.Fatalf("DecodeRuntimeFile(node) error = %v", err)
	}
	nodeEnvironment["SCW_ACCESS_KEY"] = "must-not-reach-node"
	if _, err := DecodeRuntimeFile(data, ComponentNode, mapLookup(nodeEnvironment)); err == nil {
		t.Fatal("DecodeRuntimeFile(node with credentials) error = nil")
	}

	for name, mutate := range map[string]func(map[string]string){
		"missing access":   func(values map[string]string) { delete(values, "SCW_ACCESS_KEY") },
		"empty secret":     func(values map[string]string) { values["SCW_SECRET_KEY"] = "" },
		"scope mismatch":   func(values map[string]string) { values["SCW_DEFAULT_REGION"] = "nl-ams" },
		"missing identity": func(values map[string]string) { delete(values, "INSTALLATION_ID") },
	} {
		t.Run(name, func(t *testing.T) {
			environment := controllerEnvironment()
			mutate(environment)
			_, err := DecodeRuntimeFile(data, ComponentController, mapLookup(environment))
			if err == nil {
				t.Fatal("DecodeRuntimeFile() error = nil")
			}
			if strings.Contains(err.Error(), "test-access-value") || strings.Contains(err.Error(), "test-secret-value") {
				t.Fatalf("DecodeRuntimeFile() exposed credential value: %v", err)
			}
		})
	}
}

func TestDecodeRuntimeFileRejectsClosedSchemaAndSemanticTampering(t *testing.T) {
	base := validRuntimeFileFixture(t)
	environment := mapLookup(controllerEnvironment())
	for name, mutate := range map[string]func(*runtimeFile){
		"schema":                   func(file *runtimeFile) { file.SchemaVersion = "2" },
		"generation":               func(file *runtimeFile) { file.NodeConfigGeneration = strings.Repeat("a", 64) },
		"Lease name":               func(file *runtimeFile) { file.Controller.Leadership.LeaseName = "other" },
		"zero duration":            func(file *runtimeFile) { file.Controller.ShutdownDeadlineSeconds = 0 },
		"empty chart version":      func(file *runtimeFile) { file.ChartVersion = "" },
		"missing rendered image":   func(file *runtimeFile) { file.RenderedImages = file.RenderedImages[:4] },
		"mutable production image": func(file *runtimeFile) { file.RenderedImages[0].Digest = "" },
		"reordered rendered image": func(file *runtimeFile) {
			file.RenderedImages[0], file.RenderedImages[1] = file.RenderedImages[1], file.RenderedImages[0]
		},
		"ratio": func(file *runtimeFile) {
			poolFile := file.Pools["standard"]
			poolFile.MaxLogicalOvercommitRatio = "01.0"
			file.Pools["standard"] = poolFile
		},
		"identity": func(file *runtimeFile) {
			poolFile := file.Pools["standard"]
			poolFile.DirectoryUID = "01000"
			file.Pools["standard"] = poolFile
		},
		"empty commercial types": func(file *runtimeFile) {
			file.Compatibility.QualifiedCommercialTypes = nil
		},
		"unsorted commercial types": func(file *runtimeFile) {
			file.Compatibility.QualifiedCommercialTypes = []string{"TYPE-B", "TYPE-A"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			changed.Pools = cloneRuntimePoolFiles(base.Pools)
			changed.RenderedImages = slices.Clone(base.RenderedImages)
			mutate(&changed)
			if _, err := DecodeRuntimeFile(encodeRuntimeFileFixture(t, changed), ComponentController, environment); err == nil {
				t.Fatal("DecodeRuntimeFile(tampered) error = nil")
			}
		})
	}

	encoded := encodeRuntimeFileFixture(t, base)
	unknown := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"future":true}`)...)
	if _, err := DecodeRuntimeFile(unknown, ComponentController, environment); err == nil {
		t.Fatal("DecodeRuntimeFile(unknown field) error = nil")
	}
	duplicate := append([]byte(`{"schemaVersion":"1",`), encoded[1:]...)
	if _, err := DecodeRuntimeFile(duplicate, ComponentController, environment); err == nil {
		t.Fatal("DecodeRuntimeFile(duplicate field) error = nil")
	}
	if _, err := DecodeRuntimeFile(append(encoded, []byte(` {}`)...), ComponentController, environment); err == nil {
		t.Fatal("DecodeRuntimeFile(trailing value) error = nil")
	}
	missingFalse := bytes.Replace(encoded, []byte(`"defaultClass":false,`), nil, 1)
	if bytes.Equal(missingFalse, encoded) {
		t.Fatal("test fixture did not contain defaultClass=false")
	}
	if _, err := DecodeRuntimeFile(missingFalse, ComponentController, environment); err == nil || !strings.Contains(err.Error(), "defaultClass is missing") {
		t.Fatalf("DecodeRuntimeFile(missing false field) error = %v", err)
	}
	missingCompatibility := bytes.Replace(encoded, []byte(`"compatibility":{"qualifiedCommercialTypes":["TEST-TYPE-1"]},`), nil, 1)
	if bytes.Equal(missingCompatibility, encoded) {
		t.Fatal("test fixture did not contain compatibility projection")
	}
	if _, err := DecodeRuntimeFile(missingCompatibility, ComponentController, environment); err == nil || !strings.Contains(err.Error(), "compatibility is missing") {
		t.Fatalf("DecodeRuntimeFile(missing compatibility) error = %v", err)
	}
	if _, err := DecodeRuntimeFile(make([]byte, MaxRuntimeFileBytes+1), ComponentController, environment); err == nil {
		t.Fatal("DecodeRuntimeFile(oversized) error = nil")
	}
}

func TestLoadRuntimeFileHandlesProjectedSymlinkAndBoundaries(t *testing.T) {
	data := encodeRuntimeFileFixture(t, validRuntimeFileFixture(t))
	root := t.TempDir()
	target := filepath.Join(root, "..data", "config.json")
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	projected := filepath.Join(root, "config.json")
	if err := os.Symlink(filepath.Join("..data", "config.json"), projected); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := LoadRuntimeFile(context.Background(), projected, ComponentController, mapLookup(controllerEnvironment())); err != nil {
		t.Fatalf("LoadRuntimeFile(projected symlink) error = %v", err)
	}
	if _, err := LoadRuntimeFile(context.Background(), "relative.json", ComponentController, mapLookup(controllerEnvironment())); err == nil {
		t.Fatal("LoadRuntimeFile(relative path) error = nil")
	}
	if _, err := LoadRuntimeFile(context.Background(), root, ComponentController, mapLookup(controllerEnvironment())); err == nil {
		t.Fatal("LoadRuntimeFile(directory) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := LoadRuntimeFile(canceled, projected, ComponentController, mapLookup(controllerEnvironment())); err == nil {
		t.Fatal("LoadRuntimeFile(canceled) error = nil")
	}
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if _, err := LoadRuntimeFile(nil, projected, ComponentController, mapLookup(controllerEnvironment())); err == nil {
		t.Fatal("LoadRuntimeFile(nil context) error = nil")
	}
}

// TestRenderedRuntimeConfigFromHelm is invoked by hack/verify-helm.sh with the
// freshly rendered ConfigMap payload. Keeping the bridge in the normal package
// test binary proves Helm and the production decoder share one contract.
func TestRenderedRuntimeConfigFromHelm(t *testing.T) {
	filename := os.Getenv("SFS_SUBDIR_TEST_RENDERED_CONFIG")
	if filename == "" {
		t.Skip("rendered runtime config is supplied by make helm-test")
	}
	controllerEnv := map[string]string{
		"INSTALLATION_ID":        "11111111-1111-4111-8111-111111111111",
		"SCW_ACCESS_KEY":         "fixture-access",
		"SCW_SECRET_KEY":         "fixture-secret",
		"SCW_DEFAULT_PROJECT_ID": "00000000-0000-4000-8000-000000000000",
		"SCW_DEFAULT_REGION":     "fr-par",
		"SCW_DEFAULT_ZONE":       "fr-par-1",
	}
	controller, err := LoadRuntimeFile(context.Background(), filename, ComponentController, mapLookup(controllerEnv))
	if err != nil {
		t.Fatalf("LoadRuntimeFile(rendered controller) error = %v", err)
	}
	if len(controller.Runtime.Pools) != 1 || len(controller.Runtime.Pools[0].Filesystems) != 2 || len(controller.Runtime.StorageClasses) != 1 {
		t.Fatalf("rendered controller config is incomplete: %#v", controller.Runtime)
	}
	node, err := LoadRuntimeFile(context.Background(), filename, ComponentNode, mapLookup(map[string]string{
		"INSTALLATION_ID": controllerEnv["INSTALLATION_ID"],
	}))
	if err != nil {
		t.Fatalf("LoadRuntimeFile(rendered node) error = %v", err)
	}
	if node.NodeConfigGeneration != controller.NodeConfigGeneration {
		t.Fatalf("controller/node generation = %q/%q", controller.NodeConfigGeneration, node.NodeConfigGeneration)
	}
	if len(node.Runtime.Compatibility.QualifiedCommercialTypes) != 1 || node.Runtime.Compatibility.QualifiedCommercialTypes[0] != "TEST-TYPE-1" {
		t.Fatalf("rendered compatibility = %#v", node.Runtime.Compatibility)
	}
}

func cloneRuntimePoolFiles(values map[string]runtimePoolFile) map[string]runtimePoolFile {
	result := make(map[string]runtimePoolFile, len(values))
	for name, configured := range values {
		configured.Filesystems = slices.Clone(configured.Filesystems)
		result[name] = configured
	}
	return result
}
