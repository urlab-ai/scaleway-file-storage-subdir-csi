package config

import (
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func validRuntime(t *testing.T) Runtime {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	return Runtime{
		Mode:       ModeProduction,
		DriverName: "sfs-subdir.csi.example.com",
		Installation: Installation{
			ExistingSecretName: "scaleway-sfs-subdir-csi-identity",
			IDKey:              "installationID",
			ID:                 "11111111-1111-4111-8111-111111111111",
		},
		Provider: Provider{
			Region:                        "fr-par",
			DefaultZone:                   "fr-par-1",
			ProjectID:                     "22222222-2222-4222-8222-222222222222",
			CredentialsExistingSecretName: "scaleway-sfs-subdir-csi-credentials",
			AccessKeyKey:                  "SCW_ACCESS_KEY",
			SecretKeyKey:                  "SCW_SECRET_KEY",
		},
		Controller: Controller{
			Replicas:                   1,
			UpdateStrategy:             "Recreate",
			MaxConcurrentMutations:     10,
			ShutdownDeadline:           90 * time.Second,
			TerminationGracePeriod:     120 * time.Second,
			ProgressDeadline:           65 * time.Minute,
			StartupProbeBudget:         60 * time.Minute,
			AttachReadyDeadline:        10 * time.Minute,
			MetadataRefreshInterval:    5 * time.Minute,
			DetailedTombstoneRetention: 30 * 24 * time.Hour,
			ParentMountRoot:            "/var/lib/scaleway-sfs-subdir-csi/controller-parents",
			Leadership: Leadership{
				Enabled:       true,
				LeaseDuration: 30 * time.Second,
				RenewDeadline: 20 * time.Second,
				RetryPeriod:   5 * time.Second,
			},
		},
		Node: Node{
			ParentMountRoot: FixedNodeParentMountRoot,
			KubeletPath:     "/var/lib/kubelet",
		},
		Scheduling: Scheduling{
			AllSchedulableLinuxNodesAreEligible: true,
			RequireHomogeneousEligibleNodes:     true,
		},
		Compatibility: Compatibility{QualifiedCommercialTypes: []string{"TEST-TYPE-1"}},
		Pools: []pool.Config{{
			Name:                      "standard",
			BasePath:                  "/kubernetes-volumes",
			SelectionPolicy:           pool.SelectionLeastAllocated,
			MaxParentsPerEligibleNode: 2,
			MaxLogicalOvercommitRatio: ratio,
			MinFreeBytes:              10 << 30,
			MinFreePercent:            5,
			DeletePolicy:              volume.DeletePolicyArchive,
			DirectoryMode:             "0770",
			DirectoryUID:              1000,
			DirectoryGID:              1000,
			Filesystems: []pool.ParentConfig{{
				ID:    "33333333-3333-4333-8333-333333333333",
				State: pool.ParentActive,
			}},
		}},
		StorageClasses: []StorageClass{{
			Name:                 "sfs-subdir-rwx",
			PoolName:             "standard",
			ReclaimPolicy:        "Delete",
			AllowVolumeExpansion: false,
			VolumeBindingMode:    "Immediate",
		}},
	}
}

func TestRuntimeValidateProductionDefaults(t *testing.T) {
	runtime := validRuntime(t)
	if err := runtime.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	commercialTypes, err := runtime.Compatibility.QualifiedCommercialTypeSet()
	if err != nil {
		t.Fatalf("QualifiedCommercialTypeSet() error = %v", err)
	}
	if _, present := commercialTypes["TEST-TYPE-1"]; !present || len(commercialTypes) != 1 {
		t.Fatalf("commercial type set = %#v", commercialTypes)
	}
}

func TestRuntimeRejectsUnsafeProductionOverrides(t *testing.T) {
	tests := map[string]func(*Runtime){
		"multiple replicas":        func(runtime *Runtime) { runtime.Controller.Replicas = 2 },
		"rolling strategy":         func(runtime *Runtime) { runtime.Controller.UpdateStrategy = "RollingUpdate" },
		"leadership disabled":      func(runtime *Runtime) { runtime.Controller.Leadership.Enabled = false },
		"zero tombstone retention": func(runtime *Runtime) { runtime.Controller.DetailedTombstoneRetention = 0 },
		"bad lease ordering":       func(runtime *Runtime) { runtime.Controller.Leadership.RetryPeriod = 25 * time.Second },
		"shutdown margin overflow": func(runtime *Runtime) {
			runtime.Controller.ShutdownDeadline = time.Duration(1<<63-1) - 10*time.Second
			runtime.Controller.TerminationGracePeriod = time.Duration(1<<63 - 1)
		},
		"startup margin overflow": func(runtime *Runtime) {
			runtime.Controller.StartupProbeBudget = time.Duration(1<<63-1) - time.Minute
			runtime.Controller.ProgressDeadline = time.Duration(1<<63 - 1)
		},
		"generated identity":   func(runtime *Runtime) { runtime.Installation.GenerateForDevelopmentOnly = true },
		"narrow scheduling":    func(runtime *Runtime) { runtime.Scheduling.AllSchedulableLinuxNodesAreEligible = false },
		"parent root override": func(runtime *Runtime) { runtime.Node.ParentMountRoot = "/var/lib/custom-parents" },
		"overlapping roots":    func(runtime *Runtime) { runtime.Node.ParentMountRoot = "/var/lib/kubelet/plugins" },
		"expansion enabled":    func(runtime *Runtime) { runtime.StorageClasses[0].AllowVolumeExpansion = true },
		"invalid identity key": func(runtime *Runtime) { runtime.Installation.IDKey = "bad/key" },
		"invalid access key":   func(runtime *Runtime) { runtime.Provider.AccessKeyKey = "bad key" },
		"oversized secret key": func(runtime *Runtime) { runtime.Provider.SecretKeyKey = strings.Repeat("x", 129) },
		"empty commercial allowlist": func(runtime *Runtime) {
			runtime.Compatibility.QualifiedCommercialTypes = nil
		},
		"unsorted commercial allowlist": func(runtime *Runtime) {
			runtime.Compatibility.QualifiedCommercialTypes = []string{"TYPE-B", "TYPE-A"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			runtime := validRuntime(t)
			mutate(&runtime)
			if err := runtime.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestRuntimeRejectsStorageClassForMissingPool(t *testing.T) {
	runtime := validRuntime(t)
	runtime.StorageClasses[0].PoolName = "missing"
	err := runtime.Validate()
	if err == nil || !strings.Contains(err.Error(), "missing pool") {
		t.Fatalf("Validate() error = %v, want missing pool", err)
	}
}
