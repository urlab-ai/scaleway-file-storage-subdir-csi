package pool

import (
	"strings"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Name:                      "standard",
		BasePath:                  "/kubernetes-volumes",
		SelectionPolicy:           SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 2,
		MaxLogicalOvercommitRatio: testRatio(t, "1.0"),
		MinFreeBytes:              10,
		MinFreePercent:            5,
		DeletePolicy:              volume.DeletePolicyArchive,
		DirectoryMode:             "0770",
		DirectoryUID:              1000,
		DirectoryGID:              1000,
		Filesystems: []ParentConfig{
			{ID: "11111111-1111-4111-8111-111111111111", Name: "parent-a", State: ParentActive},
			{ID: "22222222-2222-4222-8222-222222222222", Name: "parent-b", State: ParentDraining},
		},
	}
}

func TestValidateConfigsRejectsParentReuseAcrossPools(t *testing.T) {
	first := testConfig(t)
	second := testConfig(t)
	second.Name = "premium"
	second.Filesystems = []ParentConfig{{ID: first.Filesystems[0].ID, State: ParentActive}}

	err := ValidateConfigs([]Config{first, second})
	if err == nil || !strings.Contains(err.Error(), "appears in pools") {
		t.Fatalf("ValidateConfigs() error = %v, want cross-pool duplicate", err)
	}
}

func TestConfigRejectsParentCountOverNodeContract(t *testing.T) {
	config := testConfig(t)
	config.MaxParentsPerEligibleNode = 1
	if err := config.Validate(); err == nil {
		t.Fatal("Validate() error = nil")
	}
}
