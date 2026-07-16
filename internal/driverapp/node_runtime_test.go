package driverapp

import (
	"testing"

	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func TestNodeRuntimeInputsAreStableAndComplete(t *testing.T) {
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	configured := []pool.Config{{
		Name: "standard", BasePath: "/kubernetes-volumes",
		SelectionPolicy: pool.SelectionLeastAllocated, MaxParentsPerEligibleNode: 1,
		MaxLogicalOvercommitRatio: ratio, MinFreeBytes: 1, MinFreePercent: 5,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
		Filesystems: []pool.ParentConfig{{
			ID: "33333333-3333-4333-8333-333333333333", Name: "parent-a", State: pool.ParentActive,
		}},
	}}
	parents, parentIDs, pools, err := nodeRuntimeInputs(configured)
	if err != nil {
		t.Fatalf("nodeRuntimeInputs() error = %v", err)
	}
	if len(parents) != 1 || len(parentIDs) != 1 || parentIDs[0] != configured[0].Filesystems[0].ID {
		t.Fatalf("node parents = %#v / %#v", parents, parentIDs)
	}
	if len(pools) != 1 || pools[0] != configured[0].Name {
		t.Fatalf("node pools = %#v", pools)
	}

	broken := append([]pool.Config(nil), configured...)
	broken[0].Filesystems = nil
	if _, _, _, err := nodeRuntimeInputs(broken); err == nil {
		t.Fatal("nodeRuntimeInputs(invalid pools) error = nil")
	}
}
