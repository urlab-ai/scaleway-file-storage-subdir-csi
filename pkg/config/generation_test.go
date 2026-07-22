package config

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
)

func TestNodeConfigGenerationMatchesCanonicalHelmProjection(t *testing.T) {
	runtime := validRuntime(t)
	driverDigest := "sha256:" + strings.Repeat("1", 64)
	generation, err := NodeConfigGeneration(
		runtime.DriverName, driverDigest, runtime.Provider.Region, runtime.Node.ParentMountRoot,
		runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools,
	)
	if err != nil {
		t.Fatalf("NodeConfigGeneration() error = %v", err)
	}
	canonical := `{"accessModes":["SINGLE_NODE_WRITER","MULTI_NODE_MULTI_WRITER"],"driverImageDigest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","driverName":"file-storage-subdir.csi.urlab.ai","kubeletPath":"/var/lib/kubelet","nodeParentMountRoot":"/var/lib/scaleway-sfs-subdir-csi/parents","ownershipSchema":"1","parents":{"33333333-3333-4333-8333-333333333333":{"basePath":"/kubernetes-volumes","pool":"standard"}},"qualifiedCommercialTypes":["TEST-TYPE-1"],"region":"fr-par"}`
	sum := sha256.Sum256([]byte(canonical))
	want := hex.EncodeToString(sum[:])
	if generation != want {
		t.Fatalf("NodeConfigGeneration() = %q, want %q", generation, want)
	}
}

func TestNodeConfigGenerationMatchesDevelopmentChartFixture(t *testing.T) {
	runtime := validRuntime(t)
	runtime.Pools[0].Filesystems = []pool.ParentConfig{
		{ID: "00000000-0000-4000-8000-000000000001", Name: "sfs-subdir-pool-standard-01", State: pool.ParentActive},
		{ID: "00000000-0000-4000-8000-000000000002", Name: "sfs-subdir-pool-standard-02", State: pool.ParentActive},
	}
	generation, err := NodeConfigGeneration(runtime.DriverName, "", runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration(chart fixture) error = %v", err)
	}
	const want = "5fb2269e0c5f31a63d22699096b876007ae467480c0c61e9a26d12e06f1fc190"
	if generation != want {
		t.Fatalf("chart fixture generation = %q, want %q", generation, want)
	}
}

func TestNodeConfigGenerationIsOrderStableAndIdentitySensitive(t *testing.T) {
	runtime := validRuntime(t)
	driverDigest := "sha256:" + strings.Repeat("1", 64)
	first, err := NodeConfigGeneration(runtime.DriverName, driverDigest, runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration() error = %v", err)
	}
	runtime.Pools[0].Filesystems[0].Name = "display-name-does-not-authorize-mounts"
	runtime.Pools[0].Filesystems[0].State = "draining"
	unchanged, err := NodeConfigGeneration(runtime.DriverName, driverDigest, runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration(non-node fields) error = %v", err)
	}
	if unchanged != first {
		t.Fatal("provider display name or placement state changed node generation")
	}
	runtime.Pools[0].BasePath = "/other-volumes"
	changed, err := NodeConfigGeneration(runtime.DriverName, driverDigest, runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration(changed mapping) error = %v", err)
	}
	if changed == first {
		t.Fatal("base path identity did not change node generation")
	}
	runtime = validRuntime(t)
	runtime.Compatibility.QualifiedCommercialTypes = []string{"TEST-TYPE-2"}
	changed, err = NodeConfigGeneration(runtime.DriverName, driverDigest, runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration(changed commercial type) error = %v", err)
	}
	if changed == first {
		t.Fatal("commercial type allowlist did not change node generation")
	}
	runtime = validRuntime(t)
	changed, err = NodeConfigGeneration(runtime.DriverName, "sha256:"+strings.Repeat("2", 64), runtime.Provider.Region, runtime.Node.ParentMountRoot, runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools)
	if err != nil {
		t.Fatalf("NodeConfigGeneration(changed image) error = %v", err)
	}
	if changed == first {
		t.Fatal("driver image digest did not change node generation")
	}
}
