package mount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const preflightDriverName = "sfs-subdir.csi.example.com"

func TestPreflightNodePathsProvesResolvedAndKernelTopology(t *testing.T) {
	layout := validNodePathLayout(t)
	mountInfo := validNodeMountInfo(layout)
	proof, err := PreflightNodePaths(context.Background(), layout, strings.NewReader(mountInfo))
	if err != nil {
		t.Fatalf("PreflightNodePaths() error = %v", err)
	}
	if err := proof.validate(); err != nil {
		t.Fatalf("proof.validate() error = %v", err)
	}
	if proof.anchors.parentRoot != layout.ParentMountRoot || proof.anchors.plugins != filepath.ToSlash(filepath.Join(layout.KubeletPath, "plugins")) || proof.anchors.csi != filepath.ToSlash(filepath.Dir(layout.CSISocketPath)) {
		t.Fatalf("proof anchors = %#v", proof.anchors)
	}
}

func TestInspectNodePathsRejectsResolvedAliasesAndUnsafeEntries(t *testing.T) {
	t.Run("kubelet symlink aliases parent", func(t *testing.T) {
		root := t.TempDir()
		parent := filepath.Join(root, "parent")
		mustMkdirAll(t, filepath.Join(parent, "plugins", preflightDriverName))
		mustMkdirAll(t, filepath.Join(parent, "pods"))
		if err := os.Symlink(parent, filepath.Join(root, "kubelet")); err != nil {
			t.Fatalf("Symlink() error = %v", err)
		}
		csi := filepath.Join(root, "csi")
		mustMkdirAll(t, csi)
		layout := NodePathLayout{
			DriverName: preflightDriverName, ParentMountRoot: filepath.ToSlash(parent),
			KubeletPath:   filepath.ToSlash(filepath.Join(root, "kubelet")),
			CSISocketPath: filepath.ToSlash(filepath.Join(csi, "csi.sock")),
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "resolved parent mount root") {
			t.Fatalf("inspectNodePaths(alias) error = %v", err)
		}
	})

	t.Run("CSI directory symlink aliases parent", func(t *testing.T) {
		layout := validNodePathLayout(t)
		if err := os.Remove(filepath.Dir(layout.CSISocketPath)); err != nil {
			t.Fatalf("Remove(CSI directory) error = %v", err)
		}
		if err := os.Symlink(layout.ParentMountRoot, filepath.Dir(layout.CSISocketPath)); err != nil {
			t.Fatalf("Symlink(CSI directory) error = %v", err)
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "CSI socket directory") {
			t.Fatalf("inspectNodePaths(CSI alias) error = %v", err)
		}
	})

	t.Run("dangling registry symlink", func(t *testing.T) {
		layout := validNodePathLayout(t)
		registry := filepath.Join(layout.KubeletPath, "plugins_registry")
		if err := os.Symlink(filepath.Join(filepath.Dir(registry), "missing"), registry); err != nil {
			t.Fatalf("Symlink(registry) error = %v", err)
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "dangling symlink") {
			t.Fatalf("inspectNodePaths(dangling registry) error = %v", err)
		}
	})

	t.Run("missing staging tail resolves through parent alias", func(t *testing.T) {
		layout := validNodePathLayout(t)
		plugins := filepath.Join(layout.KubeletPath, "plugins")
		if err := os.Symlink(layout.ParentMountRoot, filepath.Join(plugins, "kubernetes.io")); err != nil {
			t.Fatalf("Symlink(staging ancestor) error = %v", err)
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "kubelet staging tree") {
			t.Fatalf("inspectNodePaths(staging alias) error = %v", err)
		}
	})

	t.Run("registry is a file", func(t *testing.T) {
		layout := validNodePathLayout(t)
		registry := filepath.Join(layout.KubeletPath, "plugins_registry")
		if err := os.WriteFile(registry, []byte("not-a-directory"), 0o600); err != nil {
			t.Fatalf("WriteFile(registry) error = %v", err)
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("inspectNodePaths(file registry) error = %v", err)
		}
	})

	t.Run("required plugin directory missing", func(t *testing.T) {
		layout := validNodePathLayout(t)
		if err := os.Remove(filepath.Join(layout.KubeletPath, "plugins", layout.DriverName)); err != nil {
			t.Fatalf("Remove(plugin directory) error = %v", err)
		}
		if _, err := inspectNodePaths(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "required kubelet plugin directory") {
			t.Fatalf("inspectNodePaths(missing plugin) error = %v", err)
		}
	})
}

func TestInspectNodePathsRejectsInvalidLayoutAndCancellation(t *testing.T) {
	layout := validNodePathLayout(t)
	invalid := []NodePathLayout{
		{},
		{DriverName: layout.DriverName, ParentMountRoot: "relative", KubeletPath: layout.KubeletPath, CSISocketPath: layout.CSISocketPath},
		{DriverName: layout.DriverName, ParentMountRoot: layout.ParentMountRoot, KubeletPath: layout.KubeletPath, CSISocketPath: "/"},
		{DriverName: layout.DriverName, ParentMountRoot: filepath.ToSlash(filepath.Join(layout.KubeletPath, "parents")), KubeletPath: layout.KubeletPath, CSISocketPath: layout.CSISocketPath},
	}
	for _, candidate := range invalid {
		if _, err := inspectNodePaths(context.Background(), candidate); err == nil {
			t.Errorf("inspectNodePaths(%#v) error = nil", candidate)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := inspectNodePaths(canceled, layout); err == nil {
		t.Fatal("inspectNodePaths(canceled) error = nil")
	}
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if _, err := PreflightNodePaths(nil, layout, strings.NewReader(validNodeMountInfo(layout))); err == nil {
		t.Fatal("PreflightNodePaths(nil context) error = nil")
	}
	if _, err := PreflightNodePaths(context.Background(), layout, nil); err == nil {
		t.Fatal("PreflightNodePaths(nil mountinfo) error = nil")
	}
}

func TestValidateNodeMountTopologyRejectsMissingStackedUnpropagatedAndAliasedMounts(t *testing.T) {
	layout := validNodePathLayout(t)
	proof, err := inspectNodePaths(context.Background(), layout)
	if err != nil {
		t.Fatalf("inspectNodePaths() error = %v", err)
	}
	entries := parseNodeMountInfo(t, validNodeMountInfo(layout))
	if err := validateNodeMountTopology(entries, proof); err != nil {
		t.Fatalf("validateNodeMountTopology(valid) error = %v", err)
	}

	find := func(target string) int {
		for index, entry := range entries {
			if entry.MountPoint == target {
				return index
			}
		}
		t.Fatalf("fixture mount %q is missing", target)
		return -1
	}
	parentIndex := find(layout.ParentMountRoot)
	pluginsIndex := find(filepath.ToSlash(filepath.Join(layout.KubeletPath, "plugins")))
	podsIndex := find(filepath.ToSlash(filepath.Join(layout.KubeletPath, "pods")))
	csiIndex := find(filepath.ToSlash(filepath.Dir(layout.CSISocketPath)))

	tests := []struct {
		name   string
		mutate func([]MountInfoEntry) []MountInfoEntry
		want   string
	}{
		{name: "missing parent", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			return slices.Delete(values, parentIndex, parentIndex+1)
		}, want: "no exact mountinfo entry"},
		{name: "stacked parent", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			duplicate := values[parentIndex]
			duplicate.MountID = 999
			return append(values, duplicate)
		}, want: "stacked mount"},
		{name: "parent not shared", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[parentIndex].Optional = nil
			return values
		}, want: "not a shared"},
		{name: "plugins not shared", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[pluginsIndex].Optional = nil
			return values
		}, want: "kubelet plugins"},
		{name: "parent aliases plugin ancestor", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[pluginsIndex].DeviceID = values[parentIndex].DeviceID
			values[pluginsIndex].Root = values[parentIndex].Root + "/plugins"
			return values
		}, want: "alias overlapping roots"},
		{name: "parent aliases pods", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[podsIndex].DeviceID = values[parentIndex].DeviceID
			values[podsIndex].Root = values[parentIndex].Root
			return values
		}, want: "alias overlapping roots"},
		{name: "parent aliases CSI", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[csiIndex].DeviceID = values[parentIndex].DeviceID
			values[csiIndex].Root = values[parentIndex].Root + "/socket"
			return values
		}, want: "alias overlapping roots"},
		{name: "malformed device", mutate: func(values []MountInfoEntry) []MountInfoEntry {
			values[parentIndex].DeviceID = "bad"
			return values
		}, want: "device ID"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := slices.Clone(entries)
			changed = test.mutate(changed)
			err := validateNodeMountTopology(changed, proof)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateNodeMountTopology() error = %v, want %q", err, test.want)
			}
		})
	}

	differentDevice := slices.Clone(entries)
	differentDevice[pluginsIndex].Root = entries[parentIndex].Root
	differentDevice[pluginsIndex].DeviceID = "9:9"
	if err := validateNodeMountTopology(differentDevice, proof); err != nil {
		t.Fatalf("different devices with equal local roots must remain distinct: %v", err)
	}
	if err := validateNodeMountTopology(entries, NodePathPreflight{}); err == nil {
		t.Fatal("validateNodeMountTopology(empty proof) error = nil")
	}
}

func validNodePathLayout(t *testing.T) NodePathLayout {
	t.Helper()
	root := t.TempDir()
	parent := filepath.Join(root, "parents")
	kubelet := filepath.Join(root, "kubelet")
	csi := filepath.Join(root, "csi")
	mustMkdirAll(t, parent)
	mustMkdirAll(t, filepath.Join(kubelet, "plugins", preflightDriverName))
	mustMkdirAll(t, filepath.Join(kubelet, "pods"))
	mustMkdirAll(t, csi)
	return NodePathLayout{
		DriverName: preflightDriverName, ParentMountRoot: filepath.ToSlash(parent),
		KubeletPath: filepath.ToSlash(kubelet), CSISocketPath: filepath.ToSlash(filepath.Join(csi, "csi.sock")),
	}
}

func validNodeMountInfo(layout NodePathLayout) string {
	plugins := filepath.ToSlash(filepath.Join(layout.KubeletPath, "plugins"))
	pods := filepath.ToSlash(filepath.Join(layout.KubeletPath, "pods"))
	csi := filepath.ToSlash(filepath.Dir(layout.CSISocketPath))
	return fmt.Sprintf(
		"100 1 8:1 /host/driver-parents %s rw shared:10 - ext4 /dev/root rw\n"+
			"101 1 8:1 /host/kubelet/plugins %s rw shared:11 - ext4 /dev/root rw\n"+
			"102 1 8:1 /host/kubelet/pods %s rw shared:12 - ext4 /dev/root rw\n"+
			"103 1 8:1 /host/kubelet/plugins/%s %s rw - ext4 /dev/root rw\n",
		layout.ParentMountRoot, plugins, pods, preflightDriverName, csi,
	)
}

func parseNodeMountInfo(t *testing.T, value string) []MountInfoEntry {
	t.Helper()
	entries, err := ParseMountInfo(strings.NewReader(value))
	if err != nil {
		t.Fatalf("ParseMountInfo() error = %v", err)
	}
	return entries
}

func mustMkdirAll(t *testing.T, value string) {
	t.Helper()
	if err := os.MkdirAll(value, 0o700); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", value, err)
	}
}
