package mount

import (
	"errors"
	"strings"
	"testing"
)

const (
	mountInfoParentRoot = "/var/lib/scaleway-sfs-subdir-csi/parents"
	mountInfoKubelet    = "/var/lib/kubelet"
	mountInfoDriver     = "file-storage-subdir.csi.urlab.ai"
	mountInfoParentID   = "11111111-1111-4111-8111-111111111111"
)

func validMountInfoText() string {
	parent := mountInfoParentRoot + "/" + mountInfoParentID
	stage := mountInfoKubelet + "/plugins/kubernetes.io/csi/" + mountInfoDriver + "/volume-a/globalmount"
	publish := mountInfoKubelet + "/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount"
	return strings.Join([]string{
		"36 25 0:42 / " + parent + " rw,relatime - virtiofs " + mountInfoParentID + " rw",
		"37 25 0:42 /kubernetes-volumes/tenant--claim--0123456789ab " + stage + " rw,relatime - virtiofs " + mountInfoParentID + " rw",
		"38 25 0:42 /kubernetes-volumes/tenant--claim--0123456789ab " + publish + " ro,relatime - virtiofs " + mountInfoParentID + " rw",
	}, "\n")
}

func TestParseAndBuildMountInfoUsesKernelBackingIdentity(t *testing.T) {
	raw, err := ParseMountInfo(strings.NewReader(validMountInfoText()))
	if err != nil {
		t.Fatalf("ParseMountInfo() error = %v", err)
	}
	table, err := BuildTableFromMountInfo(raw, mountInfoParentRoot, mountInfoKubelet, mountInfoDriver)
	if err != nil {
		t.Fatalf("BuildTableFromMountInfo() error = %v", err)
	}
	if len(table.Entries) != 3 {
		t.Fatalf("table entries = %d, want 3", len(table.Entries))
	}
	mapping := mountMapping()
	parent := mountInfoParentRoot + "/" + mountInfoParentID
	stage := mountInfoKubelet + "/plugins/kubernetes.io/csi/" + mountInfoDriver + "/volume-a/globalmount"
	publish := mountInfoKubelet + "/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount"
	if _, err := ValidateStage(table, parent, stage, mapping, mountCapability()); err != nil {
		t.Fatalf("ValidateStage(parsed) error = %v", err)
	}
	if _, err := ValidatePublish(table, stage, publish, mapping, mountCapability(), true); err != nil {
		t.Fatalf("ValidatePublish(parsed) error = %v", err)
	}
}

func TestBuildMountInfoRetainsForeignMountAtDriverTarget(t *testing.T) {
	text := strings.Replace(validMountInfoText(), "37 25 0:42 /kubernetes-volumes/tenant--claim--0123456789ab", "37 25 8:1 /foreign", 1)
	text = strings.Replace(text, "- virtiofs "+mountInfoParentID+" rw", "- ext4 /dev/sda1 rw", 1)
	raw, err := ParseMountInfo(strings.NewReader(text))
	if err != nil {
		t.Fatalf("ParseMountInfo() error = %v", err)
	}
	table, err := BuildTableFromMountInfo(raw, mountInfoParentRoot, mountInfoKubelet, mountInfoDriver)
	if err != nil {
		t.Fatalf("BuildTableFromMountInfo() error = %v", err)
	}
	stage := mountInfoKubelet + "/plugins/kubernetes.io/csi/" + mountInfoDriver + "/volume-a/globalmount"
	if _, err := ValidateStage(table, mountInfoParentRoot+"/"+mountInfoParentID, stage, mountMapping(), mountCapability()); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("ValidateStage(foreign mount) error = %v", err)
	}
}

func TestBuildMountInfoRetainsForeignDescendantsBelowProtectedRoots(t *testing.T) {
	parent := mountInfoParentRoot + "/" + mountInfoParentID
	publish := mountInfoKubelet + "/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount"
	text := validMountInfoText() + "\n" + strings.Join([]string{
		"39 36 8:1 / " + parent + "/foreign-child rw - ext4 /dev/sda1 rw",
		"40 38 0:99 / " + publish + "/nested rw - tmpfs tmpfs rw",
	}, "\n")
	raw, err := ParseMountInfo(strings.NewReader(text))
	if err != nil {
		t.Fatalf("ParseMountInfo() error = %v", err)
	}
	table, err := BuildTableFromMountInfo(raw, mountInfoParentRoot, mountInfoKubelet, mountInfoDriver)
	if err != nil {
		t.Fatalf("BuildTableFromMountInfo() error = %v", err)
	}
	foreign := 0
	for _, entry := range table.Entries {
		if entry.Kind == KindForeign {
			foreign++
		}
	}
	if foreign != 2 {
		t.Fatalf("foreign descendants = %d, table = %#v", foreign, table.Entries)
	}
}

func TestParseMountInfoDecodesEscapesAndRejectsMalformedInput(t *testing.T) {
	line := "1 2 0:1 / /var/lib/escaped\\040path rw - tmpfs tmpfs rw"
	entries, err := ParseMountInfo(strings.NewReader(line))
	if err != nil {
		t.Fatalf("ParseMountInfo(escaped) error = %v", err)
	}
	if entries[0].MountPoint != "/var/lib/escaped path" {
		t.Fatalf("decoded mount point = %q", entries[0].MountPoint)
	}
	for _, invalid := range []string{
		"",
		"1 2 0:1 / /target rw tmpfs tmpfs rw",
		"zero 2 0:1 / /target rw - tmpfs tmpfs rw",
		"1 2 device / /target rw - tmpfs tmpfs rw",
		"1 2 0:1 / /target\\999 rw - tmpfs tmpfs rw",
		"1 1 0:1 / / rw - tmpfs tmpfs rw\n1 1 0:2 / /other rw - tmpfs tmpfs rw",
	} {
		if _, err := ParseMountInfo(strings.NewReader(invalid)); err == nil {
			t.Fatalf("ParseMountInfo(%q) error = nil", invalid)
		}
	}
}

func TestParseMountInfoAcceptsOnlyClosedNSFSNamespaceRoots(t *testing.T) {
	line := "617 594 0:4 mnt:[4026532372] /run/snapd/ns/lxd.mnt rw - nsfs nsfs rw"
	entries, err := ParseMountInfo(strings.NewReader(line))
	if err != nil {
		t.Fatalf("ParseMountInfo(nsfs) error = %v", err)
	}
	if len(entries) != 1 || entries[0].Root != "mnt:[4026532372]" || entries[0].FilesystemType != "nsfs" {
		t.Fatalf("ParseMountInfo(nsfs) entries = %#v", entries)
	}
	for _, invalid := range []string{
		"1 2 0:1 relative /target rw - tmpfs tmpfs rw",
		"1 2 0:1 mnt:[] /target rw - nsfs nsfs rw",
		"1 2 0:1 mnt:[0] /target rw - nsfs nsfs rw",
		"1 2 0:1 MNT:[1] /target rw - nsfs nsfs rw",
		"1 2 0:1 mnt:[1] /target rw - nsfs foreign rw",
		"1 2 0:1 ../mnt:[1] /target rw - nsfs nsfs rw",
	} {
		if _, err := ParseMountInfo(strings.NewReader(invalid)); err == nil {
			t.Fatalf("ParseMountInfo(%q) error = nil", invalid)
		}
	}
}

func TestParseMountInfoRejectsSnapshotBeyondAggregateBound(t *testing.T) {
	first := "1 1 0:1 / / rw - tmpfs tmpfs rw"
	second := "2 1 0:1 / /other rw - tmpfs tmpfs rw"
	if _, err := parseMountInfoBounded(strings.NewReader(first+"\n"+second), len(first)+1); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("parseMountInfoBounded() error = %v", err)
	}
}
