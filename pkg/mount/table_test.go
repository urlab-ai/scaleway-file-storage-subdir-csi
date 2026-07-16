package mount

import (
	"errors"
	"path"
	"strings"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func mountMapping() volume.Mapping {
	return volume.Mapping{
		PoolName: "standard", ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		BasePath: "/kubernetes-volumes", DirectoryName: "tenant--claim--0123456789ab",
		LogicalVolumeID: "lv-cba6af669a8d67780b6f36aecd3c58af",
	}
}

func mountCapability() volume.Capability {
	return volume.Capability{AccessMode: volume.AccessModeMultiNodeMultiWriter, AccessType: "mount", FilesystemType: "virtiofs", MountFlags: []string{}}
}

func exactMountTable() (Table, string, string, string) {
	mapping := mountMapping()
	parent := "/var/lib/scaleway-sfs-subdir-csi/parents/" + mapping.ParentFilesystemID
	stage := "/var/lib/kubelet/plugins/kubernetes.io/csi/sfs-subdir.csi.example.com/volume/globalmount"
	publish := "/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount"
	backing := path.Join(mapping.BasePath, mapping.DirectoryName)
	return Table{Entries: []Entry{
		{MountID: 1, DeviceID: "0:42", Kind: KindParent, Target: parent, SourcePath: mapping.ParentFilesystemID, FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID, ParentFilesystemID: mapping.ParentFilesystemID, BackingRelativePath: "/"},
		{MountID: 2, ParentMountID: 1, DeviceID: "0:42", Kind: KindStage, Target: stage, SourcePath: path.Join(parent, strings.TrimPrefix(mapping.BasePath, "/"), mapping.DirectoryName), FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID, ParentFilesystemID: mapping.ParentFilesystemID, BackingRelativePath: backing, AccessMode: volume.AccessModeMultiNodeMultiWriter},
		{MountID: 3, ParentMountID: 2, DeviceID: "0:42", Kind: KindPublish, Target: publish, SourcePath: stage, FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID, ParentFilesystemID: mapping.ParentFilesystemID, BackingRelativePath: backing, AccessMode: volume.AccessModeMultiNodeMultiWriter},
	}}, parent, stage, publish
}

func TestValidateExactParentStageAndPublishGraph(t *testing.T) {
	table, parent, stage, publish := exactMountTable()
	if _, err := ValidateParent(table, parent, mountMapping().ParentFilesystemID); err != nil {
		t.Fatalf("ValidateParent() error = %v", err)
	}
	if _, err := ValidateStage(table, parent, stage, mountMapping(), mountCapability()); err != nil {
		t.Fatalf("ValidateStage() error = %v", err)
	}
	if _, err := ValidatePublish(table, stage, publish, mountMapping(), mountCapability(), false); err != nil {
		t.Fatalf("ValidatePublish() error = %v", err)
	}
}

func TestValidatePublishRejectsReadOnlyMismatchAndStackedMount(t *testing.T) {
	table, _, stage, publish := exactMountTable()
	if _, err := ValidatePublish(table, stage, publish, mountMapping(), mountCapability(), true); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("ValidatePublish(read-only mismatch) error = %v", err)
	}
	stack := table.Entries[len(table.Entries)-1]
	stack.MountID = 4
	table.Entries = append(table.Entries, stack)
	if _, err := ValidatePublish(table, stage, publish, mountMapping(), mountCapability(), false); !errors.Is(err, ErrStackedMount) {
		t.Fatalf("ValidatePublish(stacked) error = %v", err)
	}
}

func TestValidateStageUsesKernelDeviceIdentityNotUnobservableBindSourcePath(t *testing.T) {
	table, parent, stage, _ := exactMountTable()
	for index := range table.Entries {
		if table.Entries[index].Target == stage {
			table.Entries[index].SourcePath = "/alias/to/logical-directory"
		}
	}
	if _, err := ValidateStage(table, parent, stage, mountMapping(), mountCapability()); err != nil {
		t.Fatalf("ValidateStage(unobservable source path) error = %v", err)
	}
	for index := range table.Entries {
		if table.Entries[index].Target == stage {
			table.Entries[index].DeviceID = "0:99"
		}
	}
	if _, err := ValidateStage(table, parent, stage, mountMapping(), mountCapability()); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("ValidateStage(foreign device) error = %v", err)
	}
}
