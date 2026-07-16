package csiadapter

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestExtractCSILogContextUsesOnlyValidatedIdentityAndPaths(t *testing.T) {
	mapping := volume.Mapping{
		PoolName: "standard", ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		BasePath: "/kubernetes-volumes", DirectoryName: "pvc-a-lv-0123456789ab",
		LogicalVolumeID: "lv-0123456789abcdef0123456789abcdef",
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	request := &csi.NodePublishVolumeRequest{
		VolumeId: handle.String(), StagingTargetPath: "/var/lib/kubelet/plugins/staging",
		TargetPath: "/var/lib/kubelet/pods/target",
	}
	context := extractCSILogContext(request, nil)
	if context.logicalVolumeID != mapping.LogicalVolumeID || context.stagingPath != request.StagingTargetPath || context.targetPath != request.TargetPath {
		t.Fatalf("log context = %#v", context)
	}

	invalid := extractCSILogContext(&csi.ControllerPublishVolumeRequest{
		VolumeId: "foreign-secret-value", NodeId: "not-a-node",
	}, nil)
	if invalid.logicalVolumeID != "" || invalid.nodeID != "" {
		t.Fatalf("invalid identities escaped log validation: %#v", invalid)
	}
}
