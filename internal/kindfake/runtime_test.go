package kindfake

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func validFakeOptions() Options {
	return Options{
		Component: config.ComponentNode, CSIEndpointPath: "/csi/csi.sock",
		DriverName: "file-storage-subdir.csi.urlab.ai", NodeName: "kind-control-plane",
		DataRoot:    "/var/lib/scaleway-sfs-subdir-csi/parents/.kind-fake-data",
		KubeletPath: "/var/lib/kubelet", LiveAddress: ":9811",
	}
}

func fakeCreateRequest() driver.CreateRequest {
	return driver.CreateRequest{
		Name: "pvc-aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", RequiredBytes: 16 << 20,
		PVCNamespace: "workloads", PVCName: "shared-data",
		Parameters: volume.CreateParameters{
			PoolName: fakePoolName, DeletePolicy: volume.DeletePolicyArchive,
			DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
			AccessType: "mount", FilesystemType: "virtiofs",
			AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
		},
	}
}

func TestOptionsRejectsUnsafeIntegrationScope(t *testing.T) {
	valid := validFakeOptions()
	if err := valid.Validate(); err != nil {
		t.Fatalf("Options.Validate() error = %v", err)
	}
	for name, mutate := range map[string]func(*Options){
		"component":     func(value *Options) { value.Component = "both" },
		"relative data": func(value *Options) { value.DataRoot = "relative" },
		"overlap":       func(value *Options) { value.DataRoot = value.KubeletPath + "/data" },
		"node":          func(value *Options) { value.NodeName = "bad/node" },
		"listener":      func(value *Options) { value.LiveAddress = ":not-a-port" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := valid
			mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("Options.Validate(invalid) error = nil")
			}
		})
	}
}

func TestControllerCoreDerivesStableValidVolume(t *testing.T) {
	controller := &controllerCore{driverName: validFakeOptions().DriverName}
	request := fakeCreateRequest()
	first, err := controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	second, err := controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create(retry) error = %v", err)
	}
	if !reflect.DeepEqual(first, second) || first.CapacityBytes != request.RequiredBytes {
		t.Fatalf("Create() results differ: %#v / %#v", first, second)
	}
	if err := validateHandleContext(first.VolumeHandle, first.VolumeContext); err != nil {
		t.Fatalf("validateHandleContext() error = %v", err)
	}
	capability := volume.Capability{
		AccessType: "mount", FilesystemType: "virtiofs", AccessMode: volume.AccessModeMultiNodeMultiWriter,
	}
	if err := controller.Publish(context.Background(), driver.PublishRequest{
		VolumeHandle: first.VolumeHandle, VolumeContext: first.VolumeContext,
		NodeID: deterministicNodeID("kind-control-plane"), Capability: capability,
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	validated, err := controller.Validate(context.Background(), driver.ValidateCapabilitiesRequest{
		VolumeHandle: first.VolumeHandle, VolumeContext: first.VolumeContext,
		Capabilities: []volume.Capability{capability},
	})
	if err != nil || !validated.Confirmed {
		t.Fatalf("Validate() = %#v, %v", validated, err)
	}
	if err := controller.Delete(context.Background(), "foreign.example/volume"); err != nil {
		t.Fatalf("Delete(foreign) error = %v", err)
	}
}

type recordedMountBackend struct {
	ensures  []fakeMountOperation
	unmounts []fakeMountOperation
}

type fakeMountOperation struct {
	source, target string
	flag           bool
}

func (backend *recordedMountBackend) EnsureBind(_ context.Context, source, target string, readOnly bool) error {
	backend.ensures = append(backend.ensures, fakeMountOperation{source: source, target: target, flag: readOnly})
	return nil
}

func (backend *recordedMountBackend) UnmountBind(_ context.Context, source, target string, removeTarget bool) error {
	backend.unmounts = append(backend.unmounts, fakeMountOperation{source: source, target: target, flag: removeTarget})
	return nil
}

func TestNodeCoreUsesOnlyValidatedKubeletTargets(t *testing.T) {
	options := validFakeOptions()
	mounts := &recordedMountBackend{}
	coreValue, err := newPortableNodeCore(options, mounts)
	if err != nil {
		t.Fatalf("newPortableNodeCore() error = %v", err)
	}
	node := coreValue.(*nodeCore)
	controller := &controllerCore{driverName: options.DriverName}
	created, err := controller.Create(context.Background(), fakeCreateRequest())
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	capability := volume.Capability{
		AccessType: "mount", FilesystemType: "virtiofs", AccessMode: volume.AccessModeMultiNodeMultiWriter,
	}
	stage := filepath.Join(options.KubeletPath, "plugins/kubernetes.io/csi", options.DriverName, "volume-a", "globalmount")
	target := filepath.Join(options.KubeletPath, "pods", "pod-a", "volumes", "kubernetes.io~csi", "pv-a", "mount")
	if err := node.Stage(context.Background(), created.VolumeHandle, created.VolumeContext, stage, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := node.Publish(context.Background(), created.VolumeHandle, created.VolumeContext, stage, target, capability, true); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := node.Unpublish(context.Background(), created.VolumeHandle, target); err != nil {
		t.Fatalf("Unpublish() error = %v", err)
	}
	if err := node.Unstage(context.Background(), created.VolumeHandle, stage); err != nil {
		t.Fatalf("Unstage() error = %v", err)
	}
	handle, _ := volume.ParseHandle(created.VolumeHandle)
	source := filepath.Join(options.DataRoot, handle.LogicalVolumeID)
	wantEnsures := []fakeMountOperation{
		{source: source, target: stage},
		{source: source, target: stage},
		{source: stage, target: target, flag: true},
	}
	wantUnmounts := []fakeMountOperation{
		{source: source, target: target, flag: true},
		{source: source, target: stage},
	}
	if !reflect.DeepEqual(mounts.ensures, wantEnsures) || !reflect.DeepEqual(mounts.unmounts, wantUnmounts) {
		t.Fatalf("mount operations = %#v / %#v", mounts.ensures, mounts.unmounts)
	}
	if err := node.Publish(context.Background(), created.VolumeHandle, created.VolumeContext, stage, "/tmp/foreign", capability, false); err == nil {
		t.Fatal("Publish(foreign target) error = nil")
	}
}
