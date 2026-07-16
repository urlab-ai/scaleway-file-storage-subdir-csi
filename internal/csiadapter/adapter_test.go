package csiadapter

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	testDriverName = "file-storage-subdir.csi.urlab.ai"
	testVolumeID   = "sfs1:lv-11111111111111111111111111111111:mh-22222222222222222222222222222222"
	testNodeID     = "fr-par-1/33333333-3333-4333-8333-333333333333"
)

type fakeCreateCore struct {
	request driver.CreateRequest
	result  driver.CreateResponse
	err     error
	calls   int
}

func (core *fakeCreateCore) Create(_ context.Context, request driver.CreateRequest) (driver.CreateResponse, error) {
	core.calls++
	core.request = request
	return core.result, core.err
}

type fakeDeleteCore struct {
	volumeID string
	err      error
	calls    int
}

func (core *fakeDeleteCore) Delete(_ context.Context, volumeID string) error {
	core.calls++
	core.volumeID = volumeID
	return core.err
}

type fakePublishCore struct {
	publishRequest driver.PublishRequest
	unpublishID    string
	unpublishNode  string
	publishErr     error
	unpublishErr   error
	publishCalls   int
	unpublishCalls int
}

func (core *fakePublishCore) Publish(_ context.Context, request driver.PublishRequest) error {
	core.publishCalls++
	core.publishRequest = request
	return core.publishErr
}

func (core *fakePublishCore) Unpublish(_ context.Context, volumeID, nodeID string) error {
	core.unpublishCalls++
	core.unpublishID, core.unpublishNode = volumeID, nodeID
	return core.unpublishErr
}

type fakeValidateCore struct {
	request driver.ValidateCapabilitiesRequest
	result  driver.ValidateCapabilitiesResult
	err     error
	calls   int
}

func (core *fakeValidateCore) Validate(_ context.Context, request driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	core.calls++
	core.request = request
	return core.result, core.err
}

type fakeNodeCore struct {
	info         driver.NodeInfo
	stageErr     error
	unstageErr   error
	publishErr   error
	unpublishErr error
	stageCalls   int
	publishCalls int
	capability   volume.Capability
	readOnly     bool
}

func (core *fakeNodeCore) GetInfo() driver.NodeInfo { return core.info }

func (core *fakeNodeCore) Stage(_ context.Context, _ string, _ map[string]string, _ string, capability volume.Capability) error {
	core.stageCalls++
	core.capability = capability
	return core.stageErr
}

func (core *fakeNodeCore) Unstage(context.Context, string, string) error { return core.unstageErr }

func (core *fakeNodeCore) Publish(_ context.Context, _ string, _ map[string]string, _, _ string, capability volume.Capability, readOnly bool) error {
	core.publishCalls++
	core.capability, core.readOnly = capability, readOnly
	return core.publishErr
}

func (core *fakeNodeCore) Unpublish(context.Context, string, string) error { return core.unpublishErr }

func testPool(t *testing.T) pool.Config {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	return pool.Config{
		Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
		Filesystems: []pool.ParentConfig{{ID: "44444444-4444-4444-8444-444444444444", Name: "parent", State: pool.ParentActive}},
	}
}

func mountCapability(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

func blockCapability(mode csi.VolumeCapability_AccessMode_Mode) *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: mode},
	}
}

func testController(t *testing.T) (*ControllerServer, *fakeCreateCore, *fakeDeleteCore, *fakePublishCore, *fakeValidateCore) {
	t.Helper()
	create := &fakeCreateCore{result: driver.CreateResponse{VolumeHandle: testVolumeID, CapacityBytes: 1024, VolumeContext: map[string]string{"schemaVersion": "1"}}}
	deleteCore := &fakeDeleteCore{}
	publish := &fakePublishCore{}
	validate := &fakeValidateCore{result: driver.ValidateCapabilitiesResult{Confirmed: true}}
	server, err := NewControllerServer(ControllerCores{Create: create, Delete: deleteCore, Publish: publish, Validate: validate}, []pool.Config{testPool(t)})
	if err != nil {
		t.Fatalf("NewControllerServer() error = %v", err)
	}
	return server, create, deleteCore, publish, validate
}

func TestIdentityWireContractIsExactAndProbeIsCached(t *testing.T) {
	readiness := &driver.Readiness{}
	if err := readiness.Set(false, "startup reconciliation incomplete"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	core, err := driver.NewIdentityServiceCore(testDriverName, "1.2.3", readiness)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore() error = %v", err)
	}
	server, err := NewIdentityServer(core)
	if err != nil {
		t.Fatalf("NewIdentityServer() error = %v", err)
	}
	info, _ := server.GetPluginInfo(context.Background(), nil)
	if info.Name != testDriverName || info.VendorVersion != "1.2.3" {
		t.Fatalf("GetPluginInfo() = %#v", info)
	}
	capabilities, _ := server.GetPluginCapabilities(context.Background(), nil)
	if len(capabilities.Capabilities) != 1 || capabilities.Capabilities[0].GetService().Type != csi.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Fatalf("GetPluginCapabilities() = %#v", capabilities)
	}
	probe, _ := server.Probe(context.Background(), nil)
	if probe.Ready == nil || probe.Ready.Value {
		t.Fatalf("Probe(unready) = %#v", probe)
	}
	if err := readiness.Set(true, ""); err != nil {
		t.Fatalf("Set(ready) error = %v", err)
	}
	probe, _ = server.Probe(context.Background(), nil)
	if probe.Ready == nil || !probe.Ready.Value {
		t.Fatalf("Probe(ready) = %#v", probe)
	}
}

func TestCreateVolumeTranslatesClosedParametersCapabilitiesAndCapacity(t *testing.T) {
	server, create, _, _, _ := testController(t)
	request := &csi.CreateVolumeRequest{
		Name: "pvc-request", CapacityRange: &csi.CapacityRange{RequiredBytes: 512, LimitBytes: 2048},
		VolumeCapabilities: []*csi.VolumeCapability{
			mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
			mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER),
		},
		Parameters: map[string]string{
			parameterPoolName: "standard", parameterDeletePolicy: "retain", parameterDirectoryUID: "42",
			parameterPVCNamespace: "tenant-a", parameterPVCName: "data",
		},
	}
	response, err := server.CreateVolume(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	if response.Volume == nil || response.Volume.VolumeId != testVolumeID || response.Volume.CapacityBytes != 1024 {
		t.Fatalf("CreateVolume() = %#v", response)
	}
	if create.calls != 1 || create.request.RequiredBytes != 512 || create.request.LimitBytes != 2048 || create.request.PVCNamespace != "tenant-a" || create.request.PVCName != "data" {
		t.Fatalf("core request = %#v", create.request)
	}
	if create.request.Parameters.DeletePolicy != volume.DeletePolicyRetain || create.request.Parameters.DirectoryUID != 42 || create.request.Parameters.DirectoryGID != 1000 || create.request.Parameters.DirectoryMode != "0770" {
		t.Fatalf("resolved parameters = %#v", create.request.Parameters)
	}
	wantModes := []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter, volume.AccessModeSingleNodeWriter}
	if !reflect.DeepEqual(create.request.Parameters.AccessModes, wantModes) {
		t.Fatalf("access modes = %#v, want %#v", create.request.Parameters.AccessModes, wantModes)
	}
	response.Volume.VolumeContext["schemaVersion"] = "changed"
	if create.result.VolumeContext["schemaVersion"] != "1" {
		t.Fatal("CreateVolume response aliases core volume context")
	}
}

func TestCreateVolumeRejectsUnsupportedOrAmbiguousWireInputBeforeCore(t *testing.T) {
	server, create, _, _, _ := testController(t)
	base := func() *csi.CreateVolumeRequest {
		return &csi.CreateVolumeRequest{Name: "pvc", VolumeCapabilities: []*csi.VolumeCapability{mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)}, Parameters: map[string]string{parameterPoolName: "standard"}}
	}
	tests := map[string]func(*csi.CreateVolumeRequest){
		"block": func(request *csi.CreateVolumeRequest) {
			request.VolumeCapabilities[0] = blockCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
		},
		"unknown param":  func(request *csi.CreateVolumeRequest) { request.Parameters["secretKey"] = "value" },
		"content source": func(request *csi.CreateVolumeRequest) { request.VolumeContentSource = &csi.VolumeContentSource{} },
		"topology": func(request *csi.CreateVolumeRequest) {
			request.AccessibilityRequirements = &csi.TopologyRequirement{Requisite: []*csi.Topology{{}}}
		},
		"mutable": func(request *csi.CreateVolumeRequest) { request.MutableParameters = map[string]string{"x": "y"} },
		"secret":  func(request *csi.CreateVolumeRequest) { request.Secrets = map[string]string{"x": "y"} },
		"bad capacity": func(request *csi.CreateVolumeRequest) {
			request.CapacityRange = &csi.CapacityRange{RequiredBytes: 2, LimitBytes: 1}
		},
		"noncanonical uid": func(request *csi.CreateVolumeRequest) { request.Parameters[parameterDirectoryUID] = "01" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			before := create.calls
			request := base()
			mutate(request)
			_, err := server.CreateVolume(context.Background(), request)
			if err == nil || (status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.OutOfRange) {
				t.Fatalf("CreateVolume() error = %v", err)
			}
			if create.calls != before {
				t.Fatal("invalid request reached Create core")
			}
		})
	}
}

func TestValidateCapabilitiesPreservesRPCSpecificUnsupportedResponse(t *testing.T) {
	server, _, _, _, validate := testController(t)
	unsupported := &csi.ValidateVolumeCapabilitiesRequest{VolumeId: testVolumeID, VolumeCapabilities: []*csi.VolumeCapability{blockCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)}}
	response, err := server.ValidateVolumeCapabilities(context.Background(), unsupported)
	if err != nil || response.Confirmed != nil || response.Message == "" || validate.calls != 0 {
		t.Fatalf("ValidateVolumeCapabilities(unsupported) = %#v, %v, calls=%d", response, err, validate.calls)
	}

	request := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: testVolumeID, VolumeContext: map[string]string{"schemaVersion": "1"},
		VolumeCapabilities: []*csi.VolumeCapability{mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)},
		Parameters:         map[string]string{parameterPoolName: "standard"},
	}
	response, err = server.ValidateVolumeCapabilities(context.Background(), request)
	if err != nil || response.Confirmed == nil || len(response.Confirmed.VolumeCapabilities) != 1 {
		t.Fatalf("ValidateVolumeCapabilities(confirmed) = %#v, %v", response, err)
	}
	if validate.request.Parameters == nil || validate.request.Parameters.PoolName != "standard" {
		t.Fatalf("ValidateVolumeCapabilities parameters = %#v", validate.request.Parameters)
	}
	validate.err = k8s.ErrNotFound
	if _, err := server.ValidateVolumeCapabilities(context.Background(), request); status.Code(err) != codes.NotFound {
		t.Fatalf("ValidateVolumeCapabilities(NotFound) error = %v", err)
	}
}

func TestControllerPublishAndCapabilitiesUseExactV1Surface(t *testing.T) {
	server, _, _, publish, _ := testController(t)
	capabilities, _ := server.ControllerGetCapabilities(context.Background(), nil)
	if len(capabilities.Capabilities) != 2 || capabilities.Capabilities[0].GetRpc().Type != csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME || capabilities.Capabilities[1].GetRpc().Type != csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME {
		t.Fatalf("ControllerGetCapabilities() = %#v", capabilities)
	}
	request := &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolumeID, NodeId: testNodeID, VolumeContext: map[string]string{"schemaVersion": "1"},
		VolumeCapability: mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	}
	response, err := server.ControllerPublishVolume(context.Background(), request)
	if err != nil || response.PublishContext == nil || len(response.PublishContext) != 0 || publish.publishCalls != 1 {
		t.Fatalf("ControllerPublishVolume() = %#v, %v, calls=%d", response, err, publish.publishCalls)
	}
	request.VolumeCapability = blockCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
	if _, err := server.ControllerPublishVolume(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ControllerPublishVolume(block) error = %v", err)
	}
	publish.publishErr = driver.ErrSingleNodeConflict
	request.VolumeCapability = mountCapability(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)
	if _, err := server.ControllerPublishVolume(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ControllerPublishVolume(conflict) error = %v", err)
	}
}

func TestNodeWireContractOmitsPhysicalLimitAndMapsMountConflict(t *testing.T) {
	core := &fakeNodeCore{info: driver.NodeInfo{NodeID: testNodeID}}
	server, err := NewNodeServer(core)
	if err != nil {
		t.Fatalf("NewNodeServer() error = %v", err)
	}
	info, _ := server.NodeGetInfo(context.Background(), nil)
	if info.NodeId != testNodeID || info.MaxVolumesPerNode != 0 || info.AccessibleTopology != nil {
		t.Fatalf("NodeGetInfo() = %#v", info)
	}
	capabilities, _ := server.NodeGetCapabilities(context.Background(), nil)
	if len(capabilities.Capabilities) != 1 || capabilities.Capabilities[0].GetRpc().Type != csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME {
		t.Fatalf("NodeGetCapabilities() = %#v", capabilities)
	}
	request := &csi.NodeStageVolumeRequest{
		VolumeId: testVolumeID, StagingTargetPath: "/staging", VolumeContext: map[string]string{"schemaVersion": "1"},
		VolumeCapability: mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	}
	if _, err := server.NodeStageVolume(context.Background(), request); err != nil || core.stageCalls != 1 {
		t.Fatalf("NodeStageVolume() error = %v, calls=%d", err, core.stageCalls)
	}
	request.VolumeCapability = blockCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)
	if _, err := server.NodeStageVolume(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("NodeStageVolume(block) error = %v", err)
	}
	core.publishErr = mount.ErrMountConflict
	publish := &csi.NodePublishVolumeRequest{
		VolumeId: testVolumeID, StagingTargetPath: "/staging", TargetPath: "/target", Readonly: true,
		VolumeContext: map[string]string{"schemaVersion": "1"}, VolumeCapability: mountCapability(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER),
	}
	if _, err := server.NodePublishVolume(context.Background(), publish); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("NodePublishVolume(mount conflict) error = %v", err)
	}
	if !core.readOnly {
		t.Fatal("NodePublishVolume did not preserve readonly")
	}
	core.unstageErr = driver.ErrNodePrecondition
	if _, err := server.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: testVolumeID, StagingTargetPath: "/staging",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("NodeUnstageVolume(child publish) error = %v", err)
	}
}

func TestCoreErrorMappingIsBoundedAndFailClosed(t *testing.T) {
	tests := map[error]codes.Code{
		context.Canceled: contextCode(codes.Canceled), context.DeadlineExceeded: codes.DeadlineExceeded,
		k8s.ErrUnavailable: codes.Unavailable, driver.ErrVolumeInUse: codes.FailedPrecondition,
		pool.ErrNoLogicalCapacity: codes.ResourceExhausted, pool.ErrPhysicalCapacityExhausted: codes.ResourceExhausted,
		volume.ErrCapacityOutOfRange: codes.OutOfRange, volume.ErrInvalidHandle: codes.InvalidArgument,
		volume.ErrForeignHandle: codes.InvalidArgument, volume.ErrInvalidContext: codes.InvalidArgument,
		volume.ErrContextMismatch: codes.InvalidArgument, driver.ErrInvalidNodePath: codes.InvalidArgument,
		driver.ErrCapabilityMismatch: codes.FailedPrecondition, driver.ErrStagingPrerequisite: codes.FailedPrecondition,
		driver.ErrNodePrecondition: codes.FailedPrecondition, safety.ErrUnsafeLivePath: codes.FailedPrecondition,
		safety.ErrTargetConflict:  codes.AlreadyExists,
		mount.ErrMountUnavailable: codes.Unavailable,
		errors.Join(driver.ErrStagingPrerequisite, mount.ErrMountConflict): codes.FailedPrecondition,
		errors.New("unknown invariant"):                                    codes.Internal,
	}
	for input, want := range tests {
		if got := status.Code(mapCoreError(input)); got != want {
			t.Errorf("mapCoreError(%v) = %s, want %s", input, got, want)
		}
	}
	message := boundedStatusMessage(string(make([]byte, maxStatusMessageBytes+100)))
	if len(message) > maxStatusMessageBytes || message == "" {
		t.Fatalf("bounded status message length = %d", len(message))
	}
}

func contextCode(code codes.Code) codes.Code { return code }

func TestGRPCServerRegistersOnlyTheSelectedComponentAndDrains(t *testing.T) {
	readiness := &driver.Readiness{}
	if err := readiness.Set(true, ""); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	identityCore, err := driver.NewIdentityServiceCore(testDriverName, "1.2.3", readiness)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore() error = %v", err)
	}
	identity, err := NewIdentityServer(identityCore)
	if err != nil {
		t.Fatalf("NewIdentityServer() error = %v", err)
	}
	node, err := NewNodeServer(&fakeNodeCore{info: driver.NodeInfo{NodeID: testNodeID}})
	if err != nil {
		t.Fatalf("NewNodeServer() error = %v", err)
	}
	server, err := NewGRPCServer(config.ComponentNode, identity, nil, node)
	if err != nil {
		t.Fatalf("NewGRPCServer() error = %v", err)
	}
	listener := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, time.Second) }()

	connection, err := grpc.NewClient("passthrough:///csi", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		cancel()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close gRPC client connection: %v", err)
		}
	})
	callCtx, callCancel := context.WithTimeout(context.Background(), time.Second)
	defer callCancel()
	info, err := csi.NewIdentityClient(connection).GetPluginInfo(callCtx, &csi.GetPluginInfoRequest{})
	if err != nil || info.Name != testDriverName {
		cancel()
		t.Fatalf("GetPluginInfo() = %#v, %v", info, err)
	}
	nodeInfo, err := csi.NewNodeClient(connection).NodeGetInfo(callCtx, &csi.NodeGetInfoRequest{})
	if err != nil || nodeInfo.NodeId != testNodeID {
		cancel()
		t.Fatalf("NodeGetInfo() = %#v, %v", nodeInfo, err)
	}
	if _, err := csi.NewControllerClient(connection).ControllerGetCapabilities(callCtx, &csi.ControllerGetCapabilitiesRequest{}); status.Code(err) != codes.Unimplemented {
		cancel()
		t.Fatalf("controller RPC on node socket error = %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not drain after cancellation")
	}
}
