package csisanity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"

	"scaleway-sfs-subdir-csi/internal/csiadapter"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	sanityDriverName = "sfs-subdir.csi.example.com"
	sanityNodeID     = "fr-par-1/11111111-1111-4111-8111-111111111111"
)

type driverIDGenerator struct{ counter atomic.Uint64 }

func (generator *driverIDGenerator) GenerateUniqueValidVolumeID() string {
	return sanityVolumeHandle(fmt.Sprintf("generated-%d", generator.counter.Add(1)))
}

func (*driverIDGenerator) GenerateInvalidVolumeID() string { return "invalid-volume-id" }

func (generator *driverIDGenerator) GenerateUniqueValidNodeID() string {
	value := generator.counter.Add(1)
	return fmt.Sprintf("fr-par-1/00000000-0000-4000-8000-%012d", value)
}

func (*driverIDGenerator) GenerateInvalidNodeID() string { return "invalid-node-id" }

func TestCSIControllerAndNodeSanity(t *testing.T) {
	temporaryRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks(temp root) error = %v", err)
	}
	temporary, err := os.MkdirTemp(temporaryRoot, "sfs-csi-sanity-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporary) })
	if err := os.Chmod(temporary, 0o755); err != nil {
		t.Fatalf("Chmod(sanity socket directory) error = %v", err)
	}
	controllerSocket := filepath.Join(temporary, "controller.sock")
	nodeSocket := filepath.Join(temporary, "node.sock")
	controllerListener, err := driver.ListenCSIUnix(controllerSocket)
	if err != nil {
		t.Fatalf("ListenCSIUnix(controller) error = %v", err)
	}
	nodeListener, err := driver.ListenCSIUnix(nodeSocket)
	if err != nil {
		t.Fatalf("ListenCSIUnix(node) error = %v", errors.Join(err, controllerListener.Close()))
	}

	readiness := &driver.Readiness{}
	if err := readiness.Set(true, ""); err != nil {
		t.Fatalf("readiness.Set() error = %v", err)
	}
	identityCore, err := driver.NewIdentityServiceCore(sanityDriverName, "1.0.0", readiness)
	if err != nil {
		t.Fatalf("NewIdentityServiceCore() error = %v", err)
	}
	controllerIdentity, err := csiadapter.NewIdentityServer(identityCore)
	if err != nil {
		t.Fatalf("NewIdentityServer(controller) error = %v", err)
	}
	nodeIdentity, err := csiadapter.NewIdentityServer(identityCore)
	if err != nil {
		t.Fatalf("NewIdentityServer(node) error = %v", err)
	}
	kubeletRoot := filepath.Join(temporary, "kubelet")
	parentRoot := filepath.Join(temporary, "parents")
	core, nodeCore, err := newSanityProductionHarness(kubeletRoot, parentRoot)
	if err != nil {
		t.Fatalf("newSanityProductionHarness() error = %v", err)
	}
	controllerServer, err := csiadapter.NewControllerServer(csiadapter.ControllerCores{
		Create: core, Delete: core, Publish: core, Validate: core,
	}, []pool.Config{sanityPool(t)})
	if err != nil {
		t.Fatalf("NewControllerServer() error = %v", err)
	}
	nodeServer, err := csiadapter.NewNodeServer(nodeCore)
	if err != nil {
		t.Fatalf("NewNodeServer() error = %v", err)
	}
	controllerGRPC, err := csiadapter.NewGRPCServer(config.ComponentController, controllerIdentity, controllerServer, nil)
	if err != nil {
		t.Fatalf("NewGRPCServer(controller) error = %v", err)
	}
	nodeGRPC, err := csiadapter.NewGRPCServer(config.ComponentNode, nodeIdentity, nil, nodeServer)
	if err != nil {
		t.Fatalf("NewGRPCServer(node) error = %v", err)
	}

	serveCtx, cancel := context.WithCancel(context.Background())
	controllerResult := make(chan error, 1)
	nodeResult := make(chan error, 1)
	go func() { controllerResult <- controllerGRPC.Serve(serveCtx, controllerListener, 2*time.Second) }()
	go func() { nodeResult <- nodeGRPC.Serve(serveCtx, nodeListener, 2*time.Second) }()
	defer func() {
		cancel()
		if err := <-controllerResult; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stop controller sanity server: %v", err)
		}
		if err := <-nodeResult; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stop node sanity server: %v", err)
		}
	}()

	configuration := sanity.NewTestConfig()
	configuration.Address = "unix://" + nodeSocket
	configuration.ControllerAddress = "unix://" + controllerSocket
	configuration.TargetPath = filepath.Join(kubeletRoot, "pods/pod-sanity/volumes/kubernetes.io~csi/pv-sanity")
	configuration.StagingPath = filepath.Join(kubeletRoot, "plugins/kubernetes.io/csi", sanityDriverName, "volume-sanity/globalmount")
	// csi-test intentionally uses os.Mkdir rather than MkdirAll for its mount
	// fixtures. Create only their kubelet-owned parents; the suite continues to
	// create and remove the actual target and staging paths for every case.
	for _, parent := range []string{filepath.Dir(configuration.TargetPath), filepath.Dir(configuration.StagingPath)} {
		if err := os.MkdirAll(parent, 0o755); err != nil {
			t.Fatalf("MkdirAll(sanity kubelet fixture parent) error = %v", err)
		}
	}
	configuration.TestVolumeSize = 1024 * 1024
	configuration.TestVolumeParameters = map[string]string{"poolName": "standard"}
	configuration.IDGen = &driverIDGenerator{}
	configuration.IdempotentCount = 2
	sanity.Test(t, configuration)
	controllerCreateCalls, controllerDeleteCalls := core.creates.Load(), core.deletes.Load()
	controllerPublishCalls, controllerUnpublishCalls, controllerValidateCalls := core.publishes.Load(), core.unpublishes.Load(), core.validates.Load()
	if controllerCreateCalls == 0 || controllerDeleteCalls == 0 || controllerPublishCalls == 0 || controllerUnpublishCalls == 0 || controllerValidateCalls == 0 {
		t.Fatalf("CSI sanity did not execute every controller lifecycle core: create=%d delete=%d publish=%d unpublish=%d validate=%d", controllerCreateCalls, controllerDeleteCalls, controllerPublishCalls, controllerUnpublishCalls, controllerValidateCalls)
	}
	if nodeCore.stageCalls.Load() == 0 || nodeCore.unstageCalls.Load() == 0 || nodeCore.publishCalls.Load() == 0 || nodeCore.unpublishCalls.Load() == 0 {
		t.Fatalf("CSI sanity did not execute every node lifecycle core: stage=%d unstage=%d publish=%d unpublish=%d", nodeCore.stageCalls.Load(), nodeCore.unstageCalls.Load(), nodeCore.publishCalls.Load(), nodeCore.unpublishCalls.Load())
	}
	t.Logf("CSI sanity production-core counts: controller create=%d delete=%d publish=%d unpublish=%d validate=%d; node stage=%d unstage=%d publish=%d unpublish=%d",
		controllerCreateCalls, controllerDeleteCalls, controllerPublishCalls, controllerUnpublishCalls, controllerValidateCalls,
		nodeCore.stageCalls.Load(), nodeCore.unstageCalls.Load(), nodeCore.publishCalls.Load(), nodeCore.unpublishCalls.Load())
}

func sanityPool(t *testing.T) pool.Config {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	return pool.Config{
		Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
		Filesystems: []pool.ParentConfig{{
			ID: "22222222-2222-4222-8222-222222222222", Name: "sanity-parent", State: pool.ParentActive,
		}},
	}
}

func sanityVolumeHandle(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "sfs1:lv-" + hex.EncodeToString(sum[:16]) + ":mh-" + hex.EncodeToString(sum[16:])
}
