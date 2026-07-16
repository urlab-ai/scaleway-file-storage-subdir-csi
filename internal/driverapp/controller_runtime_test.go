package driverapp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	internaluuid "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/uuid"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func controllerShellStartup(t *testing.T) Startup {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	runtime := config.Runtime{
		Mode: config.ModeDevelopment, DriverName: "file-storage-subdir.csi.urlab.ai",
		Installation: config.Installation{
			ExistingSecretName: "driver-identity", IDKey: "installationID",
			ID: "11111111-1111-4111-8111-111111111111",
		},
		Provider: config.Provider{
			Region: "fr-par", DefaultZone: "fr-par-1", ProjectID: "22222222-2222-4222-8222-222222222222",
			CredentialsExistingSecretName: "driver-credentials", AccessKeyKey: "SCW_ACCESS_KEY", SecretKeyKey: "SCW_SECRET_KEY",
		},
		Controller: config.Controller{
			Replicas: 1, UpdateStrategy: "Recreate", MaxConcurrentMutations: 10,
			ShutdownDeadline: 90 * time.Second, TerminationGracePeriod: 120 * time.Second,
			ProgressDeadline: 65 * time.Minute, StartupProbeBudget: time.Hour,
			AttachReadyDeadline: 10 * time.Minute, MetadataRefreshInterval: 5 * time.Minute,
			DetailedTombstoneRetention: 30 * 24 * time.Hour,
			ParentMountRoot:            "/controller-parents", Leadership: config.Leadership{
				Enabled: true, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second,
			},
		},
		Node: config.Node{ParentMountRoot: "/node-parents", KubeletPath: "/var/lib/kubelet"},
		Scheduling: config.Scheduling{
			AllSchedulableLinuxNodesAreEligible: true, RequireHomogeneousEligibleNodes: true,
		},
		Compatibility: config.Compatibility{QualifiedCommercialTypes: []string{"TEST-TYPE-1"}},
		Pools: []pool.Config{{
			Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
			MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio, MinFreeBytes: 1, MinFreePercent: 5,
			DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
			Filesystems: []pool.ParentConfig{{ID: "33333333-3333-4333-8333-333333333333", Name: "parent-a", State: pool.ParentActive}},
		}},
		StorageClasses: []config.StorageClass{{
			Name: "sfs-subdir-rwx", PoolName: "standard", ReclaimPolicy: "Delete", VolumeBindingMode: "Immediate",
		}},
	}
	return Startup{Options: Options{Component: config.ComponentController}, Config: config.Loaded{Runtime: runtime}}
}

func TestForwardControllerLeadershipWaitsForReleaseDisposition(t *testing.T) {
	result := make(chan error)
	disposition := make(chan bool, 1)
	events := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		forwardControllerLeadership(result, disposition, events)
		close(done)
	}()
	result <- nil
	select {
	case event := <-events:
		t.Fatalf("expected release reported before CAS disposition: %v", event)
	default:
	}
	disposition <- true
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("successful release disposition did not unblock leadership forwarder")
	}
	select {
	case event := <-events:
		t.Fatalf("successful release became leadership failure: %v", event)
	default:
	}

	result = make(chan error, 1)
	disposition = make(chan bool, 1)
	result <- nil
	disposition <- false
	go forwardControllerLeadership(result, disposition, events)
	select {
	case event := <-events:
		if event != nil {
			t.Fatalf("failed release event = %v, want nil unexpected-stop marker", event)
		}
	case <-time.After(time.Second):
		t.Fatal("failed release did not reach leadership supervisor")
	}
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved TCP address: %v", err)
	}
	return address
}

func TestControllerRuntimeShellServesLiveAndUnreadyBeforeActiveInstall(t *testing.T) {
	startup := controllerShellStartup(t)
	runtime, err := newControllerRuntimeShell(startup, clock.Real{}, internaluuid.Random{})
	if err != nil {
		t.Fatalf("newControllerRuntimeShell() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close controller runtime: %v", err)
		}
	})
	if ready, reason := runtime.readiness.Snapshot(); ready || reason != controllerStartupUnready {
		t.Fatalf("initial readiness = %t/%q", ready, reason)
	}
	if _, err := runtime.cores.Create(context.Background(), driver.CreateRequest{}); !errors.Is(err, k8s.ErrUnavailable) {
		t.Fatalf("pre-start Create() error = %v", err)
	}
	_, err = runtime.adminHandler.HandleAdminOperation(context.Background(), admin.CommandGCSubmit, admin.MutationRequest{}, nil)
	var operationError *admin.OperationError
	if !errors.As(err, &operationError) || operationError.Code != admin.ErrorUnavailable {
		t.Fatalf("pre-start admin error = %v", err)
	}

	temporaryRoot, err := os.MkdirTemp("/tmp", "sfs-csi-shell-")
	if err != nil {
		t.Fatalf("create short test socket root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporaryRoot) })
	root, err := filepath.EvalSymlinks(temporaryRoot)
	if err != nil {
		t.Fatalf("resolve test socket root: %v", err)
	}
	csiDirectory := filepath.Join(root, "csi")
	if err := os.Mkdir(csiDirectory, 0o755); err != nil {
		t.Fatalf("Mkdir(CSI) error = %v", err)
	}
	startup.Options.CSIEndpointPath = filepath.Join(csiDirectory, "csi.sock")
	startup.Options.AdminEndpointPath = filepath.Join(root, "admin", "admin.sock")
	startup.Options.LiveAddress = unusedTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- runtime.serve(ctx, startup.Options) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case serveErr := <-serveResult:
			t.Fatalf("controller shell stopped before liveness became available: %v", serveErr)
		default:
		}
		response, requestErr := http.Get("http://" + startup.Options.LiveAddress + "/livez")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("pre-start /livez status = %d", response.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pre-start /livez did not become available: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	connection, err := grpc.NewClient("unix://"+startup.Options.CSIEndpointPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		<-serveResult
		t.Fatalf("dial pre-start CSI socket: %v", err)
	}
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 5*time.Second)
	probe, err := csi.NewIdentityClient(connection).Probe(probeCtx, &csi.ProbeRequest{})
	cancelProbe()
	_ = connection.Close()
	if err != nil || probe.GetReady().GetValue() {
		cancel()
		<-serveResult
		t.Fatalf("pre-start Probe() = %#v, %v", probe, err)
	}

	cancel()
	if err := <-serveResult; err != nil {
		t.Fatalf("serve() shutdown error = %v", err)
	}
}

type fakeControllerLeadership struct {
	err error
	ctx context.Context
}

func (leadership fakeControllerLeadership) RequireActiveLeadership(context.Context) error {
	return leadership.err
}

func (leadership fakeControllerLeadership) Context() context.Context {
	if leadership.ctx == nil {
		return context.Background()
	}
	return leadership.ctx
}

type fakeProvisioningAvailability struct{ err error }

func (availability fakeProvisioningAvailability) RequireProvisioning(context.Context) error {
	return availability.err
}

type fakeGuardedControllerCore struct {
	createCalls    int
	deleteCalls    int
	publishCalls   int
	unpublishCalls int
	validateCalls  int
}

func (core *fakeGuardedControllerCore) Create(context.Context, driver.CreateRequest) (driver.CreateResponse, error) {
	core.createCalls++
	return driver.CreateResponse{}, nil
}

func (core *fakeGuardedControllerCore) Delete(context.Context, string) error {
	core.deleteCalls++
	return nil
}

func (core *fakeGuardedControllerCore) Publish(context.Context, driver.PublishRequest) error {
	core.publishCalls++
	return nil
}

func (core *fakeGuardedControllerCore) Unpublish(context.Context, string, string) error {
	core.unpublishCalls++
	return nil
}

func (core *fakeGuardedControllerCore) Validate(context.Context, driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	core.validateCalls++
	return driver.ValidateCapabilitiesResult{}, nil
}

func TestLeadershipControllerCoresRejectEveryMutationBeforeCallingCore(t *testing.T) {
	core := &fakeGuardedControllerCore{}
	guarded := &leadershipControllerCores{
		leadership: fakeControllerLeadership{err: coordination.ErrLeadershipNotActive},
		create:     core, delete: core, publish: core, validate: core, availability: fakeProvisioningAvailability{},
		shutdown: context.Background(),
	}
	if _, err := guarded.Create(context.Background(), driver.CreateRequest{}); !errors.Is(err, coordination.ErrLeadershipNotActive) {
		t.Fatalf("Create() error = %v", err)
	}
	if err := guarded.Delete(context.Background(), "volume"); !errors.Is(err, coordination.ErrLeadershipNotActive) {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := guarded.Publish(context.Background(), driver.PublishRequest{}); !errors.Is(err, coordination.ErrLeadershipNotActive) {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := guarded.Unpublish(context.Background(), "volume", "node"); !errors.Is(err, coordination.ErrLeadershipNotActive) {
		t.Fatalf("Unpublish() error = %v", err)
	}
	if core.createCalls+core.deleteCalls+core.publishCalls+core.unpublishCalls != 0 {
		t.Fatalf("leadership rejection reached mutating core: %#v", core)
	}
	if _, err := guarded.Validate(context.Background(), driver.ValidateCapabilitiesRequest{}); err != nil {
		t.Fatalf("Validate(read-only) error = %v", err)
	}
	if core.validateCalls != 1 {
		t.Fatalf("Validate calls = %d, want 1", core.validateCalls)
	}
}

func TestLeadershipControllerCoresDelegateWithActiveLeadership(t *testing.T) {
	core := &fakeGuardedControllerCore{}
	guarded := &leadershipControllerCores{
		leadership: fakeControllerLeadership{}, create: core, delete: core, publish: core, validate: core,
		availability: fakeProvisioningAvailability{}, shutdown: context.Background(),
	}
	if _, err := guarded.Create(context.Background(), driver.CreateRequest{}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := guarded.Delete(context.Background(), "volume"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := guarded.Publish(context.Background(), driver.PublishRequest{}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := guarded.Unpublish(context.Background(), "volume", "node"); err != nil {
		t.Fatalf("Unpublish() error = %v", err)
	}
	if core.createCalls != 1 || core.deleteCalls != 1 || core.publishCalls != 1 || core.unpublishCalls != 1 {
		t.Fatalf("active delegation calls = %#v", core)
	}
}

func TestLeadershipControllerCoresBlockOnlyProvisioningWhenAvailabilityDegraded(t *testing.T) {
	core := &fakeGuardedControllerCore{}
	degraded := errors.New("global inventory unavailable")
	guarded := &leadershipControllerCores{
		leadership: fakeControllerLeadership{}, create: core, delete: core, publish: core, validate: core,
		availability: fakeProvisioningAvailability{err: degraded}, shutdown: context.Background(),
	}
	if _, err := guarded.Create(context.Background(), driver.CreateRequest{}); !errors.Is(err, degraded) {
		t.Fatalf("Create(degraded) error = %v", err)
	}
	if err := guarded.Publish(context.Background(), driver.PublishRequest{}); !errors.Is(err, degraded) {
		t.Fatalf("Publish(degraded) error = %v", err)
	}
	if err := guarded.Delete(context.Background(), "volume"); err != nil {
		t.Fatalf("Delete(degraded) error = %v", err)
	}
	if err := guarded.Unpublish(context.Background(), "volume", "node"); err != nil {
		t.Fatalf("Unpublish(degraded) error = %v", err)
	}
	if core.createCalls != 0 || core.publishCalls != 0 || core.deleteCalls != 1 || core.unpublishCalls != 1 {
		t.Fatalf("degraded delegation calls = %#v", core)
	}
}

type blockingGuardedControllerCore struct {
	started chan struct{}
}

func (core *blockingGuardedControllerCore) Create(ctx context.Context, _ driver.CreateRequest) (driver.CreateResponse, error) {
	close(core.started)
	<-ctx.Done()
	return driver.CreateResponse{}, ctx.Err()
}

func (core *blockingGuardedControllerCore) Delete(context.Context, string) error { return nil }
func (core *blockingGuardedControllerCore) Publish(context.Context, driver.PublishRequest) error {
	return nil
}
func (core *blockingGuardedControllerCore) Unpublish(context.Context, string, string) error {
	return nil
}
func (core *blockingGuardedControllerCore) Validate(context.Context, driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	return driver.ValidateCapabilitiesResult{}, nil
}

func TestLeadershipControllerCoresCancelAdmittedMutationWhenAuthorityEnds(t *testing.T) {
	for _, test := range []struct {
		name       string
		cancelLead bool
	}{
		{name: "lease-loss", cancelLead: true},
		{name: "process-shutdown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			leadershipCtx, cancelLeadership := context.WithCancel(context.Background())
			shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
			defer cancelLeadership()
			defer cancelShutdown()
			core := &blockingGuardedControllerCore{started: make(chan struct{})}
			guarded := &leadershipControllerCores{
				leadership: fakeControllerLeadership{ctx: leadershipCtx}, shutdown: shutdownCtx,
				create: core, delete: core, publish: core, validate: core, availability: fakeProvisioningAvailability{},
			}
			result := make(chan error, 1)
			go func() {
				_, err := guarded.Create(context.Background(), driver.CreateRequest{})
				result <- err
			}()
			<-core.started
			if test.cancelLead {
				cancelLeadership()
			} else {
				cancelShutdown()
			}
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("Create() error = %v, want context cancellation", err)
			}
		})
	}
}

func TestWaitForOperatorApprovalRequiresExactModeAndRuntimeIdentity(t *testing.T) {
	const (
		namespace      = "driver-system"
		installationID = "22222222-2222-4222-8222-222222222222"
		clusterUID     = "33333333-3333-4333-8333-333333333333"
	)
	immutable := true
	client := fake.NewClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: coordination.ApprovalSecretNameV1, Namespace: namespace,
		UID: types.UID("66666666-6666-4666-8666-666666666666"),
	}, Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: map[string][]byte{
		"schemaVersion": []byte("1"), "mode": []byte("abnormal-takeover"),
		"requestID":      []byte("11111111-1111-4111-8111-111111111111"),
		"installationID": []byte(installationID), "activeClusterUID": []byte(clusterUID),
		"previousHolderPodUID":     []byte("44444444-4444-4444-8444-444444444444"),
		"previousHolderNodeName":   []byte("worker-1"),
		"previousHolderCSINodeID":  []byte("fr-par-1/55555555-5555-4555-8555-555555555555"),
		"previousHolderInstanceID": []byte("55555555-5555-4555-8555-555555555555"),
		"previousHolderZone":       []byte("fr-par-1"),
		"checkpointRequestID":      {}, "checkpointManifestSHA256": {}, "recoveryFenceScope": {},
		"reason":     []byte("previous controller was fenced"),
		"approvedAt": []byte("2026-07-13T10:01:00Z"), "expiresAt": []byte("2026-07-13T10:31:00Z"),
	}})
	manual := clock.NewManual(time.Date(2026, 7, 13, 10, 2, 0, 0, time.UTC))
	approval, err := waitForOperatorApproval(
		context.Background(), client, namespace, coordination.ApprovalAbnormalTakeover,
		installationID, clusterUID, time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC), manual,
	)
	if err != nil {
		t.Fatalf("waitForOperatorApproval() error = %v", err)
	}
	if approval.SecretUID != "66666666-6666-4666-8666-666666666666" || approval.Mode != coordination.ApprovalAbnormalTakeover {
		t.Fatalf("operator approval = %#v", approval)
	}
	if _, err := waitForOperatorApproval(
		context.Background(), client, namespace, coordination.ApprovalMissingLeaseRecovery,
		installationID, clusterUID, time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC), manual,
	); err == nil {
		t.Fatal("waitForOperatorApproval(wrong mode) error = nil")
	}
}

func TestValidateRecoveryCheckpointIdentityRequiresExactParentsAndInstallation(t *testing.T) {
	manifest := recovery.CheckpointManifest{
		SchemaVersion:       volume.SchemaVersionV1,
		CheckpointRequestID: "11111111-1111-4111-8111-111111111111",
		DriverName:          "file-storage-subdir.csi.urlab.ai", BackupTimestamp: "2026-07-13T10:00:00Z",
		ActiveClusterUID:   "22222222-2222-4222-8222-222222222222",
		InstallationIDHash: recovery.SHA256Digest([]byte("33333333-3333-4333-8333-333333333333")),
		ChartVersion:       "1.0.0", Images: []recovery.ImageDigest{{Name: "driver", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		LeadershipLeaseUID:  "44444444-4444-4444-8444-444444444444",
		HolderEvidence: coordination.HolderEvidence{
			SchemaVersion: volume.SchemaVersionV1,
			PodUID:        "55555555-5555-4555-8555-555555555555", NodeName: "worker-1",
			CSINodeID:  "fr-par-1/66666666-6666-4666-8666-666666666666",
			InstanceID: "66666666-6666-4666-8666-666666666666", Zone: "fr-par-1",
			InstallationID:   "33333333-3333-4333-8333-333333333333",
			ActiveClusterUID: "22222222-2222-4222-8222-222222222222",
		},
		KubernetesObjects: recovery.ObjectInventorySummary{Count: 1, AggregateSHA256: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		Parents: []recovery.ParentInventory{{
			ParentFilesystemID: "77777777-7777-4777-8777-777777777777",
			ParentOwnerSHA256:  "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			AggregateSHA256:    "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		}},
	}
	if err := validateRecoveryCheckpointIdentity(
		manifest, manifest.DriverName, "33333333-3333-4333-8333-333333333333",
		manifest.ActiveClusterUID, []string{"77777777-7777-4777-8777-777777777777"},
	); err != nil {
		t.Fatalf("validateRecoveryCheckpointIdentity() error = %v", err)
	}
	if err := validateRecoveryCheckpointIdentity(
		manifest, manifest.DriverName, "33333333-3333-4333-8333-333333333333",
		manifest.ActiveClusterUID, []string{"88888888-8888-4888-8888-888888888888"},
	); err == nil {
		t.Fatal("validateRecoveryCheckpointIdentity(wrong parents) error = nil")
	}
}
