package driverapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/csiadapter"
	internaluuid "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/uuid"
	buildversion "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/health"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/parentfs"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const (
	controllerLivenessMaxStall  = 30 * time.Second
	controllerHeartbeatInterval = 5 * time.Second
	controllerStartupUnready    = "controller startup is incomplete"
)

type controllerRuntime struct {
	grpc             *csiadapter.GRPCServer
	admin            *admin.WireServer
	checkpointExport *admin.CheckpointExportServer
	readiness        *driver.Readiness
	liveness         *health.Liveness
	metrics          *observability.ControllerMetrics
	metricFailures   chan error
	availability     *controllerAvailability
	gate             *coordination.MutationGate
	cores            *deferredControllerCores
	adminHandler     *deferredAdminHandler
	exportWorkflow   *deferredCheckpointExportWorkflow
	leadershipEvents chan error
	backgroundEvents chan error
	backgroundWG     sync.WaitGroup
	activeMu         sync.RWMutex
	active           *controllerActiveRuntime
	ids              internaluuid.Generator
	shutdown         time.Duration
}

type controllerActiveRuntime struct {
	lease                 *coordination.LeaseRuntime
	leadership            *coordination.LeadershipSession
	leaseRun              <-chan error
	targets               *safety.ParentTargetManager
	maintenance           *controllerMaintenance
	maintenanceCtx        context.Context
	cancelMaintenance     context.CancelFunc
	leadershipDisposition chan bool
}

func runController(ctx context.Context, startup Startup) (returnErr error) {
	operationClock := clock.Real{}
	ids := internaluuid.Random{}
	runtime, err := newControllerRuntimeShell(startup, operationClock, ids)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, runtime.Close()) }()

	processCtx, cancelProcess := context.WithCancel(ctx)
	defer cancelProcess()
	serveResult := make(chan error, 1)
	go func() { serveResult <- runtime.serve(processCtx, startup.Options) }()
	buildResult := make(chan error, 1)
	go func() {
		client, buildErr := k8s.NewInClusterClientset("github.com/urlab-ai/scaleway-file-storage-subdir-csi/" + buildversion.Version)
		configured := startup.Config.Runtime
		if buildErr == nil {
			var provider scaleway.API
			provider, buildErr = scaleway.NewSDKAPI(scaleway.SDKOptions{
				Region: configured.Provider.Region, ProjectID: configured.Provider.ProjectID,
				Zone: configured.Provider.DefaultZone, AccessKey: os.Getenv("SCW_ACCESS_KEY"),
				SecretKey: os.Getenv("SCW_SECRET_KEY"), UserAgent: "github.com/urlab-ai/scaleway-file-storage-subdir-csi/" + buildversion.Version,
			})
			if buildErr == nil {
				var mounter mount.Interface
				mounter, buildErr = mount.NewKernelMounter(configured.Controller.ParentMountRoot, configured.Node.KubeletPath, configured.DriverName)
				if buildErr == nil {
					kernelPreflight, ok := mounter.(interface{ KernelPreflight(context.Context) error })
					if !ok {
						buildErr = fmt.Errorf("production kernel mounter has no startup preflight")
					} else {
						buildErr = kernelPreflight.KernelPreflight(processCtx)
					}
				}
				if buildErr == nil {
					var statfs pool.StatFSSampler
					statfs, buildErr = pool.NewOSStatFSSampler(operationClock)
					if buildErr == nil {
						_, buildErr = buildControllerRuntime(
							processCtx, startup, client, scaleway.NewLocalMetadataSource(), provider, mounter,
							statfs, operationClock, ids, runtime,
						)
					}
				}
			}
		}
		buildResult <- buildErr
	}()

	select {
	case buildErr := <-buildResult:
		if buildErr != nil {
			cancelProcess()
			serveErr := <-serveResult
			return errors.Join(buildErr, serveErr)
		}
		return <-serveResult
	case serveErr := <-serveResult:
		cancelProcess()
		buildErr := <-buildResult
		return errors.Join(serveErr, buildErr)
	}
}

// newControllerRuntimeShell constructs only local, non-mutating serving state.
// It deliberately opens no listener itself and performs no Kubernetes,
// provider, mount, or parent-filesystem I/O. This lets the controller expose
// shallow liveness and cached CSI readiness while recovery waits for an
// external approval or another bounded startup dependency.
func newControllerRuntimeShell(startup Startup, operationClock clock.Clock, ids internaluuid.Generator) (*controllerRuntime, error) {
	if startup.Options.Component != config.ComponentController {
		return nil, fmt.Errorf("controller runtime shell received component %q", startup.Options.Component)
	}
	if operationClock == nil || ids == nil {
		return nil, fmt.Errorf("controller runtime shell dependency is nil")
	}
	configured := startup.Config.Runtime
	if err := configured.Validate(); err != nil {
		return nil, fmt.Errorf("validate controller runtime configuration: %w", err)
	}
	readiness := &driver.Readiness{}
	if err := readiness.Set(false, controllerStartupUnready); err != nil {
		return nil, err
	}
	identityCore, err := driver.NewIdentityServiceCore(configured.DriverName, buildversion.Version, readiness)
	if err != nil {
		return nil, err
	}
	identityServer, err := csiadapter.NewIdentityServer(identityCore)
	if err != nil {
		return nil, err
	}
	cores := &deferredControllerCores{}
	controllerServer, err := csiadapter.NewControllerServer(csiadapter.ControllerCores{
		Create: cores, Delete: cores, Publish: cores, Validate: cores,
	}, configured.Pools)
	if err != nil {
		return nil, err
	}
	_, parentRefs := controllerParentRuntimeInputs(configured.Pools)
	metrics, err := observability.NewControllerMetrics(parentRefs)
	if err != nil {
		return nil, err
	}
	if err := metrics.SetReady(false); err != nil {
		return nil, err
	}
	if err := metrics.SetLeader(false); err != nil {
		return nil, err
	}
	availability, err := newControllerAvailability(readiness, metrics)
	if err != nil {
		return nil, err
	}
	metricFailures := make(chan error, 1)
	grpcServer, err := csiadapter.NewObservedGRPCServer(
		config.ComponentController, identityServer, controllerServer, nil,
		controllerCSIObserver{metrics: metrics}, firstRuntimeFailureReporter(metricFailures),
	)
	if err != nil {
		return nil, err
	}
	adminHandler := &deferredAdminHandler{}
	handshake := admin.HandshakeResponse{
		DriverVersion: buildversion.Version, ProtocolMajor: admin.ProtocolMajorV1,
		MinimumMinor: admin.ProtocolMinorV1, MaximumMinor: admin.ProtocolMinorV1,
	}
	adminServer, err := admin.NewWireServer(handshake, adminHandler, admin.DefaultServerOptions())
	if err != nil {
		return nil, err
	}
	exportWorkflow := &deferredCheckpointExportWorkflow{}
	checkpointExport, err := admin.NewCheckpointExportServer(handshake, exportWorkflow, admin.DefaultCheckpointExportTimeout())
	if err != nil {
		return nil, err
	}
	gate, err := coordination.NewMutationGate(configured.Controller.MaxConcurrentMutations)
	if err != nil {
		return nil, err
	}
	liveness, err := health.NewLiveness(operationClock, controllerLivenessMaxStall)
	if err != nil {
		return nil, err
	}
	return &controllerRuntime{
		grpc: grpcServer, admin: adminServer, checkpointExport: checkpointExport, readiness: readiness, liveness: liveness,
		metrics: metrics, metricFailures: metricFailures, availability: availability,
		gate: gate, cores: cores, adminHandler: adminHandler, exportWorkflow: exportWorkflow,
		leadershipEvents: make(chan error, 1), backgroundEvents: make(chan error, 1), ids: ids,
		shutdown: configured.Controller.ShutdownDeadline,
	}, nil
}

type deferredControllerCores struct {
	mu        sync.RWMutex
	installed *leadershipControllerCores
}

func (cores *deferredControllerCores) Install(installed *leadershipControllerCores) error {
	if installed == nil || installed.leadership == nil || installed.shutdown == nil || installed.create == nil || installed.delete == nil || installed.publish == nil || installed.validate == nil || installed.availability == nil {
		return fmt.Errorf("installed controller core set is incomplete")
	}
	cores.mu.Lock()
	defer cores.mu.Unlock()
	if cores.installed != nil {
		return fmt.Errorf("controller core set is already installed")
	}
	cores.installed = installed
	return nil
}

func (cores *deferredControllerCores) current() (*leadershipControllerCores, error) {
	cores.mu.RLock()
	installed := cores.installed
	cores.mu.RUnlock()
	if installed == nil {
		return nil, fmt.Errorf("%w: controller startup is incomplete", k8s.ErrUnavailable)
	}
	return installed, nil
}

func (cores *deferredControllerCores) Create(ctx context.Context, request driver.CreateRequest) (driver.CreateResponse, error) {
	installed, err := cores.current()
	if err != nil {
		return driver.CreateResponse{}, err
	}
	return installed.Create(ctx, request)
}

func (cores *deferredControllerCores) Delete(ctx context.Context, volumeID string) error {
	installed, err := cores.current()
	if err != nil {
		return err
	}
	return installed.Delete(ctx, volumeID)
}

func (cores *deferredControllerCores) Publish(ctx context.Context, request driver.PublishRequest) error {
	installed, err := cores.current()
	if err != nil {
		return err
	}
	return installed.Publish(ctx, request)
}

func (cores *deferredControllerCores) Unpublish(ctx context.Context, volumeID, nodeID string) error {
	installed, err := cores.current()
	if err != nil {
		return err
	}
	return installed.Unpublish(ctx, volumeID, nodeID)
}

func (cores *deferredControllerCores) Validate(ctx context.Context, request driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	installed, err := cores.current()
	if err != nil {
		return driver.ValidateCapabilitiesResult{}, err
	}
	return installed.Validate(ctx, request)
}

type deferredAdminHandler struct {
	mu        sync.RWMutex
	installed admin.OperationHandler
}

type deferredCheckpointExportWorkflow struct {
	mu        sync.RWMutex
	installed admin.CheckpointExportWorkflow
}

func (workflow *deferredCheckpointExportWorkflow) Install(installed admin.CheckpointExportWorkflow) error {
	if installed == nil {
		return fmt.Errorf("installed checkpoint export workflow is nil")
	}
	workflow.mu.Lock()
	defer workflow.mu.Unlock()
	if workflow.installed != nil {
		return fmt.Errorf("checkpoint export workflow is already installed")
	}
	workflow.installed = installed
	return nil
}

func (workflow *deferredCheckpointExportWorkflow) BuildExport(ctx context.Context, requestID string) (recovery.CheckpointExportPackage, string, error) {
	workflow.mu.RLock()
	installed := workflow.installed
	workflow.mu.RUnlock()
	if installed == nil {
		return recovery.CheckpointExportPackage{}, "", admin.NewOperationError(admin.ErrorUnavailable, fmt.Errorf("controller startup is incomplete"))
	}
	return installed.BuildExport(ctx, requestID)
}

func (handler *deferredAdminHandler) Install(installed admin.OperationHandler) error {
	if installed == nil {
		return fmt.Errorf("installed admin handler is nil")
	}
	handler.mu.Lock()
	defer handler.mu.Unlock()
	if handler.installed != nil {
		return fmt.Errorf("admin handler is already installed")
	}
	handler.installed = installed
	return nil
}

func (handler *deferredAdminHandler) HandleAdminOperation(ctx context.Context, command admin.Command, request admin.MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	handler.mu.RLock()
	installed := handler.installed
	handler.mu.RUnlock()
	if installed == nil {
		return nil, admin.NewOperationError(admin.ErrorUnavailable, fmt.Errorf("controller startup is incomplete"))
	}
	return installed.HandleAdminOperation(ctx, command, request, payload)
}

func buildControllerRuntime(
	ctx context.Context,
	startup Startup,
	client kubernetes.Interface,
	metadata scaleway.MetadataSource,
	provider scaleway.API,
	mounter mount.Interface,
	statfs pool.StatFSSampler,
	operationClock clock.Clock,
	ids internaluuid.Generator,
	runtime *controllerRuntime,
) (_ *controllerRuntime, returnErr error) {
	if startup.Options.Component != config.ComponentController {
		return nil, fmt.Errorf("controller runtime received component %q", startup.Options.Component)
	}
	if client == nil || metadata == nil || provider == nil || mounter == nil || statfs == nil || operationClock == nil || ids == nil || runtime == nil {
		return nil, fmt.Errorf("controller runtime dependency is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	configured := startup.Config.Runtime
	if err := configured.Validate(); err != nil {
		return nil, fmt.Errorf("validate controller runtime configuration: %w", err)
	}
	provider, err := newObservedScalewayAPI(provider, runtime.metrics, firstRuntimeFailureReporter(runtime.metricFailures))
	if err != nil {
		return nil, err
	}

	clusterUID, err := k8s.ReadActiveClusterUID(ctx, client.CoreV1())
	if err != nil {
		return nil, err
	}
	identity, err := scaleway.ResolveNodeIdentity(ctx, metadata, configured.Provider.Region, configured.Compatibility.QualifiedCommercialTypes)
	if err != nil {
		return nil, err
	}
	localNodeID, err := identity.NodeID()
	if err != nil {
		return nil, err
	}
	podUID := os.Getenv("POD_UID")
	nodeName := os.Getenv("NODE_NAME")
	parentEvents, err := newKubernetesParentEventRecorder(
		client.CoreV1().Events(startup.Config.ControllerNamespace), operationClock,
		startup.Config.ControllerNamespace, os.Getenv("POD_NAME"), podUID,
	)
	if err != nil {
		return nil, err
	}
	holder, err := coordination.NewHolderEvidence(
		podUID, nodeName, localNodeID, identity.InstanceID, identity.Zone,
		configured.Installation.ID, clusterUID,
	)
	if err != nil {
		return nil, fmt.Errorf("construct controller holder evidence: %w", err)
	}

	configMaps, err := k8s.NewClientGoConfigMaps(client.CoreV1())
	if err != nil {
		return nil, err
	}
	allocations, err := k8s.NewAllocationStore(configMaps, startup.Config.ControllerNamespace, configured.DriverName, configured.Installation.ID)
	if err != nil {
		return nil, err
	}
	reservationJournals, err := k8s.NewReservationJournalStore(configMaps, startup.Config.ControllerNamespace, configured.DriverName, configured.Installation.ID)
	if err != nil {
		return nil, err
	}
	configuredPoolNames := make([]string, 0, len(configured.Pools))
	for _, configuredPool := range configured.Pools {
		configuredPoolNames = append(configuredPoolNames, configuredPool.Name)
	}
	slices.Sort(configuredPoolNames)
	volumeAttachments, err := k8s.NewClientGoVolumeAttachments(client.CoreV1(), client.StorageV1(), configured.DriverName)
	if err != nil {
		return nil, err
	}
	nodeInventory, err := k8s.NewClientGoNodeInventory(
		client.CoreV1(), client.StorageV1(), startup.Config.ControllerNamespace,
		configured.DriverName, "scaleway-sfs-subdir-csi", startup.Config.HelmReleaseName,
	)
	if err != nil {
		return nil, err
	}
	authorizations, err := newControllerNodeAuthorizations(nodeInventory, provider, startup.Config)
	if err != nil {
		return nil, err
	}

	parentIDs, _ := controllerParentRuntimeInputs(configured.Pools)
	targets, err := safety.OpenParentTargetManager(configured.Controller.ParentMountRoot)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			if active := runtime.activeSnapshot(); active != nil && active.cancelMaintenance != nil {
				active.cancelMaintenance()
				runtime.backgroundWG.Wait()
			}
			returnErr = errors.Join(returnErr, targets.Close())
		}
	}()
	for _, parentID := range parentIDs {
		if err := targets.Ensure(ctx, parentID); err != nil {
			return nil, fmt.Errorf("prepare controller parent target %q: %w", parentID, err)
		}
	}

	attachments, err := scaleway.NewAttachmentManager(provider, operationClock, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: configured.Controller.AttachReadyDeadline, InitialBackoff: time.Second, MaximumBackoff: 15 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	detachments, err := scaleway.NewDetachmentManager(provider, operationClock, scaleway.RandomJitter{}, scaleway.AttachConfig{
		Deadline: configured.Controller.AttachReadyDeadline, InitialBackoff: time.Second, MaximumBackoff: 15 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	parentAccess, err := newControllerParentAccess(configured, localNodeID, authorizations, attachments, mounter)
	if err != nil {
		return nil, err
	}
	parentBackend, err := parentfs.NewBackend(parentAccess)
	if err != nil {
		return nil, err
	}
	bootstrapEvidence, err := newKubernetesParentBootstrapEvidence(allocations, volumeAttachments)
	if err != nil {
		return nil, err
	}

	leaseStore, err := k8s.NewClientGoLeaseStore(client.CoordinationV1(), startup.Config.ControllerNamespace)
	if err != nil {
		return nil, err
	}
	leadershipFatal := make(chan error, 1)
	leaseRuntime, err := coordination.NewLeaseRuntime(leaseStore, holder, coordination.LeaseTiming{
		LeaseDuration: configured.Controller.Leadership.LeaseDuration,
		RenewDeadline: configured.Controller.Leadership.RenewDeadline,
		RetryPeriod:   configured.Controller.Leadership.RetryPeriod,
	}, operationClock, func(err error) {
		select {
		case leadershipFatal <- err:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	approvalFence, err := scaleway.NewApprovalFenceVerifier(
		provider, configured.Provider.Region, configured.Provider.ProjectID, localNodeID, parentIDs,
	)
	if err != nil {
		return nil, err
	}
	acquired, acquireErr := leaseRuntime.Acquire(ctx, true)
	if errors.Is(acquireErr, coordination.ErrAbnormalTakeoverApprovalRequired) {
		conditionObservedAt := operationClock.Now()
		approval, err := waitForOperatorApproval(
			ctx, client, startup.Config.ControllerNamespace, coordination.ApprovalAbnormalTakeover,
			configured.Installation.ID, clusterUID, conditionObservedAt, operationClock,
		)
		if err != nil {
			return nil, fmt.Errorf("wait for abnormal takeover approval: %w", err)
		}
		acquired, err = leaseRuntime.AcquireApproved(ctx, approval, conditionObservedAt, "", "", approvalFence)
		if err != nil {
			return nil, fmt.Errorf("consume abnormal takeover approval: %w", err)
		}
		acquireErr = nil
	}
	if acquireErr != nil && !errors.Is(acquireErr, coordination.ErrMissingLeaseRecoveryRequired) {
		return nil, acquireErr
	}
	if acquired.Session == nil {
		return nil, fmt.Errorf("controller Lease acquisition returned no session")
	}
	leaseRun, err := startControllerLeadership(ctx, acquired.Session)
	if err != nil {
		return nil, err
	}
	coldStartCtx, cancelColdStart, err := controllerOperationContext(ctx, acquired.Session.Context())
	if err != nil {
		return nil, err
	}
	defer func() { cancelColdStart() }()
	stopAcquired := true
	defer func() {
		if stopAcquired {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			returnErr = errors.Join(returnErr, acquired.Session.Stop(stopCtx))
			cancel()
		}
	}()

	bootstrap, err := newParentBootstrapManager(
		startup.Config, clusterUID, localNodeID, acquired.Session, provider,
		authorizations, parentAccess, bootstrapEvidence, operationClock, ids,
	)
	if err != nil {
		return nil, err
	}
	if !acquired.MutationAllowed {
		provisionalDrained := false
		discovery, err := newFreshInstallationDiscovery(
			bootstrap, allocations, volumeAttachments, reservationJournals, configuredPoolNames, clusterUID,
			configured.Controller.AttachReadyDeadline, scaleway.RandomJitter{},
		)
		if err != nil {
			return nil, err
		}
		promoted, err := leaseRuntime.PromoteFreshInstallation(ctx, discovery)
		if err != nil {
			if acquired.Session.Context().Err() != nil {
				return nil, fmt.Errorf("promote fresh controller installation after provisional session stopped: %w", err)
			}
			if recoveryErr := bootstrap.DiscoverExistingReadOnly(coldStartCtx); recoveryErr != nil {
				return nil, errors.Join(
					fmt.Errorf("fresh installation discovery: %w", err),
					fmt.Errorf("read-only missing-Lease recovery discovery: %w", recoveryErr),
				)
			}
			checkpointSecret, recoveryErr := k8s.ReadCheckpointSecret(coldStartCtx, client.CoreV1(), startup.Config.ControllerNamespace)
			if recoveryErr != nil {
				return nil, fmt.Errorf("read missing-Lease recovery checkpoint: %w", recoveryErr)
			}
			manifest, manifestSHA256, recoveryErr := recovery.ValidateCheckpointSecret(recovery.CheckpointSecret{
				Name: checkpointSecret.Name, Type: checkpointSecret.Type,
				Immutable: checkpointSecret.Immutable, Data: checkpointSecret.Data,
			})
			if recoveryErr != nil {
				return nil, fmt.Errorf("validate missing-Lease recovery checkpoint: %w", recoveryErr)
			}
			if recoveryErr := validateRecoveryCheckpointIdentity(manifest, configured.DriverName, configured.Installation.ID, clusterUID, parentIDs); recoveryErr != nil {
				return nil, recoveryErr
			}
			recoveryInventory, recoveryErr := newStartupInventoryReader(
				bootstrap.parents, configured.DriverName, configured.Installation.ID, clusterUID,
				startup.Config.ControllerNamespace, startup.Config.HelmReleaseName,
				allocations, volumeAttachments, parentBackend,
			)
			if recoveryErr != nil {
				return nil, recoveryErr
			}
			recoverySnapshot, recoveryErr := recoveryInventory.Read(coldStartCtx)
			if recoveryErr != nil {
				return nil, fmt.Errorf("read missing-Lease recovery inventory: %w", recoveryErr)
			}
			if _, recoveryErr := recovery.BuildStartupInventoryPlan(recoverySnapshot); recoveryErr != nil {
				return nil, fmt.Errorf("validate missing-Lease recovery inventory: %w", recoveryErr)
			}
			parentSummaries, recoveryErr := recovery.BuildParentInventorySummaries(coldStartCtx, recoverySnapshot.Parents)
			if recoveryErr != nil {
				return nil, fmt.Errorf("summarize missing-Lease parent inventories: %w", recoveryErr)
			}
			restoredJournals, recoveryErr := reservationJournals.CheckpointObjects(coldStartCtx, configuredPoolNames, clusterUID)
			if recoveryErr != nil {
				return nil, fmt.Errorf("read missing-Lease reservation journals: %w", recoveryErr)
			}
			objectSummary, recoveryErr := recovery.BuildRestoreKubernetesObjectSummary(
				startup.Config.ControllerNamespace, recoverySnapshot.Allocations, restoredJournals, recoverySnapshot.PersistentVolumes,
			)
			if recoveryErr != nil {
				return nil, fmt.Errorf("summarize missing-Lease Kubernetes objects: %w", recoveryErr)
			}
			images := make([]recovery.ImageDigest, 0, len(startup.Config.RenderedImages))
			for _, rendered := range startup.Config.RenderedImages {
				images = append(images, recovery.ImageDigest{Name: rendered.Name, Digest: rendered.Digest})
			}
			if recoveryErr := recovery.VerifyRestoredCheckpoint(manifest, recovery.RestoredCheckpointState{
				DriverName: configured.DriverName, InstallationID: configured.Installation.ID,
				ActiveClusterUID: clusterUID, ChartVersion: startup.Config.ChartVersion,
				Images: images, KubernetesObjects: objectSummary, Parents: parentSummaries,
			}); recoveryErr != nil {
				return nil, fmt.Errorf("verify complete restored checkpoint: %w", recoveryErr)
			}
			marker, present, recoveryErr := coordination.ParseDiscoveryMarker(acquired.Session.Snapshot().Annotations, holder)
			if recoveryErr != nil || !present {
				if recoveryErr == nil {
					recoveryErr = fmt.Errorf("provisional discovery marker is absent")
				}
				return nil, recoveryErr
			}
			conditionObservedAt, recoveryErr := marker.ObservationTime()
			if recoveryErr != nil {
				return nil, recoveryErr
			}
			approval, recoveryErr := waitForOperatorApproval(
				coldStartCtx, client, startup.Config.ControllerNamespace, coordination.ApprovalMissingLeaseRecovery,
				configured.Installation.ID, clusterUID, conditionObservedAt, operationClock,
			)
			if recoveryErr != nil {
				return nil, fmt.Errorf("wait for missing-Lease recovery approval: %w", recoveryErr)
			}
			if recoveryErr := acquired.Session.Stop(ctx); recoveryErr != nil {
				return nil, fmt.Errorf("stop provisional recovery leadership: %w", recoveryErr)
			}
			cancelColdStart()
			if runErr := <-leaseRun; runErr != nil {
				return nil, fmt.Errorf("drain provisional recovery leadership: %w", runErr)
			}
			provisionalDrained = true
			promoted, recoveryErr = leaseRuntime.AcquireApproved(
				ctx, approval, conditionObservedAt,
				manifest.CheckpointRequestID, manifestSHA256, approvalFence,
			)
			if recoveryErr != nil {
				return nil, fmt.Errorf("consume missing-Lease recovery approval: %w", recoveryErr)
			}
		}
		if !provisionalDrained {
			if runErr := <-leaseRun; runErr != nil {
				return nil, fmt.Errorf("drain provisional leadership: %w", runErr)
			}
		}
		acquired = promoted
		leaseRun, err = startControllerLeadership(ctx, promoted.Session)
		if err != nil {
			return nil, err
		}
		if err := bootstrap.replaceLeadership(promoted.Session); err != nil {
			return nil, err
		}
		cancelColdStart()
		coldStartCtx, cancelColdStart, err = controllerOperationContext(ctx, promoted.Session.Context())
		if err != nil {
			return nil, err
		}
	}
	if err := acquired.Session.RequireActiveLeadership(coldStartCtx); err != nil {
		return nil, err
	}
	if err := reservationJournals.EnsureConfigured(coldStartCtx, configuredPoolNames, clusterUID); err != nil {
		return nil, fmt.Errorf("validate permanent reservation journal set: %w", err)
	}
	if err := bootstrap.EnsureAll(coldStartCtx); err != nil {
		return nil, fmt.Errorf("bootstrap configured parents: %w", err)
	}

	gate := runtime.gate
	if gate == nil || gate.Limit() != int(configured.Controller.MaxConcurrentMutations) {
		return nil, fmt.Errorf("controller runtime shell mutation gate differs from configuration")
	}
	volumeLocks := coordination.NewKeyedLock()
	creation, err := driver.NewCreationReconciler(parentBackend.Creation())
	if err != nil {
		return nil, err
	}
	placer, err := driver.NewProductionParentPlacer(
		configured.DriverName, configured.Installation.ID, clusterUID,
		configured.Provider.Region, configured.Provider.ProjectID, configured.Pools,
		allocations, provider, parentAccess, statfs, operationClock,
	)
	if err != nil {
		return nil, err
	}
	runtimeInventory, err := newControllerRuntimeInventory(parentAccess, placer, runtime.metrics, parentEvents, configured.Pools)
	if err != nil {
		return nil, err
	}
	uninstallCleaner, err := newControllerUninstallCleaner(
		configured.Provider.Region, configured.Provider.ProjectID,
		configured.Controller.ParentMountRoot, parentIDs, mounter, provider, detachments,
	)
	if err != nil {
		return nil, err
	}
	createController, err := driver.NewCreateController(
		configured.DriverName, configured.Installation.ID, clusterUID,
		allocations, reservationJournals, placer, creation, gate, volumeLocks, operationClock,
		acquired.Session.Context(), ctx,
	)
	if err != nil {
		return nil, err
	}
	normalNodes, err := k8s.NewClientGoNormalNodeEvidence(client.CoreV1(), client.StorageV1(), configured.DriverName)
	if err != nil {
		return nil, err
	}
	providerFence, err := scaleway.NewFenceChecker(provider, configured.Provider.Region, configured.Provider.ProjectID)
	if err != nil {
		return nil, err
	}
	fences, err := driver.NewConservativeFenceVerifier(normalNodes, providerFence)
	if err != nil {
		return nil, err
	}
	publishController, err := driver.NewPublishController(
		configured.DriverName, configured.Installation.ID, clusterUID,
		allocations, parentBackend.PublishOwnerships(), parentAccess, authorizations, fences,
		gate, volumeLocks, operationClock,
	)
	if err != nil {
		return nil, err
	}
	missingDelete, err := newMissingDeleteResolver(
		configured.DriverName, configured.Installation.ID, clusterUID,
		startup.Config.ControllerNamespace, startup.Config.HelmReleaseName,
		configured.Pools, volumeAttachments, parentBackend, ids, operationClock,
	)
	if err != nil {
		return nil, err
	}
	deleteController, err := driver.NewDeleteController(
		configured.DriverName, configured.Installation.ID, clusterUID,
		allocations, parentBackend.LifecycleOwnerships(), missingDelete,
		volumeAttachments, parentBackend.Filesystem(), ids, gate, volumeLocks, operationClock,
	)
	if err != nil {
		return nil, err
	}
	gcController, err := driver.NewGCController(
		configured.DriverName, configured.Installation.ID, clusterUID,
		allocations, parentBackend.LifecycleOwnerships(), volumeAttachments, volumeAttachments,
		acquired.Session, parentBackend.Filesystem(), ids, gate, volumeLocks, operationClock,
	)
	if err != nil {
		return nil, err
	}
	readOnlyParentBackend, err := parentfs.NewBackend(controllerReadOnlyParentAccess{delegate: parentAccess})
	if err != nil {
		return nil, err
	}
	validator, err := driver.NewCapabilityValidator(allocations, readOnlyParentBackend.LifecycleOwnerships())
	if err != nil {
		return nil, err
	}

	recoveryVerifier, err := newStartupKubernetesRecoveryVerifier(allocations, volumeAttachments)
	if err != nil {
		return nil, err
	}
	pvReconstructor, err := recovery.NewPVBackedReconstructor(
		configured.DriverName, configured.Installation.ID, clusterUID,
		recoveryVerifier, allocations, ids, operationClock,
	)
	if err != nil {
		return nil, err
	}
	ownershipReconstructor, err := recovery.NewOwnershipOnlyReconstructor(
		configured.DriverName, configured.Installation.ID, clusterUID,
		recoveryVerifier, allocations, ids, operationClock,
	)
	if err != nil {
		return nil, err
	}
	recoveryAdmission, err := newRecoveryMutationAdmission(acquired.Session, gate, volumeLocks)
	if err != nil {
		return nil, err
	}
	guardedPVReconstructor, err := newGuardedPVBackedReconstructor(recoveryAdmission, pvReconstructor)
	if err != nil {
		return nil, err
	}
	guardedOwnershipReconstructor, err := newGuardedOwnershipOnlyReconstructor(recoveryAdmission, ownershipReconstructor)
	if err != nil {
		return nil, err
	}
	lifecycle, err := driver.NewLifecycleCrashReconciler(
		allocations, createController, publishController, deleteController, gcController,
	)
	if err != nil {
		return nil, err
	}
	allocationCompactor, err := driver.NewAllocationCompactor(
		configured.DriverName, configured.Installation.ID, clusterUID,
		configured.Controller.DetailedTombstoneRetention, allocations,
		parentBackend.LifecycleOwnerships(), acquired.Session, gate, volumeLocks, operationClock,
	)
	if err != nil {
		return nil, err
	}
	compaction, err := driver.NewAllocationCompactionReconciler(
		allocations, allocationCompactor, configured.Controller.DetailedTombstoneRetention,
		parentIDs, operationClock,
	)
	if err != nil {
		return nil, err
	}
	startupReconciler, err := recovery.NewStartupReconciler(guardedPVReconstructor, guardedOwnershipReconstructor, lifecycle)
	if err != nil {
		return nil, err
	}
	startupInventory, err := newStartupInventoryReader(
		bootstrap.parents, configured.DriverName, configured.Installation.ID, clusterUID,
		startup.Config.ControllerNamespace, startup.Config.HelmReleaseName,
		allocations, volumeAttachments, parentBackend,
	)
	if err != nil {
		return nil, err
	}
	if err := reconcileControllerColdStart(
		coldStartCtx, gate, reservationJournals, configuredPoolNames, clusterUID,
		allocations, startupInventory, startupReconciler,
	); err != nil {
		return nil, err
	}
	maintenance, err := newControllerMaintenance(
		configured.Controller.MetadataRefreshInterval, operationClock, acquired.Session, gate,
		runtimeInventory, lifecycle, compaction, runtime.metrics, runtime.availability,
	)
	if err != nil {
		return nil, err
	}
	if err := maintenance.ReconcileOnce(coldStartCtx); err != nil {
		return nil, fmt.Errorf("run initial controller maintenance: %w", err)
	}
	// The remaining setup only wires immutable handlers. Release the startup
	// cancellation callbacks now; active RPCs and periodic maintenance receive
	// their own session-bound contexts below.
	cancelColdStart()
	operationalInventory, err := newStartupInventoryReader(
		bootstrap.parents, configured.DriverName, configured.Installation.ID, clusterUID,
		startup.Config.ControllerNamespace, startup.Config.HelmReleaseName,
		allocations, volumeAttachments, readOnlyParentBackend,
	)
	if err != nil {
		return nil, err
	}
	checkpointReader, err := newControllerCheckpointSnapshotReader(
		operationalInventory, acquired.Session, reservationJournals, configuredPoolNames, clusterUID, startup.Config,
	)
	if err != nil {
		return nil, err
	}
	checkpointCapture, err := recovery.NewSnapshotCheckpointCapture(checkpointReader, operationClock)
	if err != nil {
		return nil, err
	}
	checkpointExporter, err := recovery.NewSnapshotCheckpointExportBuilder(
		startup.Config.ControllerNamespace, operationalInventory, checkpointReader,
	)
	if err != nil {
		return nil, err
	}
	checkpointResume, err := newControllerCheckpointResumeReconciler(
		startupInventory, startupReconciler, reservationJournals, checkpointReader,
		allocations, placer, configuredPoolNames, clusterUID,
	)
	if err != nil {
		return nil, err
	}
	checkpointCoordinator, err := recovery.NewCheckpointCoordinator(gate, acquired.Session, checkpointCapture, checkpointExporter, checkpointResume)
	if err != nil {
		return nil, err
	}
	checkpointWorkflow, err := newControllerCheckpointWorkflow(
		checkpointCoordinator, runtime.availability, acquired.Session.Context(), ctx,
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.exportWorkflow.Install(checkpointWorkflow); err != nil {
		return nil, err
	}
	checkpointOperation, err := admin.NewCheckpointCommandOperation(checkpointWorkflow)
	if err != nil {
		return nil, err
	}
	upgradeReader, err := newControllerUpgradeLiveStateReader(operationalInventory, nodeInventory, acquired.Session, startup.Config, clusterUID)
	if err != nil {
		return nil, err
	}
	upgradeOperation, err := admin.NewUpgradeCommandOperation(upgradeReader)
	if err != nil {
		return nil, err
	}
	journalBarrier, err := newControllerReservationJournalBarrier(reservationJournals, allocations, placer, configuredPoolNames, clusterUID)
	if err != nil {
		return nil, err
	}
	uninstallWorkflow, err := newControllerUninstallWorkflow(
		gate, runtime.availability, acquired.Session, journalBarrier, runtimeInventory, uninstallCleaner, leaseRuntime,
		runtime.completeExpectedLeadershipStop,
	)
	if err != nil {
		return nil, err
	}
	uninstallOperation, err := admin.NewControllerUninstallCommandOperation(uninstallWorkflow)
	if err != nil {
		return nil, err
	}
	decommissionWorkflow, err := newControllerDecommissionWorkflow(
		gate, runtime.availability, acquired.Session, journalBarrier, operationalInventory, runtimeInventory,
		uninstallCleaner, leaseRuntime, runtime.completeExpectedLeadershipStop, configured.Pools,
	)
	if err != nil {
		return nil, err
	}
	decommissionOperation, err := admin.NewControllerDecommissionCommandOperation(decommissionWorkflow)
	if err != nil {
		return nil, err
	}

	guarded := &leadershipControllerCores{
		leadership: acquired.Session, create: createController, delete: deleteController,
		publish: publishController, validate: validator, availability: runtime.availability, shutdown: ctx,
	}
	gcRequests, err := driver.NewGCRequestSubmitter(
		configured.DriverName, configured.Installation.ID, clusterUID,
		allocations, acquired.Session, gate, volumeLocks, operationClock,
	)
	if err != nil {
		return nil, err
	}
	gcOperation, err := admin.NewGCCommandOperation(gcRequests, gcController)
	if err != nil {
		return nil, err
	}
	mux, err := admin.NewOperationMux(gcOperation, checkpointOperation, upgradeOperation, uninstallOperation, decommissionOperation)
	if err != nil {
		return nil, err
	}
	authorizedAdmin, err := newAuthorityBoundAdminHandler(mux, acquired.Session.Context(), ctx)
	if err != nil {
		return nil, err
	}
	if err := runtime.metrics.SetLeader(true); err != nil {
		return nil, err
	}
	select {
	case fatal := <-leadershipFatal:
		return nil, fmt.Errorf("controller leadership failed during startup: %w", fatal)
	default:
	}
	if err := runtime.cores.Install(guarded); err != nil {
		return nil, err
	}
	if err := runtime.adminHandler.Install(authorizedAdmin); err != nil {
		return nil, err
	}
	maintenanceCtx, cancelMaintenance, err := controllerOperationContext(ctx, acquired.Session.Context())
	if err != nil {
		return nil, err
	}
	if err := runtime.installActive(&controllerActiveRuntime{
		lease: leaseRuntime, leadership: acquired.Session, leaseRun: leaseRun, targets: targets,
		maintenance: maintenance, maintenanceCtx: maintenanceCtx, cancelMaintenance: cancelMaintenance,
		leadershipDisposition: make(chan bool, 1),
	}); err != nil {
		cancelMaintenance()
		return nil, err
	}
	if err := runtime.availability.CompleteStartup(); err != nil {
		return nil, err
	}

	stopAcquired = false
	return runtime, nil
}

func (runtime *controllerRuntime) installActive(active *controllerActiveRuntime) error {
	if runtime == nil || active == nil || active.lease == nil || active.leadership == nil || active.leaseRun == nil || active.targets == nil || active.maintenance == nil || active.maintenanceCtx == nil || active.cancelMaintenance == nil || active.leadershipDisposition == nil {
		return fmt.Errorf("active controller runtime is incomplete")
	}
	runtime.activeMu.Lock()
	if runtime.active != nil {
		runtime.activeMu.Unlock()
		return fmt.Errorf("active controller runtime is already installed")
	}
	runtime.active = active
	runtime.activeMu.Unlock()
	go forwardControllerLeadership(active.leaseRun, active.leadershipDisposition, runtime.leadershipEvents)
	runtime.backgroundWG.Add(1)
	go func() {
		defer runtime.backgroundWG.Done()
		if err := active.maintenance.Run(active.maintenanceCtx); err != nil {
			select {
			case runtime.backgroundEvents <- err:
			default:
			}
		}
	}()
	return nil
}

func forwardControllerLeadership(result <-chan error, disposition <-chan bool, events chan<- error) {
	err := <-result
	if err == nil {
		// LeadershipSession.Stop returns before the release CAS. Wait for that
		// CAS disposition so an intentional uninstall release cannot tear down
		// the admin server before its response is written.
		if expected := <-disposition; expected {
			return
		}
	}
	events <- err
}

func (runtime *controllerRuntime) completeExpectedLeadershipStop(success bool) {
	active := runtime.activeSnapshot()
	if active == nil || active.leadershipDisposition == nil {
		return
	}
	if success {
		active.cancelMaintenance()
		if err := runtime.metrics.SetLeader(false); err != nil {
			firstRuntimeFailureReporter(runtime.metricFailures)(err)
		}
	}
	select {
	case active.leadershipDisposition <- success:
	default:
	}
}

func (runtime *controllerRuntime) activeSnapshot() *controllerActiveRuntime {
	if runtime == nil {
		return nil
	}
	runtime.activeMu.RLock()
	active := runtime.active
	runtime.activeMu.RUnlock()
	return active
}

func startControllerLeadership(ctx context.Context, session *coordination.LeadershipSession) (<-chan error, error) {
	result := make(chan error, 1)
	go func() { result <- session.Run(context.Background()) }()
	if err := session.WaitStarted(ctx); err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stopErr := session.Stop(stopCtx)
		cancel()
		runErr := <-result
		return nil, errors.Join(err, stopErr, runErr)
	}
	return result, nil
}

type leadershipControllerCores struct {
	leadership interface {
		RequireActiveLeadership(context.Context) error
		Context() context.Context
	}
	shutdown context.Context
	create   interface {
		Create(context.Context, driver.CreateRequest) (driver.CreateResponse, error)
	}
	delete interface {
		Delete(context.Context, string) error
	}
	publish interface {
		Publish(context.Context, driver.PublishRequest) error
		Unpublish(context.Context, string, string) error
	}
	validate interface {
		Validate(context.Context, driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error)
	}
	availability interface {
		RequireProvisioning(context.Context) error
	}
}

func (cores *leadershipControllerCores) Create(ctx context.Context, request driver.CreateRequest) (driver.CreateResponse, error) {
	operationCtx, cancel, err := cores.mutationContext(ctx)
	if err != nil {
		return driver.CreateResponse{}, err
	}
	defer cancel()
	if err := cores.availability.RequireProvisioning(operationCtx); err != nil {
		return driver.CreateResponse{}, err
	}
	return cores.create.Create(operationCtx, request)
}

func (cores *leadershipControllerCores) Delete(ctx context.Context, volumeID string) error {
	operationCtx, cancel, err := cores.mutationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return cores.delete.Delete(operationCtx, volumeID)
}

func (cores *leadershipControllerCores) Publish(ctx context.Context, request driver.PublishRequest) error {
	operationCtx, cancel, err := cores.mutationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	if err := cores.availability.RequireProvisioning(operationCtx); err != nil {
		return err
	}
	return cores.publish.Publish(operationCtx, request)
}

func (cores *leadershipControllerCores) Unpublish(ctx context.Context, volumeID, nodeID string) error {
	operationCtx, cancel, err := cores.mutationContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return cores.publish.Unpublish(operationCtx, volumeID, nodeID)
}

func (cores *leadershipControllerCores) Validate(ctx context.Context, request driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	return cores.validate.Validate(ctx, request)
}

// mutationContext closes the admission race between a successful leadership
// check and a subsequent Lease loss.  Every blocking dependency receives the
// derived context, so it can stop at its next documented durable boundary.
func (cores *leadershipControllerCores) mutationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	operationCtx, cancel, err := controllerOperationContext(ctx, cores.leadership.Context(), cores.shutdown)
	if err != nil {
		return nil, nil, err
	}
	if err := cores.leadership.RequireActiveLeadership(operationCtx); err != nil {
		cancel()
		return nil, nil, err
	}
	return operationCtx, cancel, nil
}

func controllerParentRuntimeInputs(pools []pool.Config) ([]string, []observability.ParentRef) {
	parentIDs := make([]string, 0)
	refs := make([]observability.ParentRef, 0)
	for _, configuredPool := range pools {
		for _, parent := range configuredPool.Filesystems {
			parentIDs = append(parentIDs, parent.ID)
			refs = append(refs, observability.ParentRef{Pool: configuredPool.Name, Parent: parent.ID})
		}
	}
	slices.Sort(parentIDs)
	slices.SortFunc(refs, func(left, right observability.ParentRef) int {
		if left.Pool != right.Pool {
			return compareStrings(left.Pool, right.Pool)
		}
		return compareStrings(left.Parent, right.Parent)
	})
	return parentIDs, refs
}

func compareStrings(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func waitForOperatorApproval(
	ctx context.Context,
	client kubernetes.Interface,
	namespace string,
	mode coordination.ApprovalMode,
	installationID, clusterUID string,
	conditionObservedAt time.Time,
	operationClock clock.Clock,
) (coordination.OperatorApproval, error) {
	if client == nil || operationClock == nil {
		return coordination.OperatorApproval{}, fmt.Errorf("operator approval dependency is nil")
	}
	for {
		approval, err := k8s.ReadOperatorApproval(ctx, client.CoreV1(), namespace)
		if err == nil {
			if approval.Mode != mode {
				return coordination.OperatorApproval{}, fmt.Errorf("operator approval mode %q does not match blocked mode %q", approval.Mode, mode)
			}
			if approval.InstallationID != installationID || approval.ActiveClusterUID != clusterUID {
				return coordination.OperatorApproval{}, fmt.Errorf("operator approval belongs to another installation or cluster")
			}
			if err := approval.ValidateAt(operationClock.Now(), conditionObservedAt); err != nil {
				return coordination.OperatorApproval{}, err
			}
			return approval, nil
		}
		if !errors.Is(err, k8s.ErrNotFound) {
			return coordination.OperatorApproval{}, err
		}
		timer := operationClock.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return coordination.OperatorApproval{}, ctx.Err()
		case <-timer.C():
		}
	}
}

func validateRecoveryCheckpointIdentity(manifest recovery.CheckpointManifest, driverName, installationID, clusterUID string, parentIDs []string) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if manifest.DriverName != driverName || manifest.ActiveClusterUID != clusterUID || manifest.InstallationIDHash != recovery.SHA256Digest([]byte(installationID)) {
		return fmt.Errorf("checkpoint belongs to another driver installation or cluster")
	}
	checkpointParents := make([]string, 0, len(manifest.Parents))
	for _, parent := range manifest.Parents {
		checkpointParents = append(checkpointParents, parent.ParentFilesystemID)
	}
	slices.Sort(checkpointParents)
	if !slices.Equal(checkpointParents, parentIDs) {
		return fmt.Errorf("checkpoint configured-parent set differs from runtime")
	}
	return nil
}

type controllerServeTask struct {
	name string
	run  func(context.Context) error
}

func (runtime *controllerRuntime) serve(ctx context.Context, options Options) error {
	if runtime == nil || runtime.grpc == nil || runtime.admin == nil || runtime.checkpointExport == nil || runtime.readiness == nil || runtime.liveness == nil || runtime.metrics == nil || runtime.metricFailures == nil || runtime.availability == nil || runtime.gate == nil || runtime.cores == nil || runtime.adminHandler == nil || runtime.exportWorkflow == nil || runtime.leadershipEvents == nil || runtime.backgroundEvents == nil || runtime.ids == nil || runtime.shutdown <= 0 {
		return fmt.Errorf("controller runtime is incomplete")
	}
	csiListener, err := driver.ListenCSIUnix(options.CSIEndpointPath)
	if err != nil {
		return err
	}
	startupListeners := []io.Closer{csiListener}
	listenersHandedOff := false
	defer func() {
		if listenersHandedOff {
			return
		}
		for _, listener := range startupListeners {
			_ = listener.Close()
		}
	}()
	adminListener, err := admin.ListenUnix(options.AdminEndpointPath)
	if err != nil {
		return err
	}
	startupListeners = append(startupListeners, adminListener)
	exportSocketPath, err := admin.CheckpointExportUnixSocketPath(options.AdminEndpointPath)
	if err != nil {
		return err
	}
	exportListener, err := admin.ListenUnix(exportSocketPath)
	if err != nil {
		return err
	}
	startupListeners = append(startupListeners, exportListener)
	liveListener, err := ListenHTTP(ctx, options.LiveAddress)
	if err != nil {
		return err
	}
	startupListeners = append(startupListeners, liveListener)
	liveServer, err := NewLivenessHTTPServer(runtime.liveness)
	if err != nil {
		return err
	}

	serveCtx, cancelServe := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelServe()
	tasks := []controllerServeTask{
		{name: "CSI", run: func(taskCtx context.Context) (returnErr error) {
			defer func() {
				if err := csiListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					returnErr = errors.Join(returnErr, err)
				}
			}()
			return runtime.grpc.Serve(taskCtx, csiListener, runtime.shutdown)
		}},
		{name: "admin", run: func(taskCtx context.Context) error { return runtime.admin.Serve(taskCtx, adminListener) }},
		{name: "checkpoint export", run: func(taskCtx context.Context) error {
			return runtime.checkpointExport.Serve(taskCtx, exportListener)
		}},
		{name: "liveness", run: func(taskCtx context.Context) error { return liveServer.Serve(taskCtx, liveListener) }},
		{name: "heartbeat", run: runtime.heartbeat},
	}
	if options.MetricsAddress != "" {
		metricsListener, listenErr := ListenHTTP(ctx, options.MetricsAddress)
		if listenErr != nil {
			return listenErr
		}
		startupListeners = append(startupListeners, metricsListener)
		metricsServer, serverErr := NewMetricsHTTPServer(runtime.metrics)
		if serverErr != nil {
			return serverErr
		}
		tasks = append(tasks, controllerServeTask{name: "metrics", run: func(taskCtx context.Context) error {
			return metricsServer.Serve(taskCtx, metricsListener)
		}})
	}
	type taskResult struct {
		name string
		err  error
	}
	results := make(chan taskResult, len(tasks))
	listenersHandedOff = true
	for _, task := range tasks {
		task := task
		go func() { results <- taskResult{name: task.name, err: task.run(serveCtx)} }()
	}

	var primary error
	orderly := false
	select {
	case <-ctx.Done():
		orderly = true
	case leadershipErr := <-runtime.leadershipEvents:
		if leadershipErr == nil {
			primary = fmt.Errorf("controller leadership stopped unexpectedly")
		} else {
			primary = fmt.Errorf("controller leadership failed: %w", leadershipErr)
		}
	case metricErr := <-runtime.metricFailures:
		if metricErr == nil {
			metricErr = fmt.Errorf("nil CSI metrics failure")
		}
		primary = fmt.Errorf("record controller CSI metrics: %w", metricErr)
	case backgroundErr := <-runtime.backgroundEvents:
		if backgroundErr == nil {
			backgroundErr = fmt.Errorf("nil controller background failure")
		}
		primary = fmt.Errorf("controller background maintenance failed: %w", backgroundErr)
	case completed := <-results:
		if completed.err != nil {
			primary = fmt.Errorf("serve controller %s endpoint: %w", completed.name, completed.err)
		} else {
			primary = fmt.Errorf("controller %s endpoint stopped unexpectedly", completed.name)
		}
		tasks = slices.DeleteFunc(tasks, func(task controllerServeTask) bool { return task.name == completed.name })
	}
	_ = runtime.availability.Shutdown()
	// Start endpoint shutdown immediately.  The CSI and admin servers now drain
	// in parallel with mutation quiesce, so their own bounded shutdown cannot
	// consume a second full deadline after the controller safety protocol.
	cancelServe()

	if orderly {
		if active := runtime.activeSnapshot(); active != nil {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), runtime.shutdown)
			requestID, idErr := runtime.ids.New()
			if idErr == nil {
				idErr = runtime.gate.BeginQuiesce(shutdownCtx, requestID)
			}
			if idErr == nil {
				_, idErr = active.lease.ReleaseGracefully(shutdownCtx, requestID, runtime.gate, false)
				if idErr == nil {
					runtime.completeExpectedLeadershipStop(true)
				} else if active.leadership.Context().Err() != nil {
					runtime.completeExpectedLeadershipStop(false)
				}
			}
			cancelShutdown()
			if idErr != nil {
				primary = errors.Join(primary, fmt.Errorf("gracefully release controller leadership: %w", idErr))
			}
		}
	}
	for range len(tasks) {
		completed := <-results
		if completed.err != nil {
			primary = errors.Join(primary, fmt.Errorf("stop controller %s endpoint: %w", completed.name, completed.err))
		}
	}
	runtime.liveness.Close()
	_ = runtime.metrics.SetLeader(false)
	return primary
}

func (runtime *controllerRuntime) heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(controllerHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runtime.liveness.Heartbeat(); err != nil {
				return err
			}
			if err := runtime.metrics.SetMutationQueue(uint64(runtime.gate.Inflight()), uint64(runtime.gate.Queued())); err != nil {
				return err
			}
		}
	}
}

func (runtime *controllerRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var errs []error
	active := runtime.activeSnapshot()
	if active != nil && active.cancelMaintenance != nil {
		active.cancelMaintenance()
	}
	runtime.backgroundWG.Wait()
	if active != nil && active.leadership != nil {
		// Close is already irrevocably tearing down the process. Mark this Stop
		// as expected so the Lease-run forwarder cannot remain blocked waiting
		// for an uninstall or orderly-shutdown release disposition.
		if active.leadershipDisposition != nil {
			select {
			case active.leadershipDisposition <- true:
			default:
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		errs = append(errs, active.leadership.Stop(ctx))
		cancel()
	}
	if active != nil && active.targets != nil {
		errs = append(errs, active.targets.Close())
	}
	return errors.Join(errs...)
}
