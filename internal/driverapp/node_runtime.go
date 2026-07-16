package driverapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"slices"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/internal/csiadapter"
	buildversion "scaleway-sfs-subdir-csi/internal/version"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/health"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/observability"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
)

const (
	nodeLivenessMaxStall   = 30 * time.Second
	nodeHeartbeatInterval  = 5 * time.Second
	nodeStartupUnreadyText = "node startup is incomplete"
	nodeMaxMutations       = 10
)

type nodeRuntime struct {
	grpc           *csiadapter.GRPCServer
	admin          *admin.WireServer
	readiness      *driver.Readiness
	liveness       *health.Liveness
	metrics        *observability.NodeMetrics
	metricFailures chan error
	mounter        mount.Interface
	parents        []driver.NodeParentConfiguration
	parentRoot     string
	targets        *safety.KubeletTargetManager
	parentTargets  *safety.ParentTargetManager
	shutdown       time.Duration
}

func runNode(ctx context.Context, startup Startup) (returnErr error) {
	mountInfo, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return fmt.Errorf("open node startup mountinfo: %w", err)
	}
	runtime, err := buildNodeRuntime(ctx, startup, scaleway.NewLocalMetadataSource(), mountInfo)
	closeErr := mountInfo.Close()
	if err != nil {
		return errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return errors.Join(fmt.Errorf("close node startup mountinfo: %w", closeErr), runtime.Close())
	}
	defer func() { returnErr = errors.Join(returnErr, runtime.Close()) }()
	return runtime.serve(ctx, startup.Options)
}

func buildNodeRuntime(ctx context.Context, startup Startup, metadata scaleway.MetadataSource, mountInfo io.Reader) (*nodeRuntime, error) {
	if startup.Options.Component != config.ComponentNode {
		return nil, fmt.Errorf("node runtime received component %q", startup.Options.Component)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	configured := startup.Config.Runtime
	if err := configured.Validate(); err != nil {
		return nil, fmt.Errorf("validate node runtime configuration: %w", err)
	}
	if _, err := mount.PreflightNodePaths(ctx, mount.NodePathLayout{
		DriverName: configured.DriverName, ParentMountRoot: configured.Node.ParentMountRoot,
		KubeletPath: configured.Node.KubeletPath, CSISocketPath: startup.Options.CSIEndpointPath,
	}, mountInfo); err != nil {
		return nil, fmt.Errorf("preflight node paths: %w", err)
	}
	identity, err := scaleway.ResolveNodeIdentity(ctx, metadata, configured.Provider.Region, configured.Compatibility.QualifiedCommercialTypes)
	if err != nil {
		return nil, err
	}
	nodeID, err := identity.NodeID()
	if err != nil {
		return nil, err
	}
	parents, parentIDs, poolNames, err := nodeRuntimeInputs(configured.Pools)
	if err != nil {
		return nil, err
	}
	metrics, err := observability.NewNodeMetrics(poolNames)
	if err != nil {
		return nil, err
	}
	metricFailures := make(chan error, 1)

	parentTargets, err := safety.OpenParentTargetManager(configured.Node.ParentMountRoot)
	if err != nil {
		return nil, err
	}
	closeParentTargets := true
	defer func() {
		if closeParentTargets {
			_ = parentTargets.Close()
		}
	}()
	for _, parentID := range parentIDs {
		if err := parentTargets.Ensure(ctx, parentID); err != nil {
			return nil, fmt.Errorf("prepare node parent target %q: %w", parentID, err)
		}
	}

	targets, err := safety.OpenKubeletTargetManager(configured.Node.KubeletPath, configured.DriverName)
	if err != nil {
		return nil, err
	}
	closeTargets := true
	defer func() {
		if closeTargets {
			_ = targets.Close()
		}
	}()
	paths, err := driver.NewNodePathPolicy(configured.DriverName, configured.Node.KubeletPath, configured.Node.ParentMountRoot)
	if err != nil {
		return nil, err
	}
	registry, err := driver.NewNodeParentRegistry(configured.DriverName, configured.Installation.ID, parents)
	if err != nil {
		return nil, err
	}
	authorizer, err := driver.NewFilesystemNodeAuthorizer(registry, safety.NewOSNodeAuthorizationFilesystem())
	if err != nil {
		return nil, err
	}
	kernelMounter, err := mount.NewKernelMounter(configured.Node.ParentMountRoot, configured.Node.KubeletPath, configured.DriverName)
	if err != nil {
		return nil, err
	}
	kernelPreflight, ok := kernelMounter.(interface{ KernelPreflight(context.Context) error })
	if !ok {
		return nil, fmt.Errorf("production kernel mounter has no startup preflight")
	}
	if err := kernelPreflight.KernelPreflight(ctx); err != nil {
		return nil, fmt.Errorf("verify required Linux mount APIs and private quarantine: %w", err)
	}
	mounter, err := newObservedNodeMounter(kernelMounter, metrics, firstRuntimeFailureReporter(metricFailures))
	if err != nil {
		return nil, err
	}
	nodeGate, err := coordination.NewMutationGate(nodeMaxMutations)
	if err != nil {
		return nil, err
	}
	readiness := &driver.Readiness{}
	if err := readiness.Set(false, nodeStartupUnreadyText); err != nil {
		return nil, err
	}
	node, err := driver.NewNodeService(
		nodeID, paths, authorizer, targets, mounter, nodeGate,
		coordination.NewKeyedLock(), coordination.NewKeyedLock(),
	)
	if err != nil {
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
	nodeServer, err := csiadapter.NewNodeServer(node)
	if err != nil {
		return nil, err
	}
	grpcServer, err := csiadapter.NewObservedGRPCServer(
		config.ComponentNode, identityServer, nil, nodeServer,
		nodeCSIObserver{metrics: metrics}, firstRuntimeFailureReporter(metricFailures),
	)
	if err != nil {
		return nil, err
	}

	uninstall, err := admin.NewNodeUninstallCommandOperation(
		nodeID, configured.Node.ParentMountRoot, parentIDs, mounter, nodeGate,
		func(requestID string) error {
			return readiness.Set(false, "node terminal mount cleanup is active for request "+requestID)
		},
	)
	if err != nil {
		return nil, err
	}
	mux, err := admin.NewOperationMux(uninstall)
	if err != nil {
		return nil, err
	}
	adminServer, err := admin.NewWireServer(admin.HandshakeResponse{
		DriverVersion: buildversion.Version,
		ProtocolMajor: admin.ProtocolMajorV1,
		MinimumMinor:  admin.ProtocolMinorV1,
		MaximumMinor:  admin.ProtocolMinorV1,
	}, mux, admin.DefaultServerOptions())
	if err != nil {
		return nil, err
	}
	liveness, err := health.NewLiveness(clock.Real{}, nodeLivenessMaxStall)
	if err != nil {
		return nil, err
	}

	closeTargets = false
	closeParentTargets = false
	return &nodeRuntime{
		grpc: grpcServer, admin: adminServer, readiness: readiness,
		liveness: liveness, metrics: metrics, metricFailures: metricFailures,
		mounter: mounter, parents: parents, parentRoot: configured.Node.ParentMountRoot, targets: targets,
		parentTargets: parentTargets, shutdown: configured.Controller.ShutdownDeadline,
	}, nil
}

func nodeRuntimeInputs(pools []pool.Config) ([]driver.NodeParentConfiguration, []string, []string, error) {
	if err := pool.ValidateConfigs(pools); err != nil {
		return nil, nil, nil, err
	}
	parents := make([]driver.NodeParentConfiguration, 0)
	poolNames := make([]string, 0, len(pools))
	for _, configuredPool := range pools {
		poolNames = append(poolNames, configuredPool.Name)
		for _, parent := range configuredPool.Filesystems {
			parents = append(parents, driver.NodeParentConfiguration{
				PoolName: configuredPool.Name, ParentFilesystemID: parent.ID, BasePath: configuredPool.BasePath,
			})
		}
	}
	slices.Sort(poolNames)
	slices.SortFunc(parents, func(left, right driver.NodeParentConfiguration) int {
		if left.ParentFilesystemID < right.ParentFilesystemID {
			return -1
		}
		if left.ParentFilesystemID > right.ParentFilesystemID {
			return 1
		}
		return 0
	})
	parentIDs := make([]string, 0, len(parents))
	for _, parent := range parents {
		parentIDs = append(parentIDs, parent.ParentFilesystemID)
	}
	return parents, parentIDs, poolNames, nil
}

type nodeServeTask struct {
	name string
	run  func(context.Context) error
}

func (runtime *nodeRuntime) serve(ctx context.Context, options Options) error {
	if runtime == nil || runtime.grpc == nil || runtime.admin == nil || runtime.readiness == nil || runtime.liveness == nil || runtime.metrics == nil || runtime.metricFailures == nil || runtime.mounter == nil || len(runtime.parents) == 0 || runtime.parentRoot == "" || runtime.shutdown <= 0 {
		return fmt.Errorf("node runtime is incomplete")
	}
	csiListener, err := driver.ListenCSIUnix(options.CSIEndpointPath)
	if err != nil {
		return err
	}
	adminListener, err := admin.ListenUnix(options.AdminEndpointPath)
	if err != nil {
		_ = csiListener.Close()
		return err
	}
	liveListener, err := ListenHTTP(ctx, options.LiveAddress)
	if err != nil {
		_ = adminListener.Close()
		_ = csiListener.Close()
		return err
	}
	liveServer, err := NewLivenessHTTPServer(runtime.liveness)
	if err != nil {
		_ = liveListener.Close()
		_ = adminListener.Close()
		_ = csiListener.Close()
		return err
	}

	tasks := []nodeServeTask{
		{name: "CSI", run: func(taskCtx context.Context) (returnErr error) {
			defer func() {
				if err := csiListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					returnErr = errors.Join(returnErr, err)
				}
			}()
			return runtime.grpc.Serve(taskCtx, csiListener, runtime.shutdown)
		}},
		{name: "admin", run: func(taskCtx context.Context) error { return runtime.admin.Serve(taskCtx, adminListener) }},
		{name: "liveness", run: func(taskCtx context.Context) error { return liveServer.Serve(taskCtx, liveListener) }},
		{name: "heartbeat", run: runtime.heartbeat},
	}
	if err := runtime.refreshParentMountMetrics(ctx); err != nil {
		_ = liveListener.Close()
		_ = adminListener.Close()
		_ = csiListener.Close()
		return err
	}
	if options.MetricsAddress != "" {
		metricsListener, listenErr := ListenHTTP(ctx, options.MetricsAddress)
		if listenErr != nil {
			_ = liveListener.Close()
			_ = adminListener.Close()
			_ = csiListener.Close()
			return listenErr
		}
		metricsServer, serverErr := NewMetricsHTTPServer(runtime.metrics)
		if serverErr != nil {
			_ = metricsListener.Close()
			_ = liveListener.Close()
			_ = adminListener.Close()
			_ = csiListener.Close()
			return serverErr
		}
		tasks = append(tasks, nodeServeTask{name: "metrics", run: func(taskCtx context.Context) error {
			return metricsServer.Serve(taskCtx, metricsListener)
		}})
	}
	if err := runtime.readiness.Set(true, ""); err != nil {
		_ = liveListener.Close()
		_ = adminListener.Close()
		_ = csiListener.Close()
		return err
	}

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(tasks))
	for _, task := range tasks {
		task := task
		go func() { results <- result{name: task.name, err: task.run(serveCtx)} }()
	}
	var first result
	completedTasks := 0
	select {
	case first = <-results:
		completedTasks = 1
	case metricErr := <-runtime.metricFailures:
		if metricErr == nil {
			metricErr = fmt.Errorf("nil CSI metrics failure")
		}
		first = result{name: "CSI metrics", err: metricErr}
	}
	cancel()
	allErrors := make([]error, 0, len(tasks))
	if first.err != nil {
		allErrors = append(allErrors, fmt.Errorf("serve node %s endpoint: %w", first.name, first.err))
	} else if ctx.Err() == nil {
		allErrors = append(allErrors, fmt.Errorf("node %s endpoint stopped unexpectedly", first.name))
	}
	for completedTasks < len(tasks) {
		completed := <-results
		completedTasks++
		if completed.err != nil {
			allErrors = append(allErrors, fmt.Errorf("stop node %s endpoint: %w", completed.name, completed.err))
		}
	}
	_ = runtime.readiness.Set(false, "node process is shutting down")
	runtime.liveness.Close()
	return errors.Join(allErrors...)
}

func (runtime *nodeRuntime) heartbeat(ctx context.Context) error {
	ticker := time.NewTicker(nodeHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runtime.liveness.Heartbeat(); err != nil {
				return err
			}
			if err := runtime.refreshParentMountMetrics(ctx); err != nil {
				return err
			}
		}
	}
}

func (runtime *nodeRuntime) refreshParentMountMetrics(ctx context.Context) error {
	table, err := runtime.mounter.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("refresh node parent mount metrics: %w", err)
	}
	counts := make(map[string]uint64)
	for _, parent := range runtime.parents {
		if _, present := counts[parent.PoolName]; !present {
			counts[parent.PoolName] = 0
		}
		target := path.Join(runtime.parentRoot, parent.ParentFilesystemID)
		if _, err := table.Exact(target); errors.Is(err, mount.ErrNotMounted) {
			continue
		} else if err != nil {
			return fmt.Errorf("inspect node parent %q for metrics: %w", parent.ParentFilesystemID, err)
		}
		if _, err := mount.ValidateParent(table, target, parent.ParentFilesystemID); err != nil {
			return fmt.Errorf("validate node parent %q for metrics: %w", parent.ParentFilesystemID, err)
		}
		counts[parent.PoolName]++
	}
	pools := make([]string, 0, len(counts))
	for poolName := range counts {
		pools = append(pools, poolName)
	}
	slices.Sort(pools)
	for _, poolName := range pools {
		if err := runtime.metrics.SetParentMounts(poolName, counts[poolName]); err != nil {
			return err
		}
	}
	return nil
}

// Close releases long-lived directory descriptors after all serving goroutines
// have drained. It never unmounts warm parents during ordinary pod shutdown.
func (runtime *nodeRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	var errs []error
	if runtime.targets != nil {
		errs = append(errs, runtime.targets.Close())
	}
	if runtime.parentTargets != nil {
		errs = append(errs, runtime.parentTargets.Close())
	}
	return errors.Join(errs...)
}
