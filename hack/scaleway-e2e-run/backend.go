package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/releasequalification"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const (
	requiredFileStorageClusterTag        = "scw-filestorage-csi"
	provisioningDiscoveryStableReads     = 5
	provisioningDiscoveryMaximumAttempts = 10
	provisioningDiscoveryInitialBackoff  = 5 * time.Second
	provisioningDiscoveryMaximumBackoff  = 30 * time.Second
	maximumProviderReviewAge             = 24 * time.Hour
	maximumProviderReviewFutureSkew      = time.Minute
)

type scalewayBackend struct {
	request       e2erunner.Request
	plan          e2eplan.Plan
	client        *scw.Client
	kubernetes    *k8sapi.API
	file          *fileapi.API
	instance      *instanceapi.API
	inventoryPath string
	kubeconfig    string
	scenarioTool  string
}

func newScalewayBackend(request e2erunner.Request, plan e2eplan.Plan) (*scalewayBackend, error) {
	client, err := scw.NewClient(scw.WithEnv())
	if err != nil {
		return nil, fmt.Errorf("construct Scaleway client from environment: %w", err)
	}
	project, present := client.GetDefaultProjectID()
	if !present || project != plan.ProjectID {
		return nil, fmt.Errorf("SCW_DEFAULT_PROJECT_ID must equal the exact planned Project")
	}
	working, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	scenarioTool := filepath.Join(working, "hack", "run-kapsule-e2e.sh")
	info, err := os.Lstat(scenarioTool)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return nil, fmt.Errorf("checked-in Kapsule scenario runner is unavailable or not executable")
	}
	return &scalewayBackend{
		request: request, plan: plan, client: client,
		kubernetes: k8sapi.NewAPI(client), file: fileapi.NewAPI(client), instance: instanceapi.NewAPI(client),
		inventoryPath: plan.CleanupInventoryPath,
		kubeconfig:    filepath.Join(filepath.Dir(plan.CleanupInventoryPath), ".kubeconfig-"+plan.RunID),
		scenarioTool:  scenarioTool,
	}, nil
}

func (backend *scalewayBackend) LivePreflight(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan) error {
	candidateBytes, err := os.ReadFile(request.CandidateManifest)
	if err != nil {
		return fmt.Errorf("read candidate manifest: %w", err)
	}
	candidate, err := releasequalification.DecodeCandidate(candidateBytes)
	if err != nil {
		return fmt.Errorf("decode candidate manifest: %w", err)
	}
	candidateDirectory := filepath.Dir(request.CandidateManifest)
	if filepath.Dir(request.ChartPackage) != candidateDirectory || filepath.Dir(request.ReleaseValues) != candidateDirectory || filepath.Dir(request.AdminBinary) != candidateDirectory ||
		filepath.Base(request.ChartPackage) != candidate.ChartFile || filepath.Base(request.ReleaseValues) != candidate.ValuesFile {
		return fmt.Errorf("candidate chart, values, admin binary, and manifest must come from one exact artifact directory")
	}
	if err := releasequalification.VerifyCandidateArtifacts(candidateDirectory, candidate, filepath.Base(request.AdminBinary)); err != nil {
		return fmt.Errorf("verify candidate artifacts: %w", err)
	}
	candidateDigest, err := releasequalification.CandidateManifestDigest(candidateBytes)
	if err != nil || candidateDigest != plan.Artifacts.CandidateDigest {
		return fmt.Errorf("candidate manifest differs from the planned digest: %w", err)
	}
	adminInfo, err := os.Lstat(request.AdminBinary)
	if err != nil || !adminInfo.Mode().IsRegular() || adminInfo.Mode()&0o111 == 0 {
		return fmt.Errorf("candidate csi-admin is unavailable, non-regular, or non-executable")
	}
	if candidate.GitCommit != plan.Artifacts.GitCommit || candidate.ChartSHA256 != plan.Artifacts.ChartDigest || !sameArtifactImages(candidate.Images, plan.Artifacts.Images) {
		return fmt.Errorf("closed E2E plan names another candidate")
	}
	if !slices.Contains(candidate.QualifiedCommercialTypes, plan.NodePool.CommercialType) {
		return fmt.Errorf("planned commercial type %q is absent from the candidate allowlist", plan.NodePool.CommercialType)
	}
	if err := validateProviderReviewFresh(plan.ProviderReview, time.Now().UTC()); err != nil {
		return err
	}
	for _, command := range []string{"kubectl", "helm", "jq", "scw"} {
		if _, err := exec.LookPath(command); err != nil {
			return fmt.Errorf("required scenario command %q is unavailable", command)
		}
	}
	region := scw.Region(plan.Region)
	versions, err := backend.kubernetes.ListVersions(&k8sapi.ListVersionsRequest{Region: region}, scw.WithContext(ctx))
	if err != nil {
		return err
	}
	versionFound := false
	for _, version := range versions.Versions {
		if version != nil && version.Name == request.KapsuleVersion && slices.Contains(version.AvailableCnis, k8sapi.CNI("cilium")) && slices.Contains(version.AvailableContainerRuntimes, k8sapi.Runtime("containerd")) {
			versionFound = true
		}
	}
	if !versionFound {
		return fmt.Errorf("planned Kapsule version is not currently available with cilium and containerd")
	}
	types, err := backend.kubernetes.ListClusterTypes(&k8sapi.ListClusterTypesRequest{Region: region}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return err
	}
	typeFound := false
	for _, clusterType := range types.ClusterTypes {
		if clusterType != nil && clusterType.Name == request.KapsuleType && clusterType.Availability.String() == "available" {
			typeFound = true
		}
	}
	if !typeFound {
		return fmt.Errorf("planned Kapsule type is not currently available")
	}
	serverTypes, err := backend.instance.ListServersTypes(&instanceapi.ListServersTypesRequest{Zone: scw.Zone(request.Zone)}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("list live commercial-type capabilities: %w", err)
	}
	if serverTypes == nil {
		return fmt.Errorf("list live commercial-type capabilities returned an empty response")
	}
	serverType, present := serverTypes.Servers[plan.NodePool.CommercialType]
	if !present {
		return fmt.Errorf("planned commercial type %q is unavailable in zone %q", plan.NodePool.CommercialType, request.Zone)
	}
	if serverType == nil || serverType.Capabilities == nil || serverType.EndOfService {
		return fmt.Errorf("planned commercial type has no File Storage attachment capabilities or is end-of-service")
	}
	if err := validateAttachmentCapacity(serverType.Capabilities.MaxFileSystems, plan.Parents.Count); err != nil {
		return err
	}
	filesystems, err := backend.file.ListFileSystems(&fileapi.ListFileSystemsRequest{Region: region, ProjectID: &plan.ProjectID}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("validate File Storage read access: %w", err)
	}
	if filesystems == nil {
		return fmt.Errorf("validate File Storage regional availability returned an empty response")
	}
	return nil
}

func validateProviderReviewFresh(review e2eplan.ProviderReview, now time.Time) error {
	observed, err := time.Parse(time.RFC3339Nano, review.ObservedAt)
	if err != nil {
		return fmt.Errorf("parse provider review observation: %w", err)
	}
	if observed.After(now.Add(maximumProviderReviewFutureSkew)) {
		return fmt.Errorf("provider review observation is too far in the future")
	}
	if observed.Before(now.Add(-maximumProviderReviewAge)) {
		return fmt.Errorf("provider product, quota, and pricing review is older than %s", maximumProviderReviewAge)
	}
	return nil
}

func sameArtifactImages(left, right []e2eplan.ImageDigest) bool {
	left = slices.Clone(left)
	right = slices.Clone(right)
	compare := func(a, b e2eplan.ImageDigest) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(left, compare)
	slices.SortFunc(right, compare)
	return slices.Equal(left, right)
}

func (backend *scalewayBackend) Provision(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan) (e2ecleanup.Inventory, error) {
	inventory := backend.seedInventory()
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	region := scw.Region(plan.Region)
	if plan.Cluster.CreatedByRun {
		if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindCluster, plan.ResourcePrefix); err != nil {
			return inventory, err
		}
		project := plan.ProjectID
		cluster, err := backend.kubernetes.CreateCluster(&k8sapi.CreateClusterRequest{
			Region: region, ProjectID: &project, Type: request.KapsuleType,
			Name: plan.ResourcePrefix, Description: "Disposable SFS subdirectory CSI qualification " + plan.RunID,
			Tags: []string{plan.OwnershipTag, requiredFileStorageClusterTag}, Version: request.KapsuleVersion, Cni: k8sapi.CNI("cilium"),
			Pools: []*k8sapi.CreateClusterRequestPoolConfig{}, FeatureGates: []string{}, AdmissionPlugins: []string{}, ApiserverCertSans: []string{},
		}, scw.WithContext(ctx))
		if err != nil {
			return inventory, err
		}
		if cluster == nil {
			return inventory, fmt.Errorf("create Kapsule cluster returned an empty response")
		}
		if err := backend.completeProviderCreate(&inventory, backend.resource(e2ecleanup.ResourceKindCluster, cluster.ID, cluster.Name, true, cluster.Tags)); err != nil {
			return inventory, err
		}
		readyCluster, err := backend.kubernetes.WaitForCluster(&k8sapi.WaitForClusterRequest{Region: region, ClusterID: cluster.ID}, scw.WithContext(ctx))
		if err != nil {
			return inventory, err
		}
		if readyCluster == nil || readyCluster.ID != cluster.ID || readyCluster.ProjectID != plan.ProjectID || readyCluster.Region.String() != plan.Region ||
			!slices.Contains(readyCluster.Tags, plan.OwnershipTag) || !slices.Contains(readyCluster.Tags, requiredFileStorageClusterTag) {
			return inventory, fmt.Errorf("created Kapsule cluster does not expose the exact run and File Storage tags")
		}
	} else {
		cluster, err := backend.kubernetes.GetCluster(&k8sapi.GetClusterRequest{Region: region, ClusterID: plan.Cluster.ExistingID}, scw.WithContext(ctx))
		if err != nil || cluster == nil || cluster.ProjectID != plan.ProjectID || cluster.Region.String() != plan.Region || !slices.Contains(cluster.Tags, requiredFileStorageClusterTag) {
			return inventory, fmt.Errorf("validate reused exact cluster: %w", err)
		}
		inventory.Resources = append(inventory.Resources, backend.resource(e2ecleanup.ResourceKindCluster, cluster.ID, cluster.Name, false, cluster.Tags))
		if err := backend.writeInventory(inventory); err != nil {
			return inventory, err
		}
	}
	clusterID := resourceID(inventory, e2ecleanup.ResourceKindCluster, 0)
	poolName := plan.ResourcePrefix + "-nodes"
	if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindNodePool, poolName); err != nil {
		return inventory, err
	}
	pool, err := backend.kubernetes.CreatePool(&k8sapi.CreatePoolRequest{
		Region: region, ClusterID: clusterID, Name: poolName, NodeType: plan.NodePool.CommercialType,
		Autoscaling: false, Size: plan.NodePool.Count, ContainerRuntime: k8sapi.Runtime("containerd"), Autohealing: false,
		Tags: []string{plan.OwnershipTag}, KubeletArgs: map[string]string{}, Zone: scw.Zone(request.Zone), RootVolumeType: k8sapi.PoolVolumeType("default_volume_type"),
	}, scw.WithContext(ctx))
	if err != nil {
		return inventory, err
	}
	if pool == nil {
		return inventory, fmt.Errorf("create Kapsule node pool returned an empty response")
	}
	if err := backend.completeProviderCreate(&inventory, backend.resource(e2ecleanup.ResourceKindNodePool, pool.ID, pool.Name, true, pool.Tags)); err != nil {
		return inventory, err
	}
	if _, err := backend.kubernetes.WaitForPool(&k8sapi.WaitForPoolRequest{Region: region, PoolID: pool.ID}, scw.WithContext(ctx)); err != nil {
		return inventory, err
	}
	for index := uint32(0); index < plan.Parents.Count; index++ {
		name := fmt.Sprintf("%s-parent-%c", plan.ResourcePrefix, 'a'+index)
		if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindParent, name); err != nil {
			return inventory, err
		}
		filesystem, err := backend.file.CreateFileSystem(&fileapi.CreateFileSystemRequest{
			Region: region, Name: name, ProjectID: plan.ProjectID, Size: plan.Parents.SizeBytes, Tags: []string{plan.OwnershipTag},
		}, scw.WithContext(ctx))
		if err != nil {
			return inventory, err
		}
		if filesystem == nil {
			return inventory, fmt.Errorf("create File Storage parent returned an empty response")
		}
		if err := backend.completeProviderCreate(&inventory, backend.resource(e2ecleanup.ResourceKindParent, filesystem.ID, filesystem.Name, true, filesystem.Tags)); err != nil {
			return inventory, err
		}
		if _, err := backend.file.WaitForFileSystem(&fileapi.WaitForFileSystemRequest{Region: region, FilesystemID: filesystem.ID}, scw.WithContext(ctx)); err != nil {
			return inventory, err
		}
	}
	if plan.Profile == e2eplan.ProfileReleaseCandidate {
		project := plan.ProjectID
		image := request.InstanceImage
		instanceName := plan.ResourcePrefix + "-recovery"
		if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindInstance, instanceName); err != nil {
			return inventory, err
		}
		server, err := backend.instance.CreateServer(&instanceapi.CreateServerRequest{
			Zone: scw.Zone(request.Zone), Name: instanceName, CommercialType: plan.NodePool.CommercialType,
			Image: &image, Project: &project, Tags: []string{plan.OwnershipTag}, Protected: false,
		}, scw.WithContext(ctx))
		if err != nil {
			return inventory, fmt.Errorf("create disposable recovery Instance: %w", err)
		}
		if server == nil || server.Server == nil {
			return inventory, fmt.Errorf("create disposable recovery Instance returned an empty response")
		}
		if err := backend.completeProviderCreate(&inventory, backend.resource(e2ecleanup.ResourceKindInstance, server.Server.ID, server.Server.Name, true, server.Server.Tags)); err != nil {
			return inventory, err
		}
	}
	kubeconfig, err := backend.kubernetes.GetClusterKubeConfig(&k8sapi.GetClusterKubeConfigRequest{Region: region, ClusterID: clusterID}, scw.WithContext(ctx))
	if err != nil {
		return inventory, err
	}
	if err := replaceDurableFile(backend.kubeconfig, kubeconfig.GetRaw(), 0o600); err != nil {
		return inventory, err
	}
	inventory.Phase = e2ecleanup.PhaseReady
	inventory.Preconditions = allCleanupPreconditions(false)
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return inventory, backend.writeInventory(inventory)
}

func (backend *scalewayBackend) RunScenarios(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory) ([]e2erunner.ScenarioResult, error) {
	evidenceDirectory := filepath.Dir(plan.CleanupInventoryPath)
	arguments := []string{"--kubeconfig=" + backend.kubeconfig, "--chart=" + request.ChartPackage,
		"--values=" + request.ReleaseValues, "--namespace=" + request.DriverNamespace, "--release=" + request.HelmRelease,
		"--admin=" + request.AdminBinary, "--workload-image=" + request.WorkloadImage,
		"--project-id=" + plan.ProjectID, "--region=" + plan.Region, "--run-id=" + plan.RunID,
		"--cluster-id=" + resourceID(inventory, e2ecleanup.ResourceKindCluster, 0),
		"--parent-a=" + resourceID(inventory, e2ecleanup.ResourceKindParent, 0), "--parent-b=" + resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
		"--evidence-dir=" + evidenceDirectory}
	if request.PreviousChart != "" {
		arguments = append(arguments, "--previous-chart="+request.PreviousChart, "--previous-values="+request.PreviousValues)
	}
	pre, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-pre", arguments)
	if err != nil {
		return nil, err
	}
	provider, err := backend.runProviderScenarios(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	post, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-post", arguments)
	if err != nil {
		return nil, err
	}
	results := append(pre, provider...)
	results = append(results, post...)
	if err := e2erunner.ValidateScenarioResults(results); err != nil {
		return nil, err
	}
	return results, nil
}

func (backend *scalewayBackend) runScenarioPhase(ctx context.Context, evidenceDirectory, phase string, common []string) ([]e2erunner.ScenarioResult, error) {
	resultsPath := filepath.Join(evidenceDirectory, "scenario-results-"+phase+".json")
	arguments := append([]string{phase}, common...)
	arguments = append(arguments, "--results="+resultsPath)
	if err := backend.runScenarioCommand(ctx, arguments...); err != nil {
		return nil, err
	}
	encoded, err := os.ReadFile(resultsPath)
	if err != nil {
		return nil, err
	}
	var results []e2erunner.ScenarioResult
	if err := strictjson.Decode(encoded, &results); err != nil {
		return nil, err
	}
	// Validate basenames before joining any backend-controlled output with the
	// retained evidence directory. This keeps a compromised scenario process
	// from turning evidence verification into an arbitrary file read.
	if err := e2erunner.ValidateScenarioSubset(results); err != nil {
		return nil, err
	}
	for _, result := range results {
		digest, err := fileSHA256(filepath.Join(evidenceDirectory, result.EvidenceFile))
		if err != nil || digest != result.EvidenceSHA {
			return nil, fmt.Errorf("scenario evidence %q digest mismatch: %w", result.EvidenceFile, err)
		}
	}
	return results, nil
}

func (backend *scalewayBackend) Cleanup(ctx context.Context, request e2erunner.Request, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
	var err error
	if inventory.Phase == e2ecleanup.PhaseProvisioning {
		inventory, err = backend.confirmStableProvisioningDiscovery(ctx, inventory)
	} else {
		inventory, err = backend.reconcileRunResources(ctx, inventory)
	}
	if err != nil {
		return inventory, fmt.Errorf("reconcile exact run resources before cleanup: %w", err)
	}
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	evidenceDirectory := filepath.Dir(backend.plan.CleanupInventoryPath)
	preconditionsPath := filepath.Join(evidenceDirectory, "cleanup-preconditions.json")
	if _, err := os.Stat(backend.kubeconfig); err == nil {
		validator, executableErr := os.Executable()
		if executableErr != nil {
			return inventory, fmt.Errorf("locate uninstall evidence validator: %w", executableErr)
		}
		parentA := resourceID(inventory, e2ecleanup.ResourceKindParent, 0)
		parentB := resourceID(inventory, e2ecleanup.ResourceKindParent, 1)
		if parentA == "" || parentB == "" {
			return inventory, fmt.Errorf("safe uninstall requires the two retained parent IDs")
		}
		if err := backend.runScenarioCommand(ctx, "cleanup", "--kubeconfig="+backend.kubeconfig,
			"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
			"--admin="+request.AdminBinary,
			"--run-id="+backend.plan.RunID,
			"--parent-a="+parentA, "--parent-b="+parentB,
			"--validator="+validator,
			"--preconditions="+preconditionsPath, "--evidence-dir="+evidenceDirectory); err != nil {
			return inventory, err
		}
		encoded, err := os.ReadFile(preconditionsPath)
		if err != nil {
			return inventory, err
		}
		if err := strictjson.Decode(encoded, &inventory.Preconditions); err != nil {
			return inventory, err
		}
	} else if !os.IsNotExist(err) {
		return inventory, fmt.Errorf("inspect retained E2E kubeconfig: %w", err)
	} else if inventory.Phase != e2ecleanup.PhaseProvisioning && inventory.Phase != e2ecleanup.PhaseComplete {
		return inventory, fmt.Errorf("refuse cleanup without the retained kubeconfig after phase %q", inventory.Phase)
	} else if inventory.Phase == e2ecleanup.PhaseComplete {
		completePlan, buildErr := e2ecleanup.Build(inventory, time.Now().UTC())
		if buildErr != nil {
			return inventory, fmt.Errorf("validate complete cleanup without kubeconfig: %w", buildErr)
		}
		if !completePlan.CleanupComplete {
			return inventory, fmt.Errorf("complete cleanup without kubeconfig is not conclusively empty")
		}
	}
	inventory.Phase = e2ecleanup.PhaseCleanup
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	plan, err := e2ecleanup.Build(inventory, time.Now().UTC())
	if err == nil && plan.CleanupComplete {
		inventory.Phase = e2ecleanup.PhaseComplete
		inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := backend.writeInventory(inventory); err != nil {
			return inventory, err
		}
		if err := removeRetainedKubeconfig(backend.kubeconfig); err != nil {
			return inventory, err
		}
		return inventory, nil
	}
	if err != nil || !plan.ReadyForImmediateApproval {
		return inventory, fmt.Errorf("cleanup barriers do not authorize exact-ID deletion: %w", err)
	}
	for _, action := range plan.DeleteActions {
		resource, found := inventoryResource(inventory, action.ID)
		if !found || resource.Kind != action.Kind || resource.Name != action.Name || !resource.CreatedByRun {
			return inventory, fmt.Errorf("cleanup action differs from retained run-owned resource %q", action.ID)
		}
		if err := backend.deleteExact(ctx, resource, inventory); err != nil {
			return inventory, err
		}
		for index := range inventory.Resources {
			if inventory.Resources[index].ID == action.ID {
				inventory.Resources[index].State = e2ecleanup.ResourceStateAbsent
			}
		}
		inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := backend.writeInventory(inventory); err != nil {
			return inventory, err
		}
	}
	inventory.Phase = e2ecleanup.PhaseComplete
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	if err := removeRetainedKubeconfig(backend.kubeconfig); err != nil {
		return inventory, err
	}
	return inventory, nil
}

func validateAttachmentCapacity(maxFileSystems, parentCount uint32) error {
	if parentCount == 0 {
		return fmt.Errorf("planned parent count must be positive")
	}
	if maxFileSystems < parentCount {
		return fmt.Errorf("planned commercial type supports %d File Storage attachments but the run requires %d parents", maxFileSystems, parentCount)
	}
	return nil
}

func removeRetainedKubeconfig(path string) error {
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("remove retained E2E kubeconfig %q: %w", path, err)
}

// confirmStableProvisioningDiscovery prevents one temporarily empty list from
// turning an ambiguous provider Create into false cleanup completion. A failed
// provisioning phase may legitimately contain only a prefix of the plan, so
// the backend repeatedly discovers every deterministic run name and requires a
// stable exact-ID set before it can treat that prefix as authoritative. Any
// error or deadline preserves the provisioning ledger for cleanup-only retry.
func (backend *scalewayBackend) confirmStableProvisioningDiscovery(ctx context.Context, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
	return confirmStableProvisioningDiscovery(ctx, inventory, backend.reconcileRunResources, waitForProvisioningDiscovery)
}

func confirmStableProvisioningDiscovery(
	ctx context.Context,
	inventory e2ecleanup.Inventory,
	reconcile func(context.Context, e2ecleanup.Inventory) (e2ecleanup.Inventory, error),
	wait func(context.Context, time.Duration) error,
) (e2ecleanup.Inventory, error) {
	if reconcile == nil || wait == nil {
		return inventory, fmt.Errorf("provider-create discovery dependency is nil")
	}
	stableReads := 0
	var previousSnapshot []byte
	for attempt := 0; attempt < provisioningDiscoveryMaximumAttempts; attempt++ {
		observed, err := reconcile(ctx, inventory)
		if err != nil {
			return inventory, err
		}
		inventory = observed
		resolveDiscoveredCreateIntent(&inventory)
		snapshot, err := provisioningDiscoverySnapshot(inventory)
		if err != nil {
			return inventory, err
		}
		if bytes.Equal(snapshot, previousSnapshot) {
			stableReads++
		} else {
			previousSnapshot = snapshot
			stableReads = 1
		}
		if stableReads >= provisioningDiscoveryStableReads {
			if inventory.PendingCreate != nil {
				return inventory, fmt.Errorf("provider Create for %s %s remains unresolved after stable exact-ID discovery", inventory.PendingCreate.Kind, inventory.PendingCreate.Name)
			}
			return inventory, nil
		}
		backoff := provisioningDiscoveryInitialBackoff << min(attempt, 3)
		if backoff > provisioningDiscoveryMaximumBackoff {
			backoff = provisioningDiscoveryMaximumBackoff
		}
		if err := wait(ctx, backoff); err != nil {
			return inventory, fmt.Errorf("wait for conclusive provider-create discovery: %w", err)
		}
	}
	return inventory, fmt.Errorf("provider-create discovery did not stabilize after %d attempts", provisioningDiscoveryMaximumAttempts)
}

func provisioningDiscoverySnapshot(inventory e2ecleanup.Inventory) ([]byte, error) {
	resources := slices.Clone(inventory.Resources)
	for index := range resources {
		resources[index].Tags = slices.Clone(resources[index].Tags)
		slices.Sort(resources[index].Tags)
	}
	slices.SortFunc(resources, func(left, right e2ecleanup.Resource) int {
		if comparison := strings.Compare(left.Kind, right.Kind); comparison != 0 {
			return comparison
		}
		if comparison := strings.Compare(left.Name, right.Name); comparison != 0 {
			return comparison
		}
		return strings.Compare(left.ID, right.ID)
	})
	return canonicaljson.Marshal(struct {
		PendingCreate *e2ecleanup.CreateIntent `json:"pendingCreate,omitempty"`
		Resources     []e2ecleanup.Resource    `json:"resources"`
	}{PendingCreate: inventory.PendingCreate, Resources: resources})
}

func waitForProvisioningDiscovery(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (backend *scalewayBackend) deleteExact(ctx context.Context, resource e2ecleanup.Resource, inventory e2ecleanup.Inventory) error {
	region := scw.Region(backend.plan.Region)
	switch resource.Kind {
	case e2ecleanup.ResourceKindInstance:
		observed, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: scw.Zone(backend.request.Zone), ServerID: resource.ID}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || observed.Server == nil || observed.Server.Project != backend.plan.ProjectID || observed.Server.Name != resource.Name || !slices.Contains(observed.Server.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("refuse deletion of mismatched disposable Instance %s: %w", resource.ID, err)
		}
		if err := backend.instance.DeleteServer(&instanceapi.DeleteServerRequest{Zone: scw.Zone(backend.request.Zone), ServerID: resource.ID}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
			return err
		}
	case e2ecleanup.ResourceKindNodePool:
		observed, err := backend.kubernetes.GetPool(&k8sapi.GetPoolRequest{Region: region, PoolID: resource.ID}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || observed.Name != resource.Name || observed.ClusterID != resourceID(inventory, e2ecleanup.ResourceKindCluster, 0) || !slices.Contains(observed.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("refuse deletion of mismatched node pool %s: %w", resource.ID, err)
		}
		if _, err := backend.kubernetes.DeletePool(&k8sapi.DeletePoolRequest{Region: region, PoolID: resource.ID}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
			return err
		}
	case e2ecleanup.ResourceKindParent:
		filesystem, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: resource.ID}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || filesystem.ProjectID != backend.plan.ProjectID || filesystem.Name != resource.Name || !slices.Contains(filesystem.Tags, backend.plan.OwnershipTag) || filesystem.NumberOfAttachments != 0 {
			return fmt.Errorf("parent %s is unavailable, mismatched, or still attached: %w", resource.ID, err)
		}
		if err := backend.file.DeleteFileSystem(&fileapi.DeleteFileSystemRequest{Region: region, FilesystemID: resource.ID}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
			return err
		}
	case e2ecleanup.ResourceKindCluster:
		observed, err := backend.kubernetes.GetCluster(&k8sapi.GetClusterRequest{Region: region, ClusterID: resource.ID}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || observed.ProjectID != backend.plan.ProjectID || observed.Name != resource.Name || !slices.Contains(observed.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("refuse deletion of mismatched cluster %s: %w", resource.ID, err)
		}
		if _, err := backend.kubernetes.DeleteCluster(&k8sapi.DeleteClusterRequest{Region: region, ClusterID: resource.ID, WithAdditionalResources: false}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
			return err
		}
	default:
		return fmt.Errorf("unsupported exact cleanup kind %q", resource.Kind)
	}
	return backend.waitAbsent(ctx, resource.Kind, resource.ID)
}

func (backend *scalewayBackend) waitAbsent(ctx context.Context, kind, id string) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		present, err := backend.exactPresent(ctx, kind, id)
		if err != nil {
			return err
		}
		if !present {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) exactPresent(ctx context.Context, kind, id string) (bool, error) {
	region := scw.Region(backend.plan.Region)
	var err error
	switch kind {
	case e2ecleanup.ResourceKindInstance:
		_, err = backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: scw.Zone(backend.request.Zone), ServerID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindNodePool:
		_, err = backend.kubernetes.GetPool(&k8sapi.GetPoolRequest{Region: region, PoolID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindParent:
		_, err = backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindCluster:
		_, err = backend.kubernetes.GetCluster(&k8sapi.GetClusterRequest{Region: region, ClusterID: id}, scw.WithContext(ctx))
	default:
		return false, fmt.Errorf("unsupported exact observation kind %q", kind)
	}
	if err == nil {
		return true, nil
	}
	if providerNotFound(err) {
		return false, nil
	}
	return false, err
}

func (backend *scalewayBackend) runScenarioCommand(ctx context.Context, arguments ...string) error {
	command := exec.CommandContext(ctx, backend.scenarioTool, arguments...)
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	logPath := filepath.Join(filepath.Dir(backend.plan.CleanupInventoryPath), "scenario-runner.log")
	if writeErr := replaceDurableFile(logPath, output, 0o600); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	if err != nil {
		return fmt.Errorf("Kapsule scenario command failed: %w", err) //nolint:staticcheck // Kapsule is a product name.
	}
	return nil
}

func (backend *scalewayBackend) resource(kind, id, name string, created bool, tags []string) e2ecleanup.Resource {
	return e2ecleanup.Resource{Kind: kind, ID: id, Name: name, ProjectID: backend.plan.ProjectID,
		Region: backend.plan.Region, Tags: slices.Clone(tags), CreatedByRun: created, State: e2ecleanup.ResourceStatePresent}
}

func (backend *scalewayBackend) beginProviderCreate(inventory *e2ecleanup.Inventory, kind, name string) error {
	if inventory.PendingCreate != nil {
		return fmt.Errorf("provider Create for %s %s is already unresolved", inventory.PendingCreate.Kind, inventory.PendingCreate.Name)
	}
	next := *inventory
	next.PendingCreate = &e2ecleanup.CreateIntent{Kind: kind, Name: name}
	next.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := backend.writeInventory(next); err != nil {
		return fmt.Errorf("persist provider Create intent for %s %s: %w", kind, name, err)
	}
	*inventory = next
	return nil
}

func (backend *scalewayBackend) completeProviderCreate(inventory *e2ecleanup.Inventory, resource e2ecleanup.Resource) error {
	if inventory.PendingCreate == nil || inventory.PendingCreate.Kind != resource.Kind || inventory.PendingCreate.Name != resource.Name {
		return fmt.Errorf("provider Create result for %s %s differs from retained intent", resource.Kind, resource.Name)
	}
	inventory.Resources = append(inventory.Resources, resource)
	inventory.PendingCreate = nil
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := backend.writeInventory(*inventory); err != nil {
		return fmt.Errorf("persist provider Create result for %s %s: %w", resource.Kind, resource.Name, err)
	}
	return nil
}

func (backend *scalewayBackend) writeInventory(inventory e2ecleanup.Inventory) error {
	encoded, err := canonicaljson.Marshal(inventory)
	if err != nil {
		return err
	}
	return replaceDurableFile(backend.inventoryPath, append(encoded, '\n'), 0o600)
}

func resourceID(inventory e2ecleanup.Inventory, kind string, ordinal int) string {
	for _, resource := range inventory.Resources {
		if resource.Kind == kind {
			if ordinal == 0 {
				return resource.ID
			}
			ordinal--
		}
	}
	return ""
}

func inventoryResource(inventory e2ecleanup.Inventory, id string) (e2ecleanup.Resource, bool) {
	for _, resource := range inventory.Resources {
		if resource.ID == id {
			return resource, true
		}
	}
	return e2ecleanup.Resource{}, false
}

func providerNotFound(err error) bool {
	var response *scw.ResponseError
	if errors.As(err, &response) && response.StatusCode == 404 {
		return true
	}
	var notFound *scw.ResourceNotFoundError
	return errors.As(err, &notFound)
}

func allCleanupPreconditions(value bool) e2ecleanup.Preconditions {
	return e2ecleanup.Preconditions{WorkloadPodsRemoved: value, PVCsRemoved: value, PVsRemoved: value,
		VolumeAttachmentsRemoved: value, UnpublishAndUnstageComplete: value, PublishedNodeFencesCleared: value,
		UninstallPrepareComplete: value, NodeDaemonSetStopped: value, NodeMountsAbsent: value,
		ControllerMountsAbsent: value, ParentAttachmentsAbsent: value, ControllerStopped: value, HelmUninstalled: value}
}
