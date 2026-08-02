package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	blockapi "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	vpcapi "github.com/scaleway/scaleway-sdk-go/api/vpc/v2"
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
	request        e2erunner.Request
	plan           e2eplan.Plan
	kubernetes     *k8sapi.API
	block          *blockapi.API
	file           *fileapi.API
	instance       *instanceapi.API
	vpc            *vpcapi.API
	inventoryPath  string
	kubeconfig     string
	scenarioTool   string
	maxFileSystems uint32
}

type plannedBootstrapAbortEvidence struct {
	SchemaVersion               string `json:"schemaVersion"`
	RunID                       string `json:"runId"`
	Profile                     string `json:"profile"`
	Region                      string `json:"region"`
	ClusterCreatedByRun         bool   `json:"clusterCreatedByRun"`
	Namespace                   string `json:"namespace"`
	HelmRelease                 string `json:"helmRelease"`
	HelmStatus                  string `json:"helmStatus"`
	ParentA                     string `json:"parentA"`
	ParentB                     string `json:"parentB"`
	ScenarioEntries             int    `json:"scenarioEntries"`
	InitialWorkloadPods         int    `json:"initialWorkloadPods"`
	InitialPVCs                 int    `json:"initialPVCs"`
	WorkloadPods                int    `json:"workloadPods"`
	PVCs                        int    `json:"pvcs"`
	PVs                         int    `json:"pvs"`
	VolumeAttachments           int    `json:"volumeAttachments"`
	DriverCSINodeRegistrations  int    `json:"driverCSINodeRegistrations"`
	DurableRecords              int    `json:"durableRecords"`
	ParentAAttachments          int    `json:"parentAAttachments"`
	ParentBAttachments          int    `json:"parentBAttachments"`
	ParentAReportedAttachments  int    `json:"parentAReportedAttachments"`
	ParentBReportedAttachments  int    `json:"parentBReportedAttachments"`
	FreshBootstrapPlanVerified  bool   `json:"freshBootstrapPlanVerified"`
	PlannedControllerInstanceID string `json:"plannedControllerInstanceId"`
	PlannedControllerZone       string `json:"plannedControllerZone"`
	PlannedParentAttachments    int    `json:"plannedParentAttachments"`
	ParentAttachmentsAbsent     bool   `json:"parentAttachmentsAbsent"`
	HelmUninstalled             bool   `json:"helmUninstalled"`
	NamespaceRemoved            bool   `json:"namespaceRemoved"`
}

func newScalewayBackend(request e2erunner.Request, plan e2eplan.Plan) (*scalewayBackend, error) {
	client, err := newRegionalScalewayClientFromEnvironment(plan)
	if err != nil {
		return nil, err
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
		request: request, plan: plan,
		kubernetes: k8sapi.NewAPI(client), block: blockapi.NewAPI(client), file: fileapi.NewAPI(client),
		instance: instanceapi.NewAPI(client), vpc: vpcapi.NewAPI(client),
		inventoryPath: plan.CleanupInventoryPath,
		kubeconfig:    filepath.Join(filepath.Dir(plan.CleanupInventoryPath), ".kubeconfig-"+plan.RunID),
		scenarioTool:  scenarioTool,
	}, nil
}

// newRegionalScalewayClientFromEnvironment deliberately copies only
// credentials and the closed Project/region scope from the environment client.
// In particular it never copies SCW_DEFAULT_ZONE: the pinned File Storage SDK
// fills an omitted attachment-list zone from that client default, which would
// turn the required regional cleanup inventory into a one-zone view.
func newRegionalScalewayClientFromEnvironment(plan e2eplan.Plan) (*scw.Client, error) {
	if strings.TrimSpace(os.Getenv("SCW_DEFAULT_ORGANIZATION_ID")) == "" {
		return nil, fmt.Errorf("SCW_DEFAULT_ORGANIZATION_ID is required for provider CLI preflight")
	}
	environmentClient, err := scw.NewClient(scw.WithEnv())
	if err != nil {
		return nil, fmt.Errorf("load Scaleway authority from environment")
	}
	project, present := environmentClient.GetDefaultProjectID()
	if !present || project != plan.ProjectID {
		return nil, fmt.Errorf("SCW_DEFAULT_PROJECT_ID must equal the exact planned Project")
	}
	if organization, present := environmentClient.GetDefaultOrganizationID(); !present || organization == "" {
		// The Go APIs below are Project-scoped, but the deliberately narrow scw
		// CLI reads in the scenario harness still require this non-secret scope
		// value when the runner has no persistent Scaleway profile. Refuse before
		// creating billable resources instead of discovering it after bootstrap.
		return nil, fmt.Errorf("SCW_DEFAULT_ORGANIZATION_ID is required for provider CLI preflight")
	}
	accessKey, accessPresent := environmentClient.GetAccessKey()
	secretKey, secretPresent := environmentClient.GetSecretKey()
	if !accessPresent || !secretPresent {
		return nil, fmt.Errorf("SCW_ACCESS_KEY and SCW_SECRET_KEY are required for approved live execution")
	}
	client, err := scw.NewClient(
		scw.WithAuth(accessKey, secretKey),
		scw.WithDefaultProjectID(plan.ProjectID),
		scw.WithDefaultRegion(scw.Region(plan.Region)),
		scw.WithUserAgent("scaleway-sfs-subdir-csi-e2e/1"),
	)
	if err != nil {
		return nil, fmt.Errorf("construct zone-free regional Scaleway client")
	}
	if _, hasZone := client.GetDefaultZone(); hasZone {
		return nil, fmt.Errorf("regional Scaleway client unexpectedly has a default zone")
	}
	return client, nil
}

func (backend *scalewayBackend) LivePreflight(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan) error {
	candidate, err := validateLocalCandidateArtifacts(ctx, request, plan)
	if err != nil {
		return err
	}
	if err := validateLocalPredecessorArtifacts(request, candidate); err != nil {
		return err
	}
	if !slices.Contains(candidate.QualifiedCommercialTypes, plan.NodePool.CommercialType) {
		return fmt.Errorf("planned commercial type %q is absent from the candidate allowlist", plan.NodePool.CommercialType)
	}
	if err := validateProviderReviewFresh(plan.ProviderReview, time.Now().UTC()); err != nil {
		return err
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
	if types == nil {
		return fmt.Errorf("list regional Kapsule types returned an empty response")
	}
	typeFound := false
	var typeAvailability k8sapi.ClusterTypeAvailability
	for _, clusterType := range types.ClusterTypes {
		if clusterType != nil && clusterType.Name == request.KapsuleType {
			if typeFound {
				return fmt.Errorf("regional catalog contains duplicate Kapsule type %q", request.KapsuleType)
			}
			typeFound = true
			typeAvailability = clusterType.Availability
		}
	}
	if !typeFound {
		return fmt.Errorf("planned Kapsule type %q is absent from the regional catalog", request.KapsuleType)
	}
	if !creatableClusterTypeAvailability(typeAvailability) {
		return fmt.Errorf("planned Kapsule type %q has non-creatable availability %q", request.KapsuleType, string(typeAvailability))
	}
	if err := backend.refreshPlannedAttachmentCapability(ctx, request, plan); err != nil {
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

func (backend *scalewayBackend) refreshPlannedAttachmentCapability(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
) error {
	serverTypes, err := backend.instance.ListServersTypes(
		&instanceapi.ListServersTypesRequest{Zone: scw.Zone(request.Zone)},
		scw.WithAllPages(),
		scw.WithContext(ctx),
	)
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
	backend.maxFileSystems = serverType.Capabilities.MaxFileSystems
	return nil
}

func validateLocalCandidateArtifacts(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
) (releasequalification.CandidateManifest, error) {
	candidateBytes, err := readExactArtifactManifest(request.CandidateManifest)
	if err != nil {
		return releasequalification.CandidateManifest{}, fmt.Errorf("read candidate manifest: %w", err)
	}
	candidate, err := releasequalification.DecodeCandidate(candidateBytes)
	if err != nil {
		return releasequalification.CandidateManifest{}, fmt.Errorf("decode candidate manifest: %w", err)
	}
	candidateDirectory := filepath.Dir(request.CandidateManifest)
	if filepath.Dir(request.ChartPackage) != candidateDirectory || filepath.Dir(request.ReleaseValues) != candidateDirectory || filepath.Dir(request.AdminBinary) != candidateDirectory ||
		filepath.Base(request.ChartPackage) != candidate.ChartFile || filepath.Base(request.ReleaseValues) != candidate.ValuesFile {
		return releasequalification.CandidateManifest{}, fmt.Errorf("candidate chart, values, admin binary, and manifest must come from one exact artifact directory")
	}
	if err := releasequalification.VerifyCandidateArtifacts(candidateDirectory, candidate, filepath.Base(request.AdminBinary)); err != nil {
		return releasequalification.CandidateManifest{}, fmt.Errorf("verify candidate artifacts: %w", err)
	}
	candidateDigest, err := releasequalification.CandidateManifestDigest(candidateBytes)
	if err != nil {
		return releasequalification.CandidateManifest{}, fmt.Errorf("digest candidate manifest: %w", err)
	}
	if candidateDigest != plan.Artifacts.CandidateDigest {
		return releasequalification.CandidateManifest{}, fmt.Errorf("candidate manifest differs from the planned digest")
	}
	adminInfo, err := os.Lstat(request.AdminBinary)
	if err != nil || !adminInfo.Mode().IsRegular() || adminInfo.Mode()&0o111 == 0 {
		return releasequalification.CandidateManifest{}, fmt.Errorf("candidate csi-admin is unavailable, non-regular, or non-executable")
	}
	if candidate.GitCommit != plan.Artifacts.GitCommit || candidate.ChartSHA256 != plan.Artifacts.ChartDigest || !sameArtifactImages(candidate.Images, plan.Artifacts.Images) {
		return releasequalification.CandidateManifest{}, fmt.Errorf("closed E2E plan names another candidate")
	}
	if err := runCredentialFreeCommand(ctx, request.AdminBinary, "version"); err != nil {
		return releasequalification.CandidateManifest{}, fmt.Errorf("execute exact candidate csi-admin before provider mutation: %w", err)
	}
	for _, command := range []string{"kubectl", "helm", "jq", "scw"} {
		if _, err := exec.LookPath(command); err != nil {
			return releasequalification.CandidateManifest{}, fmt.Errorf("required scenario command %q is unavailable", command)
		}
	}
	return candidate, nil
}

func validateLocalPredecessorArtifacts(request e2erunner.Request, candidate releasequalification.CandidateManifest) error {
	if request.Predecessor != nil {
		previousBytes, previousErr := readExactArtifactManifest(request.PreviousManifest)
		if previousErr != nil {
			return fmt.Errorf("read exact public predecessor manifest: %w", previousErr)
		}
		previous, previousErr := releasequalification.DecodeCandidate(previousBytes)
		if previousErr != nil {
			return fmt.Errorf("decode exact public predecessor manifest: %w", previousErr)
		}
		compatibilityIdentity, previousErr := releasequalification.CandidateManifestDigest(previousBytes)
		if previousErr != nil {
			return fmt.Errorf("digest exact public predecessor manifest: %w", previousErr)
		}
		chartDigest, chartErr := releasequalification.DigestFile(request.PreviousChart)
		valuesDigest, valuesErr := releasequalification.DigestFile(request.PreviousValues)
		if chartErr != nil || valuesErr != nil {
			return fmt.Errorf("hash exact public predecessor artifacts: %w", errors.Join(chartErr, valuesErr))
		}
		if chartDigest != request.Predecessor.ChartSHA256 || valuesDigest != request.Predecessor.ValuesSHA256 {
			return fmt.Errorf("local predecessor chart or values differs from the closed public identity")
		}
		previousDirectory := filepath.Dir(request.PreviousManifest)
		if filepath.Dir(request.PreviousChart) != previousDirectory ||
			filepath.Dir(request.PreviousValues) != previousDirectory ||
			filepath.Base(request.PreviousChart) != previous.ChartFile ||
			filepath.Base(request.PreviousValues) != previous.ValuesFile ||
			previous.ReleaseTag != request.Predecessor.ReleaseTag ||
			previous.Version != request.Predecessor.Version ||
			previous.ChartSHA256 != request.Predecessor.ChartSHA256 ||
			previous.ValuesSHA256 != request.Predecessor.ValuesSHA256 ||
			previous.DriverImage != request.Predecessor.DriverImage ||
			previous.DriverName != candidate.DriverName ||
			compatibilityIdentity != request.Predecessor.CompatibilityIdentity {
			return fmt.Errorf("public predecessor manifest differs from the closed compatibility identity")
		}
		if request.Predecessor.DriverImage == candidate.DriverImage {
			return fmt.Errorf("public predecessor and candidate use the same driver image")
		}
	}
	return nil
}

func readExactArtifactManifest(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("artifact manifest must be an exact regular file")
	}
	return os.ReadFile(path)
}

func runCredentialFreeCommand(ctx context.Context, name string, arguments ...string) error {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Env = environmentWithoutScalewayCredentials()
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if len(message) > 2048 {
			message = message[:2048]
		}
		return fmt.Errorf("run %s: %w: %s", filepath.Base(name), err, message)
	}
	return nil
}

// creatableClusterTypeAvailability follows the provider's closed stock
// contract: scarce means limited availability and still permits creation,
// while shortage and any future or missing value fail closed.
func creatableClusterTypeAvailability(availability k8sapi.ClusterTypeAvailability) bool {
	return availability == k8sapi.ClusterTypeAvailabilityAvailable || availability == k8sapi.ClusterTypeAvailabilityScarce
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
	if !plan.Cluster.CreatedByRun {
		return inventory, fmt.Errorf("v1 real E2E refuses a cluster not owned by the exact run")
	}
	privateNetworkName := plan.ResourcePrefix + "-network"
	if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindPrivateNetwork, privateNetworkName); err != nil {
		return inventory, err
	}
	privateNetwork, err := backend.vpc.CreatePrivateNetwork(&vpcapi.CreatePrivateNetworkRequest{
		Region: region, Name: privateNetworkName, ProjectID: plan.ProjectID,
		Tags: []string{plan.OwnershipTag}, Subnets: []scw.IPNet{}, DefaultRoutePropagationEnabled: false,
	}, scw.WithContext(ctx))
	if err != nil {
		return inventory, fmt.Errorf("create Kapsule Private Network: %w", err)
	}
	if privateNetwork == nil || privateNetwork.ID == "" {
		return inventory, fmt.Errorf("create Kapsule Private Network returned an empty response")
	}
	if err := backend.completeProviderCreate(&inventory, backend.resource(e2ecleanup.ResourceKindPrivateNetwork, privateNetwork.ID, privateNetwork.Name, true, privateNetwork.Tags)); err != nil {
		return inventory, err
	}
	if privateNetwork.ProjectID != plan.ProjectID || privateNetwork.Region.String() != plan.Region || privateNetwork.Name != privateNetworkName || privateNetwork.VpcID == "" || !slices.Contains(privateNetwork.Tags, plan.OwnershipTag) {
		return inventory, fmt.Errorf("created Private Network differs from the exact run-owned scope")
	}

	if err := backend.beginProviderCreate(&inventory, e2ecleanup.ResourceKindCluster, plan.ResourcePrefix); err != nil {
		return inventory, err
	}
	project := plan.ProjectID
	cluster, err := backend.kubernetes.CreateCluster(&k8sapi.CreateClusterRequest{
		Region: region, ProjectID: &project, Type: request.KapsuleType,
		Name: plan.ResourcePrefix, Description: "Disposable SFS subdirectory CSI qualification " + plan.RunID,
		Tags: []string{plan.OwnershipTag, requiredFileStorageClusterTag}, Version: request.KapsuleVersion, Cni: k8sapi.CNI("cilium"),
		Pools: []*k8sapi.CreateClusterRequestPoolConfig{}, FeatureGates: []string{}, AdmissionPlugins: []string{}, ApiserverCertSans: []string{},
		PrivateNetworkID: &privateNetwork.ID,
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
	if cluster.PrivateNetworkID == nil || *cluster.PrivateNetworkID != privateNetwork.ID {
		return inventory, fmt.Errorf("created Kapsule cluster differs from the exact run-owned Private Network")
	}
	readyCluster, err := backend.kubernetes.WaitForCluster(&k8sapi.WaitForClusterRequest{Region: region, ClusterID: cluster.ID}, scw.WithContext(ctx))
	if err != nil {
		return inventory, err
	}
	if readyCluster == nil || readyCluster.ID != cluster.ID || readyCluster.ProjectID != plan.ProjectID || readyCluster.Region.String() != plan.Region ||
		readyCluster.PrivateNetworkID == nil || *readyCluster.PrivateNetworkID != privateNetwork.ID ||
		!slices.Contains(readyCluster.Tags, plan.OwnershipTag) || !slices.Contains(readyCluster.Tags, requiredFileStorageClusterTag) {
		return inventory, fmt.Errorf("created Kapsule cluster does not expose the exact network, run, and File Storage identity")
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
	readyPool, err := backend.kubernetes.WaitForPool(
		&k8sapi.WaitForPoolRequest{Region: region, PoolID: pool.ID},
		scw.WithContext(ctx),
	)
	if err != nil {
		return inventory, err
	}
	if err := validateReplacementPool(
		readyPool,
		plan,
		clusterID,
		pool.ID,
		plan.NodePool.Count,
	); err != nil {
		return inventory, fmt.Errorf("validate initial exact Kapsule pool: %w", err)
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
		rootVolume, err := backend.normalizeDisposableInstanceRootVolume(ctx, server.Server)
		if err != nil {
			return inventory, fmt.Errorf("journal disposable recovery Instance root volume: %w", err)
		}
		instanceResource := backend.resource(e2ecleanup.ResourceKindInstance, server.Server.ID, server.Server.Name, true, server.Server.Tags)
		if err := backend.completeDisposableInstanceCreate(&inventory, instanceResource, rootVolume); err != nil {
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
	arguments, err := backend.scenarioArguments(request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	if plan.Profile == e2eplan.ProfileBase {
		smoke, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-smoke", arguments)
		if err != nil {
			return nil, err
		}
		attachment, err := backend.providerAttachmentScenario(ctx, request, inventory, evidenceDirectory, "provider-attachment-inventory")
		if err != nil {
			return nil, err
		}
		results := append(smoke, attachment)
		if err := e2erunner.ValidateSmokeScenarioResults(results); err != nil {
			return nil, err
		}
		if err := e2erunner.ValidateCandidateScenarioImages(plan.Profile, results, plan.Artifacts.Images); err != nil {
			return nil, err
		}
		return results, nil
	}
	pre, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-pre", arguments)
	if err != nil {
		return nil, err
	}
	if request.Predecessor == nil {
		return nil, fmt.Errorf("release qualification lost its closed predecessor identity")
	}
	if err := e2erunner.ValidatePredecessorScenario(pre, *request.Predecessor); err != nil {
		return nil, err
	}
	if err := e2erunner.ValidateCandidateScenarioImages(plan.Profile, pre, plan.Artifacts.Images); err != nil {
		return nil, err
	}
	provider, err := backend.runProviderScenarios(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	destructive, err := backend.runDestructiveControllerAndNodeScenarios(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	mid, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-mid", arguments)
	if err != nil {
		return nil, err
	}
	recovery, err := backend.runCheckpointRecoveryScenarios(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	post, err := backend.runScenarioPhase(ctx, evidenceDirectory, "run-post", arguments)
	if err != nil {
		return nil, err
	}
	results := append(pre, provider...)
	results = append(results, destructive...)
	results = append(results, mid...)
	results = append(results, recovery...)
	results = append(results, post...)
	if err := e2erunner.ValidateScenarioResults(results); err != nil {
		return nil, err
	}
	return results, nil
}

func (backend *scalewayBackend) scenarioArguments(
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
	evidenceDirectory string,
) ([]string, error) {
	validator, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate scenario evidence validator: %w", err)
	}
	arguments := []string{"--kubeconfig=" + backend.kubeconfig, "--chart=" + request.ChartPackage,
		"--values=" + request.ReleaseValues, "--namespace=" + request.DriverNamespace, "--release=" + request.HelmRelease,
		"--admin=" + request.AdminBinary, "--workload-image=" + request.WorkloadImage,
		"--profile=" + plan.Profile,
		"--validator=" + validator,
		fmt.Sprintf("--max-filesystems=%d", backend.maxFileSystems),
		"--project-id=" + plan.ProjectID, "--region=" + plan.Region, "--run-id=" + plan.RunID,
		"--cluster-id=" + resourceID(inventory, e2ecleanup.ResourceKindCluster, 0),
		"--parent-a=" + resourceID(inventory, e2ecleanup.ResourceKindParent, 0), "--parent-b=" + resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
		"--evidence-dir=" + evidenceDirectory}
	if request.PreviousChart != "" {
		predecessor := request.Predecessor
		if predecessor == nil {
			return nil, fmt.Errorf("scenario arguments require the closed predecessor identity")
		}
		arguments = append(arguments,
			"--previous-chart="+request.PreviousChart,
			"--previous-values="+request.PreviousValues,
			"--predecessor-kind="+predecessor.Kind,
			"--predecessor-version="+predecessor.Version,
			"--predecessor-release-tag="+predecessor.ReleaseTag,
			"--predecessor-public-reference="+predecessor.PublicReference,
			"--predecessor-compatibility-identity="+predecessor.CompatibilityIdentity,
			"--predecessor-chart-sha256="+predecessor.ChartSHA256,
			"--predecessor-values-sha256="+predecessor.ValuesSHA256,
			"--predecessor-driver-image="+predecessor.DriverImage,
		)
	}
	return arguments, nil
}

func (backend *scalewayBackend) runScenarioPhase(ctx context.Context, evidenceDirectory, phase string, common []string) ([]e2erunner.ScenarioResult, error) {
	resultsFile := "scenario-results-" + phase + ".json"
	resultsPath := filepath.Join(evidenceDirectory, resultsFile)
	arguments := append([]string{phase}, common...)
	arguments = append(arguments, "--results="+resultsPath)
	if err := backend.runScenarioCommand(ctx, arguments...); err != nil {
		return nil, err
	}
	return loadRetainedScenarioResultsFile(evidenceDirectory, resultsFile, backend.plan.RunID)
}

// loadRetainedScenarioResultsFile revalidates an already completed scenario
// phase exactly as a newly executed phase. This is the only safe bridge from a
// retained full-run prefix into diagnostic mode: filenames remain basenames,
// every evidence digest is rehashed, JSON proofs are re-embedded, and semantic
// proof validation is repeated for the exact run ID.
func loadRetainedScenarioResultsFile(
	evidenceDirectory string,
	resultsFile string,
	runID string,
) ([]e2erunner.ScenarioResult, error) {
	if resultsFile == "" || filepath.Base(resultsFile) != resultsFile {
		return nil, fmt.Errorf("scenario results filename is not an exact basename")
	}
	resultsPath := filepath.Join(evidenceDirectory, resultsFile)
	if err := requireExactDiagnosticFile(resultsPath); err != nil {
		return nil, fmt.Errorf("scenario results %q: %w", resultsFile, err)
	}
	encoded, err := os.ReadFile(resultsPath)
	if err != nil {
		return nil, err
	}
	var results []e2erunner.ScenarioResult
	if err := strictjson.Decode(encoded, &results); err != nil {
		return nil, err
	}
	if err := hydrateRetainedScenarioResults(evidenceDirectory, results); err != nil {
		return nil, err
	}
	if err := e2erunner.ValidateAvailableScenarioProofsForRun(results, runID); err != nil {
		return nil, err
	}
	return results, nil
}

func hydrateRetainedScenarioResults(evidenceDirectory string, results []e2erunner.ScenarioResult) error {
	// Validate basenames before joining any backend-controlled output with the
	// retained evidence directory. This keeps a compromised scenario process
	// from turning evidence verification into an arbitrary file read.
	if err := e2erunner.ValidateScenarioSubset(results); err != nil {
		return err
	}
	for index := range results {
		result := &results[index]
		evidencePath := filepath.Join(evidenceDirectory, result.EvidenceFile)
		info, err := os.Lstat(evidencePath)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 16<<20 {
			return fmt.Errorf("scenario evidence %q must be an exact regular file of 1 to 16 MiB: %w", result.EvidenceFile, err)
		}
		digest, err := fileSHA256(evidencePath)
		if err != nil || digest != result.EvidenceSHA {
			return fmt.Errorf("scenario evidence %q digest mismatch: %w", result.EvidenceFile, err)
		}
		if filepath.Ext(result.EvidenceFile) == ".json" {
			proof, err := os.ReadFile(evidencePath)
			if err != nil {
				return fmt.Errorf("read scenario proof %q: %w", result.EvidenceFile, err)
			}
			result.Proof = bytes.TrimSpace(proof)
			result.ProofSHA256, err = e2erunner.CompactScenarioProofDigest(result.Proof)
			if err != nil {
				return fmt.Errorf("digest scenario proof %q: %w", result.EvidenceFile, err)
			}
		}
	}
	return nil
}

func (backend *scalewayBackend) Cleanup(ctx context.Context, request e2erunner.Request, inventory e2ecleanup.Inventory) (e2ecleanup.Inventory, error) {
	if _, err := validateLocalCandidateArtifacts(ctx, request, backend.plan); err != nil {
		return inventory, fmt.Errorf("revalidate exact candidate before cleanup recovery: %w", err)
	}
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
	if err := backend.recoverDisposableInstanceAttachments(ctx, request, inventory); err != nil {
		return inventory, err
	}
	evidenceDirectory := filepath.Dir(backend.plan.CleanupInventoryPath)
	preconditionsPath := filepath.Join(evidenceDirectory, "cleanup-preconditions.json")
	if _, err := os.Stat(backend.kubeconfig); err == nil {
		if err := backend.recoverInterruptedCheckpoint(ctx, request, backend.plan, inventory); err != nil {
			return inventory, err
		}
		if err := backend.recoverRetainedControllerFreeze(ctx, request, backend.plan, inventory); err != nil {
			return inventory, err
		}
		if err := backend.recoverInterruptedControllerFailure(ctx, request, backend.plan, inventory); err != nil {
			return inventory, err
		}
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
			"--admin="+request.AdminBinary, "--chart="+request.ChartPackage, "--values="+request.ReleaseValues,
			"--project-id="+backend.plan.ProjectID,
			"--profile="+backend.plan.Profile, "--region="+backend.plan.Region,
			fmt.Sprintf("--cluster-created-by-run=%t", backend.plan.Cluster.CreatedByRun),
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
		if inventory.Preconditions.BootstrapAbortComplete && !inventory.Preconditions.ParentAttachmentsAbsent {
			inventory, err = backend.recoverPlannedFreshBootstrapAttachments(ctx, request, inventory, evidenceDirectory)
			if err != nil {
				return inventory, err
			}
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

// recoverPlannedFreshBootstrapAttachments is a run-owned-cluster cleanup path,
// not a driver ownership shortcut. The in-cluster proof has already bound all
// surviving attachments to the durable pre-attach Lease plan. This method
// independently revalidates the exact run ledger and provider attachments,
// deletes only the run-created node pool, and waits for conclusive detachment
// before ordinary exact-ID parent cleanup can proceed.
func (backend *scalewayBackend) recoverPlannedFreshBootstrapAttachments(
	ctx context.Context,
	request e2erunner.Request,
	inventory e2ecleanup.Inventory,
	evidenceDirectory string,
) (e2ecleanup.Inventory, error) {
	proofPath := filepath.Join(evidenceDirectory, "bootstrap-abort-cleanup-"+backend.plan.RunID+".json")
	info, err := os.Lstat(proofPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 64<<10 {
		return inventory, fmt.Errorf("planned bootstrap-abort evidence is unavailable or invalid: %w", err)
	}
	encoded, err := os.ReadFile(proofPath)
	if err != nil {
		return inventory, err
	}
	var proof plannedBootstrapAbortEvidence
	if err := strictjson.Decode(encoded, &proof); err != nil {
		return inventory, fmt.Errorf("decode planned bootstrap-abort evidence: %w", err)
	}
	if err := backend.validatePlannedBootstrapAbortEvidence(request, inventory, proof); err != nil {
		return inventory, err
	}
	cluster, clusterFound := inventoryResourceByKind(inventory, e2ecleanup.ResourceKindCluster)
	if !clusterFound || !cluster.CreatedByRun {
		return inventory, fmt.Errorf("planned bootstrap attachment cleanup requires a run-created cluster")
	}
	nodePool, found := inventoryResourceByKind(inventory, e2ecleanup.ResourceKindNodePool)
	if !found || !nodePool.CreatedByRun {
		return inventory, fmt.Errorf("planned bootstrap attachment cleanup requires the exact run-owned node pool")
	}
	if err := backend.verifyPlannedBootstrapProviderAttachments(ctx, inventory, proof); err != nil {
		return inventory, err
	}
	switch nodePool.State {
	case e2ecleanup.ResourceStatePresent:
		if err := backend.deleteExact(ctx, nodePool, inventory); err != nil {
			return inventory, fmt.Errorf("delete exact run-owned node pool after planned bootstrap abort: %w", err)
		}
		for index := range inventory.Resources {
			if inventory.Resources[index].ID == nodePool.ID {
				inventory.Resources[index].State = e2ecleanup.ResourceStateAbsent
			}
		}
		// Persist conclusive node-pool absence before waiting for asynchronous
		// File Storage detach. If this process stops in that window, the next
		// cleanup run can resume without repeating or broadening the deletion.
		inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := backend.writeInventory(inventory); err != nil {
			return inventory, err
		}
	case e2ecleanup.ResourceStateAbsent:
		// A prior cleanup process may have deleted the exact node pool and
		// stopped before recording the final attachment barrier. The live
		// attachment subset was revalidated above, so waiting is safe.
	case e2ecleanup.ResourceStateUnknown:
		return inventory, fmt.Errorf("planned bootstrap node pool has unknown provider state")
	default:
		return inventory, fmt.Errorf("planned bootstrap node pool has invalid provider state %q", nodePool.State)
	}
	if err := backend.waitForRunParentAttachmentsAbsent(ctx, inventory); err != nil {
		return inventory, err
	}
	inventory.Preconditions.ParentAttachmentsAbsent = true
	inventory, err = backend.reconcileRunResources(ctx, inventory)
	if err != nil {
		return inventory, fmt.Errorf("reconcile run resources after planned bootstrap detachment: %w", err)
	}
	if err := backend.writeInventory(inventory); err != nil {
		return inventory, err
	}
	return inventory, nil
}

// validatePlannedBootstrapAbortEvidence is kept pure so the destructive
// cleanup boundary can be exhaustively tested without a provider mutation.
func (backend *scalewayBackend) validatePlannedBootstrapAbortEvidence(
	request e2erunner.Request,
	inventory e2ecleanup.Inventory,
	proof plannedBootstrapAbortEvidence,
) error {
	parents := []string{
		resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
		resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
	}
	proofParents := []string{proof.ParentA, proof.ParentB}
	slices.Sort(parents)
	slices.Sort(proofParents)
	if proof.SchemaVersion != "2" || proof.RunID != backend.plan.RunID || proof.Profile != backend.plan.Profile ||
		proof.Region != backend.plan.Region || proof.Namespace != request.DriverNamespace || proof.HelmRelease != request.HelmRelease ||
		!proof.ClusterCreatedByRun || proof.HelmStatus != "failed" || !proof.FreshBootstrapPlanVerified ||
		proof.PlannedControllerInstanceID == "" || proof.PlannedControllerZone != request.Zone ||
		proof.PlannedParentAttachments <= 0 || proof.PlannedParentAttachments > len(parents) ||
		proof.ParentAAttachments < 0 || proof.ParentAAttachments > 1 ||
		proof.ParentBAttachments < 0 || proof.ParentBAttachments > 1 ||
		proof.ParentAttachmentsAbsent || !proof.HelmUninstalled || !proof.NamespaceRemoved ||
		proof.ScenarioEntries != 0 || proof.InitialWorkloadPods != 0 || proof.InitialPVCs != 0 ||
		proof.WorkloadPods != 0 || proof.PVCs != 0 || proof.PVs != 0 || proof.VolumeAttachments != 0 ||
		proof.DriverCSINodeRegistrations != 0 || proof.DurableRecords != 0 ||
		proof.ParentAAttachments != proof.ParentAReportedAttachments ||
		proof.ParentBAttachments != proof.ParentBReportedAttachments ||
		proof.PlannedParentAttachments != proof.ParentAAttachments+proof.ParentBAttachments ||
		!slices.Equal(parents, proofParents) {
		return fmt.Errorf("planned bootstrap-abort evidence differs from the exact run scope")
	}
	return nil
}

func (backend *scalewayBackend) verifyPlannedBootstrapProviderAttachments(ctx context.Context, inventory e2ecleanup.Inventory, proof plannedBootstrapAbortEvidence) error {
	region := scw.Region(backend.plan.Region)
	total := 0
	expectedCounts := map[string]int{
		proof.ParentA: proof.ParentAAttachments,
		proof.ParentB: proof.ParentBAttachments,
	}
	for _, parentID := range []string{proof.ParentA, proof.ParentB} {
		resource, found := inventoryResource(inventory, parentID)
		if !found || resource.Kind != e2ecleanup.ResourceKindParent || !resource.CreatedByRun || resource.State != e2ecleanup.ResourceStatePresent {
			return fmt.Errorf("planned bootstrap parent %q is absent from the exact run ledger", parentID)
		}
		filesystem, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: parentID}, scw.WithContext(ctx))
		if err != nil || filesystem == nil || filesystem.ProjectID != backend.plan.ProjectID || filesystem.Name != resource.Name ||
			!slices.Contains(filesystem.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("planned bootstrap parent %q differs from the run-owned provider resource: %w", parentID, err)
		}
		attachments, err := backend.file.ListAttachments(
			&fileapi.ListAttachmentsRequest{Region: region, FilesystemID: &parentID},
			scw.WithAllPages(), scw.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("list planned bootstrap parent %q attachments: %w", parentID, err)
		}
		if attachments == nil {
			return fmt.Errorf("planned bootstrap parent %q returned no attachment inventory", parentID)
		}
		if err := validatePlannedParentAttachmentSnapshot(
			parentID, proof.PlannedControllerInstanceID, proof.PlannedControllerZone,
			expectedCounts[parentID], filesystem.NumberOfAttachments, attachments.Attachments,
		); err != nil {
			return err
		}
		total += len(attachments.Attachments)
	}
	if total > proof.PlannedParentAttachments {
		return fmt.Errorf("planned bootstrap attachment count increased from %d to %d", proof.PlannedParentAttachments, total)
	}
	return nil
}

// validatePlannedParentAttachmentSnapshot accepts only a monotonic subset of
// the exact attachments retained in the pre-delete proof. Deletions may detach
// the two parents at different times, including across cleanup-process restarts;
// an added, moved, duplicated, or foreign attachment always fails closed.
func validatePlannedParentAttachmentSnapshot(
	parentID, controllerInstanceID, controllerZone string,
	maximumCount int,
	reportedCount uint32,
	attachments []*fileapi.Attachment,
) error {
	if maximumCount < 0 || maximumCount > 1 || len(attachments) > maximumCount || uint32(len(attachments)) != reportedCount {
		return fmt.Errorf("planned bootstrap parent %q attachment inventories exceed or disagree with the retained proof", parentID)
	}
	for _, attachment := range attachments {
		if attachment == nil || attachment.Zone == nil || attachment.FilesystemID != parentID || attachment.ResourceID != controllerInstanceID ||
			attachment.Zone.String() != controllerZone || attachment.ResourceType != fileapi.AttachmentResourceTypeInstanceServer {
			return fmt.Errorf("planned bootstrap parent %q has a foreign or mismatched attachment", parentID)
		}
	}
	return nil
}

func (backend *scalewayBackend) waitForRunParentAttachmentsAbsent(ctx context.Context, inventory e2ecleanup.Inventory) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		allAbsent := true
		for _, parentID := range []string{
			resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
			resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
		} {
			filesystem, err := backend.file.GetFileSystem(
				&fileapi.GetFileSystemRequest{Region: scw.Region(backend.plan.Region), FilesystemID: parentID},
				scw.WithContext(ctx),
			)
			if err != nil {
				if !providerObservationRetryable(ctx, err) {
					return fmt.Errorf("read parent %q while waiting for bootstrap detachment: %w", parentID, err)
				}
				allAbsent = false
				continue
			}
			attachments, err := backend.file.ListAttachments(
				&fileapi.ListAttachmentsRequest{Region: scw.Region(backend.plan.Region), FilesystemID: &parentID},
				scw.WithAllPages(), scw.WithContext(ctx),
			)
			if err != nil {
				if !providerObservationRetryable(ctx, err) {
					return fmt.Errorf("list parent %q while waiting for bootstrap detachment: %w", parentID, err)
				}
				allAbsent = false
				continue
			}
			if filesystem == nil || attachments == nil || filesystem.NumberOfAttachments != 0 || len(attachments.Attachments) != 0 {
				allAbsent = false
			}
		}
		if allAbsent {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for planned bootstrap parent detachment: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func inventoryResourceByKind(inventory e2ecleanup.Inventory, kind string) (e2ecleanup.Resource, bool) {
	for _, resource := range inventory.Resources {
		if resource.Kind == kind {
			return resource, true
		}
	}
	return e2ecleanup.Resource{}, false
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
	case e2ecleanup.ResourceKindInstanceRootVolume:
		observed, err := backend.block.GetVolume(&blockapi.GetVolumeRequest{
			Zone: scw.Zone(backend.request.Zone), VolumeID: resource.ID,
		}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || observed == nil || observed.ProjectID != backend.plan.ProjectID ||
			observed.Zone.String() != backend.request.Zone || observed.Name != resource.Name ||
			!slices.Contains(observed.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("refuse deletion of mismatched disposable Instance root volume %s: %w", resource.ID, err)
		}
		available := blockapi.VolumeStatusAvailable
		observed, err = backend.block.WaitForVolumeAndReferences(&blockapi.WaitForVolumeAndReferencesRequest{
			Zone: scw.Zone(backend.request.Zone), VolumeID: resource.ID, VolumeTerminalStatus: &available,
		}, scw.WithContext(ctx))
		if err != nil {
			return fmt.Errorf("wait for disposable Instance root volume detach: %w", err)
		}
		if observed == nil || observed.ProjectID != backend.plan.ProjectID ||
			observed.Zone.String() != backend.request.Zone || observed.Name != resource.Name ||
			!slices.Contains(observed.Tags, backend.plan.OwnershipTag) ||
			observed.Status != blockapi.VolumeStatusAvailable || len(observed.References) != 0 {
			return fmt.Errorf("refuse deletion of attached or mismatched disposable Instance root volume %s", resource.ID)
		}
		if err := backend.block.DeleteVolume(&blockapi.DeleteVolumeRequest{
			Zone: scw.Zone(backend.request.Zone), VolumeID: resource.ID,
		}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
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
	case e2ecleanup.ResourceKindPrivateNetwork:
		observed, err := backend.vpc.GetPrivateNetwork(&vpcapi.GetPrivateNetworkRequest{Region: region, PrivateNetworkID: resource.ID}, scw.WithContext(ctx))
		if err != nil && providerNotFound(err) {
			return nil
		}
		if err != nil || observed == nil || observed.ProjectID != backend.plan.ProjectID || observed.Region.String() != backend.plan.Region || observed.Name != resource.Name || !slices.Contains(observed.Tags, backend.plan.OwnershipTag) {
			return fmt.Errorf("refuse deletion of mismatched Private Network %s: %w", resource.ID, err)
		}
		if err := backend.vpc.DeletePrivateNetwork(&vpcapi.DeletePrivateNetworkRequest{Region: region, PrivateNetworkID: resource.ID}, scw.WithContext(ctx)); err != nil && !providerNotFound(err) {
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
			if !providerObservationRetryable(ctx, err) {
				return err
			}
		} else if !present {
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
	case e2ecleanup.ResourceKindInstanceRootVolume:
		_, err = backend.block.GetVolume(&blockapi.GetVolumeRequest{Zone: scw.Zone(backend.request.Zone), VolumeID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindNodePool:
		_, err = backend.kubernetes.GetPool(&k8sapi.GetPoolRequest{Region: region, PoolID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindParent:
		_, err = backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindCluster:
		_, err = backend.kubernetes.GetCluster(&k8sapi.GetClusterRequest{Region: region, ClusterID: id}, scw.WithContext(ctx))
	case e2ecleanup.ResourceKindPrivateNetwork:
		_, err = backend.vpc.GetPrivateNetwork(&vpcapi.GetPrivateNetworkRequest{Region: region, PrivateNetworkID: id}, scw.WithContext(ctx))
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
	// The checked-in scenario tool immediately moves the two credentials into
	// unexported shell variables and scopes them to its exact scw calls. It also
	// needs their values once over stdin to create the controller-only Secret.
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

// environmentWithoutScalewayCredentials prevents general-purpose child tools
// from inheriting provider authority they do not need. It also removes any
// ambient kubeconfig so callers can append exactly one run-owned KUBECONFIG.
// The scenario shell has a narrower explicit boundary because it creates the
// controller Secret and runs the few provider CLI observations required by the
// real E2E contract.
func environmentWithoutScalewayCredentials() []string {
	environment := os.Environ()
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name == "SCW_ACCESS_KEY" || name == "SCW_SECRET_KEY" || name == "KUBECONFIG" {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
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

// completeDisposableInstanceCreate clears the single Instance Create intent
// only in the same durable generation that records both provider-created
// resources. DeleteServer does not delete its root Block Storage volume, so
// recording only the Instance would make cleanup non-resumable after a crash.
func (backend *scalewayBackend) completeDisposableInstanceCreate(
	inventory *e2ecleanup.Inventory,
	instanceResource e2ecleanup.Resource,
	rootVolumeResource e2ecleanup.Resource,
) error {
	if inventory.SchemaVersion != e2ecleanup.SchemaVersionV2 {
		return fmt.Errorf("disposable Instance root-volume journaling requires cleanup schema v2")
	}
	if inventory.PendingCreate == nil ||
		inventory.PendingCreate.Kind != e2ecleanup.ResourceKindInstance ||
		inventory.PendingCreate.Name != instanceResource.Name {
		return fmt.Errorf("provider Create result for %s %s differs from retained intent", instanceResource.Kind, instanceResource.Name)
	}
	if instanceResource.Kind != e2ecleanup.ResourceKindInstance ||
		rootVolumeResource.Kind != e2ecleanup.ResourceKindInstanceRootVolume ||
		instanceResource.ID == rootVolumeResource.ID {
		return fmt.Errorf("disposable Instance Create returned an invalid Instance/root-volume pair")
	}
	inventory.Resources = append(inventory.Resources, instanceResource, rootVolumeResource)
	inventory.PendingCreate = nil
	inventory.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := backend.writeInventory(*inventory); err != nil {
		return fmt.Errorf("persist provider Create result for disposable Instance and root volume: %w", err)
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

// providerObservationRetryable recognizes only availability failures that can
// safely be retried inside an already bounded read-only polling loop. Permanent
// authorization, validation, not-found, conflict, quota, and precondition
// failures must remain visible immediately; treating one of those as eventual
// consistency would hide a broken qualification or unsafe cleanup.
func providerObservationRetryable(ctx context.Context, err error) bool {
	if err == nil || (ctx != nil && ctx.Err() != nil) {
		return false
	}
	var transient *scw.TransientStateError
	var locked *scw.ResourceLockedError
	if errors.As(err, &transient) || errors.As(err, &locked) {
		return true
	}
	var response *scw.ResponseError
	if errors.As(err, &response) {
		return response.StatusCode == http.StatusRequestTimeout ||
			response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode >= http.StatusInternalServerError
	}
	var networkError net.Error
	return errors.As(err, &networkError)
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
		UninstallPrepareComplete: value, BootstrapAbortComplete: false, NodeDaemonSetStopped: value, NodeMountsAbsent: value,
		ControllerMountsAbsent: value, ParentAttachmentsAbsent: value, ControllerStopped: value, HelmUninstalled: value}
}
