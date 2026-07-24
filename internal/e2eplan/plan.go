package e2eplan

import (
	"fmt"
	"math/big"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	releasecompat "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/compatibility"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	// SchemaVersionV1 is the closed preflight-plan schema.
	SchemaVersionV1 = "1"
	// ProfileBase covers the normal real-Kapsule scenario without destructive
	// recovery fault injection.
	ProfileBase = "base"
	// ProfileReleaseCandidate adds the disposable destructive recovery profile.
	ProfileReleaseCandidate = "release-candidate"
	// ClusterCreate creates a run-owned ephemeral Kapsule cluster.
	ClusterCreate = "create"
	// ClusterReuse uses one explicitly identified pre-existing test cluster.
	ClusterReuse                = "reuse"
	fileStorageMinimumSizeBytes = uint64(25_000_000_000)
	fileStorageMaximumSizeBytes = uint64(50_000_000_000_000)
	fileStorageGrowthStepBytes  = uint64(100_000_000_000)
)

var (
	dnsLabelPattern       = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)
	decimalCostPattern    = regexp.MustCompile(`^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,6})?$`)
	lowerHexDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	commitPattern         = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	imageReferencePattern = regexp.MustCompile(`^[^[:space:]@]+@sha256:[0-9a-f]{64}$`)
)

var requiredImages = []string{
	"csi-node-driver-registrar",
	"driver",
	"external-attacher",
	"external-provisioner",
	"livenessprobe",
}

// Request is the complete human-supplied input required to produce a plan.
// It deliberately contains no credential values.
type Request struct {
	SchemaVersion          string          `json:"schemaVersion"`
	Profile                string          `json:"profile"`
	RunID                  string          `json:"runId"`
	ProjectID              string          `json:"projectId"`
	Region                 string          `json:"region"`
	ResourcePrefix         string          `json:"resourcePrefix"`
	EvidenceDirectory      string          `json:"evidenceDirectory"`
	Cluster                ClusterRequest  `json:"cluster"`
	NodePool               NodePoolRequest `json:"nodePool"`
	Parents                ParentRequest   `json:"parents"`
	EstimatedHourlyCostEUR string          `json:"estimatedHourlyCostEur"`
	CostSource             string          `json:"costSource"`
	ProviderReview         ProviderReview  `json:"providerReview"`
	Artifacts              Artifacts       `json:"artifacts"`
}

// ClusterRequest states whether the cluster is run-owned or explicitly reused.
type ClusterRequest struct {
	Disposition string `json:"disposition"`
	ExistingID  string `json:"existingId,omitempty"`
}

// NodePoolRequest fixes the minimal node count and candidate commercial type.
type NodePoolRequest struct {
	Count          uint32 `json:"count"`
	CommercialType string `json:"commercialType"`
}

// ParentRequest fixes the two run-owned File Storage resources required by the
// real-provider scenarios. Base smoke uses the product minimum; release
// qualification reserves one supported growth step.
type ParentRequest struct {
	Count     uint32 `json:"count"`
	SizeBytes uint64 `json:"sizeBytes"`
}

// ProviderReview is the explicit fresh operator evidence for provider facts
// that the pinned Scaleway APIs do not expose as a stable machine-readable
// preflight. Live region, File Storage, and attachment-capability reads remain
// mandatory in addition to this attestation.
type ProviderReview struct {
	ObservedAt          string `json:"observedAt"`
	ProductStatus       string `json:"productStatus"`
	ProductStatusSource string `json:"productStatusSource"`
	// PublicBetaAccepted is retained in the v1 request schema for strict decode
	// compatibility. File Storage is GA and new requests must set it to false.
	PublicBetaAccepted        bool   `json:"publicBetaAccepted"`
	FileStorageQuotaRemaining uint64 `json:"fileStorageQuotaRemaining"`
	QuotaSource               string `json:"quotaSource"`
}

// Artifacts binds the plan to one exact source and rendered artifact set.
type Artifacts struct {
	GitCommit       string        `json:"gitCommit"`
	CandidateDigest string        `json:"candidateDigest"`
	ChartDigest     string        `json:"chartDigest"`
	Images          []ImageDigest `json:"images"`
}

// ImageDigest names one required driver or sidecar immutable reference.
type ImageDigest struct {
	Name      string `json:"name"`
	Reference string `json:"reference"`
}

// Plan is immutable review evidence. MutationAuthorized is always false: a
// separate immediate user approval is required immediately before execution.
type Plan struct {
	SchemaVersion                        string         `json:"schemaVersion"`
	DryRun                               bool           `json:"dryRun"`
	MutationAuthorized                   bool           `json:"mutationAuthorized"`
	RequiresImmediateApproval            bool           `json:"requiresImmediateApproval"`
	RunID                                string         `json:"runId"`
	ProjectID                            string         `json:"projectId"`
	Region                               string         `json:"region"`
	ResourcePrefix                       string         `json:"resourcePrefix"`
	OwnershipTag                         string         `json:"ownershipTag"`
	Profile                              string         `json:"profile"`
	Cluster                              ClusterPlan    `json:"cluster"`
	NodePool                             NodePoolPlan   `json:"nodePool"`
	DisposableInstance                   *InstancePlan  `json:"disposableInstance,omitempty"`
	Parents                              ParentRequest  `json:"parents"`
	EstimatedHourlyCostEUR               string         `json:"estimatedHourlyCostEur"`
	CostSource                           string         `json:"costSource"`
	ProviderReview                       ProviderReview `json:"providerReview"`
	Artifacts                            Artifacts      `json:"artifacts"`
	PlannedResources                     []ResourcePlan `json:"plannedResources"`
	DestructiveOperations                []string       `json:"destructiveOperations"`
	CleanupInventoryPath                 string         `json:"cleanupInventoryPath"`
	CleanupCommand                       []string       `json:"cleanupCommand"`
	LiveProductAndQuotaPreflightRequired bool           `json:"liveProductAndQuotaPreflightRequired"`
}

// ClusterPlan records ownership so cleanup can never delete a reused cluster.
type ClusterPlan struct {
	Disposition     string `json:"disposition"`
	ExistingID      string `json:"existingId,omitempty"`
	CreatedByRun    bool   `json:"createdByRun"`
	DeleteOnCleanup bool   `json:"deleteOnCleanup"`
}

// NodePoolPlan records that the E2E node pool is always newly created for the
// run, including when the enclosing test cluster is reused. This prevents the
// suite from mutating or retaining a pre-existing pool.
type NodePoolPlan struct {
	Count           uint32 `json:"count"`
	CommercialType  string `json:"commercialType"`
	CreatedByRun    bool   `json:"createdByRun"`
	DeleteOnCleanup bool   `json:"deleteOnCleanup"`
}

// InstancePlan records the one standalone release-candidate Instance. It uses
// the same explicitly selected, release-qualified commercial type as the node
// pool and is reused serially to keep resource count and cost bounded.
type InstancePlan struct {
	Count           uint32 `json:"count"`
	CommercialType  string `json:"commercialType"`
	CreatedByRun    bool   `json:"createdByRun"`
	DeleteOnCleanup bool   `json:"deleteOnCleanup"`
}

// ResourcePlan is one bounded planned provider resource class included in the
// explicit scope, cost review, and cleanup contract.
type ResourcePlan struct {
	Kind            string `json:"kind"`
	Count           uint32 `json:"count"`
	CreatedByRun    bool   `json:"createdByRun"`
	DeleteOnCleanup bool   `json:"deleteOnCleanup"`
}

// Build validates a request and derives the non-authorizing review plan.
func Build(request Request) (Plan, error) {
	if err := request.Validate(); err != nil {
		return Plan{}, err
	}
	images := slices.Clone(request.Artifacts.Images)
	slices.SortFunc(images, func(left, right ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	request.Artifacts.Images = images
	clusterOwned := request.Cluster.Disposition == ClusterCreate
	inventoryPath := filepath.Join(request.EvidenceDirectory, "scaleway-e2e-inventory-"+request.RunID+".json")
	operations := []string{
		"force-delete one run-owned driver controller Pod and wait for replacement",
		"delete the exact run-owned node pool during verified cleanup",
		"detach exact run-owned parent IDs during verified cleanup",
		"delete exact run-owned parent IDs after verified uninstall",
	}
	if clusterOwned {
		operations = append(operations,
			"delete the exact run-owned ephemeral cluster during cleanup",
			"delete the exact run-owned Private Network after cluster deletion",
		)
	}
	if request.Profile == ProfileReleaseCandidate {
		operations = append(operations,
			"delete the exact run-owned disposable Instance root volume after deleting its Instance",
			"resize one run-owned parent filesystem",
			"attach and detach the two run-owned parents on the standalone run-owned disposable Instance",
			"drain and uncordon one node from the exact run-owned Kapsule node pool",
			"hard-stop the exact run-owned Kapsule node hosting the controller for abnormal-takeover fencing",
			"replace that exact stopped run-owned Kapsule node during compatibility revalidation",
			"delete and recreate the dedicated run-owned driver namespace for checkpoint recovery",
			"scale the exact run-owned Kapsule node pool to zero and restore its planned size for checkpoint fencing",
			"decommission, detach, and remove the second run-owned parent from the driver configuration",
			"stop or delete only disposable run-owned Instances for recovery fencing",
			"add the fresh second parent and restart the controller after its ownership claim is complete",
		)
	}
	plannedResources := make([]ResourcePlan, 0, 7)
	if clusterOwned {
		plannedResources = append(plannedResources, ResourcePlan{
			Kind: "private-network", Count: 1, CreatedByRun: true, DeleteOnCleanup: true,
		})
	}
	plannedResources = append(plannedResources,
		ResourcePlan{Kind: "kapsule-cluster", Count: 1, CreatedByRun: clusterOwned, DeleteOnCleanup: clusterOwned},
		ResourcePlan{Kind: "kapsule-node-pool", Count: 1, CreatedByRun: true, DeleteOnCleanup: true},
		ResourcePlan{Kind: "kapsule-node", Count: request.NodePool.Count, CreatedByRun: true, DeleteOnCleanup: true},
		ResourcePlan{Kind: "file-storage-parent", Count: request.Parents.Count, CreatedByRun: true, DeleteOnCleanup: true},
	)
	if request.Profile == ProfileReleaseCandidate {
		// One standalone Instance is reused serially across the destructive
		// recovery scenarios. Its provider-created root Block Storage volume is
		// a separate billable resource and therefore has its own exact cleanup
		// ledger entry. Both costs are part of the explicit aggregate cost.
		plannedResources = append(plannedResources,
			ResourcePlan{Kind: "disposable-instance", Count: 1, CreatedByRun: true, DeleteOnCleanup: true},
			ResourcePlan{Kind: "disposable-instance-root-volume", Count: 1, CreatedByRun: true, DeleteOnCleanup: true},
		)
	}
	var disposableInstance *InstancePlan
	if request.Profile == ProfileReleaseCandidate {
		disposableInstance = &InstancePlan{
			Count: 1, CommercialType: request.NodePool.CommercialType,
			CreatedByRun: true, DeleteOnCleanup: true,
		}
	}
	return Plan{
		SchemaVersion: SchemaVersionV1, DryRun: true, MutationAuthorized: false,
		RequiresImmediateApproval: true,
		RunID:                     request.RunID, ProjectID: request.ProjectID, Region: request.Region,
		ResourcePrefix: request.ResourcePrefix,
		OwnershipTag:   "sfs-subdir-e2e-run=" + request.RunID,
		Profile:        request.Profile,
		Cluster: ClusterPlan{
			Disposition: request.Cluster.Disposition, ExistingID: request.Cluster.ExistingID,
			CreatedByRun: clusterOwned, DeleteOnCleanup: clusterOwned,
		},
		NodePool: NodePoolPlan{
			Count: request.NodePool.Count, CommercialType: request.NodePool.CommercialType,
			CreatedByRun: true, DeleteOnCleanup: true,
		},
		DisposableInstance:     disposableInstance,
		Parents:                request.Parents,
		EstimatedHourlyCostEUR: request.EstimatedHourlyCostEUR, CostSource: request.CostSource,
		ProviderReview:        request.ProviderReview,
		Artifacts:             request.Artifacts,
		PlannedResources:      plannedResources,
		DestructiveOperations: operations,
		CleanupInventoryPath:  inventoryPath,
		CleanupCommand: []string{
			"go", "run", "./hack/scaleway-e2e-cleanup", "--inventory=" + inventoryPath, "--dry-run",
		},
		LiveProductAndQuotaPreflightRequired: true,
	}, nil
}

// Validate enforces the closed, cost-explicit, run-owned planning contract.
func (request Request) Validate() error {
	if request.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("E2E plan schema %q is unsupported", request.SchemaVersion)
	}
	if request.Profile != ProfileBase && request.Profile != ProfileReleaseCandidate {
		return fmt.Errorf("E2E profile %q is unsupported", request.Profile)
	}
	if err := volume.ValidateOperationID(request.RunID); err != nil {
		return fmt.Errorf("E2E run ID: %w", err)
	}
	if err := volume.ValidateInstallationID(request.ProjectID); err != nil {
		return fmt.Errorf("E2E Project ID: %w", err)
	}
	if request.Region != "fr-par" {
		return fmt.Errorf("v1 real E2E region must be fr-par")
	}
	if len(request.ResourcePrefix) > 63 || !dnsLabelPattern.MatchString(request.ResourcePrefix) || !strings.Contains(request.ResourcePrefix, request.RunID) {
		return fmt.Errorf("resource prefix must be a DNS label of at most 63 bytes containing the complete run ID")
	}
	if request.EvidenceDirectory == "" || request.EvidenceDirectory == string(filepath.Separator) || !filepath.IsAbs(request.EvidenceDirectory) || filepath.Clean(request.EvidenceDirectory) != request.EvidenceDirectory || strings.ContainsAny(request.EvidenceDirectory, "\x00\r\n") {
		return fmt.Errorf("evidence directory must be a clean absolute non-root path")
	}
	if request.Cluster.Disposition != ClusterCreate && request.Cluster.Disposition != ClusterReuse {
		return fmt.Errorf("cluster disposition must be create or reuse")
	}
	if request.Cluster.Disposition == ClusterCreate && request.Cluster.ExistingID != "" {
		return fmt.Errorf("a run-created cluster must not claim an existing ID")
	}
	if request.Cluster.Disposition == ClusterReuse {
		if err := volume.ValidateInstallationID(request.Cluster.ExistingID); err != nil {
			return fmt.Errorf("reused cluster exact ID: %w", err)
		}
	}
	if request.Profile == ProfileBase && request.Cluster.Disposition != ClusterCreate {
		return fmt.Errorf("base smoke requires a run-owned ephemeral cluster")
	}
	if request.Profile == ProfileBase && request.NodePool.Count != 2 {
		return fmt.Errorf("base smoke requires exactly two fresh nodes")
	}
	if request.Profile == ProfileReleaseCandidate && (request.NodePool.Count < 2 || request.NodePool.Count > 3) {
		return fmt.Errorf("release qualification node count must be 2 or 3")
	}
	if err := releasecompat.ValidateCommercialTypes([]string{request.NodePool.CommercialType}); err != nil {
		return fmt.Errorf("node commercial type: %w", err)
	}
	if request.Parents.Count != 2 {
		return fmt.Errorf("real E2E requires exactly two run-owned parents")
	}
	if request.Profile == ProfileBase && request.Parents.SizeBytes != fileStorageMinimumSizeBytes {
		return fmt.Errorf("base smoke requires two product-minimum 25 GB parents")
	}
	if request.Profile == ProfileReleaseCandidate && (request.Parents.SizeBytes < fileStorageGrowthStepBytes || request.Parents.SizeBytes > fileStorageMaximumSizeBytes-fileStorageGrowthStepBytes || request.Parents.SizeBytes%fileStorageGrowthStepBytes != 0) {
		return fmt.Errorf("release qualification requires two parents from 100 GB to 49.9 TB in 100 GB increments, leaving one growth step")
	}
	if err := validateCost(request.EstimatedHourlyCostEUR); err != nil {
		return err
	}
	if !boundedText(request.CostSource, 512) {
		return fmt.Errorf("cost source must be single-line UTF-8 containing 1 to 512 bytes")
	}
	if err := request.ProviderReview.validate(request.Parents.Count); err != nil {
		return err
	}
	if !commitPattern.MatchString(request.Artifacts.GitCommit) {
		return fmt.Errorf("artifact Git commit must be complete lowercase 40- or 64-hex")
	}
	if !lowerHexDigestPattern.MatchString(request.Artifacts.ChartDigest) {
		return fmt.Errorf("chart digest must be a lowercase sha256 digest")
	}
	if !lowerHexDigestPattern.MatchString(request.Artifacts.CandidateDigest) {
		return fmt.Errorf("candidate-manifest digest must be a lowercase sha256 digest")
	}
	if err := validateImages(request.Artifacts.Images); err != nil {
		return err
	}
	return nil
}

func (review ProviderReview) validate(requiredParents uint32) error {
	observed, err := time.Parse(time.RFC3339Nano, review.ObservedAt)
	if err != nil || observed.Location() != time.UTC || observed.Format(time.RFC3339Nano) != review.ObservedAt {
		return fmt.Errorf("provider review observedAt must be a canonical UTC RFC3339 timestamp")
	}
	if review.ProductStatus != "ga" {
		return fmt.Errorf("provider review product status must be ga")
	}
	if review.PublicBetaAccepted {
		return fmt.Errorf("publicBetaAccepted must be false for the GA File Storage offer")
	}
	if !boundedText(review.ProductStatusSource, 512) || !boundedText(review.QuotaSource, 512) {
		return fmt.Errorf("provider review sources must be single-line UTF-8 containing 1 to 512 bytes")
	}
	if review.FileStorageQuotaRemaining < uint64(requiredParents) {
		return fmt.Errorf("provider review File Storage quota cannot create the planned parents")
	}
	return nil
}

func validateCost(value string) error {
	if !decimalCostPattern.MatchString(value) {
		return fmt.Errorf("estimated hourly EUR cost must be a canonical non-exponent decimal with at most six fractional digits")
	}
	parsed, ok := new(big.Rat).SetString(value)
	if !ok || parsed.Sign() <= 0 {
		return fmt.Errorf("estimated hourly EUR cost must be positive")
	}
	return nil
}

func validateImages(images []ImageDigest) error {
	if len(images) != len(requiredImages) {
		return fmt.Errorf("artifact plan must contain exactly %d required images", len(requiredImages))
	}
	names := make([]string, 0, len(images))
	seen := make(map[string]struct{}, len(images))
	for _, image := range images {
		if _, duplicate := seen[image.Name]; duplicate {
			return fmt.Errorf("artifact image name %q is duplicated", image.Name)
		}
		seen[image.Name] = struct{}{}
		names = append(names, image.Name)
		if !imageReferencePattern.MatchString(image.Reference) || strings.ContainsAny(image.Reference, "\x00\r\n") {
			return fmt.Errorf("artifact image %q must use repository@sha256:<digest>", image.Name)
		}
	}
	slices.Sort(names)
	if !slices.Equal(names, requiredImages) {
		return fmt.Errorf("artifact image names differ from the closed required set")
	}
	return nil
}

func boundedText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

// Encode returns canonical JSON suitable for retained preflight evidence.
func Encode(plan Plan) ([]byte, error) {
	if plan.SchemaVersion != SchemaVersionV1 || !plan.DryRun || plan.MutationAuthorized || !plan.RequiresImmediateApproval {
		return nil, fmt.Errorf("E2E plan is not a non-authorizing v1 dry-run")
	}
	return canonicaljson.Marshal(plan)
}
