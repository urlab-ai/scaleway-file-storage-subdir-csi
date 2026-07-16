package e2ecleanup

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/e2eplan"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	// SchemaVersionV1 is the closed cleanup inventory and review-plan schema.
	SchemaVersionV1 = "1"

	// ResourceKindCluster identifies one exact Kapsule cluster.
	ResourceKindCluster = "kapsule-cluster"
	// ResourceKindNodePool identifies the run-created Kapsule node pool. A
	// fresh pool is mandatory even when the cluster is reused.
	ResourceKindNodePool = "kapsule-node-pool"
	// ResourceKindParent identifies one run-created File Storage parent.
	ResourceKindParent = "file-storage-parent"
	// ResourceKindInstance identifies the one standalone run-created Instance
	// reused serially by the release-candidate recovery scenarios.
	ResourceKindInstance = "disposable-instance"

	// ResourceStatePresent means a fresh exact-ID lookup found the resource.
	ResourceStatePresent = "present"
	// ResourceStateAbsent means a fresh exact-ID lookup conclusively proved
	// absence. Forbidden, timed-out, partial, or failed reads are not absence.
	ResourceStateAbsent = "absent"
	// ResourceStateUnknown preserves an inconclusive lookup as a blocker.
	ResourceStateUnknown = "unknown"
	// PhaseProvisioning permits a crash-durable partial exact-ID ledger while
	// the fixed resource set is still being created.
	PhaseProvisioning = "provisioning"
	// PhaseReady means the complete planned resource set exists for scenarios.
	PhaseReady = "ready"
	// PhaseCleanup means scenario execution ended and cleanup is active.
	PhaseCleanup = "cleanup"
	// PhaseComplete retains the exact created prefix with every run-owned ID
	// absent. A successful qualification applies the stricter complete-profile
	// requirement at the evidence boundary.
	PhaseComplete = "complete"

	maximumTags = 64
)

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$`)

// MaximumObservationAge bounds how long an inventory can be used as cleanup
// review evidence. An execution backend must still repeat live reads after the
// immediate user approval and before every mutation.
const MaximumObservationAge = 10 * time.Minute

// MaximumFutureSkew tolerates only a small clock offset in retained evidence.
const MaximumFutureSkew = time.Minute

// Inventory is the complete retained input for a cleanup review. CreatedByRun
// is the creation ledger fact; tags, project, region, name, and state are the
// corresponding exact resource observation.
type Inventory struct {
	SchemaVersion  string        `json:"schemaVersion"`
	Phase          string        `json:"phase"`
	Profile        string        `json:"profile"`
	RunID          string        `json:"runId"`
	ProjectID      string        `json:"projectId"`
	Region         string        `json:"region"`
	ResourcePrefix string        `json:"resourcePrefix"`
	OwnershipTag   string        `json:"ownershipTag"`
	ObservedAt     string        `json:"observedAt"`
	Preconditions  Preconditions `json:"preconditions"`
	PendingCreate  *CreateIntent `json:"pendingCreate,omitempty"`
	Resources      []Resource    `json:"resources"`
}

// CreateIntent records the one sequential provider Create that may have been
// emitted without a conclusive response. It prevents a temporarily empty list
// from being treated as proof that the resource does not exist.
type CreateIntent struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// Preconditions are the ordered Kubernetes, mount, and provider cleanup
// barriers required before billable resources may be deleted. These booleans
// are retained evidence, not authority; a future execution backend must
// revalidate every condition against live state.
type Preconditions struct {
	WorkloadPodsRemoved         bool `json:"workloadPodsRemoved"`
	PVCsRemoved                 bool `json:"pvcsRemoved"`
	PVsRemoved                  bool `json:"pvsRemoved"`
	VolumeAttachmentsRemoved    bool `json:"volumeAttachmentsRemoved"`
	UnpublishAndUnstageComplete bool `json:"unpublishAndUnstageComplete"`
	PublishedNodeFencesCleared  bool `json:"publishedNodeFencesCleared"`
	UninstallPrepareComplete    bool `json:"uninstallPrepareComplete"`
	NodeDaemonSetStopped        bool `json:"nodeDaemonSetStopped"`
	NodeMountsAbsent            bool `json:"nodeMountsAbsent"`
	ControllerMountsAbsent      bool `json:"controllerMountsAbsent"`
	ParentAttachmentsAbsent     bool `json:"parentAttachmentsAbsent"`
	ControllerStopped           bool `json:"controllerStopped"`
	HelmUninstalled             bool `json:"helmUninstalled"`
}

// Resource is one exact-ID creation record and current observation. Every
// entry is retained, including a reused cluster and conclusively absent
// resources, so cleanup cannot silently broaden or forget its inventory.
type Resource struct {
	Kind         string   `json:"kind"`
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	ProjectID    string   `json:"projectId"`
	Region       string   `json:"region"`
	Tags         []string `json:"tags"`
	CreatedByRun bool     `json:"createdByRun"`
	State        string   `json:"state"`
}

// Plan is canonical review evidence. It can identify exact candidates, but it
// can never authorize or perform a mutation.
type Plan struct {
	SchemaVersion             string              `json:"schemaVersion"`
	DryRun                    bool                `json:"dryRun"`
	MutationAuthorized        bool                `json:"mutationAuthorized"`
	ExecutionBackendAvailable bool                `json:"executionBackendAvailable"`
	RequiresImmediateApproval bool                `json:"requiresImmediateApproval"`
	ReadyForImmediateApproval bool                `json:"readyForImmediateApproval"`
	CleanupComplete           bool                `json:"cleanupComplete"`
	RunID                     string              `json:"runId"`
	ProjectID                 string              `json:"projectId"`
	Region                    string              `json:"region"`
	ResourcePrefix            string              `json:"resourcePrefix"`
	OwnershipTag              string              `json:"ownershipTag"`
	ObservedAt                string              `json:"observedAt"`
	Blockers                  []string            `json:"blockers"`
	DeleteActions             []DeleteAction      `json:"deleteActions"`
	AlreadyAbsent             []ResourceReference `json:"alreadyAbsent"`
	RetainedResources         []ResourceReference `json:"retainedResources"`
	SurvivingRunResources     []ResourceReference `json:"survivingRunResources"`
}

// DeleteAction is one exact, ordered candidate operation. It contains neither
// a selector nor mutation authority.
type DeleteAction struct {
	Order     uint32 `json:"order"`
	Operation string `json:"operation"`
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	Name      string `json:"name"`
}

// ResourceReference is bounded audit output for a retained, absent, or
// surviving resource.
type ResourceReference struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// Build validates the complete closed inventory and derives a fail-closed
// cleanup review at now. Unknown or stale observations and incomplete
// uninstall barriers produce blockers and no delete actions.
func Build(inventory Inventory, now time.Time) (Plan, error) {
	observation, err := inventory.validateStatic()
	if err != nil {
		return Plan{}, err
	}
	if now.IsZero() {
		return Plan{}, fmt.Errorf("cleanup review time must be non-zero")
	}
	now = now.UTC()

	resources := slices.Clone(inventory.Resources)
	slices.SortFunc(resources, compareResources)
	blockers := preconditionBlockers(inventory.Preconditions)
	if inventory.PendingCreate != nil {
		blockers = append(blockers, fmt.Sprintf("provider Create for %s %s remains unresolved", inventory.PendingCreate.Kind, inventory.PendingCreate.Name))
	}
	if observation.After(now.Add(MaximumFutureSkew)) {
		blockers = append(blockers, "inventory observation is too far in the future")
	}
	if observation.Before(now.Add(-MaximumObservationAge)) {
		blockers = append(blockers, "inventory observation is stale")
	}

	alreadyAbsent := make([]ResourceReference, 0, len(resources))
	retained := make([]ResourceReference, 0, 1)
	surviving := make([]ResourceReference, 0, len(resources))
	candidates := make([]Resource, 0, len(resources))
	for _, resource := range resources {
		reference := resource.reference()
		if !resource.CreatedByRun {
			retained = append(retained, reference)
			continue
		}
		switch resource.State {
		case ResourceStateAbsent:
			alreadyAbsent = append(alreadyAbsent, reference)
		case ResourceStatePresent:
			surviving = append(surviving, reference)
			candidates = append(candidates, resource)
		case ResourceStateUnknown:
			surviving = append(surviving, reference)
			blockers = append(blockers, fmt.Sprintf("%s %s has unknown provider state", resource.Kind, resource.ID))
		}
	}

	blockers = sortedUnique(blockers)
	actions := make([]DeleteAction, 0, len(candidates))
	if len(blockers) == 0 {
		slices.SortFunc(candidates, compareDeletionOrder)
		for index, resource := range candidates {
			actions = append(actions, DeleteAction{
				Order: uint32(index + 1), Operation: "delete-exact-id",
				Kind: resource.Kind, ID: resource.ID, Name: resource.Name,
			})
		}
	}
	cleanupComplete := len(blockers) == 0 && len(candidates) == 0
	return Plan{
		SchemaVersion: SchemaVersionV1, DryRun: true, MutationAuthorized: false,
		ExecutionBackendAvailable: false, RequiresImmediateApproval: true,
		ReadyForImmediateApproval: len(blockers) == 0 && len(actions) > 0,
		CleanupComplete:           cleanupComplete,
		RunID:                     inventory.RunID, ProjectID: inventory.ProjectID, Region: inventory.Region,
		ResourcePrefix: inventory.ResourcePrefix, OwnershipTag: inventory.OwnershipTag,
		ObservedAt: inventory.ObservedAt, Blockers: blockers, DeleteActions: actions,
		AlreadyAbsent: alreadyAbsent, RetainedResources: retained, SurvivingRunResources: surviving,
	}, nil
}

func (inventory Inventory) validateStatic() (time.Time, error) {
	if inventory.SchemaVersion != SchemaVersionV1 {
		return time.Time{}, fmt.Errorf("E2E cleanup schema %q is unsupported", inventory.SchemaVersion)
	}
	if inventory.Phase != PhaseProvisioning && inventory.Phase != PhaseReady && inventory.Phase != PhaseCleanup && inventory.Phase != PhaseComplete {
		return time.Time{}, fmt.Errorf("E2E cleanup phase %q is unsupported", inventory.Phase)
	}
	if inventory.Profile != e2eplan.ProfileBase && inventory.Profile != e2eplan.ProfileReleaseCandidate {
		return time.Time{}, fmt.Errorf("E2E cleanup profile %q is unsupported", inventory.Profile)
	}
	if err := volume.ValidateOperationID(inventory.RunID); err != nil {
		return time.Time{}, fmt.Errorf("E2E cleanup run ID: %w", err)
	}
	if err := volume.ValidateInstallationID(inventory.ProjectID); err != nil {
		return time.Time{}, fmt.Errorf("E2E cleanup Project ID: %w", err)
	}
	if inventory.Region != "fr-par" {
		return time.Time{}, fmt.Errorf("v1 real E2E cleanup region must be fr-par")
	}
	if len(inventory.ResourcePrefix) > 63 || !dnsLabelPattern.MatchString(inventory.ResourcePrefix) || !strings.Contains(inventory.ResourcePrefix, inventory.RunID) {
		return time.Time{}, fmt.Errorf("resource prefix must be a DNS label of at most 63 bytes containing the complete run ID")
	}
	wantTag := "sfs-subdir-e2e-run=" + inventory.RunID
	if inventory.OwnershipTag != wantTag {
		return time.Time{}, fmt.Errorf("ownership tag must equal %q", wantTag)
	}
	observation, err := time.Parse(time.RFC3339Nano, inventory.ObservedAt)
	if err != nil || observation.Location() != time.UTC || observation.Format(time.RFC3339Nano) != inventory.ObservedAt {
		return time.Time{}, fmt.Errorf("observedAt must be a canonical UTC RFC3339 timestamp")
	}
	wantResources := 4
	if inventory.Profile == e2eplan.ProfileReleaseCandidate {
		wantResources = 5
	}
	// A controlled failure may stop provisioning after any durable prefix of the
	// planned resources. Cleanup and its final audit must remain able to carry
	// that exact partial creation ledger; requiring phantom, never-created IDs
	// would make standalone cleanup permanently non-resumable.
	partialLedgerAllowed := inventory.Phase == PhaseProvisioning || inventory.Phase == PhaseCleanup || inventory.Phase == PhaseComplete
	if len(inventory.Resources) > wantResources || (!partialLedgerAllowed && len(inventory.Resources) != wantResources) {
		return time.Time{}, fmt.Errorf("cleanup inventory has %d resources; profile %q requires %d", len(inventory.Resources), inventory.Profile, wantResources)
	}

	counts := make(map[string]int, 3)
	seenIDs := make(map[string]struct{}, len(inventory.Resources))
	for index, resource := range inventory.Resources {
		if err := resource.validate(inventory, seenIDs); err != nil {
			return time.Time{}, fmt.Errorf("resource %d: %w", index, err)
		}
		counts[resource.Kind]++
	}
	wantKinds := 3
	wantInstances := 0
	if inventory.Profile == e2eplan.ProfileReleaseCandidate {
		wantKinds = 4
		wantInstances = 1
	}
	completeKinds := counts[ResourceKindCluster] == 1 && counts[ResourceKindNodePool] == 1 && counts[ResourceKindParent] == 2 && counts[ResourceKindInstance] == wantInstances && len(counts) == wantKinds
	withinPartialBounds := counts[ResourceKindCluster] <= 1 && counts[ResourceKindNodePool] <= 1 && counts[ResourceKindParent] <= 2 && counts[ResourceKindInstance] <= wantInstances && len(counts) <= wantKinds
	if (partialLedgerAllowed && !withinPartialBounds) || (!partialLedgerAllowed && !completeKinds) {
		return time.Time{}, fmt.Errorf("cleanup inventory resource classes do not match profile %q", inventory.Profile)
	}
	if inventory.PendingCreate != nil {
		if inventory.Phase != PhaseProvisioning && inventory.Phase != PhaseCleanup {
			return time.Time{}, fmt.Errorf("pending provider Create is invalid in phase %q", inventory.Phase)
		}
		if err := inventory.PendingCreate.validate(inventory); err != nil {
			return time.Time{}, fmt.Errorf("pending provider Create: %w", err)
		}
		for _, resource := range inventory.Resources {
			if resource.Kind == inventory.PendingCreate.Kind && resource.Name == inventory.PendingCreate.Name {
				return time.Time{}, fmt.Errorf("pending provider Create already has retained exact resource %q", resource.ID)
			}
		}
	}
	return observation, nil
}

func (intent CreateIntent) validate(inventory Inventory) error {
	wantName := ""
	switch intent.Kind {
	case ResourceKindCluster:
		if !inventoryClusterCreatedByRun(inventory) {
			return fmt.Errorf("reused cluster cannot have a provider Create intent")
		}
		wantName = inventory.ResourcePrefix
	case ResourceKindNodePool:
		wantName = inventory.ResourcePrefix + "-nodes"
	case ResourceKindParent:
		if intent.Name != inventory.ResourcePrefix+"-parent-a" && intent.Name != inventory.ResourcePrefix+"-parent-b" {
			return fmt.Errorf("parent name %q is outside the fixed plan", intent.Name)
		}
		return nil
	case ResourceKindInstance:
		if inventory.Profile != e2eplan.ProfileReleaseCandidate {
			return fmt.Errorf("disposable Instance is absent from profile %q", inventory.Profile)
		}
		wantName = inventory.ResourcePrefix + "-recovery"
	default:
		return fmt.Errorf("resource kind %q is unsupported", intent.Kind)
	}
	if intent.Name != wantName {
		return fmt.Errorf("resource name %q differs from fixed name %q", intent.Name, wantName)
	}
	return nil
}

func inventoryClusterCreatedByRun(inventory Inventory) bool {
	for _, resource := range inventory.Resources {
		if resource.Kind == ResourceKindCluster {
			return resource.CreatedByRun
		}
	}
	// Before the first cluster Create there is no retained resource yet. The
	// fixed name and ownership tag still bind the intent to this run.
	return len(inventory.Resources) == 0
}

func (resource Resource) validate(inventory Inventory, seenIDs map[string]struct{}) error {
	switch resource.Kind {
	case ResourceKindCluster, ResourceKindNodePool, ResourceKindParent, ResourceKindInstance:
	default:
		return fmt.Errorf("resource kind %q is unsupported", resource.Kind)
	}
	if err := volume.ValidateInstallationID(resource.ID); err != nil {
		return fmt.Errorf("exact resource ID: %w", err)
	}
	if _, duplicate := seenIDs[resource.ID]; duplicate {
		return fmt.Errorf("exact resource ID %q is duplicated", resource.ID)
	}
	seenIDs[resource.ID] = struct{}{}
	if !boundedText(resource.Name, 255) {
		return fmt.Errorf("resource name must be single-line UTF-8 containing 1 to 255 bytes")
	}
	if resource.ProjectID != inventory.ProjectID || resource.Region != inventory.Region {
		return fmt.Errorf("resource %s scope differs from the cleanup scope", resource.ID)
	}
	if resource.State != ResourceStatePresent && resource.State != ResourceStateAbsent && resource.State != ResourceStateUnknown {
		return fmt.Errorf("resource state %q is unsupported", resource.State)
	}
	if len(resource.Tags) > maximumTags {
		return fmt.Errorf("resource has more than %d tags", maximumTags)
	}
	seenTags := make(map[string]struct{}, len(resource.Tags))
	for _, tag := range resource.Tags {
		if !boundedText(tag, 128) {
			return fmt.Errorf("resource tag must be single-line UTF-8 containing 1 to 128 bytes")
		}
		if _, duplicate := seenTags[tag]; duplicate {
			return fmt.Errorf("resource tag %q is duplicated", tag)
		}
		seenTags[tag] = struct{}{}
	}
	if !resource.CreatedByRun {
		if resource.Kind != ResourceKindCluster {
			return fmt.Errorf("only the explicitly reused cluster may be pre-existing")
		}
		return nil
	}
	if resource.Name != inventory.ResourcePrefix && !strings.HasPrefix(resource.Name, inventory.ResourcePrefix+"-") {
		return fmt.Errorf("run-owned resource name %q is outside prefix %q", resource.Name, inventory.ResourcePrefix)
	}
	if _, tagged := seenTags[inventory.OwnershipTag]; !tagged {
		return fmt.Errorf("run-owned resource %s lacks the exact ownership tag", resource.ID)
	}
	return nil
}

func preconditionBlockers(preconditions Preconditions) []string {
	tests := []struct {
		complete bool
		message  string
	}{
		{preconditions.WorkloadPodsRemoved, "run-owned workload Pods are not proved removed"},
		{preconditions.PVCsRemoved, "run-owned PVCs are not proved removed"},
		{preconditions.PVsRemoved, "test PVs are not proved removed"},
		{preconditions.VolumeAttachmentsRemoved, "VolumeAttachments are not proved removed"},
		{preconditions.UnpublishAndUnstageComplete, "normal unpublish and unstage are not proved complete"},
		{preconditions.PublishedNodeFencesCleared, "published-node fences are not proved cleared"},
		{preconditions.UninstallPrepareComplete, "csi-admin uninstall prepare is not proved complete"},
		{preconditions.NodeDaemonSetStopped, "node DaemonSet is not proved stopped"},
		{preconditions.NodeMountsAbsent, "node mounts are not proved absent"},
		{preconditions.ControllerMountsAbsent, "controller mounts are not proved absent"},
		{preconditions.ParentAttachmentsAbsent, "parent attachments are not proved absent"},
		{preconditions.ControllerStopped, "controller is not proved stopped"},
		{preconditions.HelmUninstalled, "Helm release is not proved uninstalled after safe prepare"},
	}
	blockers := make([]string, 0, len(tests))
	for _, test := range tests {
		if !test.complete {
			blockers = append(blockers, test.message)
		}
	}
	return blockers
}

func compareResources(left, right Resource) int {
	if comparison := strings.Compare(left.Kind, right.Kind); comparison != 0 {
		return comparison
	}
	return strings.Compare(left.ID, right.ID)
}

func compareDeletionOrder(left, right Resource) int {
	leftRank, rightRank := deletionRank(left.Kind), deletionRank(right.Kind)
	if leftRank != rightRank {
		return leftRank - rightRank
	}
	return strings.Compare(left.ID, right.ID)
}

func deletionRank(kind string) int {
	switch kind {
	case ResourceKindInstance:
		return 1
	case ResourceKindNodePool:
		return 2
	case ResourceKindParent:
		return 3
	case ResourceKindCluster:
		return 4
	default:
		return 5
	}
}

func sortedUnique(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}

func (resource Resource) reference() ResourceReference {
	return ResourceReference{Kind: resource.Kind, ID: resource.ID, Name: resource.Name, State: resource.State}
}

func boundedText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsAny(value, "\x00\r\n")
}

// ValidateInventoryPath enforces the absolute, exact path form emitted by the
// preflight planner. It does not inspect the filesystem object at that path.
func ValidateInventoryPath(path string) error {
	if path == "" || path == string(filepath.Separator) || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
		return fmt.Errorf("inventory path must be clean, absolute, and non-root")
	}
	return nil
}

// Encode returns canonical JSON suitable for retained cleanup evidence.
func Encode(plan Plan) ([]byte, error) {
	if plan.SchemaVersion != SchemaVersionV1 || !plan.DryRun || plan.MutationAuthorized || plan.ExecutionBackendAvailable || !plan.RequiresImmediateApproval {
		return nil, fmt.Errorf("E2E cleanup plan is not a non-authorizing v1 dry-run")
	}
	if plan.ReadyForImmediateApproval && (len(plan.Blockers) != 0 || len(plan.DeleteActions) == 0 || plan.CleanupComplete) {
		return nil, fmt.Errorf("E2E cleanup approval readiness is inconsistent")
	}
	if len(plan.Blockers) != 0 && len(plan.DeleteActions) != 0 {
		return nil, fmt.Errorf("blocked E2E cleanup plan contains delete actions")
	}
	return canonicaljson.Marshal(plan)
}
