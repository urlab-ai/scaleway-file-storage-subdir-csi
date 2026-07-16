package csisanity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"scaleway-sfs-subdir-csi/internal/clock"
	internaluuid "scaleway-sfs-subdir-csi/internal/uuid"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	sanityInstallationID = "33333333-3333-4333-8333-333333333333"
	sanityClusterUID     = "44444444-4444-4444-8444-444444444444"
	sanityParentID       = "22222222-2222-4222-8222-222222222222"
	// CSI sanity appends "/target" to its configured target root. The real
	// NodeService accepts kubelet's exact final component, "/mount". A narrow
	// test-only path adapter below translates only that upstream fixture name;
	// the production NodePathPolicy still validates the resulting exact shape.
	sanityTargetFixtureName = "target"
)

// sanityProductionCores instruments, but does not replace, the production
// controller state machines. Separate counters prove each CSI RPC reached its
// corresponding core; this avoids the previous misleading cross-wired counts.
type sanityProductionCores struct {
	create      *driver.CreateController
	delete      *driver.DeleteController
	publish     *driver.PublishController
	validate    *driver.CapabilityValidator
	creates     atomic.Uint64
	deletes     atomic.Uint64
	publishes   atomic.Uint64
	unpublishes atomic.Uint64
	validates   atomic.Uint64
	contextMu   sync.Mutex
	contexts    map[string]map[string]string
}

func (cores *sanityProductionCores) Create(ctx context.Context, request driver.CreateRequest) (driver.CreateResponse, error) {
	cores.creates.Add(1)
	response, err := cores.create.Create(ctx, request)
	if err == nil {
		cores.contextMu.Lock()
		cores.contexts[response.VolumeHandle] = cloneStrings(response.VolumeContext)
		cores.contextMu.Unlock()
	}
	return response, err
}

func (cores *sanityProductionCores) Delete(ctx context.Context, volumeID string) error {
	cores.deletes.Add(1)
	return cores.delete.Delete(ctx, volumeID)
}

func (cores *sanityProductionCores) Publish(ctx context.Context, request driver.PublishRequest) error {
	cores.publishes.Add(1)
	if len(request.VolumeContext) == 0 && request.NodeID == sanityNodeID {
		// csi-test v5.4 omits CreateVolume's context from several otherwise
		// valid ControllerPublish requests. Replay only the exact context
		// returned for this handle and only for the real NodeGetInfo identity.
		// Unknown-node probes remain untouched and therefore prove the product's
		// read-only NotFound-before-context behavior.
		cores.contextMu.Lock()
		request.VolumeContext = cloneStrings(cores.contexts[request.VolumeHandle])
		cores.contextMu.Unlock()
	}
	return cores.publish.Publish(ctx, request)
}

func (cores *sanityProductionCores) Unpublish(ctx context.Context, volumeID, nodeID string) error {
	cores.unpublishes.Add(1)
	return cores.publish.Unpublish(ctx, volumeID, nodeID)
}

func (cores *sanityProductionCores) Validate(ctx context.Context, request driver.ValidateCapabilitiesRequest) (driver.ValidateCapabilitiesResult, error) {
	cores.validates.Add(1)
	return cores.validate.Validate(ctx, request)
}

type sanityPlacer struct{}

func (sanityPlacer) PlaceAndReserve(_ context.Context, _ driver.CreateRequest, _ uint64, _ string, reserve driver.AllocationReservation) (k8s.StoredAllocation, error) {
	return reserve(driver.Placement{ParentFilesystemID: sanityParentID, BasePath: "/kubernetes-volumes"})
}

func (sanityPlacer) MarkPoolResolved(context.Context, string) error { return nil }

// sanityOwnershipState is the fake filesystem boundary shared by the real
// create, publish, validate, delete, and Node state machines. It stores the
// exact sealed ownership generations those cores produce; lifecycle rules are
// still checked by volume.ValidateOwnershipUpdate.
type sanityOwnershipState struct {
	mu      sync.Mutex
	records map[string]volume.OwnershipRecord
}

func newSanityOwnershipState() *sanityOwnershipState {
	return &sanityOwnershipState{records: make(map[string]volume.OwnershipRecord)}
}

func (state *sanityOwnershipState) load(logicalID string) (volume.OwnershipRecord, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	record, present := state.records[logicalID]
	if !present {
		return nil, driver.ErrOwnershipNotFound
	}
	return record, nil
}

type sanityCreationBackend struct{ state *sanityOwnershipState }

func (backend sanityCreationBackend) LoadOwnership(_ context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	return backend.state.load(allocation.LogicalVolumeID)
}

func (sanityCreationBackend) PrepareDirectory(context.Context, *volume.DetailedAllocationRecord) error {
	return nil
}

func (backend sanityCreationBackend) CreateOwnership(_ context.Context, ownership *volume.DetailedOwnershipRecord) error {
	backend.state.mu.Lock()
	defer backend.state.mu.Unlock()
	if _, present := backend.state.records[ownership.LogicalVolumeID]; present {
		return k8s.ErrAlreadyExists
	}
	backend.state.records[ownership.LogicalVolumeID] = ownership
	return nil
}

func (sanityCreationBackend) VerifyDirectory(context.Context, *volume.DetailedAllocationRecord) error {
	return nil
}

type sanityLifecycleOwnerships struct{ state *sanityOwnershipState }

func (store sanityLifecycleOwnerships) Load(_ context.Context, allocation *volume.DetailedAllocationRecord) (volume.OwnershipRecord, error) {
	return store.state.load(allocation.LogicalVolumeID)
}

func (store sanityLifecycleOwnerships) UpdateDetailed(_ context.Context, current, next *volume.DetailedOwnershipRecord) error {
	store.state.mu.Lock()
	defer store.state.mu.Unlock()
	if store.state.records[current.LogicalVolumeID] != current {
		return fmt.Errorf("stale sanity ownership generation")
	}
	if err := volume.ValidateOwnershipUpdate(current, next); err != nil {
		return err
	}
	store.state.records[current.LogicalVolumeID] = next
	return nil
}

func (store sanityLifecycleOwnerships) Compact(_ context.Context, current *volume.DetailedOwnershipRecord, next *volume.CompactDeletedOwnershipRecord) error {
	store.state.mu.Lock()
	defer store.state.mu.Unlock()
	if store.state.records[current.LogicalVolumeID] != current {
		return fmt.Errorf("stale sanity ownership generation")
	}
	if err := volume.ValidateOwnershipUpdate(current, next); err != nil {
		return err
	}
	store.state.records[current.LogicalVolumeID] = next
	return nil
}

type sanityPublishOwnerships struct{ state *sanityOwnershipState }

func (store sanityPublishOwnerships) LoadDetailed(_ context.Context, allocation *volume.DetailedAllocationRecord) (driver.StoredOwnership, error) {
	record, err := store.state.load(allocation.LogicalVolumeID)
	if err != nil {
		return driver.StoredOwnership{}, err
	}
	detailed, ok := record.(*volume.DetailedOwnershipRecord)
	if !ok {
		return driver.StoredOwnership{}, fmt.Errorf("sanity ownership is terminal")
	}
	return driver.StoredOwnership{Record: detailed}, nil
}

func (store sanityPublishOwnerships) UpdateDetailed(_ context.Context, current driver.StoredOwnership, next *volume.DetailedOwnershipRecord) (driver.StoredOwnership, error) {
	store.state.mu.Lock()
	defer store.state.mu.Unlock()
	if current.Record == nil || store.state.records[current.Record.LogicalVolumeID] != current.Record {
		return driver.StoredOwnership{}, fmt.Errorf("stale sanity publish ownership generation")
	}
	if err := volume.ValidateOwnershipUpdate(current.Record, next); err != nil {
		return driver.StoredOwnership{}, err
	}
	store.state.records[next.LogicalVolumeID] = next
	return driver.StoredOwnership{Record: next}, nil
}

type sanityMissingDeleteResolver struct{}

func (sanityMissingDeleteResolver) ResolveMissing(context.Context, volume.Handle) (driver.MissingDeleteResolution, error) {
	return driver.MissingDeleteResolution{ConclusiveAbsence: true, AbsenceReason: "CSI sanity generated an unknown volume"}, nil
}

type sanityAttachmentChecker struct{}

func (sanityAttachmentChecker) HasAttachment(context.Context, string) (bool, error) {
	return false, nil
}

type sanityDeleteFilesystem struct{}

func (sanityDeleteFilesystem) PrepareDisposition(context.Context, *volume.DetailedAllocationRecord) error {
	return nil
}

func (sanityDeleteFilesystem) RemoveQuarantine(context.Context, *volume.DetailedAllocationRecord) error {
	return nil
}

type sanityAttachmentPublisher struct{}

func (sanityAttachmentPublisher) EnsureAttached(_ context.Context, _ *volume.DetailedAllocationRecord, nodeID string) error {
	if nodeID != sanityNodeID {
		return k8s.ErrNotFound
	}
	return nil
}

type sanityNodeExistenceReader struct{}

func (sanityNodeExistenceReader) NodeExists(_ context.Context, nodeID string) (bool, error) {
	return nodeID == sanityNodeID, nil
}

type sanityFenceVerifier struct{}

func (sanityFenceVerifier) SafeToClear(context.Context, string, string) error { return nil }

type sanityNodeAuthorizer struct {
	state *sanityOwnershipState
}

func (authorizer sanityNodeAuthorizer) ValidateParentContext(immutable volume.ImmutableContext) error {
	if err := immutable.Validate(); err != nil {
		return err
	}
	if immutable.InstallationID != sanityInstallationID || immutable.ParentFilesystemID != sanityParentID || immutable.PoolName != "standard" || immutable.BasePath != "/kubernetes-volumes" {
		return fmt.Errorf("sanity volume context differs from configured parent")
	}
	return nil
}

func (authorizer sanityNodeAuthorizer) AuthorizeStage(_ context.Context, handle volume.Handle, immutable volume.ImmutableContext, capability volume.Capability, nodeID, _ string) (*volume.DetailedOwnershipRecord, *os.File, error) {
	ownership, err := authorizer.authorize(handle, immutable, nodeID)
	if err != nil {
		return nil, nil, err
	}
	normalized, err := volume.NormalizeCapability(capability)
	if err != nil {
		return nil, nil, err
	}
	if normalized.AccessType != ownership.NormalizedCreateParameters.AccessType || normalized.FilesystemType != ownership.NormalizedCreateParameters.FilesystemType || !containsAccessMode(ownership.NormalizedCreateParameters.AccessModes, normalized.AccessMode) {
		return nil, nil, fmt.Errorf("sanity Node capability differs from ownership")
	}
	return ownership, nil, nil
}

func (authorizer sanityNodeAuthorizer) AuthorizePublish(_ context.Context, handle volume.Handle, immutable volume.ImmutableContext, nodeID, _ string) (*volume.DetailedOwnershipRecord, error) {
	return authorizer.authorize(handle, immutable, nodeID)
}

func (authorizer sanityNodeAuthorizer) authorize(handle volume.Handle, immutable volume.ImmutableContext, nodeID string) (*volume.DetailedOwnershipRecord, error) {
	if err := authorizer.ValidateParentContext(immutable); err != nil {
		return nil, err
	}
	record, err := authorizer.state.load(handle.LogicalVolumeID)
	if err != nil {
		return nil, err
	}
	ownership, ok := record.(*volume.DetailedOwnershipRecord)
	if !ok || ownership.State != volume.StateReady {
		return nil, driver.ErrVolumeNotReady
	}
	if ownership.MappingHash != handle.MappingHash || ownership.DirectoryName != immutable.DirectoryName || !containsString(ownership.PublishedNodeIDs, nodeID) {
		return nil, driver.ErrNodePublicationFenceMissing
	}
	return ownership, nil
}

func (authorizer sanityNodeAuthorizer) ResolveCleanup(_ context.Context, handle volume.Handle, _ string, parentFilesystemID, backingRelativePath string) (*volume.DetailedOwnershipRecord, error) {
	record, err := authorizer.state.load(handle.LogicalVolumeID)
	if err != nil {
		return nil, err
	}
	ownership, ok := record.(*volume.DetailedOwnershipRecord)
	if !ok || ownership.MappingHash != handle.MappingHash || ownership.ParentFilesystemID != parentFilesystemID || path.Join(ownership.BasePath, ownership.DirectoryName) != backingRelativePath {
		return nil, fmt.Errorf("sanity cleanup mapping differs from ownership")
	}
	return ownership, nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsAccessMode(values []volume.AccessMode, wanted volume.AccessMode) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

type sanityNodeTargets struct {
	mu      sync.Mutex
	targets map[string]struct{}
}

func (*sanityNodeTargets) ValidateStaging(context.Context, string) (*os.File, error) {
	return nil, nil
}

func (targets *sanityNodeTargets) EnsurePublishTarget(_ context.Context, target string) (*os.File, bool, error) {
	targets.mu.Lock()
	defer targets.mu.Unlock()
	_, present := targets.targets[target]
	targets.targets[target] = struct{}{}
	// The production core receives the kubelet-exact "/mount" path. The
	// upstream sanity suite checks its generic sibling "/target" on the host,
	// so the fake path boundary mirrors only that directory for the suite's
	// existence assertion; mount graph authority remains the fake mounter.
	if err := os.MkdirAll(sanityFixtureTarget(target), 0o755); err != nil {
		return nil, false, err
	}
	return nil, !present, nil
}

func (targets *sanityNodeTargets) RemovePublishTargetIfEmpty(_ context.Context, target string, _ *mount.TargetIdentity) error {
	targets.mu.Lock()
	defer targets.mu.Unlock()
	delete(targets.targets, target)
	if err := os.Remove(sanityFixtureTarget(target)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func sanityFixtureTarget(target string) string {
	if strings.HasSuffix(target, "/mount") {
		return strings.TrimSuffix(target, "/mount") + "/" + sanityTargetFixtureName
	}
	return target
}

type sanityProductionNode struct {
	service        *driver.NodeService
	stageCalls     atomic.Uint64
	unstageCalls   atomic.Uint64
	publishCalls   atomic.Uint64
	unpublishCalls atomic.Uint64
}

func (node *sanityProductionNode) GetInfo() driver.NodeInfo { return node.service.GetInfo() }

func (node *sanityProductionNode) Stage(ctx context.Context, handle string, values map[string]string, staging string, capability volume.Capability) error {
	node.stageCalls.Add(1)
	return node.service.Stage(ctx, handle, values, staging, capability)
}

func (node *sanityProductionNode) Unstage(ctx context.Context, handle, staging string) error {
	node.unstageCalls.Add(1)
	return node.service.Unstage(ctx, handle, staging)
}

func (node *sanityProductionNode) Publish(ctx context.Context, handle string, values map[string]string, staging, target string, capability volume.Capability, readOnly bool) error {
	node.publishCalls.Add(1)
	return node.service.Publish(ctx, handle, values, staging, translateSanityTarget(target), capability, readOnly)
}

func (node *sanityProductionNode) Unpublish(ctx context.Context, handle, target string) error {
	node.unpublishCalls.Add(1)
	return node.service.Unpublish(ctx, handle, translateSanityTarget(target))
}

func translateSanityTarget(target string) string {
	suffix := "/" + sanityTargetFixtureName
	if !strings.HasSuffix(target, suffix) {
		return target
	}
	return strings.TrimSuffix(target, suffix) + "/mount"
}

func newSanityProductionHarness(kubeletRoot, parentRoot string) (*sanityProductionCores, *sanityProductionNode, error) {
	client := k8s.NewFakeConfigMapClient()
	allocations, err := k8s.NewAllocationStore(client, "scaleway-sfs-subdir-csi", sanityDriverName, sanityInstallationID)
	if err != nil {
		return nil, nil, err
	}
	reservationJournals, err := k8s.NewReservationJournalStore(client, "scaleway-sfs-subdir-csi", sanityDriverName, sanityInstallationID)
	if err != nil {
		return nil, nil, err
	}
	if err := reservationJournals.BootstrapFresh(context.Background(), []string{"standard"}, sanityClusterUID); err != nil {
		return nil, nil, err
	}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		return nil, nil, err
	}
	locks := coordination.NewKeyedLock()
	ownershipState := newSanityOwnershipState()
	creation, err := driver.NewCreationReconciler(sanityCreationBackend{state: ownershipState})
	if err != nil {
		return nil, nil, err
	}
	create, err := driver.NewCreateController(sanityDriverName, sanityInstallationID, sanityClusterUID, allocations, reservationJournals, sanityPlacer{}, creation, gate, locks, clock.Real{})
	if err != nil {
		return nil, nil, err
	}
	lifecycle := sanityLifecycleOwnerships{state: ownershipState}
	deleteCore, err := driver.NewDeleteController(sanityDriverName, sanityInstallationID, sanityClusterUID, allocations, lifecycle, sanityMissingDeleteResolver{}, sanityAttachmentChecker{}, sanityDeleteFilesystem{}, internaluuid.Random{}, gate, locks, clock.Real{})
	if err != nil {
		return nil, nil, err
	}
	publish, err := driver.NewPublishController(sanityDriverName, sanityInstallationID, sanityClusterUID, allocations, sanityPublishOwnerships{state: ownershipState}, sanityAttachmentPublisher{}, sanityNodeExistenceReader{}, sanityFenceVerifier{}, gate, locks, clock.Real{})
	if err != nil {
		return nil, nil, err
	}
	validate, err := driver.NewCapabilityValidator(allocations, lifecycle)
	if err != nil {
		return nil, nil, err
	}
	controller := &sanityProductionCores{
		create: create, delete: deleteCore, publish: publish, validate: validate,
		contexts: make(map[string]map[string]string),
	}

	paths, err := driver.NewNodePathPolicy(sanityDriverName, kubeletRoot, parentRoot)
	if err != nil {
		return nil, nil, err
	}
	targets := &sanityNodeTargets{targets: make(map[string]struct{})}
	nodeGate, err := coordination.NewMutationGate(10)
	if err != nil {
		return nil, nil, err
	}
	nodeService, err := driver.NewNodeService(sanityNodeID, paths, sanityNodeAuthorizer{state: ownershipState}, targets, mount.NewFake(), nodeGate, coordination.NewKeyedLock(), coordination.NewKeyedLock())
	if err != nil {
		return nil, nil, err
	}
	return controller, &sanityProductionNode{service: nodeService}, nil
}

func cloneStrings(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

var (
	_ driver.CreationBackend         = sanityCreationBackend{}
	_ driver.LifecycleOwnershipStore = sanityLifecycleOwnerships{}
	_ driver.OwnershipStateStore     = sanityPublishOwnerships{}
	_ driver.NodeExistenceReader     = sanityNodeExistenceReader{}
	_ driver.NodeAuthorizer          = sanityNodeAuthorizer{}
	_ driver.NodeTargetManager       = (*sanityNodeTargets)(nil)
)
