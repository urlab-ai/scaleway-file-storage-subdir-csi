package driver

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type loggedAllocationStore struct {
	delegate   *k8s.AllocationStore
	operations *[]string
	failUpdate error
}

func (store *loggedAllocationStore) Get(ctx context.Context, logicalID string) (k8s.StoredAllocation, error) {
	return store.delegate.Get(ctx, logicalID)
}

func (store *loggedAllocationStore) Create(ctx context.Context, record volume.AllocationRecord) (k8s.StoredAllocation, error) {
	return store.delegate.Create(ctx, record)
}

func (store *loggedAllocationStore) Update(ctx context.Context, current k8s.StoredAllocation, next volume.AllocationRecord) (k8s.StoredAllocation, error) {
	*store.operations = append(*store.operations, "allocation")
	if store.failUpdate != nil {
		err := store.failUpdate
		store.failUpdate = nil
		return k8s.StoredAllocation{}, err
	}
	return store.delegate.Update(ctx, current, next)
}

type fakeOwnershipStateStore struct {
	current    *volume.DetailedOwnershipRecord
	operations *[]string
	failUpdate error
}

func (store *fakeOwnershipStateStore) LoadDetailed(_ context.Context, _ *volume.DetailedAllocationRecord) (StoredOwnership, error) {
	if store.current == nil {
		return StoredOwnership{}, ErrOwnershipNotFound
	}
	return StoredOwnership{Record: store.current}, nil
}

func (store *fakeOwnershipStateStore) UpdateDetailed(_ context.Context, current StoredOwnership, next *volume.DetailedOwnershipRecord) (StoredOwnership, error) {
	*store.operations = append(*store.operations, "ownership")
	if store.failUpdate != nil {
		err := store.failUpdate
		store.failUpdate = nil
		return StoredOwnership{}, err
	}
	if current.Record != store.current {
		return StoredOwnership{}, errors.New("stale ownership generation")
	}
	if err := volume.ValidateOwnershipUpdate(current.Record, next); err != nil {
		return StoredOwnership{}, err
	}
	store.current = next
	return StoredOwnership{Record: next}, nil
}

type fakeAttachmentPublisher struct {
	operations *[]string
	calls      []string
	err        error
}

type fakeNodeExistenceReader struct {
	known bool
	err   error
	calls []string
}

func (reader *fakeNodeExistenceReader) NodeExists(_ context.Context, nodeID string) (bool, error) {
	reader.calls = append(reader.calls, nodeID)
	return reader.known, reader.err
}

func (publisher *fakeAttachmentPublisher) EnsureAttached(_ context.Context, _ *volume.DetailedAllocationRecord, nodeID string) error {
	*publisher.operations = append(*publisher.operations, "attach")
	publisher.calls = append(publisher.calls, nodeID)
	return publisher.err
}

type fakeFenceVerifier struct {
	operations *[]string
	calls      []string
	errors     map[string]error
}

func (verifier *fakeFenceVerifier) SafeToClear(_ context.Context, nodeID, _ string) error {
	*verifier.operations = append(*verifier.operations, "fence")
	verifier.calls = append(verifier.calls, nodeID)
	return verifier.errors[nodeID]
}

type publishHarness struct {
	controller  *PublishController
	allocations *loggedAllocationStore
	ownerships  *fakeOwnershipStateStore
	attachments *fakeAttachmentPublisher
	nodes       *fakeNodeExistenceReader
	fences      *fakeFenceVerifier
	response    CreateResponse
	allocation  *volume.DetailedAllocationRecord
	operations  []string
}

func newPublishHarness(t *testing.T, createRequest CreateRequest) *publishHarness {
	t.Helper()
	create := newCreateHarness(t)
	response, err := create.controller.Create(context.Background(), createRequest)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	logicalID, err := volume.LogicalVolumeID(driverTestName, createRequest.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	stored, err := create.store.Get(context.Background(), logicalID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	ownership, err := ownershipFromCreatingAllocation(allocation)
	if err != nil {
		t.Fatalf("ownershipFromCreatingAllocation() error = %v", err)
	}
	harness := &publishHarness{response: response, allocation: allocation}
	harness.allocations = &loggedAllocationStore{delegate: create.store, operations: &harness.operations}
	harness.ownerships = &fakeOwnershipStateStore{current: ownership, operations: &harness.operations}
	harness.attachments = &fakeAttachmentPublisher{operations: &harness.operations}
	harness.nodes = &fakeNodeExistenceReader{known: true}
	harness.fences = &fakeFenceVerifier{operations: &harness.operations, errors: make(map[string]error)}
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	harness.controller, err = NewPublishController(
		driverTestName,
		driverTestInstallationID,
		driverTestClusterUID,
		harness.allocations,
		harness.ownerships,
		harness.attachments,
		harness.nodes,
		harness.fences,
		gate,
		coordination.NewKeyedLock(),
		clock.NewManual(time.Date(2026, 7, 13, 12, 1, 0, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewPublishController() error = %v", err)
	}
	return harness
}

func publishRequest(harness *publishHarness, nodeID string, mode volume.AccessMode) PublishRequest {
	return PublishRequest{
		VolumeHandle:  harness.response.VolumeHandle,
		NodeID:        nodeID,
		VolumeContext: harness.response.VolumeContext,
		Capability: volume.Capability{
			AccessMode:     mode,
			AccessType:     "mount",
			FilesystemType: "virtiofs",
			MountFlags:     []string{},
		},
	}
}

func TestPublishAttachesThenWritesAllocationThenOwnership(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	wantOperations := []string{"attach", "allocation", "ownership"}
	if !slices.Equal(harness.operations, wantOperations) {
		t.Fatalf("operations = %#v, want %#v", harness.operations, wantOperations)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	allocation := stored.Record.(*volume.DetailedAllocationRecord)
	if !slices.Equal(allocation.PublishedNodeIDs, []string{nodeID}) || !slices.Equal(harness.ownerships.current.PublishedNodeIDs, []string{nodeID}) {
		t.Fatalf("published fences allocation=%#v ownership=%#v", allocation.PublishedNodeIDs, harness.ownerships.current.PublishedNodeIDs)
	}
}

func TestPublishRejectsCapabilityThatDiffersFromDurableAllocation(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	request := publishRequest(harness, "fr-par-1/44444444-4444-4444-8444-444444444444", volume.AccessModeSingleNodeWriter)
	if err := harness.controller.Publish(context.Background(), request); !errors.Is(err, ErrCapabilityMismatch) {
		t.Fatalf("Publish(capability mismatch) error = %v, want ErrCapabilityMismatch", err)
	}
	if len(harness.operations) != 0 || len(harness.attachments.calls) != 0 {
		t.Fatalf("capability mismatch caused side effects: operations=%#v attaches=%#v", harness.operations, harness.attachments.calls)
	}
}

func TestPublishContextlessUnknownNodeReturnsNotFoundWithoutSideEffects(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	harness.nodes.known = false
	request := publishRequest(harness, "fr-par-1/44444444-4444-4444-8444-444444444444", volume.AccessModeMultiNodeMultiWriter)
	request.VolumeContext = nil
	err := harness.controller.Publish(context.Background(), request)
	if !errors.Is(err, k8s.ErrNotFound) {
		t.Fatalf("Publish(context-less unknown node) error = %v, want NotFound", err)
	}
	if len(harness.nodes.calls) != 1 || len(harness.operations) != 0 || len(harness.attachments.calls) != 0 {
		t.Fatalf("unknown-node proof calls/side effects = %#v / %#v / %#v", harness.nodes.calls, harness.operations, harness.attachments.calls)
	}
}

func TestPublishContextlessKnownNodeStillRequiresImmutableContext(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	request := publishRequest(harness, "fr-par-1/44444444-4444-4444-8444-444444444444", volume.AccessModeMultiNodeMultiWriter)
	request.VolumeContext = nil
	err := harness.controller.Publish(context.Background(), request)
	if !errors.Is(err, volume.ErrInvalidContext) {
		t.Fatalf("Publish(context-less known node) error = %v, want invalid context", err)
	}
	if len(harness.nodes.calls) != 1 || len(harness.operations) != 0 || len(harness.attachments.calls) != 0 {
		t.Fatalf("known-node missing-context calls/side effects = %#v / %#v / %#v", harness.nodes.calls, harness.operations, harness.attachments.calls)
	}
}

func TestPublishCrashBetweenDualWritesRestoresUnionBeforeSuccess(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"
	harness.ownerships.failUpdate = errors.New("injected ownership write failure")
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err == nil {
		t.Fatal("Publish(first) error = nil")
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	if !slices.Contains(stored.Record.(*volume.DetailedAllocationRecord).PublishedNodeIDs, nodeID) || slices.Contains(harness.ownerships.current.PublishedNodeIDs, nodeID) {
		t.Fatal("first crash did not leave conservative one-sided allocation fence")
	}
	harness.operations = nil
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err != nil {
		t.Fatalf("Publish(retry) error = %v", err)
	}
	if !slices.Equal(harness.ownerships.current.PublishedNodeIDs, []string{nodeID}) {
		t.Fatalf("retry ownership fences = %#v", harness.ownerships.current.PublishedNodeIDs)
	}
	if !slices.Equal(harness.operations, []string{"ownership", "attach"}) {
		t.Fatalf("retry operations = %#v, want conservative union repair then attach", harness.operations)
	}
}

func TestReconcilePublishedFencesRestoresUnionWithoutProviderAttachment(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"
	harness.ownerships.failUpdate = errors.New("injected ownership write failure")
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err == nil {
		t.Fatal("Publish(first) error = nil")
	}
	harness.operations = nil
	if err := harness.controller.ReconcilePublishedFences(context.Background(), harness.allocation.LogicalVolumeID); err != nil {
		t.Fatalf("ReconcilePublishedFences() error = %v", err)
	}
	if !slices.Equal(harness.operations, []string{"ownership"}) || len(harness.attachments.calls) != 1 {
		t.Fatalf("fence repair operations/attachment calls = %#v/%d", harness.operations, len(harness.attachments.calls))
	}
	if !slices.Equal(harness.ownerships.current.PublishedNodeIDs, []string{nodeID}) {
		t.Fatalf("repaired ownership fences = %#v", harness.ownerships.current.PublishedNodeIDs)
	}
}

func TestUnpublishVerifiesThenWritesOwnershipThenAllocation(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	harness.operations = nil
	if err := harness.controller.Unpublish(context.Background(), harness.response.VolumeHandle, nodeID); err != nil {
		t.Fatalf("Unpublish() error = %v", err)
	}
	if !slices.Equal(harness.operations, []string{"fence", "ownership", "allocation"}) {
		t.Fatalf("unpublish operations = %#v", harness.operations)
	}
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	if len(stored.Record.(*volume.DetailedAllocationRecord).PublishedNodeIDs) != 0 || len(harness.ownerships.current.PublishedNodeIDs) != 0 {
		t.Fatal("unpublish left a durable fence")
	}
}

func TestUnpublishCrashRestoresUnionAndReverifiesBeforeRemoval(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	harness.allocations.failUpdate = errors.New("injected allocation write failure")
	if err := harness.controller.Unpublish(context.Background(), harness.response.VolumeHandle, nodeID); err == nil {
		t.Fatal("Unpublish(first) error = nil")
	}
	if slices.Contains(harness.ownerships.current.PublishedNodeIDs, nodeID) {
		t.Fatal("first unpublish did not remove ownership fence before allocation")
	}
	harness.operations = nil
	if err := harness.controller.Unpublish(context.Background(), harness.response.VolumeHandle, nodeID); err != nil {
		t.Fatalf("Unpublish(retry) error = %v", err)
	}
	if !slices.Equal(harness.operations, []string{"ownership", "fence", "ownership", "allocation"}) {
		t.Fatalf("retry operations = %#v, want union restore then reverified removal", harness.operations)
	}
	if len(harness.fences.calls) != 2 {
		t.Fatalf("fence verifier calls = %d, want 2", len(harness.fences.calls))
	}
}

func TestEmptyNodeUnpublishEvaluatesEveryPersistedNode(t *testing.T) {
	harness := newPublishHarness(t, validCreateRequest())
	nodes := []string{
		"fr-par-1/44444444-4444-4444-8444-444444444444",
		"fr-par-2/55555555-5555-4555-8555-555555555555",
	}
	for _, nodeID := range nodes {
		if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err != nil {
			t.Fatalf("Publish(%s) error = %v", nodeID, err)
		}
	}
	if err := harness.controller.Unpublish(context.Background(), harness.response.VolumeHandle, ""); err != nil {
		t.Fatalf("Unpublish(all) error = %v", err)
	}
	if !slices.Equal(harness.fences.calls, nodes) {
		t.Fatalf("fence calls = %#v, want %#v", harness.fences.calls, nodes)
	}
}

func TestSingleNodeWriterRejectsSecondDistinctNodeBeforeAttach(t *testing.T) {
	request := validCreateRequest()
	request.Parameters.AccessModes = []volume.AccessMode{volume.AccessModeSingleNodeWriter}
	harness := newPublishHarness(t, request)
	first := "fr-par-1/44444444-4444-4444-8444-444444444444"
	second := "fr-par-2/55555555-5555-4555-8555-555555555555"
	if err := harness.controller.Publish(context.Background(), publishRequest(harness, first, volume.AccessModeSingleNodeWriter)); err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	err := harness.controller.Publish(context.Background(), publishRequest(harness, second, volume.AccessModeSingleNodeWriter))
	if !errors.Is(err, ErrSingleNodeConflict) {
		t.Fatalf("Publish(second) error = %v", err)
	}
	if len(harness.attachments.calls) != 1 {
		t.Fatalf("attachment calls = %d, want 1", len(harness.attachments.calls))
	}
}

func TestPublishMutationsRejectRuntimeClusterMismatchBeforeSideEffects(t *testing.T) {
	const otherCluster = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	nodeID := "fr-par-1/44444444-4444-4444-8444-444444444444"

	t.Run("publish", func(t *testing.T) {
		harness := newPublishHarness(t, validCreateRequest())
		harness.controller.clusterUID = otherCluster
		if err := harness.controller.Publish(context.Background(), publishRequest(harness, nodeID, volume.AccessModeMultiNodeMultiWriter)); err == nil {
			t.Fatal("Publish(copied allocation) error = nil")
		}
		if len(harness.operations) != 0 || len(harness.attachments.calls) != 0 {
			t.Fatalf("copied allocation publish side effects = %v / %v", harness.operations, harness.attachments.calls)
		}
	})

	t.Run("unpublish", func(t *testing.T) {
		harness := newPublishHarness(t, validCreateRequest())
		harness.controller.clusterUID = otherCluster
		if err := harness.controller.Unpublish(context.Background(), harness.response.VolumeHandle, nodeID); err == nil {
			t.Fatal("Unpublish(copied allocation) error = nil")
		}
		if len(harness.operations) != 0 || len(harness.fences.calls) != 0 {
			t.Fatalf("copied allocation unpublish side effects = %v / %v", harness.operations, harness.fences.calls)
		}
	})

	t.Run("reconcile", func(t *testing.T) {
		harness := newPublishHarness(t, validCreateRequest())
		harness.controller.clusterUID = otherCluster
		if err := harness.controller.ReconcilePublishedFences(context.Background(), harness.allocation.LogicalVolumeID); err == nil {
			t.Fatal("ReconcilePublishedFences(copied allocation) error = nil")
		}
		if len(harness.operations) != 0 {
			t.Fatalf("copied allocation reconcile side effects = %v", harness.operations)
		}
	})
}
