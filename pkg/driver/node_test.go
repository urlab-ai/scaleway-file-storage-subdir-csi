package driver

import (
	"context"
	"errors"
	"os"
	"path"
	"slices"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const localNodeID = "fr-par-1/44444444-4444-4444-8444-444444444444"

type fakeNodeAuthorizer struct {
	ownership    *volume.DetailedOwnershipRecord
	contextErr   error
	authErr      error
	cleanupErr   error
	mounter      *mount.Fake
	operations   []string
	authCalls    int
	cleanupCalls int
}

func (authorizer *fakeNodeAuthorizer) ValidateParentContext(immutableContext volume.ImmutableContext) error {
	authorizer.operations = append(authorizer.operations, "validate-context")
	if authorizer.contextErr != nil {
		return authorizer.contextErr
	}
	if authorizer.ownership != nil && (immutableContext.ParentFilesystemID != authorizer.ownership.ParentFilesystemID || immutableContext.PoolName != authorizer.ownership.PoolName || immutableContext.BasePath != authorizer.ownership.BasePath) {
		return errors.New("immutable context selects an unconfigured parent")
	}
	return nil
}

func (authorizer *fakeNodeAuthorizer) AuthorizeStage(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, capability volume.Capability, nodeID, parentTarget string) (*volume.DetailedOwnershipRecord, *os.File, error) {
	authorizer.operations = append(authorizer.operations, "authorize-stage")
	if authorizer.mounter != nil {
		table, err := authorizer.mounter.Snapshot(ctx)
		if err != nil {
			return nil, nil, err
		}
		if entry, err := table.Exact(parentTarget); err != nil || entry.Kind != mount.KindParent {
			return nil, nil, errors.New("stage authorization ran before parent mount")
		}
	}
	ownership, err := authorizer.authorizeMount(handle, immutableContext, nodeID)
	if err != nil {
		return nil, nil, err
	}
	if err := validateCapabilityAgainstOwnership(capability, ownership); err != nil {
		return nil, nil, err
	}
	return ownership, nil, nil
}

func (authorizer *fakeNodeAuthorizer) AuthorizePublish(_ context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, nodeID, _ string) (*volume.DetailedOwnershipRecord, error) {
	authorizer.operations = append(authorizer.operations, "authorize-publish")
	return authorizer.authorizeMount(handle, immutableContext, nodeID)
}

func (authorizer *fakeNodeAuthorizer) authorizeMount(handle volume.Handle, immutableContext volume.ImmutableContext, nodeID string) (*volume.DetailedOwnershipRecord, error) {
	authorizer.authCalls++
	if authorizer.authErr != nil {
		return nil, authorizer.authErr
	}
	if authorizer.ownership == nil {
		return nil, nil
	}
	if authorizer.ownership.State != volume.StateReady || !slices.Contains(authorizer.ownership.PublishedNodeIDs, nodeID) {
		return nil, ErrNodePublicationFenceMissing
	}
	if handle.LogicalVolumeID != authorizer.ownership.LogicalVolumeID || handle.MappingHash != authorizer.ownership.MappingHash || immutableContext.ParentFilesystemID != authorizer.ownership.ParentFilesystemID || immutableContext.DirectoryName != authorizer.ownership.DirectoryName {
		return nil, errors.New("immutable context disagrees with ownership")
	}
	return authorizer.ownership, nil
}

func (authorizer *fakeNodeAuthorizer) ResolveCleanup(_ context.Context, handle volume.Handle, _ string, parentFilesystemID, backingRelativePath string) (*volume.DetailedOwnershipRecord, error) {
	authorizer.operations = append(authorizer.operations, "resolve-cleanup")
	authorizer.cleanupCalls++
	if authorizer.cleanupErr != nil {
		return nil, authorizer.cleanupErr
	}
	if authorizer.ownership == nil {
		return nil, nil
	}
	if handle.LogicalVolumeID != authorizer.ownership.LogicalVolumeID || handle.MappingHash != authorizer.ownership.MappingHash {
		return nil, errors.New("cleanup handle mismatch")
	}
	if parentFilesystemID != authorizer.ownership.ParentFilesystemID || backingRelativePath != "/kubernetes-volumes/"+authorizer.ownership.DirectoryName {
		return nil, errors.New("cleanup mount identity mismatch")
	}
	return authorizer.ownership, nil
}

type fakeNodeTargets struct {
	staging    map[string]bool
	publish    map[string]bool
	operations []string
}

func (targets *fakeNodeTargets) ValidateStaging(_ context.Context, stagingPath string) (*os.File, error) {
	targets.operations = append(targets.operations, "validate-stage:"+stagingPath)
	if !targets.staging[stagingPath] {
		return nil, errors.New("staging directory missing")
	}
	return nil, nil
}

func (targets *fakeNodeTargets) EnsurePublishTarget(_ context.Context, targetPath string) (*os.File, bool, error) {
	targets.operations = append(targets.operations, "ensure-publish:"+targetPath)
	if targets.publish[targetPath] {
		return nil, false, nil
	}
	targets.publish[targetPath] = true
	return nil, true, nil
}

func (targets *fakeNodeTargets) RemovePublishTargetIfEmpty(_ context.Context, targetPath string, _ *mount.TargetIdentity) error {
	targets.operations = append(targets.operations, "remove-publish:"+targetPath)
	delete(targets.publish, targetPath)
	return nil
}

type nodeHarness struct {
	service     *NodeService
	mounter     *mount.Fake
	parentLocks *coordination.KeyedLock
	authorizer  *fakeNodeAuthorizer
	targets     *fakeNodeTargets
	response    CreateResponse
	owner       *volume.DetailedOwnershipRecord
	staging     string
	targetA     string
	targetB     string
}

type reconcileFailMounter struct {
	mount.Interface
	err   error
	calls int
}

func (mounter *reconcileFailMounter) ReconcileQuarantines(context.Context) error {
	mounter.calls++
	return mounter.err
}

func newNodeHarness(t *testing.T, mode volume.AccessMode) *nodeHarness {
	t.Helper()
	request := validCreateRequest()
	request.Parameters.AccessModes = []volume.AccessMode{mode}
	create := newCreateHarness(t)
	response, err := create.controller.Create(context.Background(), request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	handle, err := volume.ParseHandle(response.VolumeHandle)
	if err != nil {
		t.Fatalf("ParseHandle() error = %v", err)
	}
	stored, err := create.store.Get(context.Background(), handle.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	owner, err := ownershipFromCreatingAllocation(stored.Record.(*volume.DetailedAllocationRecord))
	if err != nil {
		t.Fatalf("ownershipFromCreatingAllocation() error = %v", err)
	}
	owner, err = ownershipWithPublishedNodes(owner, []string{localNodeID})
	if err != nil {
		t.Fatalf("ownershipWithPublishedNodes() error = %v", err)
	}
	paths, err := NewNodePathPolicy(driverTestName, "/var/lib/kubelet", "/var/lib/scaleway-sfs-subdir-csi/parents")
	if err != nil {
		t.Fatalf("NewNodePathPolicy() error = %v", err)
	}
	harness := &nodeHarness{
		mounter:    mount.NewFake(),
		authorizer: &fakeNodeAuthorizer{ownership: owner},
		targets:    &fakeNodeTargets{staging: make(map[string]bool), publish: make(map[string]bool)},
		response:   response, owner: owner,
		staging: "/var/lib/kubelet/plugins/kubernetes.io/csi/file-storage-subdir.csi.urlab.ai/volume-a/globalmount",
		targetA: "/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount",
		targetB: "/var/lib/kubelet/pods/pod-b/volumes/kubernetes.io~csi/pv-a/mount",
	}
	harness.targets.staging[harness.staging] = true
	harness.authorizer.mounter = harness.mounter
	harness.parentLocks = coordination.NewKeyedLock()
	harness.service, err = NewNodeService(localNodeID, paths, harness.authorizer, harness.targets, harness.mounter, newNodeTestGate(t), coordination.NewKeyedLock(), harness.parentLocks)
	if err != nil {
		t.Fatalf("NewNodeService() error = %v", err)
	}
	return harness
}

func TestNodePublishWaitsForParentLockInsideVolumeLock(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	unblockParent, err := harness.parentLocks.Lock(context.Background(), harness.owner.ParentFilesystemID)
	if err != nil {
		t.Fatalf("Lock(parent) error = %v", err)
	}
	defer unblockParent()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- harness.service.Publish(ctx, harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false)
	}()
	// Cancellation is deterministic proof that Publish was waiting on the
	// existing context-aware parent lock. Without that lock it would create and
	// mount the target before this cancellation is observed.
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(blocked parent lock) error = %v, want context cancellation", err)
	}
	if harness.targets.publish[harness.targetA] {
		t.Fatal("Publish crossed the blocked parent lock and created its target")
	}
}

func TestNodePublishWaitsForTargetLockBeforeTouchingTarget(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	unblockTarget, err := harness.service.targetLocks.Lock(context.Background(), harness.targetA)
	if err != nil {
		t.Fatalf("Lock(target) error = %v", err)
	}
	defer unblockTarget()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- harness.service.Publish(ctx, harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false)
	}()
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(blocked target lock) error = %v, want context cancellation", err)
	}
	if harness.targets.publish[harness.targetA] {
		t.Fatal("Publish touched the target while its target-path lock was held")
	}
}

func TestNodeUnpublishReconcilesQuarantineBeforeAbsentSuccess(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	injected := errors.New("unresolved private quarantine")
	wrapped := &reconcileFailMounter{Interface: harness.mounter, err: injected}
	service, err := NewNodeService(
		localNodeID, harness.service.paths, harness.authorizer, harness.targets, wrapped,
		newNodeTestGate(t), coordination.NewKeyedLock(), coordination.NewKeyedLock(),
	)
	if err != nil {
		t.Fatalf("NewNodeService() error = %v", err)
	}
	if err := service.Unpublish(context.Background(), harness.response.VolumeHandle, harness.targetA); !errors.Is(err, injected) {
		t.Fatalf("Unpublish(unresolved quarantine) error = %v", err)
	}
	if wrapped.calls != 1 || len(harness.targets.operations) != 0 {
		t.Fatalf("quarantine calls/target operations = %d/%#v", wrapped.calls, harness.targets.operations)
	}
}

func newNodeTestGate(t *testing.T) *coordination.MutationGate {
	t.Helper()
	gate, err := coordination.NewMutationGate(10)
	if err != nil {
		t.Fatalf("NewMutationGate() error = %v", err)
	}
	return gate
}

func nodeCapability(mode volume.AccessMode) volume.Capability {
	return volume.Capability{AccessMode: mode, AccessType: "mount", FilesystemType: "virtiofs", MountFlags: []string{}}
}

func TestNodeGetInfoReturnsOnlyImmutableLogicalNodeIdentity(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	if got := harness.service.GetInfo(); got != (NodeInfo{NodeID: localNodeID}) {
		t.Fatalf("GetInfo() = %#v", got)
	}
	if operations := harness.mounter.Operations(); len(operations) != 0 {
		t.Fatalf("GetInfo() performed mount I/O: %#v", operations)
	}
	if harness.authorizer.authCalls != 0 || harness.authorizer.cleanupCalls != 0 {
		t.Fatal("GetInfo() performed authorization I/O")
	}
}

func TestNodeMutationGateRejectsRPCsAfterTerminalPrepare(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	requestID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if err := harness.service.gate.BeginQuiesce(context.Background(), requestID); err != nil {
		t.Fatal(err)
	}
	err := harness.service.Stage(
		context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext,
		harness.staging, nodeCapability(volume.AccessModeMultiNodeMultiWriter),
	)
	if !errors.Is(err, coordination.ErrMutationQuiesced) {
		t.Fatalf("Stage(quiesced) error = %v, want ErrMutationQuiesced", err)
	}
	if table, snapshotErr := harness.mounter.Snapshot(context.Background()); snapshotErr != nil || len(table.Entries) != 0 {
		t.Fatalf("quiesced Stage changed mount table: %#v, %v", table, snapshotErr)
	}
}

func TestNodeStageMountsParentThenLogicalBindAndRetriesExactly(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	want := []string{
		"mount-parent:/var/lib/scaleway-sfs-subdir-csi/parents/33333333-3333-4333-8333-333333333333",
		"bind:" + harness.staging,
	}
	if got := harness.mounter.Operations(); !slices.Equal(got, want) {
		t.Fatalf("mount operations = %#v, want %#v", got, want)
	}
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage(retry) error = %v", err)
	}
	if got := harness.mounter.Operations(); !slices.Equal(got, want) {
		t.Fatalf("Stage(retry) mutated mount table: %#v", got)
	}
	if !slices.Equal(harness.targets.operations, []string{"validate-stage:" + harness.staging, "validate-stage:" + harness.staging}) {
		t.Fatalf("staging target operations = %#v", harness.targets.operations)
	}
}

func TestNodeStageRollsBackExactBindAfterAmbiguousAdapterFailure(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	bindErr := errors.New("post-bind verification failed")
	harness.mounter.BindAfterError = bindErr
	err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, nodeCapability(volume.AccessModeMultiNodeMultiWriter))
	if !errors.Is(err, bindErr) {
		t.Fatalf("Stage() error = %v, want bind error", err)
	}
	table, snapshotErr := harness.mounter.Snapshot(context.Background())
	if snapshotErr != nil {
		t.Fatalf("Snapshot() error = %v", snapshotErr)
	}
	if _, exactErr := table.Exact(harness.staging); !errors.Is(exactErr, mount.ErrNotMounted) {
		t.Fatalf("staging mount after rollback error = %v", exactErr)
	}
	if operations := harness.mounter.Operations(); !slices.Contains(operations, "unmount:"+harness.staging) {
		t.Fatalf("mount operations = %v", operations)
	}
}

func TestNodeStageRollbackSurvivesCancellationAndRefusesForeignMount(t *testing.T) {
	t.Run("cancelled exact mount", func(t *testing.T) {
		harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
		capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
		if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
			t.Fatalf("Stage() error = %v", err)
		}
		table, err := harness.mounter.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot() error = %v", err)
		}
		created, err := table.Exact(harness.staging)
		if err != nil {
			t.Fatalf("Exact(staging) error = %v", err)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		original := errors.New("ambiguous bind result")
		err = harness.service.rollbackCreatedBind(cancelled, harness.staging, mount.BindResult{Mutation: mount.BindMutationCreated, MountID: created.MountID}, original)
		if !errors.Is(err, original) {
			t.Fatalf("rollbackCreatedBind() error = %v", err)
		}
		table, snapshotErr := harness.mounter.Snapshot(context.Background())
		if snapshotErr != nil {
			t.Fatalf("Snapshot() error = %v", snapshotErr)
		}
		if _, exactErr := table.Exact(harness.staging); !errors.Is(exactErr, mount.ErrNotMounted) {
			t.Fatalf("staging mount after cancelled rollback error = %v", exactErr)
		}
	})

	t.Run("foreign mount", func(t *testing.T) {
		harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
		mapping := mappingFromOwnership(harness.owner)
		parentTarget, err := harness.service.paths.ParentTarget(mapping.ParentFilesystemID)
		if err != nil {
			t.Fatalf("ParentTarget() error = %v", err)
		}
		if err := harness.mounter.MountParent(context.Background(), mapping.ParentFilesystemID, parentTarget); err != nil {
			t.Fatalf("MountParent() error = %v", err)
		}
		harness.mounter.Seed(mount.Entry{
			Kind: mount.KindStage, Target: harness.staging,
			DeviceID: "foreign-device", FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID,
			ParentFilesystemID: mapping.ParentFilesystemID, BackingRelativePath: mapping.BasePath + "/" + mapping.DirectoryName,
			AccessMode: volume.AccessModeMultiNodeMultiWriter,
		})
		original := errors.New("ambiguous bind result")
		err = harness.service.rollbackCreatedBind(context.Background(), harness.staging, mount.BindResult{Mutation: mount.BindMutationNone}, original)
		if !errors.Is(err, original) {
			t.Fatalf("rollbackCreatedBind(no provenance) error = %v", err)
		}
		table, snapshotErr := harness.mounter.Snapshot(context.Background())
		if snapshotErr != nil {
			t.Fatalf("Snapshot() error = %v", snapshotErr)
		}
		if _, exactErr := table.Exact(harness.staging); exactErr != nil {
			t.Fatalf("foreign staging mount was removed: %v", exactErr)
		}
	})
}

func TestNodePublishReportsMissingStagingPrerequisite(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false)
	if !errors.Is(err, ErrStagingPrerequisite) || !errors.Is(err, mount.ErrNotMounted) {
		t.Fatalf("Publish(missing staging) error = %v, want ErrStagingPrerequisite and ErrNotMounted", err)
	}
}

func TestNodePublishReadOnlyAndExactRetry(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, true); err != nil {
		t.Fatalf("Publish(read-only) error = %v", err)
	}
	table, err := harness.mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	entry, err := table.Exact(harness.targetA)
	if err != nil {
		t.Fatalf("Exact(target) error = %v", err)
	}
	if !entry.ReadOnly {
		t.Fatal("read-only publish target is writable")
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, true); err != nil {
		t.Fatalf("Publish(retry) error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); !errors.Is(err, mount.ErrMountConflict) {
		t.Fatalf("Publish(read-only conflict) error = %v", err)
	}
}

func TestNodePublishRollbackRemovesOnlyNewUnMountedTarget(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	harness.mounter.BindError = errors.New("injected bind failure")
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); err == nil {
		t.Fatal("Publish(bind failure) error = nil")
	}
	if harness.targets.publish[harness.targetA] {
		t.Fatal("new target remained after unmounted bind failure")
	}
}

func TestNodePublishRollsBackExactAmbiguousBindBeforeRemovingTarget(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	bindErr := errors.New("post-bind verification failed")
	harness.mounter.BindAfterError = bindErr
	err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false)
	if !errors.Is(err, bindErr) {
		t.Fatalf("Publish() error = %v, want bind error", err)
	}
	table, snapshotErr := harness.mounter.Snapshot(context.Background())
	if snapshotErr != nil {
		t.Fatalf("Snapshot() error = %v", snapshotErr)
	}
	if _, exactErr := table.Exact(harness.targetA); !errors.Is(exactErr, mount.ErrNotMounted) {
		t.Fatalf("publish mount after rollback error = %v", exactErr)
	}
	if harness.targets.publish[harness.targetA] {
		t.Fatal("driver-created publish target survived exact bind rollback")
	}
	operations := harness.mounter.Operations()
	if !slices.Contains(operations, "unmount:"+harness.targetA) {
		t.Fatalf("mount operations = %v", operations)
	}
}

func TestSingleNodeWriterRejectsSecondNodeTarget(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeSingleNodeWriter)
	capability := nodeCapability(volume.AccessModeSingleNodeWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetB, capability, false); !errors.Is(err, ErrSingleNodeTargetConflict) {
		t.Fatalf("Publish(second target) error = %v", err)
	}
	if harness.targets.publish[harness.targetB] {
		t.Fatal("second SINGLE_NODE_WRITER target was created")
	}
}

func TestNodeUnpublishAndUnstageUseExactMountIDsAndOwnership(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if err := harness.service.Unstage(context.Background(), harness.response.VolumeHandle, harness.staging); err == nil {
		t.Fatal("Unstage(with publish child) error = nil")
	} else if !errors.Is(err, ErrNodePrecondition) {
		t.Fatalf("Unstage(with publish child) error = %v, want ErrNodePrecondition", err)
	}
	if err := harness.service.Unpublish(context.Background(), harness.response.VolumeHandle, harness.targetA); err != nil {
		t.Fatalf("Unpublish() error = %v", err)
	}
	if harness.targets.publish[harness.targetA] {
		t.Fatal("publish target directory remained")
	}
	if err := harness.service.Unstage(context.Background(), harness.response.VolumeHandle, harness.staging); err != nil {
		t.Fatalf("Unstage() error = %v", err)
	}
	if !harness.targets.staging[harness.staging] {
		t.Fatal("CO-owned staging directory was removed")
	}
	table, err := harness.mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	parentTarget := "/var/lib/scaleway-sfs-subdir-csi/parents/33333333-3333-4333-8333-333333333333"
	if _, err := table.Exact(parentTarget); err != nil {
		t.Fatalf("warm parent was removed: %v", err)
	}
}

func TestNodeUnstageClassifiesIncompatibleStagingAsPrecondition(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	mapping := mappingFromOwnership(harness.owner)
	parentTarget, err := harness.service.paths.ParentTarget(mapping.ParentFilesystemID)
	if err != nil {
		t.Fatalf("ParentTarget() error = %v", err)
	}
	if err := harness.mounter.MountParent(context.Background(), mapping.ParentFilesystemID, parentTarget); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	table, err := harness.mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	parent, err := table.Exact(parentTarget)
	if err != nil {
		t.Fatalf("Exact(parent) error = %v", err)
	}
	harness.mounter.Seed(mount.Entry{
		Kind: mount.KindStage, Target: harness.staging, DeviceID: parent.DeviceID,
		FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID,
		ParentFilesystemID:  mapping.ParentFilesystemID,
		BackingRelativePath: path.Join(mapping.BasePath, mapping.DirectoryName), ReadOnly: true,
	})
	err = harness.service.Unstage(context.Background(), harness.response.VolumeHandle, harness.staging)
	if !errors.Is(err, ErrNodePrecondition) || !errors.Is(err, mount.ErrMountConflict) {
		t.Fatalf("Unstage(incompatible staging) error = %v, want ErrNodePrecondition and ErrMountConflict", err)
	}
}

func TestNodeUnpublishRejectsStackedTargetWithoutUnmount(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	table, err := harness.mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	entry, err := table.Exact(harness.targetA)
	if err != nil {
		t.Fatalf("Exact() error = %v", err)
	}
	entry.MountID = 99
	harness.mounter.Seed(entry)
	if err := harness.service.Unpublish(context.Background(), harness.response.VolumeHandle, harness.targetA); !errors.Is(err, mount.ErrStackedMount) {
		t.Fatalf("Unpublish(stacked) error = %v", err)
	}
	if !harness.targets.publish[harness.targetA] {
		t.Fatal("stacked target directory was removed")
	}
}

func TestNodeUnpublishRejectsForeignDescendantWithoutUnmount(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	capability := nodeCapability(volume.AccessModeMultiNodeMultiWriter)
	if err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, capability); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if err := harness.service.Publish(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, harness.targetA, capability, false); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	harness.mounter.Seed(mount.Entry{
		MountID: 99, Kind: mount.KindForeign,
		Target: harness.targetA + "/nested", ParentFilesystemID: harness.owner.ParentFilesystemID,
	})
	if err := harness.service.Unpublish(context.Background(), harness.response.VolumeHandle, harness.targetA); !errors.Is(err, mount.ErrForeignMount) {
		t.Fatalf("Unpublish(foreign descendant) error = %v", err)
	}
	table, err := harness.mounter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if _, err := table.Exact(harness.targetA); err != nil {
		t.Fatalf("publish mount was removed despite foreign descendant: %v", err)
	}
}

func TestNodeStageRejectsMissingLocalPublishedFenceBeforeMount(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	harness.authorizer.ownership.PublishedNodeIDs = []string{}
	sealed, err := harness.authorizer.ownership.Seal()
	if err != nil {
		t.Fatalf("ownership.Seal() error = %v", err)
	}
	harness.authorizer.ownership = &sealed
	err = harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, nodeCapability(volume.AccessModeMultiNodeMultiWriter))
	if !errors.Is(err, ErrNodePublicationFenceMissing) {
		t.Fatalf("Stage(missing local fence) error = %v", err)
	}
	want := []string{"mount-parent:/var/lib/scaleway-sfs-subdir-csi/parents/33333333-3333-4333-8333-333333333333"}
	if got := harness.mounter.Operations(); !slices.Equal(got, want) {
		t.Fatalf("unauthorized Stage performed more than the prerequisite parent mount: %#v", got)
	}
}

func TestNodeMountRejectsCapabilityThatDiffersFromOwnership(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, nodeCapability(volume.AccessModeSingleNodeWriter))
	if !errors.Is(err, ErrCapabilityMismatch) {
		t.Fatalf("Stage(capability mismatch) error = %v, want ErrCapabilityMismatch", err)
	}
	want := []string{"mount-parent:/var/lib/scaleway-sfs-subdir-csi/parents/33333333-3333-4333-8333-333333333333"}
	if got := harness.mounter.Operations(); !slices.Equal(got, want) {
		t.Fatalf("capability mismatch performed more than the prerequisite parent mount: %#v", got)
	}
}

func TestNodeMountRejectsNilOwnershipRecord(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	harness.authorizer.ownership = nil
	err := harness.service.Stage(context.Background(), harness.response.VolumeHandle, harness.response.VolumeContext, harness.staging, nodeCapability(volume.AccessModeMultiNodeMultiWriter))
	if err == nil {
		t.Fatal("Stage(nil ownership) error = nil")
	}
	want := []string{"mount-parent:/var/lib/scaleway-sfs-subdir-csi/parents/33333333-3333-4333-8333-333333333333"}
	if got := harness.mounter.Operations(); !slices.Equal(got, want) {
		t.Fatalf("nil ownership performed more than the prerequisite parent mount: %#v", got)
	}
}

func TestNodeAbsentCleanupDoesNotRequireOwnershipRead(t *testing.T) {
	harness := newNodeHarness(t, volume.AccessModeMultiNodeMultiWriter)
	harness.authorizer.cleanupErr = errors.New("injected ownership outage")

	if err := harness.service.Unpublish(context.Background(), harness.response.VolumeHandle, harness.targetA); err != nil {
		t.Fatalf("Unpublish(absent) error = %v", err)
	}
	if err := harness.service.Unstage(context.Background(), harness.response.VolumeHandle, harness.staging); err != nil {
		t.Fatalf("Unstage(absent) error = %v", err)
	}
	if harness.authorizer.cleanupCalls != 0 {
		t.Fatalf("absent cleanup performed %d ownership reads, want 0", harness.authorizer.cleanupCalls)
	}
}
