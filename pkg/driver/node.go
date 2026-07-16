package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const nodeMountRollbackTimeout = 5 * time.Second

var (
	// ErrNodePublicationFenceMissing rejects new stage/publish after controller
	// unpublish or before ControllerPublish durable completion.
	ErrNodePublicationFenceMissing = errors.New("local node is absent from published-node authorization")
	// ErrSingleNodeTargetConflict enforces one node-local target for
	// SINGLE_NODE_WRITER.
	ErrSingleNodeTargetConflict = errors.New("single-node volume already has another publish target")
	// ErrCapabilityMismatch identifies a supported wire capability that differs
	// from the immutable capability recorded for the logical volume.
	ErrCapabilityMismatch = errors.New("requested capability differs from durable volume capability")
	// ErrStagingPrerequisite identifies a missing or incompatible staging mount
	// while serving NodePublishVolume.
	ErrStagingPrerequisite = errors.New("node publish staging prerequisite is not satisfied")
	// ErrNodePrecondition marks valid Node RPC input that cannot be served
	// because mounted ownership, logical data, or the live mount graph no longer
	// satisfies the durable volume contract.
	ErrNodePrecondition = errors.New("node volume precondition is not satisfied")
)

// NodeAuthorizer validates the configured parent before mounting and then
// authenticates parent-global and per-volume filesystem ownership through the
// mounted parent. Cleanup accepts non-authorizing detailed lifecycle states but
// preserves exact mapping identity.
type NodeAuthorizer interface {
	ValidateParentContext(immutableContext volume.ImmutableContext) error
	AuthorizeStage(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, capability volume.Capability, localNodeID, parentTarget string) (*volume.DetailedOwnershipRecord, *os.File, error)
	AuthorizePublish(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, localNodeID, parentTarget string) (*volume.DetailedOwnershipRecord, error)
	ResolveCleanup(ctx context.Context, handle volume.Handle, parentTarget, parentFilesystemID, backingRelativePath string) (*volume.DetailedOwnershipRecord, error)
}

// NodeTargetManager performs no-follow host-path validation and owns only pod
// publish target creation/removal. It never creates or removes staging paths.
type NodeTargetManager interface {
	ValidateStaging(ctx context.Context, stagingPath string) (*os.File, error)
	EnsurePublishTarget(ctx context.Context, targetPath string) (target *os.File, created bool, err error)
	RemovePublishTargetIfEmpty(ctx context.Context, targetPath string, expected *mount.TargetIdentity) error
}

// NodeInfo is the exact provider-independent NodeGetInfo projection for v1.
// It deliberately has no maximum-volume or accessible-topology field: physical
// parent attachment limits are not logical PVC limits, and v1 does not expose
// topology constraints.
type NodeInfo struct {
	NodeID string
}

// NodeService implements the provider-independent stage/publish lifecycle.
type NodeService struct {
	nodeID      string
	paths       *NodePathPolicy
	authorizer  NodeAuthorizer
	targets     NodeTargetManager
	mounter     mount.Interface
	gate        *coordination.MutationGate
	volumeLocks *coordination.KeyedLock
	targetLocks *coordination.KeyedLock
	parentLocks *coordination.KeyedLock
}

// NewNodeService validates local identity and mount boundaries.
func NewNodeService(nodeID string, paths *NodePathPolicy, authorizer NodeAuthorizer, targets NodeTargetManager, mounter mount.Interface, gate *coordination.MutationGate, volumeLocks, parentLocks *coordination.KeyedLock) (*NodeService, error) {
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	if paths == nil || authorizer == nil || targets == nil || mounter == nil || gate == nil || volumeLocks == nil || parentLocks == nil {
		return nil, fmt.Errorf("node service dependency is nil")
	}
	return &NodeService{
		nodeID: nodeID, paths: paths, authorizer: authorizer, targets: targets, mounter: mounter, gate: gate,
		volumeLocks: volumeLocks, targetLocks: coordination.NewKeyedLock(), parentLocks: parentLocks,
	}, nil
}

// GetInfo returns the immutable local identity loaded during node startup.
// It performs no metadata, Kubernetes, provider, mount, or filesystem I/O.
func (service *NodeService) GetInfo() NodeInfo {
	return NodeInfo{NodeID: service.nodeID}
}

// Stage validates authorization, mounts/validates the warm parent under the
// nested parent lock, and bind-mounts the exact logical directory to the
// CO-owned staging path.
func (service *NodeService) Stage(ctx context.Context, volumeHandle string, contextValues map[string]string, stagingPath string, capability volume.Capability) (returnErr error) {
	handle, immutableContext, normalizedCapability, err := parseNodeMountRequest(volumeHandle, contextValues, capability)
	if err != nil {
		return err
	}
	if err := service.paths.ValidateStagingPath(stagingPath); err != nil {
		return err
	}
	releaseMutation, err := service.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlockVolume, err := service.volumeLocks.Lock(ctx, handle.LogicalVolumeID)
	if err != nil {
		return err
	}
	defer unlockVolume()
	if err := service.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted exact-unmounts before stage: %w", err)
	}
	stagingTarget, err := service.targets.ValidateStaging(ctx, stagingPath)
	if err != nil {
		return err
	}
	if stagingTarget != nil {
		defer func() { returnErr = errors.Join(returnErr, stagingTarget.Close()) }()
	}
	if err := service.authorizer.ValidateParentContext(immutableContext); err != nil {
		return err
	}
	mapping := mappingFromContext(immutableContext)
	parentTarget, err := service.paths.ParentTarget(mapping.ParentFilesystemID)
	if err != nil {
		return err
	}
	unlockParent, err := service.parentLocks.Lock(ctx, mapping.ParentFilesystemID)
	if err != nil {
		return err
	}
	if err := service.ensureParentMounted(ctx, parentTarget, mapping.ParentFilesystemID); err != nil {
		unlockParent()
		return err
	}
	unlockParent()
	ownership, logicalSource, err := service.authorizer.AuthorizeStage(ctx, handle, immutableContext, normalizedCapability, service.nodeID, parentTarget)
	if err != nil {
		return err
	}
	if logicalSource != nil {
		defer func() { returnErr = errors.Join(returnErr, logicalSource.Close()) }()
	}
	if err := validateCapabilityAgainstOwnership(normalizedCapability, ownership); err != nil {
		return err
	}
	if mappingFromOwnership(ownership) != mapping {
		return fmt.Errorf("authorized ownership mapping differs from immutable volume context: %w", volume.ErrContextMismatch)
	}

	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	parentMount, err := mount.ValidateParent(table, parentTarget, mapping.ParentFilesystemID)
	if err != nil {
		return fmt.Errorf("stage parent validation: %w", err)
	}
	if _, err := table.Exact(stagingPath); err == nil {
		_, validationErr := mount.ValidateStage(table, parentTarget, stagingPath, mapping, normalizedCapability)
		return validationErr
	} else if !errors.Is(err, mount.ErrNotMounted) {
		return err
	}
	entry := mount.Entry{
		Kind: mount.KindStage, Target: stagingPath,
		SourcePath:     path.Join(parentTarget, strings.TrimPrefix(mapping.BasePath, "/"), mapping.DirectoryName),
		SourceMountID:  parentMount.MountID,
		FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID,
		ParentFilesystemID:  mapping.ParentFilesystemID,
		BackingRelativePath: path.Join(mapping.BasePath, mapping.DirectoryName),
		AccessMode:          normalizedCapability.AccessMode,
	}
	bindResult, err := service.mounter.Bind(ctx, mount.BindRequest{Entry: entry, Source: logicalSource, Target: stagingTarget})
	if err != nil {
		return service.rollbackCreatedBind(ctx, stagingPath, bindResult, err)
	}
	return nil
}

// Publish proves the staging graph, enforces target-count semantics, creates
// only the pod target, and binds it with the requested read-only mode.
func (service *NodeService) Publish(ctx context.Context, volumeHandle string, contextValues map[string]string, stagingPath, targetPath string, capability volume.Capability, readOnly bool) (returnErr error) {
	handle, immutableContext, normalizedCapability, err := parseNodeMountRequest(volumeHandle, contextValues, capability)
	if err != nil {
		return err
	}
	if err := service.paths.ValidateStagingPath(stagingPath); err != nil {
		return err
	}
	if err := service.paths.ValidatePublishPath(targetPath); err != nil {
		return err
	}
	releaseMutation, err := service.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := service.volumeLocks.Lock(ctx, handle.LogicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()
	// Target paths are supplied independently of the logical volume handle. A
	// second CSI call using another handle must not replace or remove this target
	// between validation, bind, rollback, and cleanup. The global Node lock order
	// is volume -> target -> parent; no path acquires these locks in reverse.
	unlockTarget, err := service.targetLocks.Lock(ctx, targetPath)
	if err != nil {
		return err
	}
	defer unlockTarget()
	if err := service.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted exact-unmounts before publish: %w", err)
	}
	stagingSource, err := service.targets.ValidateStaging(ctx, stagingPath)
	if err != nil {
		return err
	}
	if stagingSource != nil {
		defer func() { returnErr = errors.Join(returnErr, stagingSource.Close()) }()
	}
	if err := service.authorizer.ValidateParentContext(immutableContext); err != nil {
		return err
	}
	mapping := mappingFromContext(immutableContext)
	parentTarget, err := service.paths.ParentTarget(mapping.ParentFilesystemID)
	if err != nil {
		return err
	}
	// Publish validates and consumes the shared parent/staging graph.  Keep the
	// parent lock nested inside the logical-volume lock through that check and
	// the bind so Stage or another parent check-and-act cannot change the graph
	// between validation and use.
	unlockParent, err := service.parentLocks.Lock(ctx, mapping.ParentFilesystemID)
	if err != nil {
		return err
	}
	defer unlockParent()
	ownership, err := service.authorizer.AuthorizePublish(ctx, handle, immutableContext, service.nodeID, parentTarget)
	if err != nil {
		return err
	}
	if err := validateCapabilityAgainstOwnership(normalizedCapability, ownership); err != nil {
		return err
	}
	if mappingFromOwnership(ownership) != mapping {
		return fmt.Errorf("authorized ownership mapping differs from immutable volume context: %w", volume.ErrContextMismatch)
	}
	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	stagingMount, err := mount.ValidateStage(table, parentTarget, stagingPath, mapping, normalizedCapability)
	if err != nil {
		return fmt.Errorf("publish staging validation: %w: %w", err, ErrStagingPrerequisite)
	}
	if normalizedCapability.AccessMode == volume.AccessModeSingleNodeWriter {
		for _, entry := range table.Entries {
			if entry.Kind == mount.KindPublish && entry.ParentFilesystemID == mapping.ParentFilesystemID && entry.BackingRelativePath == path.Join(mapping.BasePath, mapping.DirectoryName) && entry.Target != targetPath {
				return ErrSingleNodeTargetConflict
			}
		}
	}
	if _, err := table.Exact(targetPath); err == nil {
		_, validationErr := mount.ValidatePublish(table, stagingPath, targetPath, mapping, normalizedCapability, readOnly)
		return validationErr
	} else if !errors.Is(err, mount.ErrNotMounted) {
		return err
	}
	publishTarget, created, err := service.targets.EnsurePublishTarget(ctx, targetPath)
	if err != nil {
		return err
	}
	if publishTarget != nil {
		defer func() { returnErr = errors.Join(returnErr, publishTarget.Close()) }()
	}
	entry := mount.Entry{
		Kind: mount.KindPublish, Target: targetPath, SourcePath: stagingPath,
		SourceMountID:  stagingMount.MountID,
		FilesystemType: "virtiofs", FilesystemSource: mapping.ParentFilesystemID,
		ParentFilesystemID:  mapping.ParentFilesystemID,
		BackingRelativePath: path.Join(mapping.BasePath, mapping.DirectoryName),
		ReadOnly:            readOnly, AccessMode: normalizedCapability.AccessMode,
	}
	bindResult, err := service.mounter.Bind(ctx, mount.BindRequest{Entry: entry, Source: stagingSource, Target: publishTarget})
	if err != nil {
		return service.rollbackPublishBind(ctx, targetPath, publishTarget, created, bindResult, err)
	}
	return nil
}

// Unpublish validates the exact target graph before unmounting one mount ID and
// removes only the now-empty driver-created pod target.
func (service *NodeService) Unpublish(ctx context.Context, volumeHandle, targetPath string) error {
	handle, err := volume.ParseHandle(volumeHandle)
	if err != nil {
		return err
	}
	if err := service.paths.ValidatePublishPath(targetPath); err != nil {
		return err
	}
	releaseMutation, err := service.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := service.volumeLocks.Lock(ctx, handle.LogicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()
	unlockTarget, err := service.targetLocks.Lock(ctx, targetPath)
	if err != nil {
		return err
	}
	defer unlockTarget()
	if err := service.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted exact-unmounts before unpublish: %w", err)
	}
	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	entry, err := table.Exact(targetPath)
	if errors.Is(err, mount.ErrNotMounted) {
		// There is no exact-unmount boundary from which to obtain the directory
		// generation. Preserve an already-unmounted empty target rather than
		// guessing that it still belongs to this historical RPC.
		return nil
	}
	if err != nil {
		return err
	}
	parentTarget, err := service.paths.ParentTarget(entry.ParentFilesystemID)
	if err != nil {
		return err
	}
	ownership, err := service.authorizer.ResolveCleanup(ctx, handle, parentTarget, entry.ParentFilesystemID, entry.BackingRelativePath)
	if err != nil {
		return err
	}
	if ownership == nil {
		return fmt.Errorf("cleanup ownership record is nil")
	}
	if err := validateCleanupPublish(table, entry, mappingFromOwnership(ownership)); err != nil {
		return err
	}
	unmounted, err := service.mounter.UnmountExact(ctx, targetPath, entry.MountID)
	if err != nil {
		return err
	}
	if unmounted.Target == nil {
		return fmt.Errorf("exact unmount returned no underlying target identity")
	}
	return service.targets.RemovePublishTargetIfEmpty(ctx, targetPath, unmounted.Target)
}

// Unstage validates and unmounts only the exact logical bind. It never removes
// the CO-owned staging directory and keeps the shared parent mounted.
func (service *NodeService) Unstage(ctx context.Context, volumeHandle, stagingPath string) error {
	handle, err := volume.ParseHandle(volumeHandle)
	if err != nil {
		return err
	}
	if err := service.paths.ValidateStagingPath(stagingPath); err != nil {
		return err
	}
	releaseMutation, err := service.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := service.volumeLocks.Lock(ctx, handle.LogicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := service.mounter.ReconcileQuarantines(ctx); err != nil {
		return fmt.Errorf("reconcile interrupted exact-unmounts before unstage: %w", err)
	}
	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	entry, err := table.Exact(stagingPath)
	if errors.Is(err, mount.ErrNotMounted) {
		return nil
	}
	if err != nil {
		return err
	}
	parentTarget, err := service.paths.ParentTarget(entry.ParentFilesystemID)
	if err != nil {
		return err
	}
	ownership, err := service.authorizer.ResolveCleanup(ctx, handle, parentTarget, entry.ParentFilesystemID, entry.BackingRelativePath)
	if err != nil {
		return err
	}
	if ownership == nil {
		return fmt.Errorf("cleanup ownership record is nil")
	}
	mapping := mappingFromOwnership(ownership)
	capability := volume.Capability{AccessMode: ownership.NormalizedCreateParameters.AccessModes[0], AccessType: "mount", FilesystemType: "virtiofs", MountFlags: []string{}}
	if _, err := mount.ValidateStage(table, parentTarget, stagingPath, mapping, capability); err != nil {
		return fmt.Errorf("validate staging target before unstage: %w: %w", ErrNodePrecondition, err)
	}
	for _, candidate := range table.Entries {
		if candidate.Kind == mount.KindPublish && candidate.ParentFilesystemID == mapping.ParentFilesystemID && candidate.BackingRelativePath == path.Join(mapping.BasePath, mapping.DirectoryName) {
			return fmt.Errorf("staging target still has child publish mount %q: %w", candidate.Target, ErrNodePrecondition)
		}
		if strings.HasPrefix(candidate.Target, stagingPath+"/") {
			return fmt.Errorf("staging target still has nested mount %q: %w", candidate.Target, mount.ErrForeignMount)
		}
	}
	_, err = service.mounter.UnmountExact(ctx, stagingPath, entry.MountID)
	return err
}
func (service *NodeService) ensureParentMounted(ctx context.Context, target, parentFilesystemID string) error {
	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, err := table.Exact(target); err == nil {
		_, validationErr := mount.ValidateParent(table, target, parentFilesystemID)
		return validationErr
	} else if !errors.Is(err, mount.ErrNotMounted) {
		return err
	}
	if err := service.mounter.MountParent(ctx, parentFilesystemID, target); err != nil {
		return err
	}
	table, err = service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	_, err = mount.ValidateParent(table, target, parentFilesystemID)
	return err
}

func (service *NodeService) rollbackCreatedBind(ctx context.Context, target string, result mount.BindResult, original error) error {
	if result.Mutation == mount.BindMutationNone {
		return original
	}
	if result.Mutation != mount.BindMutationCreated || result.MountID == 0 {
		// Ambiguous provenance is deliberately left mounted.  A retry can
		// authenticate the live graph, whereas guessing here could unmount a
		// concurrent actor's healthy target.
		return original
	}
	// A CSI cancellation can race with a successful mount followed by failed
	// verification. Cleanup must outlive that cancellation, but remains tightly
	// bounded and may use only the generation the mounter attributes to this
	// exact call.
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeMountRollbackTimeout)
	defer cancel()
	_, unmountErr := service.mounter.UnmountExact(rollbackCtx, target, result.MountID)
	return errors.Join(original, unmountErr)
}

func (service *NodeService) rollbackPublishBind(ctx context.Context, targetPath string, target *os.File, created bool, result mount.BindResult, original error) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeMountRollbackTimeout)
	defer cancel()
	if result.Mutation == mount.BindMutationNone {
		return errors.Join(original, service.rollbackPublishTarget(rollbackCtx, targetPath, target, created))
	}
	if result.Mutation != mount.BindMutationCreated || result.MountID == 0 {
		// The target directory must remain while a mount may exist.  Removing it
		// after an ambiguous syscall result would make later graph recovery harder
		// and could race a concurrent mount owner.
		return original
	}
	if _, unmountErr := service.mounter.UnmountExact(rollbackCtx, targetPath, result.MountID); unmountErr != nil {
		return errors.Join(original, unmountErr)
	}
	return errors.Join(original, service.rollbackPublishTarget(rollbackCtx, targetPath, target, created))
}

func (service *NodeService) rollbackPublishTarget(ctx context.Context, targetPath string, target *os.File, created bool) error {
	if !created {
		return nil
	}
	table, err := service.mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	if _, err := table.Exact(targetPath); err == nil {
		return fmt.Errorf("publish bind failed but target remains mounted")
	} else if !errors.Is(err, mount.ErrNotMounted) {
		return err
	}
	var identity *mount.TargetIdentity
	if target != nil {
		derived, err := mount.TargetIdentityForFile(target)
		if err != nil {
			return err
		}
		identity = &derived
	}
	return service.targets.RemovePublishTargetIfEmpty(ctx, targetPath, identity)
}

func parseNodeMountRequest(volumeHandle string, contextValues map[string]string, capability volume.Capability) (volume.Handle, volume.ImmutableContext, volume.Capability, error) {
	handle, err := volume.ParseHandle(volumeHandle)
	if err != nil {
		return volume.Handle{}, volume.ImmutableContext{}, volume.Capability{}, err
	}
	immutableContext, err := volume.ParseImmutableContext(contextValues)
	if err != nil {
		return volume.Handle{}, volume.ImmutableContext{}, volume.Capability{}, err
	}
	normalized, err := volume.NormalizeCapability(capability)
	if err != nil {
		return volume.Handle{}, volume.ImmutableContext{}, volume.Capability{}, err
	}
	mapping := volume.Mapping{
		PoolName: immutableContext.PoolName, ParentFilesystemID: immutableContext.ParentFilesystemID,
		BasePath: immutableContext.BasePath, DirectoryName: immutableContext.DirectoryName,
		LogicalVolumeID: immutableContext.LogicalVolumeID,
	}
	if err := handle.ValidateMapping(mapping); err != nil {
		return volume.Handle{}, volume.ImmutableContext{}, volume.Capability{}, fmt.Errorf("handle mapping differs from volume context: %w: %v", volume.ErrContextMismatch, err)
	}
	return handle, immutableContext, normalized, nil
}

func mappingFromOwnership(ownership *volume.DetailedOwnershipRecord) volume.Mapping {
	return volume.Mapping{
		PoolName: ownership.PoolName, ParentFilesystemID: ownership.ParentFilesystemID,
		BasePath: ownership.BasePath, DirectoryName: ownership.DirectoryName,
		LogicalVolumeID: ownership.LogicalVolumeID,
	}
}

func mappingFromContext(context volume.ImmutableContext) volume.Mapping {
	return volume.Mapping{
		PoolName: context.PoolName, ParentFilesystemID: context.ParentFilesystemID,
		BasePath: context.BasePath, DirectoryName: context.DirectoryName,
		LogicalVolumeID: context.LogicalVolumeID,
	}
}

func validateCapabilityAgainstOwnership(capability volume.Capability, ownership *volume.DetailedOwnershipRecord) error {
	if ownership == nil {
		return fmt.Errorf("mount ownership record is nil")
	}
	if !slices.Contains(ownership.NormalizedCreateParameters.AccessModes, capability.AccessMode) ||
		ownership.NormalizedCreateParameters.AccessType != capability.AccessType ||
		ownership.NormalizedCreateParameters.FilesystemType != capability.FilesystemType {
		return fmt.Errorf("node capability differs from durable ownership capability: %w", ErrCapabilityMismatch)
	}
	return nil
}

func validateCleanupPublish(table mount.Table, entry mount.Entry, mapping volume.Mapping) error {
	if entry.Kind != mount.KindPublish || entry.DeviceID == "" || entry.ParentFilesystemID != mapping.ParentFilesystemID || entry.FilesystemSource != mapping.ParentFilesystemID || entry.FilesystemType != "virtiofs" || entry.BackingRelativePath != path.Join(mapping.BasePath, mapping.DirectoryName) {
		return fmt.Errorf("publish cleanup target is foreign: %w", mount.ErrForeignMount)
	}
	stages := make([]mount.Entry, 0, 1)
	for _, candidate := range table.Entries {
		if strings.HasPrefix(candidate.Target, entry.Target+"/") {
			return fmt.Errorf("publish cleanup target has foreign descendant %q: %w", candidate.Target, mount.ErrForeignMount)
		}
		if candidate.Kind == mount.KindStage && candidate.ParentFilesystemID == mapping.ParentFilesystemID && candidate.FilesystemSource == entry.FilesystemSource && candidate.FilesystemType == entry.FilesystemType && candidate.DeviceID == entry.DeviceID && candidate.BackingRelativePath == entry.BackingRelativePath {
			stages = append(stages, candidate)
		}
	}
	if len(stages) != 1 {
		return fmt.Errorf("publish cleanup found %d matching staging mounts, want exactly one: %w", len(stages), mount.ErrForeignMount)
	}
	return nil
}
