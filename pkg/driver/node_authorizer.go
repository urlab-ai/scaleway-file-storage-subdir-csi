package driver

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"slices"

	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// NodeParentConfiguration is the node-visible immutable identity of one
// configured parent. Provider names and lifecycle placement state are omitted
// because they do not authorize filesystem access.
type NodeParentConfiguration struct {
	PoolName           string
	ParentFilesystemID string
	BasePath           string
}

// NodeParentRegistry is the validated, immutable parent allow-list used before
// any node mount. It also authenticates mounted parent claims after the mount
// becomes readable.
type NodeParentRegistry struct {
	driverName     string
	installationID string
	parents        map[string]NodeParentConfiguration
}

// NewNodeParentRegistry constructs a closed parent allow-list. A physical
// filesystem ID may occur only once, which prevents an ambiguous pool/base
// mapping from authorizing a mount.
func NewNodeParentRegistry(driverName, installationID string, configured []NodeParentConfiguration) (*NodeParentRegistry, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if len(configured) == 0 {
		return nil, fmt.Errorf("node parent registry is empty")
	}
	parents := make(map[string]NodeParentConfiguration, len(configured))
	for index, parent := range configured {
		if err := volume.ValidatePoolName(parent.PoolName); err != nil {
			return nil, fmt.Errorf("node parent %d pool: %w", index, err)
		}
		if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
			return nil, fmt.Errorf("node parent %d: %w", index, err)
		}
		if err := volume.ValidateBasePath(parent.BasePath); err != nil {
			return nil, fmt.Errorf("node parent %d: %w", index, err)
		}
		if _, duplicate := parents[parent.ParentFilesystemID]; duplicate {
			return nil, fmt.Errorf("node parent filesystem %q is configured more than once", parent.ParentFilesystemID)
		}
		parents[parent.ParentFilesystemID] = parent
	}
	return &NodeParentRegistry{
		driverName:     driverName,
		installationID: installationID,
		parents:        parents,
	}, nil
}

// ValidateContext proves that the untrusted CSI context selects one exact
// configured parent before the node attempts a mount.
func (registry *NodeParentRegistry) ValidateContext(context volume.ImmutableContext) error {
	if registry == nil {
		return fmt.Errorf("node parent registry is nil")
	}
	if err := context.Validate(); err != nil {
		return err
	}
	parent, exists := registry.parents[context.ParentFilesystemID]
	if !exists {
		return fmt.Errorf("parent filesystem %q is not configured on this driver: %w", context.ParentFilesystemID, volume.ErrContextMismatch)
	}
	if context.InstallationID != registry.installationID ||
		context.PoolName != parent.PoolName ||
		context.BasePath != parent.BasePath {
		return fmt.Errorf("volume context disagrees with configured installation, pool, or base path: %w", volume.ErrContextMismatch)
	}
	return nil
}

// ValidateClaim authenticates the immutable root-level claim on a mounted
// parent against process identity, the configured pool mapping, and the
// active-cluster identity carried by independently authenticated CSI state.
// The node has no Kubernetes API authority, so it must never invent or load a
// second cluster identity source.
func (registry *NodeParentRegistry) ValidateClaim(claim volume.ParentOwnerRecord, parentFilesystemID, activeClusterUID string) error {
	if registry == nil {
		return fmt.Errorf("node parent registry is nil")
	}
	if err := volume.ValidateClusterUID(activeClusterUID); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	parent, exists := registry.parents[parentFilesystemID]
	if !exists {
		return fmt.Errorf("parent filesystem %q is not configured on this driver", parentFilesystemID)
	}
	if claim.DriverName != registry.driverName ||
		claim.InstallationID != registry.installationID ||
		claim.ActiveClusterUID != activeClusterUID ||
		claim.ParentFilesystemID != parent.ParentFilesystemID ||
		claim.BasePath != parent.BasePath {
		return fmt.Errorf("mounted parent claim disagrees with configured driver identity: %w", ErrNodePrecondition)
	}
	return nil
}

// MappingFromMount reconstructs the only mapping allowed by a mounted backing
// identity. It accepts exactly one direct logical-directory child below the
// configured base path and proves the resulting mapping hash against the
// compact handle.
func (registry *NodeParentRegistry) MappingFromMount(handle volume.Handle, parentFilesystemID, backingRelativePath string) (volume.Mapping, error) {
	if registry == nil {
		return volume.Mapping{}, fmt.Errorf("node parent registry is nil")
	}
	parent, exists := registry.parents[parentFilesystemID]
	if !exists {
		return volume.Mapping{}, fmt.Errorf("parent filesystem %q is not configured on this driver", parentFilesystemID)
	}
	if backingRelativePath == "" || backingRelativePath[0] != '/' || path.Clean(backingRelativePath) != backingRelativePath || path.Dir(backingRelativePath) != parent.BasePath {
		return volume.Mapping{}, fmt.Errorf("mounted backing path %q is not a direct child of configured base path %q", backingRelativePath, parent.BasePath)
	}
	mapping := volume.Mapping{
		PoolName:           parent.PoolName,
		ParentFilesystemID: parent.ParentFilesystemID,
		BasePath:           parent.BasePath,
		DirectoryName:      path.Base(backingRelativePath),
		LogicalVolumeID:    handle.LogicalVolumeID,
	}
	if err := handle.ValidateMapping(mapping); err != nil {
		return volume.Mapping{}, err
	}
	return mapping, nil
}

// NodeAuthorizationFilesystem is an already-mounted, no-follow filesystem
// reader and logical-root identity boundary. Implementations must keep every
// operation confined beneath parentTarget and must reject symlink or mount
// replacement while validating/applying directory identity.
type NodeAuthorizationFilesystem interface {
	ReadParentClaim(ctx context.Context, parentTarget string) ([]byte, error)
	ReadOwnership(ctx context.Context, parentTarget, basePath, logicalVolumeID string) ([]byte, error)
	ValidateAndApplyDirectory(ctx context.Context, parentTarget, basePath, directoryName string, uid, gid uint32, mode string) (*os.File, error)
}

// FilesystemNodeAuthorizer validates the root claim and per-volume ownership
// envelope using only the locally mounted parent. It never calls an
// authenticated provider API.
type FilesystemNodeAuthorizer struct {
	registry   *NodeParentRegistry
	filesystem NodeAuthorizationFilesystem
}

// NewFilesystemNodeAuthorizer validates the node authorization boundaries.
func NewFilesystemNodeAuthorizer(registry *NodeParentRegistry, filesystem NodeAuthorizationFilesystem) (*FilesystemNodeAuthorizer, error) {
	if registry == nil || filesystem == nil {
		return nil, fmt.Errorf("node authorizer dependency is nil")
	}
	return &FilesystemNodeAuthorizer{registry: registry, filesystem: filesystem}, nil
}

// ValidateParentContext rejects an unconfigured or cross-installation context
// before any parent mount operation.
func (authorizer *FilesystemNodeAuthorizer) ValidateParentContext(context volume.ImmutableContext) error {
	return authorizer.registry.ValidateContext(context)
}

// AuthorizeStage authenticates mounted metadata, the local publish fence, and
// the existing data root before applying its configured UID, GID, and mode.
func (authorizer *FilesystemNodeAuthorizer) AuthorizeStage(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, capability volume.Capability, localNodeID, parentTarget string) (*volume.DetailedOwnershipRecord, *os.File, error) {
	ownership, err := authorizer.authorizeMount(ctx, handle, immutableContext, localNodeID, parentTarget)
	if err != nil {
		return nil, nil, err
	}
	if err := validateCapabilityAgainstOwnership(capability, ownership); err != nil {
		return nil, nil, err
	}
	directory, err := authorizer.filesystem.ValidateAndApplyDirectory(ctx, parentTarget, ownership.BasePath, ownership.DirectoryName, ownership.DirectoryUID, ownership.DirectoryGID, ownership.DirectoryMode)
	if err != nil {
		return nil, nil, fmt.Errorf("validate and apply logical directory identity: %w", err)
	}
	return ownership, directory, nil
}

// AuthorizePublish revalidates mounted metadata and the local publish fence
// without changing directory ownership or mode.
func (authorizer *FilesystemNodeAuthorizer) AuthorizePublish(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, localNodeID, parentTarget string) (*volume.DetailedOwnershipRecord, error) {
	return authorizer.authorizeMount(ctx, handle, immutableContext, localNodeID, parentTarget)
}

// ResolveCleanup authenticates a mounted graph using its configured parent and
// backing path. Detailed non-Ready states are accepted only for unmounting;
// they never authorize a new mount.
func (authorizer *FilesystemNodeAuthorizer) ResolveCleanup(ctx context.Context, handle volume.Handle, parentTarget, parentFilesystemID, backingRelativePath string) (*volume.DetailedOwnershipRecord, error) {
	mapping, err := authorizer.registry.MappingFromMount(handle, parentFilesystemID, backingRelativePath)
	if err != nil {
		return nil, err
	}
	ownership, err := authorizer.readDetailedOwnership(ctx, parentTarget, mapping.BasePath, handle.LogicalVolumeID)
	if err != nil {
		return nil, err
	}
	if err := authorizer.validateParentClaim(ctx, parentTarget, parentFilesystemID, ownership.ActiveClusterUID); err != nil {
		return nil, err
	}
	if err := authorizer.validateOwnershipMapping(handle, mapping, ownership); err != nil {
		return nil, err
	}
	return ownership, nil
}

func (authorizer *FilesystemNodeAuthorizer) authorizeMount(ctx context.Context, handle volume.Handle, immutableContext volume.ImmutableContext, localNodeID, parentTarget string) (*volume.DetailedOwnershipRecord, error) {
	if err := authorizer.registry.ValidateContext(immutableContext); err != nil {
		return nil, err
	}
	if err := volume.ValidateNodeID(localNodeID); err != nil {
		return nil, err
	}
	if err := authorizer.validateParentClaim(ctx, parentTarget, immutableContext.ParentFilesystemID, immutableContext.ActiveClusterUID); err != nil {
		return nil, err
	}
	ownership, err := authorizer.readDetailedOwnership(ctx, parentTarget, immutableContext.BasePath, handle.LogicalVolumeID)
	if err != nil {
		return nil, err
	}
	if ownership.State != volume.StateReady {
		return nil, fmt.Errorf("ownership state %q does not authorize a node mount: %w", ownership.State, ErrNodePrecondition)
	}
	if err := volume.ValidateContextAgainstOwnership(handle.String(), immutableContext, ownership); err != nil {
		return nil, err
	}
	if !slices.Contains(ownership.PublishedNodeIDs, localNodeID) {
		return nil, ErrNodePublicationFenceMissing
	}
	return ownership, nil
}

func (authorizer *FilesystemNodeAuthorizer) validateParentClaim(ctx context.Context, parentTarget, parentFilesystemID, activeClusterUID string) error {
	encoded, err := authorizer.filesystem.ReadParentClaim(ctx, parentTarget)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, safety.ErrEntryNotFound) {
			return fmt.Errorf("mounted parent claim is absent: %w", ErrNodePrecondition)
		}
		return fmt.Errorf("read mounted parent claim: %w", err)
	}
	claim, err := volume.DecodeParentOwnerRecord(encoded)
	if err != nil {
		return fmt.Errorf("decode mounted parent claim: %w", err)
	}
	if err := authorizer.registry.ValidateClaim(claim, parentFilesystemID, activeClusterUID); err != nil {
		return fmt.Errorf("validate mounted parent claim: %w", err)
	}
	return nil
}

func (authorizer *FilesystemNodeAuthorizer) readDetailedOwnership(ctx context.Context, parentTarget, basePath, logicalVolumeID string) (*volume.DetailedOwnershipRecord, error) {
	encoded, err := authorizer.filesystem.ReadOwnership(ctx, parentTarget, basePath, logicalVolumeID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, safety.ErrEntryNotFound) {
			return nil, fmt.Errorf("mounted ownership record is absent: %w", ErrNodePrecondition)
		}
		return nil, fmt.Errorf("read mounted ownership record: %w", err)
	}
	record, err := volume.DecodeOwnershipRecord(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode mounted ownership record: %w", err)
	}
	detailed, ok := record.(*volume.DetailedOwnershipRecord)
	if !ok {
		return nil, fmt.Errorf("ownership kind %q cannot authorize or identify a live mount: %w", record.Kind(), ErrNodePrecondition)
	}
	return detailed, nil
}

func (authorizer *FilesystemNodeAuthorizer) validateOwnershipMapping(handle volume.Handle, mapping volume.Mapping, ownership *volume.DetailedOwnershipRecord) error {
	if ownership == nil {
		return fmt.Errorf("cleanup ownership record is nil")
	}
	if ownership.DriverName != authorizer.registry.driverName ||
		ownership.InstallationID != authorizer.registry.installationID ||
		ownership.VolumeHandle != handle.String() ||
		ownership.LogicalVolumeID != mapping.LogicalVolumeID ||
		ownership.MappingHash != handle.MappingHash ||
		ownership.PoolName != mapping.PoolName ||
		ownership.ParentFilesystemID != mapping.ParentFilesystemID ||
		ownership.BasePath != mapping.BasePath ||
		ownership.DirectoryName != mapping.DirectoryName {
		return fmt.Errorf("mounted ownership identity disagrees with handle and backing path: %w", ErrNodePrecondition)
	}
	return nil
}
