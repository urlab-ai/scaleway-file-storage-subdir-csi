package safety

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	archivedDirectory           = ".archived"
	deletedDirectory            = ".deleted"
	metadataDirectory           = ".sfs-subdir-csi"
	maxFilesystemComponentBytes = 255
	// MaxRegularFileInventoryEntries covers the complete supported v1 history
	// of 1,000 active allocations plus 10,000 permanent tombstones and leaves a
	// bounded margin for crash-temporary metadata files.  Keeping one shared
	// limit prevents the parentfs adapter and Linux implementation from silently
	// disagreeing at scale.
	MaxRegularFileInventoryEntries = 16 * 1024
)

// LifecycleFS is the narrow filesystem mutation boundary for logical data
// directories. Implementations must anchor every operation under an already
// opened parent root, reject final symlinks where stated, and make
// RemoveTreeNoFollow refuse every nested mount boundary. RenameNoReplace must
// use the strongest filesystem primitive available and preserve the narrowly
// scoped release-qualified virtiofs compatibility contract in section 6.15 of
// the specification.
type LifecycleFS interface {
	MkdirExclusive(ctx context.Context, relative string, mode uint32) error
	ChownNoFollow(ctx context.Context, relative string, uid, gid uint32) error
	ChmodNoFollow(ctx context.Context, relative string, mode uint32) error
	SyncNode(ctx context.Context, relative string) error
	RenameNoReplace(ctx context.Context, source, destination string) error
	SyncDir(ctx context.Context, relative string) error
	RemoveTreeNoFollow(ctx context.Context, relative string) error
}

// DirectoryInspector proves conclusive presence or absence of one exact real
// directory below the parent root. Absence is conclusive only after every
// parent component was opened safely; symlinks, mount boundaries, unreadable
// parents, and identity replacement return errors rather than false.
type DirectoryInspector interface {
	InspectDirectory(ctx context.Context, relative string) (present bool, err error)
}

// DirectoryState is one stable no-follow logical-root observation. Empty is
// meaningful only when Present is true. Mode contains permission bits only.
type DirectoryState struct {
	Present bool
	Empty   bool
	Mode    uint32
	UID     uint32
	GID     uint32
}

// DirectoryStateInspector additionally proves emptiness and root identity for
// the narrow CreatingDirectory crash-repair window. It must reject nested
// mounts and pathname replacement rather than returning a partial state.
type DirectoryStateInspector interface {
	InspectDirectoryState(ctx context.Context, relative string) (DirectoryState, error)
}

// DirectoryFileLister returns a stable sorted complete direct-child inventory
// of regular files from one descriptor-confined directory. Implementations
// must reject symlinks, directories, special files, nested mounts, replacement,
// and more than maximum entries rather than returning a partial list.
type DirectoryFileLister interface {
	ListRegularFiles(ctx context.Context, relative string, maximum int) ([]string, error)
}

// DirectoryLifecycle applies the required durability barriers around logical
// directory creation, archive/quarantine rename, and quarantine removal.
type DirectoryLifecycle struct {
	filesystem LifecycleFS
	inspector  DirectoryStateInspector
}

// NewDirectoryLifecycle validates its confined filesystem boundary.
func NewDirectoryLifecycle(filesystem LifecycleFS) (*DirectoryLifecycle, error) {
	if filesystem == nil {
		return nil, fmt.Errorf("lifecycle filesystem is nil")
	}
	inspector, _ := filesystem.(DirectoryStateInspector)
	return &DirectoryLifecycle{filesystem: filesystem, inspector: inspector}, nil
}

// PrepareLogicalDirectory creates a missing logical root or repairs only an
// existing empty root left by a crash after exclusive mkdir and before Ready
// ownership installation. A non-empty unowned path is never adopted.
func (lifecycle *DirectoryLifecycle) PrepareLogicalDirectory(ctx context.Context, basePath, directoryName, mode string, uid, gid uint32) error {
	if lifecycle.inspector == nil {
		return fmt.Errorf("lifecycle filesystem cannot prove directory state")
	}
	base, destination, err := logicalDirectoryPaths(basePath, directoryName)
	if err != nil {
		return err
	}
	parsedMode, err := parseMode(mode)
	if err != nil {
		return err
	}
	if uid > 2147483647 || gid > 2147483647 {
		return fmt.Errorf("directory UID and GID must not exceed 2147483647")
	}
	state, err := lifecycle.inspector.InspectDirectoryState(ctx, destination)
	if err != nil {
		return fmt.Errorf("inspect logical directory before creation repair: %w", err)
	}
	if !state.Present {
		return lifecycle.CreateLogicalDirectory(ctx, basePath, directoryName, mode, uid, gid)
	}
	if !state.Empty {
		return fmt.Errorf("existing unowned logical directory %q: %w", destination, ErrDirectoryNotEmpty)
	}
	if err := lifecycle.filesystem.ChownNoFollow(ctx, destination, uid, gid); err != nil {
		return fmt.Errorf("repair logical directory ownership: %w", err)
	}
	if err := lifecycle.filesystem.ChmodNoFollow(ctx, destination, parsedMode); err != nil {
		return fmt.Errorf("repair logical directory mode: %w", err)
	}
	if err := lifecycle.filesystem.SyncNode(ctx, destination); err != nil {
		return fmt.Errorf("sync repaired logical directory inode: %w", err)
	}
	if err := lifecycle.filesystem.SyncDir(ctx, base); err != nil {
		return fmt.Errorf("sync base directory after logical directory repair: %w", err)
	}
	return nil
}

// VerifyLogicalDirectory proves that the exact logical root exists with the
// configured identity. Workload content is permitted; only the root inode's
// mode, UID, GID, mount boundary, and stable pathname are checked.
func (lifecycle *DirectoryLifecycle) VerifyLogicalDirectory(ctx context.Context, basePath, directoryName, mode string, uid, gid uint32) error {
	if lifecycle.inspector == nil {
		return fmt.Errorf("lifecycle filesystem cannot prove directory state")
	}
	_, destination, err := logicalDirectoryPaths(basePath, directoryName)
	if err != nil {
		return err
	}
	parsedMode, err := parseMode(mode)
	if err != nil {
		return err
	}
	state, err := lifecycle.inspector.InspectDirectoryState(ctx, destination)
	if err != nil {
		return fmt.Errorf("inspect logical directory identity: %w", err)
	}
	if !state.Present {
		return fmt.Errorf("logical directory %q is absent", destination)
	}
	if state.Mode != parsedMode || state.UID != uid || state.GID != gid {
		return fmt.Errorf("logical directory %q identity is mode %04o uid %d gid %d, want %04o/%d/%d", destination, state.Mode, state.UID, state.GID, parsedMode, uid, gid)
	}
	return nil
}

// CreateLogicalDirectory creates and configures only the logical volume root.
// It never recursively changes workload data. Ownership metadata must be
// installed separately before the allocation can become Ready.
func (lifecycle *DirectoryLifecycle) CreateLogicalDirectory(ctx context.Context, basePath, directoryName, mode string, uid, gid uint32) error {
	base, destination, err := logicalDirectoryPaths(basePath, directoryName)
	if err != nil {
		return err
	}
	parsedMode, err := parseMode(mode)
	if err != nil {
		return err
	}
	if uid > 2147483647 || gid > 2147483647 {
		return fmt.Errorf("directory UID and GID must not exceed 2147483647")
	}
	if err := lifecycle.filesystem.MkdirExclusive(ctx, destination, parsedMode); err != nil {
		return fmt.Errorf("create logical directory %q: %w", destination, err)
	}
	if err := lifecycle.filesystem.ChownNoFollow(ctx, destination, uid, gid); err != nil {
		return fmt.Errorf("set logical directory ownership: %w", err)
	}
	if err := lifecycle.filesystem.ChmodNoFollow(ctx, destination, parsedMode); err != nil {
		return fmt.Errorf("set logical directory mode: %w", err)
	}
	if err := lifecycle.filesystem.SyncNode(ctx, destination); err != nil {
		return fmt.Errorf("sync logical directory inode: %w", err)
	}
	if err := lifecycle.filesystem.SyncDir(ctx, base); err != nil {
		return fmt.Errorf("sync base directory after logical directory creation: %w", err)
	}
	return nil
}

// Archive moves one exact logical root to a persisted collision-resistant path
// and syncs both source and destination parents before the state may advance.
func (lifecycle *DirectoryLifecycle) Archive(ctx context.Context, basePath, directoryName, archivedPath string) error {
	return lifecycle.renameToManagedDirectory(ctx, basePath, directoryName, archivedPath, archivedDirectory)
}

// Quarantine moves one exact logical root to the persisted .deleted path before
// recursive removal can ever be authorized.
func (lifecycle *DirectoryLifecycle) Quarantine(ctx context.Context, basePath, directoryName, quarantinePath string) error {
	return lifecycle.renameToManagedDirectory(ctx, basePath, directoryName, quarantinePath, deletedDirectory)
}

// QuarantineForGC moves one exact persisted archived or retained path to its
// pre-recorded .deleted target. It accepts only a direct child of the base path
// (retain) or of the managed .archived directory (archive), never an arbitrary
// descendant supplied by an operator request.
func (lifecycle *DirectoryLifecycle) QuarantineForGC(ctx context.Context, basePath, sourcePath, quarantinePath string) error {
	source, sourceParent, err := validateGCSource(basePath, sourcePath)
	if err != nil {
		return err
	}
	target, err := validateManagedTarget(basePath, quarantinePath, deletedDirectory)
	if err != nil {
		return err
	}
	if err := lifecycle.filesystem.RenameNoReplace(ctx, source, target); err != nil {
		return fmt.Errorf("move GC source %q to quarantine %q: %w", source, target, err)
	}
	if err := lifecycle.filesystem.SyncDir(ctx, sourceParent); err != nil {
		return fmt.Errorf("sync GC source directory after rename: %w", err)
	}
	deletedParent, err := RelativeToParent(path.Join(basePath, deletedDirectory))
	if err != nil {
		return err
	}
	if err := lifecycle.filesystem.SyncDir(ctx, deletedParent); err != nil {
		return fmt.Errorf("sync GC quarantine directory after rename: %w", err)
	}
	return nil
}

// RemoveQuarantine recursively removes only a validated .deleted child. The
// backend must reject symlink traversal and mount boundaries. Completion is not
// durable until the .deleted directory itself is synced.
func (lifecycle *DirectoryLifecycle) RemoveQuarantine(ctx context.Context, basePath, quarantinePath string) error {
	base, err := RelativeToParent(basePath)
	if err != nil {
		return err
	}
	quarantine, err := validateManagedTarget(basePath, quarantinePath, deletedDirectory)
	if err != nil {
		return err
	}
	if err := lifecycle.filesystem.RemoveTreeNoFollow(ctx, quarantine); err != nil {
		return fmt.Errorf("remove quarantined logical directory %q: %w", quarantine, err)
	}
	deletedParent := path.Join(base, deletedDirectory)
	if err := lifecycle.filesystem.SyncDir(ctx, deletedParent); err != nil {
		return fmt.Errorf("sync deleted directory after recursive removal: %w", err)
	}
	return nil
}

// SyncDeletedDirectory makes an already-absent quarantine result durable. It is
// used only when matching remove-start evidence exists in both durable records.
func (lifecycle *DirectoryLifecycle) SyncDeletedDirectory(ctx context.Context, basePath string) error {
	base, err := RelativeToParent(basePath)
	if err != nil {
		return err
	}
	deletedParent := path.Join(base, deletedDirectory)
	if err := lifecycle.filesystem.SyncDir(ctx, deletedParent); err != nil {
		return fmt.Errorf("sync deleted directory after observed absence: %w", err)
	}
	return nil
}

func (lifecycle *DirectoryLifecycle) renameToManagedDirectory(ctx context.Context, basePath, directoryName, targetPath, managedDirectory string) error {
	base, source, err := logicalDirectoryPaths(basePath, directoryName)
	if err != nil {
		return err
	}
	target, err := validateManagedTarget(basePath, targetPath, managedDirectory)
	if err != nil {
		return err
	}
	if err := lifecycle.filesystem.RenameNoReplace(ctx, source, target); err != nil {
		return fmt.Errorf("move logical directory %q to %q: %w", source, target, err)
	}
	if err := lifecycle.filesystem.SyncDir(ctx, base); err != nil {
		return fmt.Errorf("sync source base directory after rename: %w", err)
	}
	targetParent := path.Join(base, managedDirectory)
	if err := lifecycle.filesystem.SyncDir(ctx, targetParent); err != nil {
		return fmt.Errorf("sync managed destination directory after rename: %w", err)
	}
	return nil
}

func logicalDirectoryPaths(basePath, directoryName string) (base, destination string, err error) {
	if err := volume.ValidateBasePath(basePath); err != nil {
		return "", "", err
	}
	if err := volume.ValidateDirectoryName(directoryName); err != nil {
		return "", "", err
	}
	base, err = RelativeToParent(basePath)
	if err != nil {
		return "", "", err
	}
	destination, err = JoinRelative(base, directoryName)
	if err != nil {
		return "", "", err
	}
	return base, destination, nil
}

func validateManagedTarget(basePath, targetPath, managedDirectory string) (string, error) {
	if managedDirectory != archivedDirectory && managedDirectory != deletedDirectory {
		return "", fmt.Errorf("managed directory %q is unsupported", managedDirectory)
	}
	if err := volume.ValidateBasePath(basePath); err != nil {
		return "", err
	}
	if targetPath == "" || !strings.HasPrefix(targetPath, "/") || path.Clean(targetPath) != targetPath {
		return "", fmt.Errorf("managed target %q must be absolute and normalized", targetPath)
	}
	expectedParent := path.Join(basePath, managedDirectory)
	if path.Dir(targetPath) != expectedParent {
		return "", fmt.Errorf("managed target %q is not a direct child of %q", targetPath, expectedParent)
	}
	component := path.Base(targetPath)
	if err := validateManagedComponent(component); err != nil {
		return "", err
	}
	relative, err := RelativeToParent(targetPath)
	if err != nil {
		return "", err
	}
	return relative, nil
}

func validateGCSource(basePath, sourcePath string) (relative, parent string, err error) {
	if err := volume.ValidateBasePath(basePath); err != nil {
		return "", "", err
	}
	if sourcePath == "" || !strings.HasPrefix(sourcePath, "/") || path.Clean(sourcePath) != sourcePath {
		return "", "", fmt.Errorf("GC source %q must be absolute and normalized", sourcePath)
	}
	sourceParent := path.Dir(sourcePath)
	switch sourceParent {
	case basePath:
		if err := volume.ValidateDirectoryName(path.Base(sourcePath)); err != nil {
			return "", "", fmt.Errorf("retained GC source: %w", err)
		}
	case path.Join(basePath, archivedDirectory):
		if err := validateManagedComponent(path.Base(sourcePath)); err != nil {
			return "", "", fmt.Errorf("archived GC source: %w", err)
		}
	default:
		return "", "", fmt.Errorf("GC source %q is outside the retained or archived roots", sourcePath)
	}
	relative, err = RelativeToParent(sourcePath)
	if err != nil {
		return "", "", err
	}
	parent, err = RelativeToParent(sourceParent)
	if err != nil {
		return "", "", err
	}
	return relative, parent, nil
}

func validateManagedComponent(component string) error {
	if !utf8.ValidString(component) || len(component) == 0 || len(component) > maxFilesystemComponentBytes {
		return fmt.Errorf("managed target component must contain 1 to %d UTF-8 bytes", maxFilesystemComponentBytes)
	}
	if component == "." || component == ".." || strings.Contains(component, "..") {
		return fmt.Errorf("managed target component %q contains traversal", component)
	}
	for _, character := range component {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' && character != '_' && character != '.' {
			return fmt.Errorf("managed target component %q contains unsafe characters", component)
		}
	}
	if component == archivedDirectory || component == deletedDirectory || component == metadataDirectory {
		return fmt.Errorf("managed target component %q is reserved", component)
	}
	return nil
}

func parseMode(mode string) (uint32, error) {
	if err := volume.ValidateDirectoryMode(mode); err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("parse directory mode %q: %w", mode, err)
	}
	return uint32(parsed), nil
}
