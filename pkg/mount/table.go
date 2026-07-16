package mount

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

var (
	ErrNotMounted    = errors.New("target is not mounted")
	ErrStackedMount  = errors.New("target has stacked mounts")
	ErrForeignMount  = errors.New("target mount identity is foreign or mismatched")
	ErrMountConflict = errors.New("target is already mounted incompatibly")
)

// Kind identifies the driver's normalized mount role.
type Kind string

const (
	KindParent  Kind = "parent"
	KindStage   Kind = "stage"
	KindPublish Kind = "publish"
	// KindForeign retains a mount below a driver-protected root whose role is
	// not one of the exact parent/stage/publish targets. It must stay visible so
	// destructive workflows can fail closed instead of moving it with a parent.
	KindForeign Kind = "foreign"
	// KindQuarantine is an interrupted exact-unmount still visible in the
	// private quarantine. Read-only inventories report it; mutating workflows
	// must reconcile it before concluding that a public target is absent.
	KindQuarantine Kind = "quarantine"
)

// Entry is one normalized live Linux mountinfo record. SourcePath is the exact
// path used for the bind operation; ParentFilesystemID and BackingRelativePath
// prove the ultimate virtiofs backing independently of path aliases.
type Entry struct {
	// MountID is the non-reusable STATX_MNT_ID_UNIQUE generation in a live
	// KernelMounter snapshot. Fakes use monotonically increasing generations.
	// MountInfoID retains the namespace-local reusable ID used for stack order
	// and ParentMountID relationships in /proc/self/mountinfo.
	MountID       uint64
	MountInfoID   uint64
	ParentMountID uint64
	// SourceMountID is transient bind authorization: callers set it to the
	// non-reusable generation of the already-validated parent (Stage) or
	// staging mount (Publish). Live mount-table entries leave it zero.
	SourceMountID       uint64
	DeviceID            string
	Kind                Kind
	Target              string
	SourcePath          string
	FilesystemType      string
	FilesystemSource    string
	ParentFilesystemID  string
	BackingRelativePath string
	ReadOnly            bool
	AccessMode          volume.AccessMode
}

// Table is one coherent live mount-table snapshot.
type Table struct {
	Entries []Entry
}

// AtTarget returns the exact target stack in mount ID order.
func (table Table) AtTarget(target string) []Entry {
	result := make([]Entry, 0)
	for _, entry := range table.Entries {
		if entry.Target == target {
			result = append(result, entry)
		}
	}
	slices.SortFunc(result, func(left, right Entry) int {
		leftOrder, rightOrder := left.MountInfoID, right.MountInfoID
		if leftOrder == 0 {
			leftOrder = left.MountID
		}
		if rightOrder == 0 {
			rightOrder = right.MountID
		}
		if leftOrder < rightOrder {
			return -1
		}
		if leftOrder > rightOrder {
			return 1
		}
		return 0
	})
	return result
}

// Exact returns one mount or a typed absence/stacking error.
func (table Table) Exact(target string) (Entry, error) {
	entries := table.AtTarget(target)
	switch len(entries) {
	case 0:
		return Entry{}, ErrNotMounted
	case 1:
		return entries[0], nil
	default:
		return Entry{}, fmt.Errorf("target %q has %d mount layers: %w", target, len(entries), ErrStackedMount)
	}
}

// ValidateParent proves an exact flagless virtiofs parent mount.
func ValidateParent(table Table, target, parentFilesystemID string) (Entry, error) {
	return validateParentFilesystem(table, target, parentFilesystemID, "virtiofs", parentFilesystemID)
}

func validateParentFilesystem(table Table, target, parentFilesystemID, filesystemType, filesystemSource string) (Entry, error) {
	entry, err := table.Exact(target)
	if err != nil {
		return Entry{}, err
	}
	if entry.Kind != KindParent || entry.Target != target || entry.DeviceID == "" || entry.FilesystemType != filesystemType || entry.FilesystemSource != filesystemSource || entry.ParentFilesystemID != parentFilesystemID || entry.BackingRelativePath != "/" || entry.ReadOnly {
		return Entry{}, fmt.Errorf("parent target %q is not exact %s source %q: %w", target, filesystemType, filesystemSource, ErrForeignMount)
	}
	return entry, nil
}

// ValidateStage proves an exact logical-directory bind backed by the expected
// parent, capability, and writable staging target.
func ValidateStage(table Table, parentTarget, stagingTarget string, mapping volume.Mapping, capability volume.Capability) (Entry, error) {
	if _, err := volume.NormalizeCapability(capability); err != nil {
		return Entry{}, err
	}
	entry, err := table.Exact(stagingTarget)
	if err != nil {
		return Entry{}, err
	}
	expectedRelative := path.Join(mapping.BasePath, mapping.DirectoryName)
	if entry.Kind != KindStage || entry.Target != stagingTarget || entry.FilesystemType != "virtiofs" || entry.FilesystemSource != mapping.ParentFilesystemID || entry.ParentFilesystemID != mapping.ParentFilesystemID || entry.BackingRelativePath != expectedRelative || entry.ReadOnly {
		return Entry{}, fmt.Errorf("staging target %q does not match logical mapping: %w", stagingTarget, ErrMountConflict)
	}
	parent, err := ValidateParent(table, parentTarget, mapping.ParentFilesystemID)
	if err != nil {
		return Entry{}, fmt.Errorf("staging backing parent: %w", err)
	}
	if entry.DeviceID == "" || entry.DeviceID != parent.DeviceID {
		return Entry{}, fmt.Errorf("staging target device differs from parent: %w", ErrForeignMount)
	}
	return entry, nil
}

// ValidatePublish proves an exact bind from the requested staging path and the
// same ultimate logical directory, including read-only mode.
func ValidatePublish(table Table, stagingTarget, publishTarget string, mapping volume.Mapping, capability volume.Capability, readOnly bool) (Entry, error) {
	if _, err := volume.NormalizeCapability(capability); err != nil {
		return Entry{}, err
	}
	stage, err := table.Exact(stagingTarget)
	if err != nil {
		return Entry{}, fmt.Errorf("staging target: %w", err)
	}
	entry, err := table.Exact(publishTarget)
	if err != nil {
		return Entry{}, err
	}
	if stage.Kind != KindStage || entry.Kind != KindPublish || entry.ParentFilesystemID != mapping.ParentFilesystemID || entry.FilesystemSource != mapping.ParentFilesystemID || entry.FilesystemType != "virtiofs" || entry.BackingRelativePath != path.Join(mapping.BasePath, mapping.DirectoryName) || entry.ReadOnly != readOnly {
		return Entry{}, fmt.Errorf("publish target %q does not match requested staging graph: %w", publishTarget, ErrMountConflict)
	}
	if stage.ParentFilesystemID != entry.ParentFilesystemID || stage.FilesystemSource != entry.FilesystemSource || stage.FilesystemType != entry.FilesystemType || stage.DeviceID == "" || stage.DeviceID != entry.DeviceID || stage.BackingRelativePath != entry.BackingRelativePath {
		return Entry{}, fmt.Errorf("publish target and staging backing identity differ: %w", ErrForeignMount)
	}
	return entry, nil
}

// ValidateAbsoluteNormalizedPath rejects relative, root, and non-normalized
// mount targets before any mount-table lookup.
func ValidateAbsoluteNormalizedPath(value string) error {
	if value == "" || value == "/" || !strings.HasPrefix(value, "/") || path.Clean(value) != value {
		return fmt.Errorf("mount path %q must be absolute, normalized, and non-root", value)
	}
	return nil
}
