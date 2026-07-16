//go:build linux

package mount

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// KernelMounter is the Linux detached-mount and exact-unmount adapter backed by
// coherent /proc/self/mountinfo snapshots. Every unmount revalidates the exact
// mount generation.
type KernelMounter struct {
	config       kernelConfig
	quarantineMu sync.Mutex
	// beforeBindMove and afterBindMove are deterministic privileged-test hooks
	// around the only syscall that exposes a detached bind at its target.
	// Production constructors always leave them nil.
	beforeBindMove func(string)
	afterBindMove  func(string)
	// beforeParentMount deterministically replaces a parent target after its
	// descriptor is authenticated but before move_mount(2). Production constructors
	// always leave it nil.
	beforeParentMount func(string)
	// beforeExactUnmount is a deterministic privileged-test hook placed after
	// the mount FD has been authenticated and before the fd-anchored unmount.
	// Production constructors always leave it nil.
	beforeExactUnmount func(string)
	// afterExactUnmountRevalidation injects a race in the syscall-sized window
	// after the final public-target proof and before move_mount. Production
	// constructors always leave it nil.
	afterExactUnmountRevalidation func(string)
	// afterExactUnmountMove injects a process-crash equivalent after the exact
	// mount has left its CSI target but before it is detached from the private
	// quarantine. Production constructors always leave it nil.
	afterExactUnmountMove func(string, uint64) error
}

const (
	maxMountFDInfoBytes   = 64 * 1024
	quarantineNamePrefix  = "mnt-"
	quarantineProbeSource = ".probe-source"
	quarantineProbeTarget = ".probe-target"
)

// NewKernelMounter constructs the production Linux mount boundary.
func NewKernelMounter(parentRoot, kubeletPath, driverName string) (Interface, error) {
	config := kernelConfig{
		mountInfoPath: "/proc/self/mountinfo", parentRoot: parentRoot,
		kubeletPath: kubeletPath, quarantineRoot: DefaultQuarantineRoot, driverName: driverName,
	}
	if err := validateKernelConfig(config); err != nil {
		return nil, err
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	return &KernelMounter{config: config}, nil
}

// ReconcileQuarantines resumes only deterministic, generation-authenticated
// exact-unmounts in the private quarantine. It never scans public paths to
// infer ownership and fails closed on malformed, stacked, or foreign entries.
func (mounter *KernelMounter) ReconcileQuarantines(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mounter.quarantineMu.Lock()
	defer mounter.quarantineMu.Unlock()
	return mounter.recoverPrivateQuarantinesLocked(ctx)
}

// Snapshot parses one complete live kernel table.
func (mounter *KernelMounter) Snapshot(ctx context.Context) (table Table, returnErr error) {
	if err := ctx.Err(); err != nil {
		return Table{}, err
	}
	file, err := os.Open(mounter.config.mountInfoPath)
	if err != nil {
		return Table{}, fmt.Errorf("open mountinfo: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	raw, err := ParseMountInfo(file)
	if err != nil {
		return Table{}, err
	}
	if _, err := uniqueMountGeneration(mounter.config.parentRoot, 0); err != nil {
		return Table{}, fmt.Errorf("probe required unique mount-generation support: %w", err)
	}
	table, err = BuildTableFromMountInfo(raw, mounter.config.parentRoot, mounter.config.kubeletPath, mounter.config.driverName)
	if err != nil {
		return Table{}, err
	}
	for _, entry := range raw {
		if strings.HasPrefix(entry.MountPoint, mounter.config.quarantineRoot+"/") {
			table.Entries = append(table.Entries, Entry{
				MountID: entry.MountID, MountInfoID: entry.MountID, ParentMountID: entry.ParentMountID,
				DeviceID: entry.DeviceID, Kind: KindQuarantine, Target: entry.MountPoint,
				FilesystemType: entry.FilesystemType, FilesystemSource: entry.MountSource,
				BackingRelativePath: entry.Root, ReadOnly: mountOptionsContain(entry.MountOptions, "ro"),
			})
		}
	}
	if err := enrichUniqueMountGenerations(&table); err != nil {
		return Table{}, err
	}
	return table, nil
}

// enrichUniqueMountGenerations replaces reusable mountinfo IDs with statx
// generations for every unstacked driver target. Opening the target and
// comparing its fdinfo mount ID to the parsed snapshot closes the pathname
// replacement window; an unavailable STATX_MNT_ID_UNIQUE fails closed.
func enrichUniqueMountGenerations(table *Table) error {
	if table == nil {
		return fmt.Errorf("mount table is nil")
	}
	targetCounts := make(map[string]int, len(table.Entries))
	for _, entry := range table.Entries {
		targetCounts[entry.Target]++
	}
	for index := range table.Entries {
		entry := &table.Entries[index]
		if targetCounts[entry.Target] != 1 {
			continue
		}
		if entry.MountInfoID == 0 || entry.MountID != entry.MountInfoID {
			return fmt.Errorf("mount target %q lacks coherent mountinfo identity: %w", entry.Target, ErrForeignMount)
		}
		generation, err := uniqueMountGeneration(entry.Target, entry.MountInfoID)
		if err != nil {
			return fmt.Errorf("prove unique mount generation for %q: %w", entry.Target, err)
		}
		entry.MountID = generation
	}
	return nil
}

func uniqueMountGeneration(target string, expectedMountInfoID uint64) (generation uint64, returnErr error) {
	fd, err := openAbsoluteDirectoryNoFollow(target)
	if err != nil {
		return 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(fd)) }()
	return uniqueMountGenerationForFD(fd, expectedMountInfoID)
}

func uniqueMountGenerationForFD(fd int, expectedMountInfoID uint64) (uint64, error) {
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return 0, err
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFDIR {
		return 0, fmt.Errorf("mount target is not a no-follow directory: %w", ErrForeignMount)
	}
	mountInfoID, err := mountInfoIDForFD(fd)
	if err != nil {
		return 0, err
	}
	if expectedMountInfoID != 0 && mountInfoID != expectedMountInfoID {
		return 0, fmt.Errorf("mountinfo ID changed from %d to %d: %w", expectedMountInfoID, mountInfoID, ErrForeignMount)
	}
	var statx unix.Statx_t
	if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH|unix.AT_NO_AUTOMOUNT, unix.STATX_MNT_ID_UNIQUE, &statx); err != nil {
		return 0, fmt.Errorf("statx STATX_MNT_ID_UNIQUE is required: %w", err)
	}
	if statx.Mask&unix.STATX_MNT_ID_UNIQUE == 0 || statx.Mnt_id == 0 {
		return 0, fmt.Errorf("statx did not return STATX_MNT_ID_UNIQUE")
	}
	return statx.Mnt_id, nil
}

func mountInfoIDForFD(fd int) (mountID uint64, returnErr error) {
	file, err := os.Open("/proc/self/fdinfo/" + strconv.Itoa(fd))
	if err != nil {
		return 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	scanner := bufio.NewScanner(io.LimitReader(file, maxMountFDInfoBytes))
	for scanner.Scan() {
		key, value, present := strings.Cut(scanner.Text(), ":")
		if key != "mnt_id" || !present {
			continue
		}
		parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil || parsed == 0 {
			return 0, fmt.Errorf("fdinfo mount ID %q is invalid", strings.TrimSpace(value))
		}
		return parsed, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("fdinfo has no mount ID")
}

// MountParent mounts the exact configured File Storage ID with the fixed,
// flagless virtiofs profile.
func (mounter *KernelMounter) MountParent(ctx context.Context, parentFilesystemID, target string) error {
	return mounter.mountParentFilesystem(ctx, parentFilesystemID, target, "virtiofs", parentFilesystemID, "")
}

// mountParentFilesystem keeps the production parent-mount protocol testable
// on generic privileged Linux CI where virtiofs is unavailable. Production
// calls it only with the fixed flagless virtiofs profile above.
func (mounter *KernelMounter) mountParentFilesystem(ctx context.Context, parentFilesystemID, target, filesystemType, source, data string) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return err
	}
	if err := ValidateAbsoluteNormalizedPath(target); err != nil {
		return err
	}
	targetFD, err := openAbsoluteDirectoryNoFollow(target)
	if err != nil {
		return fmt.Errorf("open virtiofs parent target %q without following symlinks: %w", target, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	if filesystemType == "" || strings.ContainsAny(filesystemType+source+data, "\x00\r\n") {
		return fmt.Errorf("parent mount filesystem fixture is invalid")
	}
	mountFD, err := createDetachedFilesystem(filesystemType, source, data)
	if err != nil {
		if filesystemType == "virtiofs" && parentMountReadinessUnavailable(err) {
			return fmt.Errorf("create detached %s parent %q: %w: %w", filesystemType, parentFilesystemID, err, ErrMountUnavailable)
		}
		return fmt.Errorf("create detached %s parent %q: %w", filesystemType, parentFilesystemID, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(mountFD)) }()
	generation, err := uniqueMountGenerationForFD(mountFD, 0)
	if err != nil {
		return fmt.Errorf("authenticate newly mounted virtiofs parent %q at %q: %w", parentFilesystemID, target, err)
	}
	rollback := func(cause error) error {
		if rollbackErr := mounter.moveOwnedMountFDToPrivateQuarantine(mountFD, generation); rollbackErr != nil {
			return errors.Join(cause, fmt.Errorf("rollback owned parent mount generation %d: %w", generation, rollbackErr))
		}
		return cause
	}
	if mounter.beforeParentMount != nil {
		mounter.beforeParentMount(target)
	}
	if err := requireEmptyDirectoryFD(targetFD); err != nil {
		return fmt.Errorf("parent target %q is not an empty authenticated directory: %w", target, err)
	}
	if err := unix.MoveMount(mountFD, "", targetFD, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		if filesystemType == "virtiofs" && parentMountReadinessUnavailable(err) {
			return fmt.Errorf("move detached %s parent %q to %q: %w: %w", filesystemType, parentFilesystemID, target, err, ErrMountUnavailable)
		}
		return fmt.Errorf("move detached %s parent %q to %q: %w", filesystemType, parentFilesystemID, target, err)
	}
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	table, err := mounter.Snapshot(ctx)
	if err != nil {
		return rollback(fmt.Errorf("verify mounted virtiofs parent %q: %w", parentFilesystemID, err))
	}
	expectedSource := source
	if expectedSource == "" {
		expectedSource = filesystemType
	}
	created, err := validateParentFilesystem(table, target, parentFilesystemID, filesystemType, expectedSource)
	if err != nil {
		return rollback(fmt.Errorf("verify mounted virtiofs parent %q at configured target: %w", parentFilesystemID, err))
	}
	if created.MountID != generation {
		return rollback(fmt.Errorf("mounted virtiofs parent generation changed from %d to %d: %w", generation, created.MountID, ErrForeignMount))
	}
	return nil
}

func parentMountReadinessUnavailable(err error) bool {
	// These errno values are the closed set observed when an already-authorized
	// File Storage attachment has not appeared in the node's virtiofs endpoint
	// yet. Unsupported mount APIs (ENOSYS/EOPNOTSUPP/EINVAL) remain Internal and
	// are also rejected by startup preflight where they can be probed safely.
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.ENODEV) ||
		errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ETIMEDOUT)
}

func createDetachedFilesystem(filesystemType, source, data string) (mountFD int, returnErr error) {
	mountFD = -1
	filesystemFD, err := unix.Fsopen(filesystemType, unix.FSOPEN_CLOEXEC)
	if err != nil {
		return -1, err
	}
	defer func() {
		if closeErr := unix.Close(filesystemFD); closeErr != nil {
			if mountFD >= 0 {
				_ = unix.Close(mountFD)
				mountFD = -1
			}
			returnErr = errors.Join(returnErr, closeErr)
		}
	}()
	if source != "" {
		if err := unix.FsconfigSetString(filesystemFD, "source", source); err != nil {
			return -1, fmt.Errorf("configure source: %w", err)
		}
	}
	if data != "" {
		for _, option := range strings.Split(data, ",") {
			key, value, present := strings.Cut(option, "=")
			if key == "" {
				return -1, fmt.Errorf("empty filesystem option")
			}
			if present {
				if err := unix.FsconfigSetString(filesystemFD, key, value); err != nil {
					return -1, fmt.Errorf("configure option %q: %w", key, err)
				}
			} else if err := unix.FsconfigSetFlag(filesystemFD, key); err != nil {
				return -1, fmt.Errorf("configure flag %q: %w", key, err)
			}
		}
	}
	if err := unix.FsconfigCreate(filesystemFD); err != nil {
		return -1, fmt.Errorf("create filesystem context: %w", err)
	}
	mountFD, err = unix.Fsmount(filesystemFD, unix.FSMOUNT_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("create detached mount: %w", err)
	}
	return mountFD, nil
}

// Bind clones one source as a detached mount object, applies read-only before
// exposure when requested, and moves that exact object to the target. The
// returned generation comes from the owned mount FD rather than a pathname
// observation after a pathname mount, so rollback can never target a replacement.
func (mounter *KernelMounter) Bind(ctx context.Context, request BindRequest) (result BindResult, returnErr error) {
	entry := request.Entry
	if err := ctx.Err(); err != nil {
		return BindResult{Mutation: BindMutationNone}, err
	}
	if entry.Kind != KindStage && entry.Kind != KindPublish {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind kind %q is unsupported", entry.Kind)
	}
	if err := ValidateAbsoluteNormalizedPath(entry.SourcePath); err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source: %w", err)
	}
	if err := ValidateAbsoluteNormalizedPath(entry.Target); err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind target: %w", err)
	}
	if entry.SourceMountID == 0 {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source has no authenticated mount generation: %w", ErrForeignMount)
	}
	if request.Source == nil || request.Target == nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source or target descriptor is absent: %w", ErrForeignMount)
	}
	before, err := mounter.Snapshot(ctx)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, err
	}
	if _, err := before.Exact(entry.Target); err == nil {
		return BindResult{Mutation: BindMutationNone}, ErrMountConflict
	} else if !errors.Is(err, ErrNotMounted) {
		return BindResult{Mutation: BindMutationNone}, err
	}

	sourceFD, err := unix.FcntlInt(request.Source.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("duplicate authenticated bind source %q: %w", entry.SourcePath, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(sourceFD)) }()
	sourceGeneration, err := uniqueMountGenerationForFD(sourceFD, 0)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("authenticate bind source %q: %w", entry.SourcePath, err)
	}
	if sourceGeneration != entry.SourceMountID {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind source generation changed from %d to %d: %w", entry.SourceMountID, sourceGeneration, ErrForeignMount)
	}
	bindFD, err := unix.OpenTree(
		sourceFD, "",
		uint(unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC|unix.AT_EMPTY_PATH|unix.AT_NO_AUTOMOUNT),
	)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("clone bind source %q as detached mount: %w", entry.SourcePath, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(bindFD)) }()
	attribute := unix.MountAttr{Propagation: unix.MS_PRIVATE}
	attributeDescription := "private"
	if entry.ReadOnly {
		attribute.Attr_set = unix.MOUNT_ATTR_RDONLY
		attributeDescription = "private and read-only"
	}
	if err := unix.MountSetattr(bindFD, "", unix.AT_EMPTY_PATH, &attribute); err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("set detached bind %q %s: %w", entry.SourcePath, attributeDescription, err)
	}
	createdGeneration, err := uniqueMountGenerationForFD(bindFD, 0)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("authenticate detached bind source %q: %w", entry.SourcePath, err)
	}
	targetFD, err := unix.FcntlInt(request.Target.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("duplicate authenticated bind target %q: %w", entry.Target, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	if err := ctx.Err(); err != nil {
		return BindResult{Mutation: BindMutationNone}, err
	}
	if mounter.beforeBindMove != nil {
		mounter.beforeBindMove(entry.Target)
	}
	if err := requireEmptyDirectoryFD(targetFD); err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("bind target %q is not an empty authenticated directory: %w", entry.Target, err)
	}
	if err := unix.MoveMount(bindFD, "", targetFD, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		return BindResult{Mutation: BindMutationNone}, fmt.Errorf("move detached bind %q to %q: %w", entry.SourcePath, entry.Target, err)
	}
	if mounter.afterBindMove != nil {
		mounter.afterBindMove(entry.Target)
	}
	if err := ctx.Err(); err != nil {
		return mounter.rollbackOwnedBind(bindFD, entry.Target, createdGeneration, err)
	}
	after, err := mounter.Snapshot(ctx)
	if err != nil {
		return mounter.rollbackOwnedBind(bindFD, entry.Target, createdGeneration, fmt.Errorf("verify created bind mount: %w", err))
	}
	created, err := after.Exact(entry.Target)
	if err != nil {
		return mounter.rollbackOwnedBind(bindFD, entry.Target, createdGeneration, fmt.Errorf("verify created bind target: %w", err))
	}
	if created.MountID != createdGeneration || created.Kind != entry.Kind || created.FilesystemType != entry.FilesystemType || created.FilesystemSource != entry.FilesystemSource || created.ParentFilesystemID != entry.ParentFilesystemID || created.BackingRelativePath != entry.BackingRelativePath || created.ReadOnly != entry.ReadOnly {
		return mounter.rollbackOwnedBind(bindFD, entry.Target, createdGeneration, fmt.Errorf("created bind identity differs from requested backing: %w", ErrForeignMount))
	}
	return BindResult{Mutation: BindMutationCreated, MountID: createdGeneration}, nil
}

func requireEmptyDirectoryFD(directoryFD int) error {
	freshFD, err := unix.Openat(directoryFD, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(freshFD), "authenticated mount target")
	if directory == nil {
		_ = unix.Close(freshFD)
		return fmt.Errorf("construct target directory descriptor")
	}
	names, readErr := directory.Readdirnames(1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return errors.Join(readErr, closeErr)
	}
	if len(names) != 0 {
		return errors.Join(fmt.Errorf("directory contains existing entry %q", names[0]), closeErr)
	}
	return closeErr
}

func (mounter *KernelMounter) rollbackOwnedBind(bindFD int, target string, mountID uint64, bindErr error) (BindResult, error) {
	if err := mounter.moveOwnedMountFDToPrivateQuarantine(bindFD, mountID); err != nil {
		return BindResult{Mutation: BindMutationAmbiguous}, errors.Join(
			bindErr,
			fmt.Errorf("quarantine owned bind target %q ID %d after failed verification: %w", target, mountID, err),
		)
	}
	return BindResult{Mutation: BindMutationNone}, bindErr
}

// UnmountExact refuses an absent, stacked, or replaced target, then anchors the
// action to a mount FD whose reusable mountinfo ID and non-reusable statx
// generation both match the caller's snapshot. The authenticated mount object
// is moved through that FD into a private emptyDir quarantine which does not
// propagate to the host. A deterministic generation name permits recovery in
// the same container; a process/container crash destroys the private mount
// namespace, so no host-visible orphan survives. The original target pathname
// is never used after the proof.
func (mounter *KernelMounter) UnmountExact(ctx context.Context, target string, mountID uint64) (result UnmountResult, returnErr error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := ValidateAbsoluteNormalizedPath(target); err != nil {
		return result, err
	}
	table, err := mounter.Snapshot(ctx)
	if err != nil {
		return result, err
	}
	entry, err := table.Exact(target)
	if err != nil {
		return result, err
	}
	if err := rejectMountDescendants(table, target); err != nil {
		return result, err
	}
	if entry.MountID != mountID {
		return result, fmt.Errorf("mount generation changed from %d to %d: %w", mountID, entry.MountID, ErrForeignMount)
	}
	targetFD, err := openAbsoluteDirectoryNoFollow(target)
	if err != nil {
		return result, fmt.Errorf("open exact mount target %q without following symlinks: %w", target, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	targetParentFD, targetLeaf, err := openAbsoluteParentNoFollow(target)
	if err != nil {
		return result, fmt.Errorf("open exact mount target parent %q: %w", target, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetParentFD)) }()
	mountFD, err := unix.OpenTree(
		targetParentFD, targetLeaf,
		uint(unix.OPEN_TREE_CLOEXEC|unix.AT_NO_AUTOMOUNT|unix.AT_SYMLINK_NOFOLLOW),
	)
	if err != nil {
		return result, fmt.Errorf("open exact mount target %q: %w", target, err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(mountFD)) }()
	openedGeneration, err := uniqueMountGenerationForFD(mountFD, entry.MountInfoID)
	if err != nil {
		return result, fmt.Errorf("authenticate exact mount FD for %q: %w", target, err)
	}
	if openedGeneration != mountID {
		return result, fmt.Errorf("mount generation changed from %d to %d before unmount: %w", mountID, openedGeneration, ErrForeignMount)
	}
	if mounter.beforeExactUnmount != nil {
		mounter.beforeExactUnmount(target)
	}
	if err := mounter.moveMountFDToPrivateQuarantine(mountFD, target, mountID); err != nil {
		return result, fmt.Errorf("move exact target %q ID %d to private quarantine: %w", target, mountID, err)
	}
	// The destructive action was FD-anchored, but the RPC contract also
	// requires the caller-visible target to be empty afterwards. A concurrent
	// replacement is preserved and reported rather than silently treated as a
	// successful unpublish of that replacement.
	after, err := mounter.Snapshot(ctx)
	if err != nil {
		return result, fmt.Errorf("verify exact target %q after detached quarantine: %w", target, err)
	}
	if _, err := after.Exact(target); err == nil {
		return result, fmt.Errorf("mount target %q was replaced during exact unmount: %w", target, ErrForeignMount)
	} else if !errors.Is(err, ErrNotMounted) {
		return result, fmt.Errorf("mount target %q remained ambiguous after exact unmount: %w", target, err)
	}
	identity, err := underlyingTargetIdentity(target)
	if err != nil {
		return result, fmt.Errorf("authenticate underlying target %q after exact unmount: %w", target, err)
	}
	result.Target = &identity
	return result, nil
}

func underlyingTargetIdentity(target string) (identity TargetIdentity, returnErr error) {
	fd, err := openAbsoluteDirectoryNoFollow(target)
	if err != nil {
		return TargetIdentity{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(fd)) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return TargetIdentity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return TargetIdentity{}, fmt.Errorf("underlying target is not a directory")
	}
	return TargetIdentity{Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}

// moveMountFDToPrivateQuarantine removes one owned mount from its public target
// and detaches it under a fixed private emptyDir mount. The generation is part
// of the quarantine name, so an interrupted detach can be authenticated and
// resumed without guessing ownership.
func (mounter *KernelMounter) moveMountFDToPrivateQuarantine(mountFD int, originalTarget string, mountID uint64) (returnErr error) {
	return mounter.quarantineMountFD(mountFD, originalTarget, mountID)
}

// moveOwnedMountFDToPrivateQuarantine rolls back a mount whose generation was
// created and retained by this process. It intentionally does not consult the
// caller-visible pathname: that pathname may now name a replacement, while the
// owned mount FD remains the only safe action authority.
func (mounter *KernelMounter) moveOwnedMountFDToPrivateQuarantine(mountFD int, mountID uint64) error {
	return mounter.quarantineMountFD(mountFD, "", mountID)
}

func (mounter *KernelMounter) quarantineMountFD(mountFD int, originalTarget string, mountID uint64) (returnErr error) {
	mounter.quarantineMu.Lock()
	defer mounter.quarantineMu.Unlock()
	if err := mounter.recoverPrivateQuarantinesLocked(context.Background()); err != nil {
		return err
	}
	rootFD, err := unix.Open(mounter.config.quarantineRoot, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(rootFD)) }()
	name := quarantineMountName(mountID)
	if err := unix.Mkdirat(rootFD, name, 0o700); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("mount quarantine %q already exists: %w", name, ErrMountConflict)
		}
		return err
	}
	removeDirectory := true
	defer func() {
		if removeDirectory {
			returnErr = errors.Join(returnErr, unix.Unlinkat(rootFD, name, unix.AT_REMOVEDIR))
		}
	}()
	targetFD, err := unix.Openat(rootFD, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	// Revalidate the public target at the last possible point before the
	// fd-anchored move. This rejects a stack or replacement installed after the
	// original open_tree proof. move_mount remains the authority for the action;
	// this pathname read can only veto it, never select what gets detached.
	if originalTarget != "" {
		if err := mounter.requireExactTargetGeneration(context.Background(), originalTarget, mountID); err != nil {
			return err
		}
		if mounter.afterExactUnmountRevalidation != nil {
			mounter.afterExactUnmountRevalidation(originalTarget)
		}
	}
	if err := unix.MoveMount(mountFD, "", targetFD, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		// Linux rejects moving an attached mount whose kubelet ancestry
		// participates in shared propagation. That is the production Helm
		// topology, not an unsafe target. It can also reject rollback of a mount
		// that this process created and still owns by FD. In these two cases only,
		// detach the exact authenticated mount object directly through its mount
		// FD. The syscall is atomic, so it has no interrupted-quarantine state to
		// recover. Public targets repeat the complete graph proof after the failed
		// move; newly-created rollback mounts repeat their non-reusable generation
		// proof. No pathname is used to select the object being detached.
		if !errors.Is(err, unix.EINVAL) {
			return err
		}
		if originalTarget != "" {
			if proofErr := mounter.requireExactTargetGeneration(context.Background(), originalTarget, mountID); proofErr != nil {
				return proofErr
			}
		} else {
			generation, proofErr := uniqueMountGenerationForFD(mountFD, 0)
			if proofErr != nil || generation != mountID {
				return fmt.Errorf("owned rollback mount generation changed from %d to %d: %w", mountID, generation, errors.Join(proofErr, ErrForeignMount))
			}
		}
		if detachErr := unix.Unmount("/proc/self/fd/"+strconv.Itoa(mountFD), unix.MNT_DETACH); detachErr != nil {
			return fmt.Errorf("detach exact propagated mount FD: %w", detachErr)
		}
		removeDirectory = true
		return nil
	}
	// Once move_mount succeeds the directory must remain available for recovery
	// until detach is proven complete. A crash closes the container namespace;
	// an injected or ordinary error is resumed by KernelPreflight.
	removeDirectory = false
	if originalTarget != "" && mounter.afterExactUnmountMove != nil {
		if err := mounter.afterExactUnmountMove(originalTarget, mountID); err != nil {
			return err
		}
	}
	if err := mounter.detachQuarantinedMount(rootFD, name, mountID); err != nil {
		// If a mount was stacked in the syscall-sized interval after the final
		// public-target validation, move_mount can carry that child with the
		// authenticated parent. Never detach such a foreign tree. Best-effort
		// restoration moves the still-intact tree back to the now-empty original
		// directory and leaves every layer mounted.
		if originalTarget != "" && (errors.Is(err, ErrStackedMount) || errors.Is(err, ErrForeignMount)) {
			if restoreErr := mounter.restoreQuarantinedMount(mountFD, rootFD, name, originalTarget, mountID); restoreErr != nil {
				return errors.Join(err, fmt.Errorf("restore quarantined mount tree to %q: %w", originalTarget, restoreErr))
			}
			removeDirectory = true
		}
		return err
	}
	removeDirectory = true
	return nil
}

func (mounter *KernelMounter) requireExactTargetGeneration(ctx context.Context, target string, mountID uint64) error {
	return mounter.requireTargetGeneration(ctx, target, mountID, false)
}

func (mounter *KernelMounter) requireTargetGeneration(ctx context.Context, target string, mountID uint64, allowDescendants bool) error {
	table, err := mounter.Snapshot(ctx)
	if err != nil {
		return err
	}
	entry, err := table.Exact(target)
	if err != nil {
		return err
	}
	if !allowDescendants {
		if err := rejectMountDescendants(table, target); err != nil {
			return err
		}
	}
	if entry.MountID != mountID {
		return fmt.Errorf("mount generation changed from %d to %d: %w", mountID, entry.MountID, ErrForeignMount)
	}
	return nil
}

func (mounter *KernelMounter) restoreQuarantinedMount(mountFD, rootFD int, name, originalTarget string, mountID uint64) (returnErr error) {
	generation, err := uniqueMountGenerationForFD(mountFD, 0)
	if err != nil || generation != mountID {
		return fmt.Errorf("quarantined source generation changed from %d to %d: %w", mountID, generation, errors.Join(err, ErrForeignMount))
	}
	layers, descendants, err := mounter.mountTopologyAt(originalTarget)
	if err != nil {
		return err
	}
	if layers != 0 || descendants != 0 {
		return fmt.Errorf("original target gained %d replacement layers and %d descendants before restore: %w", layers, descendants, ErrForeignMount)
	}
	targetFD, err := openAbsoluteDirectoryNoFollow(originalTarget)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	if layers, descendants, err = mounter.mountTopologyAt(originalTarget); err != nil {
		return err
	} else if layers != 0 || descendants != 0 {
		return fmt.Errorf("original target gained %d replacement layers and %d descendants during restore: %w", layers, descendants, ErrForeignMount)
	}
	if err := unix.MoveMount(mountFD, "", targetFD, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		return err
	}
	if err := mounter.requireTargetGeneration(context.Background(), originalTarget, mountID, true); err != nil {
		return fmt.Errorf("verify restored exact target: %w", err)
	}
	quarantineFD, err := unix.Openat(rootFD, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(quarantineFD)) }()
	if err := requireSameMount(rootFD, quarantineFD); err != nil {
		return fmt.Errorf("quarantine remained mounted after restore: %w", err)
	}
	restoredGeneration, err := uniqueMountGenerationForFD(mountFD, 0)
	if err != nil || restoredGeneration != mountID {
		return fmt.Errorf("restored mount generation changed from %d to %d: %w", mountID, restoredGeneration, errors.Join(err, ErrForeignMount))
	}
	return nil
}

func quarantineMountName(mountID uint64) string {
	return fmt.Sprintf("%s%016x", quarantineNamePrefix, mountID)
}

// KernelPreflight proves the mount API quarantine and recovers any interrupted
// detach left in this still-running container namespace. The emptyDir itself is
// private to the container, so a container crash destroys both the namespace
// and any mount that had already moved into it.
func (mounter *KernelMounter) KernelPreflight(ctx context.Context) error {
	if err := mounter.ReconcileQuarantines(ctx); err != nil {
		return err
	}
	return mounter.probeMountAPI(ctx)
}

func (mounter *KernelMounter) recoverPrivateQuarantinesLocked(ctx context.Context) (returnErr error) {
	if err := mounter.validatePrivateQuarantine(); err != nil {
		return err
	}
	rootFD, err := unix.Open(mounter.config.quarantineRoot, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(rootFD)) }()
	entries, err := os.ReadDir("/proc/self/fd/" + strconv.Itoa(rootFD))
	if err != nil {
		return fmt.Errorf("list private mount quarantine: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Name() == quarantineProbeSource || entry.Name() == quarantineProbeTarget {
			targetFD, openErr := unix.Openat(rootFD, entry.Name(), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				return openErr
			}
			plainErr := requireSameMount(rootFD, targetFD)
			closeErr := unix.Close(targetFD)
			if plainErr != nil {
				return fmt.Errorf("kernel probe directory %q retained a mount: %w", entry.Name(), plainErr)
			}
			if closeErr != nil {
				return closeErr
			}
			if err := unix.Unlinkat(rootFD, entry.Name(), unix.AT_REMOVEDIR); err != nil {
				return err
			}
			continue
		}
		mountID, err := parseQuarantineMountName(entry.Name())
		if err != nil || !entry.IsDir() {
			if err == nil {
				err = fmt.Errorf("entry is not a directory")
			}
			return fmt.Errorf("private mount quarantine contains unrecognized entry %q: %w", entry.Name(), err)
		}
		targetFD, err := unix.Openat(rootFD, entry.Name(), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		plainErr := requireSameMount(rootFD, targetFD)
		if closeErr := unix.Close(targetFD); closeErr != nil {
			return closeErr
		}
		if plainErr != nil {
			if err := mounter.detachQuarantinedMount(rootFD, entry.Name(), mountID); err != nil {
				return fmt.Errorf("recover quarantined mount %q: %w", entry.Name(), err)
			}
		}
		if err := unix.Unlinkat(rootFD, entry.Name(), unix.AT_REMOVEDIR); err != nil {
			return fmt.Errorf("remove recovered quarantine %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func (mounter *KernelMounter) probeMountAPI(ctx context.Context) (returnErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	rootFD, err := unix.Open(mounter.config.quarantineRoot, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(rootFD)) }()
	filesystemProbeFD, err := createDetachedFilesystem("tmpfs", "", "size=4k,mode=0700")
	if err != nil {
		return fmt.Errorf("probe fsopen/fsconfig/fsmount support: %w", err)
	}
	if err := unix.Close(filesystemProbeFD); err != nil {
		return fmt.Errorf("close detached filesystem probe: %w", err)
	}
	created := make([]string, 0, 2)
	defer func() {
		for index := len(created) - 1; index >= 0; index-- {
			returnErr = errors.Join(returnErr, unix.Unlinkat(rootFD, created[index], unix.AT_REMOVEDIR))
		}
	}()
	for _, name := range []string{quarantineProbeSource, quarantineProbeTarget} {
		if err := unix.Mkdirat(rootFD, name, 0o700); err != nil {
			return fmt.Errorf("create kernel mount probe directory %q: %w", name, err)
		}
		created = append(created, name)
	}
	sourceFD, err := unix.Openat(rootFD, quarantineProbeSource, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(sourceFD)) }()
	probeFD, err := unix.OpenTree(sourceFD, "", uint(unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC|unix.AT_EMPTY_PATH|unix.AT_NO_AUTOMOUNT))
	if err != nil {
		return fmt.Errorf("probe open_tree detached bind support: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(probeFD)) }()
	attribute := unix.MountAttr{Attr_set: unix.MOUNT_ATTR_RDONLY, Propagation: unix.MS_PRIVATE}
	if err := unix.MountSetattr(probeFD, "", unix.AT_EMPTY_PATH, &attribute); err != nil {
		return fmt.Errorf("probe mount_setattr read-only support: %w", err)
	}
	generation, err := uniqueMountGenerationForFD(probeFD, 0)
	if err != nil {
		return fmt.Errorf("probe detached mount generation: %w", err)
	}
	targetFD, err := unix.Openat(rootFD, quarantineProbeTarget, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(targetFD)) }()
	if err := unix.MoveMount(probeFD, "", targetFD, "", unix.MOVE_MOUNT_F_EMPTY_PATH|unix.MOVE_MOUNT_T_EMPTY_PATH); err != nil {
		return fmt.Errorf("probe move_mount support: %w", err)
	}
	exactFD, err := unix.OpenTree(rootFD, quarantineProbeTarget, uint(unix.OPEN_TREE_CLOEXEC|unix.AT_NO_AUTOMOUNT|unix.AT_SYMLINK_NOFOLLOW))
	if err != nil {
		return fmt.Errorf("open installed kernel probe mount: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(exactFD)) }()
	installedGeneration, err := uniqueMountGenerationForFD(exactFD, 0)
	if err != nil || installedGeneration != generation {
		return fmt.Errorf("kernel probe mount generation changed from %d to %d: %w", generation, installedGeneration, errors.Join(err, ErrForeignMount))
	}
	if err := unix.Unmount("/proc/self/fd/"+strconv.Itoa(exactFD), unix.MNT_DETACH); err != nil {
		return fmt.Errorf("detach kernel probe mount: %w", err)
	}
	if err := requireSameMount(rootFD, targetFD); err != nil {
		return fmt.Errorf("kernel probe target remained mounted: %w", err)
	}
	return nil
}

func (mounter *KernelMounter) validatePrivateQuarantine() (returnErr error) {
	file, err := os.Open(mounter.config.mountInfoPath)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	entries, err := ParseMountInfo(file)
	if err != nil {
		return err
	}
	entry, err := exactStartupMount(entries, "mount quarantine root", mounter.config.quarantineRoot, false)
	if err != nil {
		return err
	}
	for _, option := range entry.Optional {
		if strings.HasPrefix(option, "shared:") || strings.HasPrefix(option, "master:") || strings.HasPrefix(option, "propagate_from:") {
			return fmt.Errorf("mount quarantine root %q is not private", mounter.config.quarantineRoot)
		}
	}
	return nil
}

func (mounter *KernelMounter) detachQuarantinedMount(rootFD int, name string, mountID uint64) (returnErr error) {
	quarantineTarget := mounter.config.quarantineRoot + "/" + name
	layers, descendants, err := mounter.mountTopologyAt(quarantineTarget)
	if err != nil {
		return err
	}
	if layers != 1 {
		return fmt.Errorf("quarantine target %q has %d mount layers: %w", quarantineTarget, layers, ErrStackedMount)
	}
	if descendants != 0 {
		return fmt.Errorf("quarantine target %q contains %d foreign descendant mounts: %w", quarantineTarget, descendants, ErrForeignMount)
	}
	exactFD, err := unix.OpenTree(rootFD, name, uint(unix.OPEN_TREE_CLOEXEC|unix.AT_NO_AUTOMOUNT|unix.AT_SYMLINK_NOFOLLOW))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(exactFD)) }()
	generation, err := uniqueMountGenerationForFD(exactFD, 0)
	if err != nil {
		return err
	}
	if generation != mountID {
		return fmt.Errorf("quarantine generation %d differs from encoded ID %d: %w", generation, mountID, ErrForeignMount)
	}
	if err := unix.Unmount("/proc/self/fd/"+strconv.Itoa(exactFD), unix.MNT_DETACH); err != nil {
		return err
	}
	underlyingFD, err := unix.Openat(rootFD, name, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, unix.Close(underlyingFD)) }()
	return requireSameMount(rootFD, underlyingFD)
}

func (mounter *KernelMounter) mountTopologyAt(target string) (layers, descendants int, returnErr error) {
	file, err := os.Open(mounter.config.mountInfoPath)
	if err != nil {
		return 0, 0, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	entries, err := ParseMountInfo(file)
	if err != nil {
		return 0, 0, err
	}
	for _, entry := range entries {
		if entry.MountPoint == target {
			layers++
		} else if strings.HasPrefix(entry.MountPoint, target+"/") {
			descendants++
		}
	}
	return layers, descendants, nil
}

func rejectMountDescendants(table Table, target string) error {
	for _, entry := range table.Entries {
		if strings.HasPrefix(entry.Target, target+"/") {
			return fmt.Errorf("mount target %q has foreign descendant %q: %w", target, entry.Target, ErrForeignMount)
		}
	}
	return nil
}

// openAbsoluteDirectoryNoFollow resolves every component from the filesystem
// root with O_NOFOLLOW. Protecting only the final component would still allow
// an intermediate symlink to retarget a mount operation.
func openAbsoluteDirectoryNoFollow(absolute string) (int, error) {
	if err := ValidateAbsoluteNormalizedPath(absolute); err != nil {
		return -1, err
	}
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(absolute, "/"), "/") {
		next, openErr := unix.Openat(current, component, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(current)
		if openErr != nil {
			return -1, openErr
		}
		current = next
	}
	return current, nil
}

// openAbsoluteParentNoFollow resolves every parent component without following
// symlinks and returns the final leaf separately. A dirfd-relative mount
// syscall can then resolve the current leaf atomically instead of acting on an
// inode that may already have been renamed away from the CSI pathname.
func openAbsoluteParentNoFollow(absolute string) (int, string, error) {
	if err := ValidateAbsoluteNormalizedPath(absolute); err != nil {
		return -1, "", err
	}
	components := strings.Split(strings.TrimPrefix(absolute, "/"), "/")
	leaf := components[len(components)-1]
	current, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", err
	}
	for _, component := range components[:len(components)-1] {
		next, openErr := unix.Openat(current, component, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		closeErr := unix.Close(current)
		if openErr != nil || closeErr != nil {
			return -1, "", errors.Join(openErr, closeErr)
		}
		current = next
	}
	return current, leaf, nil
}

func parseQuarantineMountName(name string) (uint64, error) {
	if !strings.HasPrefix(name, quarantineNamePrefix) || len(name) != len(quarantineNamePrefix)+16 {
		return 0, fmt.Errorf("name does not match %s<16-hex-digits>", quarantineNamePrefix)
	}
	mountID, err := strconv.ParseUint(strings.TrimPrefix(name, quarantineNamePrefix), 16, 64)
	if err != nil || mountID == 0 || quarantineMountName(mountID) != name {
		return 0, fmt.Errorf("mount generation is invalid")
	}
	return mountID, nil
}

func requireSameMount(parentFD, childFD int) error {
	parentMountID, err := mountInfoIDForFD(parentFD)
	if err != nil {
		return err
	}
	childMountID, err := mountInfoIDForFD(childFD)
	if err != nil {
		return err
	}
	if parentMountID != childMountID {
		return fmt.Errorf("mountinfo ID changed from %d to %d: %w", parentMountID, childMountID, ErrForeignMount)
	}
	return nil
}
