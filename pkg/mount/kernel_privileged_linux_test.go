//go:build linux

package mount

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const privilegedMountTestEnvironment = "SFS_SUBDIR_PRIVILEGED_LINUX_TEST"

// TestPrivilegedKernelMountNamespace runs its assertions in a disposable mount
// namespace. The ordinary unit suite skips it unless CI or a developer opts in
// explicitly; the privileged CI job runs it as root and treats any namespace or
// mount failure as a test failure.
func TestPrivilegedKernelMountNamespace(t *testing.T) {
	if os.Getenv("SFS_SUBDIR_MOUNT_HELPER") == "1" {
		runPrivilegedKernelMountAssertions(t)
		return
	}
	if os.Getenv(privilegedMountTestEnvironment) != "1" {
		t.Skip("set SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 and run as root")
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPrivilegedKernelMountNamespace$", "-test.v")
	command.Env = append(os.Environ(), "SFS_SUBDIR_MOUNT_HELPER=1")
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("privileged mount namespace helper failed: %v\n%s", err, output)
	}
}

func runPrivilegedKernelMountAssertions(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("privileged mount test helper must run as root")
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make disposable mount namespace private: %v", err)
	}
	root := t.TempDir()
	parentRoot := filepath.Join(root, "parents")
	kubeletRoot := filepath.Join(root, "kubelet")
	quarantineSource := filepath.Join(root, "quarantine-source")
	quarantineRoot := filepath.Join(root, "quarantine")
	parentID := "11111111-1111-4111-8111-111111111111"
	driverName := "file-storage-subdir.csi.urlab.ai"
	parentTarget := filepath.Join(parentRoot, parentID)
	backing := filepath.Join(parentTarget, "kubernetes-volumes", "tenant--claim--0123456789ab")
	stageTarget := filepath.Join(kubeletRoot, "plugins", "kubernetes.io", "csi", driverName, "volume-a", "globalmount")
	publishTarget := filepath.Join(kubeletRoot, "pods", "pod-a", "volumes", "kubernetes.io~csi", "pv-a", "mount")
	for _, directory := range []string{parentTarget, stageTarget, publishTarget, quarantineSource, quarantineRoot} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatalf("create mount test directory %q: %v", directory, err)
		}
	}
	// Model the chart's Bidirectional parent-root hostPath. Attached child
	// mounts below a shared anchor cannot always be moved into a private
	// quarantine, so owned rollback must safely fall back to the exact mount FD.
	if err := syscall.Mount(parentRoot, parentRoot, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		t.Fatalf("bind parent root as propagation anchor: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(parentRoot, syscall.MNT_DETACH) })
	if err := syscall.Mount("", parentRoot, "", syscall.MS_SHARED, ""); err != nil {
		t.Fatalf("make parent root shared: %v", err)
	}
	// Model the Kapsule/containerd emptyDir shape observed by the real E2E:
	// the dedicated bind is non-propagating into the host but initially remains
	// a slave of its runtime mount. KernelPreflight must convert this exact mount
	// to private before it is ever used as unmount authority.
	if err := syscall.Mount("mount-quarantine", quarantineSource, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
		t.Fatalf("mount quarantine source: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(quarantineSource, syscall.MNT_DETACH) })
	if err := syscall.Mount("", quarantineSource, "", syscall.MS_SHARED, ""); err != nil {
		t.Fatalf("make quarantine source shared: %v", err)
	}
	if err := syscall.Mount(quarantineSource, quarantineRoot, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind quarantine root: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(quarantineRoot, syscall.MNT_DETACH) })
	if err := syscall.Mount("", quarantineRoot, "", syscall.MS_SLAVE, ""); err != nil {
		t.Fatalf("make quarantine root a runtime slave mount: %v", err)
	}
	if err := syscall.Mount(parentID, parentTarget, "tmpfs", 0, "size=4m,mode=0700"); err != nil {
		t.Fatalf("mount disposable parent: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(parentTarget, syscall.MNT_DETACH) })
	if err := os.MkdirAll(backing, 0o770); err != nil {
		t.Fatalf("create logical backing directory: %v", err)
	}
	if err := syscall.Mount(backing, stageTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind logical directory to staging: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(stageTarget, syscall.MNT_DETACH) })
	if err := syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind staging to publish target: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(publishTarget, syscall.MNT_DETACH) })

	configured, err := NewKernelMounter(parentRoot, kubeletRoot, driverName)
	if err != nil {
		t.Fatalf("NewKernelMounter() error = %v", err)
	}
	kernel := configured.(*KernelMounter)
	kernel.config.quarantineRoot = quarantineRoot
	before, err := kernel.quarantineMountEntry()
	if err != nil || quarantineMountIsPrivate(before) {
		t.Fatalf("Kapsule-shaped quarantine before preflight = %#v, %v", before, err)
	}
	beforeGeneration, err := uniqueMountGeneration(quarantineRoot, before.MountID)
	if err != nil {
		t.Fatalf("quarantine generation before preflight: %v", err)
	}
	if err := kernel.KernelPreflight(context.Background()); err != nil {
		t.Fatalf("KernelPreflight() error = %v", err)
	}
	after, err := kernel.quarantineMountEntry()
	if err != nil || !quarantineMountIsPrivate(after) || after.MountID != before.MountID {
		t.Fatalf("private quarantine after preflight = %#v, %v", after, err)
	}
	afterGeneration, err := uniqueMountGeneration(quarantineRoot, after.MountID)
	if err != nil || afterGeneration != beforeGeneration {
		t.Fatalf("quarantine generation before/after preflight = %d/%d, %v", beforeGeneration, afterGeneration, err)
	}
	// MountParent acts through the opened target inode. If that inode is
	// renamed before move_mount(2), post-validation must rollback only the newly
	// owned generation instead of leaking an invisible live parent mount.
	parentRaceID := "22222222-2222-4222-8222-222222222222"
	parentRaceTarget := filepath.Join(parentRoot, parentRaceID)
	parentRaceRelocated := parentRaceTarget + "-relocated"
	if err := os.Mkdir(parentRaceTarget, 0o700); err != nil {
		t.Fatalf("create parent-race target: %v", err)
	}
	var parentRaceErr error
	kernel.beforeParentMount = func(target string) {
		if target != parentRaceTarget {
			return
		}
		if parentRaceErr = os.Rename(parentRaceTarget, parentRaceRelocated); parentRaceErr == nil {
			parentRaceErr = os.Mkdir(parentRaceTarget, 0o700)
		}
	}
	err = kernel.mountParentFilesystem(context.Background(), parentRaceID, parentRaceTarget, "tmpfs", "", "size=1m,mode=0700")
	kernel.beforeParentMount = nil
	if err == nil || parentRaceErr != nil {
		t.Fatalf("mountParentFilesystem(renamed target) error = %v, rename=%v", err, parentRaceErr)
	}
	parentRaceLayers, parentRaceDescendants, topologyErr := kernel.mountTopologyAt(parentRaceTarget)
	if topologyErr != nil || parentRaceLayers != 0 || parentRaceDescendants != 0 {
		t.Fatalf("replacement parent target topology = %d/%d, %v", parentRaceLayers, parentRaceDescendants, topologyErr)
	}
	parentRaceLayers, parentRaceDescendants, topologyErr = kernel.mountTopologyAt(parentRaceRelocated)
	if topologyErr != nil || parentRaceLayers != 0 || parentRaceDescendants != 0 {
		t.Fatalf("relocated parent target leaked mount topology = %d/%d, %v", parentRaceLayers, parentRaceDescendants, topologyErr)
	}
	table, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	parent, err := table.Exact(parentTarget)
	if err != nil || parent.Kind != KindParent || parent.ParentFilesystemID != parentID || parent.BackingRelativePath != "/" {
		t.Fatalf("parent mount identity/error = %#v/%v", parent, err)
	}
	stage, err := table.Exact(stageTarget)
	if err != nil || stage.Kind != KindStage || stage.ParentFilesystemID != parentID || stage.BackingRelativePath != "/kubernetes-volumes/tenant--claim--0123456789ab" {
		t.Fatalf("stage mount identity/error = %#v/%v", stage, err)
	}
	publish, err := table.Exact(publishTarget)
	if err != nil || publish.Kind != KindPublish || publish.ParentFilesystemID != parentID || publish.BackingRelativePath != stage.BackingRelativePath || publish.DeviceID != stage.DeviceID {
		t.Fatalf("publish mount identity/error = %#v/%v", publish, err)
	}
	// The disposable filesystem is tmpfs, not virtiofs. The production graph
	// validator must therefore reject it even though the real kernel bind roots
	// and devices agree.
	mapping := volumeMappingForPrivilegedTest(parentID)
	if _, err := ValidateStage(table, parentTarget, stageTarget, mapping, privilegedMountCapability()); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("ValidateStage(non-virtiofs) error = %v, want ErrMountConflict", err)
	}

	oldPublishID := publish.MountID
	if err := syscall.Unmount(publishTarget, 0); err != nil {
		t.Fatalf("replace publish mount, unmount old: %v", err)
	}
	if err := syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("replace publish mount, bind new: %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), publishTarget, oldPublishID); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("UnmountExact(stale ID) error = %v, want ErrForeignMount", err)
	}
	if _, err := kernel.Snapshot(context.Background()); err != nil {
		t.Fatalf("stale-ID rejection damaged mount table: %v", err)
	}

	if err := syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("create stacked publish mount: %v", err)
	}
	stacked, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(stacked) error = %v", err)
	}
	if _, err := stacked.Exact(publishTarget); !errors.Is(err, ErrStackedMount) {
		t.Fatalf("Exact(stacked) error = %v, want ErrStackedMount", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), publishTarget, oldPublishID); !errors.Is(err, ErrStackedMount) {
		t.Fatalf("UnmountExact(stacked) error = %v, want ErrStackedMount", err)
	}
	if got := len(stacked.AtTarget(publishTarget)); got != 2 {
		t.Fatalf("stacked target layers = %d, want 2", got)
	}
	if err := syscall.Unmount(publishTarget, 0); err != nil {
		t.Fatalf("remove top stacked mount: %v", err)
	}
	remaining, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(after stack cleanup) error = %v", err)
	}
	exact, err := remaining.Exact(publishTarget)
	if err != nil {
		t.Fatalf("Exact(remaining publish) error = %v", err)
	}
	// Add a genuinely stacked top mount after UnmountExact authenticates the
	// previously visible generation. The FD-anchored action may either detach
	// that old covered object or fail closed, but it must never detach the new
	// top layer installed by the concurrent actor.
	var stackRaceErr error
	kernel.afterExactUnmountRevalidation = func(target string) {
		if target == publishTarget {
			stackRaceErr = syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, "")
		}
	}
	if _, err := kernel.UnmountExact(context.Background(), publishTarget, exact.MountID); err == nil {
		t.Fatal("UnmountExact(concurrent stack) error = nil")
	}
	kernel.afterExactUnmountRevalidation = nil
	if stackRaceErr != nil {
		t.Fatalf("install concurrent top mount: %v", stackRaceErr)
	}
	stackRaceTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(after exact-unmount stack race) error = %v", err)
	}
	stackRaceLayers := stackRaceTable.AtTarget(publishTarget)
	if len(stackRaceLayers) == 0 || stackRaceLayers[len(stackRaceLayers)-1].MountID == exact.MountID {
		t.Fatalf("concurrent top mount was removed: %#v", stackRaceLayers)
	}
	for len(stackRaceLayers) > 0 {
		if err := syscall.Unmount(publishTarget, syscall.MNT_DETACH); err != nil {
			t.Fatalf("clean stack-race layer: %v", err)
		}
		stackRaceTable, err = kernel.Snapshot(context.Background())
		if err != nil {
			t.Fatalf("Snapshot(clean stack-race layer) error = %v", err)
		}
		stackRaceLayers = stackRaceTable.AtTarget(publishTarget)
	}
	if err := syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("restore publish mount after stack race: %v", err)
	}
	remaining, err = kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(restored publish) error = %v", err)
	}
	exact, err = remaining.Exact(publishTarget)
	if err != nil {
		t.Fatalf("Exact(restored publish) error = %v", err)
	}
	// Inject a new top mount only after UnmountExact has opened and authenticated
	// the original mount FD. The fd-anchored action must detach the old layer and
	// leave the replacement untouched; a pathname umount would remove the new
	// top layer instead.
	var replacementErr error
	kernel.beforeExactUnmount = func(target string) {
		if target == publishTarget {
			if replacementErr = syscall.Unmount(publishTarget, syscall.MNT_DETACH); replacementErr == nil {
				replacementErr = syscall.Mount(stageTarget, publishTarget, "", syscall.MS_BIND, "")
			}
		}
	}
	if _, err := kernel.UnmountExact(context.Background(), publishTarget, exact.MountID); err == nil {
		t.Fatal("UnmountExact(concurrent replacement) error = nil")
	}
	kernel.beforeExactUnmount = nil
	if replacementErr != nil {
		t.Fatalf("install replacement during exact unmount: %v", replacementErr)
	}
	afterRace, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(after exact-unmount race) error = %v", err)
	}
	replacement, err := afterRace.Exact(publishTarget)
	if err != nil {
		t.Fatalf("replacement mount was not preserved: %v", err)
	}
	if replacement.MountID == exact.MountID {
		t.Fatal("replacement reused the old non-reusable mount generation")
	}
	unmountedReplacement, err := kernel.UnmountExact(context.Background(), publishTarget, replacement.MountID)
	if err != nil {
		t.Fatalf("UnmountExact(replacement publish) error = %v", err)
	}
	if unmountedReplacement.Target == nil || unmountedReplacement.Target.Inode == 0 {
		t.Fatalf("UnmountExact did not return underlying target identity: %#v", unmountedReplacement)
	}

	// A crash after move_mount must leave only a deterministic entry inside the
	// private emptyDir. The same public reconciliation operation used by normal
	// CSI/admin retries authenticates its encoded generation, detaches it, and
	// removes the directory before a retry can report success.
	crashTarget := filepath.Join(kubeletRoot, "pods", "pod-crash", "volumes", "kubernetes.io~csi", "pv-crash", "mount")
	if err := os.MkdirAll(crashTarget, 0o700); err != nil {
		t.Fatalf("create crash target: %v", err)
	}
	if err := syscall.Mount(stageTarget, crashTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind crash target: %v", err)
	}
	crashTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(crash target) error = %v", err)
	}
	crashMount, err := crashTable.Exact(crashTarget)
	if err != nil {
		t.Fatalf("Exact(crash target) error = %v", err)
	}
	injectedCrash := errors.New("injected crash after move_mount")
	kernel.afterExactUnmountMove = func(string, uint64) error { return injectedCrash }
	if _, err := kernel.UnmountExact(context.Background(), crashTarget, crashMount.MountID); !errors.Is(err, injectedCrash) {
		t.Fatalf("UnmountExact(injected crash) error = %v", err)
	}
	kernel.afterExactUnmountMove = nil
	if entries, err := os.ReadDir(quarantineRoot); err != nil || len(entries) != 1 {
		t.Fatalf("quarantine after injected crash = %v, %v", entries, err)
	}
	if err := kernel.ReconcileQuarantines(context.Background()); err != nil {
		t.Fatalf("ReconcileQuarantines(recover quarantine) error = %v", err)
	}
	if entries, err := os.ReadDir(quarantineRoot); err != nil || len(entries) != 0 {
		t.Fatalf("quarantine after recovery = %v, %v", entries, err)
	}

	// A foreign descendant is part of a different mount object even though it
	// sits below the CSI target. It must veto exact-unmount both when already
	// present and when injected in the final proof-to-move interval.
	nestedTarget := filepath.Join(kubeletRoot, "pods", "pod-nested", "volumes", "kubernetes.io~csi", "pv-nested", "mount")
	if err := os.MkdirAll(nestedTarget, 0o700); err != nil {
		t.Fatalf("create nested target: %v", err)
	}
	if err := syscall.Mount(stageTarget, nestedTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind nested target: %v", err)
	}
	nestedChild := filepath.Join(nestedTarget, "foreign-child")
	if err := os.Mkdir(nestedChild, 0o700); err != nil {
		t.Fatalf("create nested child: %v", err)
	}
	if err := syscall.Mount("foreign-child", nestedChild, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
		t.Fatalf("mount foreign descendant: %v", err)
	}
	nestedTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(nested target) error = %v", err)
	}
	nestedMount, err := nestedTable.Exact(nestedTarget)
	if err != nil {
		t.Fatalf("Exact(nested target) error = %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), nestedTarget, nestedMount.MountID); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("UnmountExact(existing descendant) error = %v, want ErrForeignMount", err)
	}
	if err := os.WriteFile(filepath.Join(nestedChild, "must-survive"), []byte("safe"), 0o600); err != nil {
		t.Fatalf("foreign descendant was detached: %v", err)
	}
	if err := syscall.Unmount(nestedChild, syscall.MNT_DETACH); err != nil {
		t.Fatalf("remove existing foreign descendant: %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), nestedTarget, nestedMount.MountID); err != nil {
		t.Fatalf("UnmountExact(after descendant removal) error = %v", err)
	}

	descendantRaceTarget := filepath.Join(kubeletRoot, "pods", "pod-descendant-race", "volumes", "kubernetes.io~csi", "pv-descendant-race", "mount")
	if err := os.MkdirAll(descendantRaceTarget, 0o700); err != nil {
		t.Fatalf("create descendant-race target: %v", err)
	}
	if err := syscall.Mount(stageTarget, descendantRaceTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("bind descendant-race target: %v", err)
	}
	descendantRaceChild := filepath.Join(descendantRaceTarget, "foreign-race-child")
	if err := os.Mkdir(descendantRaceChild, 0o700); err != nil {
		t.Fatalf("create descendant-race child: %v", err)
	}
	descendantRaceTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(descendant-race target) error = %v", err)
	}
	descendantRaceMount, err := descendantRaceTable.Exact(descendantRaceTarget)
	if err != nil {
		t.Fatalf("Exact(descendant-race target) error = %v", err)
	}
	var descendantRaceErr error
	kernel.afterExactUnmountRevalidation = func(target string) {
		if target == descendantRaceTarget {
			descendantRaceErr = syscall.Mount("foreign-race-child", descendantRaceChild, "tmpfs", 0, "size=1m,mode=0700")
		}
	}
	if _, err := kernel.UnmountExact(context.Background(), descendantRaceTarget, descendantRaceMount.MountID); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("UnmountExact(concurrent descendant) error = %v, want ErrForeignMount", err)
	}
	kernel.afterExactUnmountRevalidation = nil
	if descendantRaceErr != nil {
		t.Fatalf("install concurrent foreign descendant: %v", descendantRaceErr)
	}
	restoredRaceTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(restored descendant-race target) error = %v", err)
	}
	restoredRaceMount, err := restoredRaceTable.Exact(descendantRaceTarget)
	if err != nil || restoredRaceMount.MountID != descendantRaceMount.MountID {
		t.Fatalf("restored descendant-race mount = %#v, %v", restoredRaceMount, err)
	}
	if err := os.WriteFile(filepath.Join(descendantRaceChild, "must-survive"), []byte("safe"), 0o600); err != nil {
		t.Fatalf("concurrent foreign descendant was detached: %v", err)
	}
	if err := syscall.Unmount(descendantRaceChild, syscall.MNT_DETACH); err != nil {
		t.Fatalf("remove concurrent foreign descendant: %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), descendantRaceTarget, descendantRaceMount.MountID); err != nil {
		t.Fatalf("UnmountExact(restored target) error = %v", err)
	}

	// Replace the staging pathname after KernelMounter has authenticated and
	// cloned its exact generation but before move_mount exposes the clone. The
	// new target must still contain the original logical root, proving the bind
	// does not reopen a concurrently replaced source pathname.
	sourceRaceTarget := filepath.Join(kubeletRoot, "pods", "pod-source-race", "volumes", "kubernetes.io~csi", "pv-source-race", "mount")
	alternateBacking := filepath.Join(parentTarget, "kubernetes-volumes", "foreign--claim--fedcba987654")
	for _, directory := range []string{sourceRaceTarget, alternateBacking} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatalf("create source-race directory %q: %v", directory, err)
		}
	}
	var sourceReplacementErr error
	clonedOriginalSource := false
	kernel.beforeBindMove = func(target string) {
		if target == sourceRaceTarget {
			if sourceReplacementErr = syscall.Unmount(stageTarget, syscall.MNT_DETACH); sourceReplacementErr == nil {
				sourceReplacementErr = syscall.Mount(alternateBacking, stageTarget, "", syscall.MS_BIND, "")
			}
		}
	}
	kernel.afterBindMove = func(target string) {
		if target != sourceRaceTarget || sourceReplacementErr != nil {
			return
		}
		observed, snapshotErr := kernel.Snapshot(context.Background())
		if snapshotErr != nil {
			sourceReplacementErr = snapshotErr
			return
		}
		installed, exactErr := observed.Exact(target)
		if exactErr != nil {
			sourceReplacementErr = exactErr
			return
		}
		clonedOriginalSource = installed.BackingRelativePath == "/kubernetes-volumes/tenant--claim--0123456789ab"
	}
	bindResult, bindErr := kernel.Bind(context.Background(), privilegedBindRequest(t, Entry{
		Kind: KindPublish, Target: sourceRaceTarget, SourcePath: stageTarget,
		SourceMountID:  stage.MountID,
		FilesystemType: "virtiofs", FilesystemSource: parentID, ParentFilesystemID: parentID,
		BackingRelativePath: "/kubernetes-volumes/tenant--claim--0123456789ab",
		AccessMode:          volume.AccessModeMultiNodeMultiWriter,
	}))
	kernel.beforeBindMove = nil
	kernel.afterBindMove = nil
	if bindErr == nil || bindResult.Mutation != BindMutationNone || sourceReplacementErr != nil || !clonedOriginalSource {
		t.Fatalf("Bind(concurrent source replacement) = %#v, %v, replacement=%v, clonedOriginal=%v", bindResult, bindErr, sourceReplacementErr, clonedOriginalSource)
	}
	if err := syscall.Unmount(stageTarget, syscall.MNT_DETACH); err != nil {
		t.Fatalf("remove source replacement: %v", err)
	}
	if err := syscall.Mount(backing, stageTarget, "", syscall.MS_BIND, ""); err != nil {
		t.Fatalf("restore staging source: %v", err)
	}
	afterRace, err = kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(restored staging source) error = %v", err)
	}
	stage, err = afterRace.Exact(stageTarget)
	if err != nil {
		t.Fatalf("Exact(restored staging source) error = %v", err)
	}

	// Replace the bind after move_mount but before KernelMounter attributes the
	// target. The owned bind FD is rolled back through the private quarantine;
	// the replacement remains untouched and can never become rollback evidence.
	bindRaceTarget := filepath.Join(kubeletRoot, "pods", "pod-bind-race", "volumes", "kubernetes.io~csi", "pv-bind-race", "mount")
	if err := os.MkdirAll(bindRaceTarget, 0o700); err != nil {
		t.Fatalf("create bind-race target: %v", err)
	}
	var bindReplacementErr error
	kernel.afterBindMove = func(target string) {
		if target == bindRaceTarget {
			if bindReplacementErr = syscall.Unmount(target, syscall.MNT_DETACH); bindReplacementErr == nil {
				bindReplacementErr = syscall.Mount(stageTarget, target, "", syscall.MS_BIND, "")
			}
		}
	}
	bindResult, bindErr = kernel.Bind(context.Background(), privilegedBindRequest(t, Entry{
		Kind: KindPublish, Target: bindRaceTarget, SourcePath: stageTarget,
		SourceMountID:  stage.MountID,
		FilesystemType: "virtiofs", FilesystemSource: parentID, ParentFilesystemID: parentID,
		BackingRelativePath: "/kubernetes-volumes/tenant--claim--0123456789ab",
		AccessMode:          volume.AccessModeMultiNodeMultiWriter,
	}))
	kernel.afterBindMove = nil
	// The replacement is installed by another actor after our mount was
	// detached, so the safest classification is Ambiguous: the caller must not
	// issue any pathname rollback. Closing bindFD releases our detached object.
	if bindErr == nil || bindResult.Mutation != BindMutationAmbiguous || bindReplacementErr != nil {
		t.Fatalf("Bind(concurrent replacement) = %#v, %v, replacement=%v", bindResult, bindErr, bindReplacementErr)
	}
	bindRaceTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(bind replacement) error = %v", err)
	}
	if _, err := bindRaceTable.Exact(bindRaceTarget); err != nil {
		t.Fatalf("Bind rollback removed replacement: %v", err)
	}
	if err := syscall.Unmount(bindRaceTarget, syscall.MNT_DETACH); err != nil {
		t.Fatalf("remove preserved bind replacement: %v", err)
	}

	// Rename the originally opened target directory before move_mount and put a
	// fresh directory at the CSI pathname. The authenticated target FD remains
	// the action authority, so the bind is installed on the relocated inode and
	// then rolled back when the CSI pathname no longer names that inode.
	targetRename := filepath.Join(kubeletRoot, "pods", "pod-target-rename", "volumes", "kubernetes.io~csi", "pv-target-rename", "mount")
	relocatedTarget := targetRename + "-relocated"
	if err := os.MkdirAll(targetRename, 0o700); err != nil {
		t.Fatalf("create target-rename directory: %v", err)
	}
	var targetRenameErr error
	installedAtAuthenticatedTarget := false
	kernel.beforeBindMove = func(target string) {
		if target != targetRename {
			return
		}
		if targetRenameErr = os.Rename(targetRename, relocatedTarget); targetRenameErr == nil {
			targetRenameErr = os.Mkdir(targetRename, 0o700)
		}
	}
	kernel.afterBindMove = func(target string) {
		if target != targetRename || targetRenameErr != nil {
			return
		}
		currentLayers, currentDescendants, currentErr := kernel.mountTopologyAt(targetRename)
		if currentErr != nil {
			targetRenameErr = currentErr
			return
		}
		relocatedLayers, relocatedDescendants, relocatedErr := kernel.mountTopologyAt(relocatedTarget)
		if relocatedErr != nil {
			targetRenameErr = relocatedErr
			return
		}
		installedAtAuthenticatedTarget = currentLayers == 0 && currentDescendants == 0 && relocatedLayers == 1 && relocatedDescendants == 0
	}
	bindResult, bindErr = kernel.Bind(context.Background(), privilegedBindRequest(t, Entry{
		Kind: KindPublish, Target: targetRename, SourcePath: stageTarget,
		SourceMountID:  stage.MountID,
		FilesystemType: "virtiofs", FilesystemSource: parentID, ParentFilesystemID: parentID,
		BackingRelativePath: "/kubernetes-volumes/tenant--claim--0123456789ab",
		AccessMode:          volume.AccessModeMultiNodeMultiWriter,
	}))
	kernel.beforeBindMove = nil
	kernel.afterBindMove = nil
	if bindErr == nil || bindResult.Mutation != BindMutationNone || targetRenameErr != nil || !installedAtAuthenticatedTarget {
		t.Fatalf("Bind(renamed target) = %#v, %v, rename=%v, installedAtAuthenticated=%v", bindResult, bindErr, targetRenameErr, installedAtAuthenticatedTarget)
	}
	if table, err := kernel.Snapshot(context.Background()); err != nil {
		t.Fatalf("Snapshot(after target-rename rollback) error = %v", err)
	} else if _, err := table.Exact(targetRename); !errors.Is(err, ErrNotMounted) {
		t.Fatalf("renamed-target failed bind remained mounted: %v", err)
	}

	// Read-only is applied to the detached object before it becomes visible.
	// The tmpfs fixture is intentionally rejected later as non-virtiofs, but
	// the hook proves no writable exposure window exists at the target.
	readOnlyTarget := filepath.Join(kubeletRoot, "pods", "pod-readonly", "volumes", "kubernetes.io~csi", "pv-readonly", "mount")
	if err := os.MkdirAll(readOnlyTarget, 0o700); err != nil {
		t.Fatalf("create read-only target: %v", err)
	}
	readOnlyObserved := false
	kernel.afterBindMove = func(target string) {
		if target != readOnlyTarget {
			return
		}
		observed, snapshotErr := kernel.Snapshot(context.Background())
		if snapshotErr != nil {
			bindReplacementErr = snapshotErr
			return
		}
		entry, exactErr := observed.Exact(target)
		if exactErr != nil {
			bindReplacementErr = exactErr
			return
		}
		readOnlyObserved = entry.ReadOnly
	}
	bindResult, bindErr = kernel.Bind(context.Background(), privilegedBindRequest(t, Entry{
		Kind: KindPublish, Target: readOnlyTarget, SourcePath: stageTarget,
		SourceMountID:  stage.MountID,
		FilesystemType: "virtiofs", FilesystemSource: parentID, ParentFilesystemID: parentID,
		BackingRelativePath: "/kubernetes-volumes/tenant--claim--0123456789ab",
		ReadOnly:            true, AccessMode: volume.AccessModeMultiNodeMultiWriter,
	}))
	kernel.afterBindMove = nil
	if bindErr == nil || bindResult.Mutation != BindMutationNone || bindReplacementErr != nil || !readOnlyObserved {
		t.Fatalf("Bind(read-only detached object) = %#v, %v, hook=%v, observedRO=%v", bindResult, bindErr, bindReplacementErr, readOnlyObserved)
	}
	if table, err := kernel.Snapshot(context.Background()); err != nil {
		t.Fatalf("Snapshot(after read-only rollback) error = %v", err)
	} else if _, err := table.Exact(readOnlyTarget); !errors.Is(err, ErrNotMounted) {
		t.Fatalf("read-only failed bind remained mounted: %v", err)
	}

	stage, err = afterRace.Exact(stageTarget)
	if err != nil {
		t.Fatalf("Exact(stage before cleanup) error = %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), stageTarget, stage.MountID); err != nil {
		t.Fatalf("UnmountExact(stage) error = %v", err)
	}
	parent, err = afterRace.Exact(parentTarget)
	if err != nil {
		t.Fatalf("Exact(parent before cleanup) error = %v", err)
	}
	if _, err := kernel.UnmountExact(context.Background(), parentTarget, parent.MountID); err != nil {
		t.Fatalf("UnmountExact(parent) error = %v", err)
	}
	finalTable, err := kernel.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot(final) error = %v", err)
	}
	for _, target := range []string{publishTarget, stageTarget, parentTarget} {
		if _, err := finalTable.Exact(target); !errors.Is(err, ErrNotMounted) {
			t.Fatalf("final target %q error = %v, want ErrNotMounted", target, err)
		}
	}
}

func privilegedBindRequest(t *testing.T, entry Entry) BindRequest {
	t.Helper()
	source, err := os.Open(entry.SourcePath)
	if err != nil {
		t.Fatalf("open privileged bind source %q: %v", entry.SourcePath, err)
	}
	t.Cleanup(func() { _ = source.Close() })
	target, err := os.Open(entry.Target)
	if err != nil {
		_ = source.Close()
		t.Fatalf("open privileged bind target %q: %v", entry.Target, err)
	}
	t.Cleanup(func() { _ = target.Close() })
	return BindRequest{Entry: entry, Source: source, Target: target}
}

func volumeMappingForPrivilegedTest(parentID string) volume.Mapping {
	return volume.Mapping{
		PoolName: "standard", ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes",
		DirectoryName: "tenant--claim--0123456789ab", LogicalVolumeID: "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}

func privilegedMountCapability() volume.Capability {
	return volume.Capability{AccessType: "mount", FilesystemType: "virtiofs", AccessMode: volume.AccessModeMultiNodeMultiWriter}
}
