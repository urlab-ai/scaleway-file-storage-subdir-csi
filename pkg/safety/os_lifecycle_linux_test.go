//go:build linux && (amd64 || arm64)

package safety

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func openTestOSLifecycle(t *testing.T) (*OSLifecycleFS, string) {
	t.Helper()
	root := t.TempDir()
	for _, relative := range []string{
		"kubernetes-volumes",
		"kubernetes-volumes/.archived",
		"kubernetes-volumes/.deleted",
		"kubernetes-volumes/.sfs-subdir-csi",
	} {
		if err := os.Mkdir(filepath.Join(root, relative), 0o700); err != nil {
			t.Fatalf("Mkdir(%q) error = %v", relative, err)
		}
	}
	filesystem, err := OpenOSLifecycleFS(root)
	if err != nil {
		t.Fatalf("OpenOSLifecycleFS() error = %v", err)
	}
	t.Cleanup(func() {
		if err := filesystem.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return filesystem, root
}

func TestOSLifecycleCreateArchiveAndNoReplace(t *testing.T) {
	filesystem, root := openTestOSLifecycle(t)
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	if err := lifecycle.CreateLogicalDirectory(
		context.Background(), "/kubernetes-volumes", testDirectory, "0770",
		uint32(os.Getuid()), uint32(os.Getgid()),
	); err != nil {
		t.Fatalf("CreateLogicalDirectory() error = %v", err)
	}
	source := filepath.Join(root, "kubernetes-volumes", testDirectory)
	if info, err := os.Stat(source); err != nil || info.Mode().Perm() != 0o770 {
		t.Fatalf("created source info/error = %#v, %v", info, err)
	}
	target := "/kubernetes-volumes/.archived/archive-target"
	if err := lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, target); err != nil {
		t.Fatalf("Archive() error = %v", err)
	}
	if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("archived source Lstat() error = %v", err)
	}
	if info, err := os.Stat(filepath.Join(root, strings.TrimPrefix(target, "/"))); err != nil || !info.IsDir() {
		t.Fatalf("archive target info/error = %#v, %v", info, err)
	}

	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatalf("Mkdir(second source) error = %v", err)
	}
	err = lifecycle.Archive(context.Background(), "/kubernetes-volumes", testDirectory, target)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("Archive(existing target) error = %v", err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("no-replace source was changed: %v", err)
	}
}

func TestOSLifecycleRecursiveRemovalUnlinksSymlinkWithoutEscaping(t *testing.T) {
	filesystem, root := openTestOSLifecycle(t)
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	quarantine := filepath.Join(root, "kubernetes-volumes/.deleted/quarantine-a")
	if err := os.MkdirAll(filepath.Join(quarantine, "nested"), 0o700); err != nil {
		t.Fatalf("MkdirAll(quarantine) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(quarantine, "nested/data"), []byte("remove"), 0o600); err != nil {
		t.Fatalf("WriteFile(quarantine) error = %v", err)
	}
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "must-survive")
	if err := os.WriteFile(outsideFile, []byte("safe"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside) error = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(quarantine, "escape")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if err := lifecycle.RemoveQuarantine(context.Background(), "/kubernetes-volumes", "/kubernetes-volumes/.deleted/quarantine-a"); err != nil {
		t.Fatalf("RemoveQuarantine() error = %v", err)
	}
	if _, err := os.Lstat(quarantine); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine Lstat() error = %v", err)
	}
	if data, err := os.ReadFile(outsideFile); err != nil || string(data) != "safe" {
		t.Fatalf("outside data/error = %q, %v", data, err)
	}
}

func TestOSLifecycleRejectsIntermediateSymlink(t *testing.T) {
	filesystem, root := openTestOSLifecycle(t)
	if err := os.Symlink("kubernetes-volumes", filepath.Join(root, "alias")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if err := filesystem.SyncDir(context.Background(), "alias/.deleted"); err == nil {
		t.Fatal("SyncDir(intermediate symlink) error = nil")
	}
}

func TestOSLifecycleInspectionDistinguishesAbsenceFromUnsafeEntry(t *testing.T) {
	filesystem, root := openTestOSLifecycle(t)
	presentPath := filepath.Join(root, "kubernetes-volumes/.deleted/present")
	if err := os.Mkdir(presentPath, 0o700); err != nil {
		t.Fatalf("Mkdir(present) error = %v", err)
	}
	present, err := filesystem.InspectDirectory(context.Background(), "kubernetes-volumes/.deleted/present")
	if err != nil || !present {
		t.Fatalf("InspectDirectory(present) = %v, %v", present, err)
	}
	present, err = filesystem.InspectDirectory(context.Background(), "kubernetes-volumes/.deleted/absent")
	if err != nil || present {
		t.Fatalf("InspectDirectory(absent) = %v, %v", present, err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "kubernetes-volumes/.deleted/unsafe")); err != nil {
		t.Fatalf("Symlink(unsafe) error = %v", err)
	}
	if _, err := filesystem.InspectDirectory(context.Background(), "kubernetes-volumes/.deleted/unsafe"); err == nil {
		t.Fatal("InspectDirectory(symlink) error = nil")
	}
}

// TestPrivilegedOSLifecycleRejectsNestedMount proves the production recursive
// removal adapter observes the kernel mount graph, not only directory entries.
// The disposable namespace ensures neither the nested mount nor cleanup can
// affect the host running the test.
func TestPrivilegedOSLifecycleRejectsNestedMount(t *testing.T) {
	if os.Getenv("SFS_SUBDIR_SAFETY_HELPER") == "1" {
		runPrivilegedOSLifecycleNestedMount(t)
		return
	}
	if os.Getenv("SFS_SUBDIR_PRIVILEGED_LINUX_TEST") != "1" {
		t.Skip("set SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 and run as root")
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPrivilegedOSLifecycleRejectsNestedMount$", "-test.v")
	command.Env = append(os.Environ(), "SFS_SUBDIR_SAFETY_HELPER=1")
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("privileged lifecycle helper failed: %v\n%s", err, output)
	}
}

// TestPrivilegedOSDurableFSLateNestedMount proves metadata mutations keep the
// already-authenticated parent FD through the final syscall. The hook installs
// a new mount after that FD is opened; operations must still affect the original
// hidden directory and never the newly mounted surface.
func TestPrivilegedOSDurableFSLateNestedMount(t *testing.T) {
	if os.Getenv("SFS_SUBDIR_DURABLE_HELPER") == "1" {
		runPrivilegedOSDurableLateNestedMount(t)
		return
	}
	if os.Getenv("SFS_SUBDIR_PRIVILEGED_LINUX_TEST") != "1" {
		t.Skip("set SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 and run as root")
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPrivilegedOSDurableFSLateNestedMount$", "-test.v")
	command.Env = append(os.Environ(), "SFS_SUBDIR_DURABLE_HELPER=1")
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("privileged durable helper failed: %v\n%s", err, output)
	}
}

func runPrivilegedOSDurableLateNestedMount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("privileged durable helper must run as root")
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make disposable mount namespace private: %v", err)
	}
	root := t.TempDir()
	metadata := filepath.Join(root, "metadata")
	if err := os.Mkdir(metadata, 0o700); err != nil {
		t.Fatalf("Mkdir(metadata) error = %v", err)
	}
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	defer filesystem.Close()

	mounted := false
	mountAfterProof := func(operation string) {
		filesystem.beforeMutation = func(got string) error {
			if got != operation {
				return nil
			}
			if err := syscall.Mount("late-metadata-proof", metadata, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
				return err
			}
			mounted = true
			return nil
		}
	}
	unmount := func() {
		if mounted {
			if err := syscall.Unmount(metadata, 0); err != nil {
				t.Fatalf("unmount late metadata proof: %v", err)
			}
			mounted = false
		}
		filesystem.beforeMutation = nil
	}
	t.Cleanup(func() {
		if mounted {
			_ = syscall.Unmount(metadata, syscall.MNT_DETACH)
		}
	})

	mountAfterProof("create-exclusive")
	if err := filesystem.CreateExclusive(context.Background(), "metadata/create.tmp", []byte("created"), 0o600); err != nil {
		t.Fatalf("CreateExclusive(late mount) error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(metadata, "create.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late mounted surface received create: %v", err)
	}
	unmount()
	if data, err := os.ReadFile(filepath.Join(metadata, "create.tmp")); err != nil || string(data) != "created" {
		t.Fatalf("hidden authenticated create = %q, %v", data, err)
	}

	if err := os.WriteFile(filepath.Join(metadata, "record.json"), []byte("old"), 0o600); err != nil {
		t.Fatalf("WriteFile(record) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(metadata, "replace.tmp"), []byte("new"), 0o600); err != nil {
		t.Fatalf("WriteFile(replacement) error = %v", err)
	}
	mountAfterProof("replace-expected")
	if err := filesystem.ReplaceExpected(context.Background(), "metadata/replace.tmp", "metadata/record.json", []byte("old")); err != nil {
		t.Fatalf("ReplaceExpected(late mount) error = %v", err)
	}
	unmount()
	if data, err := os.ReadFile(filepath.Join(metadata, "record.json")); err != nil || string(data) != "new" {
		t.Fatalf("hidden authenticated replacement = %q, %v", data, err)
	}

	if err := os.WriteFile(filepath.Join(metadata, "remove.me"), []byte("remove"), 0o600); err != nil {
		t.Fatalf("WriteFile(remove) error = %v", err)
	}
	mountAfterProof("remove-exact")
	if err := filesystem.RemoveExact(context.Background(), "metadata/remove.me"); err != nil {
		t.Fatalf("RemoveExact(late mount) error = %v", err)
	}
	unmount()
	if _, err := os.Lstat(filepath.Join(metadata, "remove.me")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hidden authenticated remove error = %v", err)
	}
}

func runPrivilegedOSLifecycleNestedMount(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("privileged lifecycle helper must run as root")
	}
	if err := syscall.Mount("", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, ""); err != nil {
		t.Fatalf("make disposable mount namespace private: %v", err)
	}
	filesystem, root := openTestOSLifecycle(t)
	lifecycle, err := NewDirectoryLifecycle(filesystem)
	if err != nil {
		t.Fatalf("NewDirectoryLifecycle() error = %v", err)
	}
	quarantine := filepath.Join(root, "kubernetes-volumes/.deleted/quarantine-mounted")
	nested := filepath.Join(quarantine, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatalf("create nested mount target: %v", err)
	}
	if err := syscall.Mount("nested-proof", nested, "tmpfs", 0, "size=1m,mode=0700"); err != nil {
		t.Fatalf("mount nested disposable filesystem: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Unmount(nested, syscall.MNT_DETACH) })
	if err := os.WriteFile(filepath.Join(nested, "must-survive"), []byte("safe"), 0o600); err != nil {
		t.Fatalf("write nested mount proof: %v", err)
	}
	durable, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	defer func() {
		if err := durable.Close(); err != nil {
			t.Errorf("close durable filesystem: %v", err)
		}
	}()
	if _, err := durable.ReadFileNoFollow(context.Background(), "kubernetes-volumes/.deleted/quarantine-mounted/nested/must-survive"); err == nil {
		t.Fatal("OSDurableFS crossed a nested mount boundary")
	}

	err = lifecycle.RemoveQuarantine(context.Background(), "/kubernetes-volumes", "/kubernetes-volumes/.deleted/quarantine-mounted")
	if err == nil {
		t.Fatal("RemoveQuarantine(nested kernel mount) error = nil")
	}
	if data, readErr := os.ReadFile(filepath.Join(nested, "must-survive")); readErr != nil || string(data) != "safe" {
		t.Fatalf("nested mount was traversed or damaged: %q, %v", data, readErr)
	}
	if info, statErr := os.Stat(quarantine); statErr != nil || !info.IsDir() {
		t.Fatalf("quarantine root was removed after nested-mount rejection: %#v, %v", info, statErr)
	}
}

func TestParseLinuxMountIDIsClosedAndPositive(t *testing.T) {
	if got, err := parseLinuxMountID(strings.NewReader("pos:\t0\nmnt_id:\t42\n")); err != nil || got != 42 {
		t.Fatalf("parseLinuxMountID() = %d, %v", got, err)
	}
	for name, input := range map[string]string{
		"missing":   "pos:\t0\n",
		"zero":      "mnt_id:\t0\n",
		"malformed": "mnt_id:\tnot-a-number\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseLinuxMountID(strings.NewReader(input)); err == nil {
				t.Fatal("parseLinuxMountID(invalid) error = nil")
			}
		})
	}
}
