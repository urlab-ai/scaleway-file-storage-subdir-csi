package safety

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOSDurableFSNoReplaceAndExpectedReplace(t *testing.T) {
	root := t.TempDir()
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	ctx := context.Background()

	if err := filesystem.CreateExclusive(ctx, "first.tmp", []byte("first"), 0o600); err != nil {
		t.Fatalf("CreateExclusive(first) error = %v", err)
	}
	if err := filesystem.SyncFile(ctx, "first.tmp"); err != nil {
		t.Fatalf("SyncFile(first) error = %v", err)
	}
	if err := filesystem.RenameNoReplace(ctx, "first.tmp", "record.json"); err != nil {
		t.Fatalf("RenameNoReplace(first) error = %v", err)
	}
	if err := filesystem.SyncDir(ctx, "."); err != nil {
		t.Fatalf("SyncDir(first) error = %v", err)
	}
	if err := filesystem.CreateExclusive(ctx, "second.tmp", []byte("second"), 0o600); err != nil {
		t.Fatalf("CreateExclusive(second) error = %v", err)
	}
	if err := filesystem.RenameNoReplace(ctx, "second.tmp", "record.json"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("RenameNoReplace(existing) error = %v", err)
	}
	if got, err := filesystem.ReadFileNoFollow(ctx, "record.json"); err != nil || string(got) != "first" {
		t.Fatalf("ReadFileNoFollow(existing) = %q, %v", got, err)
	}

	if err := filesystem.SyncFile(ctx, "second.tmp"); err != nil {
		t.Fatalf("SyncFile(second) error = %v", err)
	}
	if err := filesystem.ReplaceExpected(ctx, "second.tmp", "record.json", []byte("wrong")); !errors.Is(err, ErrExpectedGenerationMismatch) {
		t.Fatalf("ReplaceExpected(wrong) error = %v", err)
	}
	if err := filesystem.ReplaceExpected(ctx, "second.tmp", "record.json", []byte("first")); err != nil {
		t.Fatalf("ReplaceExpected(correct) error = %v", err)
	}
	if err := filesystem.SyncDir(ctx, "."); err != nil {
		t.Fatalf("SyncDir(second) error = %v", err)
	}
	if got, err := filesystem.ReadFileNoFollow(ctx, "record.json"); err != nil || string(got) != "second" {
		t.Fatalf("ReadFileNoFollow(replaced) = %q, %v", got, err)
	}
}

func TestOSDurableFSRejectsSymlinkRead(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside) error = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	if _, err := filesystem.ReadFileNoFollow(context.Background(), "link"); err == nil {
		t.Fatal("ReadFileNoFollow(symlink) error = nil")
	}
}

func TestOSDurableFSRemoveExactUnlinksSymlinkWithoutFollowingIt(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("must remain"), 0o600); err != nil {
		t.Fatalf("WriteFile(outside) error = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	if err := filesystem.RemoveExact(context.Background(), "link"); err != nil {
		t.Fatalf("RemoveExact() error = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "link")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Lstat(link) error = %v, want not exist", err)
	}
	content, err := os.ReadFile(outside)
	if err != nil || string(content) != "must remain" {
		t.Fatalf("outside content = %q, error = %v", content, err)
	}
	if err := filesystem.RemoveExact(context.Background(), "link"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("RemoveExact(absent) error = %v, want ErrEntryNotFound", err)
	}
}

func TestOSDurableFSRejectsIntermediateInRootSymlinkAlias(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "real"), 0o700); err != nil {
		t.Fatalf("Mkdir(real) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "real", "record.json"), []byte("foreign"), 0o600); err != nil {
		t.Fatalf("WriteFile(record) error = %v", err)
	}
	if err := os.Symlink("real", filepath.Join(root, "metadata")); err != nil {
		t.Fatalf("Symlink(metadata) error = %v", err)
	}
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	if _, err := filesystem.ReadFileNoFollow(context.Background(), "metadata/record.json"); err == nil {
		t.Fatal("ReadFileNoFollow(intermediate symlink) error = nil")
	}
}

func TestOSDurableFSBoundsMetadataRead(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "oversized.json"), make([]byte, maxDurableMetadataBytes+1), 0o600); err != nil {
		t.Fatalf("WriteFile(oversized) error = %v", err)
	}
	filesystem, err := OpenOSDurableFS(root)
	if err != nil {
		t.Fatalf("OpenOSDurableFS() error = %v", err)
	}
	t.Cleanup(func() { _ = filesystem.Close() })
	if _, err := filesystem.ReadFileNoFollow(context.Background(), "oversized.json"); err == nil {
		t.Fatal("ReadFileNoFollow(oversized) error = nil")
	}
}
