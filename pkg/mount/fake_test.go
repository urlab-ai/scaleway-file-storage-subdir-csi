package mount

import (
	"context"
	"errors"
	"testing"
)

func TestFakeUnmountRequiresExactUnstackedMountID(t *testing.T) {
	mounter := NewFake()
	mounter.Seed(Entry{MountID: 10, Target: "/target"})
	if _, err := mounter.UnmountExact(context.Background(), "/target", 11); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("UnmountExact(changed ID) error = %v", err)
	}
	mounter.Seed(Entry{MountID: 12, Target: "/target"})
	if _, err := mounter.UnmountExact(context.Background(), "/target", 10); !errors.Is(err, ErrStackedMount) {
		t.Fatalf("UnmountExact(stacked) error = %v", err)
	}
}
