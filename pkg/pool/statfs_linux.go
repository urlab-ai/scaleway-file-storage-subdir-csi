//go:build linux

package pool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"syscall"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
)

// OSStatFSSampler anchors fstatfs(2) to a no-follow directory descriptor so a
// pathname replacement cannot redirect capacity accounting to another mount.
type OSStatFSSampler struct{ clock clock.Clock }

// NewOSStatFSSampler constructs the Linux sampler.
func NewOSStatFSSampler(operationClock clock.Clock) (*OSStatFSSampler, error) {
	if operationClock == nil {
		return nil, fmt.Errorf("statfs clock is nil")
	}
	return &OSStatFSSampler{clock: operationClock}, nil
}

// Sample reads f_bsize and f_bavail only; CheckPhysicalSpace performs checked
// multiplication and observed-size consistency validation.
func (sampler *OSStatFSSampler) Sample(ctx context.Context, parentRoot string) (sample StatFSSample, returnErr error) {
	if err := ctx.Err(); err != nil {
		return StatFSSample{}, err
	}
	if err := mount.ValidateAbsoluteNormalizedPath(parentRoot); err != nil {
		return StatFSSample{}, err
	}
	fd, err := syscall.Open(parentRoot, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return StatFSSample{}, fmt.Errorf("open parent for fstatfs %q: %w", parentRoot, err)
	}
	defer func() { returnErr = errors.Join(returnErr, syscall.Close(fd)) }()
	var state syscall.Statfs_t
	if err := syscall.Fstatfs(fd, &state); err != nil {
		return StatFSSample{}, fmt.Errorf("fstatfs parent %q: %w", parentRoot, err)
	}
	if state.Bsize <= 0 || uint64(state.Bavail) > math.MaxInt64 {
		return StatFSSample{}, fmt.Errorf("parent %q returned invalid fstatfs block size or available blocks: %w", parentRoot, ErrInvalidStatFS)
	}
	if err := ctx.Err(); err != nil {
		return StatFSSample{}, err
	}
	return StatFSSample{
		BlockSizeBytes: int64(state.Bsize), AvailableBlocks: int64(state.Bavail), ObservedAt: sampler.clock.Now(),
	}, nil
}

var _ StatFSSampler = (*OSStatFSSampler)(nil)
