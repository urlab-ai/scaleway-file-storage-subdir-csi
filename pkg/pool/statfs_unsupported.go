//go:build !linux

package pool

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
)

// OSStatFSSampler is unavailable outside Linux because production controller
// mounts and descriptor-anchored fstatfs are Linux contracts.
type OSStatFSSampler struct{}

// NewOSStatFSSampler rejects unsupported kernels.
func NewOSStatFSSampler(operationClock clock.Clock) (*OSStatFSSampler, error) {
	if operationClock == nil {
		return nil, fmt.Errorf("statfs clock is nil")
	}
	return nil, fmt.Errorf("descriptor-anchored statfs requires Linux")
}

// Sample always rejects outside Linux.
func (*OSStatFSSampler) Sample(context.Context, string) (StatFSSample, error) {
	return StatFSSample{}, fmt.Errorf("descriptor-anchored statfs requires Linux")
}

var _ StatFSSampler = (*OSStatFSSampler)(nil)
