package driverapp

import (
	"context"
	"fmt"
	"log/slog"

	"scaleway-sfs-subdir-csi/pkg/mount"
)

type nodeMountErrorMetric interface {
	AddMountError(count uint64) error
}

// observedNodeMounter counts failed node kernel/mount-table operations without
// changing their exact result. A metrics failure is delivered out of band so
// an already-ambiguous mount result is never replaced by an observability error.
type observedNodeMounter struct {
	delegate mount.Interface
	metrics  nodeMountErrorMetric
	failure  func(error)
}

func newObservedNodeMounter(delegate mount.Interface, metrics nodeMountErrorMetric, failure func(error)) (*observedNodeMounter, error) {
	if delegate == nil || metrics == nil || failure == nil {
		return nil, fmt.Errorf("observed node mounter dependency is nil")
	}
	return &observedNodeMounter{delegate: delegate, metrics: metrics, failure: failure}, nil
}

func (mounter *observedNodeMounter) ReconcileQuarantines(ctx context.Context) error {
	err := mounter.delegate.ReconcileQuarantines(ctx)
	mounter.observe(ctx, "reconcile-mount-quarantines", err)
	return err
}

func (mounter *observedNodeMounter) Snapshot(ctx context.Context) (mount.Table, error) {
	result, err := mounter.delegate.Snapshot(ctx)
	mounter.observe(ctx, "snapshot-mount-table", err)
	return result, err
}

func (mounter *observedNodeMounter) MountParent(ctx context.Context, parentFilesystemID, target string) error {
	err := mounter.delegate.MountParent(ctx, parentFilesystemID, target)
	mounter.observe(ctx, "mount-parent", err, "parent_filesystem_id", parentFilesystemID, "target_path", target)
	return err
}

func (mounter *observedNodeMounter) Bind(ctx context.Context, request mount.BindRequest) (mount.BindResult, error) {
	result, err := mounter.delegate.Bind(ctx, request)
	mounter.observe(ctx, "bind-mount", err, "source_path", request.Entry.SourcePath, "target_path", request.Entry.Target)
	return result, err
}

func (mounter *observedNodeMounter) UnmountExact(ctx context.Context, target string, mountID uint64) (mount.UnmountResult, error) {
	result, err := mounter.delegate.UnmountExact(ctx, target, mountID)
	mounter.observe(ctx, "unmount-exact", err, "target_path", target, "mount_id", mountID)
	return result, err
}

func (mounter *observedNodeMounter) observe(ctx context.Context, operation string, mountErr error, attributes ...any) {
	if mountErr == nil {
		slog.DebugContext(ctx, "mount operation completed", append([]any{"mount_operation", operation}, attributes...)...)
		return
	}
	attributes = append([]any{"mount_operation", operation}, attributes...)
	attributes = append(attributes, "error", mountErr)
	slog.WarnContext(ctx, "mount operation failed", attributes...)
	if err := mounter.metrics.AddMountError(1); err != nil {
		mounter.failure(fmt.Errorf("record %s failure: %w", operation, err))
	}
}

var _ mount.Interface = (*observedNodeMounter)(nil)
