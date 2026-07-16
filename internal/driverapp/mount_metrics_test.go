package driverapp

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/observability"
)

type fakeMountErrorMetric struct {
	count uint64
	err   error
}

func (metrics *fakeMountErrorMetric) AddMountError(count uint64) error {
	metrics.count += count
	return metrics.err
}

func TestObservedNodeMounterPreservesMountErrorAndReportsMetricFailure(t *testing.T) {
	delegate := mount.NewFake()
	mountErr := errors.New("kernel mount failed")
	delegate.MountError = mountErr
	metricErr := errors.New("registry failed")
	metrics := &fakeMountErrorMetric{err: metricErr}
	var reported error
	observed, err := newObservedNodeMounter(delegate, metrics, func(err error) { reported = err })
	if err != nil {
		t.Fatalf("newObservedNodeMounter() error = %v", err)
	}
	if err := observed.MountParent(context.Background(), "11111111-1111-4111-8111-111111111111", "/parents/one"); err != mountErr {
		t.Fatalf("MountParent() error = %v, want exact delegate error", err)
	}
	if metrics.count != 1 || !errors.Is(reported, metricErr) {
		t.Fatalf("mount error metric/report = %d/%v", metrics.count, reported)
	}
}

func TestNodeRuntimeRefreshesVerifiedParentMountCounts(t *testing.T) {
	const (
		parentA = "11111111-1111-4111-8111-111111111111"
		parentB = "22222222-2222-4222-8222-222222222222"
		root    = "/parents"
	)
	metrics, err := observability.NewNodeMetrics([]string{"standard"})
	if err != nil {
		t.Fatalf("NewNodeMetrics() error = %v", err)
	}
	mounter := mount.NewFake()
	if err := mounter.MountParent(context.Background(), parentA, root+"/"+parentA); err != nil {
		t.Fatalf("MountParent() error = %v", err)
	}
	runtime := &nodeRuntime{
		metrics: metrics, mounter: mounter, parentRoot: root,
		parents: []driver.NodeParentConfiguration{
			{PoolName: "standard", ParentFilesystemID: parentA, BasePath: "/kubernetes-volumes"},
			{PoolName: "standard", ParentFilesystemID: parentB, BasePath: "/kubernetes-volumes"},
		},
	}
	if err := runtime.refreshParentMountMetrics(context.Background()); err != nil {
		t.Fatalf("refreshParentMountMetrics() error = %v", err)
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	if !strings.Contains(output.String(), `sfs_subdir_node_parent_mounts{pool="standard"} 1`+"\n") {
		t.Fatalf("node parent mount metric is missing:\n%s", output.String())
	}

	mounter.Seed(mount.Entry{
		Kind: mount.KindParent, Target: root + "/" + parentB,
		FilesystemType: "virtiofs", FilesystemSource: parentA,
		ParentFilesystemID: parentA, BackingRelativePath: "/", DeviceID: "virtiofs:" + parentA,
	})
	if err := runtime.refreshParentMountMetrics(context.Background()); !errors.Is(err, mount.ErrForeignMount) {
		t.Fatalf("refreshParentMountMetrics(foreign) error = %v", err)
	}
}
