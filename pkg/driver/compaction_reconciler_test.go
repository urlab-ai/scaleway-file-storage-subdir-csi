package driver

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type fakeCompactionLister struct {
	items []k8s.StoredAllocation
	err   error
	calls int
}

func (lister *fakeCompactionLister) List(context.Context) ([]k8s.StoredAllocation, error) {
	lister.calls++
	return slices.Clone(lister.items), lister.err
}

type fakeCompactionExecutor struct {
	logicalIDs []string
	err        error
}

func (executor *fakeCompactionExecutor) Compact(_ context.Context, logicalVolumeID string) error {
	executor.logicalIDs = append(executor.logicalIDs, logicalVolumeID)
	return executor.err
}

func detailedDeletedStored(t *testing.T) (k8s.StoredAllocation, string, string) {
	t.Helper()
	_, harness, _ := newCompactionHarness(t, time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC))
	stored, err := harness.allocations.Get(context.Background(), harness.allocation.LogicalVolumeID)
	if err != nil {
		t.Fatalf("allocation Get() error = %v", err)
	}
	return stored, harness.allocation.LogicalVolumeID, harness.allocation.ParentFilesystemID
}

func TestAllocationCompactionReconcilerSelectsEligibleDetailedTombstone(t *testing.T) {
	stored, logicalID, parentID := detailedDeletedStored(t)
	lister := &fakeCompactionLister{items: []k8s.StoredAllocation{stored}}
	executor := &fakeCompactionExecutor{}
	reconciler, err := NewAllocationCompactionReconciler(
		lister, executor, time.Hour, []string{parentID},
		clock.NewManual(time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewAllocationCompactionReconciler() error = %v", err)
	}
	summary, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if lister.calls != 1 || !slices.Equal(executor.logicalIDs, []string{logicalID}) || summary.Scanned != 1 || summary.Compacted != 1 {
		t.Fatalf("calls/IDs/summary = %d/%#v/%#v", lister.calls, executor.logicalIDs, summary)
	}
}

func TestAllocationCompactionReconcilerDefersRetentionAndHistoricalParent(t *testing.T) {
	stored, _, parentID := detailedDeletedStored(t)
	for name, test := range map[string]struct {
		now            time.Time
		parents        []string
		wantRetention  uint64
		wantHistorical uint64
	}{
		"retention": {
			now: time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC), parents: []string{parentID}, wantRetention: 1,
		},
		"historical parent": {
			now:     time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC),
			parents: []string{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, wantHistorical: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			executor := &fakeCompactionExecutor{}
			reconciler, err := NewAllocationCompactionReconciler(
				&fakeCompactionLister{items: []k8s.StoredAllocation{stored}}, executor,
				time.Hour, test.parents, clock.NewManual(test.now),
			)
			if err != nil {
				t.Fatalf("NewAllocationCompactionReconciler() error = %v", err)
			}
			summary, err := reconciler.Reconcile(context.Background())
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if len(executor.logicalIDs) != 0 || summary.RetentionDeferred != test.wantRetention || summary.HistoricalParentDeferred != test.wantHistorical {
				t.Fatalf("executor/summary = %#v/%#v", executor.logicalIDs, summary)
			}
		})
	}
}

func TestAllocationCompactionReconcilerSkipsCompactWithoutFollowupAndPropagatesFailure(t *testing.T) {
	stored, _, parentID := detailedDeletedStored(t)
	detailed := stored.Record.(*volume.DetailedAllocationRecord)
	stored.Record = compactAllocationFromDetailed(detailed)
	executor := &fakeCompactionExecutor{}
	reconciler, err := NewAllocationCompactionReconciler(
		&fakeCompactionLister{items: []k8s.StoredAllocation{stored}}, executor,
		time.Hour, []string{parentID}, clock.NewManual(time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewAllocationCompactionReconciler() error = %v", err)
	}
	summary, err := reconciler.Reconcile(context.Background())
	if err != nil || summary.CompactTombstones != 1 || len(executor.logicalIDs) != 0 {
		t.Fatalf("Reconcile(compact) summary/IDs/error = %#v/%#v/%v", summary, executor.logicalIDs, err)
	}

	stored.Record = detailed
	executor.err = errors.New("ownership unavailable")
	reconciler, err = NewAllocationCompactionReconciler(
		&fakeCompactionLister{items: []k8s.StoredAllocation{stored}}, executor,
		time.Hour, []string{parentID}, clock.NewManual(time.Date(2026, 7, 13, 15, 0, 1, 0, time.UTC)),
	)
	if err != nil {
		t.Fatalf("NewAllocationCompactionReconciler(failure) error = %v", err)
	}
	if _, err := reconciler.Reconcile(context.Background()); !errors.Is(err, executor.err) {
		t.Fatalf("Reconcile(failure) error = %v", err)
	}
}
