package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type staticAllocationLister struct{ stored []k8s.StoredAllocation }

func (lister *staticAllocationLister) List(context.Context) ([]k8s.StoredAllocation, error) {
	return append([]k8s.StoredAllocation(nil), lister.stored...), nil
}

type synchronizedAllocationLister struct {
	mu     sync.Mutex
	stored []k8s.StoredAllocation
}

func (lister *synchronizedAllocationLister) List(context.Context) ([]k8s.StoredAllocation, error) {
	lister.mu.Lock()
	defer lister.mu.Unlock()
	return append([]k8s.StoredAllocation(nil), lister.stored...), nil
}

func (lister *synchronizedAllocationLister) append(record volume.AllocationRecord) k8s.StoredAllocation {
	lister.mu.Lock()
	defer lister.mu.Unlock()
	stored := k8s.StoredAllocation{Record: record, ResourceVersion: fmt.Sprintf("%d", len(lister.stored)+1)}
	lister.stored = append(lister.stored, stored)
	return stored
}

type fakePlacementParentAccess struct {
	roots  map[string]string
	errors map[string]error
	calls  []string
}

func (access *fakePlacementParentAccess) EnsureMounted(_ context.Context, parentID string) (string, error) {
	access.calls = append(access.calls, parentID)
	if err := access.errors[parentID]; err != nil {
		return "", err
	}
	return access.roots[parentID], nil
}

func (access *fakePlacementParentAccess) VerifiedMountedRoot(_ context.Context, parentID string) (string, error) {
	access.calls = append(access.calls, "verify:"+parentID)
	if err := access.errors[parentID]; err != nil {
		return "", err
	}
	return access.roots[parentID], nil
}

type fakeStatFSSampler struct {
	samples map[string]pool.StatFSSample
	errors  map[string]error
	calls   []string
}

func (sampler *fakeStatFSSampler) Sample(_ context.Context, root string) (pool.StatFSSample, error) {
	sampler.calls = append(sampler.calls, root)
	if err := sampler.errors[root]; err != nil {
		return pool.StatFSSample{}, err
	}
	return sampler.samples[root], nil
}

func TestProductionParentPlacerAccountsReservationsAndUsesFreshMountedStatFS(t *testing.T) {
	placer, provider, access, sampler, manual, allocation, configured := placementHarness(t)
	request := validCreateRequest()
	request.Parameters.PoolName = configured.Name
	logicalID, err := volume.LogicalVolumeID(driverTestName, "new-placement")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	placement, err := placer.Place(context.Background(), request, 1, logicalID)
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if placement.ParentFilesystemID != allocation.ParentFilesystemID || placement.BasePath != configured.BasePath {
		t.Fatalf("placement = %#v", placement)
	}
	if len(access.calls) != 1 || len(sampler.calls) != 1 {
		t.Fatalf("parent/statfs calls = %#v / %#v", access.calls, sampler.calls)
	}

	metadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	metadata.SizeBytes--
	provider.Filesystems["fr-par/"+allocation.ParentFilesystemID] = metadata
	manual.Advance(time.Second)
	if _, err := placer.Place(context.Background(), request, 1, logicalID); err == nil {
		t.Fatal("Place(size regression) error = nil")
	}
	if len(access.calls) != 1 || len(sampler.calls) != 1 {
		t.Fatalf("size regression touched parent/statfs: %#v / %#v", access.calls, sampler.calls)
	}
}

func TestProductionParentPlacerPreservesFilesystemErrorStatus(t *testing.T) {
	placer, provider, _, _, _, allocation, configured := placementHarness(t)
	metadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	metadata.Status = scaleway.FilesystemError
	provider.Filesystems["fr-par/"+allocation.ParentFilesystemID] = metadata
	request := validCreateRequest()
	request.Parameters.PoolName = configured.Name
	logicalID, err := volume.LogicalVolumeID(driverTestName, "provider-error")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	if _, err := placer.Place(context.Background(), request, 1, logicalID); !errors.Is(err, scaleway.ErrFailedPrecondition) {
		t.Fatalf("Place(FilesystemError) error = %v", err)
	}
}

func TestProductionParentPlacerPreservesTerminalProviderReadFailures(t *testing.T) {
	for _, providerErr := range []error{
		scaleway.ErrInvalidArgument,
		scaleway.ErrNotFound,
		scaleway.ErrPermissionDenied,
		scaleway.ErrResourceExhausted,
		scaleway.ErrFailedPrecondition,
	} {
		t.Run(providerErr.Error(), func(t *testing.T) {
			placer, provider, _, _, _, _, configured := placementHarness(t)
			provider.InjectFault("get-filesystem", providerErr)
			request := validCreateRequest()
			request.Parameters.PoolName = configured.Name
			logicalID, _ := volume.LogicalVolumeID(driverTestName, "terminal-provider-read")
			if _, err := placer.Place(context.Background(), request, 1, logicalID); !errors.Is(err, providerErr) {
				t.Fatalf("Place() error = %v, want %v", err, providerErr)
			}
		})
	}
}

func TestProductionParentPlacerPreservesTerminalMountFailures(t *testing.T) {
	for _, mountErr := range []error{
		scaleway.ErrPermissionDenied,
		scaleway.ErrResourceExhausted,
		scaleway.ErrFailedPrecondition,
	} {
		t.Run(mountErr.Error(), func(t *testing.T) {
			placer, _, access, _, _, allocation, configured := placementHarness(t)
			access.errors[allocation.ParentFilesystemID] = mountErr
			request := validCreateRequest()
			request.Parameters.PoolName = configured.Name
			logicalID, _ := volume.LogicalVolumeID(driverTestName, "terminal-parent-mount")
			if _, err := placer.Place(context.Background(), request, 1, logicalID); !errors.Is(err, mountErr) {
				t.Fatalf("Place() error = %v, want %v", err, mountErr)
			}
		})
	}
}

func TestProductionParentPlacerKeepsPoolLockThroughDurableReservation(t *testing.T) {
	placer, provider, _, _, manual, allocation, configured := placementHarness(t)
	lister := &synchronizedAllocationLister{}
	placer.allocations = lister
	capacity := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID].SizeBytes
	requestA := validCreateRequest()
	requestA.Name = "concurrent-a"
	requestA.PVCName = "claim-a"
	requestA.Parameters.PoolName = configured.Name
	requestA.RequiredBytes = capacity
	requestA.LimitBytes = capacity
	requestB := requestA
	requestB.Name = "concurrent-b"
	requestB.PVCName = "claim-b"
	logicalA, err := volume.LogicalVolumeID(driverTestName, requestA.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID(A) error = %v", err)
	}
	logicalB, err := volume.LogicalVolumeID(driverTestName, requestB.Name)
	if err != nil {
		t.Fatalf("LogicalVolumeID(B) error = %v", err)
	}
	builder := &CreateController{
		driverName: driverTestName, installationID: driverTestInstallationID,
		clusterUID: driverTestClusterUID, clock: manual,
	}
	firstInsideReservation := make(chan struct{})
	allowFirstCommit := make(chan struct{})
	secondInsideReservation := make(chan struct{}, 1)
	firstResult := make(chan error, 1)
	go func() {
		_, reserveErr := placer.PlaceAndReserve(context.Background(), requestA, capacity, logicalA, func(placement Placement) (k8s.StoredAllocation, error) {
			close(firstInsideReservation)
			<-allowFirstCommit
			record, recordErr := builder.newReservedRecord(requestA, capacity, logicalA, placement)
			if recordErr != nil {
				return k8s.StoredAllocation{}, recordErr
			}
			return lister.append(record), nil
		})
		firstResult <- reserveErr
	}()
	<-firstInsideReservation
	secondResult := make(chan error, 1)
	go func() {
		_, reserveErr := placer.PlaceAndReserve(context.Background(), requestB, capacity, logicalB, func(placement Placement) (k8s.StoredAllocation, error) {
			secondInsideReservation <- struct{}{}
			record, recordErr := builder.newReservedRecord(requestB, capacity, logicalB, placement)
			if recordErr != nil {
				return k8s.StoredAllocation{}, recordErr
			}
			return lister.append(record), nil
		})
		secondResult <- reserveErr
	}()

	select {
	case <-secondInsideReservation:
		t.Fatal("second placement entered reservation while first pool lock was held")
	case <-time.After(100 * time.Millisecond):
		// The timeout bounds the negative assertion; no production readiness
		// contract depends on this duration.
	}
	close(allowFirstCommit)
	if err := <-firstResult; err != nil {
		t.Fatalf("first PlaceAndReserve() error = %v", err)
	}
	if err := <-secondResult; !errors.Is(err, pool.ErrNoLogicalCapacity) {
		t.Fatalf("second PlaceAndReserve() error = %v, want ErrNoLogicalCapacity", err)
	}
	select {
	case <-secondInsideReservation:
		t.Fatal("capacity-exhausted second placement invoked its reservation callback")
	default:
	}
}

func TestProductionParentPlacerClosesPoolAfterUnresolvedReservation(t *testing.T) {
	placer, _, _, _, _, _, configured := placementHarness(t)
	request := validCreateRequest()
	request.Parameters.PoolName = configured.Name
	logicalID, err := volume.LogicalVolumeID(driverTestName, "unresolved-reservation")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	_, err = placer.PlaceAndReserve(context.Background(), request, 1, logicalID, func(Placement) (k8s.StoredAllocation, error) {
		return k8s.StoredAllocation{}, fmt.Errorf("late Kubernetes commit remains possible: %w", ErrReservationUnresolved)
	})
	if !errors.Is(err, ErrReservationUnresolved) {
		t.Fatalf("PlaceAndReserve(unresolved) error = %v", err)
	}
	called := false
	_, err = placer.PlaceAndReserve(context.Background(), request, 1, logicalID, func(Placement) (k8s.StoredAllocation, error) {
		called = true
		return k8s.StoredAllocation{}, nil
	})
	if !errors.Is(err, ErrReservationUnresolved) || called {
		t.Fatalf("blocked pool retry error/callback = %v/%t", err, called)
	}
	if err := placer.MarkPoolResolved(context.Background(), configured.Name); err != nil {
		t.Fatalf("MarkPoolResolved() error = %v", err)
	}
	resolvedProbe := errors.New("reservation callback reached after exact resolution")
	_, err = placer.PlaceAndReserve(context.Background(), request, 1, logicalID, func(Placement) (k8s.StoredAllocation, error) {
		called = true
		return k8s.StoredAllocation{}, resolvedProbe
	})
	if !errors.Is(err, resolvedProbe) || !called {
		t.Fatalf("reopened pool error/callback = %v/%t", err, called)
	}
}

func TestProductionParentPlacerRefreshesCompleteRuntimeSnapshotWithoutMountMutation(t *testing.T) {
	placer, _, access, sampler, _, allocation, configured := placementHarness(t)
	snapshot, err := placer.RefreshRuntimeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RefreshRuntimeSnapshot() error = %v", err)
	}
	if len(snapshot.Parents) != 1 {
		t.Fatalf("runtime parents = %#v", snapshot.Parents)
	}
	parent := snapshot.Parents[0]
	if parent.PoolName != configured.Name || parent.ParentFilesystemID != allocation.ParentFilesystemID || parent.Capacity.LogicalAllocatedBytes != allocation.SelectedCapacityBytes {
		t.Fatalf("runtime parent = %#v", parent)
	}
	if parent.Volumes[allocation.State] != 1 || snapshot.AllocationCounts[configured.Name][allocation.State] != 1 || len(snapshot.HistoricalCounts) != 0 {
		t.Fatalf("runtime allocation counts = %#v / %#v", parent.Volumes, snapshot.AllocationCounts)
	}
	if len(access.calls) != 1 || access.calls[0] != "verify:"+allocation.ParentFilesystemID || len(sampler.calls) != 1 {
		t.Fatalf("runtime observation access/statfs calls = %#v / %#v", access.calls, sampler.calls)
	}
}

func TestProductionParentPlacerRefreshReportsSizeRegressionWithoutUsingStatFS(t *testing.T) {
	placer, provider, access, sampler, manual, allocation, _ := placementHarness(t)
	if _, err := placer.RefreshRuntimeSnapshot(context.Background()); err != nil {
		t.Fatalf("RefreshRuntimeSnapshot(initial) error = %v", err)
	}
	metadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	metadata.SizeBytes--
	provider.Filesystems["fr-par/"+allocation.ParentFilesystemID] = metadata
	manual.Advance(time.Second)
	snapshot, err := placer.RefreshRuntimeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RefreshRuntimeSnapshot(regression) error = %v", err)
	}
	parent := snapshot.Parents[0]
	if parent.Metadata.Condition != pool.ParentConditionCriticalSizeRegression || parent.Metadata.ObservedSizeBytes != metadata.SizeBytes || parent.Metadata.AcceptedSizeBytes != metadata.SizeBytes+1 {
		t.Fatalf("regression metadata = %#v", parent.Metadata)
	}
	if !parent.StatFS.ObservedAt.IsZero() || len(access.calls) != 1 || len(sampler.calls) != 1 {
		t.Fatalf("regression touched statfs: parent=%#v access=%#v samples=%#v", parent, access.calls, sampler.calls)
	}
}

func TestProductionParentPlacerSkipsParentLocalProviderCondition(t *testing.T) {
	placer, provider, access, sampler, _, allocation, configured := placementHarness(t)
	const secondParent = "88888888-8888-4888-8888-888888888888"
	configured.MaxParentsPerEligibleNode = 2
	configured.Filesystems = append(configured.Filesystems, pool.ParentConfig{ID: secondParent, Name: "parent-b", State: pool.ParentActive})
	placer.pools[configured.Name] = configured
	placer.trackers[secondParent] = &pool.ParentMetadataTracker{}
	firstMetadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	firstMetadata.Status = scaleway.FilesystemUpdating
	provider.Filesystems["fr-par/"+allocation.ParentFilesystemID] = firstMetadata
	provider.Filesystems["fr-par/"+secondParent] = scaleway.Filesystem{
		ID: secondParent, ProjectID: "99999999-9999-4999-8999-999999999999",
		Region: "fr-par", SizeBytes: firstMetadata.SizeBytes, Status: scaleway.FilesystemAvailable,
	}
	secondRoot := "/controller-parents/" + secondParent
	access.roots[secondParent] = secondRoot
	sampler.samples[secondRoot] = pool.StatFSSample{BlockSizeBytes: 1, AvailableBlocks: int64(firstMetadata.SizeBytes), ObservedAt: time.Unix(1, 0)}

	request := validCreateRequest()
	request.Parameters.PoolName = configured.Name
	logicalID, _ := volume.LogicalVolumeID(driverTestName, "provider-local-condition")
	placement, err := placer.Place(context.Background(), request, 1, logicalID)
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if placement.ParentFilesystemID != secondParent {
		t.Fatalf("Place() parent = %q, want healthy %q", placement.ParentFilesystemID, secondParent)
	}
	if len(access.calls) != 1 || access.calls[0] != secondParent {
		t.Fatalf("parent-local provider condition mount calls = %#v", access.calls)
	}
}

func TestProductionParentPlacerSkipsParentLocalMountFailure(t *testing.T) {
	placer, provider, access, sampler, _, allocation, configured := placementHarness(t)
	const secondParent = "88888888-8888-4888-8888-888888888888"
	configured.MaxParentsPerEligibleNode = 2
	configured.Filesystems = append(configured.Filesystems, pool.ParentConfig{ID: secondParent, Name: "parent-b", State: pool.ParentActive})
	placer.pools[configured.Name] = configured
	placer.trackers[secondParent] = &pool.ParentMetadataTracker{}
	firstMetadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	provider.Filesystems["fr-par/"+secondParent] = scaleway.Filesystem{
		ID: secondParent, ProjectID: firstMetadata.ProjectID, Region: firstMetadata.Region,
		SizeBytes: firstMetadata.SizeBytes, Status: scaleway.FilesystemAvailable,
	}
	secondRoot := "/controller-parents/" + secondParent
	access.roots[secondParent] = secondRoot
	access.errors[allocation.ParentFilesystemID] = errors.New("mount unavailable")
	sampler.samples[secondRoot] = pool.StatFSSample{BlockSizeBytes: 1, AvailableBlocks: int64(firstMetadata.SizeBytes), ObservedAt: time.Unix(1, 0)}

	request := validCreateRequest()
	request.Parameters.PoolName = configured.Name
	logicalID, _ := volume.LogicalVolumeID(driverTestName, "mount-local-failure")
	placement, err := placer.Place(context.Background(), request, 1, logicalID)
	if err != nil {
		t.Fatalf("Place() error = %v", err)
	}
	if placement.ParentFilesystemID != secondParent {
		t.Fatalf("Place() parent = %q, want healthy %q", placement.ParentFilesystemID, secondParent)
	}
}

func TestProductionParentPlacerRefreshKeepsOtherParentsWhenOneReadFails(t *testing.T) {
	placer, provider, access, sampler, _, allocation, configured := placementHarness(t)
	const secondParent = "88888888-8888-4888-8888-888888888888"
	configured.MaxParentsPerEligibleNode = 2
	configured.Filesystems = append(configured.Filesystems, pool.ParentConfig{ID: secondParent, Name: "parent-b", State: pool.ParentActive})
	placer.pools[configured.Name] = configured
	placer.trackers[secondParent] = &pool.ParentMetadataTracker{}
	firstMetadata := provider.Filesystems["fr-par/"+allocation.ParentFilesystemID]
	provider.Filesystems["fr-par/"+secondParent] = scaleway.Filesystem{
		ID: secondParent, ProjectID: firstMetadata.ProjectID, Region: firstMetadata.Region,
		SizeBytes: firstMetadata.SizeBytes, Status: scaleway.FilesystemAvailable,
	}
	secondRoot := "/controller-parents/" + secondParent
	access.roots[secondParent] = secondRoot
	sampler.samples[secondRoot] = pool.StatFSSample{BlockSizeBytes: 1, AvailableBlocks: int64(firstMetadata.SizeBytes), ObservedAt: time.Unix(1, 0)}
	provider.InjectFault("get-filesystem", scaleway.ErrUnavailable)

	snapshot, err := placer.RefreshRuntimeSnapshot(context.Background())
	if err != nil {
		t.Fatalf("RefreshRuntimeSnapshot() error = %v", err)
	}
	if len(snapshot.Parents) != 2 {
		t.Fatalf("runtime parents = %#v", snapshot.Parents)
	}
	if snapshot.Parents[0].ParentFilesystemID != allocation.ParentFilesystemID || snapshot.Parents[0].DegradationReason != ParentDegradationProviderRead {
		t.Fatalf("degraded parent = %#v", snapshot.Parents[0])
	}
	if snapshot.Parents[1].ParentFilesystemID != secondParent || snapshot.Parents[1].DegradationError != nil {
		t.Fatalf("healthy parent = %#v", snapshot.Parents[1])
	}
}

func placementHarness(t *testing.T) (*ProductionParentPlacer, *scaleway.FakeAPI, *fakePlacementParentAccess, *fakeStatFSSampler, *clock.Manual, *volume.DetailedAllocationRecord, pool.Config) {
	t.Helper()
	allocation := creatingAllocation(t)
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	observedSize := allocation.SelectedCapacityBytes + 1024
	configured := pool.Config{
		Name: allocation.PoolName, BasePath: allocation.BasePath,
		SelectionPolicy: pool.SelectionLeastAllocated, MaxParentsPerEligibleNode: 1,
		MaxLogicalOvercommitRatio: ratio, DeletePolicy: allocation.DeletePolicy,
		DirectoryMode: allocation.DirectoryMode, DirectoryUID: allocation.DirectoryUID, DirectoryGID: allocation.DirectoryGID,
		Filesystems: []pool.ParentConfig{{ID: allocation.ParentFilesystemID, Name: "parent-a", State: pool.ParentActive}},
	}
	provider := scaleway.NewFakeAPI()
	provider.Filesystems["fr-par/"+allocation.ParentFilesystemID] = scaleway.Filesystem{
		ID: allocation.ParentFilesystemID, ProjectID: "99999999-9999-4999-8999-999999999999",
		Region: "fr-par", SizeBytes: observedSize, Status: scaleway.FilesystemAvailable,
	}
	root := "/controller-parents/" + allocation.ParentFilesystemID
	access := &fakePlacementParentAccess{
		roots: map[string]string{allocation.ParentFilesystemID: root}, errors: make(map[string]error),
	}
	manual := clock.NewManual(time.Unix(1, 0))
	sampler := &fakeStatFSSampler{samples: map[string]pool.StatFSSample{root: {
		BlockSizeBytes: 1, AvailableBlocks: int64(observedSize), ObservedAt: manual.Now(),
	}}, errors: make(map[string]error)}
	placer, err := NewProductionParentPlacer(
		allocation.DriverName, allocation.InstallationID, allocation.ActiveClusterUID,
		"fr-par", "99999999-9999-4999-8999-999999999999", []pool.Config{configured},
		&staticAllocationLister{stored: []k8s.StoredAllocation{{Record: allocation, ResourceVersion: "1"}}}, provider, access, sampler, manual,
	)
	if err != nil {
		t.Fatalf("NewProductionParentPlacer() error = %v", err)
	}
	return placer, provider, access, sampler, manual, allocation, configured
}
