package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrDeletionInProgress rejects a same-name create while the durable record
	// is already non-authorizing and deleting.
	ErrDeletionInProgress = errors.New("logical volume deletion is in progress")
	// ErrNamePermanentlyReserved enforces v1's no-reuse tombstone contract.
	ErrNamePermanentlyReserved = errors.New("logical volume name is permanently reserved by a terminal tombstone")
	// ErrReservationUnresolved means a create-if-absent request may still
	// commit after the last conclusive read. The selected pool must remain
	// closed to new placements until the permanent per-pool journal is
	// reconciled against the authoritative allocation store.
	ErrReservationUnresolved = errors.New("allocation reservation result is unresolved")
)

const (
	reservationResolveAttempts = 4
	reservationResolveBackoff  = 25 * time.Millisecond
	reservationResolveTimeout  = 5 * time.Second
)

// CreateRequest is the normalized provider-independent CreateVolume input.
type CreateRequest struct {
	Name          string
	RequiredBytes uint64
	LimitBytes    uint64
	Parameters    volume.CreateParameters
	PVCNamespace  string
	PVCName       string
}

// Placement is the authoritative new-allocation result after provider refresh,
// statfs, logical capacity, lifecycle, and node compatibility checks.
type Placement struct {
	ParentFilesystemID string
	BasePath           string
}

// CreateResponse is the immutable CSI volume projection.
type CreateResponse struct {
	VolumeHandle  string
	VolumeContext map[string]string
	CapacityBytes uint64
}

// AllocationStore is the exact ConfigMap CAS surface used by controller state
// machines.
type AllocationStore interface {
	Get(ctx context.Context, logicalVolumeID string) (k8s.StoredAllocation, error)
	Create(ctx context.Context, record volume.AllocationRecord) (k8s.StoredAllocation, error)
	Update(ctx context.Context, current k8s.StoredAllocation, next volume.AllocationRecord) (k8s.StoredAllocation, error)
}

// AllocationReservation is invoked exactly once while the selected pool lock
// is still held.  The callback must only build and persist the deterministic
// Reserved record; it must not acquire another pool lock.
type AllocationReservation func(Placement) (k8s.StoredAllocation, error)

// ParentPlacer performs provider refresh, controller attachment/mount, statfs,
// least-allocated selection, and the first durable reservation as one pool-
// locked operation for a conclusive new name.
type ParentPlacer interface {
	PlaceAndReserve(ctx context.Context, request CreateRequest, selectedCapacityBytes uint64, logicalVolumeID string, reserve AllocationReservation) (k8s.StoredAllocation, error)
	MarkPoolResolved(ctx context.Context, poolName string) error
}

// CreationFilesystem resumes the controller-owned directory and ownership
// protocol for one already-persisted CreatingDirectory mapping. It must return
// only after the data directory, mode/ownership, and detailed Ready ownership
// record are durable and read-back verified.
type CreationFilesystem interface {
	EnsureCreated(ctx context.Context, record *volume.DetailedAllocationRecord) error
}

// ReservationJournal is the permanent per-pool barrier written before the
// allocation ConfigMap. Its fixed-name CAS state survives leadership changes,
// unlike the process-local pool closure used only as an additional guard.
type ReservationJournal interface {
	Begin(ctx context.Context, poolName, clusterUID string, allocation *volume.DetailedAllocationRecord) (k8s.StoredReservationJournal, error)
	Get(ctx context.Context, poolName, clusterUID string) (k8s.StoredReservationJournal, error)
	ReadCommitted(ctx context.Context, clusterUID string) ([]k8s.StoredReservationJournal, error)
	FenceForPlacement(ctx context.Context, clusterUID, logicalVolumeID string) error
	CompleteExact(ctx context.Context, poolName, clusterUID string, allocation *volume.DetailedAllocationRecord) (k8s.StoredReservationJournal, bool, error)
}

// CreateController owns CreateVolume idempotency and its dual-write order.
type CreateController struct {
	driverName             string
	installationID         string
	clusterUID             string
	store                  AllocationStore
	journal                ReservationJournal
	placer                 ParentPlacer
	filesystem             CreationFilesystem
	gate                   *coordination.MutationGate
	volumeLocks            *coordination.KeyedLock
	clock                  clock.Clock
	reservationAuthorities []context.Context
}

// NewCreateController validates immutable process identity and dependencies.
func NewCreateController(driverName, installationID, clusterUID string, store AllocationStore, journal ReservationJournal, placer ParentPlacer, filesystem CreationFilesystem, gate *coordination.MutationGate, volumeLocks *coordination.KeyedLock, operationClock clock.Clock, reservationAuthorities ...context.Context) (*CreateController, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	if store == nil || journal == nil || placer == nil || filesystem == nil || gate == nil || volumeLocks == nil || operationClock == nil {
		return nil, fmt.Errorf("CreateVolume controller dependency is nil")
	}
	for _, authority := range reservationAuthorities {
		if authority == nil {
			return nil, fmt.Errorf("CreateVolume reservation authority context is nil")
		}
	}
	return &CreateController{
		driverName:             driverName,
		installationID:         installationID,
		clusterUID:             clusterUID,
		store:                  store,
		journal:                journal,
		placer:                 placer,
		filesystem:             filesystem,
		gate:                   gate,
		volumeLocks:            volumeLocks,
		clock:                  operationClock,
		reservationAuthorities: append([]context.Context(nil), reservationAuthorities...),
	}, nil
}

// Create performs one idempotent, crash-resumable logical-volume creation.
func (controller *CreateController) Create(ctx context.Context, request CreateRequest) (CreateResponse, error) {
	if request.Name == "" {
		return CreateResponse{}, fmt.Errorf("CreateVolume request name is empty")
	}
	parameters, err := request.Parameters.Normalize()
	if err != nil {
		return CreateResponse{}, err
	}
	request.Parameters = parameters
	selectedCapacity, err := volume.SelectCapacity(request.RequiredBytes, request.LimitBytes)
	if err != nil {
		return CreateResponse{}, err
	}
	logicalID, err := volume.LogicalVolumeID(controller.driverName, request.Name)
	if err != nil {
		return CreateResponse{}, err
	}

	releaseMutation, err := controller.gate.Acquire(ctx)
	if err != nil {
		return CreateResponse{}, err
	}
	defer releaseMutation()
	unlockVolume, err := controller.volumeLocks.Lock(ctx, logicalID)
	if err != nil {
		return CreateResponse{}, err
	}
	defer unlockVolume()

	stored, err := controller.store.Get(ctx, logicalID)
	if err == nil {
		if detailed, ok := stored.Record.(*volume.DetailedAllocationRecord); ok {
			// Validate the replay before changing the independent pool journal.
			// A schema-valid same-ID record with another mapping must preserve the
			// Pending evidence and fail closed.
			if err := volume.ValidateCreateReplay(detailed, request.Name, request.RequiredBytes, request.LimitBytes, request.Parameters); err != nil {
				return CreateResponse{}, err
			}
			poolIdle, err := controller.completeReservationJournal(detailed)
			if err != nil {
				return CreateResponse{}, err
			}
			if poolIdle {
				if err := controller.placer.MarkPoolResolved(ctx, detailed.PoolName); err != nil {
					return CreateResponse{}, fmt.Errorf("reopen pool after exact reservation resolution: %w", err)
				}
			}
		}
		return controller.resume(ctx, request, stored)
	}
	if !errors.Is(err, k8s.ErrNotFound) {
		return CreateResponse{}, err
	}
	pendingStored, found, resolveErr := controller.resumePendingReservation(ctx, request, logicalID)
	if resolveErr != nil {
		return CreateResponse{}, resolveErr
	} else if found {
		return controller.resume(ctx, request, pendingStored)
	}

	stored, err = controller.placer.PlaceAndReserve(ctx, request, selectedCapacity, logicalID, func(placement Placement) (k8s.StoredAllocation, error) {
		record, recordErr := controller.newReservedRecord(request, selectedCapacity, logicalID, placement)
		if recordErr != nil {
			return k8s.StoredAllocation{}, recordErr
		}
		if err := ctx.Err(); err != nil {
			return k8s.StoredAllocation{}, err
		}
		journalCtx, cancelJournal := controller.reservationResolutionContext()
		defer cancelJournal()
		if fenceErr := controller.journal.FenceForPlacement(journalCtx, controller.clusterUID, logicalID); fenceErr != nil {
			return k8s.StoredAllocation{}, fmt.Errorf("fence committed pool journals before fresh reservation: %w", fenceErr)
		}
		if _, journalErr := controller.journal.Begin(journalCtx, request.Parameters.PoolName, controller.clusterUID, record); journalErr != nil {
			if errors.Is(journalErr, k8s.ErrUnavailable) || errors.Is(journalErr, k8s.ErrConflict) ||
				errors.Is(journalErr, context.Canceled) || errors.Is(journalErr, context.DeadlineExceeded) {
				return k8s.StoredAllocation{}, fmt.Errorf("persist pool reservation intent before allocation: %w: %w", journalErr, ErrReservationUnresolved)
			}
			return k8s.StoredAllocation{}, journalErr
		}
		stored, reservationErr := controller.persistReservation(ctx, logicalID, record)
		if reservationErr != nil {
			return k8s.StoredAllocation{}, reservationErr
		}
		authoritative, ok := stored.Record.(*volume.DetailedAllocationRecord)
		if !ok {
			return k8s.StoredAllocation{}, fmt.Errorf("new reservation allocation kind %q is not detailed", stored.Record.Kind())
		}
		if _, completed, journalErr := controller.journal.CompleteExact(journalCtx, request.Parameters.PoolName, controller.clusterUID, authoritative); journalErr != nil || !completed {
			if journalErr == nil {
				journalErr = fmt.Errorf("matching Pending journal was not completed: %w", k8s.ErrConflict)
			}
			return k8s.StoredAllocation{}, fmt.Errorf("complete pool reservation journal after allocation became authoritative: %w: %w", journalErr, ErrReservationUnresolved)
		}
		return stored, nil
	})
	if err != nil {
		return CreateResponse{}, err
	}
	return controller.resume(ctx, request, stored)
}

func (controller *CreateController) completeReservationJournal(record *volume.DetailedAllocationRecord) (bool, error) {
	resolutionCtx, cancel := controller.reservationResolutionContext()
	defer cancel()
	journal, completed, err := controller.journal.CompleteExact(resolutionCtx, record.PoolName, controller.clusterUID, record)
	if err != nil {
		return false, fmt.Errorf("complete matching pool reservation journal: %w", err)
	}
	return completed || journal.Record.State == k8s.ReservationJournalIdle, nil
}

// resumePendingReservation resolves the one exact journal intent that may have
// outlived an ambiguous allocation Create. It searches the complete committed
// journal set because the pool in a retry is untrusted and may differ from the
// pool selected before the ambiguous allocation Create. It deliberately reuses
// the committed bytes instead of performing another placement. Multiple
// Pending intents for one logical volume are corruption and fail closed.
func (controller *CreateController) resumePendingReservation(ctx context.Context, request CreateRequest, logicalID string) (k8s.StoredAllocation, bool, error) {
	journals, err := controller.journal.ReadCommitted(ctx, controller.clusterUID)
	if err != nil {
		return k8s.StoredAllocation{}, false, fmt.Errorf("read committed reservation journals before placement: %w", err)
	}
	var requested *k8s.StoredReservationJournal
	var pending *volume.DetailedAllocationRecord
	for index := range journals {
		journal := &journals[index]
		if journal.Record.PoolName == request.Parameters.PoolName {
			requested = journal
		}
		candidate, decodeErr := journal.PendingAllocation()
		if decodeErr != nil {
			return k8s.StoredAllocation{}, false, fmt.Errorf("decode reservation journal for pool %q before placement: %w", journal.Record.PoolName, decodeErr)
		}
		if candidate == nil || candidate.LogicalVolumeID != logicalID {
			continue
		}
		if pending != nil {
			return k8s.StoredAllocation{}, false, fmt.Errorf("logical volume %q has Pending reservations in pools %q and %q: %w", logicalID, pending.PoolName, candidate.PoolName, k8s.ErrConflict)
		}
		pending = candidate
	}
	if requested == nil {
		return k8s.StoredAllocation{}, false, fmt.Errorf("requested pool %q is absent from the committed reservation journal set: %w", request.Parameters.PoolName, k8s.ErrConflict)
	}
	if pending == nil {
		requestedPending, decodeErr := requested.PendingAllocation()
		if decodeErr != nil {
			return k8s.StoredAllocation{}, false, fmt.Errorf("decode requested pool reservation journal before placement: %w", decodeErr)
		}
		if requestedPending != nil {
			return k8s.StoredAllocation{}, false, fmt.Errorf("pool %q has an unresolved reservation for logical volume %q: %w", requestedPending.PoolName, requestedPending.LogicalVolumeID, ErrReservationUnresolved)
		}
		// A previous Begin may have returned an ambiguous response before any
		// allocation POST was allowed. A conclusive Idle observation ends that
		// process-local closure. The installation-wide journal fence immediately
		// before the next Begin invalidates any late old CAS, including one aimed
		// at another pool.
		if err := controller.placer.MarkPoolResolved(ctx, request.Parameters.PoolName); err != nil {
			return k8s.StoredAllocation{}, false, fmt.Errorf("reopen conclusively Idle pool before placement: %w", err)
		}
		return k8s.StoredAllocation{}, false, nil
	}
	if err := volume.ValidateCreateReplay(pending, request.Name, request.RequiredBytes, request.LimitBytes, request.Parameters); err != nil {
		return k8s.StoredAllocation{}, false, err
	}
	stored, err := controller.persistReservation(ctx, logicalID, pending)
	if err != nil {
		return k8s.StoredAllocation{}, false, err
	}
	authoritative, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return k8s.StoredAllocation{}, false, fmt.Errorf("resolved reservation allocation kind %q is not detailed", stored.Record.Kind())
	}
	if err := volume.ValidateCreateReplay(authoritative, request.Name, request.RequiredBytes, request.LimitBytes, request.Parameters); err != nil {
		return k8s.StoredAllocation{}, false, err
	}
	poolIdle, err := controller.completeReservationJournal(authoritative)
	if err != nil {
		return k8s.StoredAllocation{}, false, err
	}
	if !poolIdle {
		return k8s.StoredAllocation{}, false, fmt.Errorf("pool %q reservation changed before exact completion: %w", pending.PoolName, ErrReservationUnresolved)
	}
	if err := controller.placer.MarkPoolResolved(ctx, pending.PoolName); err != nil {
		return k8s.StoredAllocation{}, false, fmt.Errorf("reopen pool after pending reservation recovery: %w", err)
	}
	return stored, true, nil
}

func (controller *CreateController) persistReservation(ctx context.Context, logicalID string, record *volume.DetailedAllocationRecord) (k8s.StoredAllocation, error) {
	operationCtx := ctx
	cleanup := func() {}
	detached := false
	mutationUnresolved := false
	defer func() { cleanup() }()
	for attempt := 0; attempt < reservationResolveAttempts; attempt++ {
		stored, createErr := controller.store.Create(operationCtx, record)
		if createErr == nil {
			return stored, nil
		}
		ambiguous := errors.Is(createErr, k8s.ErrUnavailable) || errors.Is(createErr, context.Canceled) || errors.Is(createErr, context.DeadlineExceeded)
		if !errors.Is(createErr, k8s.ErrAlreadyExists) && !ambiguous {
			if mutationUnresolved {
				// Once any POST may have reached the apiserver, a later locally
				// definitive error says nothing about that earlier POST. Preserve
				// the unresolved marker so the durable pool journal remains Pending
				// until this or a fenced successor generation resolves the record.
				return k8s.StoredAllocation{}, fmt.Errorf("allocation create retry failed before the earlier mutation was resolved: %w: %w", createErr, ErrReservationUnresolved)
			}
			return k8s.StoredAllocation{}, createErr
		}
		if !detached {
			operationCtx, cleanup = controller.reservationResolutionContext()
			detached = true
		}

		// Resolve both a same-name create race and an ambiguous API result while
		// the pool lock still excludes another selection. A single NotFound is
		// not conclusive here: the first POST may still commit after that GET.
		// Reissuing exactly the same create-if-absent request provides the
		// Kubernetes idempotency barrier without performing another placement.
		stored, readErr := controller.store.Get(operationCtx, logicalID)
		if readErr == nil {
			return stored, nil
		}
		if !errors.Is(readErr, k8s.ErrNotFound) {
			return k8s.StoredAllocation{}, fmt.Errorf("resolve allocation create after emitted mutation: %w: %w", readErr, ErrReservationUnresolved)
		}
		// An AlreadyExists followed by NotFound is also non-conclusive. The
		// observation may be stale or the named object may be in transition;
		// reopening aggregate capacity from that contradiction is unsafe.
		mutationUnresolved = true
		if attempt+1 == reservationResolveAttempts {
			return k8s.StoredAllocation{}, fmt.Errorf("allocation create result remained ambiguous after %d exact create attempts: %w: %w", reservationResolveAttempts, k8s.ErrUnavailable, ErrReservationUnresolved)
		}
		// Retry once immediately. Subsequent unresolved attempts back off while
		// still honoring the operation's leadership/shutdown-aware context.
		if attempt > 0 {
			if err := controller.waitReservationResolution(operationCtx, reservationResolveBackoff<<uint(attempt-1)); err != nil {
				return k8s.StoredAllocation{}, fmt.Errorf("wait to resolve allocation create after emitted mutation: %w: %w", err, ErrReservationUnresolved)
			}
		}
	}
	return k8s.StoredAllocation{}, fmt.Errorf("allocation create resolution exhausted unexpectedly: %w: %w", k8s.ErrUnavailable, ErrReservationUnresolved)
}

func (controller *CreateController) reservationResolutionContext() (context.Context, context.CancelFunc) {
	resolution, cancel := context.WithTimeout(context.Background(), reservationResolveTimeout)
	stops := make([]func() bool, 0, len(controller.reservationAuthorities))
	for _, authority := range controller.reservationAuthorities {
		stops = append(stops, context.AfterFunc(authority, cancel))
		if authority.Err() != nil {
			cancel()
		}
	}
	return resolution, func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
}

func (controller *CreateController) waitReservationResolution(ctx context.Context, delay time.Duration) error {
	timer := controller.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

func (controller *CreateController) resume(ctx context.Context, request CreateRequest, stored k8s.StoredAllocation) (CreateResponse, error) {
	record, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return CreateResponse{}, fmt.Errorf("same-name allocation is terminal kind %q: %w", stored.Record.Kind(), ErrNamePermanentlyReserved)
	}
	if err := volume.ValidateCreateReplay(record, request.Name, request.RequiredBytes, request.LimitBytes, request.Parameters); err != nil {
		return CreateResponse{}, err
	}
	return controller.resumeStoredCreation(ctx, stored)
}

// ReconcileExistingCreation resumes only an already-persisted Reserved or
// CreatingDirectory crash window. It never performs placement and therefore
// cannot move a volume to a different parent when the original CSI request is
// unavailable during startup.
func (controller *CreateController) ReconcileExistingCreation(ctx context.Context, logicalVolumeID string) error {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	releaseMutation, err := controller.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer releaseMutation()
	unlock, err := controller.volumeLocks.Lock(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	defer unlock()
	stored, err := controller.store.Get(ctx, logicalVolumeID)
	if err != nil {
		return err
	}
	record, ok := stored.Record.(*volume.DetailedAllocationRecord)
	if !ok {
		return fmt.Errorf("startup creation record kind %q is not detailed", stored.Record.Kind())
	}
	if record.DriverName != controller.driverName || record.InstallationID != controller.installationID || record.ActiveClusterUID != controller.clusterUID || record.LogicalVolumeID != logicalVolumeID {
		return fmt.Errorf("startup creation record belongs to another driver installation, cluster, or logical ID")
	}
	if record.State != volume.StateReserved && record.State != volume.StateCreatingDirectory {
		return fmt.Errorf("startup creation reconciliation requires Reserved or CreatingDirectory, got %q", record.State)
	}
	poolIdle, err := controller.completeReservationJournal(record)
	if err != nil {
		return fmt.Errorf("authorize creation lifecycle from reservation journal: %w", err)
	}
	if poolIdle {
		if err := controller.placer.MarkPoolResolved(ctx, record.PoolName); err != nil {
			return fmt.Errorf("reopen pool after lifecycle reservation resolution: %w", err)
		}
	}
	_, err = controller.resumeStoredCreation(ctx, stored)
	return err
}

func (controller *CreateController) resumeStoredCreation(ctx context.Context, stored k8s.StoredAllocation) (CreateResponse, error) {
	record := stored.Record.(*volume.DetailedAllocationRecord)
	if err := validateAllocationRuntimeIdentity(record, controller.driverName, controller.installationID, controller.clusterUID); err != nil {
		return CreateResponse{}, err
	}

	switch record.State {
	case volume.StateReady:
		return createResponse(record)
	case volume.StateDeleting:
		return CreateResponse{}, ErrDeletionInProgress
	case volume.StateArchived, volume.StateRetained, volume.StateDeleted:
		return CreateResponse{}, ErrNamePermanentlyReserved
	case volume.StateReserved:
		next := cloneDetailedAllocation(record)
		next.RecordRevision++
		next.State = volume.StateCreatingDirectory
		next.UpdatedAt = canonicalNow(controller.clock.Now())
		updated, err := controller.store.Update(ctx, stored, next)
		if err != nil {
			return CreateResponse{}, err
		}
		stored = updated
		record = updated.Record.(*volume.DetailedAllocationRecord)
	case volume.StateCreatingDirectory:
	default:
		return CreateResponse{}, fmt.Errorf("allocation state %q cannot resume CreateVolume", record.State)
	}

	if err := controller.filesystem.EnsureCreated(ctx, record); err != nil {
		return CreateResponse{}, err
	}
	ready := cloneDetailedAllocation(record)
	ready.RecordRevision++
	ready.State = volume.StateReady
	ready.UpdatedAt = canonicalNow(controller.clock.Now())
	updated, err := controller.store.Update(ctx, stored, ready)
	if err != nil {
		return CreateResponse{}, err
	}
	return createResponse(updated.Record.(*volume.DetailedAllocationRecord))
}

func (controller *CreateController) newReservedRecord(request CreateRequest, selectedCapacity uint64, logicalID string, placement Placement) (*volume.DetailedAllocationRecord, error) {
	if err := volume.ValidateParentFilesystemID(placement.ParentFilesystemID); err != nil {
		return nil, err
	}
	if err := volume.ValidateBasePath(placement.BasePath); err != nil {
		return nil, err
	}
	directoryName, err := volume.DirectoryName(request.PVCNamespace, request.PVCName, logicalID)
	if err != nil {
		return nil, err
	}
	mapping := volume.Mapping{
		PoolName:           request.Parameters.PoolName,
		ParentFilesystemID: placement.ParentFilesystemID,
		BasePath:           placement.BasePath,
		DirectoryName:      directoryName,
		LogicalVolumeID:    logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		return nil, err
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		return nil, err
	}
	basePathHash, err := volume.BasePathHash(placement.BasePath)
	if err != nil {
		return nil, err
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{
		OriginalRequiredBytes: request.RequiredBytes,
		OriginalLimitBytes:    request.LimitBytes,
		SelectedCapacityBytes: selectedCapacity,
		Parameters:            request.Parameters,
	})
	if err != nil {
		return nil, err
	}
	now := canonicalNow(controller.clock.Now())
	record := &volume.DetailedAllocationRecord{
		SchemaVersion:              volume.SchemaVersionV1,
		RecordKind:                 volume.AllocationRecordDetailed,
		RecordRevision:             1,
		DriverName:                 controller.driverName,
		ActiveClusterUID:           controller.clusterUID,
		State:                      volume.StateReserved,
		InstallationID:             controller.installationID,
		CreateVolumeRequestName:    request.Name,
		RequestHash:                requestHash,
		OriginalRequiredBytes:      request.RequiredBytes,
		OriginalLimitBytes:         request.LimitBytes,
		SelectedCapacityBytes:      selectedCapacity,
		NormalizedCreateParameters: request.Parameters,
		LogicalVolumeID:            logicalID,
		VolumeHandle:               handle.String(),
		VolumeHandleHash:           handleHash,
		MappingHash:                handle.MappingHash,
		PoolName:                   request.Parameters.PoolName,
		ParentFilesystemID:         placement.ParentFilesystemID,
		BasePath:                   placement.BasePath,
		BasePathHash:               basePathHash,
		DirectoryName:              directoryName,
		ReservesCapacity:           true,
		DeletePolicy:               request.Parameters.DeletePolicy,
		DirectoryUID:               request.Parameters.DirectoryUID,
		DirectoryGID:               request.Parameters.DirectoryGID,
		DirectoryMode:              request.Parameters.DirectoryMode,
		CreatedAt:                  now,
		UpdatedAt:                  now,
		PublishedNodeIDs:           []string{},
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	// The exact CSI map must be encodable before the first durable reservation.
	// Helm validation is not a runtime trust boundary, and a Ready allocation
	// which can never be returned to the CO would permanently consume its name
	// and capacity.
	if _, err := createResponse(record); err != nil {
		return nil, fmt.Errorf("build immutable volume context before reservation: %w", err)
	}
	return record, nil
}

func createResponse(record *volume.DetailedAllocationRecord) (CreateResponse, error) {
	contextValue := volume.ImmutableContext{
		SchemaVersion:      volume.SchemaVersionV1,
		InstallationID:     record.InstallationID,
		ActiveClusterUID:   record.ActiveClusterUID,
		PoolName:           record.PoolName,
		ParentFilesystemID: record.ParentFilesystemID,
		BasePath:           record.BasePath,
		BasePathHash:       record.BasePathHash,
		DirectoryName:      record.DirectoryName,
		DirectoryMode:      record.DirectoryMode,
		DirectoryUID:       record.DirectoryUID,
		DirectoryGID:       record.DirectoryGID,
		DeletePolicy:       record.DeletePolicy,
		LogicalVolumeID:    record.LogicalVolumeID,
	}
	encodedContext, err := contextValue.Map()
	if err != nil {
		return CreateResponse{}, err
	}
	return CreateResponse{
		VolumeHandle:  record.VolumeHandle,
		VolumeContext: encodedContext,
		CapacityBytes: record.SelectedCapacityBytes,
	}, nil
}

func cloneDetailedAllocation(record *volume.DetailedAllocationRecord) *volume.DetailedAllocationRecord {
	clone := *record
	clone.PublishedNodeIDs = slices.Clone(record.PublishedNodeIDs)
	clone.NormalizedCreateParameters.AccessModes = slices.Clone(record.NormalizedCreateParameters.AccessModes)
	return &clone
}

func canonicalNow(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
