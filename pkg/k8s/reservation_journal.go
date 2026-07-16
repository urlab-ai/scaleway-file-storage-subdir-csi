package k8s

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	reservationJournalNamePrefix = "sfs-subdir-pool-journal-"
	reservationJournalDataKey    = "journal.json"
	reservationJournalAppName    = "scaleway-sfs-subdir-csi-reservation-journal"
	reservationJournalSchema     = "1"
	reservationJournalSetName    = "sfs-subdir-reservation-journal-set"
	reservationJournalSetDataKey = "journal-set.json"
	reservationJournalSetAppName = "scaleway-sfs-subdir-csi-reservation-journal-set"
	reservationTransitionRetries = 4
)

// ReservationJournalSetState is the closed bootstrap state for the complete
// installation-wide journal set. Initializing retains the previously committed
// pools separately from pending additions, so a crash never makes a missing
// established journal look like a new pool.
type ReservationJournalSetState string

const (
	ReservationJournalSetInitializing ReservationJournalSetState = "Initializing"
	ReservationJournalSetReady        ReservationJournalSetState = "Ready"
)

// ReservationJournalSetRecord commits the exact set of permanent per-pool
// journals. Pools are already authoritative. PendingPools are additions whose
// generation-zero Idle journals may still be created before the next Ready
// transition. Removing a committed pool is deliberately unsupported in v1.
type ReservationJournalSetRecord struct {
	SchemaVersion    string                     `json:"schemaVersion"`
	DriverName       string                     `json:"driverName"`
	InstallationID   string                     `json:"installationID"`
	ActiveClusterUID string                     `json:"activeClusterUID"`
	Generation       uint64                     `json:"generation"`
	State            ReservationJournalSetState `json:"state"`
	Pools            []string                   `json:"pools"`
	PendingPools     []string                   `json:"pendingPools,omitempty"`
}

// StoredReservationJournalSet pairs the validated set commitment with its
// Kubernetes CAS identity.
type StoredReservationJournalSet struct {
	Record          ReservationJournalSetRecord
	UID             string
	ResourceVersion string
}

// StoredReservationJournalObject is the exact checkpoint projection and
// Kubernetes source generation of either the journal-set commitment or one
// permanent per-pool journal.
type StoredReservationJournalObject struct {
	Name                  string
	UID                   string
	ResourceVersion       string
	RecoverableProjection []byte
}

// ReservationJournalState is the closed durable state of one pool's single
// serialized reservation slot.
type ReservationJournalState string

const (
	ReservationJournalIdle    ReservationJournalState = "Idle"
	ReservationJournalPending ReservationJournalState = "Pending"
)

// ReservationJournalRecord is the permanent per-pool barrier written before
// any allocation POST. The ConfigMap is never deleted or name-reused; CAS and a
// monotonic generation make a late update from an older controller conflict
// with a successor instead of silently reopening aggregate capacity.
type ReservationJournalRecord struct {
	SchemaVersion    string                  `json:"schemaVersion"`
	DriverName       string                  `json:"driverName"`
	InstallationID   string                  `json:"installationID"`
	ActiveClusterUID string                  `json:"activeClusterUID"`
	PoolName         string                  `json:"poolName"`
	Generation       uint64                  `json:"generation"`
	State            ReservationJournalState `json:"state"`
	LogicalVolumeID  string                  `json:"logicalVolumeID,omitempty"`
	AllocationRecord json.RawMessage         `json:"allocationRecord,omitempty"`
}

// StoredReservationJournal pairs the validated journal with Kubernetes CAS
// identity. Callers must never manufacture ResourceVersion values.
type StoredReservationJournal struct {
	Record          ReservationJournalRecord
	UID             string
	ResourceVersion string
}

// PendingAllocation decodes and validates the exact allocation guarded by a
// Pending journal. Idle journals return nil.
func (stored StoredReservationJournal) PendingAllocation() (*volume.DetailedAllocationRecord, error) {
	if stored.Record.State == ReservationJournalIdle {
		return nil, nil
	}
	record, err := volume.DecodeAllocationRecord(stored.Record.AllocationRecord)
	if err != nil {
		return nil, err
	}
	detailed, ok := record.(*volume.DetailedAllocationRecord)
	if !ok {
		return nil, fmt.Errorf("reservation journal allocation has unsupported type %T", record)
	}
	return detailed, nil
}

// ReservationJournalStore owns the permanent ConfigMap barrier for every
// configured pool. It intentionally exposes only begin/complete/reconcile, not
// generic writes.
type ReservationJournalStore struct {
	client         ConfigMapClient
	namespace      string
	driverName     string
	installationID string
}

// NewReservationJournalStore validates the immutable journal scope.
func NewReservationJournalStore(client ConfigMapClient, namespace, driverName, installationID string) (*ReservationJournalStore, error) {
	if client == nil {
		return nil, fmt.Errorf("reservation journal ConfigMap client is nil")
	}
	if namespace == "" || len(namespace) > 63 {
		return nil, fmt.Errorf("reservation journal namespace must contain 1 to 63 bytes")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return nil, err
	}
	return &ReservationJournalStore{client: client, namespace: namespace, driverName: driverName, installationID: installationID}, nil
}

// BootstrapFresh creates the installation-wide journal-set commitment before
// the fresh-installation verifier performs any provider or filesystem
// mutation. It may resume only its own Initializing record. Once Ready, missing
// committed journals are corruption and are never recreated by this method.
func (store *ReservationJournalStore) BootstrapFresh(ctx context.Context, pools []string, clusterUID string) error {
	configured, err := normalizeJournalPools(pools)
	if err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return err
	}
	current, err := store.GetSet(ctx, clusterUID)
	if errors.Is(err, ErrNotFound) {
		initial := ReservationJournalSetRecord{
			SchemaVersion: reservationJournalSchema, DriverName: store.driverName,
			InstallationID: store.installationID, ActiveClusterUID: clusterUID,
			State: ReservationJournalSetInitializing, Pools: []string{}, PendingPools: configured,
		}
		object, objectErr := store.objectForSet(initial, "")
		if objectErr != nil {
			return objectErr
		}
		created, createErr := store.client.Create(ctx, object)
		if createErr == nil {
			current, err = store.decodeSetObject(created, clusterUID)
		} else {
			observed, getErr := store.GetSet(ctx, clusterUID)
			if getErr != nil {
				return fmt.Errorf("resolve reservation journal-set bootstrap: %w", errors.Join(createErr, getErr))
			}
			current, err = observed, nil
		}
	}
	if err != nil {
		return err
	}
	if current.Record.State == ReservationJournalSetReady {
		if !slices.Equal(current.Record.Pools, configured) {
			return fmt.Errorf("ready reservation journal set differs from fresh configuration")
		}
		_, err := store.ReadCommittedSet(ctx, configured, clusterUID, false)
		return err
	}
	if len(current.Record.Pools) != 0 || !slices.Equal(current.Record.PendingPools, configured) {
		return fmt.Errorf("initializing reservation journal set differs from fresh configuration")
	}
	return store.completeSetInitialization(ctx, current, clusterUID)
}

// EnsureConfigured validates the committed journal set before serving and
// safely initializes only newly added pools. A journal already listed in Pools
// is permanent: NotFound is always fatal. Removing a committed pool online is
// unsupported in v1.
func (store *ReservationJournalStore) EnsureConfigured(ctx context.Context, pools []string, clusterUID string) error {
	configured, err := normalizeJournalPools(pools)
	if err != nil {
		return err
	}
	current, err := store.GetSet(ctx, clusterUID)
	if err != nil {
		return fmt.Errorf("read committed reservation journal set: %w", err)
	}
	if current.Record.State == ReservationJournalSetInitializing {
		for _, poolName := range current.Record.Pools {
			if !slices.Contains(configured, poolName) {
				return fmt.Errorf("configured pools removed committed journal %q", poolName)
			}
		}
		target := append(slices.Clone(current.Record.Pools), current.Record.PendingPools...)
		slices.Sort(target)
		if !slices.Equal(target, configured) {
			return fmt.Errorf("configured pools differ from in-progress journal-set extension")
		}
		return store.completeSetInitialization(ctx, current, clusterUID)
	}
	for _, poolName := range current.Record.Pools {
		if !slices.Contains(configured, poolName) {
			return fmt.Errorf("configured pools removed committed journal %q", poolName)
		}
	}
	if len(current.Record.Pools) != 0 {
		if _, err := store.ReadCommittedSet(ctx, current.Record.Pools, clusterUID, false); err != nil {
			return err
		}
	}
	additions := make([]string, 0)
	for _, poolName := range configured {
		if !slices.Contains(current.Record.Pools, poolName) {
			additions = append(additions, poolName)
		}
	}
	if len(additions) == 0 {
		return nil
	}
	next := current.Record
	next.Generation++
	next.State = ReservationJournalSetInitializing
	next.PendingPools = additions
	initializing, err := store.transitionSet(ctx, current, next)
	if err != nil {
		return fmt.Errorf("begin reservation journal-set extension: %w", err)
	}
	return store.completeSetInitialization(ctx, initializing, clusterUID)
}

// GetSet reads the exact installation-wide journal-set commitment.
func (store *ReservationJournalStore) GetSet(ctx context.Context, clusterUID string) (StoredReservationJournalSet, error) {
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return StoredReservationJournalSet{}, err
	}
	object, err := store.client.Get(ctx, store.namespace, reservationJournalSetName)
	if err != nil {
		return StoredReservationJournalSet{}, fmt.Errorf("get reservation journal-set ConfigMap %q: %w", reservationJournalSetName, err)
	}
	return store.decodeSetObject(object, clusterUID)
}

// ReadConfigured returns all committed journals in stable pool order. When
// requireIdle is true, any unresolved reservation fails closed.
func (store *ReservationJournalStore) ReadConfigured(ctx context.Context, pools []string, clusterUID string, requireIdle bool) ([]StoredReservationJournal, error) {
	configured, err := normalizeJournalPools(pools)
	if err != nil {
		return nil, err
	}
	result := make([]StoredReservationJournal, 0, len(configured))
	for _, poolName := range configured {
		journal, err := store.Get(ctx, poolName, clusterUID)
		if err != nil {
			return nil, fmt.Errorf("read permanent reservation journal for pool %q: %w", poolName, err)
		}
		if requireIdle && journal.Record.State != ReservationJournalIdle {
			return nil, fmt.Errorf("pool %q reservation journal is %q, want Idle", poolName, journal.Record.State)
		}
		result = append(result, journal)
	}
	return result, nil
}

// ReadCommittedSet validates the Ready installation-wide commitment, every
// member journal, and the absence of uncommitted journal objects. Pending
// member state is allowed only when requireIdle is false.
func (store *ReservationJournalStore) ReadCommittedSet(ctx context.Context, pools []string, clusterUID string, requireIdle bool) ([]StoredReservationJournal, error) {
	configured, err := normalizeJournalPools(pools)
	if err != nil {
		return nil, err
	}
	set, err := store.GetSet(ctx, clusterUID)
	if err != nil {
		return nil, err
	}
	if set.Record.State != ReservationJournalSetReady || !slices.Equal(set.Record.Pools, configured) {
		return nil, fmt.Errorf("reservation journal set is not Ready for the exact configured pools")
	}
	journals, err := store.ReadConfigured(ctx, configured, clusterUID, requireIdle)
	if err != nil {
		return nil, err
	}
	expectedNames := map[string]struct{}{reservationJournalSetName: {}}
	for _, journal := range journals {
		expectedNames[reservationJournalName(journal.Record.PoolName)] = struct{}{}
	}
	listed, err := store.client.List(ctx, store.namespace, nil)
	if err != nil {
		return nil, fmt.Errorf("list complete reservation journal namespace: %w", err)
	}
	for _, object := range listed {
		if !IsReservationJournalName(object.Name) {
			continue
		}
		if _, present := expectedNames[object.Name]; !present {
			return nil, fmt.Errorf("reservation journal %q is absent from the committed set", object.Name)
		}
	}
	return journals, nil
}

// ReadCommitted validates and returns the complete Ready journal set using the
// pools named by the durable set commitment itself. This is the authority for
// lookups that must span every pool, such as a same-name CreateVolume retry:
// consulting only the pool supplied by an untrusted retry could overlook an
// older Pending intent in another committed pool.
func (store *ReservationJournalStore) ReadCommitted(ctx context.Context, clusterUID string) ([]StoredReservationJournal, error) {
	set, err := store.GetSet(ctx, clusterUID)
	if err != nil {
		return nil, err
	}
	if set.Record.State != ReservationJournalSetReady {
		return nil, fmt.Errorf("reservation journal set is %q, want Ready", set.Record.State)
	}
	return store.ReadCommittedSet(ctx, set.Record.Pools, clusterUID, false)
}

// FenceForPlacement advances every currently Idle journal before a caller may
// begin a fresh reservation for logicalVolumeID. A client can lose an Update
// response while the API server is still able to commit that Update. If a
// retry selects another pool, advancing only the new pool journal cannot fence
// the old request because the two CAS operations target different ConfigMaps.
//
// Advancing all Idle journals makes the ordering conclusive: either an older
// Begin commits first and this method observes a conflict/Pending intent, or
// this method commits first and the older Begin uses a stale resourceVersion.
// Pending reservations for other logical volumes already advance their own
// journal and therefore need no additional write here.
func (store *ReservationJournalStore) FenceForPlacement(ctx context.Context, clusterUID, logicalVolumeID string) error {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	journals, err := store.ReadCommitted(ctx, clusterUID)
	if err != nil {
		return err
	}
	for _, current := range journals {
		pending, err := current.PendingAllocation()
		if err != nil {
			return fmt.Errorf("decode reservation journal for pool %q while fencing placement: %w", current.Record.PoolName, err)
		}
		if pending != nil {
			if pending.LogicalVolumeID == logicalVolumeID {
				return fmt.Errorf("logical volume %q became Pending in pool %q while fencing placement: %w", logicalVolumeID, current.Record.PoolName, ErrConflict)
			}
			continue
		}
		next := current.Record
		next.Generation++
		if _, err := store.transition(ctx, current, next); err != nil {
			return fmt.Errorf("fence Idle reservation journal for pool %q: %w", current.Record.PoolName, err)
		}
	}
	return nil
}

// CheckpointObjects returns the complete exact journal set only when the set is
// Ready, equals the configured pools, and every permanent journal is Idle. The
// global checkpoint gate must already have drained mutations before this read.
func (store *ReservationJournalStore) CheckpointObjects(ctx context.Context, pools []string, clusterUID string) ([]StoredReservationJournalObject, error) {
	configured, err := normalizeJournalPools(pools)
	if err != nil {
		return nil, err
	}
	set, err := store.GetSet(ctx, clusterUID)
	if err != nil {
		return nil, err
	}
	if set.Record.State != ReservationJournalSetReady || !slices.Equal(set.Record.Pools, configured) {
		return nil, fmt.Errorf("reservation journal set is not Ready for the exact configured pools")
	}
	setProjection, err := canonicaljson.Marshal(set.Record)
	if err != nil {
		return nil, err
	}
	objects := []StoredReservationJournalObject{{
		Name: reservationJournalSetName, UID: set.UID, ResourceVersion: set.ResourceVersion,
		RecoverableProjection: setProjection,
	}}
	journals, err := store.ReadCommittedSet(ctx, configured, clusterUID, true)
	if err != nil {
		return nil, err
	}
	for _, journal := range journals {
		projection, err := canonicaljson.Marshal(journal.Record)
		if err != nil {
			return nil, err
		}
		objects = append(objects, StoredReservationJournalObject{
			Name: reservationJournalName(journal.Record.PoolName), UID: journal.UID,
			ResourceVersion: journal.ResourceVersion, RecoverableProjection: projection,
		})
	}
	slices.SortFunc(objects, func(left, right StoredReservationJournalObject) int {
		return strings.Compare(left.Name, right.Name)
	})
	return objects, nil
}

// RestoreCheckpointObjects verifies or recreates one complete all-Idle journal
// set during the offline checkpoint-restore workflow. Per-pool objects are
// created before the set commitment, and callers create the checkpoint Secret
// only after this method and all allocation restores have converged.
func (store *ReservationJournalStore) RestoreCheckpointObjects(ctx context.Context, setRecord ReservationJournalSetRecord, journals []ReservationJournalRecord, execute bool) (created, missing []string, returnErr error) {
	if err := store.validateSetRecord(setRecord); err != nil {
		return nil, nil, err
	}
	if setRecord.State != ReservationJournalSetReady {
		return nil, nil, fmt.Errorf("restored reservation journal set is not Ready")
	}
	expected := make(map[string]ConfigMap, len(journals)+1)
	setObject, err := store.objectForSet(setRecord, "")
	if err != nil {
		return nil, nil, err
	}
	expected[setObject.Name] = setObject
	seenPools := make([]string, 0, len(journals))
	for _, record := range journals {
		if err := store.validateRecord(record); err != nil {
			return nil, nil, err
		}
		if record.State != ReservationJournalIdle {
			return nil, nil, fmt.Errorf("restored reservation journal for pool %q is not Idle", record.PoolName)
		}
		object, err := store.objectForRecord(record, "")
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := expected[object.Name]; duplicate {
			return nil, nil, fmt.Errorf("restored reservation journal %q is duplicated", object.Name)
		}
		expected[object.Name] = object
		seenPools = append(seenPools, record.PoolName)
	}
	slices.Sort(seenPools)
	if !slices.Equal(seenPools, setRecord.Pools) {
		return nil, nil, fmt.Errorf("restored reservation journals differ from committed pool set")
	}

	observed, err := store.client.List(ctx, store.namespace, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("list restored reservation journals: %w", err)
	}
	present := make(map[string]struct{}, len(observed))
	for _, object := range observed {
		if !IsReservationJournalName(object.Name) {
			continue
		}
		want, ok := expected[object.Name]
		if !ok {
			return nil, nil, fmt.Errorf("live reservation journal %q is absent from checkpoint", object.Name)
		}
		if err := equalReservationJournalObject(object, want); err != nil {
			return nil, nil, fmt.Errorf("live reservation journal %q: %w", object.Name, err)
		}
		present[object.Name] = struct{}{}
	}
	for name := range expected {
		if _, ok := present[name]; !ok {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	if _, setPresent := present[reservationJournalSetName]; setPresent && len(missing) != 0 {
		return nil, nil, fmt.Errorf("committed reservation journal set exists while permanent journals are missing")
	}
	if !execute {
		return nil, slices.Clone(missing), nil
	}
	for _, name := range missing {
		if name == reservationJournalSetName {
			continue
		}
		if err := store.createRestoreObject(ctx, expected[name]); err != nil {
			return nil, nil, err
		}
		created = append(created, name)
	}
	if _, setPresent := present[reservationJournalSetName]; !setPresent {
		if err := store.createRestoreObject(ctx, setObject); err != nil {
			return nil, nil, err
		}
		created = append(created, reservationJournalSetName)
	}
	if _, err := store.CheckpointObjects(ctx, setRecord.Pools, setRecord.ActiveClusterUID); err != nil {
		return nil, nil, fmt.Errorf("verify restored reservation journals: %w", err)
	}
	slices.Sort(created)
	return created, nil, nil
}

func (store *ReservationJournalStore) createRestoreObject(ctx context.Context, object ConfigMap) error {
	created, createErr := store.client.Create(ctx, object)
	if createErr == nil {
		return equalReservationJournalObject(created, object)
	}
	observed, getErr := store.client.Get(ctx, object.Namespace, object.Name)
	if getErr != nil {
		return fmt.Errorf("resolve restored reservation journal create %q: %w", object.Name, errors.Join(createErr, getErr))
	}
	if err := equalReservationJournalObject(observed, object); err != nil {
		return errors.Join(createErr, err)
	}
	return nil
}

func equalReservationJournalObject(observed, expected ConfigMap) error {
	if observed.Namespace != expected.Namespace || observed.Name != expected.Name || observed.UID == "" || observed.ResourceVersion == "" ||
		len(observed.Data) != len(expected.Data) {
		return fmt.Errorf("reservation journal object differs from exact checkpoint projection")
	}
	for key, value := range expected.Data {
		if observed.Data[key] != value {
			return fmt.Errorf("reservation journal data differs from exact checkpoint projection")
		}
	}
	for key, value := range expected.Labels {
		if observed.Labels[key] != value {
			return fmt.Errorf("reservation journal label %q differs from exact checkpoint projection", key)
		}
	}
	return nil
}

func (store *ReservationJournalStore) completeSetInitialization(ctx context.Context, current StoredReservationJournalSet, clusterUID string) error {
	if current.Record.State != ReservationJournalSetInitializing {
		return fmt.Errorf("reservation journal set is not initializing")
	}
	if len(current.Record.Pools) != 0 {
		if _, err := store.ReadConfigured(ctx, current.Record.Pools, clusterUID, false); err != nil {
			return err
		}
	}
	allowedNames := map[string]struct{}{reservationJournalSetName: {}}
	for _, poolName := range append(slices.Clone(current.Record.Pools), current.Record.PendingPools...) {
		allowedNames[reservationJournalName(poolName)] = struct{}{}
	}
	listed, err := store.client.List(ctx, store.namespace, nil)
	if err != nil {
		return fmt.Errorf("list journals during set initialization: %w", err)
	}
	for _, object := range listed {
		if !IsReservationJournalName(object.Name) {
			continue
		}
		if _, allowed := allowedNames[object.Name]; !allowed {
			return fmt.Errorf("reservation journal %q is outside the initializing set", object.Name)
		}
	}
	for _, poolName := range current.Record.PendingPools {
		if _, err := store.initializeJournal(ctx, poolName, clusterUID); err != nil {
			return fmt.Errorf("initialize permanent reservation journal for pool %q: %w", poolName, err)
		}
	}
	next := current.Record
	next.Generation++
	next.State = ReservationJournalSetReady
	next.Pools = append(slices.Clone(next.Pools), next.PendingPools...)
	slices.Sort(next.Pools)
	next.PendingPools = nil
	_, err = store.transitionSet(ctx, current, next)
	return err
}

func (store *ReservationJournalStore) initializeJournal(ctx context.Context, poolName, clusterUID string) (StoredReservationJournal, error) {
	idle := ReservationJournalRecord{
		SchemaVersion: reservationJournalSchema, DriverName: store.driverName,
		InstallationID: store.installationID, ActiveClusterUID: clusterUID,
		PoolName: poolName, State: ReservationJournalIdle,
	}
	object, err := store.objectForRecord(idle, "")
	if err != nil {
		return StoredReservationJournal{}, err
	}
	created, createErr := store.client.Create(ctx, object)
	if createErr == nil {
		return store.decodeObject(created, poolName, clusterUID)
	}
	observed, getErr := store.Get(ctx, poolName, clusterUID)
	if getErr != nil {
		return StoredReservationJournal{}, fmt.Errorf("resolve permanent reservation journal create: %w", errors.Join(createErr, getErr))
	}
	if observed.Record.Generation != 0 || observed.Record.State != ReservationJournalIdle {
		return StoredReservationJournal{}, fmt.Errorf("new reservation journal %q is not generation-zero Idle", poolName)
	}
	return observed, nil
}

// Get reads and validates one fixed-name journal.
func (store *ReservationJournalStore) Get(ctx context.Context, poolName, clusterUID string) (StoredReservationJournal, error) {
	if err := store.validateScope(poolName, clusterUID); err != nil {
		return StoredReservationJournal{}, err
	}
	name := reservationJournalName(poolName)
	object, err := store.client.Get(ctx, store.namespace, name)
	if err != nil {
		return StoredReservationJournal{}, fmt.Errorf("get reservation journal ConfigMap %q: %w", name, err)
	}
	return store.decodeObject(object, poolName, clusterUID)
}

// Begin durably records the exact allocation intent before its Create can be
// emitted. Replaying the same pending intent is idempotent; another intent
// keeps the pool closed.
func (store *ReservationJournalStore) Begin(ctx context.Context, poolName, clusterUID string, allocation *volume.DetailedAllocationRecord) (StoredReservationJournal, error) {
	if allocation == nil {
		return StoredReservationJournal{}, fmt.Errorf("reservation allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return StoredReservationJournal{}, err
	}
	if allocation.DriverName != store.driverName || allocation.InstallationID != store.installationID ||
		allocation.ActiveClusterUID != clusterUID || allocation.PoolName != poolName {
		return StoredReservationJournal{}, fmt.Errorf("reservation allocation does not match journal scope")
	}
	encoded, err := volume.EncodeAllocationRecord(allocation)
	if err != nil {
		return StoredReservationJournal{}, err
	}
	current, err := store.Get(ctx, poolName, clusterUID)
	if err != nil {
		return StoredReservationJournal{}, err
	}
	if current.Record.State == ReservationJournalPending {
		if current.Record.LogicalVolumeID == allocation.LogicalVolumeID && bytes.Equal(current.Record.AllocationRecord, encoded) {
			return current, nil
		}
		return StoredReservationJournal{}, fmt.Errorf("pool %q has unresolved reservation for logical volume %q: %w", poolName, current.Record.LogicalVolumeID, ErrConflict)
	}
	next := current.Record
	next.Generation++
	next.State = ReservationJournalPending
	next.LogicalVolumeID = allocation.LogicalVolumeID
	next.AllocationRecord = append(json.RawMessage(nil), encoded...)
	return store.transition(ctx, current, next)
}

// CompleteExact returns a matching Pending journal to Idle only after the
// exact canonical Reserved allocation is authoritative. A Pending intent for a
// different logical volume is left untouched so an existing-volume replay can
// remain idempotent without reopening aggregate capacity.
func (store *ReservationJournalStore) CompleteExact(ctx context.Context, poolName, clusterUID string, allocation *volume.DetailedAllocationRecord) (StoredReservationJournal, bool, error) {
	if allocation == nil {
		return StoredReservationJournal{}, false, fmt.Errorf("authoritative reservation allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return StoredReservationJournal{}, false, err
	}
	if allocation.PoolName != poolName || allocation.ActiveClusterUID != clusterUID ||
		allocation.DriverName != store.driverName || allocation.InstallationID != store.installationID {
		return StoredReservationJournal{}, false, fmt.Errorf("authoritative allocation does not match journal scope")
	}
	current, err := store.Get(ctx, poolName, clusterUID)
	if err != nil {
		return StoredReservationJournal{}, false, err
	}
	if current.Record.State == ReservationJournalIdle {
		return current, false, nil
	}
	if current.Record.LogicalVolumeID != allocation.LogicalVolumeID {
		return current, false, nil
	}
	if allocation.State != volume.StateReserved {
		return StoredReservationJournal{}, false, fmt.Errorf("matching Pending journal requires the exact Reserved allocation, got %q: %w", allocation.State, ErrConflict)
	}
	encoded, err := volume.EncodeAllocationRecord(allocation)
	if err != nil {
		return StoredReservationJournal{}, false, err
	}
	if !bytes.Equal(current.Record.AllocationRecord, encoded) {
		return StoredReservationJournal{}, false, fmt.Errorf("pool %q pending reservation differs from authoritative allocation %q: %w", poolName, allocation.LogicalVolumeID, ErrConflict)
	}
	next := current.Record
	next.Generation++
	next.State = ReservationJournalIdle
	next.LogicalVolumeID = ""
	next.AllocationRecord = nil
	completed, err := store.transition(ctx, current, next)
	return completed, err == nil, err
}

// Reconcile resolves every configured Pending journal before controller
// serving. It returns true when any Pending intent became Idle; callers always
// capture startup inventory afterward because an older allocation POST may
// have committed late even when this process did not create the record.
func (store *ReservationJournalStore) Reconcile(ctx context.Context, pools []string, clusterUID string, allocations *AllocationStore) (bool, error) {
	if allocations == nil {
		return false, fmt.Errorf("reservation reconciliation allocation store is nil")
	}
	resolvedAny := false
	for _, poolName := range pools {
		journal, err := store.Get(ctx, poolName, clusterUID)
		if err != nil {
			return false, fmt.Errorf("read reservation journal for pool %q: %w", poolName, err)
		}
		pending, err := journal.PendingAllocation()
		if err != nil {
			return false, fmt.Errorf("decode pending reservation for pool %q: %w", poolName, err)
		}
		if pending == nil {
			continue
		}
		_, err = store.ensureAllocation(ctx, allocations, pending)
		if err != nil {
			return false, fmt.Errorf("resolve pending reservation for pool %q: %w", poolName, err)
		}
		resolvedAny = true
		if _, completed, err := store.CompleteExact(ctx, poolName, clusterUID, pending); err != nil || !completed {
			if err == nil {
				err = fmt.Errorf("pending reservation changed before exact completion: %w", ErrConflict)
			}
			return false, fmt.Errorf("complete reconciled reservation for pool %q: %w", poolName, err)
		}
	}
	return resolvedAny, nil
}

func (store *ReservationJournalStore) ensureAllocation(ctx context.Context, allocations *AllocationStore, expected *volume.DetailedAllocationRecord) (bool, error) {
	for attempt := 0; attempt < reservationTransitionRetries; attempt++ {
		observed, err := allocations.Get(ctx, expected.LogicalVolumeID)
		if err == nil {
			matches, compareErr := allocationRecordsEqual(observed.Record, expected)
			if compareErr != nil {
				return false, compareErr
			}
			if !matches {
				return false, fmt.Errorf("pending reservation conflicts with existing allocation %q", expected.LogicalVolumeID)
			}
			return false, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return false, err
		}
		if _, err := allocations.Create(ctx, expected); err == nil {
			return true, nil
		} else if !errors.Is(err, ErrAlreadyExists) && !errors.Is(err, ErrUnavailable) &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return false, err
		}
	}
	return false, fmt.Errorf("pending allocation %q remained ambiguous after %d attempts: %w", expected.LogicalVolumeID, reservationTransitionRetries, ErrUnavailable)
}

func (store *ReservationJournalStore) transition(ctx context.Context, current StoredReservationJournal, next ReservationJournalRecord) (StoredReservationJournal, error) {
	for attempt := 0; attempt < reservationTransitionRetries; attempt++ {
		object, err := store.objectForRecord(next, current.ResourceVersion)
		if err != nil {
			return StoredReservationJournal{}, err
		}
		updated, updateErr := store.client.Update(ctx, object)
		if updateErr == nil {
			return store.decodeObject(updated, next.PoolName, next.ActiveClusterUID)
		}
		if !errors.Is(updateErr, ErrConflict) && !errors.Is(updateErr, ErrUnavailable) &&
			!errors.Is(updateErr, context.Canceled) && !errors.Is(updateErr, context.DeadlineExceeded) {
			return StoredReservationJournal{}, updateErr
		}
		observed, getErr := store.Get(ctx, next.PoolName, next.ActiveClusterUID)
		if getErr != nil {
			return StoredReservationJournal{}, fmt.Errorf("resolve reservation journal transition: %w", errors.Join(updateErr, getErr))
		}
		if journalsEqual(observed.Record, next) {
			return observed, nil
		}
		if !journalsEqual(observed.Record, current.Record) {
			return StoredReservationJournal{}, fmt.Errorf("reservation journal changed to another generation while resolving transition: %w", ErrConflict)
		}
		current = observed
	}
	return StoredReservationJournal{}, fmt.Errorf("reservation journal transition remained ambiguous after %d attempts: %w", reservationTransitionRetries, ErrUnavailable)
}

func (store *ReservationJournalStore) transitionSet(ctx context.Context, current StoredReservationJournalSet, next ReservationJournalSetRecord) (StoredReservationJournalSet, error) {
	for attempt := 0; attempt < reservationTransitionRetries; attempt++ {
		object, err := store.objectForSet(next, current.ResourceVersion)
		if err != nil {
			return StoredReservationJournalSet{}, err
		}
		updated, updateErr := store.client.Update(ctx, object)
		if updateErr == nil {
			return store.decodeSetObject(updated, next.ActiveClusterUID)
		}
		if !errors.Is(updateErr, ErrConflict) && !errors.Is(updateErr, ErrUnavailable) &&
			!errors.Is(updateErr, context.Canceled) && !errors.Is(updateErr, context.DeadlineExceeded) {
			return StoredReservationJournalSet{}, updateErr
		}
		observed, getErr := store.GetSet(ctx, next.ActiveClusterUID)
		if getErr != nil {
			return StoredReservationJournalSet{}, fmt.Errorf("resolve reservation journal-set transition: %w", errors.Join(updateErr, getErr))
		}
		if journalSetsEqual(observed.Record, next) {
			return observed, nil
		}
		if !journalSetsEqual(observed.Record, current.Record) {
			return StoredReservationJournalSet{}, fmt.Errorf("reservation journal set changed to another generation: %w", ErrConflict)
		}
		current = observed
	}
	return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set transition remained ambiguous after %d attempts: %w", reservationTransitionRetries, ErrUnavailable)
}

func (store *ReservationJournalStore) validateScope(poolName, clusterUID string) error {
	if strings.TrimSpace(poolName) == "" || poolName != strings.TrimSpace(poolName) || len(poolName) > 63 {
		return fmt.Errorf("reservation journal pool name is invalid")
	}
	return volume.ValidateClusterUID(clusterUID)
}

func (store *ReservationJournalStore) objectForRecord(record ReservationJournalRecord, resourceVersion string) (ConfigMap, error) {
	if err := store.validateRecord(record); err != nil {
		return ConfigMap{}, err
	}
	encoded, err := canonicaljson.Marshal(record)
	if err != nil {
		return ConfigMap{}, err
	}
	return ConfigMap{
		Namespace: store.namespace, Name: reservationJournalName(record.PoolName), ResourceVersion: resourceVersion,
		Labels: map[string]string{
			"app.kubernetes.io/name":                    reservationJournalAppName,
			store.driverName + "/installation-id":       store.installationID,
			store.driverName + "/pool-name-hash":        labelHash("pn-", record.PoolName),
			store.driverName + "/reservation-journal":   "true",
			store.driverName + "/reservation-journal-v": reservationJournalSchema,
		},
		Data: map[string]string{reservationJournalDataKey: string(encoded)},
	}, nil
}

func (store *ReservationJournalStore) objectForSet(record ReservationJournalSetRecord, resourceVersion string) (ConfigMap, error) {
	if err := store.validateSetRecord(record); err != nil {
		return ConfigMap{}, err
	}
	encoded, err := canonicaljson.Marshal(record)
	if err != nil {
		return ConfigMap{}, err
	}
	return ConfigMap{
		Namespace: store.namespace, Name: reservationJournalSetName, ResourceVersion: resourceVersion,
		Labels: map[string]string{
			"app.kubernetes.io/name":                    reservationJournalSetAppName,
			store.driverName + "/installation-id":       store.installationID,
			store.driverName + "/reservation-journal":   "set",
			store.driverName + "/reservation-journal-v": reservationJournalSchema,
		},
		Data: map[string]string{reservationJournalSetDataKey: string(encoded)},
	}, nil
}

func (store *ReservationJournalStore) decodeObject(object ConfigMap, poolName, clusterUID string) (StoredReservationJournal, error) {
	if object.Namespace != store.namespace || object.Name != reservationJournalName(poolName) || object.ResourceVersion == "" || object.UID == "" {
		return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap identity is incomplete or mismatched")
	}
	if len(object.Data) != 1 {
		return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap %q must contain exactly data key %q", object.Name, reservationJournalDataKey)
	}
	encoded, ok := object.Data[reservationJournalDataKey]
	if !ok {
		return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap %q is missing data key %q", object.Name, reservationJournalDataKey)
	}
	var record ReservationJournalRecord
	if err := strictjson.Decode([]byte(encoded), &record); err != nil {
		return StoredReservationJournal{}, fmt.Errorf("decode reservation journal ConfigMap %q: %w", object.Name, err)
	}
	canonical, err := canonicaljson.Marshal(record)
	if err != nil || !bytes.Equal(canonical, []byte(encoded)) {
		return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap %q is not canonical", object.Name)
	}
	if err := store.validateRecord(record); err != nil {
		return StoredReservationJournal{}, err
	}
	if record.PoolName != poolName || record.ActiveClusterUID != clusterUID {
		return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap %q scope differs from runtime", object.Name)
	}
	want, err := store.objectForRecord(record, object.ResourceVersion)
	if err != nil {
		return StoredReservationJournal{}, err
	}
	for key, value := range want.Labels {
		if object.Labels[key] != value {
			return StoredReservationJournal{}, fmt.Errorf("reservation journal ConfigMap %q label %q is invalid", object.Name, key)
		}
	}
	return StoredReservationJournal{Record: record, UID: object.UID, ResourceVersion: object.ResourceVersion}, nil
}

func (store *ReservationJournalStore) decodeSetObject(object ConfigMap, clusterUID string) (StoredReservationJournalSet, error) {
	if object.Namespace != store.namespace || object.Name != reservationJournalSetName || object.ResourceVersion == "" || object.UID == "" {
		return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set ConfigMap identity is incomplete or mismatched")
	}
	if len(object.Data) != 1 {
		return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set ConfigMap must contain exactly data key %q", reservationJournalSetDataKey)
	}
	encoded, present := object.Data[reservationJournalSetDataKey]
	if !present {
		return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set ConfigMap is missing data key %q", reservationJournalSetDataKey)
	}
	var record ReservationJournalSetRecord
	if err := strictjson.Decode([]byte(encoded), &record); err != nil {
		return StoredReservationJournalSet{}, fmt.Errorf("decode reservation journal-set ConfigMap: %w", err)
	}
	canonical, err := canonicaljson.Marshal(record)
	if err != nil || !bytes.Equal(canonical, []byte(encoded)) {
		return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set ConfigMap is not canonical")
	}
	if err := store.validateSetRecord(record); err != nil {
		return StoredReservationJournalSet{}, err
	}
	if record.ActiveClusterUID != clusterUID {
		return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set cluster identity differs from runtime")
	}
	want, err := store.objectForSet(record, object.ResourceVersion)
	if err != nil {
		return StoredReservationJournalSet{}, err
	}
	for key, value := range want.Labels {
		if object.Labels[key] != value {
			return StoredReservationJournalSet{}, fmt.Errorf("reservation journal-set label %q is invalid", key)
		}
	}
	return StoredReservationJournalSet{Record: record, UID: object.UID, ResourceVersion: object.ResourceVersion}, nil
}

func (store *ReservationJournalStore) validateRecord(record ReservationJournalRecord) error {
	if err := validateReservationJournalRecord(record); err != nil {
		return err
	}
	if record.DriverName != store.driverName || record.InstallationID != store.installationID {
		return fmt.Errorf("reservation journal schema or installation identity is invalid")
	}
	return nil
}

func validateReservationJournalRecord(record ReservationJournalRecord) error {
	if record.SchemaVersion != reservationJournalSchema {
		return fmt.Errorf("reservation journal schema version %q is unsupported", record.SchemaVersion)
	}
	if err := volume.ValidateDriverName(record.DriverName); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(record.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(record.ActiveClusterUID); err != nil {
		return err
	}
	if err := volume.ValidatePoolName(record.PoolName); err != nil {
		return err
	}
	switch record.State {
	case ReservationJournalIdle:
		if record.LogicalVolumeID != "" || len(record.AllocationRecord) != 0 {
			return fmt.Errorf("idle reservation journal contains a pending allocation")
		}
	case ReservationJournalPending:
		if err := volume.ValidateLogicalVolumeID(record.LogicalVolumeID); err != nil {
			return err
		}
		allocation, err := volume.DecodeAllocationRecord(record.AllocationRecord)
		if err != nil {
			return err
		}
		detailed, ok := allocation.(*volume.DetailedAllocationRecord)
		if !ok || detailed.State != volume.StateReserved || detailed.LogicalVolumeID != record.LogicalVolumeID || detailed.PoolName != record.PoolName ||
			detailed.DriverName != record.DriverName || detailed.InstallationID != record.InstallationID ||
			detailed.ActiveClusterUID != record.ActiveClusterUID {
			return fmt.Errorf("pending reservation journal allocation identity is invalid")
		}
	default:
		return fmt.Errorf("reservation journal state %q is unsupported", record.State)
	}
	return nil
}

func (store *ReservationJournalStore) validateSetRecord(record ReservationJournalSetRecord) error {
	if err := validateReservationJournalSetRecord(record); err != nil {
		return err
	}
	if record.DriverName != store.driverName || record.InstallationID != store.installationID {
		return fmt.Errorf("reservation journal-set schema or installation identity is invalid")
	}
	return nil
}

func validateReservationJournalSetRecord(record ReservationJournalSetRecord) error {
	if record.SchemaVersion != reservationJournalSchema {
		return fmt.Errorf("reservation journal-set schema version %q is unsupported", record.SchemaVersion)
	}
	if err := volume.ValidateDriverName(record.DriverName); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(record.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(record.ActiveClusterUID); err != nil {
		return err
	}
	if len(record.Pools) != 0 {
		if _, err := normalizeJournalPools(record.Pools); err != nil {
			return fmt.Errorf("reservation journal-set committed pools: %w", err)
		}
	}
	if !slices.IsSorted(record.Pools) || len(slices.Compact(slices.Clone(record.Pools))) != len(record.Pools) {
		return fmt.Errorf("reservation journal-set committed pools are not sorted and unique")
	}
	if len(record.PendingPools) != 0 {
		if _, err := normalizeJournalPools(record.PendingPools); err != nil {
			return fmt.Errorf("reservation journal-set pending pools: %w", err)
		}
		if !slices.IsSorted(record.PendingPools) || len(slices.Compact(slices.Clone(record.PendingPools))) != len(record.PendingPools) {
			return fmt.Errorf("reservation journal-set pending pools are not sorted and unique")
		}
		for _, poolName := range record.PendingPools {
			if slices.Contains(record.Pools, poolName) {
				return fmt.Errorf("reservation journal-set pool %q is both committed and pending", poolName)
			}
		}
	}
	switch record.State {
	case ReservationJournalSetInitializing:
		if len(record.PendingPools) == 0 {
			return fmt.Errorf("initializing reservation journal set has no pending pools")
		}
	case ReservationJournalSetReady:
		if len(record.Pools) == 0 || len(record.PendingPools) != 0 {
			return fmt.Errorf("ready reservation journal set must contain committed pools only")
		}
	default:
		return fmt.Errorf("reservation journal-set state %q is unsupported", record.State)
	}
	return nil
}

// DecodeReservationJournalProjection strictly decodes one checkpoint
// projection without binding it to a local installation. Restore code must
// separately match its identity to the checkpoint manifest.
func DecodeReservationJournalProjection(data []byte) (ReservationJournalRecord, error) {
	var record ReservationJournalRecord
	if err := strictjson.Decode(data, &record); err != nil {
		return ReservationJournalRecord{}, err
	}
	if err := validateReservationJournalRecord(record); err != nil {
		return ReservationJournalRecord{}, err
	}
	return record, nil
}

// DecodeReservationJournalSetProjection strictly decodes the installation-wide
// journal commitment without binding it to a local installation.
func DecodeReservationJournalSetProjection(data []byte) (ReservationJournalSetRecord, error) {
	var record ReservationJournalSetRecord
	if err := strictjson.Decode(data, &record); err != nil {
		return ReservationJournalSetRecord{}, err
	}
	if err := validateReservationJournalSetRecord(record); err != nil {
		return ReservationJournalSetRecord{}, err
	}
	return record, nil
}

func reservationJournalName(poolName string) string {
	sum := sha256.Sum256([]byte(poolName))
	return reservationJournalNamePrefix + hex.EncodeToString(sum[:16])
}

// ReservationJournalName returns the deterministic fixed ConfigMap name for a
// validated pool. It is exported for exact checkpoint/restore projections.
func ReservationJournalName(poolName string) (string, error) {
	if err := volume.ValidatePoolName(poolName); err != nil {
		return "", err
	}
	return reservationJournalName(poolName), nil
}

// ReservationJournalSetName returns the fixed installation-wide set object
// name used by checkpoint and restore.
func ReservationJournalSetName() string { return reservationJournalSetName }

// IsReservationJournalName reports whether name belongs to the fixed journal
// namespace used by checkpoint/restore object classification.
func IsReservationJournalName(name string) bool {
	return name == reservationJournalSetName || strings.HasPrefix(name, reservationJournalNamePrefix)
}

func journalsEqual(left, right ReservationJournalRecord) bool {
	leftBytes, leftErr := canonicaljson.Marshal(left)
	rightBytes, rightErr := canonicaljson.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}

func journalSetsEqual(left, right ReservationJournalSetRecord) bool {
	leftBytes, leftErr := canonicaljson.Marshal(left)
	rightBytes, rightErr := canonicaljson.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}

func normalizeJournalPools(pools []string) ([]string, error) {
	if len(pools) == 0 {
		return nil, fmt.Errorf("reservation journal set must contain at least one pool")
	}
	result := slices.Clone(pools)
	for _, poolName := range result {
		if err := volume.ValidatePoolName(poolName); err != nil {
			return nil, err
		}
	}
	slices.Sort(result)
	if len(slices.Compact(slices.Clone(result))) != len(result) {
		return nil, fmt.Errorf("reservation journal pool set contains duplicates")
	}
	return result, nil
}

func allocationRecordsEqual(left volume.AllocationRecord, right *volume.DetailedAllocationRecord) (bool, error) {
	leftBytes, err := volume.EncodeAllocationRecord(left)
	if err != nil {
		return false, err
	}
	rightBytes, err := volume.EncodeAllocationRecord(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftBytes, rightBytes), nil
}
