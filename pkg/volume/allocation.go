package volume

import (
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
)

// AllocationRecordKind is the only v1 allocation schema discriminator.
type AllocationRecordKind string

const (
	// AllocationRecordDetailed carries an active or retained full lifecycle.
	AllocationRecordDetailed AllocationRecordKind = "detailed"
	// AllocationRecordCompactDeleted permanently reserves a deleted name with
	// bounded identity and audit data.
	AllocationRecordCompactDeleted AllocationRecordKind = "compactDeleted"
	// AllocationRecordDeletedUnknown records only conclusive absence facts.
	AllocationRecordDeletedUnknown AllocationRecordKind = "deletedUnknown"
)

// AllocationState is the closed durable lifecycle state machine.
type AllocationState string

const (
	StateReserved          AllocationState = "Reserved"
	StateCreatingDirectory AllocationState = "CreatingDirectory"
	StateReady             AllocationState = "Ready"
	StateDeleting          AllocationState = "Deleting"
	StateArchived          AllocationState = "Archived"
	StateDeleted           AllocationState = "Deleted"
	StateRetained          AllocationState = "Retained"
)

// DeleteOperation is the immutable filesystem action prepared before mutation.
type DeleteOperation string

const (
	DeleteOperationArchive DeleteOperation = "archive"
	DeleteOperationDelete  DeleteOperation = "delete"
	DeleteOperationRetain  DeleteOperation = "retain"
)

const (
	// RecoverySourceOwnershipOnly records reconstruction after both allocation
	// and PV absence were conclusively proven.
	RecoverySourceOwnershipOnly = "ownership-only"
	// RecoverySourcePVAndOwnership records reconstruction from an existing PV
	// whose full handle/context matches authenticated filesystem ownership.
	RecoverySourcePVAndOwnership = "pv-and-ownership"
)

// AllocationRecord is one exact v1 durable allocation variant.
type AllocationRecord interface {
	Validate() error
	Kind() AllocationRecordKind
	LifecycleState() AllocationState
	LogicalID() string
}

// DetailedAllocationRecord is the complete ConfigMap-backed lifecycle record.
// Optional transition fields are immutable after their corresponding operation
// begins and are validated against the current state before use.
type DetailedAllocationRecord struct {
	SchemaVersion              string               `json:"schemaVersion"`
	RecordKind                 AllocationRecordKind `json:"recordKind"`
	RecordRevision             uint64               `json:"recordRevision"`
	DriverName                 string               `json:"driverName"`
	ActiveClusterUID           string               `json:"activeClusterUID"`
	State                      AllocationState      `json:"state"`
	InstallationID             string               `json:"installationID"`
	CreateVolumeRequestName    string               `json:"createVolumeRequestName"`
	RequestHash                string               `json:"requestHash"`
	OriginalRequiredBytes      uint64               `json:"originalRequiredBytes"`
	OriginalLimitBytes         uint64               `json:"originalLimitBytes"`
	SelectedCapacityBytes      uint64               `json:"selectedCapacityBytes"`
	NormalizedCreateParameters CreateParameters     `json:"normalizedCreateParameters"`
	LogicalVolumeID            string               `json:"logicalVolumeID"`
	VolumeHandle               string               `json:"volumeHandle"`
	VolumeHandleHash           string               `json:"volumeHandleHash"`
	MappingHash                string               `json:"mappingHash"`
	PoolName                   string               `json:"poolName"`
	ParentFilesystemID         string               `json:"parentFilesystemID"`
	BasePath                   string               `json:"basePath"`
	BasePathHash               string               `json:"basePathHash"`
	DirectoryName              string               `json:"directoryName"`
	ReservesCapacity           bool                 `json:"reservesCapacity"`
	DeletePolicy               DeletePolicy         `json:"deletePolicy"`
	DeleteResult               string               `json:"deleteResult,omitempty"`
	DeleteOperationID          string               `json:"deleteOperationID,omitempty"`
	DeleteOperation            DeleteOperation      `json:"deleteOperation,omitempty"`
	DeleteSourcePath           string               `json:"deleteSourcePath,omitempty"`
	DeleteTargetPath           string               `json:"deleteTargetPath,omitempty"`
	DeletePreparedAt           string               `json:"deletePreparedAt,omitempty"`
	DeleteRemoveStartedAt      string               `json:"deleteRemoveStartedAt,omitempty"`
	DeleteCompletedAt          string               `json:"deleteCompletedAt,omitempty"`
	DirectoryUID               uint32               `json:"directoryUid"`
	DirectoryGID               uint32               `json:"directoryGid"`
	DirectoryMode              string               `json:"directoryMode"`
	CreatedAt                  string               `json:"createdAt"`
	UpdatedAt                  string               `json:"updatedAt"`
	DeletedAt                  string               `json:"deletedAt,omitempty"`
	ArchivedPath               string               `json:"archivedPath,omitempty"`
	RetainedPath               string               `json:"retainedPath,omitempty"`
	QuarantinePath             string               `json:"quarantinePath,omitempty"`
	PublishedNodeIDs           []string             `json:"publishedNodeIDs"`
	GCRequestID                string               `json:"gcRequestID,omitempty"`
	GCRequestedMode            string               `json:"gcRequestedMode,omitempty"`
	GCExpectedState            AllocationState      `json:"gcExpectedState,omitempty"`
	GCRequestedAt              string               `json:"gcRequestedAt,omitempty"`
	GCOperationID              string               `json:"gcOperationID,omitempty"`
	GCTargetPath               string               `json:"gcTargetPath,omitempty"`
	GCQuarantinePath           string               `json:"gcQuarantinePath,omitempty"`
	GCStartedAt                string               `json:"gcStartedAt,omitempty"`
	GCRemoveStartedAt          string               `json:"gcRemoveStartedAt,omitempty"`
	GCCompletedAt              string               `json:"gcCompletedAt,omitempty"`
	RecoveryOperationID        string               `json:"recoveryOperationID,omitempty"`
	RecoverySource             string               `json:"recoverySource,omitempty"`
	RecoveredAt                string               `json:"recoveredAt,omitempty"`
}

// CompactDeletedAllocationRecord is the permanent non-reserving allocation
// tombstone written in place after detailed retention expires.
type CompactDeletedAllocationRecord struct {
	SchemaVersion           string               `json:"schemaVersion"`
	RecordKind              AllocationRecordKind `json:"recordKind"`
	RecordRevision          uint64               `json:"recordRevision"`
	DriverName              string               `json:"driverName"`
	InstallationID          string               `json:"installationID"`
	ActiveClusterUID        string               `json:"activeClusterUID"`
	CreateVolumeRequestName string               `json:"createVolumeRequestName"`
	LogicalVolumeID         string               `json:"logicalVolumeID"`
	VolumeHandleHash        string               `json:"volumeHandleHash"`
	MappingHash             string               `json:"mappingHash"`
	State                   AllocationState      `json:"state"`
	ParentFilesystemID      string               `json:"parentFilesystemID"`
	DirectoryName           string               `json:"directoryName"`
	ReservesCapacity        bool                 `json:"reservesCapacity"`
	DeleteResult            string               `json:"deleteResult"`
	UpdatedAt               string               `json:"updatedAt"`
	DeletedAt               string               `json:"deletedAt"`
	DeleteOperationID       string               `json:"deleteOperationID,omitempty"`
	DeleteOperation         DeleteOperation      `json:"deleteOperation,omitempty"`
	ArchivedPath            string               `json:"archivedPath,omitempty"`
	RetainedPath            string               `json:"retainedPath,omitempty"`
	QuarantinePath          string               `json:"quarantinePath,omitempty"`
	DeleteCompletedAt       string               `json:"deleteCompletedAt,omitempty"`
	GCOperationID           string               `json:"gcOperationID,omitempty"`
	GCTargetPath            string               `json:"gcTargetPath,omitempty"`
	GCQuarantinePath        string               `json:"gcQuarantinePath,omitempty"`
	GCCompletedAt           string               `json:"gcCompletedAt,omitempty"`
}

// DeletedUnknownAllocationRecord is the minimal conclusive-absence tombstone.
// It intentionally contains no parent, path, capacity, policy, or request name
// that could be guessed and later misused for filesystem mutation.
type DeletedUnknownAllocationRecord struct {
	SchemaVersion    string               `json:"schemaVersion"`
	RecordKind       AllocationRecordKind `json:"recordKind"`
	RecordRevision   uint64               `json:"recordRevision"`
	DriverName       string               `json:"driverName"`
	InstallationID   string               `json:"installationID"`
	ActiveClusterUID string               `json:"activeClusterUID"`
	LogicalVolumeID  string               `json:"logicalVolumeID"`
	VolumeHandleHash string               `json:"volumeHandleHash"`
	MappingHash      string               `json:"mappingHash"`
	State            AllocationState      `json:"state"`
	ReservesCapacity bool                 `json:"reservesCapacity"`
	AbsenceReason    string               `json:"absenceReason"`
	CreatedAt        string               `json:"createdAt"`
	UpdatedAt        string               `json:"updatedAt"`
	DeletedAt        string               `json:"deletedAt"`
}

// DecodeAllocationRecord decodes exactly one of the three v1 variants.
func DecodeAllocationRecord(data []byte) (AllocationRecord, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(data, &fields); err != nil {
		return nil, err
	}
	rawKind, present := fields["recordKind"]
	if !present {
		return nil, fmt.Errorf("allocation record kind is missing")
	}
	var kind AllocationRecordKind
	if err := strictjson.Decode(rawKind, &kind); err != nil {
		return nil, fmt.Errorf("decode allocation record kind: %w", err)
	}

	var record AllocationRecord
	switch kind {
	case AllocationRecordDetailed:
		record = &DetailedAllocationRecord{}
	case AllocationRecordCompactDeleted:
		record = &CompactDeletedAllocationRecord{}
	case AllocationRecordDeletedUnknown:
		record = &DeletedUnknownAllocationRecord{}
	default:
		return nil, fmt.Errorf("allocation record kind %q is unsupported", kind)
	}
	if err := strictjson.Decode(data, record); err != nil {
		return nil, err
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return record, nil
}

// EncodeAllocationRecord validates and canonically encodes a durable record.
func EncodeAllocationRecord(record AllocationRecord) ([]byte, error) {
	if record == nil {
		return nil, fmt.Errorf("allocation record is nil")
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(record)
}

// Kind returns the schema discriminator.
func (record *DetailedAllocationRecord) Kind() AllocationRecordKind { return record.RecordKind }

// LifecycleState returns the durable state.
func (record *DetailedAllocationRecord) LifecycleState() AllocationState { return record.State }

// LogicalID returns the deterministic logical-volume identity.
func (record *DetailedAllocationRecord) LogicalID() string { return record.LogicalVolumeID }

// Kind returns the schema discriminator.
func (record *CompactDeletedAllocationRecord) Kind() AllocationRecordKind { return record.RecordKind }

// LifecycleState returns the durable state.
func (record *CompactDeletedAllocationRecord) LifecycleState() AllocationState { return record.State }

// LogicalID returns the deterministic logical-volume identity.
func (record *CompactDeletedAllocationRecord) LogicalID() string { return record.LogicalVolumeID }

// Kind returns the schema discriminator.
func (record *DeletedUnknownAllocationRecord) Kind() AllocationRecordKind { return record.RecordKind }

// LifecycleState returns the durable state.
func (record *DeletedUnknownAllocationRecord) LifecycleState() AllocationState { return record.State }

// LogicalID returns the deterministic logical-volume identity.
func (record *DeletedUnknownAllocationRecord) LogicalID() string { return record.LogicalVolumeID }

// Reserves reports the normative capacity effect of a lifecycle state.
func (state AllocationState) Reserves() (bool, error) {
	switch state {
	case StateReserved, StateCreatingDirectory, StateReady, StateDeleting, StateArchived, StateRetained:
		return true, nil
	case StateDeleted:
		return false, nil
	default:
		return false, fmt.Errorf("allocation state %q is unsupported", state)
	}
}

// Validate verifies a detailed record and its state/field combination.
func (record *DetailedAllocationRecord) Validate() error {
	if record == nil {
		return fmt.Errorf("detailed allocation record is nil")
	}
	if record.SchemaVersion != SchemaVersionV1 || record.RecordKind != AllocationRecordDetailed {
		return fmt.Errorf("detailed allocation record has schema %q kind %q", record.SchemaVersion, record.RecordKind)
	}
	if record.RecordRevision == 0 {
		return fmt.Errorf("allocation record revision must be positive")
	}
	if err := validateCommonRecordIdentity(record.DriverName, record.InstallationID, record.ActiveClusterUID, record.LogicalVolumeID, record.VolumeHandleHash, record.MappingHash); err != nil {
		return err
	}
	if record.CreateVolumeRequestName == "" || strings.ContainsRune(record.CreateVolumeRequestName, 0) {
		return fmt.Errorf("create volume request name is empty or contains NUL")
	}
	wantLogicalVolumeID, err := LogicalVolumeID(record.DriverName, record.CreateVolumeRequestName)
	if err != nil {
		return err
	}
	if record.LogicalVolumeID != wantLogicalVolumeID {
		return fmt.Errorf("logical volume ID does not match the original create request name")
	}
	if record.SelectedCapacityBytes == 0 {
		return fmt.Errorf("selected capacity must be positive")
	}
	if !CapacityCompatible(record.SelectedCapacityBytes, record.OriginalRequiredBytes, record.OriginalLimitBytes) {
		return fmt.Errorf("selected capacity is outside the original capacity range")
	}
	parameters, err := record.NormalizedCreateParameters.Normalize()
	if err != nil {
		return err
	}
	if !EqualCreateParameters(parameters, record.NormalizedCreateParameters) {
		return fmt.Errorf("normalized create parameters are not canonical")
	}
	if record.PoolName != parameters.PoolName || record.DeletePolicy != parameters.DeletePolicy || record.DirectoryUID != parameters.DirectoryUID || record.DirectoryGID != parameters.DirectoryGID || record.DirectoryMode != parameters.DirectoryMode {
		return fmt.Errorf("duplicated immutable create fields disagree with normalized parameters")
	}
	wantRequestHash, err := RequestHash(CreateRequestIdentity{
		OriginalRequiredBytes: record.OriginalRequiredBytes,
		OriginalLimitBytes:    record.OriginalLimitBytes,
		SelectedCapacityBytes: record.SelectedCapacityBytes,
		Parameters:            parameters,
	})
	if err != nil {
		return err
	}
	if record.RequestHash != wantRequestHash {
		return fmt.Errorf("request hash mismatch: stored=%q computed=%q", record.RequestHash, wantRequestHash)
	}
	mapping := Mapping{
		PoolName:           record.PoolName,
		ParentFilesystemID: record.ParentFilesystemID,
		BasePath:           record.BasePath,
		DirectoryName:      record.DirectoryName,
		LogicalVolumeID:    record.LogicalVolumeID,
	}
	if err := mapping.Validate(); err != nil {
		return err
	}
	wantMappingHash, err := MappingHash(mapping)
	if err != nil {
		return err
	}
	if record.MappingHash != wantMappingHash {
		return fmt.Errorf("mapping hash mismatch: stored=%q computed=%q", record.MappingHash, wantMappingHash)
	}
	handle, err := ParseHandle(record.VolumeHandle)
	if err != nil {
		return err
	}
	if handle.LogicalVolumeID != record.LogicalVolumeID || handle.MappingHash != record.MappingHash {
		return fmt.Errorf("volume handle disagrees with allocation identity")
	}
	wantHandleHash, err := VolumeHandleHash(record.VolumeHandle)
	if err != nil {
		return err
	}
	if record.VolumeHandleHash != wantHandleHash {
		return fmt.Errorf("volume handle hash mismatch")
	}
	wantBasePathHash, err := BasePathHash(record.BasePath)
	if err != nil {
		return err
	}
	if record.BasePathHash != wantBasePathHash {
		return fmt.Errorf("base path hash mismatch")
	}
	reserves, err := record.State.Reserves()
	if err != nil {
		return err
	}
	if reserves != record.ReservesCapacity {
		return fmt.Errorf("state %q requires reservesCapacity=%t", record.State, reserves)
	}
	if err := validatePublishedNodes(record.PublishedNodeIDs); err != nil {
		return err
	}
	if err := validateRequiredTimestamp("createdAt", record.CreatedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp("updatedAt", record.UpdatedAt); err != nil {
		return err
	}
	if err := record.validateLifecycle(); err != nil {
		return err
	}
	return nil
}

// Validate verifies a compact permanent tombstone.
func (record *CompactDeletedAllocationRecord) Validate() error {
	if record == nil {
		return fmt.Errorf("compact deleted allocation record is nil")
	}
	if record.SchemaVersion != SchemaVersionV1 || record.RecordKind != AllocationRecordCompactDeleted || record.State != StateDeleted || record.ReservesCapacity {
		return fmt.Errorf("compact deleted allocation record has invalid schema, kind, state, or reservation")
	}
	if record.RecordRevision == 0 || record.CreateVolumeRequestName == "" || record.DeleteResult == "" {
		return fmt.Errorf("compact deleted allocation record is missing required identity or result")
	}
	if err := validateCommonRecordIdentity(record.DriverName, record.InstallationID, record.ActiveClusterUID, record.LogicalVolumeID, record.VolumeHandleHash, record.MappingHash); err != nil {
		return err
	}
	if err := ValidateParentFilesystemID(record.ParentFilesystemID); err != nil {
		return err
	}
	if err := ValidateDirectoryName(record.DirectoryName); err != nil {
		return err
	}
	wantLogicalVolumeID, err := LogicalVolumeID(record.DriverName, record.CreateVolumeRequestName)
	if err != nil {
		return err
	}
	if record.LogicalVolumeID != wantLogicalVolumeID {
		return fmt.Errorf("compact logical volume ID does not match create request name")
	}
	if err := validateRequiredTimestamp("updatedAt", record.UpdatedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp("deletedAt", record.DeletedAt); err != nil {
		return err
	}
	return validateCompactAudit(record.DeleteOperationID, record.DeleteOperation, record.DeleteCompletedAt, record.GCOperationID, record.GCCompletedAt)
}

// Validate verifies a minimal conclusive-absence tombstone.
func (record *DeletedUnknownAllocationRecord) Validate() error {
	if record == nil {
		return fmt.Errorf("deleted-unknown allocation record is nil")
	}
	if record.SchemaVersion != SchemaVersionV1 || record.RecordKind != AllocationRecordDeletedUnknown || record.State != StateDeleted || record.ReservesCapacity {
		return fmt.Errorf("deleted-unknown allocation record has invalid schema, kind, state, or reservation")
	}
	if record.RecordRevision == 0 || record.AbsenceReason == "" || len(record.AbsenceReason) > 512 {
		return fmt.Errorf("deleted-unknown allocation record has invalid revision or absence reason")
	}
	if err := validateCommonRecordIdentity(record.DriverName, record.InstallationID, record.ActiveClusterUID, record.LogicalVolumeID, record.VolumeHandleHash, record.MappingHash); err != nil {
		return err
	}
	for name, value := range map[string]string{"createdAt": record.CreatedAt, "updatedAt": record.UpdatedAt, "deletedAt": record.DeletedAt} {
		if err := validateRequiredTimestamp(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (record *DetailedAllocationRecord) validateLifecycle() error {
	if err := validateOptionalTimestampFields(map[string]string{
		"deletePreparedAt":      record.DeletePreparedAt,
		"deleteRemoveStartedAt": record.DeleteRemoveStartedAt,
		"deleteCompletedAt":     record.DeleteCompletedAt,
		"deletedAt":             record.DeletedAt,
		"gcRequestedAt":         record.GCRequestedAt,
		"gcStartedAt":           record.GCStartedAt,
		"gcRemoveStartedAt":     record.GCRemoveStartedAt,
		"gcCompletedAt":         record.GCCompletedAt,
		"recoveredAt":           record.RecoveredAt,
	}); err != nil {
		return err
	}
	recoveryFields := 0
	for _, value := range []string{record.RecoveryOperationID, record.RecoverySource, record.RecoveredAt} {
		if value != "" {
			recoveryFields++
		}
	}
	if recoveryFields != 0 && recoveryFields != 3 {
		return fmt.Errorf("allocation recovery audit fields must be all present or all absent")
	}
	if recoveryFields == 3 {
		if err := ValidateOperationID(record.RecoveryOperationID); err != nil {
			return fmt.Errorf("recovery operation: %w", err)
		}
		if record.RecoverySource != RecoverySourceOwnershipOnly && record.RecoverySource != RecoverySourcePVAndOwnership {
			return fmt.Errorf("recovery source %q is unsupported", record.RecoverySource)
		}
	}
	for field, value := range map[string]string{
		"deleteSourcePath": record.DeleteSourcePath,
		"deleteTargetPath": record.DeleteTargetPath,
		"archivedPath":     record.ArchivedPath,
		"retainedPath":     record.RetainedPath,
		"quarantinePath":   record.QuarantinePath,
		"gcTargetPath":     record.GCTargetPath,
		"gcQuarantinePath": record.GCQuarantinePath,
	} {
		if value != "" {
			if err := validatePersistedPath(record.BasePath, value); err != nil {
				return fmt.Errorf("%s: %w", field, err)
			}
		}
	}
	if err := validateGCFields(record); err != nil {
		return err
	}

	switch record.State {
	case StateReserved, StateCreatingDirectory, StateReady:
		if record.hasDeleteFields() || record.GCOperationID != "" {
			return fmt.Errorf("state %q must not contain delete or GC lifecycle progress", record.State)
		}
		if (record.State == StateReserved || record.State == StateCreatingDirectory) && len(record.PublishedNodeIDs) != 0 {
			return fmt.Errorf("state %q must not contain published-node fences", record.State)
		}
	case StateDeleting:
		if err := record.validatePreparedDelete(); err != nil {
			return err
		}
		if record.DeleteCompletedAt != "" || record.DeletedAt != "" || record.DeleteResult != "" {
			return fmt.Errorf("deleting record contains terminal delete evidence")
		}
	case StateArchived:
		if err := record.validatePreparedDelete(); err != nil {
			return err
		}
		if record.DeleteOperation != DeleteOperationArchive || record.ArchivedPath == "" || record.ArchivedPath != record.DeleteTargetPath || record.DeleteCompletedAt == "" || record.DeleteResult == "" {
			return fmt.Errorf("archived record lacks matching archive completion evidence")
		}
	case StateRetained:
		if err := record.validatePreparedDelete(); err != nil {
			return err
		}
		if record.DeleteOperation != DeleteOperationRetain || record.RetainedPath == "" || record.RetainedPath != record.DeleteTargetPath || record.DeleteCompletedAt == "" || record.DeleteResult == "" {
			return fmt.Errorf("retained record lacks matching retain completion evidence")
		}
	case StateDeleted:
		if record.DeletedAt == "" || record.DeleteResult == "" {
			return fmt.Errorf("deleted record lacks terminal timestamp or result")
		}
		if record.GCOperationID != "" {
			if record.GCCompletedAt == "" || record.GCRemoveStartedAt == "" {
				return fmt.Errorf("GC-created deleted record lacks removal and completion evidence")
			}
		} else {
			if err := record.validatePreparedDelete(); err != nil {
				return err
			}
			if record.DeleteOperation != DeleteOperationDelete || record.QuarantinePath == "" || record.QuarantinePath != record.DeleteTargetPath || record.DeleteRemoveStartedAt == "" || record.DeleteCompletedAt == "" {
				return fmt.Errorf("delete-created Deleted record lacks matching removal evidence")
			}
		}
	default:
		return fmt.Errorf("allocation state %q is unsupported", record.State)
	}
	return nil
}

func (record *DetailedAllocationRecord) validatePreparedDelete() error {
	if err := ValidateOperationID(record.DeleteOperationID); err != nil {
		return fmt.Errorf("delete operation: %w", err)
	}
	if record.DeletePreparedAt == "" || record.DeleteSourcePath == "" || record.DeleteTargetPath == "" {
		return fmt.Errorf("prepared delete requires source, target, and timestamp")
	}
	wantOperation := DeleteOperation(record.DeletePolicy)
	if record.DeleteOperation != wantOperation {
		return fmt.Errorf("delete operation %q disagrees with policy %q", record.DeleteOperation, record.DeletePolicy)
	}
	expectedSource := path.Join(record.BasePath, record.DirectoryName)
	if record.DeleteSourcePath != expectedSource {
		return fmt.Errorf("delete source path differs from immutable logical directory")
	}
	switch record.DeleteOperation {
	case DeleteOperationArchive:
		expectedTarget, err := ManagedLifecycleTarget(record.BasePath, ".archived", record.DirectoryName, record.LogicalVolumeID, record.DeletePreparedAt, record.DeleteOperationID)
		if err != nil {
			return err
		}
		if record.DeleteTargetPath != expectedTarget || record.ArchivedPath != expectedTarget || record.RetainedPath != "" || record.QuarantinePath != "" {
			return fmt.Errorf("archive target and operation-specific lifecycle paths differ")
		}
	case DeleteOperationDelete:
		expectedTarget, err := ManagedLifecycleTarget(record.BasePath, ".deleted", record.DirectoryName, record.LogicalVolumeID, record.DeletePreparedAt, record.DeleteOperationID)
		if err != nil {
			return err
		}
		if record.DeleteTargetPath != expectedTarget || record.QuarantinePath != expectedTarget || record.ArchivedPath != "" || record.RetainedPath != "" {
			return fmt.Errorf("delete quarantine and operation-specific lifecycle paths differ")
		}
	case DeleteOperationRetain:
		if record.DeleteTargetPath != expectedSource || record.RetainedPath != expectedSource || record.ArchivedPath != "" || record.QuarantinePath != "" {
			return fmt.Errorf("retain target and operation-specific lifecycle paths differ")
		}
	default:
		return fmt.Errorf("delete operation %q is unsupported", record.DeleteOperation)
	}
	if record.DeleteOperation != DeleteOperationDelete && record.DeleteRemoveStartedAt != "" {
		return fmt.Errorf("delete remove-start evidence is invalid for operation %q", record.DeleteOperation)
	}
	return nil
}

func (record *DetailedAllocationRecord) hasDeleteFields() bool {
	return record.DeleteResult != "" || record.DeleteOperationID != "" || record.DeleteOperation != "" || record.DeleteSourcePath != "" || record.DeleteTargetPath != "" || record.DeletePreparedAt != "" || record.DeleteRemoveStartedAt != "" || record.DeleteCompletedAt != "" || record.DeletedAt != "" || record.ArchivedPath != "" || record.RetainedPath != "" || record.QuarantinePath != ""
}

func validateGCFields(record *DetailedAllocationRecord) error {
	requestFields := []string{record.GCRequestID, record.GCRequestedMode, string(record.GCExpectedState), record.GCRequestedAt}
	requestCount := 0
	for _, value := range requestFields {
		if value != "" {
			requestCount++
		}
	}
	if requestCount != 0 && requestCount != len(requestFields) {
		return fmt.Errorf("GC request fields must be all present or all absent")
	}
	if requestCount != 0 {
		if err := ValidateOperationID(record.GCRequestID); err != nil {
			return fmt.Errorf("GC request: %w", err)
		}
		if record.GCExpectedState != StateArchived && record.GCExpectedState != StateRetained {
			return fmt.Errorf("GC expected state %q is not terminal data-bearing state", record.GCExpectedState)
		}
		if record.GCRequestedMode != "dry-run" && record.GCRequestedMode != "execute" {
			return fmt.Errorf("GC requested mode %q is unsupported", record.GCRequestedMode)
		}
	}
	progress := []string{record.GCOperationID, record.GCTargetPath, record.GCQuarantinePath, record.GCStartedAt}
	progressCount := 0
	for _, value := range progress {
		if value != "" {
			progressCount++
		}
	}
	if progressCount != 0 && progressCount != len(progress) {
		return fmt.Errorf("GC operation identity, paths, and start timestamp must appear together")
	}
	if progressCount != 0 {
		if requestCount == 0 || record.GCRequestedMode != "execute" {
			return fmt.Errorf("GC progress requires an execute request")
		}
		if err := ValidateOperationID(record.GCOperationID); err != nil {
			return fmt.Errorf("GC operation: %w", err)
		}
		if record.State != record.GCExpectedState && record.State != StateDeleted {
			return fmt.Errorf("GC progress state %q differs from expected predecessor %q", record.State, record.GCExpectedState)
		}
		var expectedTarget string
		switch record.GCExpectedState {
		case StateArchived:
			expectedTarget = record.ArchivedPath
		case StateRetained:
			expectedTarget = record.RetainedPath
		}
		if expectedTarget == "" || record.GCTargetPath != expectedTarget {
			return fmt.Errorf("GC target path does not match the persisted %s path", record.GCExpectedState)
		}
		expectedQuarantine, err := ManagedLifecycleTarget(record.BasePath, ".deleted", record.DirectoryName, record.LogicalVolumeID, record.GCStartedAt, record.GCOperationID)
		if err != nil {
			return err
		}
		if record.GCQuarantinePath != expectedQuarantine {
			return fmt.Errorf("GC quarantine path differs from the persisted operation identity")
		}
	} else if requestCount != 0 && record.State != record.GCExpectedState {
		return fmt.Errorf("GC request expected state %q differs from allocation state %q", record.GCExpectedState, record.State)
	}
	if record.GCRemoveStartedAt != "" && progressCount == 0 {
		return fmt.Errorf("GC remove-start evidence requires prepared GC progress")
	}
	if record.GCCompletedAt != "" && record.GCRemoveStartedAt == "" {
		return fmt.Errorf("GC completion requires remove-start evidence")
	}
	if record.GCCompletedAt != "" && record.State != StateDeleted {
		return fmt.Errorf("GC completion requires Deleted allocation state")
	}
	return nil
}

func validateCommonRecordIdentity(driverName, installationID, clusterUID, logicalVolumeID, handleHash, mappingHash string) error {
	if err := ValidateDriverName(driverName); err != nil {
		return err
	}
	if err := ValidateInstallationID(installationID); err != nil {
		return err
	}
	if err := ValidateClusterUID(clusterUID); err != nil {
		return err
	}
	if err := ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return err
	}
	if err := validateVolumeHandleHash(handleHash); err != nil {
		return err
	}
	return validateMappingHash(mappingHash)
}

func validatePublishedNodes(nodes []string) error {
	if nodes == nil {
		return fmt.Errorf("published node IDs must be an explicit array")
	}
	if !slices.IsSorted(nodes) {
		return fmt.Errorf("published node IDs must be sorted canonically")
	}
	for index, node := range nodes {
		if err := ValidateNodeID(node); err != nil {
			return err
		}
		if index > 0 && nodes[index-1] == node {
			return fmt.Errorf("published node ID %q is duplicated", node)
		}
	}
	return nil
}

func validateRequiredTimestamp(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", name)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !strings.HasSuffix(value, "Z") || parsed.UTC().Format(time.RFC3339Nano) != value {
		return fmt.Errorf("%s %q must be canonical RFC 3339 UTC", name, value)
	}
	return nil
}

func validateOptionalTimestampFields(fields map[string]string) error {
	for name, value := range fields {
		if value != "" {
			if err := validateRequiredTimestamp(name, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validatePersistedPath(basePath, candidate string) error {
	if candidate == "" || !strings.HasPrefix(candidate, "/") || path.Clean(candidate) != candidate {
		return fmt.Errorf("path %q is empty, relative, or non-normalized", candidate)
	}
	if candidate == basePath || !strings.HasPrefix(candidate, basePath+"/") {
		return fmt.Errorf("path %q is not strictly below base path %q", candidate, basePath)
	}
	return nil
}

func validateCompactAudit(deleteOperationID string, deleteOperation DeleteOperation, deleteCompletedAt, gcOperationID, gcCompletedAt string) error {
	if deleteOperationID == "" && gcOperationID == "" {
		return fmt.Errorf("compact tombstone must retain a delete or GC operation ID")
	}
	deletePresent := deleteOperationID != "" || deleteOperation != "" || deleteCompletedAt != ""
	if deletePresent && (deleteOperationID == "" || deleteOperation == "" || deleteCompletedAt == "") {
		return fmt.Errorf("compact delete audit fields must be all present or all absent")
	}
	if deleteOperationID != "" {
		if err := ValidateOperationID(deleteOperationID); err != nil {
			return err
		}
		if deleteOperation == "" || deleteCompletedAt == "" {
			return fmt.Errorf("compact delete audit requires operation and completion timestamp")
		}
	}
	gcPresent := gcOperationID != "" || gcCompletedAt != ""
	if gcPresent && (gcOperationID == "" || gcCompletedAt == "") {
		return fmt.Errorf("compact GC audit fields must be both present or both absent")
	}
	if gcOperationID != "" {
		if err := ValidateOperationID(gcOperationID); err != nil {
			return err
		}
		if gcCompletedAt == "" {
			return fmt.Errorf("compact GC audit requires completion timestamp")
		}
	}
	return validateOptionalTimestampFields(map[string]string{"deleteCompletedAt": deleteCompletedAt, "gcCompletedAt": gcCompletedAt})
}
