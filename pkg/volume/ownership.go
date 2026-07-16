package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
)

const (
	// ParentOwnerPath is the fixed immutable root-level parent claim.
	ParentOwnerPath = "/.sfs-subdir-csi-owner.json"
	// LeadershipLeaseNameV1 is embedded into parent claims and recovery
	// manifests. It is deliberately not configurable in v1.
	LeadershipLeaseNameV1 = "scaleway-sfs-subdir-csi-controller"
	// OwnershipMetadataDirectory is outside workload-visible data directories.
	OwnershipMetadataDirectory = ".sfs-subdir-csi/volumes"
)

// OwnershipRecordKind discriminates detailed records from compact tombstones.
type OwnershipRecordKind string

const (
	// OwnershipRecordDetailed authorizes lifecycle operations according to its
	// state and contains the immutable recovery envelope.
	OwnershipRecordDetailed OwnershipRecordKind = "detailed"
	// OwnershipRecordCompactDeleted is permanent and never authorizes a mount
	// or filesystem mutation.
	OwnershipRecordCompactDeleted OwnershipRecordKind = "compactDeleted"
)

// ParentOwnerRecord is the immutable parent-global ownership claim.
type ParentOwnerRecord struct {
	SchemaVersion       string `json:"schemaVersion"`
	Revision            uint64 `json:"revision"`
	DriverName          string `json:"driverName"`
	InstallationID      string `json:"installationID"`
	ActiveClusterUID    string `json:"activeClusterUID"`
	ParentFilesystemID  string `json:"parentFilesystemID"`
	BasePath            string `json:"basePath"`
	BasePathHash        string `json:"basePathHash"`
	ControllerNamespace string `json:"controllerNamespace"`
	HelmReleaseName     string `json:"helmReleaseName"`
	LeadershipLeaseName string `json:"leadershipLeaseName"`
	BootstrapAttemptID  string `json:"bootstrapAttemptID"`
	ContentChecksum     string `json:"contentChecksum"`
	CreatedAt           string `json:"createdAt"`
}

// OwnershipRecord is one exact v1 per-volume filesystem record.
type OwnershipRecord interface {
	Validate() error
	Kind() OwnershipRecordKind
	LifecycleState() AllocationState
	LogicalID() string
}

// DetailedOwnershipRecord is the authoritative filesystem ownership and
// recovery envelope stored outside the user-writable data directory.
type DetailedOwnershipRecord struct {
	SchemaVersion              string              `json:"schemaVersion"`
	RecordKind                 OwnershipRecordKind `json:"recordKind"`
	DriverName                 string              `json:"driverName"`
	InstallationID             string              `json:"installationID"`
	ActiveClusterUID           string              `json:"activeClusterUID"`
	VolumeHandle               string              `json:"volumeHandle"`
	VolumeHandleHash           string              `json:"volumeHandleHash"`
	LogicalVolumeID            string              `json:"logicalVolumeID"`
	MappingHash                string              `json:"mappingHash"`
	PoolName                   string              `json:"poolName"`
	ParentFilesystemID         string              `json:"parentFilesystemID"`
	BasePath                   string              `json:"basePath"`
	BasePathHash               string              `json:"basePathHash"`
	DirectoryName              string              `json:"directoryName"`
	CreateVolumeRequestName    string              `json:"createVolumeRequestName"`
	RequestHash                string              `json:"requestHash"`
	OriginalRequiredBytes      uint64              `json:"originalRequiredBytes"`
	OriginalLimitBytes         uint64              `json:"originalLimitBytes"`
	SelectedCapacityBytes      uint64              `json:"selectedCapacityBytes"`
	NormalizedCreateParameters CreateParameters    `json:"normalizedCreateParameters"`
	DeletePolicy               DeletePolicy        `json:"deletePolicy"`
	DirectoryUID               uint32              `json:"directoryUid"`
	DirectoryGID               uint32              `json:"directoryGid"`
	DirectoryMode              string              `json:"directoryMode"`
	PublishedNodeIDs           []string            `json:"publishedNodeIDs"`
	State                      AllocationState     `json:"state"`
	Revision                   uint64              `json:"revision"`
	ContentChecksum            string              `json:"contentChecksum"`
	CreatedAt                  string              `json:"createdAt"`
	DeleteOperationID          string              `json:"deleteOperationID,omitempty"`
	DeleteOperation            DeleteOperation     `json:"deleteOperation,omitempty"`
	DeleteSourcePath           string              `json:"deleteSourcePath,omitempty"`
	DeleteTargetPath           string              `json:"deleteTargetPath,omitempty"`
	DeletePreparedAt           string              `json:"deletePreparedAt,omitempty"`
	DeleteRemoveStartedAt      string              `json:"deleteRemoveStartedAt,omitempty"`
	DeleteCompletedAt          string              `json:"deleteCompletedAt,omitempty"`
	ArchivedPath               string              `json:"archivedPath,omitempty"`
	RetainedPath               string              `json:"retainedPath,omitempty"`
	QuarantinePath             string              `json:"quarantinePath,omitempty"`
	GCRequestID                string              `json:"gcRequestID,omitempty"`
	GCRequestedMode            string              `json:"gcRequestedMode,omitempty"`
	GCExpectedState            AllocationState     `json:"gcExpectedState,omitempty"`
	GCRequestedAt              string              `json:"gcRequestedAt,omitempty"`
	GCOperationID              string              `json:"gcOperationID,omitempty"`
	GCTargetPath               string              `json:"gcTargetPath,omitempty"`
	GCQuarantinePath           string              `json:"gcQuarantinePath,omitempty"`
	GCStartedAt                string              `json:"gcStartedAt,omitempty"`
	GCRemoveStartedAt          string              `json:"gcRemoveStartedAt,omitempty"`
	GCCompletedAt              string              `json:"gcCompletedAt,omitempty"`
}

// CompactDeletedOwnershipRecord is a permanent, non-authorizing filesystem
// tombstone paired with the Kubernetes allocation tombstone.
type CompactDeletedOwnershipRecord struct {
	SchemaVersion           string              `json:"schemaVersion"`
	RecordKind              OwnershipRecordKind `json:"recordKind"`
	Revision                uint64              `json:"revision"`
	DriverName              string              `json:"driverName"`
	InstallationID          string              `json:"installationID"`
	ActiveClusterUID        string              `json:"activeClusterUID"`
	VolumeHandleHash        string              `json:"volumeHandleHash"`
	LogicalVolumeID         string              `json:"logicalVolumeID"`
	CreateVolumeRequestName string              `json:"createVolumeRequestName"`
	MappingHash             string              `json:"mappingHash"`
	ParentFilesystemID      string              `json:"parentFilesystemID"`
	BasePathHash            string              `json:"basePathHash"`
	DirectoryName           string              `json:"directoryName"`
	State                   AllocationState     `json:"state"`
	DeleteResult            string              `json:"deleteResult"`
	UpdatedAt               string              `json:"updatedAt"`
	DeletedAt               string              `json:"deletedAt"`
	ContentChecksum         string              `json:"contentChecksum"`
	DeleteOperation         DeleteOperation     `json:"deleteOperation,omitempty"`
	ArchivedPath            string              `json:"archivedPath,omitempty"`
	RetainedPath            string              `json:"retainedPath,omitempty"`
	QuarantinePath          string              `json:"quarantinePath,omitempty"`
	DeleteOperationID       string              `json:"deleteOperationID,omitempty"`
	DeleteCompletedAt       string              `json:"deleteCompletedAt,omitempty"`
	GCOperationID           string              `json:"gcOperationID,omitempty"`
	GCTargetPath            string              `json:"gcTargetPath,omitempty"`
	GCQuarantinePath        string              `json:"gcQuarantinePath,omitempty"`
	GCCompletedAt           string              `json:"gcCompletedAt,omitempty"`
}

// OwnershipRecordPath returns the absolute metadata path for one logical ID.
func OwnershipRecordPath(basePath, logicalVolumeID string) (string, error) {
	if err := ValidateBasePath(basePath); err != nil {
		return "", err
	}
	if err := ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return "", err
	}
	return path.Join(basePath, OwnershipMetadataDirectory, logicalVolumeID+".json"), nil
}

// Seal validates a parent claim payload and writes its canonical checksum.
func (record ParentOwnerRecord) Seal() (ParentOwnerRecord, error) {
	record.ContentChecksum = ""
	if err := record.validatePayload(); err != nil {
		return ParentOwnerRecord{}, err
	}
	checksum, err := contentChecksum(record)
	if err != nil {
		return ParentOwnerRecord{}, err
	}
	record.ContentChecksum = checksum
	return record, nil
}

// Validate authenticates a complete immutable parent claim.
func (record ParentOwnerRecord) Validate() error {
	if err := record.validatePayload(); err != nil {
		return err
	}
	return validateContentChecksum(record.ContentChecksum, record)
}

// EncodeParentOwnerRecord returns canonical validated claim bytes.
func EncodeParentOwnerRecord(record ParentOwnerRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(record)
}

// DecodeParentOwnerRecord rejects open or ambiguous claim bytes.
func DecodeParentOwnerRecord(data []byte) (ParentOwnerRecord, error) {
	var record ParentOwnerRecord
	if err := strictjson.Decode(data, &record); err != nil {
		return ParentOwnerRecord{}, err
	}
	if err := record.Validate(); err != nil {
		return ParentOwnerRecord{}, err
	}
	return record, nil
}

// Seal validates a detailed ownership payload and writes its checksum.
func (record DetailedOwnershipRecord) Seal() (DetailedOwnershipRecord, error) {
	record.ContentChecksum = ""
	if err := record.validatePayload(); err != nil {
		return DetailedOwnershipRecord{}, err
	}
	checksum, err := contentChecksum(record)
	if err != nil {
		return DetailedOwnershipRecord{}, err
	}
	record.ContentChecksum = checksum
	return record, nil
}

// Validate authenticates a detailed ownership record.
func (record *DetailedOwnershipRecord) Validate() error {
	if record == nil {
		return fmt.Errorf("detailed ownership record is nil")
	}
	if err := record.validatePayload(); err != nil {
		return err
	}
	return validateContentChecksum(record.ContentChecksum, *record)
}

// Kind returns the ownership schema discriminator.
func (record *DetailedOwnershipRecord) Kind() OwnershipRecordKind { return record.RecordKind }

// LifecycleState returns the durable ownership state.
func (record *DetailedOwnershipRecord) LifecycleState() AllocationState { return record.State }

// LogicalID returns the deterministic logical-volume identity.
func (record *DetailedOwnershipRecord) LogicalID() string { return record.LogicalVolumeID }

// Seal validates a compact ownership payload and writes its checksum.
func (record CompactDeletedOwnershipRecord) Seal() (CompactDeletedOwnershipRecord, error) {
	record.ContentChecksum = ""
	if err := record.validatePayload(); err != nil {
		return CompactDeletedOwnershipRecord{}, err
	}
	checksum, err := contentChecksum(record)
	if err != nil {
		return CompactDeletedOwnershipRecord{}, err
	}
	record.ContentChecksum = checksum
	return record, nil
}

// Validate authenticates a compact ownership tombstone.
func (record *CompactDeletedOwnershipRecord) Validate() error {
	if record == nil {
		return fmt.Errorf("compact ownership record is nil")
	}
	if err := record.validatePayload(); err != nil {
		return err
	}
	return validateContentChecksum(record.ContentChecksum, *record)
}

// Kind returns the ownership schema discriminator.
func (record *CompactDeletedOwnershipRecord) Kind() OwnershipRecordKind { return record.RecordKind }

// LifecycleState returns Deleted for a compact tombstone.
func (record *CompactDeletedOwnershipRecord) LifecycleState() AllocationState { return record.State }

// LogicalID returns the deterministic logical-volume identity.
func (record *CompactDeletedOwnershipRecord) LogicalID() string { return record.LogicalVolumeID }

// EncodeOwnershipRecord returns canonical, checksum-authenticated bytes.
func EncodeOwnershipRecord(record OwnershipRecord) ([]byte, error) {
	if record == nil {
		return nil, fmt.Errorf("ownership record is nil")
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return canonicaljson.Marshal(record)
}

// DecodeOwnershipRecord accepts only detailed and compactDeleted v1 schemas.
func DecodeOwnershipRecord(data []byte) (OwnershipRecord, error) {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(data, &fields); err != nil {
		return nil, err
	}
	rawKind, present := fields["recordKind"]
	if !present {
		return nil, fmt.Errorf("ownership record kind is missing")
	}
	var kind OwnershipRecordKind
	if err := strictjson.Decode(rawKind, &kind); err != nil {
		return nil, fmt.Errorf("decode ownership record kind: %w", err)
	}
	var record OwnershipRecord
	switch kind {
	case OwnershipRecordDetailed:
		record = &DetailedOwnershipRecord{}
	case OwnershipRecordCompactDeleted:
		record = &CompactDeletedOwnershipRecord{}
	default:
		return nil, fmt.Errorf("ownership record kind %q is unsupported", kind)
	}
	if err := strictjson.Decode(data, record); err != nil {
		return nil, err
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return record, nil
}

func (record ParentOwnerRecord) validatePayload() error {
	if record.SchemaVersion != SchemaVersionV1 || record.Revision == 0 {
		return fmt.Errorf("parent owner schema version or revision is invalid")
	}
	if err := ValidateDriverName(record.DriverName); err != nil {
		return err
	}
	if err := ValidateInstallationID(record.InstallationID); err != nil {
		return err
	}
	if err := ValidateClusterUID(record.ActiveClusterUID); err != nil {
		return err
	}
	if err := ValidateParentFilesystemID(record.ParentFilesystemID); err != nil {
		return err
	}
	if err := ValidateBasePath(record.BasePath); err != nil {
		return err
	}
	wantBasePathHash, err := BasePathHash(record.BasePath)
	if err != nil {
		return err
	}
	if record.BasePathHash != wantBasePathHash {
		return fmt.Errorf("parent owner base path hash mismatch")
	}
	if err := ValidatePoolName(record.ControllerNamespace); err != nil {
		return fmt.Errorf("controller namespace: %w", err)
	}
	if err := ValidatePoolName(record.HelmReleaseName); err != nil {
		return fmt.Errorf("helm release name: %w", err)
	}
	if record.LeadershipLeaseName != LeadershipLeaseNameV1 {
		return fmt.Errorf("leadership Lease name %q does not match fixed v1 name", record.LeadershipLeaseName)
	}
	if err := ValidateOperationID(record.BootstrapAttemptID); err != nil {
		return fmt.Errorf("bootstrap attempt: %w", err)
	}
	return validateRequiredTimestamp("createdAt", record.CreatedAt)
}

func (record DetailedOwnershipRecord) validatePayload() error {
	if record.SchemaVersion != SchemaVersionV1 || record.RecordKind != OwnershipRecordDetailed || record.Revision == 0 {
		return fmt.Errorf("detailed ownership schema, kind, or revision is invalid")
	}
	if record.State == StateReserved || record.State == StateCreatingDirectory || record.State == StateDeleted {
		return fmt.Errorf("detailed ownership state %q is not a valid filesystem ownership state", record.State)
	}
	allocation := DetailedAllocationRecord{
		SchemaVersion:              record.SchemaVersion,
		RecordKind:                 AllocationRecordDetailed,
		RecordRevision:             record.Revision,
		DriverName:                 record.DriverName,
		ActiveClusterUID:           record.ActiveClusterUID,
		State:                      record.State,
		InstallationID:             record.InstallationID,
		CreateVolumeRequestName:    record.CreateVolumeRequestName,
		RequestHash:                record.RequestHash,
		OriginalRequiredBytes:      record.OriginalRequiredBytes,
		OriginalLimitBytes:         record.OriginalLimitBytes,
		SelectedCapacityBytes:      record.SelectedCapacityBytes,
		NormalizedCreateParameters: record.NormalizedCreateParameters,
		LogicalVolumeID:            record.LogicalVolumeID,
		VolumeHandle:               record.VolumeHandle,
		VolumeHandleHash:           record.VolumeHandleHash,
		MappingHash:                record.MappingHash,
		PoolName:                   record.PoolName,
		ParentFilesystemID:         record.ParentFilesystemID,
		BasePath:                   record.BasePath,
		BasePathHash:               record.BasePathHash,
		DirectoryName:              record.DirectoryName,
		ReservesCapacity:           true,
		DeletePolicy:               record.DeletePolicy,
		DirectoryUID:               record.DirectoryUID,
		DirectoryGID:               record.DirectoryGID,
		DirectoryMode:              record.DirectoryMode,
		CreatedAt:                  record.CreatedAt,
		UpdatedAt:                  record.CreatedAt,
		PublishedNodeIDs:           record.PublishedNodeIDs,
		DeleteOperationID:          record.DeleteOperationID,
		DeleteOperation:            record.DeleteOperation,
		DeleteSourcePath:           record.DeleteSourcePath,
		DeleteTargetPath:           record.DeleteTargetPath,
		DeletePreparedAt:           record.DeletePreparedAt,
		DeleteRemoveStartedAt:      record.DeleteRemoveStartedAt,
		DeleteCompletedAt:          record.DeleteCompletedAt,
		ArchivedPath:               record.ArchivedPath,
		RetainedPath:               record.RetainedPath,
		QuarantinePath:             record.QuarantinePath,
		GCRequestID:                record.GCRequestID,
		GCRequestedMode:            record.GCRequestedMode,
		GCExpectedState:            record.GCExpectedState,
		GCRequestedAt:              record.GCRequestedAt,
		GCOperationID:              record.GCOperationID,
		GCTargetPath:               record.GCTargetPath,
		GCQuarantinePath:           record.GCQuarantinePath,
		GCStartedAt:                record.GCStartedAt,
		GCRemoveStartedAt:          record.GCRemoveStartedAt,
		GCCompletedAt:              record.GCCompletedAt,
	}
	// The detailed ownership schema proves terminal outcome through state,
	// operation, and paths; deleteResult exists only in the allocation schema.
	// A non-empty validation sentinel lets the shared lifecycle validator check
	// those persisted ownership fields without inventing a serialized value.
	if record.State == StateArchived || record.State == StateRetained {
		allocation.DeleteResult = "validated-by-ownership-state"
	}
	return allocation.Validate()
}

func (record CompactDeletedOwnershipRecord) validatePayload() error {
	if record.SchemaVersion != SchemaVersionV1 || record.RecordKind != OwnershipRecordCompactDeleted || record.Revision == 0 || record.State != StateDeleted {
		return fmt.Errorf("compact ownership schema, kind, revision, or state is invalid")
	}
	if record.DeleteResult == "" || record.CreateVolumeRequestName == "" {
		return fmt.Errorf("compact ownership record lacks terminal result or request identity")
	}
	if err := validateCommonRecordIdentity(record.DriverName, record.InstallationID, record.ActiveClusterUID, record.LogicalVolumeID, record.VolumeHandleHash, record.MappingHash); err != nil {
		return err
	}
	wantLogicalID, err := LogicalVolumeID(record.DriverName, record.CreateVolumeRequestName)
	if err != nil {
		return err
	}
	if record.LogicalVolumeID != wantLogicalID {
		return fmt.Errorf("compact ownership logical ID does not match request name")
	}
	if err := ValidateParentFilesystemID(record.ParentFilesystemID); err != nil {
		return err
	}
	if err := validateBasePathHash(record.BasePathHash); err != nil {
		return err
	}
	if err := ValidateDirectoryName(record.DirectoryName); err != nil {
		return err
	}
	if err := validateRequiredTimestamp("updatedAt", record.UpdatedAt); err != nil {
		return err
	}
	if err := validateRequiredTimestamp("deletedAt", record.DeletedAt); err != nil {
		return err
	}
	return validateCompactAudit(record.DeleteOperationID, record.DeleteOperation, record.DeleteCompletedAt, record.GCOperationID, record.GCCompletedAt)
}

func contentChecksum(record any) (string, error) {
	encoded, err := canonicaljson.Marshal(record)
	if err != nil {
		return "", err
	}
	var generic map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return "", fmt.Errorf("decode record for checksum: %w", err)
	}
	delete(generic, "contentChecksum")
	payload, err := canonicaljson.Marshal(generic)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateContentChecksum(stored string, record any) error {
	if !strings.HasPrefix(stored, "sha256:") || len(stored) != len("sha256:")+sha256.Size*2 {
		return fmt.Errorf("content checksum %q is malformed", stored)
	}
	want, err := contentChecksum(record)
	if err != nil {
		return err
	}
	if stored != want {
		return fmt.Errorf("content checksum mismatch: stored=%q computed=%q", stored, want)
	}
	return nil
}
