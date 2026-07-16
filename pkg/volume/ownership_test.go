package volume

import (
	"bytes"
	"strings"
	"testing"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
)

func validParentOwner(t *testing.T) ParentOwnerRecord {
	t.Helper()
	basePathHash, err := BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	record, err := (ParentOwnerRecord{
		SchemaVersion:       SchemaVersionV1,
		Revision:            1,
		DriverName:          "sfs-subdir.csi.example.com",
		InstallationID:      "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:    "22222222-2222-4222-8222-222222222222",
		ParentFilesystemID:  "33333333-3333-4333-8333-333333333333",
		BasePath:            "/kubernetes-volumes",
		BasePathHash:        basePathHash,
		ControllerNamespace: "scaleway-sfs-subdir-csi",
		HelmReleaseName:     "scaleway-sfs-subdir-csi",
		LeadershipLeaseName: LeadershipLeaseNameV1,
		BootstrapAttemptID:  "44444444-4444-4444-8444-444444444444",
		CreatedAt:           recordTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	return record
}

func detailedOwnershipFromAllocation(t *testing.T) DetailedOwnershipRecord {
	t.Helper()
	allocation := validDetailedAllocation(t)
	record, err := (DetailedOwnershipRecord{
		SchemaVersion:              allocation.SchemaVersion,
		RecordKind:                 OwnershipRecordDetailed,
		DriverName:                 allocation.DriverName,
		InstallationID:             allocation.InstallationID,
		ActiveClusterUID:           allocation.ActiveClusterUID,
		VolumeHandle:               allocation.VolumeHandle,
		VolumeHandleHash:           allocation.VolumeHandleHash,
		LogicalVolumeID:            allocation.LogicalVolumeID,
		MappingHash:                allocation.MappingHash,
		PoolName:                   allocation.PoolName,
		ParentFilesystemID:         allocation.ParentFilesystemID,
		BasePath:                   allocation.BasePath,
		BasePathHash:               allocation.BasePathHash,
		DirectoryName:              allocation.DirectoryName,
		CreateVolumeRequestName:    allocation.CreateVolumeRequestName,
		RequestHash:                allocation.RequestHash,
		OriginalRequiredBytes:      allocation.OriginalRequiredBytes,
		OriginalLimitBytes:         allocation.OriginalLimitBytes,
		SelectedCapacityBytes:      allocation.SelectedCapacityBytes,
		NormalizedCreateParameters: allocation.NormalizedCreateParameters,
		DeletePolicy:               allocation.DeletePolicy,
		DirectoryUID:               allocation.DirectoryUID,
		DirectoryGID:               allocation.DirectoryGID,
		DirectoryMode:              allocation.DirectoryMode,
		PublishedNodeIDs:           []string{},
		State:                      StateReady,
		Revision:                   1,
		CreatedAt:                  allocation.CreatedAt,
	}).Seal()
	if err != nil {
		t.Fatalf("DetailedOwnershipRecord.Seal() error = %v", err)
	}
	return record
}

func TestParentOwnerRecordChecksumAndClosedSchema(t *testing.T) {
	record := validParentOwner(t)
	encoded, err := EncodeParentOwnerRecord(record)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	decoded, err := DecodeParentOwnerRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeParentOwnerRecord() error = %v", err)
	}
	if decoded.ContentChecksum != record.ContentChecksum {
		t.Fatalf("checksum changed across round trip: %q != %q", decoded.ContentChecksum, record.ContentChecksum)
	}

	decoded.BasePath = "/different"
	if err := decoded.Validate(); err == nil {
		t.Fatal("Validate(mutated claim) error = nil")
	}

	withUnknown := append(bytes.Clone(encoded[:len(encoded)-1]), []byte(`,"futureField":"unsafe"}`)...)
	if _, err := DecodeParentOwnerRecord(withUnknown); err == nil {
		t.Fatal("DecodeParentOwnerRecord(unknown field) error = nil")
	}
}

func TestParentOwnerRecordRejectsLeaseOrClusterMismatch(t *testing.T) {
	record := validParentOwner(t)
	record.LeadershipLeaseName = "configurable-lease"
	if _, err := record.Seal(); err == nil || !strings.Contains(err.Error(), "fixed v1 name") {
		t.Fatalf("Seal(lease mismatch) error = %v", err)
	}
	record = validParentOwner(t)
	record.ActiveClusterUID = ""
	if _, err := record.Seal(); err == nil {
		t.Fatal("Seal(empty cluster UID) error = nil")
	}
}

func TestDetailedOwnershipChecksumAndRoundTrip(t *testing.T) {
	record := detailedOwnershipFromAllocation(t)
	encoded, err := EncodeOwnershipRecord(&record)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	decoded, err := DecodeOwnershipRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeOwnershipRecord() error = %v", err)
	}
	if decoded.Kind() != OwnershipRecordDetailed || decoded.LifecycleState() != StateReady {
		t.Fatalf("decoded ownership kind/state = %q/%q", decoded.Kind(), decoded.LifecycleState())
	}

	record.DirectoryMode = "0777"
	if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated immutable") {
		t.Fatalf("Validate(mutated mode) error = %v", err)
	}
}

func TestCompactOwnershipSchema(t *testing.T) {
	detailed := validDetailedAllocation(t)
	record, err := (CompactDeletedOwnershipRecord{
		SchemaVersion:           SchemaVersionV1,
		RecordKind:              OwnershipRecordCompactDeleted,
		Revision:                10,
		DriverName:              detailed.DriverName,
		InstallationID:          detailed.InstallationID,
		ActiveClusterUID:        detailed.ActiveClusterUID,
		VolumeHandleHash:        detailed.VolumeHandleHash,
		LogicalVolumeID:         detailed.LogicalVolumeID,
		CreateVolumeRequestName: detailed.CreateVolumeRequestName,
		MappingHash:             detailed.MappingHash,
		ParentFilesystemID:      detailed.ParentFilesystemID,
		BasePathHash:            detailed.BasePathHash,
		DirectoryName:           detailed.DirectoryName,
		State:                   StateDeleted,
		DeleteResult:            "deleted",
		UpdatedAt:               recordTimestamp,
		DeletedAt:               recordTimestamp,
		DeleteOperation:         DeleteOperationDelete,
		DeleteOperationID:       "55555555-5555-4555-8555-555555555555",
		DeleteCompletedAt:       recordTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("CompactDeletedOwnershipRecord.Seal() error = %v", err)
	}
	encoded, err := EncodeOwnershipRecord(&record)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord(compact) error = %v", err)
	}
	decoded, err := DecodeOwnershipRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeOwnershipRecord(compact) error = %v", err)
	}
	if decoded.Kind() != OwnershipRecordCompactDeleted {
		t.Fatalf("decoded compact kind = %q", decoded.Kind())
	}
}

func TestDecodeOwnershipRejectsUnknownKindAndChecksumMismatch(t *testing.T) {
	if _, err := DecodeOwnershipRecord([]byte(`{"recordKind":"future"}`)); err == nil {
		t.Fatal("DecodeOwnershipRecord(unknown kind) error = nil")
	}
	record := detailedOwnershipFromAllocation(t)
	record.ContentChecksum = "sha256:" + strings.Repeat("0", 64)
	encoded, err := canonicalOwnershipWithoutValidation(record)
	if err != nil {
		t.Fatalf("canonicalOwnershipWithoutValidation() error = %v", err)
	}
	if _, err := DecodeOwnershipRecord(encoded); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("DecodeOwnershipRecord(checksum mismatch) error = %v", err)
	}
}

func canonicalOwnershipWithoutValidation(record DetailedOwnershipRecord) ([]byte, error) {
	return canonicaljson.Marshal(record)
}
