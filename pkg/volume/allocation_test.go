package volume

import (
	"bytes"
	"strings"
	"testing"
)

const recordTimestamp = "2026-07-12T12:00:00Z"

func validDetailedAllocation(t *testing.T) *DetailedAllocationRecord {
	t.Helper()
	const (
		driverName  = "file-storage-subdir.csi.urlab.ai"
		requestName = "pvc-123"
	)
	logicalID, err := LogicalVolumeID(driverName, requestName)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	mapping := Mapping{
		PoolName:           "standard",
		ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		BasePath:           "/kubernetes-volumes",
		DirectoryName:      "tenant--claim--0123456789ab",
		LogicalVolumeID:    logicalID,
	}
	handle, err := NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, err := VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatalf("VolumeHandleHash() error = %v", err)
	}
	basePathHash, err := BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	parameters, err := (CreateParameters{
		PoolName:       "standard",
		DeletePolicy:   DeletePolicyArchive,
		DirectoryUID:   1000,
		DirectoryGID:   1000,
		DirectoryMode:  "0770",
		AccessType:     "mount",
		FilesystemType: "virtiofs",
		AccessModes:    []AccessMode{AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	requestHash, err := RequestHash(CreateRequestIdentity{
		OriginalRequiredBytes: 10,
		OriginalLimitBytes:    20,
		SelectedCapacityBytes: 10,
		Parameters:            parameters,
	})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	return &DetailedAllocationRecord{
		SchemaVersion:              SchemaVersionV1,
		RecordKind:                 AllocationRecordDetailed,
		RecordRevision:             1,
		DriverName:                 driverName,
		ActiveClusterUID:           "22222222-2222-4222-8222-222222222222",
		State:                      StateReady,
		InstallationID:             "33333333-3333-4333-8333-333333333333",
		CreateVolumeRequestName:    requestName,
		RequestHash:                requestHash,
		OriginalRequiredBytes:      10,
		OriginalLimitBytes:         20,
		SelectedCapacityBytes:      10,
		NormalizedCreateParameters: parameters,
		LogicalVolumeID:            logicalID,
		VolumeHandle:               handle.String(),
		VolumeHandleHash:           handleHash,
		MappingHash:                handle.MappingHash,
		PoolName:                   mapping.PoolName,
		ParentFilesystemID:         mapping.ParentFilesystemID,
		BasePath:                   mapping.BasePath,
		BasePathHash:               basePathHash,
		DirectoryName:              mapping.DirectoryName,
		ReservesCapacity:           true,
		DeletePolicy:               DeletePolicyArchive,
		DirectoryUID:               1000,
		DirectoryGID:               1000,
		DirectoryMode:              "0770",
		CreatedAt:                  recordTimestamp,
		UpdatedAt:                  recordTimestamp,
		PublishedNodeIDs:           []string{},
	}
}

func TestDetailedAllocationRoundTrip(t *testing.T) {
	want := validDetailedAllocation(t)
	encoded, err := EncodeAllocationRecord(want)
	if err != nil {
		t.Fatalf("EncodeAllocationRecord() error = %v", err)
	}
	if bytes.HasSuffix(encoded, []byte("\n")) {
		t.Fatal("EncodeAllocationRecord() added a newline")
	}
	decoded, err := DecodeAllocationRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeAllocationRecord() error = %v", err)
	}
	got, ok := decoded.(*DetailedAllocationRecord)
	if !ok {
		t.Fatalf("DecodeAllocationRecord() type = %T", decoded)
	}
	reencoded, err := EncodeAllocationRecord(got)
	if err != nil {
		t.Fatalf("EncodeAllocationRecord(decoded) error = %v", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("canonical bytes changed across round trip:\n%s\n%s", encoded, reencoded)
	}
}

func TestDecodeAllocationRejectsUnknownAmbiguousAndLegacySchemas(t *testing.T) {
	encoded, err := EncodeAllocationRecord(validDetailedAllocation(t))
	if err != nil {
		t.Fatalf("EncodeAllocationRecord() error = %v", err)
	}
	withUnknown := append(bytes.Clone(encoded[:len(encoded)-1]), []byte(`,"futureField":true}`)...)

	tests := map[string][]byte{
		"unknown field":  withUnknown,
		"duplicate kind": []byte(`{"recordKind":"detailed","recordKind":"compactDeleted"}`),
		"legacy full":    []byte(`{"recordKind":"full"}`),
		"unknown kind":   []byte(`{"recordKind":"future"}`),
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeAllocationRecord(input); err == nil {
				t.Fatal("DecodeAllocationRecord() error = nil")
			}
		})
	}
}

func TestDetailedAllocationRejectsStateReservationAndHashMismatch(t *testing.T) {
	record := validDetailedAllocation(t)
	record.ReservesCapacity = false
	if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "reservesCapacity") {
		t.Fatalf("Validate(reservation mismatch) error = %v", err)
	}
	record = validDetailedAllocation(t)
	record.MappingHash = "mh-00000000000000000000000000000000"
	if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "mapping hash mismatch") {
		t.Fatalf("Validate(mapping mismatch) error = %v", err)
	}
}

func TestCompactAndDeletedUnknownSchemas(t *testing.T) {
	detailed := validDetailedAllocation(t)
	compact := &CompactDeletedAllocationRecord{
		SchemaVersion:           SchemaVersionV1,
		RecordKind:              AllocationRecordCompactDeleted,
		RecordRevision:          9,
		DriverName:              detailed.DriverName,
		InstallationID:          detailed.InstallationID,
		ActiveClusterUID:        detailed.ActiveClusterUID,
		CreateVolumeRequestName: detailed.CreateVolumeRequestName,
		LogicalVolumeID:         detailed.LogicalVolumeID,
		VolumeHandleHash:        detailed.VolumeHandleHash,
		MappingHash:             detailed.MappingHash,
		State:                   StateDeleted,
		ParentFilesystemID:      detailed.ParentFilesystemID,
		DirectoryName:           detailed.DirectoryName,
		ReservesCapacity:        false,
		DeleteResult:            "deleted",
		UpdatedAt:               recordTimestamp,
		DeletedAt:               recordTimestamp,
		DeleteOperationID:       "44444444-4444-4444-8444-444444444444",
		DeleteOperation:         DeleteOperationDelete,
		DeleteCompletedAt:       recordTimestamp,
	}
	encoded, err := EncodeAllocationRecord(compact)
	if err != nil {
		t.Fatalf("EncodeAllocationRecord(compact) error = %v", err)
	}
	if decoded, err := DecodeAllocationRecord(encoded); err != nil {
		t.Fatalf("DecodeAllocationRecord(compact) error = %v", err)
	} else if decoded.Kind() != AllocationRecordCompactDeleted {
		t.Fatalf("decoded compact kind = %q", decoded.Kind())
	}

	unknown := &DeletedUnknownAllocationRecord{
		SchemaVersion:    SchemaVersionV1,
		RecordKind:       AllocationRecordDeletedUnknown,
		RecordRevision:   1,
		DriverName:       detailed.DriverName,
		InstallationID:   detailed.InstallationID,
		ActiveClusterUID: detailed.ActiveClusterUID,
		LogicalVolumeID:  detailed.LogicalVolumeID,
		VolumeHandleHash: detailed.VolumeHandleHash,
		MappingHash:      detailed.MappingHash,
		State:            StateDeleted,
		ReservesCapacity: false,
		AbsenceReason:    "all authoritative sources conclusively absent",
		CreatedAt:        recordTimestamp,
		UpdatedAt:        recordTimestamp,
		DeletedAt:        recordTimestamp,
	}
	encoded, err = EncodeAllocationRecord(unknown)
	if err != nil {
		t.Fatalf("EncodeAllocationRecord(deletedUnknown) error = %v", err)
	}
	if decoded, err := DecodeAllocationRecord(encoded); err != nil {
		t.Fatalf("DecodeAllocationRecord(deletedUnknown) error = %v", err)
	} else if decoded.Kind() != AllocationRecordDeletedUnknown {
		t.Fatalf("decoded deletedUnknown kind = %q", decoded.Kind())
	}
}

func TestAllocationStateReservationTable(t *testing.T) {
	for _, state := range []AllocationState{StateReserved, StateCreatingDirectory, StateReady, StateDeleting, StateArchived, StateRetained} {
		if reserves, err := state.Reserves(); err != nil || !reserves {
			t.Errorf("%s.Reserves() = %v, %v; want true, nil", state, reserves, err)
		}
	}
	if reserves, err := StateDeleted.Reserves(); err != nil || reserves {
		t.Errorf("Deleted.Reserves() = %v, %v; want false, nil", reserves, err)
	}
}

func TestDetailedAllocationGCFieldsAreBoundToTerminalStateAndPaths(t *testing.T) {
	record := validDetailedAllocation(t)
	record.State = StateArchived
	record.DeleteOperationID = "44444444-4444-4444-8444-444444444444"
	record.DeleteOperation = DeleteOperationArchive
	record.DeleteSourcePath = record.BasePath + "/" + record.DirectoryName
	archiveTarget, err := ManagedLifecycleTarget(record.BasePath, ".archived", record.DirectoryName, record.LogicalVolumeID, recordTimestamp, record.DeleteOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget(archive) error = %v", err)
	}
	record.ArchivedPath = archiveTarget
	record.DeleteTargetPath = record.ArchivedPath
	record.DeletePreparedAt = recordTimestamp
	record.DeleteCompletedAt = recordTimestamp
	record.DeleteResult = "archived"
	record.GCRequestID = "55555555-5555-4555-8555-555555555555"
	record.GCRequestedMode = "execute"
	record.GCExpectedState = StateArchived
	record.GCRequestedAt = recordTimestamp
	if err := record.Validate(); err != nil {
		t.Fatalf("Validate(GC request) error = %v", err)
	}
	tamperedArchive := *record
	tamperedArchive.DeleteTargetPath = record.BasePath + "/.archived/other-direct-child"
	tamperedArchive.ArchivedPath = tamperedArchive.DeleteTargetPath
	if err := tamperedArchive.Validate(); err == nil {
		t.Fatal("Validate(tampered archive target) error = nil")
	}

	record.GCOperationID = "66666666-6666-4666-8666-666666666666"
	record.GCTargetPath = record.ArchivedPath
	record.GCStartedAt = recordTimestamp
	gcQuarantine, err := ManagedLifecycleTarget(record.BasePath, ".deleted", record.DirectoryName, record.LogicalVolumeID, record.GCStartedAt, record.GCOperationID)
	if err != nil {
		t.Fatalf("ManagedLifecycleTarget(GC) error = %v", err)
	}
	record.GCQuarantinePath = gcQuarantine
	if err := record.Validate(); err != nil {
		t.Fatalf("Validate(GC progress) error = %v", err)
	}
	tamperedGC := *record
	tamperedGC.GCQuarantinePath = record.BasePath + "/.deleted/other-direct-child"
	if err := tamperedGC.Validate(); err == nil {
		t.Fatal("Validate(tampered GC quarantine) error = nil")
	}

	record.GCTargetPath = record.DeleteSourcePath
	if err := record.Validate(); err == nil {
		t.Fatal("Validate(GC wrong source) error = nil")
	}
	record.GCTargetPath = record.ArchivedPath
	record.GCQuarantinePath = record.BasePath + "/outside/" + record.DirectoryName
	if err := record.Validate(); err == nil {
		t.Fatal("Validate(GC wrong quarantine parent) error = nil")
	}

	record = validDetailedAllocation(t)
	record.GCRequestID = "55555555-5555-4555-8555-555555555555"
	record.GCRequestedMode = "dry-run"
	record.GCExpectedState = StateArchived
	record.GCRequestedAt = recordTimestamp
	if err := record.Validate(); err == nil {
		t.Fatal("Validate(GC request on Ready) error = nil")
	}
}
