package safety

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	safetyTimestamp = "2026-07-12T12:00:00Z"
	safetyAttemptID = "44444444-4444-4444-8444-444444444444"
	safetyCreateID  = "55555555-5555-4555-8555-555555555555"
)

func safetyParentOwner(t *testing.T) volume.ParentOwnerRecord {
	t.Helper()
	baseHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	record, err := (volume.ParentOwnerRecord{
		SchemaVersion:       volume.SchemaVersionV1,
		Revision:            1,
		DriverName:          "file-storage-subdir.csi.urlab.ai",
		InstallationID:      "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:    "22222222-2222-4222-8222-222222222222",
		ParentFilesystemID:  "33333333-3333-4333-8333-333333333333",
		BasePath:            "/kubernetes-volumes",
		BasePathHash:        baseHash,
		ControllerNamespace: "scaleway-sfs-subdir-csi",
		HelmReleaseName:     "scaleway-sfs-subdir-csi",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  safetyAttemptID,
		CreatedAt:           safetyTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	return record
}

func safetyOwnership(t *testing.T) volume.DetailedOwnershipRecord {
	t.Helper()
	const (
		driverName  = "file-storage-subdir.csi.urlab.ai"
		requestName = "pvc-safety"
	)
	logicalID, err := volume.LogicalVolumeID(driverName, requestName)
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	mapping := volume.Mapping{
		PoolName:           "standard",
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes",
		DirectoryName:      "tenant--claim--0123456789ab",
		LogicalVolumeID:    logicalID,
	}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		t.Fatalf("NewHandle() error = %v", err)
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		t.Fatalf("VolumeHandleHash() error = %v", err)
	}
	baseHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	parameters, err := (volume.CreateParameters{
		PoolName:       mapping.PoolName,
		DeletePolicy:   volume.DeletePolicyArchive,
		DirectoryUID:   1000,
		DirectoryGID:   1000,
		DirectoryMode:  "0770",
		AccessType:     "mount",
		FilesystemType: "virtiofs",
		AccessModes:    []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter},
	}).Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, Parameters: parameters})
	if err != nil {
		t.Fatalf("RequestHash() error = %v", err)
	}
	record, err := (volume.DetailedOwnershipRecord{
		SchemaVersion:              volume.SchemaVersionV1,
		RecordKind:                 volume.OwnershipRecordDetailed,
		DriverName:                 driverName,
		InstallationID:             "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:           "22222222-2222-4222-8222-222222222222",
		VolumeHandle:               handle.String(),
		VolumeHandleHash:           handleHash,
		LogicalVolumeID:            logicalID,
		MappingHash:                handle.MappingHash,
		PoolName:                   mapping.PoolName,
		ParentFilesystemID:         mapping.ParentFilesystemID,
		BasePath:                   mapping.BasePath,
		BasePathHash:               baseHash,
		DirectoryName:              mapping.DirectoryName,
		CreateVolumeRequestName:    requestName,
		RequestHash:                requestHash,
		OriginalRequiredBytes:      1,
		SelectedCapacityBytes:      1,
		NormalizedCreateParameters: parameters,
		DeletePolicy:               volume.DeletePolicyArchive,
		DirectoryUID:               1000,
		DirectoryGID:               1000,
		DirectoryMode:              "0770",
		PublishedNodeIDs:           []string{},
		State:                      volume.StateReady,
		Revision:                   1,
		CreatedAt:                  safetyTimestamp,
	}).Seal()
	if err != nil {
		t.Fatalf("DetailedOwnershipRecord.Seal() error = %v", err)
	}
	return record
}

func TestParentClaimCrashPointsExposeNoPartialClaim(t *testing.T) {
	record := safetyParentOwner(t)
	expected, err := volume.EncodeParentOwnerRecord(record)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	for crashPoint := 1; crashPoint <= 5; crashPoint++ {
		t.Run(string(rune('0'+crashPoint)), func(t *testing.T) {
			filesystem := NewMemoryDurableFS()
			filesystem.CrashAfter(crashPoint)
			writer, err := NewMetadataWriter(filesystem)
			if err != nil {
				t.Fatalf("NewMetadataWriter() error = %v", err)
			}
			err = writer.InstallParentOwner(context.Background(), safetyAttemptID, record)
			if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("InstallParentOwner() error = %v, want injected crash", err)
			}
			filesystem.Crash()
			claim := filesystem.LiveSnapshot()[stringsTrimRoot(volume.ParentOwnerPath)]
			if claim != nil {
				if !bytes.Equal(claim, expected) {
					t.Fatalf("durable claim is partial:\n%s", claim)
				}
				if _, err := volume.DecodeParentOwnerRecord(claim); err != nil {
					t.Fatalf("durable claim validation error = %v", err)
				}
			}
			if err := writer.InstallParentOwner(context.Background(), safetyAttemptID, record); err != nil {
				t.Fatalf("InstallParentOwner() retry after crash %d error = %v", crashPoint, err)
			}
			snapshot := filesystem.LiveSnapshot()
			if !bytes.Equal(snapshot[stringsTrimRoot(volume.ParentOwnerPath)], expected) {
				t.Fatalf("InstallParentOwner() retry after crash %d claim = %s", crashPoint, snapshot[stringsTrimRoot(volume.ParentOwnerPath)])
			}
			temporary := ".sfs-subdir-csi-owner." + safetyAttemptID + ".tmp"
			if _, present := snapshot[temporary]; present {
				t.Fatalf("InstallParentOwner() retry after crash %d left temporary %q", crashPoint, temporary)
			}
		})
	}
}

func TestParentClaimNeverOverwritesExistingOwner(t *testing.T) {
	filesystem := NewMemoryDurableFS()
	const existing = `{"foreign":true}`
	if err := filesystem.SeedDurable(stringsTrimRoot(volume.ParentOwnerPath), []byte(existing)); err != nil {
		t.Fatalf("SeedDurable() error = %v", err)
	}
	writer, err := NewMetadataWriter(filesystem)
	if err != nil {
		t.Fatalf("NewMetadataWriter() error = %v", err)
	}
	err = writer.InstallParentOwner(context.Background(), safetyAttemptID, safetyParentOwner(t))
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("InstallParentOwner() error = %v, want ErrAlreadyExists", err)
	}
	if got := string(filesystem.LiveSnapshot()[stringsTrimRoot(volume.ParentOwnerPath)]); got != existing {
		t.Fatalf("existing owner changed to %s", got)
	}
}

func TestOwnershipCreationCrashPointsExposeOnlyAbsentOrCompleteRecord(t *testing.T) {
	record := safetyOwnership(t)
	expected, err := volume.EncodeOwnershipRecord(&record)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord() error = %v", err)
	}
	destination, _, err := ownershipRelativePaths(record.BasePath, record.LogicalVolumeID)
	if err != nil {
		t.Fatalf("ownershipRelativePaths() error = %v", err)
	}
	for crashPoint := 1; crashPoint <= 5; crashPoint++ {
		t.Run(string(rune('0'+crashPoint)), func(t *testing.T) {
			filesystem := NewMemoryDurableFS()
			filesystem.CrashAfter(crashPoint)
			writer, err := NewMetadataWriter(filesystem)
			if err != nil {
				t.Fatalf("NewMetadataWriter() error = %v", err)
			}
			err = writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record)
			if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("CreateOwnership() error = %v, want injected crash", err)
			}
			filesystem.Crash()
			got := filesystem.LiveSnapshot()[destination]
			if got != nil {
				if !bytes.Equal(got, expected) {
					t.Fatalf("durable ownership is partial:\n%s", got)
				}
				if _, err := volume.DecodeOwnershipRecord(got); err != nil {
					t.Fatalf("durable ownership validation error = %v", err)
				}
			}
			if err := writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record); err != nil {
				t.Fatalf("CreateOwnership() retry after crash %d error = %v", crashPoint, err)
			}
			snapshot := filesystem.LiveSnapshot()
			if !bytes.Equal(snapshot[destination], expected) {
				t.Fatalf("CreateOwnership() retry after crash %d destination = %s", crashPoint, snapshot[destination])
			}
			temporary := destination + "." + safetyCreateID + ".tmp"
			if _, present := snapshot[temporary]; present {
				t.Fatalf("CreateOwnership() retry after crash %d left temporary %q", crashPoint, temporary)
			}
		})
	}
}

func TestOwnershipCreationReadsBackAndNeverOverwritesExistingRecord(t *testing.T) {
	record := safetyOwnership(t)
	destination, _, err := ownershipRelativePaths(record.BasePath, record.LogicalVolumeID)
	if err != nil {
		t.Fatalf("ownershipRelativePaths() error = %v", err)
	}

	t.Run("success", func(t *testing.T) {
		filesystem := NewMemoryDurableFS()
		writer, err := NewMetadataWriter(filesystem)
		if err != nil {
			t.Fatalf("NewMetadataWriter() error = %v", err)
		}
		if err := writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record); err != nil {
			t.Fatalf("CreateOwnership() error = %v", err)
		}
		encoded := filesystem.LiveSnapshot()[destination]
		decoded, err := volume.DecodeOwnershipRecord(encoded)
		if err != nil {
			t.Fatalf("DecodeOwnershipRecord() error = %v", err)
		}
		if decoded.LogicalID() != record.LogicalVolumeID || decoded.LifecycleState() != volume.StateReady {
			t.Fatalf("decoded ownership = %#v", decoded)
		}
	})

	t.Run("existing destination", func(t *testing.T) {
		filesystem := NewMemoryDurableFS()
		const existing = `{"foreign":true}`
		if err := filesystem.SeedDurable(destination, []byte(existing)); err != nil {
			t.Fatalf("SeedDurable() error = %v", err)
		}
		writer, err := NewMetadataWriter(filesystem)
		if err != nil {
			t.Fatalf("NewMetadataWriter() error = %v", err)
		}
		err = writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record)
		if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("CreateOwnership() error = %v, want ErrAlreadyExists", err)
		}
		if got := string(filesystem.LiveSnapshot()[destination]); got != existing {
			t.Fatalf("existing ownership changed to %s", got)
		}
	})

	t.Run("mismatched operation temporary", func(t *testing.T) {
		filesystem := NewMemoryDurableFS()
		temporary := destination + "." + safetyCreateID + ".tmp"
		if err := filesystem.SeedDurable(temporary, []byte(`{"foreign":true}`)); err != nil {
			t.Fatalf("SeedDurable() error = %v", err)
		}
		writer, err := NewMetadataWriter(filesystem)
		if err != nil {
			t.Fatalf("NewMetadataWriter() error = %v", err)
		}
		err = writer.CreateOwnership(context.Background(), record.BasePath, safetyCreateID, &record)
		if !errors.Is(err, ErrExpectedGenerationMismatch) {
			t.Fatalf("CreateOwnership() error = %v, want ErrExpectedGenerationMismatch", err)
		}
		if _, present := filesystem.LiveSnapshot()[destination]; present {
			t.Fatal("mismatched temporary installed an ownership destination")
		}
	})
}

func TestBootstrapTemporaryRemovalRequiresRootDirectoryBarrier(t *testing.T) {
	temporary := ".sfs-subdir-csi-owner." + safetyAttemptID + ".tmp"
	for crashPoint, wantPresent := range map[int]bool{1: true, 2: false} {
		filesystem := NewMemoryDurableFS()
		if err := filesystem.SeedDurable(temporary, []byte(`{"attempt":true}`)); err != nil {
			t.Fatalf("SeedDurable() error = %v", err)
		}
		filesystem.CrashAfter(crashPoint)
		writer, err := NewMetadataWriter(filesystem)
		if err != nil {
			t.Fatalf("NewMetadataWriter() error = %v", err)
		}
		err = writer.RemoveBootstrapTemporary(context.Background(), safetyAttemptID)
		if !errors.Is(err, ErrInjectedCrash) {
			t.Fatalf("RemoveBootstrapTemporary(crash %d) error = %v", crashPoint, err)
		}
		filesystem.Crash()
		_, present := filesystem.LiveSnapshot()[temporary]
		if present != wantPresent {
			t.Fatalf("temporary presence after crash %d = %t, want %t", crashPoint, present, wantPresent)
		}
		if err := writer.RemoveBootstrapTemporary(context.Background(), safetyAttemptID); err != nil {
			t.Fatalf("RemoveBootstrapTemporary() retry after crash %d error = %v", crashPoint, err)
		}
		filesystem.Crash()
		if _, present := filesystem.LiveSnapshot()[temporary]; present {
			t.Fatalf("bootstrap temporary survived retry after crash %d", crashPoint)
		}
	}

	filesystem := NewMemoryDurableFS()
	if err := filesystem.SeedDurable(temporary, []byte(`{"attempt":true}`)); err != nil {
		t.Fatalf("SeedDurable() error = %v", err)
	}
	writer, err := NewMetadataWriter(filesystem)
	if err != nil {
		t.Fatalf("NewMetadataWriter() error = %v", err)
	}
	if err := writer.RemoveBootstrapTemporary(context.Background(), safetyAttemptID); err != nil {
		t.Fatalf("RemoveBootstrapTemporary() error = %v", err)
	}
	filesystem.Crash()
	if _, present := filesystem.LiveSnapshot()[temporary]; present {
		t.Fatal("bootstrap temporary survived successful durable removal")
	}
}

func TestOwnershipUpdateCrashKeepsOldOrNewCompleteGeneration(t *testing.T) {
	current := safetyOwnership(t)
	next := current
	next.Revision++
	next.PublishedNodeIDs = []string{"fr-par-1/55555555-5555-4555-8555-555555555555"}
	var err error
	next, err = next.Seal()
	if err != nil {
		t.Fatalf("next.Seal() error = %v", err)
	}
	currentBytes, err := volume.EncodeOwnershipRecord(&current)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord(current) error = %v", err)
	}
	nextBytes, err := volume.EncodeOwnershipRecord(&next)
	if err != nil {
		t.Fatalf("EncodeOwnershipRecord(next) error = %v", err)
	}
	destination, _, err := ownershipRelativePaths(current.BasePath, current.LogicalVolumeID)
	if err != nil {
		t.Fatalf("ownershipRelativePaths() error = %v", err)
	}
	for crashPoint := 1; crashPoint <= 4; crashPoint++ {
		filesystem := NewMemoryDurableFS()
		if err := filesystem.SeedDurable(destination, currentBytes); err != nil {
			t.Fatalf("SeedDurable() error = %v", err)
		}
		filesystem.CrashAfter(crashPoint)
		writer, err := NewMetadataWriter(filesystem)
		if err != nil {
			t.Fatalf("NewMetadataWriter() error = %v", err)
		}
		err = writer.UpdateOwnership(context.Background(), current.BasePath, "66666666-6666-4666-8666-666666666666", &current, &next)
		if !errors.Is(err, ErrInjectedCrash) {
			t.Fatalf("UpdateOwnership(crash %d) error = %v", crashPoint, err)
		}
		filesystem.Crash()
		got := filesystem.LiveSnapshot()[destination]
		if !bytes.Equal(got, currentBytes) && !bytes.Equal(got, nextBytes) {
			t.Fatalf("crash %d exposed neither complete generation: %s", crashPoint, got)
		}
		if _, err := volume.DecodeOwnershipRecord(got); err != nil {
			t.Fatalf("crash %d durable ownership validation error = %v", crashPoint, err)
		}
		if err := writer.UpdateOwnership(context.Background(), current.BasePath, "66666666-6666-4666-8666-666666666666", &current, &next); err != nil {
			t.Fatalf("UpdateOwnership() retry after crash %d error = %v", crashPoint, err)
		}
		snapshot := filesystem.LiveSnapshot()
		if !bytes.Equal(snapshot[destination], nextBytes) {
			t.Fatalf("UpdateOwnership() retry after crash %d destination = %s", crashPoint, snapshot[destination])
		}
		temporary := destination + ".66666666-6666-4666-8666-666666666666.tmp"
		if _, present := snapshot[temporary]; present {
			t.Fatalf("UpdateOwnership() retry after crash %d left temporary %q", crashPoint, temporary)
		}
	}
}

func TestOwnershipUpdateRejectsUnexpectedCurrentGeneration(t *testing.T) {
	current := safetyOwnership(t)
	next := current
	next.Revision++
	var err error
	next, err = next.Seal()
	if err != nil {
		t.Fatalf("next.Seal() error = %v", err)
	}
	destination, _, err := ownershipRelativePaths(current.BasePath, current.LogicalVolumeID)
	if err != nil {
		t.Fatalf("ownershipRelativePaths() error = %v", err)
	}
	filesystem := NewMemoryDurableFS()
	if err := filesystem.SeedDurable(destination, []byte(`{"different":true}`)); err != nil {
		t.Fatalf("SeedDurable() error = %v", err)
	}
	writer, err := NewMetadataWriter(filesystem)
	if err != nil {
		t.Fatalf("NewMetadataWriter() error = %v", err)
	}
	err = writer.UpdateOwnership(context.Background(), current.BasePath, "77777777-7777-4777-8777-777777777777", &current, &next)
	if !errors.Is(err, ErrExpectedGenerationMismatch) {
		t.Fatalf("UpdateOwnership() error = %v, want ErrExpectedGenerationMismatch", err)
	}
}
