package recovery

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// CheckpointExportInventoryReader obtains one complete fresh inventory while
// the coordinator retains the active quiesce barrier.
type CheckpointExportInventoryReader interface {
	Read(ctx context.Context) (StartupInventorySnapshot, error)
}

// CheckpointReservationJournalReader returns the complete exact all-Idle
// journal set while the checkpoint mutation gate remains closed.
type CheckpointReservationJournalReader interface {
	ReadCheckpointReservationJournals(ctx context.Context) ([]k8s.StoredReservationJournalObject, error)
}

// CheckpointExportBuilder reconstructs the complete package for the exact
// candidate retained by CheckpointCoordinator.
type CheckpointExportBuilder interface {
	BuildCheckpointExport(ctx context.Context, candidate CheckpointCandidate) (CheckpointExportPackage, error)
}

// SnapshotCheckpointExportBuilder couples the pure package constructor to the
// production startup inventory reader. It performs no mutation or repair.
type SnapshotCheckpointExportBuilder struct {
	namespace string
	reader    CheckpointExportInventoryReader
	journals  CheckpointReservationJournalReader
}

// NewSnapshotCheckpointExportBuilder validates the read-only package source.
func NewSnapshotCheckpointExportBuilder(namespace string, reader CheckpointExportInventoryReader, journals CheckpointReservationJournalReader) (*SnapshotCheckpointExportBuilder, error) {
	if !utf8.ValidString(namespace) || len(namespace) == 0 || len(namespace) > 253 || strings.ContainsAny(namespace, "\x00\r\n") {
		return nil, fmt.Errorf("checkpoint export namespace must contain 1 to 253 safe UTF-8 bytes")
	}
	if reader == nil || journals == nil {
		return nil, fmt.Errorf("checkpoint export inventory reader is nil")
	}
	return &SnapshotCheckpointExportBuilder{namespace: namespace, reader: reader, journals: journals}, nil
}

// BuildCheckpointExport reads one complete snapshot and reconstructs the exact
// candidate-bound package without repairing any inconsistency.
func (builder *SnapshotCheckpointExportBuilder) BuildCheckpointExport(ctx context.Context, candidate CheckpointCandidate) (CheckpointExportPackage, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointExportPackage{}, err
	}
	snapshot, err := builder.reader.Read(ctx)
	if err != nil {
		return CheckpointExportPackage{}, fmt.Errorf("read checkpoint export inventory: %w", err)
	}
	journalObjects, err := builder.journals.ReadCheckpointReservationJournals(ctx)
	if err != nil {
		return CheckpointExportPackage{}, fmt.Errorf("read checkpoint reservation journals: %w", err)
	}
	return BuildCheckpointExportPackage(ctx, builder.namespace, candidate, snapshot, journalObjects)
}

// BuildCheckpointExportPackage reconstructs a complete external package from
// one fresh inventory read while the checkpoint mutation barrier is still
// closed. It requires the regenerated detailed inventories to equal the
// prepare candidate byte for byte before returning any package. This closes
// the gap between hashing source generations during prepare and serializing
// their recoverable contents during export.
//
// The caller remains responsible for proving active leadership and retaining
// the checkpoint barrier across the inventory read and this function. The
// returned package contains the potentially large detailed inventories and is
// therefore intended for a dedicated export stream, never the bounded admin
// control socket.
func BuildCheckpointExportPackage(ctx context.Context, namespace string, candidate CheckpointCandidate, snapshot StartupInventorySnapshot, reservationJournals []k8s.StoredReservationJournalObject) (CheckpointExportPackage, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointExportPackage{}, err
	}
	candidate = candidate.Clone()
	if err := candidate.Validate(); err != nil {
		return CheckpointExportPackage{}, fmt.Errorf("validate checkpoint export candidate: %w", err)
	}
	if _, err := BuildStartupInventoryPlan(snapshot); err != nil {
		return CheckpointExportPackage{}, fmt.Errorf("validate checkpoint export inventory: %w", err)
	}
	if snapshot.DriverName != candidate.Manifest.DriverName ||
		snapshot.ActiveClusterUID != candidate.Manifest.ActiveClusterUID ||
		SHA256Digest([]byte(snapshot.InstallationID)) != candidate.Manifest.InstallationIDHash {
		return CheckpointExportPackage{}, fmt.Errorf("checkpoint export inventory identity differs from prepared manifest")
	}

	objectEntries, objects, err := buildCheckpointKubernetesObjectExport(namespace, snapshot.Allocations, reservationJournals, snapshot.PersistentVolumes)
	if err != nil {
		return CheckpointExportPackage{}, err
	}
	objectSummary, objectInventory, err := BuildKubernetesObjectInventory(objectEntries)
	if err != nil {
		return CheckpointExportPackage{}, err
	}
	if objectSummary != candidate.Manifest.KubernetesObjects || !bytes.Equal(objectInventory, candidate.KubernetesObjectInventoryBytes) {
		return CheckpointExportPackage{}, fmt.Errorf("checkpoint export Kubernetes inventory differs from prepared candidate")
	}

	parents, err := buildCheckpointParentExports(ctx, candidate, snapshot.Parents)
	if err != nil {
		return CheckpointExportPackage{}, err
	}
	manifestBytes, err := EncodeCheckpointManifest(candidate.Manifest)
	if err != nil {
		return CheckpointExportPackage{}, err
	}
	checkpoint := CheckpointExportPackage{
		ManifestBytes: manifestBytes, KubernetesObjectInventoryBytes: objectInventory,
		KubernetesObjects: objects, Parents: parents,
	}
	verified, _, err := VerifyCheckpointExportPackage(ctx, checkpoint)
	if err != nil {
		return CheckpointExportPackage{}, fmt.Errorf("verify constructed checkpoint export package: %w", err)
	}
	if verified.CheckpointRequestID != candidate.Manifest.CheckpointRequestID {
		return CheckpointExportPackage{}, fmt.Errorf("constructed checkpoint request differs from prepared candidate")
	}
	return checkpoint, nil
}

func buildCheckpointParentExports(ctx context.Context, candidate CheckpointCandidate, parents []CheckpointParentRecordSet) ([]ParentOwnershipExport, error) {
	exports := make([]ParentOwnershipExport, 0, len(parents))
	seen := make(map[string]struct{}, len(parents))
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, duplicate := seen[parent.ParentFilesystemID]; duplicate {
			return nil, fmt.Errorf("checkpoint export parent %q is duplicated", parent.ParentFilesystemID)
		}
		seen[parent.ParentFilesystemID] = struct{}{}
		ownerBytes, err := volume.EncodeParentOwnerRecord(parent.ParentOwner)
		if err != nil {
			return nil, fmt.Errorf("encode checkpoint export parent %q owner: %w", parent.ParentFilesystemID, err)
		}
		entries := make([]OwnershipInventoryEntry, 0, len(parent.Ownerships))
		records := make(map[string][]byte, len(parent.Ownerships))
		for _, ownership := range parent.Ownerships {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			entry, err := capturedOwnershipEntry(parent.ParentOwner, ownership)
			if err != nil {
				return nil, fmt.Errorf("build checkpoint export parent %q ownership: %w", parent.ParentFilesystemID, err)
			}
			recordBytes, err := volume.EncodeOwnershipRecord(ownership)
			if err != nil {
				return nil, fmt.Errorf("encode checkpoint export parent %q ownership %q: %w", parent.ParentFilesystemID, ownership.LogicalID(), err)
			}
			if _, duplicate := records[entry.Path]; duplicate {
				return nil, fmt.Errorf("checkpoint export parent %q contains duplicate ownership path %q", parent.ParentFilesystemID, entry.Path)
			}
			entries = append(entries, entry)
			records[entry.Path] = recordBytes
		}
		_, inventoryBytes, err := BuildParentInventory(parent.ParentFilesystemID, ownerBytes, entries)
		if err != nil {
			return nil, err
		}
		preparedInventory, present := candidate.ParentInventoryBytes[parent.ParentFilesystemID]
		if !present || !bytes.Equal(inventoryBytes, preparedInventory) {
			return nil, fmt.Errorf("checkpoint export parent %q inventory differs from prepared candidate", parent.ParentFilesystemID)
		}
		exports = append(exports, ParentOwnershipExport{
			ParentFilesystemID: parent.ParentFilesystemID, ParentOwnerBytes: ownerBytes,
			InventoryBytes: inventoryBytes, Records: records,
		})
	}
	if len(seen) != len(candidate.ParentInventoryBytes) {
		return nil, fmt.Errorf("checkpoint export parent inventory set differs from prepared candidate")
	}
	for parentID := range candidate.ParentInventoryBytes {
		if _, present := seen[parentID]; !present {
			return nil, fmt.Errorf("checkpoint export is missing prepared parent %q", parentID)
		}
	}
	slices.SortFunc(exports, func(left, right ParentOwnershipExport) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	return exports, nil
}
