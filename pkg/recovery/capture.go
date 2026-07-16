package recovery

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// CheckpointCandidate is the complete controller-generated metadata candidate
// emitted while mutation admission remains closed. Parent record contents and
// Kubernetes objects are exported separately, but these detailed inventories
// commit their exact paths, source generations, and content hashes.
type CheckpointCandidate struct {
	Manifest                       CheckpointManifest
	KubernetesObjectInventoryBytes []byte
	ParentInventoryBytes           map[string][]byte
}

// Validate proves that every detailed inventory is canonical and agrees with
// the bounded manifest. It does not verify exported object or ownership bytes;
// VerifyCheckpointExportPackage owns that later boundary.
func (candidate CheckpointCandidate) Validate() error {
	if _, err := EncodeCheckpointManifest(candidate.Manifest); err != nil {
		return err
	}
	objects, err := DecodeKubernetesObjectInventory(candidate.KubernetesObjectInventoryBytes)
	if err != nil {
		return fmt.Errorf("checkpoint candidate Kubernetes inventory: %w", err)
	}
	objectSummary, objectBytes, err := BuildKubernetesObjectInventory(objects)
	if err != nil {
		return err
	}
	if objectSummary != candidate.Manifest.KubernetesObjects || !bytes.Equal(objectBytes, candidate.KubernetesObjectInventoryBytes) {
		return fmt.Errorf("checkpoint candidate Kubernetes inventory differs from manifest")
	}
	if len(candidate.ParentInventoryBytes) != len(candidate.Manifest.Parents) {
		return fmt.Errorf("checkpoint candidate has %d parent inventories, manifest requires %d", len(candidate.ParentInventoryBytes), len(candidate.Manifest.Parents))
	}
	seen := make(map[string]struct{}, len(candidate.Manifest.Parents))
	for _, parent := range candidate.Manifest.Parents {
		inventoryBytes, present := candidate.ParentInventoryBytes[parent.ParentFilesystemID]
		if !present {
			return fmt.Errorf("checkpoint candidate is missing parent inventory %q", parent.ParentFilesystemID)
		}
		entries, err := DecodeOwnershipInventory(inventoryBytes)
		if err != nil {
			return fmt.Errorf("checkpoint candidate parent %q inventory: %w", parent.ParentFilesystemID, err)
		}
		if uint64(len(entries)) != parent.RecordCount || SHA256Digest(inventoryBytes) != parent.AggregateSHA256 {
			return fmt.Errorf("checkpoint candidate parent %q inventory differs from manifest", parent.ParentFilesystemID)
		}
		seen[parent.ParentFilesystemID] = struct{}{}
	}
	for parentID := range candidate.ParentInventoryBytes {
		if _, present := seen[parentID]; !present {
			return fmt.Errorf("checkpoint candidate contains extra parent inventory %q", parentID)
		}
	}
	return nil
}

// Clone returns a deep copy safe for an admin transport or idempotent retry.
func (candidate CheckpointCandidate) Clone() CheckpointCandidate {
	clone := CheckpointCandidate{
		Manifest:                       candidate.Manifest,
		KubernetesObjectInventoryBytes: bytes.Clone(candidate.KubernetesObjectInventoryBytes),
		ParentInventoryBytes:           make(map[string][]byte, len(candidate.ParentInventoryBytes)),
	}
	clone.Manifest.Images = slices.Clone(candidate.Manifest.Images)
	clone.Manifest.Parents = slices.Clone(candidate.Manifest.Parents)
	for parentID, inventory := range candidate.ParentInventoryBytes {
		clone.ParentInventoryBytes[parentID] = bytes.Clone(inventory)
	}
	return clone
}

// CheckpointCaptureSnapshot is one coherent typed read obtained after the
// process-wide mutation gate drained. Kubernetes object entries must be built
// from schema-defined recoverable projections and their exact source
// UID/resourceVersion generation.
type CheckpointCaptureSnapshot struct {
	Records                  CheckpointRecordSet
	KubernetesObjects        []KubernetesObjectInventoryEntry
	ChartVersion             string
	Images                   []ImageDigest
	LeadershipLeaseUID       string
	LeadershipHolderIdentity string
	HolderEvidence           coordination.HolderEvidence
}

// CheckpointSnapshotReader reads all Kubernetes and mounted-parent state in one
// uninterrupted quiesced interval. It must never repair or mutate that state.
type CheckpointSnapshotReader interface {
	ReadCheckpointSnapshot(ctx context.Context) (CheckpointCaptureSnapshot, error)
}

// SnapshotCheckpointCapture validates one coherent snapshot and derives the
// canonical candidate consumed by CheckpointCoordinator.
type SnapshotCheckpointCapture struct {
	reader CheckpointSnapshotReader
	clock  clock.Clock
}

// NewSnapshotCheckpointCapture validates the read-only capture dependencies.
func NewSnapshotCheckpointCapture(reader CheckpointSnapshotReader, operationClock clock.Clock) (*SnapshotCheckpointCapture, error) {
	if reader == nil || operationClock == nil {
		return nil, fmt.Errorf("checkpoint snapshot capture dependency is nil")
	}
	return &SnapshotCheckpointCapture{reader: reader, clock: operationClock}, nil
}

// CaptureCheckpoint reads, validates, and hashes the complete metadata set. It
// returns no partial candidate after cancellation or any one-sided record.
func (capture *SnapshotCheckpointCapture) CaptureCheckpoint(ctx context.Context, requestID string) (CheckpointCandidate, error) {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return CheckpointCandidate{}, err
	}
	if err := ctx.Err(); err != nil {
		return CheckpointCandidate{}, err
	}
	snapshot, err := capture.reader.ReadCheckpointSnapshot(ctx)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	if err := ctx.Err(); err != nil {
		return CheckpointCandidate{}, err
	}
	if err := ValidateCheckpointRecordSet(snapshot.Records); err != nil {
		return CheckpointCandidate{}, err
	}
	leaseHolder, present, err := coordination.ParseHolderEvidence(snapshot.Records.LeaseAnnotations)
	if err != nil {
		return CheckpointCandidate{}, fmt.Errorf("checkpoint Lease holder annotations: %w", err)
	}
	if !present || leaseHolder != snapshot.HolderEvidence || snapshot.LeadershipHolderIdentity != snapshot.HolderEvidence.PodUID {
		return CheckpointCandidate{}, fmt.Errorf("checkpoint holder evidence differs from the quiesced Lease generation")
	}
	objectSummary, objectInventoryBytes, err := BuildKubernetesObjectInventory(snapshot.KubernetesObjects)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	parents, parentInventories, err := buildCapturedParentInventories(ctx, snapshot.Records.Parents)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	manifest, err := NewCheckpointManifest(
		requestID, snapshot.Records.DriverName, snapshot.Records.InstallationID,
		snapshot.Records.ActiveClusterUID, snapshot.ChartVersion, snapshot.LeadershipLeaseUID,
		snapshot.HolderEvidence, capture.clock.Now(), snapshot.Images, objectSummary, parents,
	)
	if err != nil {
		return CheckpointCandidate{}, err
	}
	candidate := CheckpointCandidate{
		Manifest: manifest, KubernetesObjectInventoryBytes: objectInventoryBytes,
		ParentInventoryBytes: parentInventories,
	}
	if err := candidate.Validate(); err != nil {
		return CheckpointCandidate{}, err
	}
	return candidate, nil
}

func buildCapturedParentInventories(ctx context.Context, parents []CheckpointParentRecordSet) ([]ParentInventory, map[string][]byte, error) {
	summaries := make([]ParentInventory, 0, len(parents))
	inventories := make(map[string][]byte, len(parents))
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		ownerBytes, err := volume.EncodeParentOwnerRecord(parent.ParentOwner)
		if err != nil {
			return nil, nil, fmt.Errorf("encode checkpoint parent %q owner: %w", parent.ParentFilesystemID, err)
		}
		entries := make([]OwnershipInventoryEntry, 0, len(parent.Ownerships))
		for _, ownership := range parent.Ownerships {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			entry, err := capturedOwnershipEntry(parent.ParentOwner, ownership)
			if err != nil {
				return nil, nil, fmt.Errorf("capture parent %q ownership: %w", parent.ParentFilesystemID, err)
			}
			entries = append(entries, entry)
		}
		summary, inventoryBytes, err := BuildParentInventory(parent.ParentFilesystemID, ownerBytes, entries)
		if err != nil {
			return nil, nil, err
		}
		if _, duplicate := inventories[parent.ParentFilesystemID]; duplicate {
			return nil, nil, fmt.Errorf("checkpoint parent %q is duplicated", parent.ParentFilesystemID)
		}
		summaries = append(summaries, summary)
		inventories[parent.ParentFilesystemID] = inventoryBytes
	}
	slices.SortFunc(summaries, func(left, right ParentInventory) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	return summaries, maps.Clone(inventories), nil
}

// BuildParentInventorySummaries derives the exact restore-stable manifest
// summaries from decoded mounted-parent claims and ownership records. It is the
// read-only startup counterpart of checkpoint capture; detailed inventory bytes
// remain internal because restore compares only the manifest commitments here.
func BuildParentInventorySummaries(ctx context.Context, parents []CheckpointParentRecordSet) ([]ParentInventory, error) {
	summaries, _, err := buildCapturedParentInventories(ctx, parents)
	return summaries, err
}

func capturedOwnershipEntry(parent volume.ParentOwnerRecord, ownership volume.OwnershipRecord) (OwnershipInventoryEntry, error) {
	recordBytes, err := volume.EncodeOwnershipRecord(ownership)
	if err != nil {
		return OwnershipInventoryEntry{}, err
	}
	var revision uint64
	switch record := ownership.(type) {
	case *volume.DetailedOwnershipRecord:
		revision = record.Revision
	case *volume.CompactDeletedOwnershipRecord:
		revision = record.Revision
	default:
		return OwnershipInventoryEntry{}, fmt.Errorf("ownership type %T is unsupported", ownership)
	}
	return OwnershipInventoryEntry{
		Path:         path.Join(strings.TrimPrefix(parent.BasePath, "/"), volume.OwnershipMetadataDirectory, ownership.LogicalID()+".json"),
		RecordSHA256: SHA256Digest(recordBytes), Revision: revision,
		RecordKind: ownership.Kind(), State: ownership.LifecycleState(),
	}, nil
}
