package recovery

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const checkpointArchiveFormatV1 = "checkpoint-tar-v1"

// ValidateCheckpointExportReceiptIdentity validates the two scalar fields used
// by the admin streaming receipt without exposing internal digest helpers.
func ValidateCheckpointExportReceiptIdentity(requestID, digest string) error {
	if err := volume.ValidateOperationID(requestID); err != nil {
		return fmt.Errorf("checkpoint export request ID: %w", err)
	}
	if !validSHA256Digest(digest) {
		return fmt.Errorf("checkpoint export SHA-256 is malformed")
	}
	return nil
}

type checkpointArchiveFormat struct {
	SchemaVersion       string `json:"schemaVersion"`
	Format              string `json:"format"`
	CheckpointRequestID string `json:"checkpointRequestID"`
	ManifestSHA256      string `json:"manifestSHA256"`
}

type checkpointArchiveObjectMetadata struct {
	APIVersion            string `json:"apiVersion"`
	Kind                  string `json:"kind"`
	Namespace             string `json:"namespace,omitempty"`
	Name                  string `json:"name"`
	SourceUID             string `json:"sourceUID"`
	SourceResourceVersion string `json:"sourceResourceVersion"`
}

// WriteCheckpointArchive emits one deterministic POSIX tar artifact containing
// the exact verified checkpoint package. Every entry is a regular 0600 file;
// paths are fixed or derived only from already validated parent IDs and
// normalized ownership inventory paths. The writer never emits a partial
// archive for a package that fails verification, although an I/O failure can
// still leave the caller's destination partial and must be handled atomically
// by the operator layer.
func WriteCheckpointArchive(ctx context.Context, destination io.Writer, checkpoint CheckpointExportPackage) error {
	if destination == nil {
		return fmt.Errorf("checkpoint archive destination is nil")
	}
	manifest, _, err := VerifyCheckpointExportPackage(ctx, checkpoint)
	if err != nil {
		return fmt.Errorf("verify checkpoint archive package: %w", err)
	}
	formatBytes, err := canonicaljson.Marshal(checkpointArchiveFormat{
		SchemaVersion: volume.SchemaVersionV1, Format: checkpointArchiveFormatV1,
		CheckpointRequestID: manifest.CheckpointRequestID,
		ManifestSHA256:      SHA256Digest(checkpoint.ManifestBytes),
	})
	if err != nil {
		return err
	}

	archive := tar.NewWriter(destination)
	write := func(name string, data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		header := &tar.Header{
			Name: name, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg,
			Format: tar.FormatPAX,
		}
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if _, err := archive.Write(data); err != nil {
			return err
		}
		return nil
	}

	for _, entry := range []struct {
		name string
		data []byte
	}{
		{name: "format.json", data: formatBytes},
		{name: "checkpoint.json", data: checkpoint.ManifestBytes},
		{name: "kubernetes/inventory.json", data: checkpoint.KubernetesObjectInventoryBytes},
	} {
		if err := write(entry.name, entry.data); err != nil {
			_ = archive.Close()
			return fmt.Errorf("write checkpoint archive entry %q: %w", entry.name, err)
		}
	}

	objects := slices.Clone(checkpoint.KubernetesObjects)
	slices.SortFunc(objects, compareExportedKubernetesObjects)
	for index, object := range objects {
		prefix := fmt.Sprintf("kubernetes/objects/%08d", index)
		metadataBytes, err := canonicaljson.Marshal(checkpointArchiveObjectMetadata{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace, Name: object.Name,
			SourceUID: object.SourceUID, SourceResourceVersion: object.SourceResourceVersion,
		})
		if err != nil {
			_ = archive.Close()
			return err
		}
		if err := write(prefix+"/metadata.json", metadataBytes); err != nil {
			_ = archive.Close()
			return fmt.Errorf("write checkpoint archive Kubernetes object %d metadata: %w", index, err)
		}
		if err := write(prefix+"/projection.json", object.RecoverableProjection); err != nil {
			_ = archive.Close()
			return fmt.Errorf("write checkpoint archive Kubernetes object %d projection: %w", index, err)
		}
	}

	parents := slices.Clone(checkpoint.Parents)
	slices.SortFunc(parents, func(left, right ParentOwnershipExport) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	for _, parent := range parents {
		prefix := "parents/" + parent.ParentFilesystemID
		if err := write(prefix+"/owner.json", parent.ParentOwnerBytes); err != nil {
			_ = archive.Close()
			return fmt.Errorf("write checkpoint archive parent %q owner: %w", parent.ParentFilesystemID, err)
		}
		if err := write(prefix+"/inventory.json", parent.InventoryBytes); err != nil {
			_ = archive.Close()
			return fmt.Errorf("write checkpoint archive parent %q inventory: %w", parent.ParentFilesystemID, err)
		}
		recordPaths := make([]string, 0, len(parent.Records))
		for recordPath := range parent.Records {
			recordPaths = append(recordPaths, recordPath)
		}
		slices.Sort(recordPaths)
		for _, recordPath := range recordPaths {
			name := prefix + "/records/" + recordPath
			if err := write(name, parent.Records[recordPath]); err != nil {
				_ = archive.Close()
				return fmt.Errorf("write checkpoint archive parent %q record %q: %w", parent.ParentFilesystemID, recordPath, err)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		_ = archive.Close()
		return err
	}
	if err := archive.Close(); err != nil {
		return fmt.Errorf("finish checkpoint archive: %w", err)
	}
	return nil
}

// CheckpointArchiveSize computes the exact deterministic byte size without
// retaining a second archive copy in memory. The streaming transport uses it
// to detect truncation before accepting its authenticated completion trailer.
func CheckpointArchiveSize(ctx context.Context, checkpoint CheckpointExportPackage) (uint64, error) {
	counter := &checkpointArchiveCounter{}
	if err := WriteCheckpointArchive(ctx, counter, checkpoint); err != nil {
		return 0, err
	}
	return counter.bytes, nil
}

type checkpointArchiveCounter struct {
	bytes uint64
}

func (counter *checkpointArchiveCounter) Write(data []byte) (int, error) {
	if uint64(len(data)) > ^uint64(0)-counter.bytes {
		return 0, fmt.Errorf("checkpoint archive size overflows uint64")
	}
	counter.bytes += uint64(len(data))
	return len(data), nil
}

func compareExportedKubernetesObjects(left, right ExportedKubernetesObject) int {
	for _, compared := range []int{
		strings.Compare(left.APIVersion, right.APIVersion),
		strings.Compare(left.Kind, right.Kind),
		strings.Compare(left.Namespace, right.Namespace),
		strings.Compare(left.Name, right.Name),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}

// CheckpointTicketForExport derives the prepare-ticket commitments from the
// exact inventories already present in a verified export package.
func CheckpointTicketForExport(ctx context.Context, checkpoint CheckpointExportPackage) (CheckpointTicket, error) {
	manifest, _, err := VerifyCheckpointExportPackage(ctx, checkpoint)
	if err != nil {
		return CheckpointTicket{}, err
	}
	parentInventories := make(map[string][]byte, len(checkpoint.Parents))
	for _, parent := range checkpoint.Parents {
		parentInventories[parent.ParentFilesystemID] = bytes.Clone(parent.InventoryBytes)
	}
	candidate := CheckpointCandidate{
		Manifest: manifest, KubernetesObjectInventoryBytes: bytes.Clone(checkpoint.KubernetesObjectInventoryBytes),
		ParentInventoryBytes: parentInventories,
	}
	return BuildCheckpointTicket(candidate)
}
