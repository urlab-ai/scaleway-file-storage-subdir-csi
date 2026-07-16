package recovery

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	// MaxCheckpointArchiveBytes is the common finite bound enforced by export
	// streaming and restore parsing.
	MaxCheckpointArchiveBytes = uint64(1 << 30)
	maxCheckpointArchiveFiles = 100_000
	maxCheckpointArchivePath  = 4 * 1024
)

var checkpointObjectPathPattern = regexp.MustCompile(`^kubernetes/objects/([0-9]{8})/(metadata|projection)\.json$`)

// DecodedCheckpointArchive is a byte-for-byte deterministic, fully verified
// checkpoint package ready for restore planning. Digest and size cover the
// complete tar artifact, including deterministic headers and end blocks.
type DecodedCheckpointArchive struct {
	Package        CheckpointExportPackage
	Manifest       CheckpointManifest
	ManifestSHA256 string
	ArchiveSHA256  string
	ArchiveBytes   uint64
}

// ReadCheckpointArchive accepts only the exact deterministic POSIX tar emitted
// by WriteCheckpointArchive. It bounds bytes and entries before allocation,
// rejects links and non-regular files, verifies every package commitment, then
// regenerates the archive and compares its complete digest. That final check
// rejects reordered entries, altered headers, padding, and appended bytes that
// a permissive tar reader might otherwise ignore.
func ReadCheckpointArchive(ctx context.Context, source io.Reader) (DecodedCheckpointArchive, error) {
	if ctx == nil {
		return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive context is nil")
	}
	if source == nil {
		return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive source is nil")
	}
	if err := ctx.Err(); err != nil {
		return DecodedCheckpointArchive{}, err
	}
	limited := &io.LimitedReader{R: source, N: int64(MaxCheckpointArchiveBytes) + 1}
	inputDigest := sha256.New()
	counter := &archiveReadCounter{writer: inputDigest}
	tee := io.TeeReader(limited, counter)
	reader := tar.NewReader(tee)

	files := make(map[string][]byte)
	for entries := 0; ; entries++ {
		if err := ctx.Err(); err != nil {
			return DecodedCheckpointArchive{}, err
		}
		if entries >= maxCheckpointArchiveFiles {
			return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive exceeds %d files", maxCheckpointArchiveFiles)
		}
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return DecodedCheckpointArchive{}, fmt.Errorf("read checkpoint archive header: %w", err)
		}
		if err := validateCheckpointArchiveHeader(header); err != nil {
			return DecodedCheckpointArchive{}, err
		}
		if _, duplicate := files[header.Name]; duplicate {
			return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive path %q is duplicated", header.Name)
		}
		data, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil {
			return DecodedCheckpointArchive{}, fmt.Errorf("read checkpoint archive path %q: %w", header.Name, err)
		}
		if int64(len(data)) != header.Size {
			return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive path %q size differs from header", header.Name)
		}
		files[header.Name] = data
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return DecodedCheckpointArchive{}, fmt.Errorf("read checkpoint archive trailer: %w", err)
	}
	if counter.bytes == 0 || counter.bytes > MaxCheckpointArchiveBytes || limited.N == 0 {
		return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive must contain 1 to %d bytes", MaxCheckpointArchiveBytes)
	}

	checkpoint, format, err := checkpointPackageFromArchiveFiles(files)
	if err != nil {
		return DecodedCheckpointArchive{}, err
	}
	manifest, manifestDigest, err := VerifyCheckpointExportPackage(ctx, checkpoint)
	if err != nil {
		return DecodedCheckpointArchive{}, fmt.Errorf("verify restored checkpoint package: %w", err)
	}
	if format.CheckpointRequestID != manifest.CheckpointRequestID || format.ManifestSHA256 != manifestDigest {
		return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive format commitment differs from manifest")
	}

	regeneratedDigest := sha256.New()
	regeneratedCounter := &archiveWriteDigest{digest: regeneratedDigest}
	if err := WriteCheckpointArchive(ctx, regeneratedCounter, checkpoint); err != nil {
		return DecodedCheckpointArchive{}, fmt.Errorf("regenerate checkpoint archive: %w", err)
	}
	inputSum := inputDigest.Sum(nil)
	if regeneratedCounter.bytes != counter.bytes || !bytes.Equal(regeneratedDigest.Sum(nil), inputSum) {
		return DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive is valid tar but not the canonical deterministic artifact")
	}
	return DecodedCheckpointArchive{
		Package: checkpoint, Manifest: manifest, ManifestSHA256: manifestDigest,
		ArchiveSHA256: "sha256:" + hex.EncodeToString(inputSum), ArchiveBytes: counter.bytes,
	}, nil
}

type archiveReadCounter struct {
	writer hash.Hash
	bytes  uint64
}

func (counter *archiveReadCounter) Write(data []byte) (int, error) {
	if uint64(len(data)) > ^uint64(0)-counter.bytes {
		return 0, fmt.Errorf("checkpoint archive size overflows uint64")
	}
	counter.bytes += uint64(len(data))
	return counter.writer.Write(data)
}

type archiveWriteDigest struct {
	digest hash.Hash
	bytes  uint64
}

func (writer *archiveWriteDigest) Write(data []byte) (int, error) {
	if uint64(len(data)) > ^uint64(0)-writer.bytes {
		return 0, fmt.Errorf("checkpoint archive size overflows uint64")
	}
	writer.bytes += uint64(len(data))
	return writer.digest.Write(data)
}

func validateCheckpointArchiveHeader(header *tar.Header) error {
	if header == nil {
		return fmt.Errorf("checkpoint archive header is nil")
	}
	// TypeRegA is the historic NUL representation of a regular tar entry and
	// remains valid input even though new archives always emit TypeReg.
	//nolint:staticcheck // Rejecting this legacy regular-file encoding would break compatible archives.
	if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
		return fmt.Errorf("checkpoint archive path %q is not a regular file", header.Name)
	}
	if header.Name == "" || len(header.Name) > maxCheckpointArchivePath || path.IsAbs(header.Name) || path.Clean(header.Name) != header.Name || strings.HasPrefix(header.Name, "../") || strings.ContainsAny(header.Name, "\x00\r\n") {
		return fmt.Errorf("checkpoint archive path %q is unsafe or non-canonical", header.Name)
	}
	if header.Mode != 0o600 || header.Size <= 0 || header.Size > int64(maxExternalInventoryBytes) {
		return fmt.Errorf("checkpoint archive path %q has invalid mode or bounded size", header.Name)
	}
	// Xattrs is deprecated in favor of PAXRecords, but old readers may still
	// populate it; checking both representations is a fail-closed safety rule.
	//nolint:staticcheck // The deprecated field is intentionally inspected, never produced.
	xattrs := header.Xattrs
	if header.Linkname != "" || header.PAXRecords != nil || xattrs != nil || header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "" || (!header.ModTime.IsZero() && header.ModTime.Unix() != 0) || !header.AccessTime.IsZero() || !header.ChangeTime.IsZero() || header.Devmajor != 0 || header.Devminor != 0 {
		return fmt.Errorf("checkpoint archive path %q has non-deterministic or link metadata (link=%q uid=%d gid=%d uname=%q gname=%q mod=%s access=%s change=%s dev=%d/%d pax=%d xattrs=%d)", header.Name, header.Linkname, header.Uid, header.Gid, header.Uname, header.Gname, header.ModTime, header.AccessTime, header.ChangeTime, header.Devmajor, header.Devminor, len(header.PAXRecords), len(xattrs))
	}
	return nil
}

type archiveObjectFiles struct {
	metadata   []byte
	projection []byte
}

type archiveParentFiles struct {
	owner     []byte
	inventory []byte
	records   map[string][]byte
}

func checkpointPackageFromArchiveFiles(files map[string][]byte) (CheckpointExportPackage, checkpointArchiveFormat, error) {
	formatBytes, present := files["format.json"]
	if !present {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive is missing format.json")
	}
	manifestBytes, present := files["checkpoint.json"]
	if !present {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive is missing checkpoint.json")
	}
	inventoryBytes, present := files["kubernetes/inventory.json"]
	if !present {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive is missing Kubernetes inventory")
	}
	var format checkpointArchiveFormat
	if err := strictjson.Decode(formatBytes, &format); err != nil {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("decode checkpoint archive format: %w", err)
	}
	canonicalFormat, err := canonicaljson.Marshal(format)
	if err != nil || !bytes.Equal(canonicalFormat, formatBytes) {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive format is not canonical")
	}
	if format.SchemaVersion != volume.SchemaVersionV1 || format.Format != checkpointArchiveFormatV1 {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive format or schema is unsupported")
	}
	if err := volume.ValidateOperationID(format.CheckpointRequestID); err != nil || !validSHA256Digest(format.ManifestSHA256) {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive format identity is invalid")
	}

	objects := make(map[int]*archiveObjectFiles)
	parents := make(map[string]*archiveParentFiles)
	for name, data := range files {
		if name == "format.json" || name == "checkpoint.json" || name == "kubernetes/inventory.json" {
			continue
		}
		if matched := checkpointObjectPathPattern.FindStringSubmatch(name); matched != nil {
			index, err := strconv.Atoi(matched[1])
			if err != nil {
				return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint object index %q is invalid", matched[1])
			}
			entry := objects[index]
			if entry == nil {
				entry = &archiveObjectFiles{}
				objects[index] = entry
			}
			if matched[2] == "metadata" {
				entry.metadata = data
			} else {
				entry.projection = data
			}
			continue
		}
		if strings.HasPrefix(name, "parents/") {
			parts := strings.SplitN(strings.TrimPrefix(name, "parents/"), "/", 2)
			if len(parts) != 2 || volume.ValidateParentFilesystemID(parts[0]) != nil {
				return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint parent path %q is invalid", name)
			}
			entry := parents[parts[0]]
			if entry == nil {
				entry = &archiveParentFiles{records: make(map[string][]byte)}
				parents[parts[0]] = entry
			}
			switch {
			case parts[1] == "owner.json":
				entry.owner = data
			case parts[1] == "inventory.json":
				entry.inventory = data
			case strings.HasPrefix(parts[1], "records/"):
				recordPath := strings.TrimPrefix(parts[1], "records/")
				if recordPath == "" || path.IsAbs(recordPath) || path.Clean(recordPath) != recordPath || strings.HasPrefix(recordPath, "../") {
					return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint ownership record path %q is invalid", name)
				}
				entry.records[recordPath] = data
			default:
				return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive contains unknown parent path %q", name)
			}
			continue
		}
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive contains unknown path %q", name)
	}

	exportedObjects := make([]ExportedKubernetesObject, 0, len(objects))
	for index := 0; index < len(objects); index++ {
		entry, present := objects[index]
		if !present || len(entry.metadata) == 0 || len(entry.projection) == 0 {
			return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive Kubernetes object index %08d is missing or incomplete", index)
		}
		var metadata checkpointArchiveObjectMetadata
		if err := strictjson.Decode(entry.metadata, &metadata); err != nil {
			return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("decode checkpoint object %08d metadata: %w", index, err)
		}
		canonicalMetadata, err := canonicaljson.Marshal(metadata)
		if err != nil || !bytes.Equal(canonicalMetadata, entry.metadata) {
			return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint object %08d metadata is not canonical", index)
		}
		exportedObjects = append(exportedObjects, ExportedKubernetesObject{
			APIVersion: metadata.APIVersion, Kind: metadata.Kind, Namespace: metadata.Namespace,
			Name: metadata.Name, SourceUID: metadata.SourceUID, SourceResourceVersion: metadata.SourceResourceVersion,
			RecoverableProjection: slices.Clone(entry.projection),
		})
	}
	if len(objects) != len(exportedObjects) {
		return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive Kubernetes object indices are not contiguous")
	}

	parentExports := make([]ParentOwnershipExport, 0, len(parents))
	for parentID, entry := range parents {
		if len(entry.owner) == 0 || len(entry.inventory) == 0 {
			return CheckpointExportPackage{}, checkpointArchiveFormat{}, fmt.Errorf("checkpoint archive parent %q is incomplete", parentID)
		}
		parentExports = append(parentExports, ParentOwnershipExport{
			ParentFilesystemID: parentID, ParentOwnerBytes: slices.Clone(entry.owner),
			InventoryBytes: slices.Clone(entry.inventory), Records: cloneArchiveRecords(entry.records),
		})
	}
	slices.SortFunc(parentExports, func(left, right ParentOwnershipExport) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	return CheckpointExportPackage{
		ManifestBytes: slices.Clone(manifestBytes), KubernetesObjectInventoryBytes: slices.Clone(inventoryBytes),
		KubernetesObjects: exportedObjects, Parents: parentExports,
	}, format, nil
}

func cloneArchiveRecords(source map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(source))
	for name, data := range source {
		result[name] = slices.Clone(data)
	}
	return result
}
