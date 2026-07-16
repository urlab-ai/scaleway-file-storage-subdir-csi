package recovery

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestWriteCheckpointArchiveIsDeterministicCompleteAndSafe(t *testing.T) {
	checkpoint := validCheckpointExportPackage(t)
	var first bytes.Buffer
	if err := WriteCheckpointArchive(context.Background(), &first, checkpoint); err != nil {
		t.Fatalf("WriteCheckpointArchive() error = %v", err)
	}
	var second bytes.Buffer
	if err := WriteCheckpointArchive(context.Background(), &second, checkpoint); err != nil {
		t.Fatalf("WriteCheckpointArchive(second) error = %v", err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("checkpoint archive is not deterministic")
	}
	size, err := CheckpointArchiveSize(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("CheckpointArchiveSize() error = %v", err)
	}
	if size != uint64(first.Len()) {
		t.Fatalf("CheckpointArchiveSize() = %d, want %d", size, first.Len())
	}

	want := map[string]bool{
		"format.json": false, "checkpoint.json": false, "kubernetes/inventory.json": false,
		"kubernetes/objects/00000000/metadata.json":        false,
		"kubernetes/objects/00000000/projection.json":      false,
		"parents/" + eligibilityParent + "/owner.json":     false,
		"parents/" + eligibilityParent + "/inventory.json": false,
	}
	reader := tar.NewReader(bytes.NewReader(first.Bytes()))
	for {
		header, nextErr := reader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			t.Fatalf("tar.Next() error = %v", nextErr)
		}
		if header.Typeflag != tar.TypeReg || header.Mode != 0o600 || strings.HasPrefix(header.Name, "/") || strings.Contains(header.Name, "../") {
			t.Fatalf("unsafe checkpoint archive header = %#v", header)
		}
		if _, readErr := io.Copy(io.Discard, reader); readErr != nil {
			t.Fatalf("read archive entry %q: %v", header.Name, readErr)
		}
		if _, present := want[header.Name]; present {
			want[header.Name] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("checkpoint archive entry %q is missing", name)
		}
	}
	recordPrefix := "parents/" + eligibilityParent + "/records/kubernetes-volumes/"
	if !bytes.Contains(first.Bytes(), []byte(recordPrefix)) {
		t.Fatalf("checkpoint archive lacks ownership record prefix %q", recordPrefix)
	}

	ticket, err := CheckpointTicketForExport(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("CheckpointTicketForExport() error = %v", err)
	}
	if ticket.CheckpointRequestID == "" || ticket.ManifestSHA256 != SHA256Digest(checkpoint.ManifestBytes) {
		t.Fatalf("export ticket = %#v", ticket)
	}
}

func TestWriteCheckpointArchiveRejectsInvalidPackageBeforeWritingAndCancellation(t *testing.T) {
	checkpoint := validCheckpointExportPackage(t)
	checkpoint.KubernetesObjects[0].SourceUID = "changed"
	var output bytes.Buffer
	if err := WriteCheckpointArchive(context.Background(), &output, checkpoint); err == nil {
		t.Fatal("WriteCheckpointArchive(invalid) error = nil")
	}
	if output.Len() != 0 {
		t.Fatalf("invalid checkpoint wrote %d bytes", output.Len())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WriteCheckpointArchive(ctx, &output, validCheckpointExportPackage(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteCheckpointArchive(cancelled) error = %v", err)
	}
}

func TestReadCheckpointArchiveRequiresExactDeterministicArtifact(t *testing.T) {
	checkpoint := validCheckpointExportPackage(t)
	var archive bytes.Buffer
	if err := WriteCheckpointArchive(context.Background(), &archive, checkpoint); err != nil {
		t.Fatalf("WriteCheckpointArchive() error = %v", err)
	}
	decoded, err := ReadCheckpointArchive(context.Background(), bytes.NewReader(archive.Bytes()))
	if err != nil {
		t.Fatalf("ReadCheckpointArchive() error = %v", err)
	}
	wantManifest, err := DecodeCheckpointManifest(checkpoint.ManifestBytes)
	if err != nil {
		t.Fatalf("DecodeCheckpointManifest() error = %v", err)
	}
	if decoded.Manifest.CheckpointRequestID != wantManifest.CheckpointRequestID || decoded.ArchiveBytes != uint64(archive.Len()) || decoded.ManifestSHA256 != SHA256Digest(checkpoint.ManifestBytes) || decoded.ArchiveSHA256 == "" {
		t.Fatalf("decoded checkpoint archive = %#v", decoded)
	}

	appended := append(bytes.Clone(archive.Bytes()), 0)
	if _, err := ReadCheckpointArchive(context.Background(), bytes.NewReader(appended)); err == nil {
		t.Fatal("ReadCheckpointArchive(appended byte) error = nil")
	}
	truncated := archive.Bytes()[:archive.Len()-1]
	if _, err := ReadCheckpointArchive(context.Background(), bytes.NewReader(truncated)); err == nil {
		t.Fatal("ReadCheckpointArchive(truncated) error = nil")
	}
}

func TestReadCheckpointArchiveRejectsUnsafeTarEntryBeforePackageUse(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "../checkpoint.json", Typeflag: tar.TypeReg, Mode: 0o600, Size: 2, Format: tar.FormatPAX}); err != nil {
		t.Fatalf("WriteHeader() error = %v", err)
	}
	if _, err := writer.Write([]byte("{}")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := ReadCheckpointArchive(context.Background(), bytes.NewReader(archive.Bytes())); err == nil {
		t.Fatal("ReadCheckpointArchive(path traversal) error = nil")
	}
}
