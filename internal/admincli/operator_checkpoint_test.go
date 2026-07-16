package admincli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func validLocalCheckpointTicket(t *testing.T) recovery.CheckpointTicket {
	t.Helper()
	const (
		driverName     = "file-storage-subdir.csi.urlab.ai"
		installationID = "11111111-1111-4111-8111-111111111111"
		clusterUID     = "22222222-2222-4222-8222-222222222222"
		parentID       = "33333333-3333-4333-8333-333333333333"
	)
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	owner, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1,
		DriverName: driverName, InstallationID: installationID, ActiveClusterUID: clusterUID,
		ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes", BasePathHash: basePathHash,
		ControllerNamespace: "driver", HelmReleaseName: "driver",
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  "44444444-4444-4444-8444-444444444444",
		CreatedAt:           "2026-07-13T12:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatalf("ParentOwnerRecord.Seal() error = %v", err)
	}
	ownerBytes, err := volume.EncodeParentOwnerRecord(owner)
	if err != nil {
		t.Fatalf("EncodeParentOwnerRecord() error = %v", err)
	}
	parentSummary, parentInventory, err := recovery.BuildParentInventory(parentID, ownerBytes, nil)
	if err != nil {
		t.Fatalf("BuildParentInventory() error = %v", err)
	}
	objectSummary, objectInventory, err := recovery.BuildKubernetesObjectInventory([]recovery.KubernetesObjectInventoryEntry{})
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	holder, err := coordination.NewHolderEvidence(
		"55555555-5555-4555-8555-555555555555", "worker-a",
		"fr-par-1/66666666-6666-4666-8666-666666666666",
		"66666666-6666-4666-8666-666666666666", "fr-par-1", installationID, clusterUID,
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	manifest, err := recovery.NewCheckpointManifest(
		testRequestID, driverName, installationID, clusterUID, "1.0.0",
		"77777777-7777-4777-8777-777777777777", holder,
		time.Date(2026, 7, 13, 21, 0, 0, 0, time.UTC),
		[]recovery.ImageDigest{{Name: "controller", Digest: "sha256:" + strings.Repeat("a", 64)}},
		objectSummary, []recovery.ParentInventory{parentSummary},
	)
	if err != nil {
		t.Fatalf("NewCheckpointManifest() error = %v", err)
	}
	ticket, err := recovery.BuildCheckpointTicket(recovery.CheckpointCandidate{
		Manifest: manifest, KubernetesObjectInventoryBytes: objectInventory,
		ParentInventoryBytes: map[string][]byte{parentID: parentInventory},
	})
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	return ticket
}

type fakeLocalCheckpointExportClient struct {
	requestID string
	ticket    recovery.CheckpointTicket
	payload   []byte
	err       error
}

func (client *fakeLocalCheckpointExportClient) Export(_ context.Context, requestID string, ticket recovery.CheckpointTicket, destination io.Writer) (admin.CheckpointExportReceipt, error) {
	client.requestID, client.ticket = requestID, ticket
	if client.err != nil {
		return admin.CheckpointExportReceipt{}, client.err
	}
	if _, err := destination.Write(client.payload); err != nil {
		return admin.CheckpointExportReceipt{}, err
	}
	return admin.CheckpointExportReceipt{
		RequestID: requestID, ManifestSHA256: ticket.ManifestSHA256,
		ArchiveSHA256: "sha256:" + strings.Repeat("b", 64), ArchiveBytes: uint64(len(client.payload)),
		ArchiveFormat: admin.CheckpointExportArchiveFormatV1,
	}, nil
}

func TestLocalCheckpointExportHandshakesThenStreamsExactArchive(t *testing.T) {
	ticket := validLocalCheckpointTicket(t)
	ticketBytes, err := recovery.EncodeCheckpointTicket(ticket)
	if err != nil {
		t.Fatalf("EncodeCheckpointTicket() error = %v", err)
	}
	wire := &fakeWireClient{handshake: admin.HandshakeResponse{
		DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0,
	}}
	exporter := &fakeLocalCheckpointExportClient{payload: []byte("archive-bytes")}
	var exportPath string
	var output bytes.Buffer
	err = runWithFactories(context.Background(), []string{
		"local", "--endpoint=unix:///tmp/admin.sock", "--timeout=30m",
		"checkpoint", "export", "--request-id=" + testRequestID, "--ticket-stdin=true",
	}, bytes.NewReader(ticketBytes), &output, "1.0.0",
		func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) { return wire, nil },
		func(path, _ string, _ admin.ProtocolVersion, timeout time.Duration) (checkpointExportClient, error) {
			exportPath = path
			if timeout != 30*time.Minute {
				t.Fatalf("export timeout = %s", timeout)
			}
			return exporter, nil
		})
	if err != nil {
		t.Fatalf("runWithFactories(checkpoint export) error = %v", err)
	}
	if wire.handshakeCalls != 1 || wire.executeCalls != 0 || exportPath != "/tmp/checkpoint-export.sock" {
		t.Fatalf("wire calls/export path = %d/%d/%q", wire.handshakeCalls, wire.executeCalls, exportPath)
	}
	if exporter.requestID != testRequestID || exporter.ticket.ManifestSHA256 != ticket.ManifestSHA256 || output.String() != "archive-bytes" {
		t.Fatalf("export request/output = %q/%q/%q", exporter.requestID, exporter.ticket.ManifestSHA256, output.String())
	}
}

func TestParseOperatorCheckpointAndOutputPublicationSafety(t *testing.T) {
	output := filepath.Join(t.TempDir(), "checkpoint.tar")
	prepare, err := parseOperatorCheckpoint([]string{
		"checkpoint", "prepare", "--namespace=driver", "--release=driver",
		"--request-id=" + testRequestID, "--output-file=" + output, "--timeout=45m",
	})
	if err != nil {
		t.Fatalf("parseOperatorCheckpoint(prepare) error = %v", err)
	}
	if prepare.phase != "prepare" || prepare.outputFile != output || prepare.timeout != 45*time.Minute {
		t.Fatalf("prepare invocation = %#v", prepare)
	}
	resume, err := parseOperatorCheckpoint([]string{
		"checkpoint", "resume", "--namespace=driver", "--release=driver", "--request-id=" + testRequestID,
	})
	if err != nil || resume.phase != "resume" || resume.outputFile != "" {
		t.Fatalf("resume invocation/error = %#v/%v", resume, err)
	}
	for _, args := range [][]string{
		{"checkpoint", "prepare", "--namespace=driver", "--release=driver", "--request-id=" + testRequestID},
		{"checkpoint", "resume", "--namespace=driver", "--release=driver", "--request-id=" + testRequestID, "--output-file=" + output},
		{"checkpoint", "prepare", "--namespace=driver", "--release=driver", "--request-id=" + testRequestID, "--output-file=relative.tar"},
	} {
		if _, err := parseOperatorCheckpoint(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorCheckpoint(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}

	temporary, err := createCheckpointArchiveTemp(output)
	if err != nil {
		t.Fatalf("createCheckpointArchiveTemp() error = %v", err)
	}
	temporaryPath := temporary.Name()
	if _, err := temporary.WriteString("checkpoint"); err != nil {
		t.Fatalf("temporary.WriteString() error = %v", err)
	}
	if err := temporary.Sync(); err != nil {
		t.Fatalf("temporary.Sync() error = %v", err)
	}
	if err := temporary.Close(); err != nil {
		t.Fatalf("temporary.Close() error = %v", err)
	}
	if err := publishCheckpointArchive(temporaryPath, output); err != nil {
		t.Fatalf("publishCheckpointArchive() error = %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil || string(data) != "checkpoint" {
		t.Fatalf("published archive/error = %q/%v", data, err)
	}
	if _, err := os.Lstat(temporaryPath); !os.IsNotExist(err) {
		t.Fatalf("published temporary still exists: %v", err)
	}
	if err := requireAbsentCheckpointOutput(output); err == nil {
		t.Fatal("requireAbsentCheckpointOutput(existing) error = nil")
	}
}

func TestLocalCheckpointExportRejectsInvalidTicketBeforeHandshake(t *testing.T) {
	wire := &fakeWireClient{}
	exporter := &fakeLocalCheckpointExportClient{err: errors.New("must not run")}
	err := runWithFactories(context.Background(), []string{
		"local", "checkpoint", "export", "--request-id=" + testRequestID, "--ticket-stdin=true",
	}, strings.NewReader(`{"schemaVersion":"1"}`), io.Discard, "1.0.0",
		func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) { return wire, nil },
		func(string, string, admin.ProtocolVersion, time.Duration) (checkpointExportClient, error) {
			return exporter, nil
		})
	if err == nil || wire.handshakeCalls != 0 || exporter.requestID != "" {
		t.Fatalf("invalid ticket error/calls = %v/%d/%q", err, wire.handshakeCalls, exporter.requestID)
	}
}
