package admin

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeCheckpointExportWorkflow struct {
	checkpoint recovery.CheckpointExportPackage
	digest     string
	err        error
	requestID  string
}

func (workflow *fakeCheckpointExportWorkflow) BuildExport(_ context.Context, requestID string) (recovery.CheckpointExportPackage, string, error) {
	workflow.requestID = requestID
	return workflow.checkpoint, workflow.digest, workflow.err
}

func validAdminCheckpointExport(t *testing.T) (recovery.CheckpointExportPackage, recovery.CheckpointTicket) {
	t.Helper()
	candidate := validAdminCheckpointCandidate(t)
	manifestBytes, err := recovery.EncodeCheckpointManifest(candidate.Manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
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
	checkpoint := recovery.CheckpointExportPackage{
		ManifestBytes: manifestBytes, KubernetesObjectInventoryBytes: candidate.KubernetesObjectInventoryBytes,
		KubernetesObjects: []recovery.ExportedKubernetesObject{{
			APIVersion: "v1", Kind: "ConfigMap", Namespace: "driver", Name: "allocation-a",
			SourceUID: "uid-a", SourceResourceVersion: "42", RecoverableProjection: []byte(`{"record":"a"}`),
		}},
		Parents: []recovery.ParentOwnershipExport{{
			ParentFilesystemID: parentID, ParentOwnerBytes: ownerBytes,
			InventoryBytes: candidate.ParentInventoryBytes[parentID], Records: map[string][]byte{},
		}},
	}
	if _, _, err := recovery.VerifyCheckpointExportPackage(context.Background(), checkpoint); err != nil {
		t.Fatalf("VerifyCheckpointExportPackage() error = %v", err)
	}
	ticket, err := recovery.BuildCheckpointTicket(candidate)
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	return checkpoint, ticket
}

func TestCheckpointExportStreamAuthenticatesTicketArchiveAndTrailer(t *testing.T) {
	checkpoint, ticket := validAdminCheckpointExport(t)
	workflow := &fakeCheckpointExportWorkflow{checkpoint: checkpoint, digest: ticket.ManifestSHA256}
	server, err := NewCheckpointExportServer(HandshakeResponse{
		DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0,
	}, workflow, time.Minute)
	if err != nil {
		t.Fatalf("NewCheckpointExportServer() error = %v", err)
	}
	serverConnection, clientConnection := net.Pipe()
	serveDone := make(chan struct{})
	go func() {
		server.serveConnection(context.Background(), serverConnection)
		close(serveDone)
	}()
	client, err := newCheckpointExportClient(func(context.Context) (net.Conn, error) {
		return clientConnection, nil
	}, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, time.Minute)
	if err != nil {
		t.Fatalf("newCheckpointExportClient() error = %v", err)
	}
	var archive bytes.Buffer
	receipt, err := client.Export(context.Background(), ticket.CheckpointRequestID, ticket, &archive)
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	<-serveDone
	if workflow.requestID != ticket.CheckpointRequestID || receipt.ArchiveBytes != uint64(archive.Len()) || receipt.ManifestSHA256 != ticket.ManifestSHA256 {
		t.Fatalf("workflow/receipt = %q/%#v", workflow.requestID, receipt)
	}
	reader := tar.NewReader(bytes.NewReader(archive.Bytes()))
	header, err := reader.Next()
	if err != nil || header.Name != "format.json" {
		t.Fatalf("first archive entry = %#v, %v", header, err)
	}
}

func TestCheckpointExportStreamRejectsChangedTicketBeforeArchive(t *testing.T) {
	checkpoint, ticket := validAdminCheckpointExport(t)
	workflow := &fakeCheckpointExportWorkflow{checkpoint: checkpoint, digest: ticket.ManifestSHA256}
	server, err := NewCheckpointExportServer(HandshakeResponse{
		DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0,
	}, workflow, time.Minute)
	if err != nil {
		t.Fatalf("NewCheckpointExportServer() error = %v", err)
	}
	serverConnection, clientConnection := net.Pipe()
	go server.serveConnection(context.Background(), serverConnection)
	client, err := newCheckpointExportClient(func(context.Context) (net.Conn, error) {
		return clientConnection, nil
	}, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, time.Minute)
	if err != nil {
		t.Fatalf("newCheckpointExportClient() error = %v", err)
	}
	ticket.KubernetesObjectInventory.SizeBytes++
	var archive bytes.Buffer
	if _, err := client.Export(context.Background(), ticket.CheckpointRequestID, ticket, &archive); err == nil {
		t.Fatal("Export(changed ticket) error = nil")
	} else {
		var remote *CheckpointExportRemoteError
		if !errors.As(err, &remote) || remote.Code != ErrorFailedPrecondition {
			t.Fatalf("Export(changed ticket) error = %v", err)
		}
	}
	if archive.Len() != 0 {
		t.Fatalf("changed ticket wrote %d archive bytes", archive.Len())
	}
}

func TestCheckpointExportClientRejectsTruncatedArchiveWithoutTrailer(t *testing.T) {
	_, ticket := validAdminCheckpointExport(t)
	serverConnection, clientConnection := net.Pipe()
	go func() {
		defer func() {
			if err := serverConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close checkpoint test server connection: %v", err)
			}
		}()
		if _, err := readFrame(serverConnection); err != nil {
			return
		}
		_ = writeCheckpointExportResponse(serverConnection, CheckpointExportResponse{
			SchemaVersion: checkpointExportSchemaV1, OK: true,
			RequestID: ticket.CheckpointRequestID, ManifestSHA256: ticket.ManifestSHA256,
			ArchiveFormat: CheckpointExportArchiveFormatV1, ArchiveBytes: 10,
		})
		_, _ = io.WriteString(serverConnection, "partial")
	}()
	client, err := newCheckpointExportClient(func(context.Context) (net.Conn, error) {
		return clientConnection, nil
	}, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, time.Minute)
	if err != nil {
		t.Fatalf("newCheckpointExportClient() error = %v", err)
	}
	var archive bytes.Buffer
	if _, err := client.Export(context.Background(), ticket.CheckpointRequestID, ticket, &archive); err == nil || !strings.Contains(err.Error(), "checkpoint archive") {
		t.Fatalf("Export(truncated) error = %v", err)
	}
	if archive.String() != "partial" {
		t.Fatalf("partial destination = %q", archive.String())
	}
}
