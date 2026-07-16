package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
)

const (
	checkpointExportSchemaV1 = "1"
	// CheckpointExportArchiveFormatV1 is the deterministic streamed artifact
	// format recorded in operator receipts.
	CheckpointExportArchiveFormatV1 = "checkpoint-tar-v1"
	maxCheckpointArchiveBytes       = uint64(1 << 30)
	defaultCheckpointExportTimeout  = 30 * time.Minute
)

// CheckpointExportWorkflow rebuilds and verifies one package while retaining
// the active coordinator barrier and leadership proof.
type CheckpointExportWorkflow interface {
	BuildExport(ctx context.Context, requestID string) (recovery.CheckpointExportPackage, string, error)
}

// CheckpointExportRequest is the bounded prelude on the controller-only
// streaming socket. The prepare ticket is repeated here so the server rejects
// a stale or substituted export before writing any archive bytes.
type CheckpointExportRequest struct {
	SchemaVersion string                    `json:"schemaVersion"`
	Mutation      MutationRequest           `json:"mutation"`
	Ticket        recovery.CheckpointTicket `json:"ticket"`
}

func (request CheckpointExportRequest) validate() error {
	if request.SchemaVersion != checkpointExportSchemaV1 {
		return fmt.Errorf("checkpoint export schema version %q is unsupported", request.SchemaVersion)
	}
	if err := request.Mutation.Validate(); err != nil {
		return err
	}
	if err := request.Ticket.Validate(); err != nil {
		return err
	}
	if request.Ticket.CheckpointRequestID != request.Mutation.RequestID {
		return fmt.Errorf("checkpoint export ticket request differs from mutation request")
	}
	return nil
}

// CheckpointExportResponse precedes the raw archive and commits its exact byte
// length. A failed response is followed by no archive.
type CheckpointExportResponse struct {
	SchemaVersion  string     `json:"schemaVersion"`
	OK             bool       `json:"ok"`
	RequestID      string     `json:"requestID,omitempty"`
	ManifestSHA256 string     `json:"manifestSHA256,omitempty"`
	ArchiveFormat  string     `json:"archiveFormat,omitempty"`
	ArchiveBytes   uint64     `json:"archiveBytes,omitempty"`
	Error          *WireError `json:"error,omitempty"`
}

func (response CheckpointExportResponse) validate() error {
	if response.SchemaVersion != checkpointExportSchemaV1 {
		return fmt.Errorf("checkpoint export response schema version %q is unsupported", response.SchemaVersion)
	}
	if response.OK {
		if response.Error != nil || response.ArchiveFormat != CheckpointExportArchiveFormatV1 || response.ArchiveBytes == 0 || response.ArchiveBytes > maxCheckpointArchiveBytes {
			return fmt.Errorf("successful checkpoint export response is incomplete")
		}
		if err := recovery.ValidateCheckpointExportReceiptIdentity(response.RequestID, response.ManifestSHA256); err != nil {
			return err
		}
		return nil
	}
	if response.RequestID != "" || response.ManifestSHA256 != "" || response.ArchiveFormat != "" || response.ArchiveBytes != 0 || response.Error == nil {
		return fmt.Errorf("failed checkpoint export response has an invalid envelope")
	}
	return response.Error.Validate()
}

// CheckpointExportTrailer authenticates exact stream completion. The client
// accepts success only after reading ArchiveBytes and this matching digest.
type CheckpointExportTrailer struct {
	SchemaVersion string `json:"schemaVersion"`
	RequestID     string `json:"requestID"`
	ArchiveBytes  uint64 `json:"archiveBytes"`
	ArchiveSHA256 string `json:"archiveSHA256"`
	Complete      bool   `json:"complete"`
}

func (trailer CheckpointExportTrailer) validate() error {
	if trailer.SchemaVersion != checkpointExportSchemaV1 || !trailer.Complete || trailer.ArchiveBytes == 0 || trailer.ArchiveBytes > maxCheckpointArchiveBytes {
		return fmt.Errorf("checkpoint export completion trailer is incomplete")
	}
	return recovery.ValidateCheckpointExportReceiptIdentity(trailer.RequestID, trailer.ArchiveSHA256)
}

// CheckpointExportReceipt is the bounded operator audit returned only after a
// complete archive and trailer have been verified.
type CheckpointExportReceipt struct {
	RequestID      string `json:"requestID"`
	ManifestSHA256 string `json:"manifestSHA256"`
	ArchiveSHA256  string `json:"archiveSHA256"`
	ArchiveBytes   uint64 `json:"archiveBytes"`
	ArchiveFormat  string `json:"archiveFormat"`
}

// Validate checks a completed stream receipt.
func (receipt CheckpointExportReceipt) Validate() error {
	if err := recovery.ValidateCheckpointExportReceiptIdentity(receipt.RequestID, receipt.ManifestSHA256); err != nil {
		return err
	}
	if err := recovery.ValidateCheckpointExportReceiptIdentity(receipt.RequestID, receipt.ArchiveSHA256); err != nil {
		return err
	}
	if receipt.ArchiveBytes == 0 || receipt.ArchiveBytes > maxCheckpointArchiveBytes || receipt.ArchiveFormat != CheckpointExportArchiveFormatV1 {
		return fmt.Errorf("checkpoint export receipt archive projection is invalid")
	}
	return nil
}

// CheckpointExportServer serves the controller-only archive stream. It allows
// one active connection because CheckpointCoordinator independently serializes
// prepare, export, and resume.
type CheckpointExportServer struct {
	handshake HandshakeResponse
	workflow  CheckpointExportWorkflow
	timeout   time.Duration
}

// NewCheckpointExportServer validates the streaming workflow and compatibility
// declaration.
func NewCheckpointExportServer(handshake HandshakeResponse, workflow CheckpointExportWorkflow, timeout time.Duration) (*CheckpointExportServer, error) {
	if err := handshake.Validate(); err != nil {
		return nil, err
	}
	if workflow == nil {
		return nil, fmt.Errorf("checkpoint export workflow is nil")
	}
	if timeout < time.Second || timeout > time.Hour {
		return nil, fmt.Errorf("checkpoint export timeout must be between 1 second and 1 hour")
	}
	return &CheckpointExportServer{handshake: handshake, workflow: workflow, timeout: timeout}, nil
}

// DefaultCheckpointExportTimeout returns the bounded production stream timeout.
func DefaultCheckpointExportTimeout() time.Duration { return defaultCheckpointExportTimeout }

// Serve accepts controller-local streams until cancellation and closes every
// active connection before returning.
func (server *CheckpointExportServer) Serve(ctx context.Context, listener net.Listener) (returnErr error) {
	if listener == nil || listener.Addr() == nil || listener.Addr().Network() != "unix" {
		if listener != nil {
			_ = listener.Close()
		}
		return fmt.Errorf("checkpoint export listener must use a local Unix socket")
	}
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			returnErr = errors.Join(returnErr, fmt.Errorf("close checkpoint export listener: %w", err))
		}
	}()
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stop:
		}
	}()
	defer close(stop)
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept checkpoint export connection: %w", err)
		}
		server.serveConnection(ctx, connection)
	}
}

func (server *CheckpointExportServer) serveConnection(ctx context.Context, connection net.Conn) {
	// Closing an already canceled peer has no protocol result to report; every
	// operation error is returned on the wire before this final resource release.
	defer func() { _ = connection.Close() }()
	operationCtx, cancelOperation := context.WithTimeout(ctx, server.timeout)
	defer cancelOperation()
	stop := make(chan struct{})
	go func() {
		select {
		case <-operationCtx.Done():
			_ = connection.Close()
		case <-stop:
		}
	}()
	defer close(stop)
	if err := connection.SetDeadline(deadlineFor(operationCtx, server.timeout)); err != nil {
		return
	}
	requestBytes, err := readFrame(connection)
	if err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInvalidArgument, "invalid checkpoint export request frame"))
		return
	}
	var request CheckpointExportRequest
	if err := strictjson.Decode(requestBytes, &request); err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInvalidArgument, "invalid checkpoint export request JSON"))
		return
	}
	if err := request.validate(); err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInvalidArgument, err.Error()))
		return
	}
	if err := Negotiate(HandshakeRequest{AdminVersion: request.Mutation.AdminVersion, Protocol: request.Mutation.Protocol}, server.handshake); err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorFailedPrecondition, err.Error()))
		return
	}
	checkpoint, manifestDigest, err := server.workflow.BuildExport(operationCtx, request.Mutation.RequestID)
	if err != nil {
		classified := responseForHandlerError(CommandCheckpointPrepare, commandWorkflowError(err))
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(classified.Error.Code, classified.Error.Message))
		return
	}
	exportTicket, err := recovery.CheckpointTicketForExport(operationCtx, checkpoint)
	if err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInternal, "checkpoint export verification failed"))
		return
	}
	wantTicket, err := recovery.EncodeCheckpointTicket(request.Ticket)
	if err != nil {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInvalidArgument, err.Error()))
		return
	}
	actualTicket, err := recovery.EncodeCheckpointTicket(exportTicket)
	if err != nil || string(wantTicket) != string(actualTicket) {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorFailedPrecondition, "checkpoint export differs from prepare ticket"))
		return
	}
	archiveSize, err := recovery.CheckpointArchiveSize(operationCtx, checkpoint)
	if err != nil || archiveSize == 0 || archiveSize > maxCheckpointArchiveBytes {
		_ = writeCheckpointExportResponse(connection, checkpointExportFailure(ErrorInternal, "checkpoint archive size is invalid"))
		return
	}
	response := CheckpointExportResponse{
		SchemaVersion: checkpointExportSchemaV1, OK: true, RequestID: request.Mutation.RequestID,
		ManifestSHA256: manifestDigest, ArchiveFormat: CheckpointExportArchiveFormatV1, ArchiveBytes: archiveSize,
	}
	if err := writeCheckpointExportResponse(connection, response); err != nil {
		return
	}
	digest := sha256.New()
	counter := &checkpointStreamCounter{writer: io.MultiWriter(connection, digest)}
	if err := recovery.WriteCheckpointArchive(operationCtx, counter, checkpoint); err != nil || counter.bytes != archiveSize {
		return
	}
	trailer := CheckpointExportTrailer{
		SchemaVersion: checkpointExportSchemaV1, RequestID: request.Mutation.RequestID,
		ArchiveBytes: counter.bytes, ArchiveSHA256: digestString(digest), Complete: true,
	}
	_ = writeCheckpointExportTrailer(connection, trailer)
}

// CheckpointExportClient streams a complete archive into a caller-owned
// destination and returns only after verifying its completion trailer.
type CheckpointExportClient struct {
	dial         func(context.Context) (net.Conn, error)
	adminVersion string
	protocol     ProtocolVersion
	timeout      time.Duration
}

// NewUnixCheckpointExportClient configures the controller-only stream client.
func NewUnixCheckpointExportClient(socketPath, adminVersion string, protocol ProtocolVersion, timeout time.Duration) (*CheckpointExportClient, error) {
	if err := validateUnixSocketPath(socketPath); err != nil {
		return nil, err
	}
	return newCheckpointExportClient(func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socketPath)
	}, adminVersion, protocol, timeout)
}

func newCheckpointExportClient(dial func(context.Context) (net.Conn, error), adminVersion string, protocol ProtocolVersion, timeout time.Duration) (*CheckpointExportClient, error) {
	if dial == nil {
		return nil, fmt.Errorf("checkpoint export dialer is nil")
	}
	if err := validateHandshakeRequest(HandshakeRequest{AdminVersion: adminVersion, Protocol: protocol}); err != nil {
		return nil, err
	}
	if timeout < time.Second || timeout > time.Hour {
		return nil, fmt.Errorf("checkpoint export client timeout must be between 1 second and 1 hour")
	}
	return &CheckpointExportClient{dial: dial, adminVersion: adminVersion, protocol: protocol, timeout: timeout}, nil
}

// Export verifies the ticket before connecting and writes exactly one complete
// authenticated archive to destination.
func (client *CheckpointExportClient) Export(ctx context.Context, requestID string, ticket recovery.CheckpointTicket, destination io.Writer) (receipt CheckpointExportReceipt, returnErr error) {
	if destination == nil {
		return CheckpointExportReceipt{}, fmt.Errorf("checkpoint export destination is nil")
	}
	request := CheckpointExportRequest{
		SchemaVersion: checkpointExportSchemaV1,
		Mutation:      MutationRequest{RequestID: requestID, AdminVersion: client.adminVersion, Protocol: client.protocol},
		Ticket:        ticket.Clone(),
	}
	if err := request.validate(); err != nil {
		return CheckpointExportReceipt{}, err
	}
	connection, err := client.dial(ctx)
	if err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("connect to checkpoint export endpoint: %w", err)
	}
	defer func() {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			returnErr = errors.Join(returnErr, fmt.Errorf("close checkpoint export connection: %w", err))
		}
	}()
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-stop:
		}
	}()
	defer close(stop)
	if err := connection.SetDeadline(deadlineFor(ctx, client.timeout)); err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("set checkpoint export deadline: %w", err)
	}
	requestBytes, err := canonicaljson.Marshal(request)
	if err != nil {
		return CheckpointExportReceipt{}, err
	}
	if err := writeFrame(connection, requestBytes); err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("write checkpoint export request: %w", err)
	}
	responseBytes, err := readFrame(connection)
	if err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("read checkpoint export response: %w", err)
	}
	var response CheckpointExportResponse
	if err := strictjson.Decode(responseBytes, &response); err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("decode checkpoint export response: %w", err)
	}
	if err := response.validate(); err != nil {
		return CheckpointExportReceipt{}, err
	}
	if !response.OK {
		return CheckpointExportReceipt{}, &CheckpointExportRemoteError{Code: response.Error.Code, Message: response.Error.Message}
	}
	if response.RequestID != requestID || response.ManifestSHA256 != ticket.ManifestSHA256 {
		return CheckpointExportReceipt{}, fmt.Errorf("checkpoint export response differs from prepare ticket")
	}
	digest := sha256.New()
	written, err := io.CopyN(io.MultiWriter(destination, digest), connection, int64(response.ArchiveBytes))
	if err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("read checkpoint archive: %w", err)
	}
	if uint64(written) != response.ArchiveBytes {
		return CheckpointExportReceipt{}, fmt.Errorf("checkpoint archive is truncated")
	}
	trailerBytes, err := readFrame(connection)
	if err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("read checkpoint export trailer: %w", err)
	}
	var trailer CheckpointExportTrailer
	if err := strictjson.Decode(trailerBytes, &trailer); err != nil {
		return CheckpointExportReceipt{}, fmt.Errorf("decode checkpoint export trailer: %w", err)
	}
	if err := trailer.validate(); err != nil {
		return CheckpointExportReceipt{}, err
	}
	archiveDigest := digestString(digest)
	if trailer.RequestID != requestID || trailer.ArchiveBytes != response.ArchiveBytes || trailer.ArchiveSHA256 != archiveDigest {
		return CheckpointExportReceipt{}, fmt.Errorf("checkpoint export completion trailer differs from received archive")
	}
	receipt = CheckpointExportReceipt{
		RequestID: requestID, ManifestSHA256: response.ManifestSHA256,
		ArchiveSHA256: archiveDigest, ArchiveBytes: response.ArchiveBytes, ArchiveFormat: response.ArchiveFormat,
	}
	if err := receipt.Validate(); err != nil {
		return CheckpointExportReceipt{}, err
	}
	return receipt, nil
}

// CheckpointExportRemoteError is a validated stream prelude failure.
type CheckpointExportRemoteError struct {
	Code    ErrorCode
	Message string
}

func (remote *CheckpointExportRemoteError) Error() string {
	return fmt.Sprintf("checkpoint export failed (%s): %s", remote.Code, remote.Message)
}

func checkpointExportFailure(code ErrorCode, message string) CheckpointExportResponse {
	wireError := &WireError{Code: code, Message: boundedWireMessage(message)}
	if err := wireError.Validate(); err != nil {
		wireError = &WireError{Code: ErrorInternal, Message: "checkpoint export failed"}
	}
	return CheckpointExportResponse{SchemaVersion: checkpointExportSchemaV1, Error: wireError}
}

func writeCheckpointExportResponse(writer io.Writer, response CheckpointExportResponse) error {
	if err := response.validate(); err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(response)
	if err != nil {
		return err
	}
	return writeFrame(writer, encoded)
}

func writeCheckpointExportTrailer(writer io.Writer, trailer CheckpointExportTrailer) error {
	if err := trailer.validate(); err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(trailer)
	if err != nil {
		return err
	}
	return writeFrame(writer, encoded)
}

func digestString(digest hash.Hash) string {
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

type checkpointStreamCounter struct {
	writer io.Writer
	bytes  uint64
}

func (counter *checkpointStreamCounter) Write(data []byte) (int, error) {
	if uint64(len(data)) > ^uint64(0)-counter.bytes {
		return 0, errors.New("checkpoint export byte count overflow")
	}
	written, err := counter.writer.Write(data)
	counter.bytes += uint64(written)
	return written, err
}
