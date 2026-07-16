package admin

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
)

const (
	defaultAdminConcurrency = 4
	defaultAdminIOTimeout   = 30 * time.Second
	maxUnixSocketPathBytes  = 103
)

// ServerOptions bound connection concurrency and stalled local I/O.
type ServerOptions struct {
	MaxConcurrent int
	IOTimeout     time.Duration
}

// DefaultServerOptions returns the deliberately small controller-local admin
// transport limits. Lifecycle code applies its own stronger serialization.
func DefaultServerOptions() ServerOptions {
	return ServerOptions{MaxConcurrent: defaultAdminConcurrency, IOTimeout: defaultAdminIOTimeout}
}

// WireServer serves one request per connection. It performs compatibility
// negotiation again on every mutation, so a client cannot bypass its initial
// handshake by writing a mutation frame directly.
type WireServer struct {
	handshake HandshakeResponse
	handler   OperationHandler
	options   ServerOptions
}

// NewWireServer validates the build compatibility declaration and bounded
// transport settings.
func NewWireServer(handshake HandshakeResponse, handler OperationHandler, options ServerOptions) (*WireServer, error) {
	if err := handshake.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("admin operation handler is nil")
	}
	if options.MaxConcurrent < 1 || options.MaxConcurrent > 16 {
		return nil, fmt.Errorf("admin connection concurrency must be between 1 and 16")
	}
	if options.IOTimeout < time.Second || options.IOTimeout > 5*time.Minute {
		return nil, fmt.Errorf("admin I/O timeout must be between 1 second and 5 minutes")
	}
	return &WireServer{handshake: handshake, handler: handler, options: options}, nil
}

// Serve accepts bounded controller-local connections until cancellation. It
// owns and closes the listener, closes active connections on cancellation, and
// joins every connection goroutine before returning.
func (server *WireServer) Serve(ctx context.Context, listener net.Listener) (returnErr error) {
	if listener == nil {
		return fmt.Errorf("admin listener is nil")
	}
	if listener.Addr() == nil || listener.Addr().Network() != "unix" {
		_ = listener.Close()
		return fmt.Errorf("admin listener must use a local Unix socket")
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return nil
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			returnErr = errors.Join(returnErr, fmt.Errorf("close admin listener: %w", err))
		}
	}()

	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-serveCtx.Done():
			_ = listener.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)

	semaphore := make(chan struct{}, server.options.MaxConcurrent)
	var connections sync.WaitGroup
	waitAndReturn := func(err error) error {
		cancel()
		_ = listener.Close()
		connections.Wait()
		return err
	}

	for {
		connection, err := listener.Accept()
		if err != nil {
			if serveCtx.Err() != nil {
				return waitAndReturn(nil)
			}
			return waitAndReturn(fmt.Errorf("accept admin connection: %w", err))
		}
		select {
		case semaphore <- struct{}{}:
		case <-serveCtx.Done():
			_ = connection.Close()
			return waitAndReturn(nil)
		}
		connections.Add(1)
		go func() {
			defer connections.Done()
			defer func() { <-semaphore }()
			server.serveConnection(serveCtx, connection)
		}()
	}
}

func (server *WireServer) serveConnection(ctx context.Context, connection net.Conn) {
	// The request response already records all protocol outcomes. A final close
	// after peer cancellation only releases the bounded local socket resource.
	defer func() { _ = connection.Close() }()
	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)
	if err := connection.SetDeadline(deadlineFor(ctx, server.options.IOTimeout)); err != nil {
		// Without a kernel-enforced deadline, a local peer could hold one of the
		// bounded connection slots forever. Closing is the only fail-closed
		// response because even an error frame could block indefinitely.
		return
	}

	requestBytes, err := readFrame(connection)
	if err != nil {
		_ = writeWireResponse(connection, responseError("", ErrorInvalidArgument, "invalid admin request frame"))
		return
	}
	var request WireRequest
	if err := strictjson.Decode(requestBytes, &request); err != nil {
		_ = writeWireResponse(connection, responseError("", ErrorInvalidArgument, "invalid admin request JSON"))
		return
	}
	response := server.handle(ctx, request)
	_ = writeWireResponse(connection, response)
}

func (server *WireServer) handle(ctx context.Context, request WireRequest) WireResponse {
	if err := request.Validate(); err != nil {
		return responseError(request.Command, ErrorInvalidArgument, err.Error())
	}
	if request.Command == CommandHandshake {
		response := server.handshake
		return WireResponse{SchemaVersion: WireSchemaVersionV1, Command: request.Command, OK: true, Handshake: &response}
	}
	handshake := HandshakeRequest{AdminVersion: request.Mutation.AdminVersion, Protocol: request.Mutation.Protocol}
	if err := Negotiate(handshake, server.handshake); err != nil {
		return responseError(request.Command, ErrorFailedPrecondition, err.Error())
	}
	result, err := server.handler.HandleAdminOperation(ctx, request.Command, *request.Mutation, request.Payload)
	if err != nil {
		return responseForHandlerError(request.Command, err)
	}
	if len(result) == 0 {
		result = json.RawMessage(`{}`)
	}
	if err := validateJSONObject(result); err != nil {
		return responseError(request.Command, ErrorInternal, "admin operation returned an invalid result")
	}
	return WireResponse{SchemaVersion: WireSchemaVersionV1, Command: request.Command, OK: true, Result: append(json.RawMessage(nil), result...)}
}

// WireClient performs a separate handshake connection before every mutation,
// then sends the mutation on a new connection. The server rechecks the same
// compatibility envelope at the mutation boundary.
type WireClient struct {
	dial         func(context.Context) (net.Conn, error)
	adminVersion string
	protocol     ProtocolVersion
	ioTimeout    time.Duration
}

// NewUnixWireClient constructs a client for one absolute controller-local Unix
// socket path. Socket creation and file permissions remain runtime concerns.
func NewUnixWireClient(socketPath, adminVersion string, protocol ProtocolVersion, ioTimeout time.Duration) (*WireClient, error) {
	if err := validateUnixSocketPath(socketPath); err != nil {
		return nil, err
	}
	return newWireClient(func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "unix", socketPath)
	}, adminVersion, protocol, ioTimeout)
}

func newWireClient(dial func(context.Context) (net.Conn, error), adminVersion string, protocol ProtocolVersion, ioTimeout time.Duration) (*WireClient, error) {
	if dial == nil {
		return nil, fmt.Errorf("admin dialer is nil")
	}
	request := HandshakeRequest{AdminVersion: adminVersion, Protocol: protocol}
	if err := validateHandshakeRequest(request); err != nil {
		return nil, err
	}
	if ioTimeout < time.Second || ioTimeout > 5*time.Minute {
		return nil, fmt.Errorf("admin client I/O timeout must be between 1 second and 5 minutes")
	}
	return &WireClient{dial: dial, adminVersion: adminVersion, protocol: protocol, ioTimeout: ioTimeout}, nil
}

// Handshake obtains and validates the server's explicit compatibility range.
func (client *WireClient) Handshake(ctx context.Context) (HandshakeResponse, error) {
	request := WireRequest{
		SchemaVersion: WireSchemaVersionV1,
		Command:       CommandHandshake,
		Handshake: &HandshakeRequest{
			AdminVersion: client.adminVersion,
			Protocol:     client.protocol,
		},
	}
	response, err := client.roundTrip(ctx, request)
	if err != nil {
		return HandshakeResponse{}, err
	}
	if response.Handshake == nil {
		return HandshakeResponse{}, fmt.Errorf("admin handshake response is missing compatibility data")
	}
	if err := Negotiate(*request.Handshake, *response.Handshake); err != nil {
		return HandshakeResponse{}, err
	}
	return *response.Handshake, nil
}

// Execute handshakes first and sends a negotiated mutation only if the server
// explicitly accepts this client version. requestID must remain stable across
// retries of the same operator workflow.
func (client *WireClient) Execute(ctx context.Context, command Command, requestID string, payload json.RawMessage) (json.RawMessage, error) {
	if command == CommandHandshake || !command.valid() {
		return nil, fmt.Errorf("admin mutation command %q is unsupported", command)
	}
	if _, err := client.Handshake(ctx); err != nil {
		return nil, err
	}
	request := WireRequest{
		SchemaVersion: WireSchemaVersionV1,
		Command:       command,
		Mutation: &MutationRequest{
			RequestID: requestID, AdminVersion: client.adminVersion, Protocol: client.protocol,
		},
		Payload: payload,
	}
	response, err := client.roundTrip(ctx, request)
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), response.Result...), nil
}

func (client *WireClient) roundTrip(ctx context.Context, request WireRequest) (response WireResponse, returnErr error) {
	if err := request.Validate(); err != nil {
		return WireResponse{}, err
	}
	connection, err := client.dial(ctx)
	if err != nil {
		return WireResponse{}, fmt.Errorf("connect to local admin endpoint: %w", err)
	}
	defer func() {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			returnErr = errors.Join(returnErr, fmt.Errorf("close admin connection: %w", err))
		}
	}()
	stopCloser := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-stopCloser:
		}
	}()
	defer close(stopCloser)
	if err := connection.SetDeadline(deadlineFor(ctx, client.ioTimeout)); err != nil {
		return WireResponse{}, fmt.Errorf("set admin connection deadline: %w", err)
	}

	requestBytes, err := canonicaljson.Marshal(request)
	if err != nil {
		return WireResponse{}, err
	}
	if err := writeFrame(connection, requestBytes); err != nil {
		return WireResponse{}, fmt.Errorf("write admin request: %w", err)
	}
	responseBytes, err := readFrame(connection)
	if err != nil {
		return WireResponse{}, fmt.Errorf("read admin response: %w", err)
	}
	if err := strictjson.Decode(responseBytes, &response); err != nil {
		return WireResponse{}, fmt.Errorf("decode admin response: %w", err)
	}
	if err := response.Validate(); err != nil {
		return WireResponse{}, err
	}
	if response.Command != request.Command {
		return WireResponse{}, fmt.Errorf("admin response command %q differs from request %q", response.Command, request.Command)
	}
	if !response.OK {
		return response, &RemoteError{Command: response.Command, Code: response.Error.Code, Message: response.Error.Message}
	}
	return response, nil
}

func validateUnixSocketPath(socketPath string) error {
	if socketPath == "" || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || socketPath == string(filepath.Separator) {
		return fmt.Errorf("admin Unix socket path must be a clean absolute non-root path")
	}
	if len(socketPath) > maxUnixSocketPathBytes {
		return fmt.Errorf("admin Unix socket path exceeds portable %d-byte limit", maxUnixSocketPathBytes)
	}
	return nil
}

func deadlineFor(ctx context.Context, timeout time.Duration) time.Time {
	deadline := time.Now().Add(timeout)
	if contextDeadline, present := ctx.Deadline(); present && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

func readFrame(reader io.Reader) ([]byte, error) {
	var lengthBytes [4]byte
	if _, err := io.ReadFull(reader, lengthBytes[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes[:])
	if length == 0 || length > MaxWireMessageBytes {
		return nil, fmt.Errorf("admin frame length %d is outside [1,%d]", length, MaxWireMessageBytes)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeFrame(writer io.Writer, payload []byte) error {
	if len(payload) == 0 || len(payload) > MaxWireMessageBytes {
		return fmt.Errorf("admin frame contains %d bytes; want 1 to %d", len(payload), MaxWireMessageBytes)
	}
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], uint32(len(payload)))
	if err := writeAll(writer, lengthBytes[:]); err != nil {
		return err
	}
	return writeAll(writer, payload)
}

func writeWireResponse(writer io.Writer, response WireResponse) error {
	if err := response.Validate(); err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(response)
	if err != nil {
		return err
	}
	if len(encoded) > MaxWireMessageBytes {
		fallback := responseError(response.Command, ErrorInternal, "admin response exceeds the transport limit")
		encoded, err = canonicaljson.Marshal(fallback)
		if err != nil {
			return err
		}
	}
	return writeFrame(writer, encoded)
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) != 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

// IsRemoteError reports whether err carries one validated remote code.
func IsRemoteError(err error, code ErrorCode) bool {
	var remoteError *RemoteError
	return errors.As(err, &remoteError) && remoteError.Code == code
}
