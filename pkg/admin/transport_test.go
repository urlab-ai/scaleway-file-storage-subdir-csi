package admin

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingOperationHandler struct {
	mu      sync.Mutex
	calls   int
	command Command
	request MutationRequest
	payload json.RawMessage
	result  json.RawMessage
	err     error
}

type blockingOperationHandler struct {
	mu          sync.Mutex
	current     int
	maximum     int
	entered     chan struct{}
	releaseNext chan struct{}
}

func (handler *blockingOperationHandler) HandleAdminOperation(ctx context.Context, _ Command, _ MutationRequest, _ json.RawMessage) (json.RawMessage, error) {
	handler.mu.Lock()
	handler.current++
	if handler.current > handler.maximum {
		handler.maximum = handler.current
	}
	handler.mu.Unlock()
	handler.entered <- struct{}{}
	select {
	case <-ctx.Done():
		handler.mu.Lock()
		handler.current--
		handler.mu.Unlock()
		return nil, ctx.Err()
	case <-handler.releaseNext:
		handler.mu.Lock()
		handler.current--
		handler.mu.Unlock()
		return json.RawMessage(`{}`), nil
	}
}

func (handler *blockingOperationHandler) maximumConcurrency() int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.maximum
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type deadlineRejectingConn struct {
	net.Conn
	err error
}

func (connection deadlineRejectingConn) SetDeadline(time.Time) error { return connection.err }

type pipeAddress struct{}

func (pipeAddress) Network() string { return "unix" }
func (pipeAddress) String() string  { return "in-memory-admin-pipe" }

type pipeListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (listener *pipeListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *pipeListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (*pipeListener) Addr() net.Addr { return pipeAddress{} }

func (listener *pipeListener) Dial(ctx context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case listener.connections <- server:
		return client, nil
	case <-listener.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	}
}

func (handler *recordingOperationHandler) HandleAdminOperation(_ context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.calls++
	handler.command = command
	handler.request = request
	handler.payload = append(json.RawMessage(nil), payload...)
	return append(json.RawMessage(nil), handler.result...), handler.err
}

func (handler *recordingOperationHandler) callCount() int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.calls
}

func testWireServer(t *testing.T, handler OperationHandler, minimumMinor, maximumMinor uint32) (*WireServer, *pipeListener, context.CancelFunc, <-chan error) {
	t.Helper()
	server, err := NewWireServer(HandshakeResponse{
		DriverVersion: "1.2.0", ProtocolMajor: ProtocolMajorV1,
		MinimumMinor: minimumMinor, MaximumMinor: maximumMinor,
	}, handler, DefaultServerOptions())
	if err != nil {
		t.Fatalf("NewWireServer() error = %v", err)
	}
	listener, cancel, done := serveTestWireServer(t, server)
	return server, listener, cancel, done
}

func serveTestWireServer(t *testing.T, server *WireServer) (*pipeListener, context.CancelFunc, <-chan error) {
	t.Helper()
	listener := newPipeListener()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx, listener)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err, open := <-done:
			if open && err != nil {
				t.Errorf("WireServer.Serve() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("WireServer.Serve() did not stop")
		}
	})
	return listener, cancel, done
}

func TestWireClientHandshakesBeforeNegotiatedMutation(t *testing.T) {
	handler := &recordingOperationHandler{result: json.RawMessage(`{"accepted":true}`)}
	_, listener, _, _ := testWireServer(t, handler, 0, 0)
	client, err := newWireClient(listener.Dial, "1.2.0", ProtocolVersion{Major: 1, Minor: 0}, 5*time.Second)
	if err != nil {
		t.Fatalf("newWireClient() error = %v", err)
	}
	requestID := "11111111-1111-4111-8111-111111111111"
	result, err := client.Execute(context.Background(), CommandGCSubmit, requestID, json.RawMessage(`{"mode":"dry-run"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if string(result) != `{"accepted":true}` {
		t.Fatalf("Execute() result = %s", result)
	}
	if handler.callCount() != 1 || handler.command != CommandGCSubmit || handler.request.RequestID != requestID || string(handler.payload) != `{"mode":"dry-run"}` {
		t.Fatalf("handler observation = calls=%d command=%q request=%#v payload=%s", handler.callCount(), handler.command, handler.request, handler.payload)
	}
}

func TestWireClientIncompatibleHandshakeCannotReachMutationHandler(t *testing.T) {
	handler := &recordingOperationHandler{result: json.RawMessage(`{}`)}
	_, listener, _, _ := testWireServer(t, handler, 1, 2)
	client, err := newWireClient(listener.Dial, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, 5*time.Second)
	if err != nil {
		t.Fatalf("newWireClient() error = %v", err)
	}
	_, err = client.Execute(context.Background(), CommandCheckpointPrepare, "11111111-1111-4111-8111-111111111111", nil)
	if err == nil || handler.callCount() != 0 {
		t.Fatalf("incompatible Execute() error/calls = %v/%d", err, handler.callCount())
	}
	if _, ok := err.(*RemoteError); ok {
		t.Fatalf("client-side negotiation should stop before mutation, got remote mutation error %v", err)
	}
}

func TestWireServerRechecksCompatibilityOnDirectMutation(t *testing.T) {
	handler := &recordingOperationHandler{result: json.RawMessage(`{}`)}
	server, err := NewWireServer(
		HandshakeResponse{DriverVersion: "1.2.0", ProtocolMajor: 1, MinimumMinor: 1, MaximumMinor: 2},
		handler, DefaultServerOptions(),
	)
	if err != nil {
		t.Fatalf("NewWireServer() error = %v", err)
	}
	response := server.handle(context.Background(), WireRequest{
		SchemaVersion: WireSchemaVersionV1, Command: CommandCheckpointResume,
		Mutation: &MutationRequest{
			RequestID: "11111111-1111-4111-8111-111111111111", AdminVersion: "1.0.0",
			Protocol: ProtocolVersion{Major: 1, Minor: 0},
		},
	})
	if response.OK || response.Error == nil || response.Error.Code != ErrorFailedPrecondition || handler.callCount() != 0 {
		t.Fatalf("direct incompatible mutation response/calls = %#v/%d", response, handler.callCount())
	}
}

func TestWireServerBoundsConcurrentConnections(t *testing.T) {
	handler := &blockingOperationHandler{entered: make(chan struct{}, 3), releaseNext: make(chan struct{}, 3)}
	options := DefaultServerOptions()
	options.MaxConcurrent = 2
	server, err := NewWireServer(
		HandshakeResponse{DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0},
		handler, options,
	)
	if err != nil {
		t.Fatalf("NewWireServer() error = %v", err)
	}
	listener, _, _ := serveTestWireServer(t, server)
	client, err := newWireClient(listener.Dial, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, 5*time.Second)
	if err != nil {
		t.Fatalf("newWireClient() error = %v", err)
	}

	requestIDs := []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
	}
	errorsByRequest := make(chan error, len(requestIDs))
	for _, requestID := range requestIDs {
		requestID := requestID
		go func() {
			_, executeErr := client.Execute(context.Background(), CommandCheckpointPrepare, requestID, nil)
			errorsByRequest <- executeErr
		}()
	}
	for range 2 {
		select {
		case <-handler.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("two admitted operations did not enter")
		}
	}
	select {
	case <-handler.entered:
		t.Fatal("third operation entered above the configured connection limit")
	case <-time.After(100 * time.Millisecond):
	}
	handler.releaseNext <- struct{}{}
	select {
	case <-handler.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("queued third operation did not enter after capacity was released")
	}
	handler.releaseNext <- struct{}{}
	handler.releaseNext <- struct{}{}
	for range requestIDs {
		select {
		case err := <-errorsByRequest:
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("admin operation did not complete")
		}
	}
	if maximum := handler.maximumConcurrency(); maximum != 2 {
		t.Fatalf("maximum handler concurrency = %d, want 2", maximum)
	}
}

func TestWireRequestRejectsMixedUnknownAndMalformedPayloads(t *testing.T) {
	base := WireRequest{
		SchemaVersion: WireSchemaVersionV1, Command: CommandGCSubmit,
		Mutation: &MutationRequest{
			RequestID: "11111111-1111-4111-8111-111111111111", AdminVersion: "1.0.0",
			Protocol: ProtocolVersion{Major: 1, Minor: 0},
		},
		Payload: json.RawMessage(`{"mode":"execute"}`),
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid request Validate() error = %v", err)
	}
	for name, mutate := range map[string]func(*WireRequest){
		"unknown command":       func(request *WireRequest) { request.Command = "future.command" },
		"missing payload":       func(request *WireRequest) { request.Payload = nil },
		"non-object payload":    func(request *WireRequest) { request.Payload = json.RawMessage(`[]`) },
		"duplicate payload key": func(request *WireRequest) { request.Payload = json.RawMessage(`{"mode":"x","mode":"y"}`) },
		"mixed envelope": func(request *WireRequest) {
			request.Handshake = &HandshakeRequest{AdminVersion: "1.0.0", Protocol: ProtocolVersion{Major: 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := base
			mutate(&request)
			if err := request.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestFramingRejectsEmptyOversizedAndTruncatedMessages(t *testing.T) {
	for name, length := range map[string]uint32{"empty": 0, "oversized": MaxWireMessageBytes + 1} {
		t.Run(name, func(t *testing.T) {
			var header [4]byte
			binary.BigEndian.PutUint32(header[:], length)
			if _, err := readFrame(strings.NewReader(string(header[:]))); err == nil {
				t.Fatal("readFrame() error = nil")
			}
		})
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 10)
	if _, err := readFrame(strings.NewReader(string(header[:]) + "short")); err == nil {
		t.Fatal("readFrame(truncated) error = nil")
	}
	if err := writeFrame(&strings.Builder{}, make([]byte, MaxWireMessageBytes+1)); err == nil {
		t.Fatal("writeFrame(oversized) error = nil")
	}
	if err := writeFrame(zeroWriter{}, []byte(`{}`)); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeFrame(zero writer) error = %v", err)
	}
}

func TestOversizedResponseObjectFallsBackToBoundedInternalError(t *testing.T) {
	result := json.RawMessage(`{"value":"` + strings.Repeat("a", MaxWireMessageBytes-64) + `"}`)
	response := WireResponse{
		SchemaVersion: WireSchemaVersionV1, Command: CommandGCSubmit, OK: true, Result: result,
	}
	var framed bytes.Buffer
	if err := writeWireResponse(&framed, response); err != nil {
		t.Fatalf("writeWireResponse() error = %v", err)
	}
	encoded, err := readFrame(&framed)
	if err != nil {
		t.Fatalf("readFrame() error = %v", err)
	}
	if len(encoded) > MaxWireMessageBytes {
		t.Fatalf("fallback response contains %d bytes", len(encoded))
	}
	var decoded WireResponse
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded.OK || decoded.Error == nil || decoded.Error.Code != ErrorInternal {
		t.Fatalf("fallback response = %#v", decoded)
	}
}

func TestWireServerClassifiesDeliberateAndRedactsUnexpectedErrors(t *testing.T) {
	request := WireRequest{
		SchemaVersion: WireSchemaVersionV1, Command: CommandCheckpointPrepare,
		Mutation: &MutationRequest{
			RequestID: "11111111-1111-4111-8111-111111111111", AdminVersion: "1.0.0",
			Protocol: ProtocolVersion{Major: 1, Minor: 0},
		},
	}
	for name, handlerErr := range map[string]error{
		"deliberate": NewOperationError(ErrorFailedPrecondition, errors.New("checkpoint has active transitions")),
		"unexpected": errors.New("sensitive internal detail"),
	} {
		t.Run(name, func(t *testing.T) {
			handler := &recordingOperationHandler{err: handlerErr}
			server, err := NewWireServer(
				HandshakeResponse{DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0},
				handler, DefaultServerOptions(),
			)
			if err != nil {
				t.Fatalf("NewWireServer() error = %v", err)
			}
			response := server.handle(context.Background(), request)
			if response.OK || response.Error == nil {
				t.Fatalf("handle() response = %#v", response)
			}
			if name == "deliberate" && response.Error.Message != "checkpoint has active transitions" {
				t.Fatalf("deliberate message = %q", response.Error.Message)
			}
			if name == "unexpected" && strings.Contains(response.Error.Message, "sensitive") {
				t.Fatalf("unexpected error was exposed: %q", response.Error.Message)
			}
		})
	}
}

func TestWireServerCancellationClosesListenerAndJoins(t *testing.T) {
	handler := &recordingOperationHandler{result: json.RawMessage(`{}`)}
	_, _, cancel, done := testWireServer(t, handler, 0, 0)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WireServer.Serve() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WireServer.Serve() did not stop")
	}
}

func TestWireTransportFailsClosedWhenConnectionDeadlineCannotBeSet(t *testing.T) {
	deadlineErr := errors.New("deadline unsupported")

	t.Run("server", func(t *testing.T) {
		handler := &recordingOperationHandler{result: json.RawMessage(`{}`)}
		server, err := NewWireServer(
			HandshakeResponse{DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0},
			handler,
			DefaultServerOptions(),
		)
		if err != nil {
			t.Fatalf("NewWireServer() error = %v", err)
		}
		clientConnection, serverConnection := net.Pipe()
		defer func() {
			if err := clientConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close deadline-test client connection: %v", err)
			}
		}()
		server.serveConnection(context.Background(), deadlineRejectingConn{Conn: serverConnection, err: deadlineErr})
		if handler.callCount() != 0 {
			t.Fatalf("handler calls = %d, want 0", handler.callCount())
		}
	})

	t.Run("client", func(t *testing.T) {
		clientConnection, serverConnection := net.Pipe()
		defer func() {
			if err := serverConnection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close deadline-test server connection: %v", err)
			}
		}()
		client, err := newWireClient(func(context.Context) (net.Conn, error) {
			return deadlineRejectingConn{Conn: clientConnection, err: deadlineErr}, nil
		}, "1.0.0", ProtocolVersion{Major: 1, Minor: 0}, 5*time.Second)
		if err != nil {
			t.Fatalf("newWireClient() error = %v", err)
		}
		_, err = client.Handshake(context.Background())
		if err == nil || !strings.Contains(err.Error(), "set admin connection deadline") {
			t.Fatalf("Handshake() error = %v", err)
		}
	})
}

func TestUnixWireClientRejectsUnsafeSocketPath(t *testing.T) {
	for _, path := range []string{"", "relative.sock", "/", "/tmp/../tmp/admin.sock", "/" + strings.Repeat("a", maxUnixSocketPathBytes)} {
		if _, err := NewUnixWireClient(path, "1.0.0", ProtocolVersion{Major: 1}, 5*time.Second); err == nil {
			t.Fatalf("NewUnixWireClient(%q) error = nil", path)
		}
	}
}

func TestPipeListenerReportsUnixBoundary(t *testing.T) {
	listener := newPipeListener()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close pipe listener: %v", err)
		}
	})
	if network := listener.Addr().Network(); network != "unix" {
		t.Fatalf("pipe listener network = %q", network)
	}
}
