package driverapp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryTCPAddress struct {
	network string
}

func (address memoryTCPAddress) Network() string { return address.network }
func (address memoryTCPAddress) String() string  { return "in-memory-" + address.network }

type memoryTCPListener struct {
	connections chan net.Conn
	closed      chan struct{}
	closeOnce   sync.Once
	address     memoryTCPAddress
}

func newMemoryTCPListener(network string) *memoryTCPListener {
	return &memoryTCPListener{
		connections: make(chan net.Conn), closed: make(chan struct{}),
		address: memoryTCPAddress{network: network},
	}
}

func (listener *memoryTCPListener) Accept() (net.Conn, error) {
	select {
	case connection := <-listener.connections:
		return connection, nil
	case <-listener.closed:
		return nil, net.ErrClosed
	}
}

func (listener *memoryTCPListener) Close() error {
	listener.closeOnce.Do(func() { close(listener.closed) })
	return nil
}

func (listener *memoryTCPListener) Addr() net.Addr { return listener.address }

func (listener *memoryTCPListener) Dial(ctx context.Context) (net.Conn, error) {
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

func TestHTTPServersExposeOnlyTheirExactEndpoint(t *testing.T) {
	for _, test := range []struct {
		name     string
		endpoint string
		new      func(http.Handler) (*HTTPServer, error)
	}{
		{name: "liveness", endpoint: "/livez", new: NewLivenessHTTPServer},
		{name: "metrics", endpoint: "/metrics", new: NewMetricsHTTPServer},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server, err := test.new(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				calls++
				writer.WriteHeader(http.StatusNoContent)
			}))
			if err != nil {
				t.Fatalf("new HTTP server error = %v", err)
			}
			if server.server.ReadHeaderTimeout != httpReadHeaderTimeout || server.server.ReadTimeout != httpReadTimeout || server.server.WriteTimeout != httpWriteTimeout || server.server.IdleTimeout != httpIdleTimeout || server.server.MaxHeaderBytes != httpMaxHeaderBytes || server.server.ErrorLog == nil {
				t.Fatalf("HTTP server bounds = %#v", server.server)
			}
			listener := newMemoryTCPListener("tcp")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, listener) }()
			client := memoryHTTPClient(listener)
			for requestPath, wantStatus := range map[string]int{
				test.endpoint:              http.StatusNoContent,
				"/other":                   http.StatusNotFound,
				encodedPath(test.endpoint): http.StatusNotFound,
			} {
				response, requestErr := client.Get("http://runtime" + requestPath)
				if requestErr != nil {
					cancel()
					t.Fatalf("GET %s error = %v", requestPath, requestErr)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != wantStatus {
					cancel()
					t.Fatalf("GET %s status = %d, want %d", requestPath, response.StatusCode, wantStatus)
				}
			}
			cancel()
			if err := waitHTTPServe(t, done); err != nil {
				t.Fatalf("Serve() error = %v", err)
			}
			if calls != 1 {
				t.Fatalf("endpoint handler calls = %d, want 1", calls)
			}
			client.CloseIdleConnections()
		})
	}
}

func TestHTTPServeCancellationCancelsRequestAndJoinsSupervisor(t *testing.T) {
	entered := make(chan struct{})
	handlerDone := make(chan struct{})
	server, err := NewLivenessHTTPServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
		close(handlerDone)
	}))
	if err != nil {
		t.Fatalf("NewLivenessHTTPServer() error = %v", err)
	}
	listener := newMemoryTCPListener("tcp")
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	client := memoryHTTPClient(listener)
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := client.Get("http://runtime/livez")
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("handler did not receive request")
	}
	cancel()
	if err := waitHTTPServe(t, serveDone); err != nil {
		t.Fatalf("Serve(canceled) error = %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("request context was not canceled")
	}
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client request did not terminate")
	}
	client.CloseIdleConnections()
}

func TestHTTPServeReportsBoundedShutdownFailure(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	server, err := NewMetricsHTTPServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		close(handlerDone)
	}))
	if err != nil {
		t.Fatalf("NewMetricsHTTPServer() error = %v", err)
	}
	server.shutdownTimeout = 20 * time.Millisecond
	listener := newMemoryTCPListener("tcp")
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	client := memoryHTTPClient(listener)
	requestDone := make(chan struct{})
	go func() {
		response, _ := client.Get("http://runtime/metrics")
		if response != nil {
			_ = response.Body.Close()
		}
		close(requestDone)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("blocking handler did not start")
	}
	cancel()
	serveErr := waitHTTPServe(t, serveDone)
	if serveErr == nil || !strings.Contains(serveErr.Error(), "shutdown HTTP endpoint /metrics") {
		t.Fatalf("Serve(shutdown timeout) error = %v", serveErr)
	}
	close(release)
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking handler did not finish after release")
	}
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not finish after forced close")
	}
	client.CloseIdleConnections()
}

func TestHTTPServerRejectsInvalidLifecycleAndListeners(t *testing.T) {
	if _, err := NewLivenessHTTPServer(nil); err == nil {
		t.Fatal("NewLivenessHTTPServer(nil) error = nil")
	}
	server, err := NewLivenessHTTPServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatalf("NewLivenessHTTPServer() error = %v", err)
	}
	if err := server.Serve(context.Background(), newMemoryTCPListener("unix")); err == nil {
		t.Fatal("Serve(non-TCP) error = nil")
	}
	served, err := NewLivenessHTTPServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	if err != nil {
		t.Fatalf("NewLivenessHTTPServer(served) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := served.Serve(canceled, newMemoryTCPListener("tcp")); err != nil {
		t.Fatalf("Serve(pre-canceled) error = %v", err)
	}
	if err := served.Serve(context.Background(), newMemoryTCPListener("tcp")); err == nil || !strings.Contains(err.Error(), "already been served") {
		t.Fatalf("Serve(second call) error = %v", err)
	}
	if err := (*HTTPServer)(nil).Serve(context.Background(), newMemoryTCPListener("tcp")); err == nil {
		t.Fatal("nil HTTPServer.Serve() error = nil")
	}
	if err := (&HTTPServer{server: &http.Server{}}).Serve(context.Background(), newMemoryTCPListener("tcp")); err == nil {
		t.Fatal("Serve(invalid timeout) error = nil")
	}
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if err := server.Serve(nil, newMemoryTCPListener("tcp")); err == nil {
		t.Fatal("Serve(nil context) error = nil")
	}
}

func TestListenHTTPRejectsInvalidInputBeforeBind(t *testing.T) {
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if _, err := ListenHTTP(nil, ":8080"); err == nil {
		t.Fatal("ListenHTTP(nil context) error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ListenHTTP(canceled, ":8080"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListenHTTP(canceled) error = %v", err)
	}
	for _, address := range []string{"localhost:8080", ":0", "missing-port"} {
		if _, err := ListenHTTP(context.Background(), address); err == nil {
			t.Errorf("ListenHTTP(%q) error = nil", address)
		}
	}
}

func memoryHTTPClient(listener *memoryTCPListener) *http.Client {
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return listener.Dial(ctx)
		},
	}
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func waitHTTPServe(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP Serve did not terminate")
		return nil
	}
}

func encodedPath(endpoint string) string {
	if endpoint == "/livez" {
		return "/%6civez"
	}
	return "/%6detrics"
}
