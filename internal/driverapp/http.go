package driverapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 10 * time.Second
	httpWriteTimeout      = 15 * time.Second
	httpIdleTimeout       = 30 * time.Second
	httpShutdownTimeout   = 5 * time.Second
	httpMaxHeaderBytes    = 16 * 1024
)

// HTTPServer serves exactly one unauthenticated runtime endpoint on one
// dedicated TCP listener. Its zero value is invalid and one instance may be
// served only once.
type HTTPServer struct {
	mu              sync.Mutex
	server          *http.Server
	endpoint        string
	served          bool
	shutdownTimeout time.Duration
}

// NewLivenessHTTPServer constructs the shallow /livez server. The supplied
// handler owns only cached process health and must perform no provider or
// Kubernetes I/O.
func NewLivenessHTTPServer(handler http.Handler) (*HTTPServer, error) {
	return newHTTPServer("/livez", handler)
}

// NewMetricsHTTPServer constructs the dedicated /metrics server. It must never
// share the liveness listener because the two endpoints have different
// Kubernetes and Service exposure semantics.
func NewMetricsHTTPServer(handler http.Handler) (*HTTPServer, error) {
	return newHTTPServer("/metrics", handler)
}

func newHTTPServer(endpoint string, handler http.Handler) (*HTTPServer, error) {
	if handler == nil {
		return nil, fmt.Errorf("HTTP handler for %s is nil", endpoint)
	}
	router := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != endpoint || request.URL.RawPath != "" {
			http.NotFound(writer, request)
			return
		}
		handler.ServeHTTP(writer, request)
	})
	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
		// The standard server logger may include attacker-controlled request
		// bytes. Runtime structured logging owns bounded diagnostics instead.
		ErrorLog: log.New(io.Discard, "", 0),
	}
	return &HTTPServer{server: server, endpoint: endpoint, shutdownTimeout: httpShutdownTimeout}, nil
}

// ListenHTTP opens a numeric TCP address already accepted by the closed
// process flag contract. It honors cancellation before or during bind.
func ListenHTTP(ctx context.Context, address string) (net.Listener, error) {
	if ctx == nil {
		return nil, fmt.Errorf("HTTP listener context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	validated, err := validateListenAddress("HTTP listener", address)
	if err != nil {
		return nil, err
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", validated)
	if err != nil {
		return nil, fmt.Errorf("listen on HTTP address %q: %w", validated, err)
	}
	return listener, nil
}

// Serve owns and closes one TCP listener. Cancellation immediately cancels
// request contexts, then permits a bounded graceful drain. A timed-out drain
// force-closes connections and returns an error. Serve always joins its owned
// shutdown supervisor goroutine before returning.
func (runtime *HTTPServer) Serve(ctx context.Context, listener net.Listener) error {
	if runtime == nil || runtime.server == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return fmt.Errorf("HTTP server is nil or uninitialized")
	}
	if runtime.shutdownTimeout <= 0 {
		if listener != nil {
			_ = listener.Close()
		}
		return fmt.Errorf("HTTP server shutdown timeout is invalid")
	}
	if ctx == nil {
		if listener != nil {
			_ = listener.Close()
		}
		return fmt.Errorf("HTTP server context is nil")
	}
	if listener == nil || listener.Addr() == nil || !isTCPNetwork(listener.Addr().Network()) {
		if listener != nil {
			_ = listener.Close()
		}
		return fmt.Errorf("HTTP server requires a TCP listener")
	}
	runtime.mu.Lock()
	if runtime.served {
		runtime.mu.Unlock()
		_ = listener.Close()
		return fmt.Errorf("HTTP server for %s has already been served", runtime.endpoint)
	}
	runtime.served = true
	runtime.mu.Unlock()
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return nil
	}

	requestContext, cancelRequests := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRequests()
	runtime.server.BaseContext = func(net.Listener) context.Context { return requestContext }
	stopShutdown := make(chan struct{})
	shutdownResult := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			cancelRequests()
			shutdownContext, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), runtime.shutdownTimeout)
			err := runtime.server.Shutdown(shutdownContext)
			cancelShutdown()
			if err != nil {
				_ = runtime.server.Close()
				shutdownResult <- fmt.Errorf("shutdown HTTP endpoint %s: %w", runtime.endpoint, err)
				return
			}
			shutdownResult <- nil
		case <-stopShutdown:
			shutdownResult <- nil
		}
	}()

	serveErr := runtime.server.Serve(listener)
	close(stopShutdown)
	shutdownErr := <-shutdownResult
	cancelRequests()
	if shutdownErr != nil {
		return shutdownErr
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		_ = runtime.server.Close()
		return fmt.Errorf("serve HTTP endpoint %s: %w", runtime.endpoint, serveErr)
	}
	return nil
}

func isTCPNetwork(value string) bool {
	return value == "tcp" || value == "tcp4" || value == "tcp6"
}
