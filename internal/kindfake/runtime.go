package kindfake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/csiadapter"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	fakeInstallationID = "11111111-1111-4111-8111-111111111111"
	fakeClusterUID     = "22222222-2222-4222-8222-222222222222"
	fakeParentID       = "33333333-3333-4333-8333-333333333333"
	fakeBasePath       = "/kind-fake-volumes"
	fakePoolName       = "standard"
	fakeShutdown       = 5 * time.Second
)

// Options is the closed local integration runtime contract. DataRoot must be
// on the node hostPath rendered with Bidirectional mount propagation.
type Options struct {
	Component       config.Component
	CSIEndpointPath string
	DriverName      string
	NodeName        string
	DataRoot        string
	KubeletPath     string
	LiveAddress     string
}

// Validate rejects any attempt to use the test endpoint outside its narrow
// absolute paths and component set.
func (options Options) Validate() error {
	if options.Component != config.ComponentController && options.Component != config.ComponentNode {
		return fmt.Errorf("kind fake component must be controller or node")
	}
	if err := volume.ValidateDriverName(options.DriverName); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"CSI endpoint": options.CSIEndpointPath,
		"data root":    options.DataRoot,
		"kubelet path": options.KubeletPath,
	} {
		if value == "" || value == string(filepath.Separator) || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("kind fake %s must be a clean absolute non-root path", name)
		}
	}
	if options.DataRoot == options.KubeletPath || strings.HasPrefix(options.DataRoot, options.KubeletPath+string(filepath.Separator)) || strings.HasPrefix(options.KubeletPath, options.DataRoot+string(filepath.Separator)) {
		return fmt.Errorf("kind fake data root and kubelet path must be disjoint")
	}
	if options.NodeName == "" || len(options.NodeName) > 253 || strings.ContainsAny(options.NodeName, "\x00\r\n/") {
		return fmt.Errorf("kind fake node name must contain 1 to 253 safe bytes")
	}
	host, portText, err := net.SplitHostPort(options.LiveAddress)
	port, portErr := strconv.ParseUint(portText, 10, 16)
	if err != nil || portErr != nil || port == 0 || strconv.FormatUint(port, 10) != portText || (host != "" && net.ParseIP(host) == nil) {
		return fmt.Errorf("kind fake live address must be a numeric non-zero host:port listener")
	}
	return nil
}

// Run serves one fake component until cancellation. The caller must place it
// in a disposable integration image; released driver and csi-admin binaries do
// not import this package.
func Run(ctx context.Context, options Options) error {
	if ctx == nil {
		return fmt.Errorf("kind fake context is nil")
	}
	if err := options.Validate(); err != nil {
		return err
	}
	readiness := &driver.Readiness{}
	if err := readiness.Set(true, ""); err != nil {
		return err
	}
	identityCore, err := driver.NewIdentityServiceCore(options.DriverName, "0.0.0-dev", readiness)
	if err != nil {
		return err
	}
	identity, err := csiadapter.NewIdentityServer(identityCore)
	if err != nil {
		return err
	}

	var grpcServer *csiadapter.GRPCServer
	switch options.Component {
	case config.ComponentController:
		controller := &controllerCore{driverName: options.DriverName}
		server, serverErr := csiadapter.NewControllerServer(csiadapter.ControllerCores{
			Create: controller, Delete: controller, Publish: controller, Validate: controller,
		}, []pool.Config{fakePool()})
		if serverErr != nil {
			return serverErr
		}
		grpcServer, err = csiadapter.NewGRPCServer(config.ComponentController, identity, server, nil)
	case config.ComponentNode:
		core, coreErr := newNodeCore(options)
		if coreErr != nil {
			return coreErr
		}
		server, serverErr := csiadapter.NewNodeServer(core)
		if serverErr != nil {
			return serverErr
		}
		grpcServer, err = csiadapter.NewGRPCServer(config.ComponentNode, identity, nil, server)
	}
	if err != nil {
		return err
	}
	csiListener, err := driver.ListenCSIUnix(options.CSIEndpointPath)
	if err != nil {
		return err
	}
	liveListener, err := net.Listen("tcp", options.LiveAddress)
	if err != nil {
		_ = csiListener.Close()
		return fmt.Errorf("listen on kind fake liveness address: %w", err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() {
		results <- grpcServer.Serve(serveCtx, csiListener, fakeShutdown)
	}()
	go func() {
		results <- serveLiveness(serveCtx, liveListener)
	}()
	first := <-results
	cancel()
	second := <-results
	if ctx.Err() != nil {
		return errors.Join(nonCancellationError(first), nonCancellationError(second))
	}
	return errors.Join(first, second, fmt.Errorf("kind fake endpoint stopped unexpectedly"))
}

func serveLiveness(ctx context.Context, listener net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/livez" || request.Method != http.MethodGet {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	server := &http.Server{
		Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second,
	}
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		serveErr := <-result
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

func nonCancellationError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func fakePool() pool.Config {
	ratio, _ := pool.ParseRatio("1.0")
	return pool.Config{
		Name: fakePoolName, BasePath: fakeBasePath, SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770",
		Filesystems: []pool.ParentConfig{{ID: fakeParentID, Name: "kind-fake-parent", State: pool.ParentActive}},
	}
}
