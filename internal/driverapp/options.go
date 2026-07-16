package driverapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	buildversion "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
)

const (
	maxFlagValueBytes = 4096
	maxUnixPathBytes  = 103
)

const usageText = `Usage:
  scaleway-sfs-subdir-csi --mode=<controller|node> --endpoint=unix:///absolute/csi.sock --admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock --config=/absolute/config.json --live-address=<host:port> [--metrics-address=<host:port>]
  scaleway-sfs-subdir-csi --version
`

var allowedFlags = map[string]struct{}{
	"mode":            {},
	"endpoint":        {},
	"admin-endpoint":  {},
	"config":          {},
	"live-address":    {},
	"metrics-address": {},
}

var requiredFlags = []string{"mode", "endpoint", "admin-endpoint", "config", "live-address"}

// Options is the validated, non-secret process-listener and configuration-file
// authority. Endpoint URIs are reduced to clean Unix paths before any listener
// or projected file is opened.
type Options struct {
	Component         config.Component
	CSIEndpointPath   string
	AdminEndpointPath string
	ConfigPath        string
	LiveAddress       string
	MetricsAddress    string
}

// Startup is the complete dependency-independent process input. Secret values
// checked by config.LoadRuntimeFile are deliberately absent.
type Startup struct {
	Options Options
	Config  config.Loaded
}

type usageError struct {
	err error
}

func (err *usageError) Error() string {
	if err == nil || err.err == nil {
		return "invalid driver invocation"
	}
	return err.err.Error()
}

func (err *usageError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.err
}

// Parse validates the closed flag set without accepting positional arguments,
// aliases, duplicate flags, implicit defaults, or a network CSI endpoint.
func Parse(args []string) (Options, error) {
	values, err := uniqueFlagValues(args)
	if err != nil {
		return Options{}, usage(err)
	}
	component := config.Component(values["mode"])
	if component != config.ComponentController && component != config.ComponentNode {
		return Options{}, usage(fmt.Errorf("--mode must be controller or node"))
	}
	csiPath, err := unixEndpointPath("CSI", values["endpoint"])
	if err != nil {
		return Options{}, usage(err)
	}
	adminPath, err := unixEndpointPath("admin", values["admin-endpoint"])
	if err != nil {
		return Options{}, usage(err)
	}
	if adminPath != admin.DefaultUnixSocketPath {
		return Options{}, usage(fmt.Errorf("--admin-endpoint must be the fixed unix://%s endpoint", admin.DefaultUnixSocketPath))
	}
	if csiPath == adminPath || filepath.Dir(csiPath) == filepath.Dir(adminPath) {
		return Options{}, usage(fmt.Errorf("CSI and admin endpoints must use separate directories"))
	}
	configPath := values["config"]
	if configPath == "" || !filepath.IsAbs(configPath) || filepath.Clean(configPath) != configPath {
		return Options{}, usage(fmt.Errorf("--config must be a clean absolute path"))
	}
	liveAddress, err := validateListenAddress("--live-address", values["live-address"])
	if err != nil {
		return Options{}, usage(err)
	}
	metricsAddress := ""
	if value, present := values["metrics-address"]; present {
		metricsAddress, err = validateListenAddress("--metrics-address", value)
		if err != nil {
			return Options{}, usage(err)
		}
		if metricsAddress == liveAddress {
			return Options{}, usage(fmt.Errorf("live and metrics addresses must differ"))
		}
	}
	return Options{
		Component: component, CSIEndpointPath: csiPath, AdminEndpointPath: adminPath,
		ConfigPath: configPath, LiveAddress: liveAddress, MetricsAddress: metricsAddress,
	}, nil
}

// Load parses process options and then loads the exact component-specific
// runtime projection. It performs no listener, Kubernetes, provider, mount, or
// filesystem mutation.
func Load(ctx context.Context, args []string, lookup config.LookupEnv) (Startup, error) {
	if ctx == nil {
		return Startup{}, fmt.Errorf("driver startup context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Startup{}, err
	}
	if err := buildversion.ValidateBuildIdentity(); err != nil {
		return Startup{}, fmt.Errorf("validate driver build identity: %w", err)
	}
	options, err := Parse(args)
	if err != nil {
		return Startup{}, err
	}
	loaded, err := config.LoadRuntimeFile(ctx, options.ConfigPath, options.Component, lookup)
	if err != nil {
		return Startup{}, fmt.Errorf("load %s runtime: %w", options.Component, err)
	}
	if err := validateReleaseCompatibility(loaded.Runtime); err != nil {
		return Startup{}, err
	}
	return Startup{Options: options, Config: loaded}, nil
}

func validateReleaseCompatibility(runtime config.Runtime) error {
	if runtime.Mode != config.ModeProduction {
		return nil
	}
	embedded, err := buildversion.CommercialTypes()
	if err != nil {
		return fmt.Errorf("validate production build compatibility: %w", err)
	}
	if !slices.Equal(embedded, runtime.Compatibility.QualifiedCommercialTypes) {
		return fmt.Errorf("production runtime commercial type allowlist differs from the exact driver build")
	}
	return nil
}

// ExitCode returns 2 for command-line errors and 1 for startup/configuration
// failures. A nil error maps to success.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var usage *usageError
	if errors.As(err, &usage) {
		return 2
	}
	return 1
}

// Usage returns the stable closed driver synopsis.
func Usage() string {
	return usageText
}

func uniqueFlagValues(args []string) (map[string]string, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("driver flags are required")
	}
	if len(args) > 2*len(allowedFlags) {
		return nil, fmt.Errorf("driver invocation contains too many arguments")
	}
	values := make(map[string]string, len(allowedFlags))
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if !utf8.ValidString(argument) || len(argument) > maxFlagValueBytes+64 {
			return nil, fmt.Errorf("driver argument is not bounded valid UTF-8")
		}
		if !strings.HasPrefix(argument, "--") || argument == "--" {
			return nil, fmt.Errorf("positional or short argument %q is not supported", argument)
		}
		nameValue := strings.TrimPrefix(argument, "--")
		name, value, inline := strings.Cut(nameValue, "=")
		_, known := allowedFlags[name]
		if !known {
			return nil, fmt.Errorf("unknown driver flag --%s", name)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, fmt.Errorf("driver flag --%s is duplicated", name)
		}
		if !inline {
			if index+1 >= len(args) || strings.HasPrefix(args[index+1], "--") {
				return nil, fmt.Errorf("driver flag --%s requires a value", name)
			}
			index++
			value = args[index]
		}
		if value == "" {
			return nil, fmt.Errorf("driver flag --%s is empty", name)
		}
		if !utf8.ValidString(value) || len(value) > maxFlagValueBytes || strings.ContainsAny(value, "\x00\r\n") {
			return nil, fmt.Errorf("driver flag --%s value is not bounded single-line text", name)
		}
		values[name] = value
	}
	for _, name := range requiredFlags {
		if _, present := values[name]; !present {
			return nil, fmt.Errorf("required driver flag --%s is missing", name)
		}
	}
	return values, nil
}

func unixEndpointPath(name, endpoint string) (string, error) {
	const prefix = "unix://"
	if !strings.HasPrefix(endpoint, prefix) {
		return "", fmt.Errorf("%s endpoint must use unix:///absolute/path", name)
	}
	value := strings.TrimPrefix(endpoint, prefix)
	if value == "" || value == string(filepath.Separator) || !filepath.IsAbs(value) || filepath.Clean(value) != value || strings.ContainsAny(value, "\x00\r\n?#") {
		return "", fmt.Errorf("%s endpoint must contain a clean absolute Unix socket path", name)
	}
	if len(value) > maxUnixPathBytes {
		return "", fmt.Errorf("%s Unix socket path exceeds portable %d-byte limit", name, maxUnixPathBytes)
	}
	return value, nil
}

func validateListenAddress(name, value string) (string, error) {
	host, portText, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("%s must be a numeric host:port listener: %w", name, err)
	}
	if host != "" && net.ParseIP(host) == nil {
		return "", fmt.Errorf("%s host must be empty or a numeric IP address", name)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 || strconv.FormatUint(port, 10) != portText {
		return "", fmt.Errorf("%s port must be in [1,65535]", name)
	}
	return value, nil
}

func usage(err error) error {
	return &usageError{err: err}
}
