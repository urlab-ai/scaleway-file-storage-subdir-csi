package admincli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	defaultLocalTimeout = 30 * time.Second
	maxCLIArguments     = 32
	maxCLIArgumentBytes = 4096
)

const usageText = `Usage:
  csi-admin version
  csi-admin checkpoint prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --output-file=/absolute/checkpoint.tar [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin checkpoint resume --namespace=<namespace> --release=<helm-release> --request-id=<uuid> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin checkpoint restore --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --archive-file=/absolute/checkpoint.tar --identity-secret=<name> --identity-key=<key> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin decommission prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --parent-filesystem-id=<uuid> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin gc submit --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --logical-volume-id=<id> --mode=<dry-run|execute> --expected-state=<Archived|Retained> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin upgrade preflight --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --candidate-file=/absolute/candidate.json [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=10m]
  csi-admin uninstall prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
  csi-admin local [--endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock] [--timeout=30s] handshake
  csi-admin local [global flags] checkpoint prepare --request-id=<uuid>
  csi-admin local [global flags] checkpoint export --request-id=<uuid> --ticket-stdin=true
  csi-admin local [global flags] checkpoint resume --request-id=<uuid>
  csi-admin local [global flags] decommission inspect --request-id=<uuid> --parent-filesystem-id=<uuid>
  csi-admin local [global flags] decommission prepare --request-id=<uuid> --parent-filesystem-id=<uuid>
  csi-admin local [global flags] decommission quiesce --request-id=<uuid> --parent-filesystem-id=<uuid>
  csi-admin local [global flags] decommission cleanup --request-id=<uuid> --parent-filesystem-id=<uuid>
  csi-admin local [global flags] decommission release --request-id=<uuid> --parent-filesystem-id=<uuid>
  csi-admin local [global flags] gc submit --request-id=<uuid> --logical-volume-id=<id> --mode=<dry-run|execute> --expected-state=<Archived|Retained>
  csi-admin local [global flags] upgrade preflight --request-id=<uuid> --candidate-file=/absolute/candidate.json
  csi-admin local [global flags] upgrade preflight --request-id=<uuid> --candidate-stdin=true
  csi-admin local [global flags] uninstall inspect --request-id=<uuid>
  csi-admin local [global flags] uninstall prepare --request-id=<uuid>
  csi-admin local [global flags] uninstall quiesce --request-id=<uuid>
  csi-admin local [global flags] uninstall cleanup --request-id=<uuid>
  csi-admin local [global flags] uninstall release --request-id=<uuid>

The local form runs only inside a driver container through operator-authorized exec.
Local uninstall phases are private primitives and never authorize Helm deletion.
`

type invocation struct {
	endpoint         string
	timeout          time.Duration
	command          admin.Command
	requestID        string
	payload          json.RawMessage
	candidateFile    string
	candidateStdin   bool
	checkpointExport bool
	ticketStdin      bool
}

type wireClient interface {
	Handshake(context.Context) (admin.HandshakeResponse, error)
	Execute(context.Context, admin.Command, string, json.RawMessage) (json.RawMessage, error)
}

type clientFactory func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error)

type checkpointExportClient interface {
	Export(context.Context, string, recovery.CheckpointTicket, io.Writer) (admin.CheckpointExportReceipt, error)
}

type checkpointExportClientFactory func(string, string, admin.ProtocolVersion, time.Duration) (checkpointExportClient, error)

type usageError struct {
	err error
}

func (err *usageError) Error() string {
	if err == nil || err.err == nil {
		return "invalid csi-admin invocation"
	}
	return err.err.Error()
}

func (err *usageError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.err
}

// Run parses and executes one operator orchestration or in-container local
// admin operation. Results are written as exact validated JSON followed by one
// newline.
func Run(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	return RunWithIO(ctx, args, os.Stdin, stdout, buildVersion)
}

// RunWithIO executes an operator or in-container command with explicit input.
// Stdin is consumed only by the private bounded upgrade candidate stream used
// under kubectl exec -i; all other commands leave it untouched.
func RunWithIO(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, buildVersion string) error {
	if ctx == nil {
		return fmt.Errorf("admin command context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stdout == nil {
		return fmt.Errorf("admin command output is nil")
	}
	if len(args) > 0 && args[0] == "uninstall" {
		return runOperatorUninstall(ctx, args, stdout, buildVersion)
	}
	if len(args) > 0 && args[0] == "checkpoint" {
		return runOperatorCheckpoint(ctx, args, stdout, buildVersion)
	}
	if len(args) > 0 && args[0] == "decommission" {
		return runOperatorDecommission(ctx, args, stdout, buildVersion)
	}
	if len(args) > 0 && args[0] == "gc" {
		return runOperatorGC(ctx, args, stdout, buildVersion)
	}
	if len(args) > 0 && args[0] == "upgrade" {
		return runOperatorUpgrade(ctx, args, stdout, buildVersion)
	}
	return runWithFactories(ctx, args, stdin, stdout, buildVersion, func(socketPath, version string, protocol admin.ProtocolVersion, timeout time.Duration) (wireClient, error) {
		return admin.NewUnixWireClient(socketPath, version, protocol, timeout)
	}, func(socketPath, version string, protocol admin.ProtocolVersion, timeout time.Duration) (checkpointExportClient, error) {
		return admin.NewUnixCheckpointExportClient(socketPath, version, protocol, timeout)
	})
}

// ExitCode returns 2 for local command-line errors and 1 for runtime or remote
// operation failures. A nil error maps to success.
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

// Usage returns the closed command synopsis without build- or environment-
// dependent text.
func Usage() string {
	return usageText
}

func run(ctx context.Context, args []string, stdout io.Writer, buildVersion string, factory clientFactory) error {
	return runWithInput(ctx, args, nil, stdout, buildVersion, factory)
}

func runWithInput(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, buildVersion string, factory clientFactory) error {
	return runWithFactories(ctx, args, stdin, stdout, buildVersion, factory, func(socketPath, version string, protocol admin.ProtocolVersion, timeout time.Duration) (checkpointExportClient, error) {
		return admin.NewUnixCheckpointExportClient(socketPath, version, protocol, timeout)
	})
}

func runWithFactories(ctx context.Context, args []string, stdin io.Reader, stdout io.Writer, buildVersion string, factory clientFactory, exportFactory checkpointExportClientFactory) error {
	if ctx == nil {
		return fmt.Errorf("admin command context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stdout == nil {
		return fmt.Errorf("admin command output is nil")
	}
	if factory == nil {
		return fmt.Errorf("admin client factory is nil")
	}
	if exportFactory == nil {
		return fmt.Errorf("checkpoint export client factory is nil")
	}
	parsed, err := parseInvocation(args)
	if err != nil {
		return err
	}
	socketPath, err := localSocketPath(parsed.endpoint)
	if err != nil {
		return usage(err)
	}
	protocol := admin.ProtocolVersion{
		Major: admin.ProtocolMajorV1,
		Minor: admin.ProtocolMinorV1,
	}
	controlTimeout := parsed.timeout
	if controlTimeout > 5*time.Minute {
		controlTimeout = 5 * time.Minute
	}
	client, err := factory(socketPath, buildVersion, protocol, controlTimeout)
	if err != nil {
		return fmt.Errorf("configure local admin client: %w", err)
	}

	if parsed.command == admin.CommandHandshake {
		response, handshakeErr := client.Handshake(ctx)
		if handshakeErr != nil {
			return handshakeErr
		}
		encoded, encodeErr := canonicaljson.Marshal(response)
		if encodeErr != nil {
			return fmt.Errorf("encode admin handshake result: %w", encodeErr)
		}
		return writeResult(stdout, encoded)
	}
	if parsed.checkpointExport {
		if !parsed.ticketStdin {
			return fmt.Errorf("checkpoint export ticket input is absent")
		}
		ticket, readErr := readCheckpointTicketFromReader(ctx, stdin)
		if readErr != nil {
			return readErr
		}
		if ticket.CheckpointRequestID != parsed.requestID {
			return fmt.Errorf("checkpoint export ticket request ID differs from command")
		}
		if _, handshakeErr := client.Handshake(ctx); handshakeErr != nil {
			return handshakeErr
		}
		exportSocketPath, pathErr := admin.CheckpointExportUnixSocketPath(socketPath)
		if pathErr != nil {
			return pathErr
		}
		exporter, configureErr := exportFactory(exportSocketPath, buildVersion, protocol, parsed.timeout)
		if configureErr != nil {
			return fmt.Errorf("configure checkpoint export client: %w", configureErr)
		}
		_, exportErr := exporter.Export(ctx, parsed.requestID, ticket, stdout)
		return exportErr
	}

	payload := parsed.payload
	if parsed.command == admin.CommandUpgradePreflight {
		if parsed.candidateStdin {
			payload, err = readUpgradePayloadFromReader(ctx, stdin)
		} else {
			payload, err = readUpgradePayload(ctx, parsed.candidateFile)
		}
		if err != nil {
			return err
		}
	}
	result, err := client.Execute(ctx, parsed.command, parsed.requestID, payload)
	if err != nil {
		return err
	}
	return writeResult(stdout, result)
}

func parseInvocation(args []string) (invocation, error) {
	if len(args) == 0 || args[0] != "local" {
		return invocation{}, usage(fmt.Errorf("expected local admin command"))
	}
	if err := validateArguments(args); err != nil {
		return invocation{}, usage(err)
	}
	global, rest, err := parseLeadingFlags(args[1:], map[string]struct{}{
		"endpoint": {}, "timeout": {},
	})
	if err != nil {
		return invocation{}, usage(err)
	}
	if len(rest) == 0 {
		return invocation{}, usage(fmt.Errorf("local admin command is required"))
	}
	endpoint := "unix://" + admin.DefaultUnixSocketPath
	if value, present := global["endpoint"]; present {
		endpoint = value
	}
	timeout := defaultLocalTimeout
	if value, present := global["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return invocation{}, usage(fmt.Errorf("local I/O timeout is invalid: %w", err))
		}
	}
	maximumTimeout := 5 * time.Minute
	if len(rest) >= 2 && rest[0] == "checkpoint" && rest[1] == "export" {
		maximumTimeout = time.Hour
	}
	if timeout < time.Second || timeout > maximumTimeout {
		return invocation{}, usage(fmt.Errorf("local I/O timeout must be between 1 second and %s", maximumTimeout))
	}
	parsed := invocation{endpoint: endpoint, timeout: timeout}

	switch rest[0] {
	case "handshake":
		if len(rest) != 1 {
			return invocation{}, usage(fmt.Errorf("handshake does not accept positional arguments"))
		}
		parsed.command = admin.CommandHandshake
		return parsed, nil
	case "checkpoint":
		return parseCheckpoint(parsed, rest[1:])
	case "decommission":
		return parseLocalDecommission(parsed, rest[1:])
	case "gc":
		return parseGC(parsed, rest[1:])
	case "upgrade":
		return parseUpgrade(parsed, rest[1:])
	case "uninstall":
		return parseLocalUninstall(parsed, rest[1:])
	default:
		return invocation{}, usage(fmt.Errorf("unknown local admin command %q", rest[0]))
	}
}

func parseLocalDecommission(parsed invocation, args []string) (invocation, error) {
	commands := map[string]admin.Command{
		"inspect": admin.CommandDecommissionInspect, "prepare": admin.CommandDecommissionPrepare,
		"quiesce": admin.CommandDecommissionQuiesce, "cleanup": admin.CommandDecommissionCleanup,
		"release": admin.CommandDecommissionRelease,
	}
	if len(args) == 0 {
		return invocation{}, usage(fmt.Errorf("decommission phase is required"))
	}
	command, present := commands[args[0]]
	if !present {
		return invocation{}, usage(fmt.Errorf("unknown decommission phase %q", args[0]))
	}
	values, err := parseCommandFlags(args[1:], []string{"request-id", "parent-filesystem-id"})
	if err != nil {
		return invocation{}, usage(err)
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return invocation{}, usage(fmt.Errorf("decommission request ID: %w", err))
	}
	payload := admin.DecommissionParentPayload{ParentFilesystemID: values["parent-filesystem-id"]}
	if err := payload.Validate(); err != nil {
		return invocation{}, usage(fmt.Errorf("decommission parent: %w", err))
	}
	encoded, err := canonicaljson.Marshal(payload)
	if err != nil {
		return invocation{}, err
	}
	parsed.requestID = values["request-id"]
	parsed.payload = encoded
	parsed.command = command
	return parsed, nil
}

func parseLocalUninstall(parsed invocation, args []string) (invocation, error) {
	if len(args) == 0 {
		return invocation{}, usage(fmt.Errorf("local uninstall phase is required"))
	}
	commands := map[string]admin.Command{
		"inspect": admin.CommandUninstallInspect, "prepare": admin.CommandUninstallPrepare,
		"quiesce": admin.CommandUninstallQuiesce, "cleanup": admin.CommandUninstallCleanup,
		"release": admin.CommandUninstallRelease,
	}
	command, present := commands[args[0]]
	if !present {
		return invocation{}, usage(fmt.Errorf("unknown local uninstall phase %q", args[0]))
	}
	values, err := parseCommandFlags(args[1:], []string{"request-id"})
	if err != nil {
		return invocation{}, usage(err)
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return invocation{}, usage(fmt.Errorf("uninstall request ID: %w", err))
	}
	parsed.command = command
	parsed.requestID = values["request-id"]
	return parsed, nil
}

func parseCheckpoint(parsed invocation, args []string) (invocation, error) {
	if len(args) == 0 || (args[0] != "prepare" && args[0] != "export" && args[0] != "resume") {
		return invocation{}, usage(fmt.Errorf("checkpoint requires prepare, export, or resume"))
	}
	required := []string{"request-id"}
	if args[0] == "export" {
		required = append(required, "ticket-stdin")
	}
	values, err := parseCommandFlags(args[1:], required)
	if err != nil {
		return invocation{}, usage(err)
	}
	requestID := values["request-id"]
	if err := volume.ValidateOperationID(requestID); err != nil {
		return invocation{}, usage(fmt.Errorf("checkpoint request ID: %w", err))
	}
	parsed.requestID = requestID
	switch args[0] {
	case "prepare":
		parsed.command = admin.CommandCheckpointPrepare
	case "resume":
		parsed.command = admin.CommandCheckpointResume
	case "export":
		if values["ticket-stdin"] != "true" {
			return invocation{}, usage(fmt.Errorf("--ticket-stdin accepts only true"))
		}
		parsed.checkpointExport = true
		parsed.ticketStdin = true
	}
	return parsed, nil
}

func readCheckpointTicketFromReader(ctx context.Context, reader io.Reader) (recovery.CheckpointTicket, error) {
	if reader == nil {
		return recovery.CheckpointTicket{}, fmt.Errorf("checkpoint export ticket stdin is nil")
	}
	if err := ctx.Err(); err != nil {
		return recovery.CheckpointTicket{}, err
	}
	data, err := io.ReadAll(io.LimitReader(reader, int64(admin.MaxWireMessageBytes)+1))
	if err != nil {
		return recovery.CheckpointTicket{}, fmt.Errorf("read checkpoint export ticket: %w", err)
	}
	if len(data) > admin.MaxWireMessageBytes {
		return recovery.CheckpointTicket{}, fmt.Errorf("checkpoint export ticket exceeds %d bytes", admin.MaxWireMessageBytes)
	}
	ticket, err := recovery.DecodeCheckpointTicket(data)
	if err != nil {
		return recovery.CheckpointTicket{}, fmt.Errorf("decode checkpoint export ticket: %w", err)
	}
	return ticket, nil
}

func parseGC(parsed invocation, args []string) (invocation, error) {
	if len(args) == 0 || args[0] != "submit" {
		return invocation{}, usage(fmt.Errorf("gc requires submit"))
	}
	values, err := parseCommandFlags(args[1:], []string{"request-id", "logical-volume-id", "mode", "expected-state"})
	if err != nil {
		return invocation{}, usage(err)
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return invocation{}, usage(fmt.Errorf("GC request ID: %w", err))
	}
	payload := admin.GCCommandPayload{
		LogicalVolumeID: values["logical-volume-id"],
		Mode:            values["mode"],
		ExpectedState:   volume.AllocationState(values["expected-state"]),
	}
	if err := payload.Validate(); err != nil {
		return invocation{}, usage(fmt.Errorf("GC request: %w", err))
	}
	encoded, err := canonicaljson.Marshal(payload)
	if err != nil {
		return invocation{}, fmt.Errorf("encode GC request: %w", err)
	}
	parsed.command = admin.CommandGCSubmit
	parsed.requestID = values["request-id"]
	parsed.payload = encoded
	return parsed, nil
}

func parseUpgrade(parsed invocation, args []string) (invocation, error) {
	if len(args) == 0 || args[0] != "preflight" {
		return invocation{}, usage(fmt.Errorf("upgrade requires preflight"))
	}
	values, remaining, err := parseLeadingFlags(args[1:], map[string]struct{}{
		"request-id": {}, "candidate-file": {}, "candidate-stdin": {},
	})
	if err != nil {
		return invocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return invocation{}, usage(fmt.Errorf("upgrade preflight does not accept positional arguments"))
	}
	if _, present := values["request-id"]; !present {
		return invocation{}, usage(fmt.Errorf("required admin flag --request-id is missing"))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return invocation{}, usage(fmt.Errorf("upgrade request ID: %w", err))
	}
	candidateFile, hasFile := values["candidate-file"]
	candidateStdin, hasStdin := values["candidate-stdin"]
	if hasFile == hasStdin {
		return invocation{}, usage(fmt.Errorf("upgrade preflight requires exactly one of --candidate-file or --candidate-stdin=true"))
	}
	if hasFile {
		if err := validateCandidateFilePath(candidateFile); err != nil {
			return invocation{}, usage(err)
		}
	} else if candidateStdin != "true" {
		return invocation{}, usage(fmt.Errorf("--candidate-stdin accepts only true"))
	}
	parsed.command = admin.CommandUpgradePreflight
	parsed.requestID = values["request-id"]
	parsed.candidateFile = candidateFile
	parsed.candidateStdin = hasStdin
	return parsed, nil
}

func validateArguments(args []string) error {
	if len(args) > maxCLIArguments {
		return fmt.Errorf("admin invocation contains too many arguments")
	}
	for _, argument := range args {
		if !utf8.ValidString(argument) || len(argument) == 0 || len(argument) > maxCLIArgumentBytes || strings.ContainsAny(argument, "\x00\r\n") {
			return fmt.Errorf("admin argument is not bounded single-line UTF-8")
		}
	}
	return nil
}

func parseCommandFlags(args []string, required []string) (map[string]string, error) {
	allowed := make(map[string]struct{}, len(required))
	for _, name := range required {
		allowed[name] = struct{}{}
	}
	values, remaining, err := parseLeadingFlags(args, allowed)
	if err != nil {
		return nil, err
	}
	if len(remaining) != 0 {
		return nil, fmt.Errorf("admin command does not accept positional arguments")
	}
	for _, name := range required {
		if _, present := values[name]; !present {
			return nil, fmt.Errorf("required admin flag --%s is missing", name)
		}
	}
	return values, nil
}

func parseLeadingFlags(args []string, allowed map[string]struct{}) (map[string]string, []string, error) {
	values := make(map[string]string, len(allowed))
	index := 0
	for index < len(args) {
		argument := args[index]
		if !strings.HasPrefix(argument, "--") {
			break
		}
		nameValue := strings.TrimPrefix(argument, "--")
		name, value, inline := strings.Cut(nameValue, "=")
		if name == "" {
			return nil, nil, fmt.Errorf("empty admin flag is unsupported")
		}
		if _, present := allowed[name]; !present {
			return nil, nil, fmt.Errorf("unknown admin flag --%s", name)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, nil, fmt.Errorf("admin flag --%s is duplicated", name)
		}
		if !inline {
			index++
			if index >= len(args) || strings.HasPrefix(args[index], "--") {
				return nil, nil, fmt.Errorf("admin flag --%s requires a value", name)
			}
			value = args[index]
		}
		if value == "" {
			return nil, nil, fmt.Errorf("admin flag --%s is empty", name)
		}
		values[name] = value
		index++
	}
	return values, args[index:], nil
}

func validateCandidateFilePath(filename string) error {
	if filename == "" || filename == string(filepath.Separator) || !filepath.IsAbs(filename) || filepath.Clean(filename) != filename || strings.ContainsAny(filename, "\x00\r\n") {
		return fmt.Errorf("upgrade candidate file must be a clean absolute non-root path")
	}
	return nil
}

func localSocketPath(endpoint string) (string, error) {
	const prefix = "unix://"
	if !strings.HasPrefix(endpoint, prefix) {
		return "", fmt.Errorf("admin endpoint must use unix:///absolute/path")
	}
	socketPath := strings.TrimPrefix(endpoint, prefix)
	if socketPath == "" || socketPath == string(filepath.Separator) || !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || strings.ContainsAny(socketPath, "\x00\r\n?#") {
		return "", fmt.Errorf("admin endpoint must use a clean absolute Unix socket path without query or fragment")
	}
	return socketPath, nil
}

func readUpgradePayload(ctx context.Context, filename string) (data json.RawMessage, returnErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCandidateFilePath(filename); err != nil {
		return nil, err
	}
	before, err := os.Lstat(filename)
	if err != nil {
		return nil, fmt.Errorf("inspect upgrade candidate: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("upgrade candidate must be an exact regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open upgrade candidate: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat upgrade candidate: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("upgrade candidate changed during open")
	}
	if opened.Size() <= 0 || opened.Size() > admin.MaxWireMessageBytes {
		return nil, fmt.Errorf("upgrade candidate must contain 1 to %d bytes", admin.MaxWireMessageBytes)
	}
	data, err = io.ReadAll(io.LimitReader(file, admin.MaxWireMessageBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read upgrade candidate: %w", err)
	}
	final, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("restat upgrade candidate after read: %w", err)
	}
	if !os.SameFile(opened, final) || opened.Size() != final.Size() || !opened.ModTime().Equal(final.ModTime()) || final.Size() != int64(len(data)) {
		return nil, fmt.Errorf("upgrade candidate changed during read")
	}
	if len(data) == 0 || len(data) > admin.MaxWireMessageBytes {
		return nil, fmt.Errorf("upgrade candidate must contain 1 to %d bytes", admin.MaxWireMessageBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encodeUpgradeCandidatePayload(data)
}

func readUpgradePayloadFromReader(ctx context.Context, reader io.Reader) (json.RawMessage, error) {
	if reader == nil {
		return nil, fmt.Errorf("upgrade candidate stdin is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(reader, admin.MaxWireMessageBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read upgrade candidate stdin: %w", err)
	}
	if len(data) == 0 || len(data) > admin.MaxWireMessageBytes {
		return nil, fmt.Errorf("upgrade candidate stdin must contain 1 to %d bytes", admin.MaxWireMessageBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var payload admin.UpgradePreflightPayload
	if err := strictjson.Decode(data, &payload); err != nil {
		return nil, fmt.Errorf("decode streamed upgrade preflight payload: %w", err)
	}
	if err := admin.ValidateUpgradeCandidate(payload.Candidate); err != nil {
		return nil, fmt.Errorf("validate streamed upgrade candidate: %w", err)
	}
	encoded, err := canonicaljson.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode streamed upgrade preflight payload: %w", err)
	}
	if len(encoded) > admin.MaxWireMessageBytes {
		return nil, fmt.Errorf("upgrade preflight payload exceeds %d bytes", admin.MaxWireMessageBytes)
	}
	return encoded, nil
}

func encodeUpgradeCandidatePayload(data []byte) (json.RawMessage, error) {
	var candidate admin.UpgradeCandidate
	if err := strictjson.Decode(data, &candidate); err != nil {
		return nil, fmt.Errorf("decode upgrade candidate: %w", err)
	}
	if err := admin.ValidateUpgradeCandidate(candidate); err != nil {
		return nil, fmt.Errorf("validate upgrade candidate: %w", err)
	}
	encoded, err := canonicaljson.Marshal(admin.UpgradePreflightPayload{Candidate: candidate})
	if err != nil {
		return nil, fmt.Errorf("encode upgrade preflight payload: %w", err)
	}
	if len(encoded) > admin.MaxWireMessageBytes {
		return nil, fmt.Errorf("upgrade preflight payload exceeds %d bytes", admin.MaxWireMessageBytes)
	}
	return encoded, nil
}

func writeResult(writer io.Writer, result []byte) error {
	if len(result) == 0 {
		return fmt.Errorf("admin result is empty")
	}
	line := make([]byte, len(result)+1)
	copy(line, result)
	line[len(result)] = '\n'
	for len(line) > 0 {
		written, err := writer.Write(line)
		if err != nil {
			return fmt.Errorf("write admin result: %w", err)
		}
		if written <= 0 || written > len(line) {
			return fmt.Errorf("write admin result: invalid write count %d", written)
		}
		line = line[written:]
	}
	return nil
}

func usage(err error) error {
	return &usageError{err: err}
}
