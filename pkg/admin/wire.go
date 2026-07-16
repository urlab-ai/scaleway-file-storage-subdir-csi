package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const (
	// WireSchemaVersionV1 is the closed controller-local admin envelope schema.
	WireSchemaVersionV1 = "1"
	// MaxWireMessageBytes bounds every framed request and response before JSON
	// allocation. Command-specific handlers may impose smaller payload limits.
	MaxWireMessageBytes = 2 << 20
	maxWireErrorBytes   = 1024
)

// Command is one closed v1 controller-local admin operation.
type Command string

const (
	CommandHandshake           Command = "handshake"
	CommandCheckpointPrepare   Command = "checkpoint.prepare"
	CommandCheckpointResume    Command = "checkpoint.resume"
	CommandDecommissionInspect Command = "decommission.inspect"
	CommandDecommissionPrepare Command = "decommission.prepare"
	CommandDecommissionQuiesce Command = "decommission.quiesce"
	CommandDecommissionCleanup Command = "decommission.cleanup"
	CommandDecommissionRelease Command = "decommission.release"
	CommandGCSubmit            Command = "gc.submit"
	CommandUpgradePreflight    Command = "upgrade.preflight"
	CommandUninstallInspect    Command = "uninstall.inspect"
	CommandUninstallPrepare    Command = "uninstall.prepare"
	CommandUninstallQuiesce    Command = "uninstall.quiesce"
	CommandUninstallCleanup    Command = "uninstall.cleanup"
	CommandUninstallRelease    Command = "uninstall.release"
)

// WireRequest is one request on a single-use local connection. Handshake and
// mutation envelopes are mutually exclusive. The mutation payload is retained
// as strict JSON so the command owner, rather than the transport, validates its
// complete closed schema.
type WireRequest struct {
	SchemaVersion string            `json:"schemaVersion"`
	Command       Command           `json:"command"`
	Handshake     *HandshakeRequest `json:"handshake,omitempty"`
	Mutation      *MutationRequest  `json:"mutation,omitempty"`
	Payload       json.RawMessage   `json:"payload,omitempty"`
}

// Validate rejects unknown commands and mixed or missing envelopes before a
// handler can observe the request.
func (request WireRequest) Validate() error {
	if request.SchemaVersion != WireSchemaVersionV1 {
		return fmt.Errorf("admin wire schema version %q is unsupported", request.SchemaVersion)
	}
	if !request.Command.valid() {
		return fmt.Errorf("admin command %q is unsupported", request.Command)
	}
	if request.Command == CommandHandshake {
		if request.Handshake == nil || request.Mutation != nil || len(request.Payload) != 0 {
			return fmt.Errorf("handshake command requires only the handshake envelope")
		}
		return validateHandshakeRequest(*request.Handshake)
	}
	if request.Handshake != nil || request.Mutation == nil {
		return fmt.Errorf("mutation command requires only the mutation envelope")
	}
	if err := request.Mutation.Validate(); err != nil {
		return err
	}
	if request.Command.requiresPayload() {
		if len(request.Payload) == 0 {
			return fmt.Errorf("admin command %q requires a payload", request.Command)
		}
		if err := validateJSONObject(request.Payload); err != nil {
			return fmt.Errorf("admin command %q payload: %w", request.Command, err)
		}
		return nil
	}
	if len(request.Payload) != 0 {
		return fmt.Errorf("admin command %q does not accept a payload", request.Command)
	}
	return nil
}

func validateHandshakeRequest(request HandshakeRequest) error {
	if request.AdminVersion == "" || len(request.AdminVersion) > 128 || !utf8.ValidString(request.AdminVersion) || strings.ContainsAny(request.AdminVersion, "\x00\r\n") {
		return fmt.Errorf("admin version must be valid single-line UTF-8 containing 1 to 128 bytes")
	}
	if request.Protocol.Major == 0 {
		return fmt.Errorf("admin protocol major must be positive")
	}
	return nil
}

func (command Command) valid() bool {
	switch command {
	case CommandHandshake, CommandCheckpointPrepare, CommandCheckpointResume,
		CommandDecommissionInspect, CommandDecommissionPrepare, CommandDecommissionQuiesce,
		CommandDecommissionCleanup, CommandDecommissionRelease,
		CommandGCSubmit, CommandUpgradePreflight, CommandUninstallInspect, CommandUninstallPrepare,
		CommandUninstallQuiesce, CommandUninstallCleanup, CommandUninstallRelease:
		return true
	default:
		return false
	}
}

func (command Command) requiresPayload() bool {
	return command == CommandGCSubmit || command == CommandUpgradePreflight ||
		command == CommandDecommissionInspect || command == CommandDecommissionPrepare ||
		command == CommandDecommissionQuiesce || command == CommandDecommissionCleanup || command == CommandDecommissionRelease
}

// ErrorCode is the stable machine-readable classification returned to the
// operator client. Internal handler errors are deliberately redacted.
type ErrorCode string

const (
	ErrorInvalidArgument    ErrorCode = "invalid_argument"
	ErrorFailedPrecondition ErrorCode = "failed_precondition"
	ErrorUnavailable        ErrorCode = "unavailable"
	ErrorInternal           ErrorCode = "internal"
)

// WireError is the bounded error envelope returned instead of a result.
type WireError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// WireResponse is the closed response envelope. Invalid framing or undecodable
// requests use an empty command because no untrusted correlation value can be
// echoed safely.
type WireResponse struct {
	SchemaVersion string             `json:"schemaVersion"`
	Command       Command            `json:"command"`
	OK            bool               `json:"ok"`
	Handshake     *HandshakeResponse `json:"handshake,omitempty"`
	Result        json.RawMessage    `json:"result,omitempty"`
	Error         *WireError         `json:"error,omitempty"`
}

// Validate checks response exclusivity, correlation, and bounded error/result
// data before the client trusts it.
func (response WireResponse) Validate() error {
	if response.SchemaVersion != WireSchemaVersionV1 {
		return fmt.Errorf("admin response schema version %q is unsupported", response.SchemaVersion)
	}
	if response.Command != "" && !response.Command.valid() {
		return fmt.Errorf("admin response command %q is unsupported", response.Command)
	}
	if response.OK {
		if response.Command == "" || response.Error != nil {
			return fmt.Errorf("successful admin response has no command or contains an error")
		}
		if response.Command == CommandHandshake {
			if response.Handshake == nil || len(response.Result) != 0 {
				return fmt.Errorf("successful handshake response has an invalid result envelope")
			}
			return response.Handshake.Validate()
		}
		if response.Handshake != nil || len(response.Result) == 0 {
			return fmt.Errorf("successful mutation response has an invalid result envelope")
		}
		return validateJSONObject(response.Result)
	}
	if response.Handshake != nil || len(response.Result) != 0 || response.Error == nil {
		return fmt.Errorf("failed admin response has an invalid error envelope")
	}
	return response.Error.Validate()
}

// Validate checks that an error is stable, bounded, and safe to render as one
// terminal line.
func (wireError WireError) Validate() error {
	switch wireError.Code {
	case ErrorInvalidArgument, ErrorFailedPrecondition, ErrorUnavailable, ErrorInternal:
	default:
		return fmt.Errorf("admin error code %q is unsupported", wireError.Code)
	}
	if wireError.Message == "" || len(wireError.Message) > maxWireErrorBytes || !utf8.ValidString(wireError.Message) || strings.ContainsAny(wireError.Message, "\x00\r\n") {
		return fmt.Errorf("admin error message must be valid single-line UTF-8 containing 1 to %d bytes", maxWireErrorBytes)
	}
	return nil
}

// OperationError lets a command handler deliberately expose one bounded,
// classified operator-facing failure. Unclassified errors are not exposed by
// the transport and become a generic internal response.
type OperationError struct {
	Code ErrorCode
	Err  error
}

// Error implements error.
func (operationError *OperationError) Error() string {
	if operationError == nil || operationError.Err == nil {
		return "admin operation failed"
	}
	return operationError.Err.Error()
}

// Unwrap exposes the local cause for logging and tests; the remote response
// contains only the validated bounded message.
func (operationError *OperationError) Unwrap() error {
	if operationError == nil {
		return nil
	}
	return operationError.Err
}

// NewOperationError constructs a deliberate operator-facing handler failure.
func NewOperationError(code ErrorCode, err error) error {
	if err == nil {
		return fmt.Errorf("admin operation error cause is nil")
	}
	wireError := WireError{Code: code, Message: err.Error()}
	if validateErr := wireError.Validate(); validateErr != nil {
		return fmt.Errorf("invalid admin operation error: %w", validateErr)
	}
	return &OperationError{Code: code, Err: err}
}

// RemoteError is a validated error returned by the controller-local endpoint.
type RemoteError struct {
	Command Command
	Code    ErrorCode
	Message string
}

// Error implements error without adding unbounded context.
func (remoteError *RemoteError) Error() string {
	return fmt.Sprintf("admin command %q failed (%s): %s", remoteError.Command, remoteError.Code, remoteError.Message)
}

// OperationHandler executes only requests already validated and negotiated by
// the wire server. It must strictly decode its command payload and return a JSON
// object result. Durable mutation authorization remains in the command owner.
type OperationHandler interface {
	HandleAdminOperation(ctx context.Context, command Command, request MutationRequest, payload json.RawMessage) (json.RawMessage, error)
}

func validateJSONObject(data []byte) error {
	if len(data) == 0 || len(data) > MaxWireMessageBytes {
		return fmt.Errorf("JSON object must contain 1 to %d bytes", MaxWireMessageBytes)
	}
	var object map[string]json.RawMessage
	if err := strictjson.Decode(data, &object); err != nil {
		return err
	}
	if object == nil {
		return fmt.Errorf("JSON payload must be an object")
	}
	return nil
}

func responseError(command Command, code ErrorCode, message string) WireResponse {
	if !command.valid() {
		command = ""
	}
	wireError := &WireError{Code: code, Message: boundedWireMessage(message)}
	if err := wireError.Validate(); err != nil {
		wireError = &WireError{Code: ErrorInternal, Message: "admin operation failed"}
	}
	return WireResponse{SchemaVersion: WireSchemaVersionV1, Command: command, OK: false, Error: wireError}
}

func boundedWireMessage(message string) string {
	message = strings.ToValidUTF8(message, "?")
	message = strings.NewReplacer("\x00", " ", "\r", " ", "\n", " ").Replace(message)
	message = strings.TrimSpace(message)
	for len(message) > maxWireErrorBytes {
		_, size := utf8.DecodeLastRuneInString(message)
		message = message[:len(message)-size]
	}
	if message == "" {
		return "admin operation failed"
	}
	return message
}

func responseForHandlerError(command Command, err error) WireResponse {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return responseError(command, ErrorUnavailable, "admin operation was cancelled or exceeded its deadline")
	}
	var operationError *OperationError
	if errors.As(err, &operationError) && operationError != nil {
		return responseError(command, operationError.Code, operationError.Error())
	}
	return responseError(command, ErrorInternal, "admin operation failed")
}
