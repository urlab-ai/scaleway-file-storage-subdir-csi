package admin

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	// ProtocolMajorV1 is the only released major protocol understood by v1.
	ProtocolMajorV1 uint32 = 1
	// ProtocolMinorV1 is the initial v1 minor protocol.
	ProtocolMinorV1 uint32 = 0
)

// ProtocolVersion identifies one admin wire contract.
type ProtocolVersion struct {
	Major uint32 `json:"major"`
	Minor uint32 `json:"minor"`
}

// HandshakeRequest is sent before every read-write operator workflow.
type HandshakeRequest struct {
	AdminVersion string          `json:"adminVersion"`
	Protocol     ProtocolVersion `json:"protocol"`
}

// HandshakeResponse declares the exact controller build and accepted minor
// interval for one major protocol.
type HandshakeResponse struct {
	DriverVersion string `json:"driverVersion"`
	ProtocolMajor uint32 `json:"protocolMajor"`
	MinimumMinor  uint32 `json:"minimumMinor"`
	MaximumMinor  uint32 `json:"maximumMinor"`
}

// Validate checks a bounded server compatibility declaration.
func (response HandshakeResponse) Validate() error {
	if response.DriverVersion == "" || len(response.DriverVersion) > 128 || !utf8.ValidString(response.DriverVersion) || strings.ContainsAny(response.DriverVersion, "\x00\r\n") {
		return fmt.Errorf("driver version must be valid single-line UTF-8 containing 1 to 128 bytes")
	}
	if response.ProtocolMajor == 0 || response.MinimumMinor > response.MaximumMinor {
		return fmt.Errorf("admin protocol compatibility range is invalid")
	}
	return nil
}

// Negotiate rejects an unknown major or a minor outside the server's explicit
// compatibility range before any operator mutation begins.
func Negotiate(request HandshakeRequest, response HandshakeResponse) error {
	if err := validateHandshakeRequest(request); err != nil {
		return err
	}
	if err := response.Validate(); err != nil {
		return err
	}
	if request.Protocol.Major != response.ProtocolMajor {
		return fmt.Errorf("admin protocol major %d is incompatible with controller major %d", request.Protocol.Major, response.ProtocolMajor)
	}
	if request.Protocol.Minor < response.MinimumMinor || request.Protocol.Minor > response.MaximumMinor {
		return fmt.Errorf("admin protocol minor %d is outside controller range [%d,%d]", request.Protocol.Minor, response.MinimumMinor, response.MaximumMinor)
	}
	return nil
}

// MutationRequest is the shared bounded identity envelope for GC, checkpoint,
// upgrade, and uninstall operations. Command-specific payloads remain separate.
type MutationRequest struct {
	RequestID    string          `json:"requestID"`
	AdminVersion string          `json:"adminVersion"`
	Protocol     ProtocolVersion `json:"protocol"`
}

// Validate checks the request identity before command-specific validation.
func (request MutationRequest) Validate() error {
	if err := volume.ValidateOperationID(request.RequestID); err != nil {
		return err
	}
	return validateHandshakeRequest(HandshakeRequest{AdminVersion: request.AdminVersion, Protocol: request.Protocol})
}
