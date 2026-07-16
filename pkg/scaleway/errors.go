package scaleway

import "errors"

var (
	ErrInvalidArgument    = errors.New("invalid provider argument")
	ErrNotFound           = errors.New("provider resource not found")
	ErrPermissionDenied   = errors.New("provider permission denied")
	ErrResourceExhausted  = errors.New("provider resource exhausted")
	ErrFailedPrecondition = errors.New("provider precondition failed")
	// ErrConflict is an ambiguous provider mutation result. Unlike a proven
	// precondition failure, callers must reread authoritative state before
	// deciding whether the mutation committed.
	ErrConflict         = errors.New("provider mutation conflict")
	ErrUnavailable      = errors.New("provider unavailable or result ambiguous")
	ErrDeadlineExceeded = errors.New("provider operation deadline exceeded")
	// ErrUnknownAttachmentNode classifies a parent attachment whose Instance
	// is not authorized by the current Kubernetes Node/CSINode inventory.
	ErrUnknownAttachmentNode = errors.New("unknown attachment node")
	// ErrForeignAttachmentType classifies an attachment target type that v1
	// does not understand or authorize.
	ErrForeignAttachmentType = errors.New("foreign attachment resource type")
	// ErrAttachmentInventoryDisagreement classifies conflicting complete
	// regional, filesystem-metadata, or Instance attachment evidence.
	ErrAttachmentInventoryDisagreement = errors.New("attachment inventory disagreement")
)
