package csiadapter

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/mount"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const maxStatusMessageBytes = 1024

func statusError(code codes.Code, err error) error {
	if err == nil {
		return nil
	}
	return status.Error(code, boundedStatusMessage(err.Error()))
}

func mapCoreError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return statusError(codes.Canceled, err)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, scaleway.ErrDeadlineExceeded):
		return statusError(codes.DeadlineExceeded, err)
	case errors.Is(err, k8s.ErrNotFound), errors.Is(err, scaleway.ErrNotFound):
		return statusError(codes.NotFound, err)
	case errors.Is(err, k8s.ErrForbidden), errors.Is(err, scaleway.ErrPermissionDenied):
		return statusError(codes.PermissionDenied, err)
	case errors.Is(err, pool.ErrNoLogicalCapacity), errors.Is(err, pool.ErrPhysicalCapacityExhausted), errors.Is(err, scaleway.ErrResourceExhausted):
		return statusError(codes.ResourceExhausted, err)
	case errors.Is(err, volume.ErrCapacityOutOfRange):
		return statusError(codes.OutOfRange, err)
	case errors.Is(err, driver.ErrCapabilityMismatch), errors.Is(err, driver.ErrStagingPrerequisite),
		errors.Is(err, driver.ErrNodePrecondition), errors.Is(err, safety.ErrUnsafeLivePath):
		return statusError(codes.FailedPrecondition, err)
	case errors.Is(err, k8s.ErrAlreadyExists), errors.Is(err, volume.ErrCreateReplayIncompatible),
		errors.Is(err, driver.ErrNamePermanentlyReserved), errors.Is(err, mount.ErrMountConflict),
		errors.Is(err, safety.ErrTargetConflict):
		return statusError(codes.AlreadyExists, err)
	case errors.Is(err, k8s.ErrConflict), errors.Is(err, driver.ErrDeletionInProgress):
		return statusError(codes.Aborted, err)
	case errors.Is(err, driver.ErrVolumeInUse), errors.Is(err, driver.ErrPublishedFenceBlocked),
		errors.Is(err, driver.ErrVolumeNotReady), errors.Is(err, driver.ErrSingleNodeConflict),
		errors.Is(err, driver.ErrNodePublicationFenceMissing), errors.Is(err, driver.ErrSingleNodeTargetConflict),
		errors.Is(err, mount.ErrForeignMount), errors.Is(err, mount.ErrStackedMount),
		errors.Is(err, scaleway.ErrFailedPrecondition), errors.Is(err, coordination.ErrMutationQuiesced),
		errors.Is(err, coordination.ErrLeadershipNotActive):
		return statusError(codes.FailedPrecondition, err)
	case errors.Is(err, k8s.ErrUnavailable), errors.Is(err, scaleway.ErrUnavailable), errors.Is(err, scaleway.ErrConflict),
		errors.Is(err, pool.ErrParentInventoryUnavailable), errors.Is(err, pool.ErrNoFreshPhysicalSpace),
		errors.Is(err, pool.ErrNoNodeCompatibleParent), errors.Is(err, coordination.ErrLeaseRenewalDeadline),
		errors.Is(err, driver.ErrReservationUnresolved), errors.Is(err, mount.ErrMountUnavailable):
		return statusError(codes.Unavailable, err)
	case errors.Is(err, scaleway.ErrInvalidArgument), errors.Is(err, volume.ErrInvalidHandle),
		errors.Is(err, volume.ErrForeignHandle), errors.Is(err, volume.ErrInvalidContext),
		errors.Is(err, volume.ErrContextMismatch), errors.Is(err, driver.ErrInvalidNodePath):
		return statusError(codes.InvalidArgument, err)
	default:
		return statusError(codes.Internal, err)
	}
}

func boundedStatusMessage(message string) string {
	if !utf8.ValidString(message) {
		return "operation failed with invalid UTF-8 diagnostic"
	}
	message = strings.NewReplacer("\x00", " ", "\r", " ", "\n", " ").Replace(message)
	if len(message) <= maxStatusMessageBytes {
		return message
	}
	message = message[:maxStatusMessageBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}
