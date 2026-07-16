package scaleway

import "fmt"

// FilesystemStatus is the closed normalized File Storage status matrix.
type FilesystemStatus string

const (
	FilesystemAvailable FilesystemStatus = "available"
	FilesystemCreating  FilesystemStatus = "creating"
	FilesystemUpdating  FilesystemStatus = "updating"
	FilesystemError     FilesystemStatus = "error"
	FilesystemUnknown   FilesystemStatus = "unknown"
)

// NormalizeFilesystemStatus maps only SDK values qualified by the v1 contract.
func NormalizeFilesystemStatus(raw string) FilesystemStatus {
	switch raw {
	case "available":
		return FilesystemAvailable
	case "creating":
		return FilesystemCreating
	case "updating":
		return FilesystemUpdating
	case "error":
		return FilesystemError
	default:
		return FilesystemUnknown
	}
}

// PermitNewMutation applies the normative provider-status matrix.
func (status FilesystemStatus) PermitNewMutation() error {
	switch status {
	case FilesystemAvailable:
		return nil
	case FilesystemCreating, FilesystemUpdating, FilesystemUnknown:
		return fmt.Errorf("filesystem status %q: %w", status, ErrUnavailable)
	case FilesystemError:
		return fmt.Errorf("filesystem status %q: %w", status, ErrFailedPrecondition)
	default:
		return fmt.Errorf("filesystem status %q: %w", status, ErrUnavailable)
	}
}

// InstanceState is the closed normalized process/fencing state matrix.
type InstanceState string

const (
	InstanceRunning        InstanceState = "running"
	InstanceStarting       InstanceState = "starting"
	InstanceStopping       InstanceState = "stopping"
	InstanceLocked         InstanceState = "locked"
	InstanceStopped        InstanceState = "stopped"
	InstanceStoppedInPlace InstanceState = "stopped in place"
	InstanceUnknown        InstanceState = "unknown"
)

// NormalizeInstanceState maps only states tested by the pinned v1 SDK contract.
func NormalizeInstanceState(raw string) InstanceState {
	switch raw {
	case "running":
		return InstanceRunning
	case "starting":
		return InstanceStarting
	case "stopping":
		return InstanceStopping
	case "locked":
		return InstanceLocked
	case "stopped":
		return InstanceStopped
	case "stopped in place", "stopped_in_place":
		return InstanceStoppedInPlace
	default:
		return InstanceUnknown
	}
}

// PermitNewAttachment applies the new-publish Instance matrix.
func (state InstanceState) PermitNewAttachment() error {
	switch state {
	case InstanceRunning:
		return nil
	case InstanceStarting, InstanceStopping, InstanceLocked, InstanceUnknown:
		return fmt.Errorf("instance state %q: %w", state, ErrUnavailable)
	case InstanceStopped, InstanceStoppedInPlace:
		return fmt.Errorf("instance state %q: %w", state, ErrFailedPrecondition)
	default:
		return fmt.Errorf("instance state %q: %w", state, ErrUnavailable)
	}
}

// Fenced reports whether provider state conclusively prevents the old process
// from serving a mount. NotFound is represented by a separate conclusive API
// result because an orphan regional attachment must still be checked.
func (state InstanceState) Fenced() bool {
	return state == InstanceStopped || state == InstanceStoppedInPlace
}

// PermitOfflineDetach accepts only stable, understood Instance states for the
// explicit decommission or uninstall workflows. Running is permitted because
// those workflows separately prove the driver processes and mounts are gone.
func (state InstanceState) PermitOfflineDetach() error {
	switch state {
	case InstanceRunning, InstanceStopped, InstanceStoppedInPlace:
		return nil
	case InstanceStarting, InstanceStopping, InstanceLocked, InstanceUnknown:
		return fmt.Errorf("instance state %q: %w", state, ErrUnavailable)
	default:
		return fmt.Errorf("instance state %q: %w", state, ErrUnavailable)
	}
}

// ServerFilesystemState is the normalized Instance attachment state.
type ServerFilesystemState string

const (
	ServerFilesystemAttaching ServerFilesystemState = "attaching"
	ServerFilesystemAvailable ServerFilesystemState = "available"
	ServerFilesystemDetaching ServerFilesystemState = "detaching"
	ServerFilesystemUnknown   ServerFilesystemState = "unknown"
)

// NormalizeServerFilesystemState closes future or unreadable SDK values.
func NormalizeServerFilesystemState(raw string) ServerFilesystemState {
	switch raw {
	case "attaching":
		return ServerFilesystemAttaching
	case "available":
		return ServerFilesystemAvailable
	case "detaching":
		return ServerFilesystemDetaching
	default:
		return ServerFilesystemUnknown
	}
}
