package scaleway

import (
	"errors"
	"testing"
)

func TestFilesystemStatusMatrix(t *testing.T) {
	tests := []struct {
		raw  string
		want FilesystemStatus
		err  error
	}{
		{raw: "available", want: FilesystemAvailable},
		{raw: "creating", want: FilesystemCreating, err: ErrUnavailable},
		{raw: "updating", want: FilesystemUpdating, err: ErrUnavailable},
		{raw: "error", want: FilesystemError, err: ErrFailedPrecondition},
		{raw: "future", want: FilesystemUnknown, err: ErrUnavailable},
		{raw: "", want: FilesystemUnknown, err: ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			status := NormalizeFilesystemStatus(test.raw)
			if status != test.want {
				t.Fatalf("NormalizeFilesystemStatus() = %q, want %q", status, test.want)
			}
			err := status.PermitNewMutation()
			if test.err == nil && err != nil {
				t.Fatalf("PermitNewMutation() error = %v", err)
			}
			if test.err != nil && !errors.Is(err, test.err) {
				t.Fatalf("PermitNewMutation() error = %v, want %v", err, test.err)
			}
		})
	}
}

func TestInstanceStateMatrix(t *testing.T) {
	tests := []struct {
		raw    string
		state  InstanceState
		attach error
		detach error
		fenced bool
	}{
		{raw: "running", state: InstanceRunning},
		{raw: "starting", state: InstanceStarting, attach: ErrUnavailable, detach: ErrUnavailable},
		{raw: "stopping", state: InstanceStopping, attach: ErrUnavailable, detach: ErrUnavailable},
		{raw: "locked", state: InstanceLocked, attach: ErrUnavailable, detach: ErrUnavailable},
		{raw: "stopped", state: InstanceStopped, attach: ErrFailedPrecondition, fenced: true},
		{raw: "stopped in place", state: InstanceStoppedInPlace, attach: ErrFailedPrecondition, fenced: true},
		{raw: "future", state: InstanceUnknown, attach: ErrUnavailable, detach: ErrUnavailable},
	}
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			state := NormalizeInstanceState(test.raw)
			if state != test.state || state.Fenced() != test.fenced {
				t.Fatalf("state/fenced = %q/%v, want %q/%v", state, state.Fenced(), test.state, test.fenced)
			}
			err := state.PermitNewAttachment()
			if test.attach == nil && err != nil {
				t.Fatalf("PermitNewAttachment() error = %v", err)
			}
			if test.attach != nil && !errors.Is(err, test.attach) {
				t.Fatalf("PermitNewAttachment() error = %v, want %v", err, test.attach)
			}
			detachErr := state.PermitOfflineDetach()
			if test.detach == nil && detachErr != nil {
				t.Fatalf("PermitOfflineDetach() error = %v", detachErr)
			}
			if test.detach != nil && !errors.Is(detachErr, test.detach) {
				t.Fatalf("PermitOfflineDetach() error = %v, want %v", detachErr, test.detach)
			}
		})
	}
}

func TestServerFilesystemStateMatrix(t *testing.T) {
	for raw, want := range map[string]ServerFilesystemState{
		"attaching": ServerFilesystemAttaching,
		"available": ServerFilesystemAvailable,
		"detaching": ServerFilesystemDetaching,
		"future":    ServerFilesystemUnknown,
		"":          ServerFilesystemUnknown,
	} {
		t.Run(raw, func(t *testing.T) {
			if got := NormalizeServerFilesystemState(raw); got != want {
				t.Fatalf("NormalizeServerFilesystemState(%q) = %q, want %q", raw, got, want)
			}
		})
	}
}
