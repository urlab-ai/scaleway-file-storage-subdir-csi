package scaleway

import (
	"context"
	"errors"
	"testing"
	"time"
)

type postDetachObservationFaultAPI struct {
	API
	fault        error
	detachCalled bool
}

func (api *postDetachObservationFaultAPI) DetachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	err := api.API.DetachServerFilesystem(ctx, zone, serverID, filesystemID)
	api.detachCalled = true
	return err
}

func (api *postDetachObservationFaultAPI) GetFilesystem(ctx context.Context, region, filesystemID string) (Filesystem, error) {
	if api.detachCalled && api.fault != nil {
		err := api.fault
		api.fault = nil
		return Filesystem{}, err
	}
	return api.API.GetFilesystem(ctx, region, filesystemID)
}

func detachRequest() DetachRequest {
	request := attachRequest()
	return DetachRequest{
		Region: request.Region, ProjectID: request.ProjectID, FilesystemID: request.FilesystemID,
		Targets: []Target{request.Target},
	}
}

func testDetachmentManager(t *testing.T, api API, deadline time.Duration) *DetachmentManager {
	t.Helper()
	manager, err := NewDetachmentManager(api, &advancingClock{now: time.Unix(0, 0)}, fixedJitter{}, AttachConfig{
		Deadline: deadline, InitialBackoff: time.Second, MaximumBackoff: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDetachmentManager() error = %v", err)
	}
	return manager
}

func configureDetachTransition(api *FakeAPI) {
	request := detachRequest()
	api.FilesystemSequences[request.Region+"/"+request.FilesystemID] = []Filesystem{parentMetadata(1), parentMetadata(0)}
	api.PageSequences[request.FilesystemID+"/"] = []AttachmentPage{{Attachments: []Attachment{targetAttachment()}}, {}}
	api.ServerSequences[request.Targets[0].Zone+"/"+request.Targets[0].ServerID] = []Server{
		targetServer(ServerFilesystemAvailable, true), targetServer(ServerFilesystemUnknown, false),
	}
}

func TestEnsureDetachedCallsExactTargetAndProvesBothInventoriesAbsent(t *testing.T) {
	api := NewFakeAPI()
	configureDetachTransition(api)
	request := detachRequest()
	if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), request); err != nil {
		t.Fatalf("EnsureDetached() error = %v", err)
	}
	_, _, detaches := api.SnapshotRequests()
	if len(detaches) != 1 || detaches[0].Zone != request.Targets[0].Zone || detaches[0].ServerID != request.Targets[0].ServerID || detaches[0].FilesystemID != request.FilesystemID {
		t.Fatalf("detach calls = %#v", detaches)
	}
}

func TestEnsureDetachedResolvesAmbiguousProviderResultByReread(t *testing.T) {
	api := NewFakeAPI()
	configureDetachTransition(api)
	api.InjectFault("detach", ErrUnavailable)
	if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), detachRequest()); err != nil {
		t.Fatalf("EnsureDetached(ambiguous committed) error = %v", err)
	}
}

func TestEnsureDetachedRetriesTransientInventoryWithoutRepeatingDetach(t *testing.T) {
	t.Run("before mutation", func(t *testing.T) {
		api := NewFakeAPI()
		configureDetachTransition(api)
		api.InjectFault("get-filesystem", ErrUnavailable)
		if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), detachRequest()); err != nil {
			t.Fatalf("EnsureDetached(transient initial inventory) error = %v", err)
		}
		_, _, detaches := api.SnapshotRequests()
		if len(detaches) != 1 {
			t.Fatalf("detach calls = %d, want exactly 1", len(detaches))
		}
	})

	t.Run("after mutation", func(t *testing.T) {
		base := NewFakeAPI()
		configureDetachTransition(base)
		api := &postDetachObservationFaultAPI{API: base, fault: ErrUnavailable}
		if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), detachRequest()); err != nil {
			t.Fatalf("EnsureDetached(transient post-detach inventory) error = %v", err)
		}
		_, _, detaches := base.SnapshotRequests()
		if len(detaches) != 1 {
			t.Fatalf("detach calls = %d, want exactly 1", len(detaches))
		}
	})
}

func TestEnsureDetachedReturnsDefiniteMutationFailureAfterMandatoryReread(t *testing.T) {
	api := NewFakeAPI()
	request := detachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
	api.Servers[request.Targets[0].Zone+"/"+request.Targets[0].ServerID] = targetServer(ServerFilesystemAvailable, true)
	api.InjectFault("detach", ErrPermissionDenied)
	operationClock := &advancingClock{now: time.Unix(0, 0)}
	manager, err := NewDetachmentManager(api, operationClock, fixedJitter{}, AttachConfig{
		Deadline: time.Minute, InitialBackoff: time.Second, MaximumBackoff: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewDetachmentManager() error = %v", err)
	}
	err = manager.EnsureDetached(context.Background(), request)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("EnsureDetached(permission denied) error = %v", err)
	}
	if !operationClock.Now().Equal(time.Unix(0, 0)) {
		t.Fatalf("definite failure unexpectedly entered polling at %v", operationClock.Now())
	}
	_, _, detaches := api.SnapshotRequests()
	if len(detaches) != 1 {
		t.Fatalf("detach calls = %d, want exactly 1", len(detaches))
	}
}

func TestEnsureDetachedRejectsUnauthorizedOrContradictoryInventoryWithoutMutation(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		api := NewFakeAPI()
		request := detachRequest()
		filesystem := parentMetadata(1)
		api.Filesystems[request.Region+"/"+request.FilesystemID] = filesystem
		foreign := targetAttachment()
		foreign.ResourceID = "55555555-5555-4555-8555-555555555555"
		foreign.Zone = "fr-par-2"
		api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{foreign}}
		if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), request); !errors.Is(err, ErrFailedPrecondition) {
			t.Fatalf("EnsureDetached(foreign attachment) error = %v", err)
		}
		_, _, detaches := api.SnapshotRequests()
		if len(detaches) != 0 {
			t.Fatalf("unsafe detach calls = %#v", detaches)
		}
	})

	t.Run("inventory disagreement", func(t *testing.T) {
		api := NewFakeAPI()
		request := detachRequest()
		api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
		api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
		api.Servers[request.Targets[0].Zone+"/"+request.Targets[0].ServerID] = targetServer(ServerFilesystemUnknown, false)
		if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(context.Background(), request); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("EnsureDetached(disagreement) error = %v", err)
		}
		_, _, detaches := api.SnapshotRequests()
		if len(detaches) != 0 {
			t.Fatalf("contradictory inventory detach calls = %#v", detaches)
		}
	})
}

func TestEnsureDetachedDeadlineIsBoundedAndCancellationIsHonored(t *testing.T) {
	api := NewFakeAPI()
	request := detachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
	api.Servers[request.Targets[0].Zone+"/"+request.Targets[0].ServerID] = targetServer(ServerFilesystemDetaching, true)
	if err := testDetachmentManager(t, api, 3*time.Second).EnsureDetached(context.Background(), request); !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("EnsureDetached(deadline) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := testDetachmentManager(t, api, time.Minute).EnsureDetached(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureDetached(cancelled) error = %v", err)
	}
}

func TestDetachRequestRejectsDuplicateInstanceIdentity(t *testing.T) {
	request := detachRequest()
	duplicate := request.Targets[0]
	duplicate.Zone = "fr-par-2"
	request.Targets = append(request.Targets, duplicate)
	if err := testDetachmentManager(t, NewFakeAPI(), time.Minute).EnsureDetached(context.Background(), request); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("EnsureDetached(duplicate Instance) error = %v", err)
	}
}
