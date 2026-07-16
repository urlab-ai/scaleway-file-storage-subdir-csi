package scaleway

import (
	"context"
	"errors"
	"testing"
)

const fenceNodeID = "fr-par-1/33333333-3333-4333-8333-333333333333"

func fenceCheckerHarness(t *testing.T, attachmentCount uint32, attachments []Attachment, server *Server) (*FenceChecker, *FakeAPI) {
	t.Helper()
	api := NewFakeAPI()
	filesystem := parentMetadata(attachmentCount)
	api.Filesystems[filesystem.Region+"/"+filesystem.ID] = filesystem
	api.Pages[filesystem.ID+"/"] = AttachmentPage{Attachments: attachments}
	if server != nil {
		api.Servers[server.Zone+"/"+server.ID] = *server
	}
	checker, err := NewFenceChecker(api, filesystem.Region, filesystem.ProjectID)
	if err != nil {
		t.Fatalf("NewFenceChecker() error = %v", err)
	}
	return checker, api
}

func TestFenceCheckerAcceptsStoppedInstanceOnlyAfterAttachmentAbsence(t *testing.T) {
	server := targetServer(ServerFilesystemUnknown, false)
	server.State = InstanceStopped
	checker, _ := fenceCheckerHarness(t, 0, nil, &server)
	if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); err != nil {
		t.Fatalf("ProveFenced(stopped clean) error = %v", err)
	}

	server.Filesystems = []ServerFilesystem{{FilesystemID: parentMetadata(0).ID, State: ServerFilesystemAvailable}}
	checker, _ = fenceCheckerHarness(t, 1, []Attachment{targetAttachment()}, &server)
	if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("ProveFenced(stopped attached) error = %v", err)
	}
}

func TestFenceCheckerRejectsEveryNonTerminalInstanceState(t *testing.T) {
	for _, state := range []InstanceState{InstanceRunning, InstanceStarting, InstanceStopping, InstanceLocked, InstanceUnknown} {
		t.Run(string(state), func(t *testing.T) {
			server := targetServer(ServerFilesystemUnknown, false)
			server.State = state
			checker, _ := fenceCheckerHarness(t, 0, nil, &server)
			if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); !errors.Is(err, ErrFailedPrecondition) {
				t.Fatalf("ProveFenced(%s) error = %v", state, err)
			}
		})
	}
}

func TestFenceCheckerDeletedInstanceStillRequiresCleanRegionalInventory(t *testing.T) {
	checker, _ := fenceCheckerHarness(t, 0, nil, nil)
	if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); err != nil {
		t.Fatalf("ProveFenced(deleted clean) error = %v", err)
	}
	checker, _ = fenceCheckerHarness(t, 1, []Attachment{targetAttachment()}, nil)
	if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("ProveFenced(deleted orphan) error = %v", err)
	}
}

func TestFenceCheckerRejectsCrossRegionOrMalformedIdentityBeforeProviderRead(t *testing.T) {
	checker, api := fenceCheckerHarness(t, 0, nil, nil)
	api.InjectFault("get-filesystem", ErrUnavailable)
	if err := checker.ProveFenced(context.Background(), "nl-ams-1/33333333-3333-4333-8333-333333333333", parentMetadata(0).ID); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ProveFenced(cross-region) error = %v", err)
	}
	if err := checker.ProveFenced(context.Background(), fenceNodeID, "bad/id"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ProveFenced(malformed parent) error = %v", err)
	}
	// The first valid provider operation must still observe the injected fault;
	// invalid durable evidence cannot consume or probe provider state.
	if err := checker.ProveFenced(context.Background(), fenceNodeID, parentMetadata(0).ID); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ProveFenced(valid after rejected inputs) error = %v", err)
	}
}
