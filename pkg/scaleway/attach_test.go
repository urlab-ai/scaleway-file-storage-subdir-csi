package scaleway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
)

type fixedJitter struct{}

func (fixedJitter) Delay(base time.Duration, _ uint32) time.Duration { return base }

type advancingClock struct {
	mu  sync.Mutex
	now time.Time
}

type immediateTimer struct {
	channel chan time.Time
}

func (manual *advancingClock) Now() time.Time {
	manual.mu.Lock()
	defer manual.mu.Unlock()
	return manual.now
}

func (manual *advancingClock) NewTimer(duration time.Duration) clock.Timer {
	manual.mu.Lock()
	manual.now = manual.now.Add(duration)
	now := manual.now
	manual.mu.Unlock()
	channel := make(chan time.Time, 1)
	channel <- now
	return immediateTimer{channel: channel}
}

func (timer immediateTimer) C() <-chan time.Time { return timer.channel }
func (immediateTimer) Stop() bool                { return false }

func attachRequest() AttachRequest {
	return AttachRequest{
		Region:       "fr-par",
		ProjectID:    "22222222-2222-4222-8222-222222222222",
		FilesystemID: "11111111-1111-4111-8111-111111111111",
		Target: Target{
			Zone:     "fr-par-1",
			ServerID: "33333333-3333-4333-8333-333333333333",
		},
		ConfiguredParentIDs: map[string]struct{}{
			"11111111-1111-4111-8111-111111111111": {},
		},
		KnownInstances: map[string]Target{
			"33333333-3333-4333-8333-333333333333": {Zone: "fr-par-1", ServerID: "33333333-3333-4333-8333-333333333333"},
		},
		EligibleInstanceIDs: map[string]struct{}{
			"33333333-3333-4333-8333-333333333333": {},
		},
		QualifiedCommercialTypes: map[string]struct{}{"release-qualified": {}},
	}
}

func parentMetadata(count uint32) Filesystem {
	return Filesystem{
		ID:                  "11111111-1111-4111-8111-111111111111",
		ProjectID:           "22222222-2222-4222-8222-222222222222",
		Region:              "fr-par",
		SizeBytes:           1 << 40,
		Status:              FilesystemAvailable,
		NumberOfAttachments: count,
	}
}

func targetServer(state ServerFilesystemState, present bool) Server {
	server := Server{
		ID:             "33333333-3333-4333-8333-333333333333",
		ProjectID:      "22222222-2222-4222-8222-222222222222",
		Zone:           "fr-par-1",
		Region:         "fr-par",
		CommercialType: "release-qualified",
		State:          InstanceRunning,
		MaxFileSystems: 2,
	}
	if present {
		server.Filesystems = []ServerFilesystem{{FilesystemID: "11111111-1111-4111-8111-111111111111", State: state}}
	}
	return server
}

func targetAttachment() Attachment {
	return Attachment{
		ID:           "attachment-a",
		FilesystemID: "11111111-1111-4111-8111-111111111111",
		ResourceID:   "33333333-3333-4333-8333-333333333333",
		ResourceType: AttachmentResourceServer,
		Zone:         "fr-par-1",
	}
}

func knownServer(target Target, filesystems ...ServerFilesystem) Server {
	return Server{
		ID:             target.ServerID,
		ProjectID:      "22222222-2222-4222-8222-222222222222",
		Zone:           target.Zone,
		Region:         "fr-par",
		CommercialType: "release-qualified",
		State:          InstanceRunning,
		MaxFileSystems: 2,
		Filesystems:    append([]ServerFilesystem(nil), filesystems...),
	}
}

func testAttachmentManager(t *testing.T, api API, deadline time.Duration) *AttachmentManager {
	t.Helper()
	manager, err := NewAttachmentManager(api, &advancingClock{now: time.Unix(0, 0)}, fixedJitter{}, AttachConfig{
		Deadline:       deadline,
		InitialBackoff: time.Second,
		MaximumBackoff: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewAttachmentManager() error = %v", err)
	}
	return manager
}

func TestEnsureAttachedReturnsExistingAvailableWithoutAttachCall(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemAvailable, true)

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached() error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("attach call count = %d, want 0", len(attaches))
	}
}

func TestEnsureAttachedCallsAttachOnceAndPollsToAvailable(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	filesystemKey := request.Region + "/" + request.FilesystemID
	pageKey := request.FilesystemID + "/"
	serverKey := request.Target.Zone + "/" + request.Target.ServerID
	api.FilesystemSequences[filesystemKey] = []Filesystem{parentMetadata(0), parentMetadata(1), parentMetadata(1)}
	api.PageSequences[pageKey] = []AttachmentPage{{}, {Attachments: []Attachment{targetAttachment()}}, {Attachments: []Attachment{targetAttachment()}}}
	api.ServerSequences[serverKey] = []Server{
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemAttaching, true),
		targetServer(ServerFilesystemAvailable, true),
	}

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached() error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 || attaches[0].Zone != request.Target.Zone {
		t.Fatalf("attach calls = %#v, want one exact zonal call", attaches)
	}
}

func TestEnsureAttachedRereadsAfterAmbiguousAttachResult(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.FilesystemSequences[request.Region+"/"+request.FilesystemID] = []Filesystem{parentMetadata(0), parentMetadata(1)}
	api.PageSequences[request.FilesystemID+"/"] = []AttachmentPage{{}, {Attachments: []Attachment{targetAttachment()}}}
	api.ServerSequences[request.Target.Zone+"/"+request.Target.ServerID] = []Server{
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemAvailable, true),
	}
	api.InjectFault("attach", ErrUnavailable)

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached() error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedRereadsAfterConflictThatWasCommitted(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.FilesystemSequences[request.Region+"/"+request.FilesystemID] = []Filesystem{parentMetadata(0), parentMetadata(1)}
	api.PageSequences[request.FilesystemID+"/"] = []AttachmentPage{{}, {Attachments: []Attachment{targetAttachment()}}}
	api.ServerSequences[request.Target.Zone+"/"+request.Target.ServerID] = []Server{
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemAvailable, true),
	}
	api.InjectFault("attach", ErrConflict)

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached(committed conflict) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedConflictNeverIssuesSecondMutation(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(0)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemUnknown, false)
	api.InjectFault("attach", ErrConflict)

	err := testAttachmentManager(t, api, 3*time.Second).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrConflict) || !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("EnsureAttached(uncommitted conflict) error = %v, want conflict and deadline", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedPollsDelayedVisibilityAfterAmbiguousAttachWithoutRepeatingMutation(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.FilesystemSequences[request.Region+"/"+request.FilesystemID] = []Filesystem{parentMetadata(0), parentMetadata(0), parentMetadata(1)}
	api.PageSequences[request.FilesystemID+"/"] = []AttachmentPage{{}, {}, {Attachments: []Attachment{targetAttachment()}}}
	api.ServerSequences[request.Target.Zone+"/"+request.Target.ServerID] = []Server{
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemAvailable, true),
	}
	api.InjectFault("attach", ErrUnavailable)

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached(delayed ambiguous commit) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedRetriesTransientInventoryBeforeIssuingOneAttach(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.InjectFault("get-filesystem", ErrUnavailable)
	api.FilesystemSequences[request.Region+"/"+request.FilesystemID] = []Filesystem{parentMetadata(0), parentMetadata(1)}
	api.PageSequences[request.FilesystemID+"/"] = []AttachmentPage{{}, {Attachments: []Attachment{targetAttachment()}}}
	api.ServerSequences[request.Target.Zone+"/"+request.Target.ServerID] = []Server{
		targetServer(ServerFilesystemUnknown, false),
		targetServer(ServerFilesystemAvailable, true),
	}

	if err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request); err != nil {
		t.Fatalf("EnsureAttached(transient initial inventory) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedDoesNotRetryDefiniteAttachFailure(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(0)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemUnknown, false)
	api.InjectFault("attach", ErrPermissionDenied)

	err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("EnsureAttached(permission denied) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 1 {
		t.Fatalf("attach call count = %d, want exactly 1", len(attaches))
	}
}

func TestEnsureAttachedRejectsDetachingWithoutCompetingAttach(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemDetaching, true)

	err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("EnsureAttached(detaching) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("attach call count = %d, want 0", len(attaches))
	}
}

func TestEnsureAttachedDeadlineIsBounded(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{targetAttachment()}}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemAttaching, true)

	err := testAttachmentManager(t, api, 3*time.Second).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("EnsureAttached(deadline) error = %v", err)
	}
}

func TestEnsureAttachedFailsOnInventoryDisagreement(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(0)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemAvailable, true)

	err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("EnsureAttached(disagreement) error = %v", err)
	}
}

func TestEnsureAttachedReconcilesEveryKnownInstanceBeforeMutation(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	other := Target{Zone: "fr-par-2", ServerID: "44444444-4444-4444-8444-444444444444"}
	request.KnownInstances[other.ServerID] = other
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{{
		ID:           "attachment-b",
		FilesystemID: request.FilesystemID,
		ResourceID:   other.ServerID,
		ResourceType: AttachmentResourceServer,
		Zone:         other.Zone,
	}}}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = knownServer(request.Target)
	// The regional API reports an attachment to the other known Instance, but
	// that Instance does not report the parent. A target-only reread would miss
	// this disagreement and incorrectly authorize a new mutation.
	api.Servers[other.Zone+"/"+other.ServerID] = knownServer(other)

	err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrAttachmentInventoryDisagreement) {
		t.Fatalf("EnsureAttached(other Instance disagreement) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("attach call count = %d, want 0", len(attaches))
	}
}

func TestEnsureAttachedRejectsKnownInstanceZoneDisagreementBeforeMutation(t *testing.T) {
	api := NewFakeAPI()
	request := attachRequest()
	api.Filesystems[request.Region+"/"+request.FilesystemID] = parentMetadata(1)
	attachment := targetAttachment()
	attachment.Zone = "fr-par-2"
	api.Pages[request.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{attachment}}
	api.Servers[request.Target.Zone+"/"+request.Target.ServerID] = targetServer(ServerFilesystemAvailable, true)

	err := testAttachmentManager(t, api, time.Minute).EnsureAttached(context.Background(), request)
	if !errors.Is(err, ErrAttachmentInventoryDisagreement) {
		t.Fatalf("EnsureAttached(wrong attachment zone) error = %v", err)
	}
	_, attaches, _ := api.SnapshotRequests()
	if len(attaches) != 0 {
		t.Fatalf("attach call count = %d, want 0", len(attaches))
	}
}

func TestEnsureAttachedRejectsInvalidProviderScopeBeforeRead(t *testing.T) {
	api := NewFakeAPI()
	api.InjectFault("get-filesystem", ErrPermissionDenied)
	manager := testAttachmentManager(t, api, time.Minute)
	request := attachRequest()
	validProject := request.ProjectID
	request.ProjectID = "project"
	if err := manager.EnsureAttached(context.Background(), request); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("EnsureAttached(invalid project) error = %v", err)
	}
	request.ProjectID = validProject
	validTarget := request.Target
	request.Target.Zone = "nl-ams-1"
	if err := manager.EnsureAttached(context.Background(), request); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("EnsureAttached(cross-region target) error = %v", err)
	}
	request.Target = validTarget
	if err := manager.EnsureAttached(context.Background(), request); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("EnsureAttached(valid after rejected scope) error = %v", err)
	}
}

func TestParseNodeID(t *testing.T) {
	target, err := ParseNodeID("fr-par-2/33333333-3333-4333-8333-333333333333")
	if err != nil {
		t.Fatalf("ParseNodeID() error = %v", err)
	}
	if target.Zone != "fr-par-2" || target.ServerID != "33333333-3333-4333-8333-333333333333" {
		t.Fatalf("ParseNodeID() = %#v", target)
	}
	if _, err := ParseNodeID("invalid"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ParseNodeID(invalid) error = %v", err)
	}
}

func TestNextBackoffSaturatesWithoutDurationOverflow(t *testing.T) {
	maximum := time.Duration(1<<63 - 1)
	if got := nextBackoff(maximum/2+1, maximum); got != maximum {
		t.Fatalf("nextBackoff(overflow boundary) = %v, want %v", got, maximum)
	}
	if got := nextBackoff(time.Second, 10*time.Second); got != 2*time.Second {
		t.Fatalf("nextBackoff(normal) = %v", got)
	}
	if got := nextBackoff(8*time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("nextBackoff(clamped) = %v", got)
	}
}

func TestScaleJitterPreservesPositiveBoundedDurationWithoutOverflow(t *testing.T) {
	if got := scaleJitter(time.Second, 800); got != 800*time.Millisecond {
		t.Fatalf("scaleJitter(80%%) = %v", got)
	}
	if got := scaleJitter(time.Second, 1200); got != 1200*time.Millisecond {
		t.Fatalf("scaleJitter(120%%) = %v", got)
	}
	if got := scaleJitter(time.Nanosecond, 800); got != time.Nanosecond {
		t.Fatalf("scaleJitter(sub-permille) = %v", got)
	}
	maximum := time.Duration(1<<63 - 1)
	if got := scaleJitter(maximum, 1200); got != maximum {
		t.Fatalf("scaleJitter(overflow boundary) = %v, want %v", got, maximum)
	}
	if got := scaleJitter(time.Second, 799); got != time.Second {
		t.Fatalf("scaleJitter(invalid factor) = %v", got)
	}
}
