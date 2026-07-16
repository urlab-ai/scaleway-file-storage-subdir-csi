package driverapp

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fakeParentBootstrapLeadership struct {
	ctx        context.Context
	snapshot   coordination.LeaseSnapshot
	setCalls   []coordination.BootstrapAttempt
	clearCalls []string
	events     *[]string
	err        error
}

func (leadership *fakeParentBootstrapLeadership) Context() context.Context { return leadership.ctx }

func (leadership *fakeParentBootstrapLeadership) RequireActiveLeadership(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-leadership.ctx.Done():
		return coordination.ErrLeadershipNotActive
	default:
	}
	return leadership.err
}

func (leadership *fakeParentBootstrapLeadership) Snapshot() coordination.LeaseSnapshot {
	snapshot := leadership.snapshot
	snapshot.Annotations = maps.Clone(snapshot.Annotations)
	return snapshot
}

func (leadership *fakeParentBootstrapLeadership) SetBootstrapAttempt(_ context.Context, attempt coordination.BootstrapAttempt) error {
	if leadership.err != nil {
		return leadership.err
	}
	annotations, err := attempt.Annotations()
	if err != nil {
		return err
	}
	if current, present, err := coordination.ParseBootstrapAttempt(leadership.snapshot.Annotations); err != nil {
		return err
	} else if present && current != attempt {
		return errors.New("different bootstrap attempt is active")
	}
	if leadership.snapshot.Annotations == nil {
		leadership.snapshot.Annotations = map[string]string{}
	}
	for key, value := range annotations {
		leadership.snapshot.Annotations[key] = value
	}
	leadership.setCalls = append(leadership.setCalls, attempt)
	*leadership.events = append(*leadership.events, "set")
	return nil
}

func (leadership *fakeParentBootstrapLeadership) ClearBootstrapAttempt(_ context.Context, attemptID string) error {
	current, present, err := coordination.ParseBootstrapAttempt(leadership.snapshot.Annotations)
	if err != nil {
		return err
	}
	if !present || current.AttemptID != attemptID {
		return errors.New("clear does not match active attempt")
	}
	leadership.snapshot.Annotations = coordination.ClearBootstrapAnnotations(leadership.snapshot.Annotations)
	leadership.clearCalls = append(leadership.clearCalls, attemptID)
	*leadership.events = append(*leadership.events, "clear")
	return nil
}

type fakeParentBootstrapAccess struct {
	provider *scaleway.FakeAPI
	nodeID   string
	root     string
	events   *[]string
}

func (access *fakeParentBootstrapAccess) EnsureMounted(_ context.Context, parentID string) (string, error) {
	*access.events = append(*access.events, "mount")
	seedBootstrapProviderAttachment(access.provider, access.nodeID, parentID)
	return access.root, nil
}

type fakeParentBootstrapFilesystem struct {
	claim        volume.ParentOwnerRecord
	claimPresent bool
	rootErr      error
	closeErr     error
	events       *[]string
}

func (filesystem *fakeParentBootstrapFilesystem) Close() error {
	*filesystem.events = append(*filesystem.events, "close")
	return filesystem.closeErr
}

func (filesystem *fakeParentBootstrapFilesystem) InspectFreshRoot(context.Context) error {
	*filesystem.events = append(*filesystem.events, "inspect-fresh")
	return filesystem.rootErr
}

func (filesystem *fakeParentBootstrapFilesystem) InspectUnclaimedRoot(context.Context, string) (safety.BootstrapRootState, error) {
	*filesystem.events = append(*filesystem.events, "inspect")
	return safety.BootstrapRootState{}, filesystem.rootErr
}

func (filesystem *fakeParentBootstrapFilesystem) InspectClaimedBootstrapRoot(context.Context, string) (safety.BootstrapRootState, error) {
	*filesystem.events = append(*filesystem.events, "inspect-claimed")
	return safety.BootstrapRootState{ParentClaimPresent: filesystem.claimPresent}, filesystem.rootErr
}

func (filesystem *fakeParentBootstrapFilesystem) ReadParentClaim(context.Context) (volume.ParentOwnerRecord, bool, error) {
	*filesystem.events = append(*filesystem.events, "read")
	return filesystem.claim, filesystem.claimPresent, nil
}

func (filesystem *fakeParentBootstrapFilesystem) InstallParentClaim(_ context.Context, attemptID string, claim volume.ParentOwnerRecord) error {
	*filesystem.events = append(*filesystem.events, "install")
	if claim.BootstrapAttemptID != attemptID {
		return errors.New("claim attempt mismatch")
	}
	filesystem.claim = claim
	filesystem.claimPresent = true
	return nil
}

func (filesystem *fakeParentBootstrapFilesystem) RemoveBootstrapTemporary(context.Context, string) error {
	*filesystem.events = append(*filesystem.events, "remove-temp")
	return nil
}

func (filesystem *fakeParentBootstrapFilesystem) EnsureLayout(context.Context, string) error {
	*filesystem.events = append(*filesystem.events, "layout")
	return nil
}

type fixedBootstrapIDs struct {
	id    string
	err   error
	calls int
}

type fakeParentBootstrapEvidence struct {
	hasReferences bool
	err           error
	calls         int
}

func (evidence *fakeParentBootstrapEvidence) HasDurableReferences(context.Context, string) (bool, error) {
	evidence.calls++
	return evidence.hasReferences, evidence.err
}

func (ids *fixedBootstrapIDs) New() (string, error) {
	ids.calls++
	return ids.id, ids.err
}

func TestParentBootstrapFreshClaimPersistsJournalBeforeAttachAndClearsAfterClaim(t *testing.T) {
	manager, leadership, access, filesystem, ids, parentID := parentBootstrapTestManager(t)
	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed() error = %v", err)
	}
	wantOrder := []string{"set", "mount", "read", "inspect", "install", "read", "remove-temp", "clear", "layout", "close"}
	if !slices.Equal(*leadership.events, wantOrder) {
		t.Fatalf("bootstrap events = %#v, want %#v", *leadership.events, wantOrder)
	}
	if ids.calls != 1 || len(leadership.setCalls) != 1 || len(leadership.clearCalls) != 1 {
		t.Fatalf("ID/set/clear calls = %d/%d/%d", ids.calls, len(leadership.setCalls), len(leadership.clearCalls))
	}
	if !filesystem.claimPresent || filesystem.claim.ParentFilesystemID != parentID || filesystem.claim.BootstrapAttemptID != ids.id {
		t.Fatalf("installed parent claim = %#v", filesystem.claim)
	}
	if _, present, err := coordination.ParseBootstrapAttempt(leadership.snapshot.Annotations); err != nil || present {
		t.Fatalf("journal after completion = present=%v, error=%v", present, err)
	}
	if access.root == "" {
		t.Fatal("test access root is empty")
	}
}

func TestParentBootstrapResumeReplaysExactLeaseCASWithoutNewAttempt(t *testing.T) {
	manager, leadership, _, filesystem, ids, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	annotations, _ := attempt.Annotations()
	leadership.snapshot.Annotations = annotations
	seedBootstrapProviderAttachment(manager.provider.(*scaleway.FakeAPI), manager.localNodeID, parentID)

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(resume) error = %v", err)
	}
	if ids.calls != 0 {
		t.Fatalf("resume generated %d new IDs", ids.calls)
	}
	if len(leadership.setCalls) != 1 || leadership.setCalls[0] != attempt {
		t.Fatalf("resume Lease replay = %#v", leadership.setCalls)
	}
	if filesystem.claim.BootstrapAttemptID != attempt.AttemptID {
		t.Fatalf("resumed claim attempt = %q", filesystem.claim.BootstrapAttemptID)
	}
	if len(*leadership.events) == 0 || (*leadership.events)[0] != "set" {
		t.Fatalf("resume did not prove Lease before attach: %#v", *leadership.events)
	}
}

func TestParentBootstrapPostClaimResumeProvesEmptyBootstrapRootBeforeClear(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	annotations, _ := attempt.Annotations()
	leadership.snapshot.Annotations = annotations
	seedBootstrapProviderAttachment(manager.provider.(*scaleway.FakeAPI), manager.localNodeID, parentID)
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim, filesystem.claimPresent = claim, true

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(post-claim resume) error = %v", err)
	}
	want := []string{"set", "mount", "read", "inspect-claimed", "install", "remove-temp", "clear", "layout", "close"}
	if !slices.Equal(*leadership.events, want) {
		t.Fatalf("post-claim resume events = %#v, want %#v", *leadership.events, want)
	}
}

func TestParentBootstrapPostClaimResumeKeepsJournalWhenRootIsNotEmpty(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "77777777-7777-4777-8777-777777777777")
	annotations, _ := attempt.Annotations()
	leadership.snapshot.Annotations = annotations
	seedBootstrapProviderAttachment(manager.provider.(*scaleway.FakeAPI), manager.localNodeID, parentID)
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim, filesystem.claimPresent = claim, true
	filesystem.rootErr = errors.New("logical ownership metadata exists")

	if err := manager.EnsureClaimed(context.Background(), parentID); err == nil {
		t.Fatal("EnsureClaimed(non-empty post-claim root) error = nil")
	}
	if len(leadership.clearCalls) != 0 || slices.Contains(*leadership.events, "install") || slices.Contains(*leadership.events, "layout") {
		t.Fatalf("unsafe post-claim resume advanced: %#v", *leadership.events)
	}
	if _, present, err := coordination.ParseBootstrapAttempt(leadership.snapshot.Annotations); err != nil || !present {
		t.Fatalf("post-claim failure journal = present=%v, error=%v", present, err)
	}
}

func TestParentBootstrapRecognizesPreexistingExactClaimAfterDetachedInventory(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	oldAttempt := bootstrapAttemptForManager(t, manager, parentID, "88888888-8888-4888-8888-888888888888")
	oldClaim, err := manager.claimForAttempt(manager.parents[parentID], oldAttempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim = oldClaim
	filesystem.claimPresent = true

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(preexisting detached claim) error = %v", err)
	}
	if slices.Contains(*leadership.events, "inspect") || slices.Contains(*leadership.events, "install") {
		t.Fatalf("preexisting claim reopened adoption path: %#v", *leadership.events)
	}
	if !slices.Contains(*leadership.events, "remove-temp") || !slices.Contains(*leadership.events, "clear") || !slices.Contains(*leadership.events, "layout") {
		t.Fatalf("preexisting claim did not clean current journal safely: %#v", *leadership.events)
	}
	if filesystem.claim != oldClaim {
		t.Fatal("preexisting immutable claim was rewritten")
	}
}

func TestParentBootstrapUnsafeRootLeavesJournalForRecovery(t *testing.T) {
	manager, leadership, _, filesystem, _, parentID := parentBootstrapTestManager(t)
	filesystem.rootErr = errors.New("unexpected user data")
	if err := manager.EnsureClaimed(context.Background(), parentID); err == nil {
		t.Fatal("EnsureClaimed(unsafe root) error = nil")
	}
	if len(leadership.clearCalls) != 0 || slices.Contains(*leadership.events, "install") || slices.Contains(*leadership.events, "layout") {
		t.Fatalf("unsafe root advanced bootstrap: %#v", *leadership.events)
	}
	if _, present, err := coordination.ParseBootstrapAttempt(leadership.snapshot.Annotations); err != nil || !present {
		t.Fatalf("unsafe root journal = present=%v, error=%v", present, err)
	}
}

func TestParentBootstrapValidatesExistingAttachedClaimWithoutJournal(t *testing.T) {
	manager, leadership, _, filesystem, ids, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "99999999-9999-4999-8999-999999999999")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim, filesystem.claimPresent = claim, true
	seedBootstrapProviderAttachment(manager.provider.(*scaleway.FakeAPI), manager.localNodeID, parentID)

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(existing) error = %v", err)
	}
	if ids.calls != 0 || len(leadership.setCalls) != 0 || len(leadership.clearCalls) != 0 {
		t.Fatalf("existing claim ID/set/clear calls = %d/%d/%d", ids.calls, len(leadership.setCalls), len(leadership.clearCalls))
	}
	want := []string{"mount", "read", "layout", "close"}
	if !slices.Equal(*leadership.events, want) {
		t.Fatalf("existing claim events = %#v, want %#v", *leadership.events, want)
	}
}

func TestParentBootstrapDetachedParentWithDurableReferencesUsesExistingClaimPath(t *testing.T) {
	manager, leadership, _, filesystem, ids, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim, filesystem.claimPresent = claim, true
	evidence := manager.evidence.(*fakeParentBootstrapEvidence)
	evidence.hasReferences = true

	if err := manager.EnsureClaimed(context.Background(), parentID); err != nil {
		t.Fatalf("EnsureClaimed(detached referenced parent) error = %v", err)
	}
	if ids.calls != 0 || len(leadership.setCalls) != 0 || len(leadership.clearCalls) != 0 {
		t.Fatalf("referenced parent entered bootstrap: ID/set/clear = %d/%d/%d", ids.calls, len(leadership.setCalls), len(leadership.clearCalls))
	}
	if !slices.Equal(*leadership.events, []string{"mount", "read", "layout", "close"}) {
		t.Fatalf("referenced detached parent events = %#v", *leadership.events)
	}
}

func TestParentBootstrapReadOnlyRecoveryDiscoveryNeverMutatesFilesystemOrLease(t *testing.T) {
	manager, leadership, _, filesystem, ids, parentID := parentBootstrapTestManager(t)
	attempt := bootstrapAttemptForManager(t, manager, parentID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	claim, err := manager.claimForAttempt(manager.parents[parentID], attempt)
	if err != nil {
		t.Fatalf("claimForAttempt() error = %v", err)
	}
	filesystem.claim, filesystem.claimPresent = claim, true

	if err := manager.DiscoverExistingReadOnly(context.Background()); err != nil {
		t.Fatalf("DiscoverExistingReadOnly() error = %v", err)
	}
	if !slices.Equal(*leadership.events, []string{"mount", "read", "close"}) {
		t.Fatalf("read-only recovery discovery events = %#v", *leadership.events)
	}
	if ids.calls != 0 || len(leadership.setCalls) != 0 || len(leadership.clearCalls) != 0 {
		t.Fatalf("read-only recovery discovery mutated journal: IDs/set/clear = %d/%d/%d", ids.calls, len(leadership.setCalls), len(leadership.clearCalls))
	}
	if filesystem.claim != claim {
		t.Fatal("read-only recovery discovery changed the immutable claim")
	}
}

func TestParentBootstrapReadOnlyRecoveryDiscoveryRequiresClaim(t *testing.T) {
	manager, leadership, _, _, _, _ := parentBootstrapTestManager(t)
	if err := manager.DiscoverExistingReadOnly(context.Background()); err == nil {
		t.Fatal("DiscoverExistingReadOnly(missing claim) error = nil")
	}
	if slices.Contains(*leadership.events, "install") || slices.Contains(*leadership.events, "layout") || slices.Contains(*leadership.events, "set") {
		t.Fatalf("missing-claim recovery discovery mutated state: %#v", *leadership.events)
	}
}

func parentBootstrapTestManager(t *testing.T) (*parentBootstrapManager, *fakeParentBootstrapLeadership, *fakeParentBootstrapAccess, *fakeParentBootstrapFilesystem, *fixedBootstrapIDs, string) {
	t.Helper()
	configured, provider, inventory, localNodeID, _, parentID := controllerParentFixture(t)
	configured.Runtime.DriverName = "file-storage-subdir.csi.urlab.ai"
	configured.Runtime.Installation.ID = "11111111-1111-4111-8111-111111111111"
	configured.ControllerNamespace = "driver-system"
	configured.HelmReleaseName = "driver-release"
	events := []string{}
	leadership := &fakeParentBootstrapLeadership{
		ctx: context.Background(),
		snapshot: coordination.LeaseSnapshot{
			UID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ResourceVersion: "1", Annotations: map[string]string{},
		},
		events: &events,
	}
	authorizations, err := newControllerNodeAuthorizations(inventory, provider, configured)
	if err != nil {
		t.Fatalf("newControllerNodeAuthorizations() error = %v", err)
	}
	access := &fakeParentBootstrapAccess{provider: provider, nodeID: localNodeID, root: "/controller-parents/" + parentID, events: &events}
	ids := &fixedBootstrapIDs{id: "66666666-6666-4666-8666-666666666666"}
	evidence := &fakeParentBootstrapEvidence{}
	manager, err := newParentBootstrapManager(
		configured, "22222222-2222-4222-8222-222222222222", localNodeID,
		leadership, provider, authorizations, access, evidence,
		clock.NewManual(time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC)), ids,
	)
	if err != nil {
		t.Fatalf("newParentBootstrapManager() error = %v", err)
	}
	filesystem := &fakeParentBootstrapFilesystem{events: &events}
	manager.openFilesystem = func(string) (parentBootstrapFilesystem, error) { return filesystem, nil }
	return manager, leadership, access, filesystem, ids, parentID
}

func bootstrapAttemptForManager(t *testing.T, manager *parentBootstrapManager, parentID, attemptID string) coordination.BootstrapAttempt {
	t.Helper()
	attempt, err := coordination.NewBootstrapAttempt(
		attemptID, manager.installationID, manager.clusterUID, parentID,
		manager.localNodeID, manager.localTarget.ServerID, manager.localTarget.Zone,
		time.Date(2026, 7, 14, 9, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewBootstrapAttempt() error = %v", err)
	}
	return attempt
}

func seedBootstrapProviderAttachment(provider *scaleway.FakeAPI, nodeID, parentID string) {
	target, _ := scaleway.ParseNodeID(nodeID)
	filesystem := provider.Filesystems["fr-par/"+parentID]
	filesystem.NumberOfAttachments = 1
	provider.Filesystems["fr-par/"+parentID] = filesystem
	provider.Pages[parentID+"/"] = scaleway.AttachmentPage{Attachments: []scaleway.Attachment{{
		ID: "bootstrap-attachment", FilesystemID: parentID, ResourceID: target.ServerID,
		ResourceType: scaleway.AttachmentResourceServer, Zone: target.Zone,
	}}}
	server := provider.Servers[nodeID]
	server.Filesystems = []scaleway.ServerFilesystem{{FilesystemID: parentID, State: scaleway.ServerFilesystemAvailable}}
	provider.Servers[nodeID] = server
}
