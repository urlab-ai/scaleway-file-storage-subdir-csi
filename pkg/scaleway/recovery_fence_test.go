package scaleway

import (
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

const (
	recoveryParentA   = "11111111-1111-4111-8111-111111111111"
	recoveryParentB   = "44444444-4444-4444-8444-444444444444"
	recoveryProjectID = "22222222-2222-4222-8222-222222222222"
	recoveryInstance  = "33333333-3333-4333-8333-333333333333"
	recoveryNodeID    = "fr-par-1/" + recoveryInstance
)

func recoveryFenceHarness(t *testing.T) (*RecoveryFenceChecker, *FakeAPI) {
	t.Helper()
	api := NewFakeAPI()
	parents := []string{recoveryParentA, recoveryParentB}
	for index, parentID := range parents {
		api.Filesystems["fr-par/"+parentID] = Filesystem{
			ID: parentID, ProjectID: recoveryProjectID, Region: "fr-par",
			Status: FilesystemAvailable, NumberOfAttachments: 1,
		}
		api.Pages[parentID+"/"] = AttachmentPage{Attachments: []Attachment{{
			ID: "attachment-" + string(rune('a'+index)), FilesystemID: parentID,
			ResourceID: recoveryInstance, ResourceType: AttachmentResourceServer, Zone: "fr-par-1",
		}}}
	}
	api.Servers[recoveryNodeID] = Server{
		ID: recoveryInstance, ProjectID: recoveryProjectID, Zone: "fr-par-1", Region: "fr-par",
		CommercialType: "release-qualified", State: InstanceRunning, MaxFileSystems: 4,
		Filesystems: []ServerFilesystem{
			{FilesystemID: recoveryParentA, State: ServerFilesystemAvailable},
			{FilesystemID: recoveryParentB, State: ServerFilesystemAvailable},
		},
	}
	checker, err := NewRecoveryFenceChecker(api, "fr-par", recoveryProjectID, recoveryNodeID, parents)
	if err != nil {
		t.Fatalf("NewRecoveryFenceChecker() error = %v", err)
	}
	return checker, api
}

func TestRecoveryFenceCheckerRequiresOnlyExactProvisionalAttachments(t *testing.T) {
	checker, _ := recoveryFenceHarness(t)
	if err := checker.ProveClean(context.Background()); err != nil {
		t.Fatalf("ProveClean() error = %v", err)
	}
}

func TestRecoveryFenceCheckerRejectsOldOrUnknownAttachment(t *testing.T) {
	checker, api := recoveryFenceHarness(t)
	filesystem := api.Filesystems["fr-par/"+recoveryParentB]
	filesystem.NumberOfAttachments = 2
	api.Filesystems["fr-par/"+recoveryParentB] = filesystem
	page := api.Pages[recoveryParentB+"/"]
	page.Attachments = append(page.Attachments, Attachment{
		ID: "attachment-old", FilesystemID: recoveryParentB,
		ResourceID:   "55555555-5555-4555-8555-555555555555",
		ResourceType: AttachmentResourceServer, Zone: "fr-par-2",
	})
	api.Pages[recoveryParentB+"/"] = page
	if err := checker.ProveClean(context.Background()); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("ProveClean(old attachment) error = %v", err)
	}
}

func TestRecoveryFenceCheckerRejectsInventoryDisagreementAndProviderTransition(t *testing.T) {
	t.Run("instance inventory", func(t *testing.T) {
		checker, api := recoveryFenceHarness(t)
		server := api.Servers[recoveryNodeID]
		server.Filesystems[1].State = ServerFilesystemAttaching
		api.Servers[recoveryNodeID] = server
		if err := checker.ProveClean(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("ProveClean(attaching) error = %v", err)
		}
	})
	t.Run("parent status", func(t *testing.T) {
		checker, api := recoveryFenceHarness(t)
		filesystem := api.Filesystems["fr-par/"+recoveryParentA]
		filesystem.Status = FilesystemUpdating
		api.Filesystems["fr-par/"+recoveryParentA] = filesystem
		if err := checker.ProveClean(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("ProveClean(updating parent) error = %v", err)
		}
	})
}

func TestRecoveryFenceCheckerHonorsCancellationAndRejectsDuplicateScope(t *testing.T) {
	checker, _ := recoveryFenceHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := checker.ProveClean(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ProveClean(cancelled) error = %v", err)
	}
	if _, err := NewRecoveryFenceChecker(
		NewFakeAPI(), "fr-par", recoveryProjectID, recoveryNodeID,
		[]string{recoveryParentA, recoveryParentA},
	); err == nil {
		t.Fatal("NewRecoveryFenceChecker(duplicate parents) error = nil")
	}
}

func TestApprovalFenceVerifierChecksEveryParentForAbnormalTakeover(t *testing.T) {
	api := NewFakeAPI()
	for _, parentID := range []string{recoveryParentA, recoveryParentB} {
		api.Filesystems["fr-par/"+parentID] = Filesystem{
			ID: parentID, ProjectID: recoveryProjectID, Region: "fr-par",
			Status: FilesystemAvailable,
		}
		api.Pages[parentID+"/"] = AttachmentPage{}
	}
	previous, err := coordination.NewHolderEvidence(
		"66666666-6666-4666-8666-666666666666", "worker-old",
		"fr-par-2/55555555-5555-4555-8555-555555555555",
		"55555555-5555-4555-8555-555555555555", "fr-par-2",
		"77777777-7777-4777-8777-777777777777",
		"88888888-8888-4888-8888-888888888888",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	api.Servers[previous.CSINodeID] = Server{
		ID: previous.InstanceID, ProjectID: recoveryProjectID, Zone: previous.Zone,
		Region: "fr-par", State: InstanceStopped,
	}
	verifier, err := NewApprovalFenceVerifier(
		api, "fr-par", recoveryProjectID, recoveryNodeID,
		[]string{recoveryParentA, recoveryParentB},
	)
	if err != nil {
		t.Fatalf("NewApprovalFenceVerifier() error = %v", err)
	}
	approval := coordination.OperatorApproval{
		Mode:           coordination.ApprovalAbnormalTakeover,
		InstallationID: previous.InstallationID, ActiveClusterUID: previous.ActiveClusterUID,
		PreviousHolderPodUID: previous.PodUID, PreviousHolderNodeName: previous.NodeName,
		PreviousHolderCSINodeID: previous.CSINodeID, PreviousHolderInstanceID: previous.InstanceID,
		PreviousHolderZone: previous.Zone,
	}
	if err := verifier.VerifyAbnormalTakeover(context.Background(), approval, previous); err != nil {
		t.Fatalf("VerifyAbnormalTakeover() error = %v", err)
	}

	filesystem := api.Filesystems["fr-par/"+recoveryParentB]
	filesystem.NumberOfAttachments = 1
	api.Filesystems["fr-par/"+recoveryParentB] = filesystem
	api.Pages[recoveryParentB+"/"] = AttachmentPage{Attachments: []Attachment{{
		ID: "attachment-old", FilesystemID: recoveryParentB,
		ResourceID: previous.InstanceID, ResourceType: AttachmentResourceServer, Zone: previous.Zone,
	}}}
	if err := verifier.VerifyAbnormalTakeover(context.Background(), approval, previous); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("VerifyAbnormalTakeover(attached on second parent) error = %v", err)
	}
}

func TestApprovalFenceVerifierRequiresRecoveryScopeAndCleanInventories(t *testing.T) {
	_, api := recoveryFenceHarness(t)
	verifier, err := NewApprovalFenceVerifier(
		api, "fr-par", recoveryProjectID, recoveryNodeID,
		[]string{recoveryParentA, recoveryParentB},
	)
	if err != nil {
		t.Fatalf("NewApprovalFenceVerifier() error = %v", err)
	}
	approval := coordination.OperatorApproval{
		Mode:               coordination.ApprovalMissingLeaseRecovery,
		RecoveryFenceScope: coordination.RecoveryFenceAllPreRecoveryInstances,
	}
	if err := verifier.VerifyMissingLeaseRecovery(context.Background(), approval); err != nil {
		t.Fatalf("VerifyMissingLeaseRecovery() error = %v", err)
	}
	approval.RecoveryFenceScope = "checkpoint-holder-only"
	if err := verifier.VerifyMissingLeaseRecovery(context.Background(), approval); err == nil {
		t.Fatal("VerifyMissingLeaseRecovery(reduced scope) error = nil")
	}
}
