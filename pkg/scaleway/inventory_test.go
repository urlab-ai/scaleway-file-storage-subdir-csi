package scaleway

import (
	"context"
	"errors"
	"testing"
)

func testFilesystem(attachmentCount uint32) Filesystem {
	return Filesystem{
		ID:                  "11111111-1111-4111-8111-111111111111",
		ProjectID:           "22222222-2222-4222-8222-222222222222",
		Region:              "fr-par",
		SizeBytes:           1 << 40,
		Status:              FilesystemAvailable,
		NumberOfAttachments: attachmentCount,
	}
}

func attachment(id, instance, zone string) Attachment {
	return Attachment{
		ID:           id,
		FilesystemID: "11111111-1111-4111-8111-111111111111",
		ResourceID:   instance,
		ResourceType: AttachmentResourceServer,
		Zone:         zone,
	}
}

func TestListRegionalInventoryPaginatesWithoutZoneAndDeduplicates(t *testing.T) {
	api := NewFakeAPI()
	first := attachment("attachment-a", "instance-a", "fr-par-1")
	second := attachment("attachment-b", "instance-b", "fr-par-2")
	third := attachment("attachment-c", "instance-c", "fr-par-3")
	api.Pages[first.FilesystemID+"/"] = AttachmentPage{Attachments: []Attachment{first, second}, NextPageToken: "page-2"}
	api.Pages[first.FilesystemID+"/page-2"] = AttachmentPage{Attachments: []Attachment{second, third}}

	inventory, err := ListRegionalInventory(context.Background(), api, testFilesystem(3))
	if err != nil {
		t.Fatalf("ListRegionalInventory() error = %v", err)
	}
	if len(inventory.Attachments) != 3 {
		t.Fatalf("inventory length = %d, want 3", len(inventory.Attachments))
	}
	requests, _, _ := api.SnapshotRequests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	for _, request := range requests {
		if request.Zone != nil {
			t.Fatalf("request inherited zone filter %q", *request.Zone)
		}
	}
}

func TestListRegionalInventoryFailsClosedOnCountMismatchOrUnknownType(t *testing.T) {
	api := NewFakeAPI()
	filesystem := testFilesystem(2)
	one := attachment("attachment-a", "instance-a", "fr-par-1")
	api.Pages[filesystem.ID+"/"] = AttachmentPage{Attachments: []Attachment{one}}
	if _, err := ListRegionalInventory(context.Background(), api, filesystem); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ListRegionalInventory(count mismatch) error = %v", err)
	}

	filesystem.NumberOfAttachments = 1
	one.ResourceType = AttachmentResourceUnknown
	api.Pages[filesystem.ID+"/"] = AttachmentPage{Attachments: []Attachment{one}}
	if _, err := ListRegionalInventory(context.Background(), api, filesystem); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ListRegionalInventory(unknown type) error = %v", err)
	}
}

func TestValidateAuthorizedAttachmentsRejectsForeignInstance(t *testing.T) {
	one := attachment("attachment-a", "instance-a", "fr-par-1")
	inventory := RegionalInventory{FilesystemID: one.FilesystemID, Attachments: []Attachment{one}}
	if err := ValidateAuthorizedAttachments(inventory, map[string]Target{}); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("ValidateAuthorizedAttachments(foreign) error = %v", err)
	}
	if err := ValidateAuthorizedAttachments(inventory, map[string]Target{"instance-a": {Zone: "fr-par-1", ServerID: "instance-a"}}); err != nil {
		t.Fatalf("ValidateAuthorizedAttachments(known) error = %v", err)
	}
	if err := ValidateAuthorizedAttachments(inventory, map[string]Target{"instance-a": {Zone: "fr-par-2", ServerID: "instance-a"}}); !errors.Is(err, ErrAttachmentInventoryDisagreement) {
		t.Fatalf("ValidateAuthorizedAttachments(wrong zone) error = %v", err)
	}
}

func TestValidateAttachmentInventoryAgreementIncludesKnownInstancesWithoutRegionalEntry(t *testing.T) {
	filesystemID := "11111111-1111-4111-8111-111111111111"
	target := Target{Zone: "fr-par-1", ServerID: "instance-a"}
	known := map[string]Target{target.ServerID: target}
	server := Server{ID: target.ServerID, Zone: target.Zone, Filesystems: []ServerFilesystem{{
		FilesystemID: filesystemID,
		State:        ServerFilesystemAvailable,
	}}}
	err := ValidateAttachmentInventoryAgreement(
		RegionalInventory{FilesystemID: filesystemID},
		known,
		map[string]Server{target.ServerID: server},
	)
	if !errors.Is(err, ErrAttachmentInventoryDisagreement) {
		t.Fatalf("ValidateAttachmentInventoryAgreement() error = %v", err)
	}
}

func TestServerInventoryExclusivityAndSetUnionBudget(t *testing.T) {
	configured := map[string]struct{}{
		"11111111-1111-4111-8111-111111111111": {},
		"22222222-2222-4222-8222-222222222222": {},
	}
	server := Server{
		ID:             "instance-a",
		MaxFileSystems: 2,
		Filesystems: []ServerFilesystem{{
			FilesystemID: "11111111-1111-4111-8111-111111111111",
			State:        ServerFilesystemAttaching,
		}},
	}
	if err := ValidateExclusiveServerInventory(server, configured); err != nil {
		t.Fatalf("ValidateExclusiveServerInventory() error = %v", err)
	}
	if err := ValidatePostAttachBudget(server, configured); err != nil {
		t.Fatalf("ValidatePostAttachBudget(deduplicated union) error = %v", err)
	}

	server.Filesystems = append(server.Filesystems, ServerFilesystem{
		FilesystemID: "33333333-3333-4333-8333-333333333333",
		State:        ServerFilesystemDetaching,
	})
	if err := ValidateExclusiveServerInventory(server, configured); !errors.Is(err, ErrFailedPrecondition) {
		t.Fatalf("ValidateExclusiveServerInventory(foreign) error = %v", err)
	}
	if err := ValidatePostAttachBudget(server, configured); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("ValidatePostAttachBudget(over limit) error = %v", err)
	}
}

func TestServerInventoryRejectsUnknownAttachmentState(t *testing.T) {
	server := Server{ID: "instance-a", MaxFileSystems: 2, Filesystems: []ServerFilesystem{{
		FilesystemID: "11111111-1111-4111-8111-111111111111",
		State:        ServerFilesystemUnknown,
	}}}
	if _, err := ServerAttachmentMap(server); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ServerAttachmentMap() error = %v", err)
	}
}
