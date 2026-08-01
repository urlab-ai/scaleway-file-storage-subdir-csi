package main

import (
	"testing"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func TestValidatePlannedBootstrapAbortEvidenceRequiresExactRunScope(t *testing.T) {
	const (
		runID   = "11111111-1111-4111-8111-111111111111"
		parentA = "77777777-7777-4777-8777-777777777777"
		parentB = "88888888-8888-4888-8888-888888888888"
	)
	backend := &scalewayBackend{plan: e2eplan.Plan{
		RunID: runID, Profile: e2eplan.ProfileBase, Region: "fr-par",
	}}
	request := e2erunner.Request{
		Zone: "fr-par-1", DriverNamespace: "driver-system", HelmRelease: "driver",
	}
	inventory := e2ecleanup.Inventory{Resources: []e2ecleanup.Resource{
		{Kind: e2ecleanup.ResourceKindParent, ID: parentA},
		{Kind: e2ecleanup.ResourceKindParent, ID: parentB},
	}}
	valid := plannedBootstrapAbortEvidence{
		SchemaVersion: "2", RunID: runID, Profile: e2eplan.ProfileBase, Region: "fr-par",
		ClusterCreatedByRun: true, Namespace: request.DriverNamespace, HelmRelease: request.HelmRelease,
		HelmStatus: "failed", ParentA: parentA, ParentB: parentB,
		ParentAAttachments: 1, ParentAReportedAttachments: 1,
		ParentBAttachments: 0, ParentBReportedAttachments: 0,
		FreshBootstrapPlanVerified:  true,
		PlannedControllerInstanceID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		PlannedControllerZone:       request.Zone, PlannedParentAttachments: 1,
		HelmUninstalled: true, NamespaceRemoved: true,
	}
	if err := backend.validatePlannedBootstrapAbortEvidence(request, inventory, valid); err != nil {
		t.Fatalf("validatePlannedBootstrapAbortEvidence(valid) error = %v", err)
	}

	tests := map[string]func(*plannedBootstrapAbortEvidence){
		"wrong schema":          func(proof *plannedBootstrapAbortEvidence) { proof.SchemaVersion = "1" },
		"reused cluster":        func(proof *plannedBootstrapAbortEvidence) { proof.ClusterCreatedByRun = false },
		"foreign parent":        func(proof *plannedBootstrapAbortEvidence) { proof.ParentB = "99999999-9999-4999-8999-999999999999" },
		"missing durable plan":  func(proof *plannedBootstrapAbortEvidence) { proof.FreshBootstrapPlanVerified = false },
		"wrong controller zone": func(proof *plannedBootstrapAbortEvidence) { proof.PlannedControllerZone = "fr-par-2" },
		"negative parent count": func(proof *plannedBootstrapAbortEvidence) {
			proof.ParentAAttachments = -1
			proof.ParentAReportedAttachments = -1
		},
		"multiple parent entries": func(proof *plannedBootstrapAbortEvidence) {
			proof.ParentAAttachments = 2
			proof.ParentAReportedAttachments = 2
			proof.PlannedParentAttachments = 2
		},
		"changed attachment count": func(proof *plannedBootstrapAbortEvidence) {
			proof.ParentAReportedAttachments = 0
		},
		"workload state":      func(proof *plannedBootstrapAbortEvidence) { proof.WorkloadPods = 1 },
		"already detached":    func(proof *plannedBootstrapAbortEvidence) { proof.ParentAttachmentsAbsent = true },
		"release not removed": func(proof *plannedBootstrapAbortEvidence) { proof.HelmUninstalled = false },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			proof := valid
			mutate(&proof)
			if err := backend.validatePlannedBootstrapAbortEvidence(request, inventory, proof); err == nil {
				t.Fatal("mismatched proof was accepted")
			}
		})
	}
}

func TestValidatePlannedParentAttachmentSnapshotAcceptsOnlyMonotonicExactSubset(t *testing.T) {
	const (
		parentID   = "77777777-7777-4777-8777-777777777777"
		instanceID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		zoneName   = "fr-par-1"
	)
	zone := scw.Zone(zoneName)
	exact := &fileapi.Attachment{
		FilesystemID: parentID, ResourceID: instanceID, Zone: &zone,
		ResourceType: fileapi.AttachmentResourceTypeInstanceServer,
	}
	if err := validatePlannedParentAttachmentSnapshot(parentID, instanceID, zoneName, 1, 1, []*fileapi.Attachment{exact}); err != nil {
		t.Fatalf("validate exact planned attachment: %v", err)
	}
	if err := validatePlannedParentAttachmentSnapshot(parentID, instanceID, zoneName, 1, 0, nil); err != nil {
		t.Fatalf("validate detached monotonic subset: %v", err)
	}

	tests := map[string]struct {
		maximum  int
		reported uint32
		values   []*fileapi.Attachment
	}{
		"attachment added beyond proof": {maximum: 0, reported: 1, values: []*fileapi.Attachment{exact}},
		"provider counters disagree":    {maximum: 1, reported: 0, values: []*fileapi.Attachment{exact}},
		"nil attachment":                {maximum: 1, reported: 1, values: []*fileapi.Attachment{nil}},
		"nil zone": {maximum: 1, reported: 1, values: []*fileapi.Attachment{{
			FilesystemID: parentID, ResourceID: instanceID, ResourceType: fileapi.AttachmentResourceTypeInstanceServer,
		}}},
		"foreign instance": {maximum: 1, reported: 1, values: []*fileapi.Attachment{{
			FilesystemID: parentID, ResourceID: "99999999-9999-4999-8999-999999999999", Zone: &zone,
			ResourceType: fileapi.AttachmentResourceTypeInstanceServer,
		}}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validatePlannedParentAttachmentSnapshot(parentID, instanceID, zoneName, test.maximum, test.reported, test.values); err == nil {
				t.Fatal("unsafe attachment snapshot was accepted")
			}
		})
	}
}
