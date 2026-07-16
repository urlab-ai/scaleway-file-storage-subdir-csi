package coordination

import (
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func validAbnormalApproval(t *testing.T) OperatorApproval {
	t.Helper()
	holder := validHolderEvidence(t)
	return OperatorApproval{
		SecretUID: "55555555-5555-4555-8555-555555555555", Immutable: true,
		SchemaVersion: volume.SchemaVersionV1, Mode: ApprovalAbnormalTakeover,
		RequestID:      "66666666-6666-4666-8666-666666666666",
		InstallationID: holder.InstallationID, ActiveClusterUID: holder.ActiveClusterUID,
		PreviousHolderPodUID: holder.PodUID, PreviousHolderNodeName: holder.NodeName,
		PreviousHolderCSINodeID: holder.CSINodeID, PreviousHolderInstanceID: holder.InstanceID,
		PreviousHolderZone: holder.Zone, Reason: "Previous controller Instance was stopped and detached",
		ApprovedAt: "2026-07-13T15:00:00Z", ExpiresAt: "2026-07-13T16:00:00Z",
	}
}

func TestAbnormalApprovalRequiresFreshExactPreviousHolder(t *testing.T) {
	approval := validAbnormalApproval(t)
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	observed := time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC)
	if err := approval.ValidateAt(now, observed); err != nil {
		t.Fatalf("ValidateAt() error = %v", err)
	}
	if err := approval.ValidateAbnormalHolder(validHolderEvidence(t)); err != nil {
		t.Fatalf("ValidateAbnormalHolder() error = %v", err)
	}
	approval.PreviousHolderInstanceID = "77777777-7777-4777-8777-777777777777"
	if err := approval.ValidateAt(now, observed); err == nil {
		t.Fatal("ValidateAt(mismatched CSI identity) error = nil")
	}
}

func TestApprovalRejectsMutableExpiredOrOverlongValidity(t *testing.T) {
	base := validAbnormalApproval(t)
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	observed := time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC)
	tests := map[string]func(*OperatorApproval){
		"mutable":  func(value *OperatorApproval) { value.Immutable = false },
		"expired":  func(value *OperatorApproval) { value.ExpiresAt = "2026-07-13T15:29:59Z" },
		"overlong": func(value *OperatorApproval) { value.ExpiresAt = "2026-07-13T16:00:01Z" },
		"stale":    func(value *OperatorApproval) { value.ApprovedAt = "2026-07-13T14:58:00Z" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			approval := base
			mutate(&approval)
			if err := approval.ValidateAt(now, observed); err == nil {
				t.Fatal("ValidateAt() error = nil")
			}
		})
	}
}

func TestMissingLeaseApprovalRequiresCheckpointAndAllInstanceFence(t *testing.T) {
	approval := validAbnormalApproval(t)
	approval.Mode = ApprovalMissingLeaseRecovery
	approval.PreviousHolderPodUID = ""
	approval.PreviousHolderNodeName = ""
	approval.PreviousHolderCSINodeID = ""
	approval.PreviousHolderInstanceID = ""
	approval.PreviousHolderZone = ""
	approval.CheckpointRequestID = "77777777-7777-4777-8777-777777777777"
	approval.CheckpointManifestSHA256 = "sha256:" + strings.Repeat("a", 64)
	approval.RecoveryFenceScope = RecoveryFenceAllPreRecoveryInstances
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	observed := time.Date(2026, 7, 13, 14, 59, 0, 0, time.UTC)
	if err := approval.ValidateAt(now, observed); err != nil {
		t.Fatalf("ValidateAt(missing Lease) error = %v", err)
	}
	if err := approval.ValidateCheckpoint(approval.CheckpointRequestID, approval.CheckpointManifestSHA256); err != nil {
		t.Fatalf("ValidateCheckpoint() error = %v", err)
	}
	approval.RecoveryFenceScope = "checkpoint-holder-only"
	if err := approval.ValidateAt(now, observed); err == nil {
		t.Fatal("ValidateAt(weak recovery fence) error = nil")
	}
}

func TestApprovalConsumptionBindsSecretRequestModeAndConsumer(t *testing.T) {
	approval := validAbnormalApproval(t)
	consumption, err := NewApprovalConsumption(approval, "88888888-8888-4888-8888-888888888888", time.Date(2026, 7, 13, 15, 31, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewApprovalConsumption() error = %v", err)
	}
	if consumption.SecretUID != approval.SecretUID || consumption.RequestID != approval.RequestID || consumption.Mode != approval.Mode {
		t.Fatalf("approval consumption = %#v", consumption)
	}
	annotations, err := ApplyApprovalConsumption(map[string]string{"unrelated": "preserved"}, consumption)
	if err != nil {
		t.Fatalf("ApplyApprovalConsumption() error = %v", err)
	}
	decoded, present, err := ParseApprovalConsumption(annotations)
	if err != nil || !present || decoded != consumption {
		t.Fatalf("ParseApprovalConsumption() = %#v, %v, %v", decoded, present, err)
	}
	if annotations["unrelated"] != "preserved" {
		t.Fatal("ApplyApprovalConsumption() removed unrelated annotation")
	}
	if _, err := ApplyApprovalConsumption(annotations, consumption); err == nil {
		t.Fatal("ApplyApprovalConsumption(reuse) error = nil")
	}
}

func TestParseApprovalConsumptionRejectsPartialOrUnknownAudit(t *testing.T) {
	approval := validAbnormalApproval(t)
	consumption, err := NewApprovalConsumption(approval, "88888888-8888-4888-8888-888888888888", time.Date(2026, 7, 13, 15, 31, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewApprovalConsumption() error = %v", err)
	}
	annotations, err := consumption.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	delete(annotations, approvalConsumptionModeAnnotation)
	if _, present, err := ParseApprovalConsumption(annotations); err == nil || !present {
		t.Fatalf("ParseApprovalConsumption(partial) = present %v, error %v", present, err)
	}
	annotations, err = consumption.Annotations()
	if err != nil {
		t.Fatalf("Annotations() error = %v", err)
	}
	annotations["approvalConsumptionFutureField"] = "unsafe"
	if _, present, err := ParseApprovalConsumption(annotations); err == nil || !present {
		t.Fatalf("ParseApprovalConsumption(unknown) = present %v, error %v", present, err)
	}
}
