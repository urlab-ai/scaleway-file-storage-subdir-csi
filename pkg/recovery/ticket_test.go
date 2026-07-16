package recovery

import (
	"bytes"
	"testing"
)

func TestCheckpointTicketCanonicalRoundTripAndInventoryVerification(t *testing.T) {
	candidate := validCheckpointCandidate(t)
	ticket, err := BuildCheckpointTicket(candidate)
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	encoded, err := EncodeCheckpointTicket(ticket)
	if err != nil {
		t.Fatalf("EncodeCheckpointTicket() error = %v", err)
	}
	decoded, err := DecodeCheckpointTicket(encoded)
	if err != nil {
		t.Fatalf("DecodeCheckpointTicket() error = %v", err)
	}
	if err := decoded.VerifyDetailedInventories(candidate.KubernetesObjectInventoryBytes, candidate.ParentInventoryBytes); err != nil {
		t.Fatalf("VerifyDetailedInventories() error = %v", err)
	}
	decoded.Manifest[0] = 'x'
	if err := ticket.Validate(); err != nil {
		t.Fatalf("ticket changed through decoded clone: %v", err)
	}
}

func TestCheckpointTicketDetectsDetailedKubernetesGenerationChange(t *testing.T) {
	candidate := validCheckpointCandidate(t)
	ticket, err := BuildCheckpointTicket(candidate)
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	entries, err := DecodeKubernetesObjectInventory(candidate.KubernetesObjectInventoryBytes)
	if err != nil {
		t.Fatalf("DecodeKubernetesObjectInventory() error = %v", err)
	}
	entries[0].SourceUID = "changed-source-uid"
	summary, changedInventory, err := BuildKubernetesObjectInventory(entries)
	if err != nil {
		t.Fatalf("BuildKubernetesObjectInventory() error = %v", err)
	}
	if summary != candidate.Manifest.KubernetesObjects {
		t.Fatal("source-generation-only change unexpectedly changed restore-stable aggregate")
	}
	if err := ticket.VerifyDetailedInventories(changedInventory, candidate.ParentInventoryBytes); err == nil {
		t.Fatal("VerifyDetailedInventories(changed source generation) error = nil")
	}
}

func TestCheckpointTicketRejectsChangedMissingAndExtraParentInventories(t *testing.T) {
	candidate := validCheckpointCandidate(t)
	ticket, err := BuildCheckpointTicket(candidate)
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	changed := make(map[string][]byte, len(candidate.ParentInventoryBytes))
	for parentID, inventory := range candidate.ParentInventoryBytes {
		changed[parentID] = bytes.Clone(inventory)
		changed[parentID][0] = 'x'
		break
	}
	if err := ticket.VerifyDetailedInventories(candidate.KubernetesObjectInventoryBytes, changed); err == nil {
		t.Fatal("VerifyDetailedInventories(changed parent) error = nil")
	}
	for parentID := range changed {
		delete(changed, parentID)
		break
	}
	if err := ticket.VerifyDetailedInventories(candidate.KubernetesObjectInventoryBytes, changed); err == nil {
		t.Fatal("VerifyDetailedInventories(missing parent) error = nil")
	}
	for parentID, inventory := range candidate.ParentInventoryBytes {
		changed[parentID] = bytes.Clone(inventory)
	}
	changed["99999999-9999-4999-8999-999999999999"] = []byte(`[]`)
	if err := ticket.VerifyDetailedInventories(candidate.KubernetesObjectInventoryBytes, changed); err == nil {
		t.Fatal("VerifyDetailedInventories(extra parent) error = nil")
	}
}

func TestCheckpointTicketRejectsTamperingAndNoncanonicalJSON(t *testing.T) {
	candidate := validCheckpointCandidate(t)
	ticket, err := BuildCheckpointTicket(candidate)
	if err != nil {
		t.Fatalf("BuildCheckpointTicket() error = %v", err)
	}
	tampered := ticket.Clone()
	tampered.KubernetesObjectInventory.SizeBytes++
	if err := tampered.Validate(); err != nil {
		t.Fatalf("structurally valid independent size commitment rejected: %v", err)
	}
	if err := tampered.VerifyDetailedInventories(candidate.KubernetesObjectInventoryBytes, candidate.ParentInventoryBytes); err == nil {
		t.Fatal("tampered inventory size was accepted")
	}
	tampered = ticket.Clone()
	tampered.ManifestSHA256 = "sha256:" + string(bytes.Repeat([]byte{'0'}, 64))
	if err := tampered.Validate(); err == nil {
		t.Fatal("Validate(tampered manifest digest) error = nil")
	}
	encoded, err := EncodeCheckpointTicket(ticket)
	if err != nil {
		t.Fatalf("EncodeCheckpointTicket() error = %v", err)
	}
	noncanonical := append([]byte(" \n"), encoded...)
	if _, err := DecodeCheckpointTicket(noncanonical); err == nil {
		t.Fatal("DecodeCheckpointTicket(noncanonical) error = nil")
	}
}
