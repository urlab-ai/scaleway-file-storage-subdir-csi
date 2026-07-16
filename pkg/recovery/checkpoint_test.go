package recovery

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"scaleway-sfs-subdir-csi/pkg/coordination"
)

func validCheckpointManifest(t *testing.T) CheckpointManifest {
	t.Helper()
	holder, err := coordination.NewHolderEvidence(
		"55555555-5555-4555-8555-555555555555", "worker-a",
		"fr-par-1/66666666-6666-4666-8666-666666666666",
		"66666666-6666-4666-8666-666666666666", "fr-par-1",
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	manifest, err := NewCheckpointManifest(
		"77777777-7777-4777-8777-777777777777", "sfs-subdir.csi.example.com",
		holder.InstallationID, holder.ActiveClusterUID, "1.0.0",
		"88888888-8888-4888-8888-888888888888", holder,
		time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC),
		[]ImageDigest{{Name: "node", Digest: "sha256:" + strings.Repeat("b", 64)}, {Name: "controller", Digest: "sha256:" + strings.Repeat("a", 64)}},
		ObjectInventorySummary{Count: 10, AggregateSHA256: "sha256:" + strings.Repeat("c", 64)},
		[]ParentInventory{{ParentFilesystemID: "33333333-3333-4333-8333-333333333333", ParentOwnerSHA256: "sha256:" + strings.Repeat("d", 64), RecordCount: 5, AggregateSHA256: "sha256:" + strings.Repeat("e", 64)}},
	)
	if err != nil {
		t.Fatalf("NewCheckpointManifest() error = %v", err)
	}
	return manifest
}

func TestCheckpointManifestCanonicalRoundTripAndDigest(t *testing.T) {
	manifest := validCheckpointManifest(t)
	encoded, err := EncodeCheckpointManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	decoded, err := DecodeCheckpointManifest(encoded)
	if err != nil {
		t.Fatalf("DecodeCheckpointManifest() error = %v", err)
	}
	reencoded, err := EncodeCheckpointManifest(decoded)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest(decoded) error = %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("checkpoint manifest changed across canonical round trip")
	}
	digest, err := CheckpointManifestSHA256(manifest)
	if err != nil || !validSHA256Digest(digest) {
		t.Fatalf("CheckpointManifestSHA256() = %q, %v", digest, err)
	}
}

func TestCheckpointManifestRejectsUnknownNoncanonicalAndWrongLease(t *testing.T) {
	manifest := validCheckpointManifest(t)
	encoded, err := EncodeCheckpointManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	withUnknown := append(bytes.Clone(encoded[:len(encoded)-1]), []byte(`,"future":true}`)...)
	if _, err := DecodeCheckpointManifest(withUnknown); err == nil {
		t.Fatal("DecodeCheckpointManifest(unknown field) error = nil")
	}
	noncanonical := append([]byte(" \n"), encoded...)
	if _, err := DecodeCheckpointManifest(noncanonical); err == nil {
		t.Fatal("DecodeCheckpointManifest(noncanonical) error = nil")
	}
	manifest.LeadershipLeaseName = "replacement"
	if err := manifest.Validate(); err == nil {
		t.Fatal("Validate(wrong Lease name) error = nil")
	}
}

func TestCheckpointManifestRejectsOversizedInputAndEncoding(t *testing.T) {
	if _, err := DecodeCheckpointManifest(bytes.Repeat([]byte{'x'}, maxCheckpointManifestBytes+1)); err == nil {
		t.Fatal("DecodeCheckpointManifest(oversized) error = nil")
	}
	manifest := validCheckpointManifest(t)
	manifest.Images = make([]ImageDigest, 12_000)
	for index := range manifest.Images {
		manifest.Images[index] = ImageDigest{
			Name:   fmt.Sprintf("image-%05d", index),
			Digest: "sha256:" + strings.Repeat("a", 64),
		}
	}
	if _, err := EncodeCheckpointManifest(manifest); err == nil {
		t.Fatal("EncodeCheckpointManifest(oversized) error = nil")
	}
}

func TestCheckpointSecretRequiresFixedImmutableSingleKeyEnvelope(t *testing.T) {
	manifest := validCheckpointManifest(t)
	encoded, err := EncodeCheckpointManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	secret := CheckpointSecret{
		Name: "sfs-subdir-checkpoint", Type: "Opaque", Immutable: true,
		Data: map[string][]byte{"checkpoint.json": encoded},
	}
	decoded, digest, err := ValidateCheckpointSecret(secret)
	if err != nil {
		t.Fatalf("ValidateCheckpointSecret() error = %v", err)
	}
	if decoded.CheckpointRequestID != manifest.CheckpointRequestID || digest != SHA256Digest(encoded) {
		t.Fatalf("checkpoint Secret result = %#v, %q", decoded, digest)
	}
	secret.Data["extra"] = []byte("unsafe")
	if _, _, err := ValidateCheckpointSecret(secret); err == nil {
		t.Fatal("ValidateCheckpointSecret(extra key) error = nil")
	}
	delete(secret.Data, "extra")
	secret.Immutable = false
	if _, _, err := ValidateCheckpointSecret(secret); err == nil {
		t.Fatal("ValidateCheckpointSecret(mutable) error = nil")
	}
}
