package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/releasequalification"
)

func TestCandidateAndQualificationVerifyExactArtifacts(t *testing.T) {
	root := t.TempDir()
	candidateDir := filepath.Join(root, "candidate")
	qualificationDir := filepath.Join(root, "qualification")
	if err := os.MkdirAll(candidateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(qualificationDir, 0o755); err != nil {
		t.Fatal(err)
	}
	chart := filepath.Join(candidateDir, "chart.tgz")
	if err := os.WriteFile(chart, []byte("chart"), 0o644); err != nil {
		t.Fatal(err)
	}
	chartSum := sha256.Sum256([]byte("chart"))
	values := filepath.Join(candidateDir, "values.yaml")
	if err := os.WriteFile(values, []byte("values"), 0o644); err != nil {
		t.Fatal(err)
	}
	valuesSum := sha256.Sum256([]byte("values"))
	checksums := filepath.Join(candidateDir, "checksums.txt")
	checksumContent := hex.EncodeToString(chartSum[:]) + "  chart.tgz\n" + hex.EncodeToString(valuesSum[:]) + "  values.yaml\n"
	if err := os.WriteFile(checksums, []byte(checksumContent), 0o644); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("1", 64)
	candidatePath := filepath.Join(root, "candidate.json")
	args := []string{"candidate", "--output=" + candidatePath, "--release-tag=v1.2.3", "--commit=" + strings.Repeat("a", 40),
		"--driver-name=driver.example.org", "--commercial-types=TYPE-A", "--chart=" + chart, "--values=" + values, "--checksums=" + checksums,
		"--driver-image=registry.example/driver@" + digest,
		"--provisioner-image=registry.example/provisioner@" + digest,
		"--attacher-image=registry.example/attacher@" + digest,
		"--registrar-image=registry.example/registrar@" + digest,
		"--liveness-image=registry.example/liveness@" + digest}
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	candidateBytesWithNewline, err := os.ReadFile(candidatePath)
	if err != nil {
		t.Fatal(err)
	}
	candidateBytes := []byte(strings.TrimSuffix(string(candidateBytesWithNewline), "\n"))
	candidate, err := releasequalification.DecodeCandidate(candidateBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := releasequalification.VerifyCandidateDirectory(candidateDir, candidate); err != nil {
		t.Fatalf("VerifyCandidateDirectory() error = %v", err)
	}
	evidenceDigest := func(name string) string {
		path := filepath.Join(qualificationDir, name)
		if err := os.WriteFile(path, []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
		digest, err := releasequalification.DigestFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return digest
	}
	candidateSum := sha256.Sum256(candidateBytes)
	qualification := releasequalification.QualificationManifest{
		SchemaVersion:     releasequalification.SchemaVersionV1,
		CandidateSHA256:   "sha256:" + hex.EncodeToString(candidateSum[:]),
		LinuxEvidenceFile: "linux.json", LinuxEvidenceSHA256: evidenceDigest("linux.json"),
		KindEvidenceFile: "kind.json", KindEvidenceSHA256: evidenceDigest("kind.json"),
		Runs: []releasequalification.QualificationRun{{
			RunID: "11111111-1111-4111-8111-111111111111", ProjectID: "22222222-2222-4222-8222-222222222222",
			Region: "fr-par", Profile: e2eplan.ProfileReleaseCandidate, CommercialType: "TYPE-A", Succeeded: true, CleanupComplete: true,
			KapsuleEvidenceFile: "kapsule.json", KapsuleEvidenceSHA256: evidenceDigest("kapsule.json"),
			CleanupAuditFile: "cleanup.json", CleanupAuditSHA256: evidenceDigest("cleanup.json"),
		}},
	}
	qualificationBytes, err := releasequalification.EncodeQualification(qualification, candidate, candidateBytes)
	if err != nil {
		t.Fatal(err)
	}
	qualificationPath := filepath.Join(root, "qualification.json")
	if err := os.WriteFile(qualificationPath, qualificationBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"verify", "--candidate=" + candidatePath, "--candidate-dir=" + candidateDir,
		"--qualification=" + qualificationPath, "--qualification-dir=" + qualificationDir}); err == nil {
		t.Fatal("verify accepted digests without semantic Kapsule evidence")
	}
	if err := os.WriteFile(chart, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := releasequalification.VerifyCandidateDirectory(candidateDir, candidate); err == nil {
		t.Fatal("VerifyCandidateDirectory(changed chart) error = nil")
	}
}

func TestCleanupAuditMustMatchItsExactRunEvidence(t *testing.T) {
	inventory := e2ecleanup.Inventory{
		SchemaVersion: e2ecleanup.SchemaVersionV1, Phase: e2ecleanup.PhaseComplete,
		Profile: "base", RunID: "11111111-1111-4111-8111-111111111111",
		ProjectID: "22222222-2222-4222-8222-222222222222", Region: "fr-par",
		ResourcePrefix: "e2e-11111111-1111-4111-8111-111111111111",
		OwnershipTag:   "sfs-subdir-e2e-run=11111111-1111-4111-8111-111111111111",
		ObservedAt:     "2026-07-16T10:00:00Z", Resources: []e2ecleanup.Resource{},
	}
	evidence := e2erunner.Evidence{
		RunID: inventory.RunID, ProjectID: inventory.ProjectID, Region: inventory.Region,
		FinalInventory: inventory,
	}
	if err := e2erunner.ValidateCleanupAudit(evidence, inventory); err != nil {
		t.Fatalf("ValidateCleanupAudit() error = %v", err)
	}

	otherRun := inventory
	otherRun.RunID = "33333333-3333-4333-8333-333333333333"
	otherRun.ResourcePrefix = "e2e-33333333-3333-4333-8333-333333333333"
	otherRun.OwnershipTag = "sfs-subdir-e2e-run=33333333-3333-4333-8333-333333333333"
	if err := e2erunner.ValidateCleanupAudit(evidence, otherRun); err == nil {
		t.Fatal("ValidateCleanupAudit(other run) error = nil")
	}

	tampered := inventory
	tampered.ObservedAt = "2026-07-16T10:00:01Z"
	if err := e2erunner.ValidateCleanupAudit(evidence, tampered); err == nil {
		t.Fatal("ValidateCleanupAudit(tampered inventory) error = nil")
	}
}
