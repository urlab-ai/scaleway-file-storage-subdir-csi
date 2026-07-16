package releasequalification

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

func testCandidate() CandidateManifest {
	digest := "sha256:" + strings.Repeat("1", 64)
	return CandidateManifest{
		SchemaVersion: SchemaVersionV1, ReleaseTag: "v1.2.3", Version: "1.2.3",
		GitCommit: strings.Repeat("a", 40), DriverName: "driver.example.org",
		QualifiedCommercialTypes: []string{"TYPE-A"}, ChartFile: "chart.tgz", ChartSHA256: digest,
		ValuesFile: "values.yaml", ValuesSHA256: digest,
		DriverImage: "registry.example.org/driver@" + digest, ChecksumManifestFile: "checksums.txt",
		ChecksumManifestSHA256: digest,
		Images: []e2eplan.ImageDigest{
			{Name: "driver", Reference: "registry.example.org/driver@" + digest},
			{Name: "external-attacher", Reference: "registry.example.org/attacher@" + digest},
			{Name: "external-provisioner", Reference: "registry.example.org/provisioner@" + digest},
			{Name: "csi-node-driver-registrar", Reference: "registry.example.org/registrar@" + digest},
			{Name: "livenessprobe", Reference: "registry.example.org/liveness@" + digest},
		},
	}
}

func TestQualificationBindsExactCandidateAndCleanup(t *testing.T) {
	candidate := testCandidate()
	candidateBytes, err := EncodeCandidate(candidate)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(candidateBytes)
	digest := "sha256:" + strings.Repeat("2", 64)
	qualification := QualificationManifest{
		SchemaVersion: SchemaVersionV1, CandidateSHA256: "sha256:" + hex.EncodeToString(sum[:]),
		LinuxEvidenceFile: "linux.json", LinuxEvidenceSHA256: digest,
		KindEvidenceFile: "kind.json", KindEvidenceSHA256: digest,
		Runs: []QualificationRun{{
			RunID: "11111111-1111-4111-8111-111111111111", ProjectID: "22222222-2222-4222-8222-222222222222",
			Region: "fr-par", Profile: e2eplan.ProfileReleaseCandidate, CommercialType: "TYPE-A", KapsuleEvidenceFile: "kapsule.json", KapsuleEvidenceSHA256: digest,
			CleanupAuditFile: "cleanup.json", CleanupAuditSHA256: digest, Succeeded: true, CleanupComplete: true,
		}},
	}
	if _, err := EncodeQualification(qualification, candidate, candidateBytes); err != nil {
		t.Fatalf("EncodeQualification() error = %v", err)
	}
	qualification.Runs[0].CleanupComplete = false
	if err := qualification.Validate(candidate, candidateBytes); err == nil {
		t.Fatal("Validate(incomplete cleanup) error = nil")
	}
}

func TestCandidateManifestDigestRequiresCanonicalBytes(t *testing.T) {
	encoded, err := EncodeCandidate(testCandidate())
	if err != nil {
		t.Fatal(err)
	}
	withNewline := append(append([]byte(nil), encoded...), '\n')
	digest, err := CandidateManifestDigest(withNewline)
	if err != nil || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("CandidateManifestDigest() = %q, %v", digest, err)
	}
	pretty := append([]byte(" \n"), encoded...)
	if _, err := CandidateManifestDigest(pretty); err == nil {
		t.Fatal("CandidateManifestDigest(non-canonical) error = nil")
	}
}

func TestValidateEvidenceCandidateRejectsAnotherCandidate(t *testing.T) {
	candidate := testCandidate()
	candidateDigest := "sha256:" + strings.Repeat("3", 64)
	evidence := e2erunner.Evidence{ArtifactDigests: e2eplan.Artifacts{
		GitCommit: candidate.GitCommit, CandidateDigest: candidateDigest,
		ChartDigest: candidate.ChartSHA256, Images: candidate.Images,
	}}
	if err := ValidateEvidenceCandidate(evidence, candidate, candidateDigest); err != nil {
		t.Fatalf("ValidateEvidenceCandidate() error = %v", err)
	}

	other := candidate
	other.GitCommit = strings.Repeat("b", 40)
	if err := ValidateEvidenceCandidate(evidence, other, candidateDigest); err == nil {
		t.Fatal("ValidateEvidenceCandidate(other commit) error = nil")
	}

	evidence.ArtifactDigests.Images = slices.Clone(evidence.ArtifactDigests.Images)
	evidence.ArtifactDigests.Images[0].Reference = "registry.example.org/driver@sha256:" + strings.Repeat("4", 64)
	if err := ValidateEvidenceCandidate(evidence, candidate, candidateDigest); err == nil {
		t.Fatal("ValidateEvidenceCandidate(other image) error = nil")
	}
}
