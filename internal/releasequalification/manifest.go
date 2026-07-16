// Package releasequalification validates the small immutable handoff between
// candidate construction, real qualification, and final publication.
package releasequalification

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/compatibility"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const SchemaVersionV1 = "1"

var (
	sha256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	commitPattern = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
)

// CandidateManifest names every immutable subject that qualification and
// promotion must share. Files are relative basenames within the candidate
// artifact; paths and mutable tags are never accepted as authority.
type CandidateManifest struct {
	SchemaVersion            string                `json:"schemaVersion"`
	ReleaseTag               string                `json:"releaseTag"`
	Version                  string                `json:"version"`
	GitCommit                string                `json:"gitCommit"`
	DriverName               string                `json:"driverName"`
	QualifiedCommercialTypes []string              `json:"qualifiedCommercialTypes"`
	ChartFile                string                `json:"chartFile"`
	ChartSHA256              string                `json:"chartSha256"`
	ValuesFile               string                `json:"valuesFile"`
	ValuesSHA256             string                `json:"valuesSha256"`
	DriverImage              string                `json:"driverImage"`
	Images                   []e2eplan.ImageDigest `json:"images"`
	ChecksumManifestFile     string                `json:"checksumManifestFile"`
	ChecksumManifestSHA256   string                `json:"checksumManifestSha256"`
}

// QualificationManifest is retained only after local CI and every real
// commercial-type run have completed cleanup for the exact candidate digest.
type QualificationManifest struct {
	SchemaVersion       string             `json:"schemaVersion"`
	CandidateSHA256     string             `json:"candidateSha256"`
	LinuxEvidenceFile   string             `json:"linuxEvidenceFile"`
	LinuxEvidenceSHA256 string             `json:"linuxEvidenceSha256"`
	KindEvidenceFile    string             `json:"kindEvidenceFile"`
	KindEvidenceSHA256  string             `json:"kindEvidenceSha256"`
	Runs                []QualificationRun `json:"runs"`
}

// QualificationRun binds one commercial type to one disposable Kapsule run
// whose retained evidence proves success and complete cleanup.
type QualificationRun struct {
	RunID                 string `json:"runId"`
	ProjectID             string `json:"projectId"`
	Region                string `json:"region"`
	Profile               string `json:"profile"`
	CommercialType        string `json:"commercialType"`
	KapsuleEvidenceFile   string `json:"kapsuleEvidenceFile"`
	KapsuleEvidenceSHA256 string `json:"kapsuleEvidenceSha256"`
	CleanupAuditFile      string `json:"cleanupAuditFile"`
	CleanupAuditSHA256    string `json:"cleanupAuditSha256"`
	Succeeded             bool   `json:"succeeded"`
	CleanupComplete       bool   `json:"cleanupComplete"`
}

func (manifest CandidateManifest) Validate() error {
	if manifest.SchemaVersion != SchemaVersionV1 || !strings.HasPrefix(manifest.ReleaseTag, "v") || strings.TrimPrefix(manifest.ReleaseTag, "v") != manifest.Version {
		return fmt.Errorf("candidate release version identity is invalid")
	}
	if !commitPattern.MatchString(manifest.GitCommit) {
		return fmt.Errorf("candidate Git commit must be a complete lowercase SHA-1")
	}
	if err := volume.ValidateDriverName(manifest.DriverName); err != nil {
		return err
	}
	if err := compatibility.ValidateCommercialTypes(manifest.QualifiedCommercialTypes); err != nil {
		return err
	}
	if !safeArtifactName(manifest.ChartFile) || !safeArtifactName(manifest.ValuesFile) ||
		!safeArtifactName(manifest.ChecksumManifestFile) || !sha256Pattern.MatchString(manifest.ValuesSHA256) ||
		!sha256Pattern.MatchString(manifest.ChartSHA256) || !sha256Pattern.MatchString(manifest.ChecksumManifestSHA256) {
		return fmt.Errorf("candidate chart or checksum artifact identity is invalid")
	}
	imageRepository, imageDigest, imageFound := strings.Cut(manifest.DriverImage, "@")
	if !imageFound || imageRepository == "" || strings.Contains(imageDigest, "@") || !sha256Pattern.MatchString(imageDigest) {
		return fmt.Errorf("candidate driver image must be repository@sha256")
	}
	wantNames := []string{"csi-node-driver-registrar", "driver", "external-attacher", "external-provisioner", "livenessprobe"}
	if len(manifest.Images) != len(wantNames) {
		return fmt.Errorf("candidate must contain exactly five image identities")
	}
	images := slices.Clone(manifest.Images)
	slices.SortFunc(images, func(left, right e2eplan.ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	for index, image := range images {
		if image.Name != wantNames[index] || !strings.Contains(image.Reference, "@") {
			return fmt.Errorf("candidate image set is invalid")
		}
		parts := strings.Split(image.Reference, "@")
		if len(parts) != 2 || parts[0] == "" || !sha256Pattern.MatchString(parts[1]) {
			return fmt.Errorf("candidate image %q is not immutable", image.Name)
		}
	}
	if images[1].Reference != manifest.DriverImage {
		return fmt.Errorf("candidate driver image fields disagree")
	}
	return nil
}

func (manifest QualificationManifest) Validate(candidate CandidateManifest, candidateBytes []byte) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if manifest.SchemaVersion != SchemaVersionV1 || !safeArtifactName(manifest.LinuxEvidenceFile) ||
		!safeArtifactName(manifest.KindEvidenceFile) || !sha256Pattern.MatchString(manifest.CandidateSHA256) ||
		!sha256Pattern.MatchString(manifest.LinuxEvidenceSHA256) || !sha256Pattern.MatchString(manifest.KindEvidenceSHA256) {
		return fmt.Errorf("qualification manifest identity is invalid")
	}
	sum := sha256.Sum256(candidateBytes)
	if manifest.CandidateSHA256 != "sha256:"+hex.EncodeToString(sum[:]) {
		return fmt.Errorf("qualification references another candidate manifest")
	}
	if len(manifest.Runs) != len(candidate.QualifiedCommercialTypes) {
		return fmt.Errorf("qualification run count differs from commercial allowlist")
	}
	runs := slices.Clone(manifest.Runs)
	slices.SortFunc(runs, func(left, right QualificationRun) int {
		return strings.Compare(left.CommercialType, right.CommercialType)
	})
	for index, run := range runs {
		if run.CommercialType != candidate.QualifiedCommercialTypes[index] {
			return fmt.Errorf("qualification commercial-type set differs from candidate")
		}
		if err := volume.ValidateOperationID(run.RunID); err != nil {
			return err
		}
		if err := volume.ValidateInstallationID(run.ProjectID); err != nil {
			return err
		}
		if run.Region != "fr-par" || run.Profile != e2eplan.ProfileReleaseCandidate || !run.Succeeded || !run.CleanupComplete ||
			!safeArtifactName(run.KapsuleEvidenceFile) || !safeArtifactName(run.CleanupAuditFile) ||
			!sha256Pattern.MatchString(run.KapsuleEvidenceSHA256) || !sha256Pattern.MatchString(run.CleanupAuditSHA256) {
			return fmt.Errorf("qualification run %q is incomplete", run.RunID)
		}
	}
	return nil
}

// DecodeCandidate rejects unknown or duplicate fields and validates one
// candidate manifest before it can become release authority.
func DecodeCandidate(data []byte) (CandidateManifest, error) {
	var manifest CandidateManifest
	if err := strictjson.Decode(data, &manifest); err != nil {
		return CandidateManifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return CandidateManifest{}, err
	}
	return manifest, nil
}

// CandidateManifestDigest returns the identity used by E2E plans and
// qualification. The file may contain the single conventional trailing LF,
// but its JSON bytes must otherwise equal the canonical encoder output.
func CandidateManifestDigest(data []byte) (string, error) {
	canonicalBytes := bytes.TrimSuffix(data, []byte{'\n'})
	manifest, err := DecodeCandidate(canonicalBytes)
	if err != nil {
		return "", err
	}
	encoded, err := EncodeCandidate(manifest)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(canonicalBytes, encoded) {
		return "", fmt.Errorf("candidate manifest bytes are not canonical")
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// DecodeQualification rejects unknown or duplicate fields and validates one
// qualification manifest against the exact candidate bytes it names.
func DecodeQualification(data []byte, candidate CandidateManifest, candidateBytes []byte) (QualificationManifest, error) {
	var manifest QualificationManifest
	if err := strictjson.Decode(data, &manifest); err != nil {
		return QualificationManifest{}, err
	}
	if err := manifest.Validate(candidate, candidateBytes); err != nil {
		return QualificationManifest{}, err
	}
	return manifest, nil
}

func EncodeCandidate(manifest CandidateManifest) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	images := slices.Clone(manifest.Images)
	slices.SortFunc(images, func(left, right e2eplan.ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	manifest.Images = images
	return canonicaljson.Marshal(manifest)
}

func EncodeQualification(manifest QualificationManifest, candidate CandidateManifest, candidateBytes []byte) ([]byte, error) {
	if err := manifest.Validate(candidate, candidateBytes); err != nil {
		return nil, err
	}
	runs := slices.Clone(manifest.Runs)
	slices.SortFunc(runs, func(left, right QualificationRun) int {
		return strings.Compare(left.CommercialType, right.CommercialType)
	})
	manifest.Runs = runs
	return canonicaljson.Marshal(manifest)
}

func safeArtifactName(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00\r\n")
}
