package releasequalification

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const maxEvidenceFileBytes int64 = 512 << 20

// DigestFile returns the lowercase sha256 identity of one exact regular,
// non-symlink artifact. Release authority must never follow a replaced link.
func DigestFile(path string) (digest string, returnErr error) {
	file, err := openExactRegular(path)
	if err != nil {
		return "", err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, maxEvidenceFileBytes+1)); err != nil {
		return "", fmt.Errorf("hash release artifact %q: %w", filepath.Base(path), err)
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", fmt.Errorf("measure release artifact %q: %w", filepath.Base(path), err)
	}
	if position > maxEvidenceFileBytes {
		return "", fmt.Errorf("release artifact %q exceeds %d bytes", filepath.Base(path), maxEvidenceFileBytes)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// VerifyCandidateDirectory proves the chart and every file named by the
// checksum manifest are unchanged from candidate construction.
func VerifyCandidateDirectory(directory string, manifest CandidateManifest) error {
	return VerifyCandidateArtifacts(directory, manifest)
}

// VerifyCandidateArtifacts verifies the candidate directory and additionally
// requires every named basename to be covered by the retained checksum
// manifest. It is used by qualification to bind the exact native csi-admin
// binary that performs destructive operator workflows.
func VerifyCandidateArtifacts(directory string, manifest CandidateManifest, requiredNames ...string) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := requireDigest(directory, manifest.ChartFile, manifest.ChartSHA256); err != nil {
		return err
	}
	if err := requireDigest(directory, manifest.ValuesFile, manifest.ValuesSHA256); err != nil {
		return err
	}
	if err := requireDigest(directory, manifest.ChecksumManifestFile, manifest.ChecksumManifestSHA256); err != nil {
		return err
	}
	required := []string{manifest.ChartFile, manifest.ValuesFile}
	required = append(required, requiredNames...)
	return verifyChecksumManifest(directory, manifest.ChecksumManifestFile, required...)
}

// VerifyQualificationDirectory proves that retained Linux, kind, Kapsule, and
// cleanup evidence bytes match the closed qualification manifest.
func VerifyQualificationDirectory(directory string, manifest QualificationManifest, candidate CandidateManifest) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if err := requireDigest(directory, manifest.LinuxEvidenceFile, manifest.LinuxEvidenceSHA256); err != nil {
		return err
	}
	if err := requireDigest(directory, manifest.KindEvidenceFile, manifest.KindEvidenceSHA256); err != nil {
		return err
	}
	for _, run := range manifest.Runs {
		if err := requireDigest(directory, run.KapsuleEvidenceFile, run.KapsuleEvidenceSHA256); err != nil {
			return err
		}
		if err := requireDigest(directory, run.CleanupAuditFile, run.CleanupAuditSHA256); err != nil {
			return err
		}
		evidenceBytes, err := readExactRegular(filepath.Join(directory, run.KapsuleEvidenceFile))
		if err != nil {
			return err
		}
		var evidence e2erunner.Evidence
		if err := strictjson.Decode(evidenceBytes, &evidence); err != nil {
			return fmt.Errorf("decode Kapsule evidence %q: %w", run.KapsuleEvidenceFile, err) //nolint:staticcheck // Kapsule is a product name.
		}
		if err := evidence.Validate(); err != nil {
			return fmt.Errorf("validate Kapsule evidence %q: %w", run.KapsuleEvidenceFile, err) //nolint:staticcheck // Kapsule is a product name.
		}
		if err := ValidateEvidenceCandidate(evidence, candidate, manifest.CandidateSHA256); err != nil {
			return fmt.Errorf("validate Kapsule evidence %q candidate: %w", run.KapsuleEvidenceFile, err) //nolint:staticcheck // Kapsule is a product name.
		}
		if evidence.RunID != run.RunID || evidence.ProjectID != run.ProjectID || evidence.Region != run.Region ||
			evidence.FinalInventory.Profile != run.Profile || evidence.CommercialType != run.CommercialType {
			return fmt.Errorf("Kapsule evidence %q differs from qualification run identity", run.KapsuleEvidenceFile) //nolint:staticcheck // Kapsule is a product name.
		}
		cleanupBytes, err := readExactRegular(filepath.Join(directory, run.CleanupAuditFile))
		if err != nil {
			return err
		}
		var cleanup e2ecleanup.Inventory
		if err := strictjson.Decode(cleanupBytes, &cleanup); err != nil {
			return fmt.Errorf("decode cleanup audit %q: %w", run.CleanupAuditFile, err)
		}
		if err := e2erunner.ValidateCleanupAudit(evidence, cleanup); err != nil {
			return fmt.Errorf("validate cleanup audit %q: %w", run.CleanupAuditFile, err)
		}
	}
	return nil
}

// ValidateEvidenceCandidate binds one qualified run to the exact source,
// candidate manifest, chart, and immutable image set selected for promotion.
// It is intentionally shared by qualification creation and final verification
// so a hand-crafted manifest cannot bypass the semantic binding.
func ValidateEvidenceCandidate(evidence e2erunner.Evidence, candidate CandidateManifest, candidateDigest string) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if evidence.ArtifactDigests.GitCommit != candidate.GitCommit || evidence.ArtifactDigests.CandidateDigest != candidateDigest ||
		evidence.ArtifactDigests.ChartDigest != candidate.ChartSHA256 || !sameArtifactImages(evidence.ArtifactDigests.Images, candidate.Images) {
		return fmt.Errorf("Kapsule evidence names another candidate") //nolint:staticcheck // Kapsule is a product name.
	}
	return nil
}

func sameArtifactImages(left, right []e2eplan.ImageDigest) bool {
	left = slices.Clone(left)
	right = slices.Clone(right)
	compare := func(a, b e2eplan.ImageDigest) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(left, compare)
	slices.SortFunc(right, compare)
	return slices.Equal(left, right)
}

func readExactRegular(path string) (data []byte, returnErr error) {
	file, err := openExactRegular(path)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	data, err = io.ReadAll(io.LimitReader(file, maxEvidenceFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read release artifact %q: %w", filepath.Base(path), err)
	}
	if int64(len(data)) > maxEvidenceFileBytes {
		return nil, fmt.Errorf("release artifact %q exceeds %d bytes", filepath.Base(path), maxEvidenceFileBytes)
	}
	return data, nil
}

func verifyChecksumManifest(directory, manifestName string, requiredNames ...string) (returnErr error) {
	file, err := openExactRegular(filepath.Join(directory, manifestName))
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	scanner := bufio.NewScanner(io.LimitReader(file, maxEvidenceFileBytes+1))
	seen := make(map[string]struct{})
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || len(fields[0]) != 64 || !safeArtifactName(strings.TrimPrefix(fields[1], "*")) {
			return fmt.Errorf("release checksum manifest contains an invalid entry")
		}
		name := strings.TrimPrefix(fields[1], "*")
		if _, exists := seen[name]; exists || name == manifestName {
			return fmt.Errorf("release checksum manifest contains duplicate or recursive entry %q", name)
		}
		seen[name] = struct{}{}
		digest := "sha256:" + fields[0]
		if !sha256Pattern.MatchString(digest) {
			return fmt.Errorf("release checksum manifest contains an invalid digest")
		}
		if err := requireDigest(directory, name, digest); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read release checksum manifest: %w", err)
	}
	for _, requiredName := range requiredNames {
		if _, found := seen[requiredName]; !found {
			return fmt.Errorf("release checksum manifest does not cover artifact %q", requiredName)
		}
	}
	return nil
}

func requireDigest(directory, name, expected string) error {
	if !safeArtifactName(name) || !sha256Pattern.MatchString(expected) {
		return fmt.Errorf("release artifact identity is invalid")
	}
	actual, err := DigestFile(filepath.Join(directory, name))
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("release artifact %q digest differs from retained authority", name)
	}
	return nil
}

func openExactRegular(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect release artifact %q: %w", filepath.Base(path), err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxEvidenceFileBytes {
		return nil, fmt.Errorf("release artifact %q is not an exact bounded regular file", filepath.Base(path))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open release artifact %q: %w", filepath.Base(path), err)
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, fmt.Errorf("release artifact %q changed during open", filepath.Base(path))
	}
	return file, nil
}
