// Command release-qualification creates the immutable release-candidate
// identity and verifies a separately retained qualification before promotion.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/internal/compatibility"
	"scaleway-sfs-subdir-csi/internal/e2ecleanup"
	"scaleway-sfs-subdir-csi/internal/e2eplan"
	"scaleway-sfs-subdir-csi/internal/e2erunner"
	"scaleway-sfs-subdir-csi/internal/releasequalification"
	"scaleway-sfs-subdir-csi/internal/strictjson"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "release qualification:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return fmt.Errorf("usage: release-qualification <candidate|qualification|verify> [flags]")
	}
	switch arguments[0] {
	case "candidate":
		return writeCandidate(arguments[1:])
	case "qualification":
		return writeQualification(arguments[1:])
	case "verify":
		return verifyQualification(arguments[1:])
	default:
		return fmt.Errorf("unknown command %q", arguments[0])
	}
}

type repeatedStrings []string

func (values *repeatedStrings) String() string { return strings.Join(*values, ",") }
func (values *repeatedStrings) Set(value string) error {
	if value == "" {
		return fmt.Errorf("repeated path is empty")
	}
	*values = append(*values, value)
	return nil
}

func writeCandidate(arguments []string) error {
	flags := flag.NewFlagSet("candidate", flag.ContinueOnError)
	var output, releaseTag, commit, driverName, commercialTypes, chart, values, checksums string
	var driverImage, provisionerImage, attacherImage, registrarImage, livenessImage string
	flags.StringVar(&output, "output", "", "new candidate manifest path")
	flags.StringVar(&releaseTag, "release-tag", "", "v-prefixed release tag")
	flags.StringVar(&commit, "commit", "", "exact Git commit")
	flags.StringVar(&driverName, "driver-name", "", "immutable CSI driver name")
	flags.StringVar(&commercialTypes, "commercial-types", "", "canonical commercial-type list")
	flags.StringVar(&chart, "chart", "", "exact chart package path")
	flags.StringVar(&values, "values", "", "exact release values path")
	flags.StringVar(&checksums, "checksums", "", "exact checksum manifest path")
	flags.StringVar(&driverImage, "driver-image", "", "driver repository@sha256")
	flags.StringVar(&provisionerImage, "provisioner-image", "", "external-provisioner repository@sha256")
	flags.StringVar(&attacherImage, "attacher-image", "", "external-attacher repository@sha256")
	flags.StringVar(&registrarImage, "registrar-image", "", "node-driver-registrar repository@sha256")
	flags.StringVar(&livenessImage, "liveness-image", "", "livenessprobe repository@sha256")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || output == "" || chart == "" || values == "" || checksums == "" {
		return fmt.Errorf("candidate output, chart, values, and checksums are required")
	}
	types, err := compatibility.ParseCommercialTypes(commercialTypes)
	if err != nil {
		return err
	}
	chartDigest, err := releasequalification.DigestFile(chart)
	if err != nil {
		return err
	}
	valuesDigest, err := releasequalification.DigestFile(values)
	if err != nil {
		return err
	}
	checksumDigest, err := releasequalification.DigestFile(checksums)
	if err != nil {
		return err
	}
	manifest := releasequalification.CandidateManifest{
		SchemaVersion: releasequalification.SchemaVersionV1,
		ReleaseTag:    releaseTag, Version: strings.TrimPrefix(releaseTag, "v"), GitCommit: commit,
		DriverName: driverName, QualifiedCommercialTypes: types,
		ChartFile: filepath.Base(chart), ChartSHA256: chartDigest,
		ValuesFile: filepath.Base(values), ValuesSHA256: valuesDigest, DriverImage: driverImage,
		ChecksumManifestFile: filepath.Base(checksums), ChecksumManifestSHA256: checksumDigest,
		Images: []e2eplan.ImageDigest{
			{Name: "driver", Reference: driverImage},
			{Name: "external-provisioner", Reference: provisionerImage},
			{Name: "external-attacher", Reference: attacherImage},
			{Name: "csi-node-driver-registrar", Reference: registrarImage},
			{Name: "livenessprobe", Reference: livenessImage},
		},
	}
	encoded, err := releasequalification.EncodeCandidate(manifest)
	if err != nil {
		return err
	}
	return writeNew(output, append(encoded, '\n'))
}

func verifyQualification(arguments []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	var candidatePath, candidateDirectory, qualificationPath, qualificationDirectory string
	flags.StringVar(&candidatePath, "candidate", "", "candidate manifest path")
	flags.StringVar(&candidateDirectory, "candidate-dir", "", "candidate artifact directory")
	flags.StringVar(&qualificationPath, "qualification", "", "qualification manifest path")
	flags.StringVar(&qualificationDirectory, "qualification-dir", "", "qualification evidence directory")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || candidatePath == "" || candidateDirectory == "" || qualificationPath == "" || qualificationDirectory == "" {
		return fmt.Errorf("candidate, candidate-dir, qualification, and qualification-dir are required")
	}
	candidateBytes, err := os.ReadFile(candidatePath)
	if err != nil {
		return err
	}
	candidateBytes = []byte(strings.TrimSuffix(string(candidateBytes), "\n"))
	candidate, err := releasequalification.DecodeCandidate(candidateBytes)
	if err != nil {
		return err
	}
	if err := releasequalification.VerifyCandidateDirectory(candidateDirectory, candidate); err != nil {
		return err
	}
	qualificationBytes, err := os.ReadFile(qualificationPath)
	if err != nil {
		return err
	}
	qualification, err := releasequalification.DecodeQualification(qualificationBytes, candidate, candidateBytes)
	if err != nil {
		return err
	}
	return releasequalification.VerifyQualificationDirectory(qualificationDirectory, qualification, candidate)
}

func writeQualification(arguments []string) error {
	flags := flag.NewFlagSet("qualification", flag.ContinueOnError)
	var output, candidatePath, linuxEvidence, kindEvidence string
	var runEvidence, cleanupAudits repeatedStrings
	flags.StringVar(&output, "output", "", "new qualification manifest path")
	flags.StringVar(&candidatePath, "candidate", "", "exact candidate manifest path")
	flags.StringVar(&linuxEvidence, "linux-evidence", "", "retained Linux gate evidence")
	flags.StringVar(&kindEvidence, "kind-evidence", "", "retained kind gate evidence")
	flags.Var(&runEvidence, "run-evidence", "one retained Kapsule run evidence path; repeat per commercial type")
	flags.Var(&cleanupAudits, "cleanup-audit", "matching final cleanup inventory path; repeat per commercial type")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || output == "" || candidatePath == "" || linuxEvidence == "" || kindEvidence == "" || len(runEvidence) == 0 || len(runEvidence) != len(cleanupAudits) {
		return fmt.Errorf("qualification output, candidate, Linux/kind evidence, and paired run/cleanup evidence are required")
	}
	candidateBytesWithNewline, err := os.ReadFile(candidatePath)
	if err != nil {
		return err
	}
	candidateBytes := []byte(strings.TrimSuffix(string(candidateBytesWithNewline), "\n"))
	candidate, err := releasequalification.DecodeCandidate(candidateBytes)
	if err != nil {
		return err
	}
	linuxDigest, err := releasequalification.DigestFile(linuxEvidence)
	if err != nil {
		return err
	}
	kindDigest, err := releasequalification.DigestFile(kindEvidence)
	if err != nil {
		return err
	}
	manifest := releasequalification.QualificationManifest{
		SchemaVersion:     releasequalification.SchemaVersionV1,
		LinuxEvidenceFile: filepath.Base(linuxEvidence), LinuxEvidenceSHA256: linuxDigest,
		KindEvidenceFile: filepath.Base(kindEvidence), KindEvidenceSHA256: kindDigest,
	}
	// Candidate authority deliberately excludes the conventional trailing
	// newline, matching EncodeCandidate and promotion verification.
	candidateSum := sha256.Sum256(candidateBytes)
	manifest.CandidateSHA256 = "sha256:" + hex.EncodeToString(candidateSum[:])
	for index, evidencePath := range runEvidence {
		encoded, err := os.ReadFile(evidencePath)
		if err != nil {
			return err
		}
		var evidence e2erunner.Evidence
		if err := strictjson.Decode(encoded, &evidence); err != nil {
			return err
		}
		if err := evidence.Validate(); err != nil {
			return err
		}
		if err := releasequalification.ValidateEvidenceCandidate(evidence, candidate, manifest.CandidateSHA256); err != nil {
			return fmt.Errorf("Kapsule evidence %q names another candidate", filepath.Base(evidencePath)) //nolint:staticcheck // Kapsule is a product name.
		}
		cleanupBytes, err := os.ReadFile(cleanupAudits[index])
		if err != nil {
			return err
		}
		var cleanup e2ecleanup.Inventory
		if err := strictjson.Decode(cleanupBytes, &cleanup); err != nil {
			return err
		}
		if err := e2erunner.ValidateCleanupAudit(evidence, cleanup); err != nil {
			return fmt.Errorf("Kapsule cleanup %q: %w", filepath.Base(cleanupAudits[index]), err) //nolint:staticcheck // Kapsule is a product name.
		}
		completedAt, err := time.Parse(time.RFC3339Nano, evidence.CompletedAt)
		if err != nil {
			return err
		}
		cleanupPlan, err := e2ecleanup.Build(cleanup, completedAt)
		if err != nil || !cleanupPlan.CleanupComplete {
			return fmt.Errorf("Kapsule cleanup %q is incomplete: %w", filepath.Base(cleanupAudits[index]), err) //nolint:staticcheck // Kapsule is a product name.
		}
		evidenceDigest, err := releasequalification.DigestFile(evidencePath)
		if err != nil {
			return err
		}
		cleanupDigest, err := releasequalification.DigestFile(cleanupAudits[index])
		if err != nil {
			return err
		}
		manifest.Runs = append(manifest.Runs, releasequalification.QualificationRun{
			RunID: evidence.RunID, ProjectID: evidence.ProjectID, Region: evidence.Region,
			Profile:        evidence.FinalInventory.Profile,
			CommercialType: evidence.CommercialType, KapsuleEvidenceFile: filepath.Base(evidencePath),
			KapsuleEvidenceSHA256: evidenceDigest, CleanupAuditFile: filepath.Base(cleanupAudits[index]),
			CleanupAuditSHA256: cleanupDigest, Succeeded: true, CleanupComplete: true,
		})
	}
	encoded, err := releasequalification.EncodeQualification(manifest, candidate, candidateBytes)
	if err != nil {
		return err
	}
	return writeNew(output, append(encoded, '\n'))
}

func writeNew(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %q: %w", path, err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write %q: %w", path, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync %q: %w", path, err)
	}
	return file.Close()
}
