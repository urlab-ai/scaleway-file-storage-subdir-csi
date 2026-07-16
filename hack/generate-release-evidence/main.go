// Command generate-release-evidence emits deterministic SPDX 2.3 SBOM and
// in-toto/SLSA provenance subjects for one already-built release directory.
package main

import (
	"bufio"
	"crypto/sha1" // #nosec G505 -- SPDX 2.3 mandates SHA-1 for package verification codes.
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type evidenceSubject struct {
	Name   string            `json:"name"`
	Digest map[string]string `json:"digest"`
}

type spdxChecksum struct {
	Algorithm     string `json:"algorithm"`
	ChecksumValue string `json:"checksumValue"`
}

type spdxFile struct {
	FileName  string         `json:"fileName"`
	SPDXID    string         `json:"SPDXID"`
	Checksums []spdxChecksum `json:"checksums"`
}

type spdxPackage struct {
	Name                    string                       `json:"name"`
	SPDXID                  string                       `json:"SPDXID"`
	VersionInfo             string                       `json:"versionInfo"`
	DownloadLocation        string                       `json:"downloadLocation"`
	FilesAnalyzed           bool                         `json:"filesAnalyzed"`
	PackageVerificationCode *spdxPackageVerificationCode `json:"packageVerificationCode,omitempty"`
	LicenseConcluded        string                       `json:"licenseConcluded"`
	LicenseDeclared         string                       `json:"licenseDeclared"`
	CopyrightText           string                       `json:"copyrightText"`
}

type spdxPackageVerificationCode struct {
	Value string `json:"packageVerificationCodeValue"`
}

type spdxRelationship struct {
	SPDXElementID      string `json:"spdxElementId"`
	RelationshipType   string `json:"relationshipType"`
	RelatedSPDXElement string `json:"relatedSpdxElement"`
}

type spdxDocument struct {
	SPDXVersion       string             `json:"spdxVersion"`
	DataLicense       string             `json:"dataLicense"`
	SPDXID            string             `json:"SPDXID"`
	Name              string             `json:"name"`
	DocumentNamespace string             `json:"documentNamespace"`
	CreationInfo      spdxCreationInfo   `json:"creationInfo"`
	Packages          []spdxPackage      `json:"packages"`
	Files             []spdxFile         `json:"files"`
	Relationships     []spdxRelationship `json:"relationships"`
}

type spdxCreationInfo struct {
	Created  string   `json:"created"`
	Creators []string `json:"creators"`
}

type provenanceStatement struct {
	Type          string              `json:"_type"`
	Subject       []evidenceSubject   `json:"subject"`
	PredicateType string              `json:"predicateType"`
	Predicate     provenancePredicate `json:"predicate"`
}

type provenancePredicate struct {
	BuildDefinition provenanceBuildDefinition `json:"buildDefinition"`
	RunDetails      provenanceRunDetails      `json:"runDetails"`
}

type provenanceBuildDefinition struct {
	BuildType            string            `json:"buildType"`
	ExternalParameters   map[string]string `json:"externalParameters"`
	InternalParameters   map[string]string `json:"internalParameters"`
	ResolvedDependencies []evidenceSubject `json:"resolvedDependencies"`
}

type provenanceRunDetails struct {
	Builder  provenanceBuilder  `json:"builder"`
	Metadata provenanceMetadata `json:"metadata"`
}

type provenanceBuilder struct {
	ID string `json:"id"`
}

type provenanceMetadata struct {
	InvocationID string `json:"invocationId"`
	StartedOn    string `json:"startedOn"`
	FinishedOn   string `json:"finishedOn"`
}

func main() {
	var dist, tag, version, commit, buildDate, repository, builderID, buildType, sbomPath, provenancePath string
	flag.StringVar(&dist, "dist", "", "absolute release directory")
	flag.StringVar(&tag, "tag", "", "release tag")
	flag.StringVar(&version, "version", "", "unprefixed release version")
	flag.StringVar(&commit, "commit", "", "source commit")
	flag.StringVar(&buildDate, "build-date", "", "canonical UTC build timestamp")
	flag.StringVar(&repository, "repository", "", "public source repository URL")
	flag.StringVar(&builderID, "builder-id", "", "identity of the process or workflow that produced these artifacts")
	flag.StringVar(&buildType, "build-type", "", "versioned build procedure identity")
	flag.StringVar(&sbomPath, "sbom", "", "absolute SPDX output path")
	flag.StringVar(&provenancePath, "provenance", "", "absolute provenance output path")
	flag.Parse()
	if err := runReleaseEvidence(dist, tag, version, commit, buildDate, repository, builderID, buildType, sbomPath, provenancePath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runReleaseEvidence(dist, tag, version, commit, buildDate, repository, builderID, buildType, sbomPath, provenancePath string) error {
	if !filepath.IsAbs(dist) || !filepath.IsAbs(sbomPath) || !filepath.IsAbs(provenancePath) || filepath.Clean(dist) != dist || filepath.Clean(sbomPath) != sbomPath || filepath.Clean(provenancePath) != provenancePath {
		return fmt.Errorf("release evidence paths must be absolute and normalized")
	}
	if tag == "" || version == "" || commit == "" || repository == "" || builderID == "" || buildType == "" || strings.ContainsAny(tag+version+commit+repository+builderID+buildType, "\x00\r\n\t ") {
		return fmt.Errorf("release evidence identity is incomplete")
	}
	timestamp, err := time.Parse(time.RFC3339, buildDate)
	if err != nil || timestamp.Location() != time.UTC || timestamp.Format(time.RFC3339) != buildDate {
		return fmt.Errorf("build date must be canonical UTC RFC3339")
	}
	subjects, files, verificationCode, err := releaseSubjects(dist, tag, sbomPath, provenancePath)
	if err != nil {
		return err
	}
	if len(subjects) == 0 {
		return fmt.Errorf("release directory contains no artifacts")
	}
	modules, err := releaseModules(dist, tag)
	if err != nil {
		return err
	}
	relationships := []spdxRelationship{{SPDXElementID: "SPDXRef-DOCUMENT", RelationshipType: "DESCRIBES", RelatedSPDXElement: "SPDXRef-Package"}}
	for _, file := range files {
		relationships = append(relationships, spdxRelationship{SPDXElementID: "SPDXRef-Package", RelationshipType: "CONTAINS", RelatedSPDXElement: file.SPDXID})
	}
	for _, module := range modules {
		relationships = append(relationships, spdxRelationship{SPDXElementID: "SPDXRef-Package", RelationshipType: "DEPENDS_ON", RelatedSPDXElement: module.SPDXID})
	}
	packages := []spdxPackage{{Name: "scaleway-sfs-subdir-csi", SPDXID: "SPDXRef-Package", VersionInfo: version, DownloadLocation: "NOASSERTION", FilesAnalyzed: true, PackageVerificationCode: &spdxPackageVerificationCode{Value: verificationCode}, LicenseConcluded: "MIT", LicenseDeclared: "MIT", CopyrightText: "NOASSERTION"}}
	packages = append(packages, modules...)
	sbom := spdxDocument{
		SPDXVersion: "SPDX-2.3", DataLicense: "CC0-1.0", SPDXID: "SPDXRef-DOCUMENT",
		Name:              "scaleway-sfs-subdir-csi-" + tag,
		DocumentNamespace: strings.TrimSuffix(repository, "/") + "/releases/" + tag + "/sbom",
		CreationInfo:      spdxCreationInfo{Created: buildDate, Creators: []string{"Tool: scaleway-sfs-subdir-csi-release-evidence"}},
		Packages:          packages,
		Files:             files, Relationships: relationships,
	}
	provenance := provenanceStatement{
		Type: "https://in-toto.io/Statement/v1", Subject: subjects, PredicateType: "https://slsa.dev/provenance/v1",
		Predicate: provenancePredicate{
			BuildDefinition: provenanceBuildDefinition{
				BuildType:            buildType,
				ExternalParameters:   map[string]string{"releaseTag": tag, "version": version, "commit": commit},
				InternalParameters:   map[string]string{"buildDate": buildDate},
				ResolvedDependencies: []evidenceSubject{{Name: repository + "@" + commit, Digest: map[string]string{"gitCommit": commit}}},
			},
			RunDetails: provenanceRunDetails{Builder: provenanceBuilder{ID: builderID}, Metadata: provenanceMetadata{InvocationID: tag + "-" + commit, StartedOn: buildDate, FinishedOn: buildDate}},
		},
	}
	if err := writeCanonicalJSON(sbomPath, sbom); err != nil {
		return err
	}
	return writeCanonicalJSON(provenancePath, provenance)
}

func releaseModules(dist, tag string) ([]spdxPackage, error) {
	type moduleIdentity struct {
		path    string
		version string
	}
	unique := make(map[moduleIdentity]struct{})
	for _, arch := range []string{"amd64", "arm64"} {
		for _, command := range []string{"csi-admin", "scaleway-sfs-subdir-csi"} {
			modulePath := filepath.Join(dist, fmt.Sprintf("%s_%s_linux_%s.modules.txt", command, tag, arch))
			file, err := os.Open(modulePath)
			if err != nil {
				return nil, fmt.Errorf("open Go module sidecar %q: %w", filepath.Base(modulePath), err)
			}
			scanner := bufio.NewScanner(io.LimitReader(file, 8<<20))
			for scanner.Scan() {
				fields := strings.Fields(scanner.Text())
				if len(fields) < 3 || fields[0] != "dep" {
					continue
				}
				if fields[1] == "" || fields[2] == "" || len(fields[1]) > 512 || len(fields[2]) > 256 || strings.ContainsAny(fields[1]+fields[2], "\x00\r\n\t ") {
					_ = file.Close()
					return nil, fmt.Errorf("go module sidecar %q contains an invalid dependency", filepath.Base(modulePath))
				}
				unique[moduleIdentity{path: fields[1], version: fields[2]}] = struct{}{}
			}
			scanErr := scanner.Err()
			closeErr := file.Close()
			if scanErr != nil || closeErr != nil {
				return nil, fmt.Errorf("read go module sidecar %q: %w", filepath.Base(modulePath), errors.Join(scanErr, closeErr))
			}
		}
	}
	identities := make([]moduleIdentity, 0, len(unique))
	for identity := range unique {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].path == identities[j].path {
			return identities[i].version < identities[j].version
		}
		return identities[i].path < identities[j].path
	})
	modules := make([]spdxPackage, 0, len(identities))
	for _, identity := range identities {
		sum := sha256.Sum256([]byte(identity.path + "@" + identity.version))
		modules = append(modules, spdxPackage{
			Name: identity.path, SPDXID: "SPDXRef-GoModule-" + hex.EncodeToString(sum[:8]), VersionInfo: identity.version,
			DownloadLocation: "NOASSERTION", FilesAnalyzed: false, LicenseConcluded: "NOASSERTION",
			LicenseDeclared: "NOASSERTION", CopyrightText: "NOASSERTION",
		})
	}
	if len(modules) == 0 {
		return nil, fmt.Errorf("release binaries expose no Go module dependencies")
	}
	return modules, nil
}

func releaseSubjects(dist, tag, sbomPath, provenancePath string) ([]evidenceSubject, []spdxFile, string, error) {
	entries, err := os.ReadDir(dist)
	if err != nil {
		return nil, nil, "", err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	subjects := make([]evidenceSubject, 0, len(entries))
	files := make([]spdxFile, 0, len(entries))
	sha1Digests := make([]string, 0, len(entries))
	expected := expectedBinaryArtifacts(tag)
	for _, entry := range entries {
		path := filepath.Join(dist, entry.Name())
		if entry.IsDir() || path == sbomPath || path == provenancePath || strings.HasPrefix(entry.Name(), "checksums_") {
			continue
		}
		if _, ok := expected[entry.Name()]; !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return nil, nil, "", fmt.Errorf("release artifact %q is not an exact regular file", entry.Name())
		}
		sha256Digest, sha1Digest, err := hashFile(path)
		if err != nil {
			return nil, nil, "", err
		}
		delete(expected, entry.Name())
		sha1Digests = append(sha1Digests, sha1Digest)
		subjects = append(subjects, evidenceSubject{Name: entry.Name(), Digest: map[string]string{"sha256": sha256Digest}})
		files = append(files, spdxFile{FileName: "./" + entry.Name(), SPDXID: fmt.Sprintf("SPDXRef-File-%d", len(files)+1), Checksums: []spdxChecksum{{Algorithm: "SHA256", ChecksumValue: sha256Digest}, {Algorithm: "SHA1", ChecksumValue: sha1Digest}}})
	}
	if len(expected) != 0 {
		missing := make([]string, 0, len(expected))
		for name := range expected {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return nil, nil, "", fmt.Errorf("release directory is missing exact artifacts: %s", strings.Join(missing, ", "))
	}
	sort.Strings(sha1Digests)
	verificationHash := sha1.New() // #nosec G401 -- mandated by the SPDX 2.3 verification algorithm.
	for _, digest := range sha1Digests {
		_, _ = io.WriteString(verificationHash, digest)
	}
	return subjects, files, hex.EncodeToString(verificationHash.Sum(nil)), nil
}

func expectedBinaryArtifacts(tag string) map[string]struct{} {
	expected := make(map[string]struct{}, 12)
	for _, arch := range []string{"amd64", "arm64"} {
		for _, command := range []string{"csi-admin", "scaleway-sfs-subdir-csi"} {
			name := fmt.Sprintf("%s_%s_linux_%s", command, tag, arch)
			expected[name] = struct{}{}
			expected[name+".identity.json"] = struct{}{}
			expected[name+".modules.txt"] = struct{}{}
		}
	}
	return expected
}

func hashFile(path string) (sha256Digest, sha1Digest string, returnErr error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	sha256Hash := sha256.New()
	sha1Hash := sha1.New() // #nosec G401 -- mandated by SPDX, not used as a security boundary.
	if _, err := io.Copy(io.MultiWriter(sha256Hash, sha1Hash), file); err != nil {
		return "", "", err
	}
	return hex.EncodeToString(sha256Hash.Sum(nil)), hex.EncodeToString(sha1Hash.Sum(nil)), nil
}

func writeCanonicalJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}
