package version

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	releasecompat "scaleway-sfs-subdir-csi/internal/compatibility"
)

const developmentVersion = "0.0.0-dev"

var (
	semanticVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:(?:0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*))*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)
	lowerHexPattern        = regexp.MustCompile(`^[0-9a-f]+$`)
)

// ReleaseMetadata binds one human Git tag to the unprefixed semantic version
// embedded in binaries and to reproducible source/time identity.
type ReleaseMetadata struct {
	ReleaseTag      string   `json:"releaseTag"`
	Version         string   `json:"version"`
	Commit          string   `json:"commit"`
	BuildDate       string   `json:"buildDate"`
	CommercialTypes []string `json:"commercialTypes"`
}

// ValidateSemanticVersion validates strict SemVer 2.0 without a leading v.
// CSI vendor_version consumes this exact form.
func ValidateSemanticVersion(value string) error {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("semantic version must be single-line UTF-8 containing 1 to 128 bytes")
	}
	if !semanticVersionPattern.MatchString(value) {
		return fmt.Errorf("version %q is not strict semantic versioning without a leading v", value)
	}
	return nil
}

// ValidateBuildIdentity validates the linked process globals. The one explicit
// development identity must retain its exact unknown/empty fields; every other
// build carries a complete commit, timestamp, and commercial compatibility.
func ValidateBuildIdentity() error {
	if err := ValidateSemanticVersion(Version); err != nil {
		return err
	}
	if Version == developmentVersion {
		if Commit != "unknown" || BuildDate != "unknown" || QualifiedCommercialTypes != "" {
			return fmt.Errorf("development build must use exact unknown provenance and no commercial type claim")
		}
		return nil
	}
	if err := validateCommit(Commit); err != nil {
		return err
	}
	if err := validateBuildDate(BuildDate); err != nil {
		return err
	}
	if _, err := CommercialTypes(); err != nil {
		return fmt.Errorf("validate embedded commercial types: %w", err)
	}
	return nil
}

// Validate checks release tag/version correspondence and complete immutable
// build provenance fields. The still-undecided public tag policy may choose
// either VERSION or vVERSION, but cannot choose an unrelated tag.
func (metadata ReleaseMetadata) Validate() error {
	if err := ValidateSemanticVersion(metadata.Version); err != nil {
		return err
	}
	if metadata.Version == developmentVersion {
		return fmt.Errorf("development version %q cannot identify a release artifact", metadata.Version)
	}
	if metadata.ReleaseTag != metadata.Version && metadata.ReleaseTag != "v"+metadata.Version {
		return fmt.Errorf("release tag must equal VERSION or vVERSION")
	}
	if err := validateCommit(metadata.Commit); err != nil {
		return err
	}
	if err := validateBuildDate(metadata.BuildDate); err != nil {
		return err
	}
	if err := releasecompat.ValidateCommercialTypes(metadata.CommercialTypes); err != nil {
		return fmt.Errorf("release commercial types: %w", err)
	}
	return nil
}

func validateCommit(value string) error {
	if (len(value) != 40 && len(value) != 64) || !lowerHexPattern.MatchString(value) {
		return fmt.Errorf("commit must be a complete 40- or 64-character lowercase hexadecimal Git object ID")
	}
	return nil
}

func validateBuildDate(value string) error {
	const layout = "2006-01-02T15:04:05Z"
	parsed, err := time.Parse(layout, value)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(layout) != value {
		return fmt.Errorf("build date must be canonical UTC YYYY-MM-DDTHH:MM:SSZ")
	}
	return nil
}
