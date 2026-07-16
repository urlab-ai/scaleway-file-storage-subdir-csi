package version

import (
	"strings"
	"testing"
)

func TestValidateSemanticVersionImplementsStrictUnprefixedSemVer(t *testing.T) {
	valid := []string{
		"0.0.0", "1.2.3", "0.0.0-dev", "1.0.0-rc.1", "1.2.3-alpha.beta-1+build.42",
	}
	for _, value := range valid {
		if err := ValidateSemanticVersion(value); err != nil {
			t.Errorf("ValidateSemanticVersion(%q) error = %v", value, err)
		}
	}
	invalid := []string{
		"", "v1.2.3", "1", "1.2", "01.2.3", "1.02.3", "1.2.03", "1.2.3-01",
		"1.2.3-", "1.2.3+", "1.2.3+build..1", "1.2.3\nother", strings.Repeat("1", 129),
	}
	for _, value := range invalid {
		if err := ValidateSemanticVersion(value); err == nil {
			t.Errorf("ValidateSemanticVersion(%q) error = nil", value)
		}
	}
}

func TestValidateBuildIdentitySeparatesDevelopmentAndRelease(t *testing.T) {
	originalVersion, originalCommit, originalBuildDate, originalTypes := Version, Commit, BuildDate, QualifiedCommercialTypes
	t.Cleanup(func() {
		Version, Commit, BuildDate, QualifiedCommercialTypes = originalVersion, originalCommit, originalBuildDate, originalTypes
	})

	Version, Commit, BuildDate, QualifiedCommercialTypes = developmentVersion, "unknown", "unknown", ""
	if err := ValidateBuildIdentity(); err != nil {
		t.Fatalf("ValidateBuildIdentity(development) error = %v", err)
	}
	Commit = strings.Repeat("a", 40)
	BuildDate = "2026-07-13T12:34:56Z"
	QualifiedCommercialTypes = "TEST-TYPE-1"
	if err := ValidateBuildIdentity(); err == nil {
		t.Fatal("ValidateBuildIdentity(development with release claims) error = nil")
	}
	Version = "1.2.3"
	Commit, BuildDate, QualifiedCommercialTypes = "unknown", "unknown", ""
	if err := ValidateBuildIdentity(); err == nil {
		t.Fatal("ValidateBuildIdentity(release with unknown provenance) error = nil")
	}
	Commit = strings.Repeat("a", 40)
	BuildDate = "2026-07-13T12:34:56Z"
	QualifiedCommercialTypes = "TEST-TYPE-1"
	if err := ValidateBuildIdentity(); err != nil {
		t.Fatalf("ValidateBuildIdentity(release) error = %v", err)
	}
	Version = "v1.2.3"
	if err := ValidateBuildIdentity(); err == nil {
		t.Fatal("ValidateBuildIdentity(v-prefixed runtime) error = nil")
	}
}

func TestReleaseMetadataBindsTagRuntimeAndProvenance(t *testing.T) {
	base := ReleaseMetadata{
		ReleaseTag: "v1.2.3", Version: "1.2.3",
		Commit: strings.Repeat("b", 40), BuildDate: "2026-07-13T12:34:56Z",
		CommercialTypes: []string{"TEST-TYPE-1"},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("ReleaseMetadata.Validate(v tag) error = %v", err)
	}
	base.ReleaseTag = base.Version
	if err := base.Validate(); err == nil {
		t.Fatal("ReleaseMetadata.Validate(unprefixed tag) error = nil")
	}
	base.ReleaseTag = "v" + base.Version

	tests := map[string]func(*ReleaseMetadata){
		"unrelated tag":    func(value *ReleaseMetadata) { value.ReleaseTag = "release-1.2.3" },
		"prefixed runtime": func(value *ReleaseMetadata) { value.Version = "v1.2.3" },
		"development": func(value *ReleaseMetadata) {
			value.ReleaseTag, value.Version = "v0.0.0-dev", developmentVersion
		},
		"short commit":           func(value *ReleaseMetadata) { value.Commit = "abcdef" },
		"uppercase commit":       func(value *ReleaseMetadata) { value.Commit = strings.Repeat("A", 40) },
		"invalid timestamp":      func(value *ReleaseMetadata) { value.BuildDate = "2026-07-13T12:34:56+00:00" },
		"empty commercial types": func(value *ReleaseMetadata) { value.CommercialTypes = nil },
		"unsorted commercial types": func(value *ReleaseMetadata) {
			value.CommercialTypes = []string{"TYPE-B", "TYPE-A"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("ReleaseMetadata.Validate(invalid) error = nil")
			}
		})
	}
}
