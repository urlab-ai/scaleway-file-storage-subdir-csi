package admin

import (
	"strings"
	"testing"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

func validUpgradeState() (UpgradeLiveState, UpgradeCandidate) {
	parent := UpgradeParentMapping{
		ParentFilesystemID: "11111111-1111-4111-8111-111111111111",
		PoolName:           "standard", BasePathHash: "bp-" + strings.Repeat("a", 32),
	}
	live := UpgradeLiveState{
		DriverName:               "sfs-subdir.csi.example.com",
		InstallationIDHash:       "sha256:" + strings.Repeat("b", 64),
		ActiveClusterUID:         "22222222-2222-4222-8222-222222222222",
		LeadershipLeaseName:      volume.LeadershipLeaseNameV1,
		Parents:                  []UpgradeParentMapping{parent},
		AllocationSchemaVersions: []string{"1"}, OwnershipSchemaVersions: []string{"1"},
		NodeConfigGenerations:  []string{strings.Repeat("c", 64)},
		NodeReadableAllocation: []string{"1"}, NodeReadableOwnership: []string{"1"},
	}
	candidate := UpgradeCandidate{
		DriverName: live.DriverName, InstallationIDHash: live.InstallationIDHash,
		ActiveClusterUID: live.ActiveClusterUID, LeadershipLeaseName: live.LeadershipLeaseName,
		Parents:                   []UpgradeParentMapping{parent},
		ReadableAllocationSchemas: []string{"1"}, ReadableOwnershipSchemas: []string{"1"},
		WrittenAllocationSchema: "1", WrittenOwnershipSchema: "1",
		CandidateNodeConfigGeneration: strings.Repeat("d", 64),
	}
	return live, candidate
}

func TestValidateUpgradePreflightAcceptsCompatibleCandidateAndNewParent(t *testing.T) {
	live, candidate := validUpgradeState()
	candidate.Parents = append(candidate.Parents, UpgradeParentMapping{
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		PoolName:           "standard", BasePathHash: "bp-" + strings.Repeat("e", 32),
	})
	if err := ValidateUpgradePreflight(live, candidate); err != nil {
		t.Fatalf("ValidateUpgradePreflight() error = %v", err)
	}
}

func TestValidateUpgradePreflightRejectsEveryCompatibilityBreak(t *testing.T) {
	tests := map[string]func(*UpgradeLiveState, *UpgradeCandidate){
		"driver": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) { candidate.DriverName = "other.csi.example.com" },
		"installation": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.InstallationIDHash = "sha256:" + strings.Repeat("f", 64)
		},
		"cluster": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.ActiveClusterUID = "33333333-3333-4333-8333-333333333333"
		},
		"Lease":          func(_ *UpgradeLiveState, candidate *UpgradeCandidate) { candidate.LeadershipLeaseName = "replacement" },
		"removed parent": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) { candidate.Parents = nil },
		"changed base path": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.Parents[0].BasePathHash = "bp-" + strings.Repeat("f", 32)
		},
		"missing allocation reader": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.ReadableAllocationSchemas = []string{"2"}
		},
		"missing ownership reader": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.ReadableOwnershipSchemas = []string{"2"}
		},
		"N-1 allocation": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.ReadableAllocationSchemas = []string{"1", "2"}
			candidate.WrittenAllocationSchema = "2"
		},
		"N-1 ownership": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.ReadableOwnershipSchemas = []string{"1", "2"}
			candidate.WrittenOwnershipSchema = "2"
		},
		"generation": func(_ *UpgradeLiveState, candidate *UpgradeCandidate) {
			candidate.CandidateNodeConfigGeneration = "not-a-digest"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			live, candidate := validUpgradeState()
			mutate(&live, &candidate)
			if err := ValidateUpgradePreflight(live, candidate); err == nil {
				t.Fatal("ValidateUpgradePreflight(incompatible) error = nil")
			}
		})
	}
}

func TestValidateUpgradePreflightRejectsMalformedOrAmbiguousLiveState(t *testing.T) {
	live, candidate := validUpgradeState()
	live.Parents = append(live.Parents, live.Parents[0])
	if err := ValidateUpgradePreflight(live, candidate); err == nil {
		t.Fatal("ValidateUpgradePreflight(duplicate live parent) error = nil")
	}
	live, candidate = validUpgradeState()
	live.AllocationSchemaVersions = []string{"1", "1"}
	if err := ValidateUpgradePreflight(live, candidate); err == nil {
		t.Fatal("ValidateUpgradePreflight(duplicate schema) error = nil")
	}
	live, candidate = validUpgradeState()
	live.NodeConfigGenerations = nil
	if err := ValidateUpgradePreflight(live, candidate); err == nil {
		t.Fatal("ValidateUpgradePreflight(no Ready node generation) error = nil")
	}
}
