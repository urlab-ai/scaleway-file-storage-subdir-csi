package admin

import (
	"fmt"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

// UpgradeParentMapping is the immutable live parent projection compared before
// an online chart upgrade. Candidate configuration may add parents, but cannot
// move or reinterpret an existing one.
type UpgradeParentMapping struct {
	ParentFilesystemID string `json:"parentFilesystemID"`
	PoolName           string `json:"poolName"`
	BasePathHash       string `json:"basePathHash"`
}

// UpgradeLiveState is the authoritative projection read from current claims,
// allocations, ownership records, and Ready node-plugin Pods.
type UpgradeLiveState struct {
	DriverName               string                 `json:"driverName"`
	InstallationIDHash       string                 `json:"installationIDHash"`
	ActiveClusterUID         string                 `json:"activeClusterUID"`
	LeadershipLeaseName      string                 `json:"leadershipLeaseName"`
	Parents                  []UpgradeParentMapping `json:"parents"`
	AllocationSchemaVersions []string               `json:"allocationSchemaVersions"`
	OwnershipSchemaVersions  []string               `json:"ownershipSchemaVersions"`
	NodeConfigGenerations    []string               `json:"nodeConfigGenerations"`
	NodeReadableAllocation   []string               `json:"nodeReadableAllocation"`
	NodeReadableOwnership    []string               `json:"nodeReadableOwnership"`
}

// UpgradeCandidate is the offline-rendered compatibility declaration for the
// candidate chart and binaries.
type UpgradeCandidate struct {
	DriverName                    string                 `json:"driverName"`
	InstallationIDHash            string                 `json:"installationIDHash"`
	ActiveClusterUID              string                 `json:"activeClusterUID"`
	LeadershipLeaseName           string                 `json:"leadershipLeaseName"`
	Parents                       []UpgradeParentMapping `json:"parents"`
	ReadableAllocationSchemas     []string               `json:"readableAllocationSchemas"`
	ReadableOwnershipSchemas      []string               `json:"readableOwnershipSchemas"`
	WrittenAllocationSchema       string                 `json:"writtenAllocationSchema"`
	WrittenOwnershipSchema        string                 `json:"writtenOwnershipSchema"`
	CandidateNodeConfigGeneration string                 `json:"candidateNodeConfigGeneration"`
}

// ValidateUpgradePreflight rejects an online upgrade that changes durable
// identity, drops an existing parent mapping, loses a live schema reader, or
// writes a schema outside the current N-1 node reader contract.
func ValidateUpgradePreflight(live UpgradeLiveState, candidate UpgradeCandidate) error {
	if err := ValidateUpgradeCandidate(candidate); err != nil {
		return fmt.Errorf("candidate upgrade declaration: %w", err)
	}
	if err := validateUpgradeIdentity(live, candidate); err != nil {
		return err
	}
	liveParents, err := validateUpgradeParents(live.Parents)
	if err != nil {
		return fmt.Errorf("live upgrade parents: %w", err)
	}
	candidateParents, err := validateUpgradeParents(candidate.Parents)
	if err != nil {
		return fmt.Errorf("candidate upgrade parents: %w", err)
	}
	for parentID, current := range liveParents {
		next, present := candidateParents[parentID]
		if !present {
			return fmt.Errorf("candidate removes existing parent %q; online removal is unsupported", parentID)
		}
		if next != current {
			return fmt.Errorf("candidate changes pool or base path identity for existing parent %q", parentID)
		}
	}

	liveAllocationSchemas, err := normalizedSchemaSet("live allocation", live.AllocationSchemaVersions)
	if err != nil {
		return err
	}
	liveOwnershipSchemas, err := normalizedSchemaSet("live ownership", live.OwnershipSchemaVersions)
	if err != nil {
		return err
	}
	candidateAllocationReaders, err := normalizedSchemaSet("candidate allocation reader", candidate.ReadableAllocationSchemas)
	if err != nil {
		return err
	}
	candidateOwnershipReaders, err := normalizedSchemaSet("candidate ownership reader", candidate.ReadableOwnershipSchemas)
	if err != nil {
		return err
	}
	if missing := firstMissingSchema(liveAllocationSchemas, candidateAllocationReaders); missing != "" {
		return fmt.Errorf("candidate cannot read live allocation schema %q", missing)
	}
	if missing := firstMissingSchema(liveOwnershipSchemas, candidateOwnershipReaders); missing != "" {
		return fmt.Errorf("candidate cannot read live ownership schema %q", missing)
	}
	if _, present := candidateAllocationReaders[candidate.WrittenAllocationSchema]; !present {
		return fmt.Errorf("candidate cannot read its written allocation schema %q", candidate.WrittenAllocationSchema)
	}
	if _, present := candidateOwnershipReaders[candidate.WrittenOwnershipSchema]; !present {
		return fmt.Errorf("candidate cannot read its written ownership schema %q", candidate.WrittenOwnershipSchema)
	}
	currentAllocationReaders, err := normalizedSchemaSet("current node allocation reader", live.NodeReadableAllocation)
	if err != nil {
		return err
	}
	currentOwnershipReaders, err := normalizedSchemaSet("current node ownership reader", live.NodeReadableOwnership)
	if err != nil {
		return err
	}
	if _, present := currentAllocationReaders[candidate.WrittenAllocationSchema]; !present {
		return fmt.Errorf("candidate allocation writer schema %q is unreadable by N-1 nodes", candidate.WrittenAllocationSchema)
	}
	if _, present := currentOwnershipReaders[candidate.WrittenOwnershipSchema]; !present {
		return fmt.Errorf("candidate ownership writer schema %q is unreadable by N-1 nodes", candidate.WrittenOwnershipSchema)
	}
	if err := validateGenerationDigest(candidate.CandidateNodeConfigGeneration); err != nil {
		return fmt.Errorf("candidate node configuration generation: %w", err)
	}
	if len(live.NodeConfigGenerations) == 0 {
		return fmt.Errorf("upgrade preflight requires at least one Ready node configuration generation")
	}
	for index, generation := range live.NodeConfigGenerations {
		if err := validateGenerationDigest(generation); err != nil {
			return fmt.Errorf("live node configuration generation %d: %w", index, err)
		}
	}
	return nil
}

// ValidateUpgradeCandidate checks the offline-rendered declaration without
// reading live state. Admin handlers use it before Kubernetes or filesystem I/O
// so malformed untrusted payloads cannot trigger discovery work.
func ValidateUpgradeCandidate(candidate UpgradeCandidate) error {
	if err := volume.ValidateDriverName(candidate.DriverName); err != nil {
		return err
	}
	if !validPrefixedSHA256(candidate.InstallationIDHash) {
		return fmt.Errorf("candidate installation identity hash is malformed")
	}
	if err := volume.ValidateClusterUID(candidate.ActiveClusterUID); err != nil {
		return err
	}
	if candidate.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("candidate leadership Lease name differs from fixed v1 name")
	}
	if _, err := validateUpgradeParents(candidate.Parents); err != nil {
		return err
	}
	allocationReaders, err := normalizedSchemaSet("candidate allocation reader", candidate.ReadableAllocationSchemas)
	if err != nil {
		return err
	}
	ownershipReaders, err := normalizedSchemaSet("candidate ownership reader", candidate.ReadableOwnershipSchemas)
	if err != nil {
		return err
	}
	if _, present := allocationReaders[candidate.WrittenAllocationSchema]; !present {
		return fmt.Errorf("candidate cannot read its written allocation schema %q", candidate.WrittenAllocationSchema)
	}
	if _, present := ownershipReaders[candidate.WrittenOwnershipSchema]; !present {
		return fmt.Errorf("candidate cannot read its written ownership schema %q", candidate.WrittenOwnershipSchema)
	}
	if err := validateGenerationDigest(candidate.CandidateNodeConfigGeneration); err != nil {
		return fmt.Errorf("candidate node configuration generation: %w", err)
	}
	return nil
}

func validateUpgradeIdentity(live UpgradeLiveState, candidate UpgradeCandidate) error {
	if err := volume.ValidateDriverName(live.DriverName); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(live.ActiveClusterUID); err != nil {
		return err
	}
	if !validPrefixedSHA256(live.InstallationIDHash) {
		return fmt.Errorf("live installation identity hash is malformed")
	}
	if live.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("live leadership Lease name differs from fixed v1 name")
	}
	if candidate.DriverName != live.DriverName || candidate.InstallationIDHash != live.InstallationIDHash || candidate.ActiveClusterUID != live.ActiveClusterUID || candidate.LeadershipLeaseName != live.LeadershipLeaseName {
		return fmt.Errorf("candidate changes driver, installation, cluster, or leadership Lease identity")
	}
	return nil
}

func validateUpgradeParents(parents []UpgradeParentMapping) (map[string]UpgradeParentMapping, error) {
	result := make(map[string]UpgradeParentMapping, len(parents))
	for index, parent := range parents {
		if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
			return nil, fmt.Errorf("parent %d: %w", index, err)
		}
		if err := volume.ValidatePoolName(parent.PoolName); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(parent.BasePathHash, "bp-") || len(parent.BasePathHash) != 35 || !lowerHex(parent.BasePathHash[3:]) {
			return nil, fmt.Errorf("parent %q base path hash is malformed", parent.ParentFilesystemID)
		}
		if _, duplicate := result[parent.ParentFilesystemID]; duplicate {
			return nil, fmt.Errorf("parent %q is duplicated", parent.ParentFilesystemID)
		}
		result[parent.ParentFilesystemID] = parent
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("upgrade parent set is empty")
	}
	return result, nil
}

func normalizedSchemaSet(name string, values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%s schema set is empty", name)
	}
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || len(value) > 32 || strings.ContainsAny(value, "\x00\r\n") {
			return nil, fmt.Errorf("%s schema %q is invalid", name, value)
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("%s schema %q is duplicated", name, value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func firstMissingSchema(required, available map[string]struct{}) string {
	missing := make([]string, 0)
	for schema := range required {
		if _, present := available[schema]; !present {
			missing = append(missing, schema)
		}
	}
	slices.Sort(missing)
	if len(missing) == 0 {
		return ""
	}
	return missing[0]
}

func validateGenerationDigest(value string) error {
	if len(value) != 64 || !lowerHex(value) {
		return fmt.Errorf("generation must be 64 lowercase hexadecimal characters")
	}
	return nil
}

func validPrefixedSHA256(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && lowerHex(value[7:])
}

func lowerHex(value string) bool {
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return value != ""
}
