package driver

import (
	"fmt"
	"strings"

	releasecompat "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/compatibility"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// NodeRolloutObservation is the coherent Kubernetes and live provider
// projection for one Node, its CSINode registration, this release's node-plugin
// Pod, commercial Instance type, and physical File Storage capability.
type NodeRolloutObservation struct {
	NodeName             string
	CSINodeID            string
	OperatingSystem      string
	Schedulable          bool
	Deleting             bool
	Ready                bool
	PluginPodPresent     bool
	PluginPodReady       bool
	DriverRegistered     bool
	NodeConfigGeneration string
	CommercialType       string
	MaxFileSystems       uint32
}

// NodeAuthorizationSet separates new-publish eligibility from existing
// attachment recognition. A cordoned Linux Node with matching CSINode identity
// remains known but is not an eligible new publish target.
type NodeAuthorizationSet struct {
	EligibleNodeIDs map[string]struct{}
	KnownNodeIDs    map[string]struct{}
}

// ValidateNodeRollout proves every schedulable Linux workload node is Ready,
// advertises the exact expected driver generation, belongs to the configured
// region, uses a release-qualified type, and has a positive live attachment
// limit. It returns the authorized node-ID set used by controller publish.
func ValidateNodeRollout(observations []NodeRolloutObservation, expectedGeneration, region string, qualifiedCommercialTypes []string) (NodeAuthorizationSet, error) {
	if err := validateNodeGeneration(expectedGeneration); err != nil {
		return NodeAuthorizationSet{}, err
	}
	if region == "" || strings.ContainsAny(region, "/\x00\r\n") {
		return NodeAuthorizationSet{}, fmt.Errorf("eligible node region is invalid")
	}
	if err := releasecompat.ValidateCommercialTypes(qualifiedCommercialTypes); err != nil {
		return NodeAuthorizationSet{}, fmt.Errorf("eligible node commercial type allowlist: %w", err)
	}
	qualified := make(map[string]struct{}, len(qualifiedCommercialTypes))
	for _, commercialType := range qualifiedCommercialTypes {
		qualified[commercialType] = struct{}{}
	}
	if len(observations) == 0 {
		return NodeAuthorizationSet{}, fmt.Errorf("eligible node inventory is empty")
	}
	authorized := make(map[string]struct{})
	known := make(map[string]struct{})
	nodeNames := make(map[string]struct{}, len(observations))
	for index, observation := range observations {
		if observation.NodeName == "" || len(observation.NodeName) > 253 || strings.ContainsAny(observation.NodeName, "\x00\r\n") {
			return NodeAuthorizationSet{}, fmt.Errorf("node observation %d has invalid name", index)
		}
		if _, duplicate := nodeNames[observation.NodeName]; duplicate {
			return NodeAuthorizationSet{}, fmt.Errorf("node observation %q is duplicated", observation.NodeName)
		}
		nodeNames[observation.NodeName] = struct{}{}
		if observation.OperatingSystem != "linux" {
			continue
		}
		if observation.DriverRegistered {
			if err := volume.ValidateNodeID(observation.CSINodeID); err != nil {
				return NodeAuthorizationSet{}, fmt.Errorf("known node %q CSI node ID: %w", observation.NodeName, err)
			}
			zone := strings.SplitN(observation.CSINodeID, "/", 2)[0]
			if !strings.HasPrefix(zone, region+"-") {
				return NodeAuthorizationSet{}, fmt.Errorf("known node %q zone %q is outside region %q", observation.NodeName, zone, region)
			}
			if _, duplicate := known[observation.CSINodeID]; duplicate {
				return NodeAuthorizationSet{}, fmt.Errorf("CSI node ID %q is advertised by multiple known nodes", observation.CSINodeID)
			}
			known[observation.CSINodeID] = struct{}{}
		}
		if !observation.Schedulable {
			continue
		}
		if observation.Deleting || !observation.Ready || !observation.PluginPodPresent || !observation.PluginPodReady || !observation.DriverRegistered {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q is deleting, unready, missing its Ready plugin Pod, or lacks CSINode registration", observation.NodeName)
		}
		if err := volume.ValidateNodeID(observation.CSINodeID); err != nil {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q CSI node ID: %w", observation.NodeName, err)
		}
		zone := strings.SplitN(observation.CSINodeID, "/", 2)[0]
		if !strings.HasPrefix(zone, region+"-") {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q zone %q is outside region %q", observation.NodeName, zone, region)
		}
		if _, supported := qualified[observation.CommercialType]; !supported {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q commercial type %q is not release-qualified", observation.NodeName, observation.CommercialType)
		}
		if observation.MaxFileSystems == 0 {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q has no positive live MaxFileSystems capability", observation.NodeName)
		}
		if observation.NodeConfigGeneration != expectedGeneration {
			return NodeAuthorizationSet{}, fmt.Errorf("eligible node %q generation %q differs from expected %q", observation.NodeName, observation.NodeConfigGeneration, expectedGeneration)
		}
		if _, duplicate := authorized[observation.CSINodeID]; duplicate {
			return NodeAuthorizationSet{}, fmt.Errorf("CSI node ID %q is advertised by multiple eligible nodes", observation.CSINodeID)
		}
		authorized[observation.CSINodeID] = struct{}{}
	}
	if len(authorized) == 0 {
		return NodeAuthorizationSet{}, fmt.Errorf("no schedulable Ready Linux node satisfies the driver generation")
	}
	return NodeAuthorizationSet{EligibleNodeIDs: authorized, KnownNodeIDs: known}, nil
}

func validateNodeGeneration(value string) error {
	if len(value) != 64 {
		return fmt.Errorf("node configuration generation must contain 64 lowercase hexadecimal characters")
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return fmt.Errorf("node configuration generation must contain 64 lowercase hexadecimal characters")
		}
	}
	return nil
}

// ControllerCandidateObservation is the scheduler/preflight projection for a
// node that matches controller-only placement constraints.
type ControllerCandidateObservation struct {
	NodeName      string
	FailureDomain string
	Ready         bool
	Deleting      bool
	Compatible    bool
}

// ValidateControllerCandidates enforces the production v1 rescheduling floor:
// two Ready compatible nodes. The candidates may belong to the same Scaleway
// zone; this validation protects controller rescheduling capacity, not
// zone-level availability. It does not imply controller HA; Deployment
// strategy remains Recreate with one replica.
func ValidateControllerCandidates(candidates []ControllerCandidateObservation) error {
	nodes := make(map[string]struct{}, len(candidates))
	ready := 0
	for index, candidate := range candidates {
		if candidate.NodeName == "" || candidate.FailureDomain == "" || strings.ContainsAny(candidate.NodeName+candidate.FailureDomain, "\x00\r\n") {
			return fmt.Errorf("controller candidate %d has invalid node or failure-domain identity", index)
		}
		if _, duplicate := nodes[candidate.NodeName]; duplicate {
			return fmt.Errorf("controller candidate node %q is duplicated", candidate.NodeName)
		}
		nodes[candidate.NodeName] = struct{}{}
		if candidate.Ready && !candidate.Deleting && candidate.Compatible {
			ready++
		}
	}
	if ready < 2 {
		return fmt.Errorf("production controller requires at least two Ready compatible candidate nodes, got %d", ready)
	}
	return nil
}
