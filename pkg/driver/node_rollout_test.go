package driver

import (
	"strings"
	"testing"
)

func rolloutNode(name, nodeID, generation string) NodeRolloutObservation {
	return NodeRolloutObservation{
		NodeName: name, CSINodeID: nodeID, OperatingSystem: "linux", Schedulable: true,
		Ready: true, PluginPodPresent: true, PluginPodReady: true,
		DriverRegistered: true, NodeConfigGeneration: generation,
		CommercialType: "TEST-TYPE-1", MaxFileSystems: 2,
	}
}

var rolloutCommercialTypes = []string{"TEST-TYPE-1"}

func TestValidateNodeRolloutAuthorizesEveryExactReadyGeneration(t *testing.T) {
	generation := strings.Repeat("a", 64)
	observations := []NodeRolloutObservation{
		rolloutNode("worker-a", "fr-par-1/11111111-1111-4111-8111-111111111111", generation),
		rolloutNode("worker-b", "fr-par-2/22222222-2222-4222-8222-222222222222", generation),
		{NodeName: "windows-a", OperatingSystem: "windows", Schedulable: true},
		{NodeName: "cordoned-linux", OperatingSystem: "linux", Schedulable: false, DriverRegistered: true, CSINodeID: "fr-par-3/33333333-3333-4333-8333-333333333333"},
	}
	authorized, err := ValidateNodeRollout(observations, generation, "fr-par", rolloutCommercialTypes)
	if err != nil {
		t.Fatalf("ValidateNodeRollout() error = %v", err)
	}
	if len(authorized.EligibleNodeIDs) != 2 || len(authorized.KnownNodeIDs) != 3 {
		t.Fatalf("authorized nodes = %#v", authorized)
	}
	if _, present := authorized.KnownNodeIDs[observations[3].CSINodeID]; !present {
		t.Fatalf("known nodes missing cordoned identity %q", observations[3].CSINodeID)
	}
}

func TestValidateNodeRolloutRejectsEveryIncompleteEligibleNode(t *testing.T) {
	generation := strings.Repeat("a", 64)
	tests := map[string]func(*NodeRolloutObservation){
		"deleting":        func(node *NodeRolloutObservation) { node.Deleting = true },
		"Node unready":    func(node *NodeRolloutObservation) { node.Ready = false },
		"Pod absent":      func(node *NodeRolloutObservation) { node.PluginPodPresent = false },
		"Pod unready":     func(node *NodeRolloutObservation) { node.PluginPodReady = false },
		"CSINode missing": func(node *NodeRolloutObservation) { node.DriverRegistered = false },
		"generation":      func(node *NodeRolloutObservation) { node.NodeConfigGeneration = strings.Repeat("b", 64) },
		"node ID":         func(node *NodeRolloutObservation) { node.CSINodeID = "invalid" },
		"commercial type": func(node *NodeRolloutObservation) { node.CommercialType = "UNTESTED" },
		"attach limit":    func(node *NodeRolloutObservation) { node.MaxFileSystems = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			node := rolloutNode("worker-a", "fr-par-1/11111111-1111-4111-8111-111111111111", generation)
			mutate(&node)
			if _, err := ValidateNodeRollout([]NodeRolloutObservation{node}, generation, "fr-par", rolloutCommercialTypes); err == nil {
				t.Fatal("ValidateNodeRollout(incomplete) error = nil")
			}
		})
	}
}

func TestValidateNodeRolloutRejectsDuplicateIdentityAndMalformedGeneration(t *testing.T) {
	generation := strings.Repeat("a", 64)
	node := rolloutNode("worker-a", "fr-par-1/11111111-1111-4111-8111-111111111111", generation)
	duplicateName := rolloutNode("worker-a", "fr-par-2/22222222-2222-4222-8222-222222222222", generation)
	if _, err := ValidateNodeRollout([]NodeRolloutObservation{node, duplicateName}, generation, "fr-par", rolloutCommercialTypes); err == nil {
		t.Fatal("ValidateNodeRollout(duplicate name) error = nil")
	}
	duplicateID := rolloutNode("worker-b", node.CSINodeID, generation)
	if _, err := ValidateNodeRollout([]NodeRolloutObservation{node, duplicateID}, generation, "fr-par", rolloutCommercialTypes); err == nil {
		t.Fatal("ValidateNodeRollout(duplicate CSI node ID) error = nil")
	}
	if _, err := ValidateNodeRollout([]NodeRolloutObservation{node}, "not-a-generation", "fr-par", rolloutCommercialTypes); err == nil {
		t.Fatal("ValidateNodeRollout(malformed expected generation) error = nil")
	}
	if _, err := ValidateNodeRollout([]NodeRolloutObservation{node}, generation, "nl-ams", rolloutCommercialTypes); err == nil {
		t.Fatal("ValidateNodeRollout(cross-region node) error = nil")
	}
	if _, err := ValidateNodeRollout([]NodeRolloutObservation{node}, generation, "fr-par", nil); err == nil {
		t.Fatal("ValidateNodeRollout(empty allowlist) error = nil")
	}
}

func TestValidateControllerCandidatesAllowsTwoReadyNodesInOneFailureDomain(t *testing.T) {
	candidates := []ControllerCandidateObservation{
		{NodeName: "worker-a", FailureDomain: "fr-par-1", Ready: true, Compatible: true},
		{NodeName: "worker-b", FailureDomain: "fr-par-1", Ready: true, Compatible: true},
	}
	if err := ValidateControllerCandidates(candidates); err != nil {
		t.Fatalf("ValidateControllerCandidates() error = %v", err)
	}
	candidates[1].Deleting = true
	if err := ValidateControllerCandidates(candidates); err == nil {
		t.Fatal("ValidateControllerCandidates(one Ready candidate) error = nil")
	}
}
