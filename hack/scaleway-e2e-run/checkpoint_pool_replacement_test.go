package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

type fakePreRecoveryNodeDeletionAPI struct {
	response *k8sapi.Node
	err      error
	requests []*k8sapi.DeleteNodeRequest
}

type fakeCheckpointRetirementInstanceAPI struct {
	responses []*instanceapi.GetServerResponse
	getErrors []error
	actionErr error
	action    *instanceapi.ServerActionAndWaitRequest
}

func (fake *fakeCheckpointRetirementInstanceAPI) ServerActionAndWait(
	request *instanceapi.ServerActionAndWaitRequest,
	_ ...scw.RequestOption,
) error {
	copy := *request
	fake.action = &copy
	return fake.actionErr
}

func (fake *fakeCheckpointRetirementInstanceAPI) GetServer(
	_ *instanceapi.GetServerRequest,
	_ ...scw.RequestOption,
) (*instanceapi.GetServerResponse, error) {
	var response *instanceapi.GetServerResponse
	if len(fake.responses) != 0 {
		response = fake.responses[0]
		fake.responses = fake.responses[1:]
	}
	var err error
	if len(fake.getErrors) != 0 {
		err = fake.getErrors[0]
		fake.getErrors = fake.getErrors[1:]
	}
	return response, err
}

func (fake *fakePreRecoveryNodeDeletionAPI) DeleteNode(
	request *k8sapi.DeleteNodeRequest,
	_ ...scw.RequestOption,
) (*k8sapi.Node, error) {
	copy := *request
	fake.requests = append(fake.requests, &copy)
	return fake.response, fake.err
}

func TestExactPreRecoveryKapsuleNodeSelectsOnlyExactInstance(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	first := recoveryKapsuleNode(plan, clusterID, poolID, "node-a", "11111111-1111-4111-8111-111111111111")
	second := recoveryKapsuleNode(plan, clusterID, poolID, "node-b", "22222222-2222-4222-8222-222222222222")

	got, err := exactPreRecoveryKapsuleNode(
		[]*k8sapi.Node{first, second}, plan, clusterID, poolID, "fr-par-1",
		"22222222-2222-4222-8222-222222222222",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != second.ID {
		t.Fatalf("selected node = %#v, want %s", got, second.ID)
	}

	absent, err := exactPreRecoveryKapsuleNode(
		[]*k8sapi.Node{first}, plan, clusterID, poolID, "fr-par-1",
		"33333333-3333-4333-8333-333333333333",
	)
	if err != nil {
		t.Fatal(err)
	}
	if absent != nil {
		t.Fatalf("already retired Instance selected node %#v", absent)
	}
}

func TestExactPreRecoveryKapsuleNodeRejectsAmbiguousInventory(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	instanceID := "11111111-1111-4111-8111-111111111111"
	base := recoveryKapsuleNode(plan, clusterID, poolID, "node-a", instanceID)
	tests := map[string][]*k8sapi.Node{
		"nil node":        {nil},
		"foreign cluster": {cloneRecoveryNode(base, func(node *k8sapi.Node) { node.ClusterID = "foreign" })},
		"foreign pool":    {cloneRecoveryNode(base, func(node *k8sapi.Node) { node.PoolID = "foreign" })},
		"foreign region":  {cloneRecoveryNode(base, func(node *k8sapi.Node) { node.Region = scw.RegionNlAms })},
		"foreign zone":    {cloneRecoveryNode(base, func(node *k8sapi.Node) { node.ProviderID = "scaleway://instance/fr-par-2/" + instanceID })},
		"malformed ID":    {cloneRecoveryNode(base, func(node *k8sapi.Node) { node.ProviderID = instanceID })},
		"duplicate":       {base, cloneRecoveryNode(base, func(node *k8sapi.Node) { node.ID = "node-duplicate" })},
	}
	for name, nodes := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := exactPreRecoveryKapsuleNode(
				nodes, plan, clusterID, poolID, "fr-par-1", instanceID,
			); err == nil {
				t.Fatal("ambiguous node inventory was accepted")
			}
		})
	}
}

func TestDeleteExactPreRecoveryKapsuleNodeDisablesImplicitReplacement(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	target := recoveryKapsuleNode(
		plan, clusterID, poolID, "node-a", "11111111-1111-4111-8111-111111111111",
	)
	api := &fakePreRecoveryNodeDeletionAPI{response: target}
	if err := deleteExactPreRecoveryKapsuleNode(context.Background(), api, plan, target); err != nil {
		t.Fatal(err)
	}
	if len(api.requests) != 1 {
		t.Fatalf("DeleteNode calls = %d, want 1", len(api.requests))
	}
	request := api.requests[0]
	if request.Region.String() != plan.Region || request.NodeID != target.ID ||
		request.Replace || request.SkipDrain {
		t.Fatalf("DeleteNode request is not the exact no-replacement operation: %#v", request)
	}
}

func TestDeleteExactPreRecoveryKapsuleNodeFailsClosed(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	target := recoveryKapsuleNode(
		plan, clusterID, poolID, "node-a", "11111111-1111-4111-8111-111111111111",
	)
	t.Run("provider error", func(t *testing.T) {
		api := &fakePreRecoveryNodeDeletionAPI{err: errors.New("provider unavailable")}
		if err := deleteExactPreRecoveryKapsuleNode(context.Background(), api, plan, target); err == nil {
			t.Fatal("provider failure was accepted")
		}
	})
	t.Run("mismatched response", func(t *testing.T) {
		foreign := *target
		foreign.ID = "foreign"
		api := &fakePreRecoveryNodeDeletionAPI{response: &foreign}
		if err := deleteExactPreRecoveryKapsuleNode(context.Background(), api, plan, target); err == nil {
			t.Fatal("mismatched DeleteNode response was accepted")
		}
	})
	t.Run("empty target", func(t *testing.T) {
		api := &fakePreRecoveryNodeDeletionAPI{}
		if err := deleteExactPreRecoveryKapsuleNode(context.Background(), api, plan, nil); err == nil {
			t.Fatal("empty exact target was accepted")
		}
	})
}

func TestCheckpointRetirementRequiresAnotherReadyPoolNode(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	instanceID := "11111111-1111-4111-8111-111111111111"
	target := recoveryKapsuleNode(plan, clusterID, poolID, "node-a", instanceID)
	target.Status = k8sapi.NodeStatusDeleting
	survivor := recoveryKapsuleNode(
		plan, clusterID, poolID, "node-b", "22222222-2222-4222-8222-222222222222",
	)
	retirement := checkpointNodeRetirement{
		KapsuleNodeID: target.ID,
		NodeName:      target.Name,
		InstanceID:    instanceID,
		RootVolumeID:  "33333333-3333-4333-8333-333333333333",
	}
	if err := validateCheckpointRetirementSurvivor(
		[]*k8sapi.Node{target, survivor}, plan, clusterID, poolID, "fr-par-1", retirement,
	); err != nil {
		t.Fatalf("one Ready survivor was rejected: %v", err)
	}
	survivor.Status = k8sapi.NodeStatusDeleting
	if err := validateCheckpointRetirementSurvivor(
		[]*k8sapi.Node{target, survivor}, plan, clusterID, poolID, "fr-par-1", retirement,
	); err == nil {
		t.Fatal("checkpoint retirement without a Ready survivor was accepted")
	}
}

func TestLegacyCheckpointRetirementRequiresConclusiveNodeAbsenceAndReadySurvivor(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	absentInstanceID := "11111111-1111-4111-8111-111111111111"
	survivor := recoveryKapsuleNode(
		plan, clusterID, poolID, "node-b", "22222222-2222-4222-8222-222222222222",
	)
	retirement := checkpointNodeRetirement{
		InstanceID:    absentInstanceID,
		AlreadyAbsent: true,
	}
	if err := validateCheckpointRetirementSurvivor(
		[]*k8sapi.Node{survivor}, plan, clusterID, poolID, "fr-par-1", retirement,
	); err != nil {
		t.Fatalf("conclusively absent legacy node with one Ready survivor was rejected: %v", err)
	}

	stillPresent := recoveryKapsuleNode(
		plan, clusterID, poolID, "node-a", absentInstanceID,
	)
	stillPresent.Status = k8sapi.NodeStatusDeleting
	if err := validateCheckpointRetirementSurvivor(
		[]*k8sapi.Node{stillPresent, survivor}, plan, clusterID, poolID, "fr-par-1", retirement,
	); err == nil {
		t.Fatal("legacy already-absent record accepted an existing Kapsule node")
	}

	survivor.Status = k8sapi.NodeStatusDeleting
	if err := validateCheckpointRetirementSurvivor(
		[]*k8sapi.Node{survivor}, plan, clusterID, poolID, "fr-par-1", retirement,
	); err == nil {
		t.Fatal("legacy retirement without a Ready survivor was accepted")
	}
}

func TestStopCheckpointKapsuleInstanceUsesExactStopInPlaceFence(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	server.State = instanceapi.ServerStateRunning
	stopped := *server
	stopped.State = instanceapi.ServerStateStoppedInPlace
	api := &fakeCheckpointRetirementInstanceAPI{
		responses: []*instanceapi.GetServerResponse{
			{Server: server},
			{Server: &stopped},
		},
		actionErr: errors.New("stop response lost"),
	}
	absent, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal)
	if err != nil || absent {
		t.Fatalf("exact checkpoint stop = absent:%t error:%v", absent, err)
	}
	if api.action == nil || api.action.Zone.String() != journal.OldZone ||
		api.action.ServerID != journal.OldServerID ||
		api.action.Action != instanceapi.ServerActionStopInPlace {
		t.Fatalf("checkpoint stop request is not the exact stop-in-place fence: %#v", api.action)
	}
}

func TestStopCheckpointKapsuleInstanceAcceptsRootDetachOnlyAfterStop(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	server.State = instanceapi.ServerStateRunning
	stopped := *server
	stopped.State = instanceapi.ServerStateStoppedInPlace
	stopped.Volumes = nil
	api := &fakeCheckpointRetirementInstanceAPI{
		responses: []*instanceapi.GetServerResponse{
			{Server: server},
			{Server: &stopped},
		},
	}
	if absent, err := stopCheckpointKapsuleInstance(
		context.Background(), api, plan, journal,
	); err != nil || absent {
		t.Fatalf("post-stop root detach = absent:%t error:%v", absent, err)
	}

	runningWithoutRoot := *server
	runningWithoutRoot.Volumes = nil
	api = &fakeCheckpointRetirementInstanceAPI{
		responses: []*instanceapi.GetServerResponse{{Server: &runningWithoutRoot}},
	}
	if _, err := stopCheckpointKapsuleInstance(
		context.Background(), api, plan, journal,
	); err == nil {
		t.Fatal("running Instance without its exact attached root was accepted")
	}
	if api.action != nil {
		t.Fatal("unsafe running Instance received a stop request")
	}
}

func TestStopCheckpointKapsuleInstanceFailsClosedOnAmbiguousState(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	server.State = instanceapi.ServerStateRunning
	stillRunning := *server
	api := &fakeCheckpointRetirementInstanceAPI{
		responses: []*instanceapi.GetServerResponse{
			{Server: server},
			{Server: &stillRunning},
		},
	}
	if _, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal); err == nil {
		t.Fatal("checkpoint Instance still running after stop was accepted")
	}

	foreign := *server
	foreign.Tags = nil
	api = &fakeCheckpointRetirementInstanceAPI{
		responses: []*instanceapi.GetServerResponse{{Server: &foreign}},
	}
	if _, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal); err == nil {
		t.Fatal("foreign checkpoint Instance was accepted")
	}
}

func TestStopCheckpointKapsuleInstanceIsIdempotent(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	for _, state := range []instanceapi.ServerState{
		instanceapi.ServerStateStopped,
		instanceapi.ServerStateStoppedInPlace,
		controllerInstanceArchivedState,
	} {
		t.Run(state.String(), func(t *testing.T) {
			observed := *server
			observed.State = state
			api := &fakeCheckpointRetirementInstanceAPI{
				responses: []*instanceapi.GetServerResponse{{Server: &observed}},
			}
			absent, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal)
			if err != nil || absent || api.action != nil {
				t.Fatalf("idempotent checkpoint stop state %q = absent:%t action:%#v error:%v",
					state, absent, api.action, err)
			}
		})
		t.Run(state.String()+"-root-already-detached", func(t *testing.T) {
			observed := *server
			observed.State = state
			observed.Volumes = nil
			api := &fakeCheckpointRetirementInstanceAPI{
				responses: []*instanceapi.GetServerResponse{{Server: &observed}},
			}
			absent, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal)
			if err != nil || absent || api.action != nil {
				t.Fatalf("idempotent detached-root state %q = absent:%t action:%#v error:%v",
					state, absent, api.action, err)
			}
		})
	}

	notFound := fmt.Errorf("provider read: %w", &scw.ResourceNotFoundError{
		Resource: "instance_server", ResourceID: journal.OldServerID,
	})
	api := &fakeCheckpointRetirementInstanceAPI{getErrors: []error{notFound}}
	absent, err := stopCheckpointKapsuleInstance(context.Background(), api, plan, journal)
	if err != nil || !absent {
		t.Fatalf("already absent checkpoint Instance = absent:%t error:%v", absent, err)
	}
}

func recoveryKapsuleNode(
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	nodeID string,
	instanceID string,
) *k8sapi.Node {
	return &k8sapi.Node{
		ID: nodeID, ClusterID: clusterID, PoolID: poolID, Region: scw.Region(plan.Region),
		Name: nodeID, Status: k8sapi.NodeStatusReady,
		ProviderID: "scaleway://instance/fr-par-1/" + instanceID,
	}
}

func cloneRecoveryNode(node *k8sapi.Node, mutate func(*k8sapi.Node)) *k8sapi.Node {
	copy := *node
	mutate(&copy)
	return &copy
}
