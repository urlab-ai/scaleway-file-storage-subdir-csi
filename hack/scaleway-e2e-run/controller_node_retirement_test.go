package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	blockapi "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

type fakeControllerRetirementInstanceAPI struct {
	actionErr       error
	getResponse     *instanceapi.GetServerResponse
	getErr          error
	actionCalls     int
	requestedZone   scw.Zone
	requestedID     string
	requestedAction instanceapi.ServerAction
}

func (api *fakeControllerRetirementInstanceAPI) ServerActionAndWait(
	request *instanceapi.ServerActionAndWaitRequest,
	_ ...scw.RequestOption,
) error {
	api.actionCalls++
	api.requestedZone = request.Zone
	api.requestedID = request.ServerID
	api.requestedAction = request.Action
	return api.actionErr
}

func (api *fakeControllerRetirementInstanceAPI) GetServer(
	_ *instanceapi.GetServerRequest,
	_ ...scw.RequestOption,
) (*instanceapi.GetServerResponse, error) {
	return api.getResponse, api.getErr
}

func controllerRetirementFixture() (e2eplan.Plan, controllerRecoveryJournal, *instanceapi.Server, *blockapi.Volume) {
	plan := e2eplan.Plan{
		RunID:        recoveryTestRunID,
		ProjectID:    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		OwnershipTag: "sfs-subdir-e2e-run=" + recoveryTestRunID,
		NodePool:     e2eplan.NodePoolPlan{CommercialType: "POP2-HM-2C-16G"},
	}
	journal := controllerRecoveryJournal{
		ClusterID:        "22222222-2222-4222-8222-222222222222",
		PoolID:           "33333333-3333-4333-8333-333333333333",
		OldKapsuleNodeID: "44444444-4444-4444-8444-444444444444",
		OldServerID:      "55555555-5555-4555-8555-555555555555",
		OldRootVolumeID:  "66666666-6666-4666-8666-666666666666",
		OldZone:          "fr-par-1",
	}
	server := &instanceapi.Server{
		ID: journal.OldServerID, Project: plan.ProjectID, Zone: scw.Zone(journal.OldZone),
		CommercialType: plan.NodePool.CommercialType, State: instanceapi.ServerStateStoppedInPlace,
		Tags: []string{
			plan.OwnershipTag, "kapsule=" + journal.ClusterID,
			"pool=" + journal.PoolID, "node=" + journal.OldKapsuleNodeID,
		},
		Volumes: map[string]*instanceapi.VolumeServer{
			"0": {ID: journal.OldRootVolumeID, VolumeType: instanceapi.VolumeServerVolumeType("sbs_5k")},
		},
	}
	volume := &blockapi.Volume{
		ID: journal.OldRootVolumeID, ProjectID: plan.ProjectID, Zone: scw.Zone(journal.OldZone),
		Status: blockapi.VolumeStatusInUse,
		References: []*blockapi.Reference{{
			ProductResourceType: "instance_server", ProductResourceID: journal.OldServerID,
			Status: blockapi.ReferenceStatusAttached,
		}},
	}
	return plan, journal, server, volume
}

func TestControllerNodeRetirementRequiresExactStoppedRunOwnedServerAndRoot(t *testing.T) {
	plan, journal, server, volume := controllerRetirementFixture()
	rootID, err := validateControllerNodeServer(server, plan, journal, true)
	if err != nil || rootID != journal.OldRootVolumeID {
		t.Fatalf("validate exact controller server = %q, %v", rootID, err)
	}
	if err := validateControllerNodeRootVolume(volume, plan, journal, true); err != nil {
		t.Fatalf("validate exact attached root volume: %v", err)
	}

	running := *server
	running.State = instanceapi.ServerStateRunning
	if _, err := validateControllerNodeServer(&running, plan, journal, true); err == nil {
		t.Fatal("running controller Instance was accepted for destructive retirement")
	}
	foreign := *server
	foreign.Tags = []string{plan.OwnershipTag, "kapsule=" + journal.ClusterID, "pool=" + journal.PoolID}
	if _, err := validateControllerNodeServer(&foreign, plan, journal, true); err == nil {
		t.Fatal("controller Instance without exact Kapsule node tag was accepted")
	}
}

func TestPrepareControllerInstanceForDeletionPowersOffStoppedInPlaceServer(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	poweredOff := *server
	poweredOff.State = instanceapi.ServerStateStopped
	api := &fakeControllerRetirementInstanceAPI{
		getResponse: &instanceapi.GetServerResponse{Server: &poweredOff},
	}

	absent, err := prepareControllerInstanceForDeletion(context.Background(), api, plan, journal, server)
	if err != nil {
		t.Fatalf("prepare stopped-in-place controller Instance: %v", err)
	}
	if absent {
		t.Fatal("powered-off controller Instance reported absent")
	}
	if api.actionCalls != 1 || api.requestedZone != scw.Zone(journal.OldZone) ||
		api.requestedID != journal.OldServerID || api.requestedAction != instanceapi.ServerActionPoweroff {
		t.Fatalf("poweroff request = calls:%d zone:%q id:%q action:%q",
			api.actionCalls, api.requestedZone, api.requestedID, api.requestedAction)
	}
}

func TestPrepareControllerInstanceForDeletionAcceptsConcurrentProviderRetirement(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	notFound := fmt.Errorf("provider read: %w", &scw.ResourceNotFoundError{
		Resource:   "instance_server",
		ResourceID: journal.OldServerID,
	})
	api := &fakeControllerRetirementInstanceAPI{
		actionErr: errors.New("poweroff response lost"),
		getErr:    notFound,
	}

	absent, err := prepareControllerInstanceForDeletion(context.Background(), api, plan, journal, server)
	if err != nil {
		t.Fatalf("concurrent exact Instance deletion: %v", err)
	}
	if !absent {
		t.Fatal("concurrently deleted controller Instance reported present")
	}
}

func TestPrepareControllerInstanceForDeletionResolvesLostPoweroffResponse(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	poweredOff := *server
	poweredOff.State = instanceapi.ServerStateStopped
	api := &fakeControllerRetirementInstanceAPI{
		actionErr:   errors.New("poweroff response lost"),
		getResponse: &instanceapi.GetServerResponse{Server: &poweredOff},
	}

	absent, err := prepareControllerInstanceForDeletion(context.Background(), api, plan, journal, server)
	if err != nil || absent {
		t.Fatalf("committed poweroff with lost response = absent:%t error:%v", absent, err)
	}
}

func TestPrepareControllerInstanceForDeletionRejectsIncompleteOrForeignPoweroff(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	stillStoppedInPlace := *server
	foreign := *server
	foreign.Tags = []string{plan.OwnershipTag, "kapsule=" + journal.ClusterID, "pool=" + journal.PoolID}
	tests := []struct {
		name     string
		response *instanceapi.Server
	}{
		{name: "still stopped in place", response: &stillStoppedInPlace},
		{name: "foreign identity", response: &foreign},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			api := &fakeControllerRetirementInstanceAPI{
				getResponse: &instanceapi.GetServerResponse{Server: test.response},
			}
			if _, err := prepareControllerInstanceForDeletion(context.Background(), api, plan, journal, server); err == nil {
				t.Fatal("unsafe post-poweroff state was accepted")
			}
		})
	}
}

func TestPrepareControllerInstanceForDeletionIsIdempotentAfterPoweroffOrArchive(t *testing.T) {
	plan, journal, server, _ := controllerRetirementFixture()
	for _, state := range []instanceapi.ServerState{
		instanceapi.ServerStateStopped,
		controllerInstanceArchivedState,
	} {
		t.Run(state.String(), func(t *testing.T) {
			existing := *server
			existing.State = state
			api := &fakeControllerRetirementInstanceAPI{}
			absent, err := prepareControllerInstanceForDeletion(context.Background(), api, plan, journal, &existing)
			if err != nil || absent {
				t.Fatalf("idempotent retirement state %q = absent:%t error:%v", state, absent, err)
			}
			if api.actionCalls != 0 {
				t.Fatalf("idempotent retirement state %q requested poweroff", state)
			}
		})
	}
}

func TestControllerNodeRootDeletionRequiresDetachedExactVolume(t *testing.T) {
	plan, journal, _, volume := controllerRetirementFixture()
	volume.Status = blockapi.VolumeStatusAvailable
	volume.References = nil
	if err := validateControllerNodeRootVolume(volume, plan, journal, false); err != nil {
		t.Fatalf("validate detached exact root volume: %v", err)
	}
	volume.References = []*blockapi.Reference{{
		ProductResourceType: "instance_server",
		ProductResourceID:   "77777777-7777-4777-8777-777777777777",
		Status:              blockapi.ReferenceStatusAttached,
	}}
	if err := validateControllerNodeRootVolume(volume, plan, journal, false); err == nil {
		t.Fatal("root volume with a foreign reference was accepted for deletion")
	}
}

func TestResolveAmbiguousDeleteRequiresExactAbsence(t *testing.T) {
	deleteErr := errors.New("provider response lost")
	if err := resolveAmbiguousDelete(deleteErr, func() error { return nil }); err != nil {
		t.Fatalf("committed delete with proven absence = %v", err)
	}
	absenceErr := errors.New("resource still observable")
	err := resolveAmbiguousDelete(deleteErr, func() error { return absenceErr })
	if !errors.Is(err, deleteErr) || !errors.Is(err, absenceErr) {
		t.Fatalf("unresolved ambiguous delete = %v, want both causes", err)
	}
}
