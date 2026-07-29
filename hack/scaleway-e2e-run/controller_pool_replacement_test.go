package main

import (
	"context"
	"errors"
	"testing"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

type fakeKapsuleReplacementPoolAPI struct {
	waitResponses  []*k8sapi.Pool
	waitErrors     []error
	updateResponse *k8sapi.Pool
	updateError    error
	updateRequests []*k8sapi.UpdatePoolRequest
}

func (fake *fakeKapsuleReplacementPoolAPI) WaitForPool(
	_ *k8sapi.WaitForPoolRequest,
	_ ...scw.RequestOption,
) (*k8sapi.Pool, error) {
	if len(fake.waitResponses) == 0 && len(fake.waitErrors) == 0 {
		return nil, errors.New("unexpected WaitForPool call")
	}
	var response *k8sapi.Pool
	var err error
	if len(fake.waitResponses) > 0 {
		response = fake.waitResponses[0]
		fake.waitResponses = fake.waitResponses[1:]
	}
	if len(fake.waitErrors) > 0 {
		err = fake.waitErrors[0]
		fake.waitErrors = fake.waitErrors[1:]
	}
	return response, err
}

func (fake *fakeKapsuleReplacementPoolAPI) UpdatePool(
	request *k8sapi.UpdatePoolRequest,
	_ ...scw.RequestOption,
) (*k8sapi.Pool, error) {
	copy := *request
	if request.Size != nil {
		size := *request.Size
		copy.Size = &size
	}
	fake.updateRequests = append(fake.updateRequests, &copy)
	return fake.updateResponse, fake.updateError
}

func TestRestorePlannedKapsulePoolSizeUsesExplicitExactResize(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	settled := replacementPool(plan, clusterID, poolID, 1, k8sapi.PoolStatusReady)
	updated := replacementPool(plan, clusterID, poolID, 2, k8sapi.PoolStatusScaling)
	api := &fakeKapsuleReplacementPoolAPI{
		waitResponses:  []*k8sapi.Pool{settled},
		updateResponse: updated,
	}

	if err := restorePlannedKapsulePoolSize(context.Background(), api, plan, clusterID, poolID); err != nil {
		t.Fatal(err)
	}
	if len(api.updateRequests) != 1 {
		t.Fatalf("UpdatePool calls = %d, want 1", len(api.updateRequests))
	}
	request := api.updateRequests[0]
	if request.Region.String() != plan.Region || request.PoolID != poolID ||
		request.Size == nil || *request.Size != plan.NodePool.Count ||
		request.MinSize != nil || request.MaxSize != nil {
		t.Fatalf("UpdatePool request differs from exact planned-size restoration: %#v", request)
	}
}

func TestRestorePlannedKapsulePoolSizeIsIdempotent(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	api := &fakeKapsuleReplacementPoolAPI{
		waitResponses: []*k8sapi.Pool{
			replacementPool(plan, clusterID, poolID, plan.NodePool.Count, k8sapi.PoolStatusReady),
		},
	}
	if err := restorePlannedKapsulePoolSize(context.Background(), api, plan, clusterID, poolID); err != nil {
		t.Fatal(err)
	}
	if len(api.updateRequests) != 0 {
		t.Fatalf("idempotent restoration issued %d updates", len(api.updateRequests))
	}
}

func TestRestorePlannedKapsulePoolSizeRejectsForeignOrUnexpectedPool(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	tests := map[string]func(*k8sapi.Pool){
		"foreign cluster": func(pool *k8sapi.Pool) { pool.ClusterID = "foreign" },
		"foreign name":    func(pool *k8sapi.Pool) { pool.Name = "foreign" },
		"missing run tag": func(pool *k8sapi.Pool) { pool.Tags = nil },
		"wrong node type": func(pool *k8sapi.Pool) { pool.NodeType = "foreign" },
		"autoscaling":     func(pool *k8sapi.Pool) { pool.Autoscaling = true },
		"unexpected size": func(pool *k8sapi.Pool) { pool.Size = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			pool := replacementPool(plan, clusterID, poolID, 1, k8sapi.PoolStatusReady)
			mutate(pool)
			api := &fakeKapsuleReplacementPoolAPI{waitResponses: []*k8sapi.Pool{pool}}
			if err := restorePlannedKapsulePoolSize(context.Background(), api, plan, clusterID, poolID); err == nil {
				t.Fatal("foreign or unexpected pool was accepted")
			}
			if len(api.updateRequests) != 0 {
				t.Fatal("foreign or unexpected pool was mutated")
			}
		})
	}
}

func TestRestorePlannedKapsulePoolSizeResolvesLostUpdateResponse(t *testing.T) {
	plan, clusterID, poolID := replacementPoolFixture()
	api := &fakeKapsuleReplacementPoolAPI{
		waitResponses: []*k8sapi.Pool{
			replacementPool(plan, clusterID, poolID, 1, k8sapi.PoolStatusReady),
			replacementPool(plan, clusterID, poolID, 2, k8sapi.PoolStatusReady),
		},
		updateError: providerTimeoutError{},
	}
	if err := restorePlannedKapsulePoolSize(context.Background(), api, plan, clusterID, poolID); err != nil {
		t.Fatal(err)
	}
	if len(api.updateRequests) != 1 {
		t.Fatalf("UpdatePool calls = %d, want 1", len(api.updateRequests))
	}
}

func replacementPoolFixture() (e2eplan.Plan, string, string) {
	return e2eplan.Plan{
		Region:         "fr-par",
		ResourcePrefix: "sfs-e2e-00000000-0000-4000-8000-000000000000",
		OwnershipTag:   "sfs-subdir-e2e-run=00000000-0000-4000-8000-000000000000",
		NodePool: e2eplan.NodePoolPlan{
			Count:          2,
			CommercialType: "POP2-HM-2C-16G",
		},
	}, "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
}

func replacementPool(
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	size uint32,
	status k8sapi.PoolStatus,
) *k8sapi.Pool {
	return &k8sapi.Pool{
		ID: poolID, ClusterID: clusterID, Name: plan.ResourcePrefix + "-nodes",
		Status: status, NodeType: plan.NodePool.CommercialType, Size: size,
		Tags: []string{plan.OwnershipTag}, Region: scw.Region(plan.Region),
	}
}
