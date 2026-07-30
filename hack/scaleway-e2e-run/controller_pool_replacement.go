package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

// kapsuleReplacementPoolAPI is the smallest provider boundary needed to turn
// an exact N-1 run-owned pool back into its planned size. Keeping this boundary
// narrow makes the destructive request and its lost-response handling
// deterministic in tests.
type kapsuleReplacementPoolAPI interface {
	WaitForPool(*k8sapi.WaitForPoolRequest, ...scw.RequestOption) (*k8sapi.Pool, error)
	UpdatePool(*k8sapi.UpdatePoolRequest, ...scw.RequestOption) (*k8sapi.Pool, error)
}

// restorePlannedKapsulePoolSize replaces the node removed by the abrupt
// controller-failure scenario without relying on DeleteNode(replace=true).
// Live Kapsule qualification demonstrated that direct retirement of a
// stop_in_place Instance can leave that API path successfully converged at
// N-1. This method first waits for the exact run-owned pool to settle, validates
// every provider identity available on the Pool object, and only then restores
// its original desired size.
func (backend *scalewayBackend) restorePlannedKapsulePoolSize(
	ctx context.Context,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
) error {
	return restorePlannedKapsulePoolSize(ctx, backend.kubernetes, plan, clusterID, poolID)
}

func restorePlannedKapsulePoolSize(
	ctx context.Context,
	api kapsuleReplacementPoolAPI,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
) error {
	if api == nil || clusterID == "" || poolID == "" || plan.NodePool.Count < 2 {
		return fmt.Errorf("planned Kapsule replacement requires an exact API, cluster, pool, and at least two nodes")
	}
	timeout := 30 * time.Minute
	settled, err := api.WaitForPool(&k8sapi.WaitForPoolRequest{
		Region: scw.Region(plan.Region), PoolID: poolID, Timeout: &timeout,
	}, scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("wait for exact Kapsule pool before replacement: %w", err)
	}
	if err := validateReplacementPool(settled, plan, clusterID, poolID, plan.NodePool.Count-1, plan.NodePool.Count); err != nil {
		return err
	}
	if settled.Size == plan.NodePool.Count {
		return nil
	}

	size := plan.NodePool.Count
	updated, err := api.UpdatePool(&k8sapi.UpdatePoolRequest{
		Region: scw.Region(plan.Region), PoolID: poolID, Size: &size,
	}, scw.WithContext(ctx))
	if err == nil {
		if validateErr := validateReplacementPool(updated, plan, clusterID, poolID, size); validateErr != nil {
			return fmt.Errorf("validate accepted Kapsule replacement size: %w", validateErr)
		}
		return nil
	}
	if !providerObservationRetryable(ctx, err) {
		return fmt.Errorf("restore exact Kapsule pool to planned size %d: %w", size, err)
	}

	// UpdatePool can commit while its response is lost. Resolve only an
	// explicitly transient ambiguity through the same exact pool waiter; never
	// retry or reinterpret authorization, validation, conflict, or ownership
	// failures.
	reconciled, waitErr := api.WaitForPool(&k8sapi.WaitForPoolRequest{
		Region: scw.Region(plan.Region), PoolID: poolID, Timeout: &timeout,
	}, scw.WithContext(ctx))
	if waitErr != nil {
		return fmt.Errorf("resolve ambiguous Kapsule pool-size restoration: %w", errors.Join(err, waitErr))
	}
	if validateErr := validateReplacementPool(reconciled, plan, clusterID, poolID, size); validateErr != nil {
		return fmt.Errorf("resolve ambiguous Kapsule pool-size restoration: %w", errors.Join(err, validateErr))
	}
	return nil
}

func validateReplacementPool(
	pool *k8sapi.Pool,
	plan e2eplan.Plan,
	clusterID string,
	poolID string,
	allowedSizes ...uint32,
) error {
	if pool == nil {
		return fmt.Errorf("kapsule replacement pool response is empty")
	}
	if pool.ID != poolID {
		return fmt.Errorf("kapsule replacement pool ID differs from the exact run plan")
	}
	if pool.ClusterID != clusterID {
		return fmt.Errorf("kapsule replacement pool cluster differs from the exact run plan")
	}
	if pool.Name != plan.ResourcePrefix+"-nodes" {
		return fmt.Errorf("kapsule replacement pool name differs from the exact run plan")
	}
	if pool.Region.String() != plan.Region {
		return fmt.Errorf("kapsule replacement pool region differs from the exact run plan")
	}
	if !sameKapsuleCommercialType(pool.NodeType, plan.NodePool.CommercialType) {
		return fmt.Errorf(
			"kapsule replacement pool node type %q differs from the planned commercial type %q",
			pool.NodeType,
			plan.NodePool.CommercialType,
		)
	}
	if pool.Autoscaling || pool.Autohealing {
		return fmt.Errorf("kapsule replacement pool unexpectedly enables autoscaling or autohealing")
	}
	if !slices.Contains(pool.Tags, plan.OwnershipTag) {
		return fmt.Errorf("kapsule replacement pool lacks the exact run ownership tag")
	}
	if !slices.Contains(allowedSizes, pool.Size) {
		return fmt.Errorf("kapsule replacement pool size is %d, want one of %v", pool.Size, allowedSizes)
	}
	return nil
}

// sameKapsuleCommercialType accepts only the representation difference exposed
// by the live Scaleway APIs: the Kapsule Pool API returns lower-case words
// separated by underscores while the Instance catalog and create plan use
// upper-case words separated by hyphens. It deliberately rejects whitespace,
// omitted characters, suffixes, and every other punctuation difference so an
// actually different machine offer can never pass the ownership check.
func sameKapsuleCommercialType(observed, planned string) bool {
	normalize := func(value string) (string, bool) {
		if value == "" {
			return "", false
		}
		normalized := make([]byte, len(value))
		for index := range len(value) {
			character := value[index]
			switch {
			case character >= 'a' && character <= 'z':
				character -= 'a' - 'A'
			case character == '_':
				character = '-'
			case character >= 'A' && character <= 'Z':
			case character >= '0' && character <= '9':
			case character == '-':
			default:
				return "", false
			}
			normalized[index] = character
		}
		return string(normalized), true
	}

	normalizedObserved, observedValid := normalize(observed)
	normalizedPlanned, plannedValid := normalize(planned)
	return observedValid && plannedValid && normalizedObserved == normalizedPlanned
}
