package driverapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const (
	scalewayInstanceProviderIDPrefix = "scaleway://instance/"
	operationalRolloutInitialBackoff = time.Second
	operationalRolloutMaximumBackoff = 15 * time.Second
)

// provisionalRecoveryAuthorization is an unforgeable-outside-this-package
// capability for the one pre-approval parent inspection path. It retains the
// live provisional Lease session so every parent operation can revalidate the
// exact holder and discovery marker instead of trusting an earlier snapshot.
type provisionalRecoveryAuthorization struct {
	holder     coordination.HolderEvidence
	leadership parentBootstrapLeadership
}

func newProvisionalRecoveryAuthorization(mode coordination.AcquisitionMode, mutationAllowed bool, leadership parentBootstrapLeadership, holder coordination.HolderEvidence) (provisionalRecoveryAuthorization, error) {
	if mode != coordination.AcquisitionProvisionalRecovery || mutationAllowed {
		return provisionalRecoveryAuthorization{}, fmt.Errorf("provisional recovery authorization requires a non-mutating provisional acquisition")
	}
	if leadership == nil {
		return provisionalRecoveryAuthorization{}, fmt.Errorf("provisional recovery leadership is nil")
	}
	authorization := provisionalRecoveryAuthorization{holder: holder, leadership: leadership}
	if err := authorization.validate(context.Background()); err != nil {
		return provisionalRecoveryAuthorization{}, err
	}
	return authorization, nil
}

func (authorization provisionalRecoveryAuthorization) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if authorization.leadership == nil {
		return fmt.Errorf("provisional recovery authorization is empty")
	}
	if err := authorization.holder.Validate(); err != nil {
		return err
	}
	select {
	case <-authorization.leadership.Context().Done():
		return coordination.ErrLeadershipNotActive
	default:
	}
	snapshot := authorization.leadership.Snapshot()
	holder, present, err := coordination.ParseHolderEvidence(snapshot.Annotations)
	if err != nil {
		return fmt.Errorf("parse provisional recovery holder evidence: %w", err)
	}
	if !present || holder != authorization.holder || snapshot.HolderIdentity != authorization.holder.PodUID {
		return fmt.Errorf("provisional recovery Lease holder differs from the authorized runtime identity")
	}
	if _, present, err := coordination.ParseDiscoveryMarker(snapshot.Annotations, authorization.holder); err != nil {
		return fmt.Errorf("parse provisional recovery discovery marker: %w", err)
	} else if !present {
		return fmt.Errorf("provisional recovery discovery marker is absent")
	}
	return nil
}

// provisionalRecoveryTarget derives a singleton attachment authorization from
// the live provisional holder. It deliberately ignores node-plugin Pod and
// CSINode readiness because the recovery contract requires the DaemonSet to be
// absent before approval. The result is never stored in the normal
// authorization cache.
func (authorizations *controllerNodeAuthorizations) provisionalRecoveryTarget(ctx context.Context, authorization provisionalRecoveryAuthorization) (scaleway.Target, map[string]scaleway.Target, map[string]struct{}, error) {
	if err := authorization.validate(ctx); err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	observed, err := authorizations.inventory.Snapshot(ctx)
	if err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	var localProviderID, localOperatingSystem string
	var localReady, localDeleting, localPresent bool
	for _, node := range observed {
		if node.NodeName != authorization.holder.NodeName {
			continue
		}
		if localPresent {
			return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery Kubernetes inventory repeats local node %q", node.NodeName)
		}
		localPresent = true
		localProviderID, localOperatingSystem = node.ProviderID, node.OperatingSystem
		localReady, localDeleting = node.Ready, node.Deleting
	}
	if !localPresent {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery local Kubernetes node %q is absent", authorization.holder.NodeName)
	}
	if localOperatingSystem != "linux" || !localReady || localDeleting {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery local Kubernetes node %q is not a stable Ready Linux node", authorization.holder.NodeName)
	}
	target, err := parseScalewayInstanceProviderID(localProviderID)
	if err != nil {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery local Kubernetes node %q provider identity: %w", authorization.holder.NodeName, err)
	}
	holderTarget, err := scaleway.ParseNodeID(authorization.holder.CSINodeID)
	if err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	if target != holderTarget || target.Zone != authorization.holder.Zone || target.ServerID != authorization.holder.InstanceID {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery Kubernetes provider identity differs from Lease holder evidence")
	}
	server, err := authorizations.provider.GetServer(ctx, target.Zone, target.ServerID)
	if err != nil {
		return scaleway.Target{}, nil, nil, fmt.Errorf("read provisional recovery controller Instance: %w", err)
	}
	if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != authorizations.region || server.ProjectID != authorizations.projectID {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery controller Instance differs from configured provider scope")
	}
	if err := server.State.PermitNewAttachment(); err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	if !slices.Contains(authorizations.commercial, server.CommercialType) {
		return scaleway.Target{}, nil, nil, fmt.Errorf("provisional recovery controller Instance commercial type %q is not release-qualified", server.CommercialType)
	}
	if err := scaleway.ValidateExclusiveServerInventory(server, authorizations.parents); err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	if err := scaleway.ValidatePostAttachBudget(server, authorizations.parents); err != nil {
		return scaleway.Target{}, nil, nil, err
	}
	known := map[string]scaleway.Target{target.ServerID: target}
	eligible := map[string]struct{}{target.ServerID: {}}
	return target, known, eligible, nil
}

func parseScalewayInstanceProviderID(providerID string) (scaleway.Target, error) {
	if !strings.HasPrefix(providerID, scalewayInstanceProviderIDPrefix) {
		return scaleway.Target{}, fmt.Errorf("provider ID is not canonical Scaleway Instance identity")
	}
	nodeID := strings.TrimPrefix(providerID, scalewayInstanceProviderIDPrefix)
	target, err := scaleway.ParseNodeID(nodeID)
	if err != nil {
		return scaleway.Target{}, err
	}
	if providerID != scalewayInstanceProviderIDPrefix+target.Zone+"/"+target.ServerID {
		return scaleway.Target{}, fmt.Errorf("provider ID is not canonical Scaleway Instance identity")
	}
	return target, nil
}

// waitForOperationalNodeRollout is used only after approved recovery
// promotion. It retries the closed rollout-convergence class while the
// controller remains non-serving; every identity, provider, compatibility, or
// attachment failure returns immediately.
func (authorizations *controllerNodeAuthorizations) waitForOperationalNodeRollout(ctx context.Context, operationClock clock.Clock, jitter scaleway.Jitter, deadline time.Duration) error {
	if operationClock == nil || jitter == nil || deadline <= 0 {
		return fmt.Errorf("operational node rollout wait configuration is invalid")
	}
	waitCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	expiresAt := operationClock.Now().Add(deadline)
	backoff := operationalRolloutInitialBackoff
	for attempt := uint32(0); ; attempt++ {
		if _, err := authorizations.RefreshSnapshot(waitCtx); err == nil {
			return nil
		} else if !errors.Is(err, driver.ErrNodeRolloutNotReady) {
			return err
		} else {
			if waitErr := waitCtx.Err(); waitErr != nil {
				return errors.Join(err, fmt.Errorf("wait for operational node rollout: %w", waitErr))
			}
			remaining := expiresAt.Sub(operationClock.Now())
			if remaining <= 0 {
				return errors.Join(err, fmt.Errorf("wait for operational node rollout deadline: %w", scaleway.ErrDeadlineExceeded))
			}
			delay := jitter.Delay(backoff, attempt)
			if delay <= 0 || delay > remaining {
				delay = remaining
			}
			timer := operationClock.NewTimer(delay)
			select {
			case <-waitCtx.Done():
				timer.Stop()
				return errors.Join(err, fmt.Errorf("wait for operational node rollout: %w", waitCtx.Err()))
			case <-timer.C():
				timer.Stop()
			}
			if backoff < operationalRolloutMaximumBackoff {
				if backoff > operationalRolloutMaximumBackoff-backoff {
					backoff = operationalRolloutMaximumBackoff
				} else {
					backoff *= 2
				}
			}
		}
	}
}
