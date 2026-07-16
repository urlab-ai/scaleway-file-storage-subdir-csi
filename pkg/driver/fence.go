package driver

import (
	"context"
	"errors"
	"fmt"
)

// ErrNormalNodeEvidenceUnavailable distinguishes ambiguous Kubernetes reads
// from a conclusive non-normal path. Both preserve the fence unless provider
// fencing independently succeeds.
var ErrNormalNodeEvidenceUnavailable = errors.New("normal Node/CSINode evidence unavailable")

// NormalNodeEvidence validates that the current Node exists, is Ready, has no
// out-of-service taint, and advertises the exact CSI node ID in CSINode.
type NormalNodeEvidence interface {
	NormalUnpublishAllowed(ctx context.Context, nodeID string) (bool, error)
}

// ProviderFence proves a stopped/deleted Instance and clean parent attachment
// inventories.
type ProviderFence interface {
	ProveFenced(ctx context.Context, nodeID, parentFilesystemID string) error
}

// ConservativeFenceVerifier accepts either complete normal-node evidence or a
// conclusive provider fence. A deleted VolumeAttachment is never an input.
type ConservativeFenceVerifier struct {
	nodes    NormalNodeEvidence
	provider ProviderFence
}

// NewConservativeFenceVerifier validates its evidence sources.
func NewConservativeFenceVerifier(nodes NormalNodeEvidence, provider ProviderFence) (*ConservativeFenceVerifier, error) {
	if nodes == nil || provider == nil {
		return nil, fmt.Errorf("fence verifier dependency is nil")
	}
	return &ConservativeFenceVerifier{nodes: nodes, provider: provider}, nil
}

// SafeToClear implements the normal-or-provider-fenced decision.
func (verifier *ConservativeFenceVerifier) SafeToClear(ctx context.Context, nodeID, parentFilesystemID string) error {
	normal, nodeErr := verifier.nodes.NormalUnpublishAllowed(ctx, nodeID)
	if nodeErr == nil && normal {
		return nil
	}
	providerErr := verifier.provider.ProveFenced(ctx, nodeID, parentFilesystemID)
	if providerErr == nil {
		return nil
	}
	if nodeErr != nil {
		return errors.Join(fmt.Errorf("normal node evidence: %w", nodeErr), fmt.Errorf("provider fence: %w", providerErr))
	}
	return fmt.Errorf("node %q is outside the normal CSI path and provider fencing is not conclusive: %w", nodeID, providerErr)
}
