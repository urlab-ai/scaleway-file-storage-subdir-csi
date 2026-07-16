package scaleway

import (
	"context"
	"fmt"
	"slices"

	releasecompat "scaleway-sfs-subdir-csi/internal/compatibility"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// NodeIdentity is the validated local unauthenticated metadata projection used
// by the node service. It contains no API credential or authenticated client.
type NodeIdentity struct {
	InstanceID     string
	Zone           string
	Region         string
	CommercialType string
}

// NodeID returns the exact CSI <zone>/<serverID> identity.
func (identity NodeIdentity) NodeID() (string, error) {
	if err := identity.Validate(); err != nil {
		return "", err
	}
	return identity.Zone + "/" + identity.InstanceID, nil
}

// Validate rejects incomplete or cross-region metadata.
func (identity NodeIdentity) Validate() error {
	if err := volume.ValidateParentFilesystemID(identity.InstanceID); err != nil {
		return fmt.Errorf("metadata Instance ID: %w", err)
	}
	if err := validateProviderRegion(identity.Region); err != nil {
		return fmt.Errorf("metadata region: %w", err)
	}
	if err := validateTargetInRegion(Target{Zone: identity.Zone, ServerID: identity.InstanceID}, identity.Region); err != nil {
		return fmt.Errorf("metadata node identity: %w", err)
	}
	if err := releasecompat.ValidateCommercialTypes([]string{identity.CommercialType}); err != nil {
		return fmt.Errorf("metadata commercial type: %w", err)
	}
	return nil
}

// ValidateForRuntime binds valid local metadata to the exact node runtime
// contract. Syntax validity alone is insufficient: an otherwise well-formed
// identity from another region or an Instance type not qualified by this
// release must never make the node plugin serving.
func (identity NodeIdentity) ValidateForRuntime(expectedRegion string, qualifiedCommercialTypes []string) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := validateProviderRegion(expectedRegion); err != nil {
		return fmt.Errorf("expected runtime region: %w", err)
	}
	if identity.Region != expectedRegion {
		return fmt.Errorf("metadata region %q differs from runtime region %q", identity.Region, expectedRegion)
	}
	if err := releasecompat.ValidateCommercialTypes(qualifiedCommercialTypes); err != nil {
		return fmt.Errorf("runtime commercial type allowlist: %w", err)
	}
	if !slices.Contains(qualifiedCommercialTypes, identity.CommercialType) {
		return fmt.Errorf("metadata commercial type %q is not qualified by this release", identity.CommercialType)
	}
	return nil
}

// MetadataSource loads local Instance identity without authenticated public API
// access. The real transport is implemented only from the verified official
// driver/SDK metadata contract.
type MetadataSource interface {
	Load(ctx context.Context) (NodeIdentity, error)
}

// ResolveNodeIdentity performs the one credential-free metadata read required
// at node startup and validates it against the immutable runtime contract. It
// returns no partially trusted identity after cancellation, source failure, or
// compatibility mismatch.
func ResolveNodeIdentity(ctx context.Context, source MetadataSource, expectedRegion string, qualifiedCommercialTypes []string) (NodeIdentity, error) {
	if ctx == nil {
		return NodeIdentity{}, fmt.Errorf("node metadata context is nil")
	}
	if source == nil {
		return NodeIdentity{}, fmt.Errorf("node metadata source is nil")
	}
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	identity, err := source.Load(ctx)
	if err != nil {
		return NodeIdentity{}, fmt.Errorf("load local node metadata: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	if err := identity.ValidateForRuntime(expectedRegion, qualifiedCommercialTypes); err != nil {
		return NodeIdentity{}, fmt.Errorf("validate local node metadata: %w", err)
	}
	return identity, nil
}

// FakeMetadataSource is a deterministic credential-free node identity source.
type FakeMetadataSource struct {
	Identity NodeIdentity
	Err      error
	Calls    int
}

// Load returns configured metadata and honors cancellation.
func (source *FakeMetadataSource) Load(ctx context.Context) (NodeIdentity, error) {
	if err := ctx.Err(); err != nil {
		return NodeIdentity{}, err
	}
	source.Calls++
	if source.Err != nil {
		return NodeIdentity{}, source.Err
	}
	if err := source.Identity.Validate(); err != nil {
		return NodeIdentity{}, err
	}
	return source.Identity, nil
}
