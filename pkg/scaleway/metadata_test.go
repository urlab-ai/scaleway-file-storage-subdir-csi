package scaleway

import (
	"context"
	"errors"
	"testing"
)

type cancelingMetadataSource struct {
	identity NodeIdentity
	cancel   context.CancelFunc
}

func (source cancelingMetadataSource) Load(context.Context) (NodeIdentity, error) {
	source.cancel()
	return source.identity, nil
}

func TestNodeIdentityProducesExactNodeID(t *testing.T) {
	identity := NodeIdentity{
		InstanceID:     "11111111-1111-4111-8111-111111111111",
		Zone:           "fr-par-2",
		Region:         "fr-par",
		CommercialType: "release-qualified",
	}
	nodeID, err := identity.NodeID()
	if err != nil {
		t.Fatalf("NodeID() error = %v", err)
	}
	const want = "fr-par-2/11111111-1111-4111-8111-111111111111"
	if nodeID != want {
		t.Fatalf("NodeID() = %q, want %q", nodeID, want)
	}
	source := &FakeMetadataSource{Identity: identity}
	if _, err := source.Load(context.Background()); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if source.Calls != 1 {
		t.Fatalf("Load() calls = %d, want 1", source.Calls)
	}
}

func TestNodeIdentityRejectsRegionMismatch(t *testing.T) {
	identity := NodeIdentity{
		InstanceID:     "11111111-1111-4111-8111-111111111111",
		Zone:           "nl-ams-1",
		Region:         "fr-par",
		CommercialType: "release-qualified",
	}
	if err := identity.Validate(); err == nil {
		t.Fatal("Validate() error = nil")
	}
}

func TestNodeIdentityRejectsMalformedProviderMetadata(t *testing.T) {
	base := NodeIdentity{
		InstanceID:     "11111111-1111-4111-8111-111111111111",
		Zone:           "fr-par-2",
		Region:         "fr-par",
		CommercialType: "release-qualified",
	}
	tests := map[string]func(*NodeIdentity){
		"region":           func(identity *NodeIdentity) { identity.Region = "fr/par" },
		"zone separator":   func(identity *NodeIdentity) { identity.Zone = "fr-par-2/other" },
		"commercial slash": func(identity *NodeIdentity) { identity.CommercialType = "type/other" },
		"commercial NUL":   func(identity *NodeIdentity) { identity.CommercialType = "type\x00other" },
		"commercial UTF-8": func(identity *NodeIdentity) { identity.CommercialType = string([]byte{0xff}) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			identity := base
			mutate(&identity)
			if err := identity.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
			if _, err := identity.NodeID(); err == nil {
				t.Fatal("NodeID() error = nil")
			}
		})
	}
}

func TestResolveNodeIdentityBindsMetadataToRuntimeCompatibility(t *testing.T) {
	identity := NodeIdentity{
		InstanceID:     "11111111-1111-4111-8111-111111111111",
		Zone:           "fr-par-2",
		Region:         "fr-par",
		CommercialType: "TEST-TYPE-1",
	}
	source := &FakeMetadataSource{Identity: identity}
	resolved, err := ResolveNodeIdentity(context.Background(), source, "fr-par", []string{"TEST-TYPE-1", "TEST-TYPE-2"})
	if err != nil {
		t.Fatalf("ResolveNodeIdentity() error = %v", err)
	}
	if resolved != identity || source.Calls != 1 {
		t.Fatalf("ResolveNodeIdentity() = %#v after %d calls", resolved, source.Calls)
	}

	for name, testCase := range map[string]struct {
		region          string
		commercialTypes []string
	}{
		"different region":    {region: "nl-ams", commercialTypes: []string{"TEST-TYPE-1"}},
		"unqualified type":    {region: "fr-par", commercialTypes: []string{"TEST-TYPE-2"}},
		"empty allowlist":     {region: "fr-par", commercialTypes: nil},
		"ambiguous allowlist": {region: "fr-par", commercialTypes: []string{"TEST-TYPE-1", "TEST-TYPE-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			source := &FakeMetadataSource{Identity: identity}
			if _, err := ResolveNodeIdentity(context.Background(), source, testCase.region, testCase.commercialTypes); err == nil {
				t.Fatal("ResolveNodeIdentity() error = nil")
			}
			if source.Calls != 1 {
				t.Fatalf("metadata calls = %d, want 1", source.Calls)
			}
		})
	}
}

func TestResolveNodeIdentityFailsClosedBeforeOrAfterMetadataRead(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	source := &FakeMetadataSource{}
	if _, err := ResolveNodeIdentity(cancelled, source, "fr-par", []string{"TEST-TYPE-1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveNodeIdentity(cancelled) error = %v", err)
	}
	if source.Calls != 0 {
		t.Fatalf("cancelled metadata calls = %d, want 0", source.Calls)
	}

	identity := NodeIdentity{
		InstanceID:     "11111111-1111-4111-8111-111111111111",
		Zone:           "fr-par-2",
		Region:         "fr-par",
		CommercialType: "TEST-TYPE-1",
	}
	duringLoad, cancelDuringLoad := context.WithCancel(context.Background())
	if _, err := ResolveNodeIdentity(duringLoad, cancelingMetadataSource{identity: identity, cancel: cancelDuringLoad}, "fr-par", []string{"TEST-TYPE-1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveNodeIdentity(cancel during load) error = %v", err)
	}

	sourceErr := errors.New("metadata endpoint unavailable")
	source = &FakeMetadataSource{Err: sourceErr}
	if _, err := ResolveNodeIdentity(context.Background(), source, "fr-par", []string{"TEST-TYPE-1"}); !errors.Is(err, sourceErr) {
		t.Fatalf("ResolveNodeIdentity(source error) error = %v", err)
	}
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if _, err := ResolveNodeIdentity(nil, source, "fr-par", []string{"TEST-TYPE-1"}); err == nil {
		t.Fatal("ResolveNodeIdentity(nil context) error = nil")
	}
	if _, err := ResolveNodeIdentity(context.Background(), nil, "fr-par", []string{"TEST-TYPE-1"}); err == nil {
		t.Fatal("ResolveNodeIdentity(nil source) error = nil")
	}
}
