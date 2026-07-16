package driver

import (
	"context"
	"errors"
	"testing"
)

type fakeNodeEvidence struct {
	normal bool
	err    error
}

func (evidence fakeNodeEvidence) NormalUnpublishAllowed(context.Context, string) (bool, error) {
	return evidence.normal, evidence.err
}

type fakeProviderFence struct {
	err   error
	calls int
}

func (fence *fakeProviderFence) ProveFenced(context.Context, string, string) error {
	fence.calls++
	return fence.err
}

func TestConservativeFenceVerifierUsesNormalEvidenceWithoutProvider(t *testing.T) {
	provider := &fakeProviderFence{err: errors.New("must not be called")}
	verifier, err := NewConservativeFenceVerifier(fakeNodeEvidence{normal: true}, provider)
	if err != nil {
		t.Fatalf("NewConservativeFenceVerifier() error = %v", err)
	}
	if err := verifier.SafeToClear(context.Background(), "node", "parent"); err != nil {
		t.Fatalf("SafeToClear(normal) error = %v", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestConservativeFenceVerifierRequiresProviderWhenNormalEvidenceMissing(t *testing.T) {
	provider := &fakeProviderFence{}
	verifier, err := NewConservativeFenceVerifier(fakeNodeEvidence{normal: false}, provider)
	if err != nil {
		t.Fatalf("NewConservativeFenceVerifier() error = %v", err)
	}
	if err := verifier.SafeToClear(context.Background(), "node", "parent"); err != nil {
		t.Fatalf("SafeToClear(provider fenced) error = %v", err)
	}
	provider.err = errors.New("Instance still running")
	if err := verifier.SafeToClear(context.Background(), "node", "parent"); err == nil {
		t.Fatal("SafeToClear(unfenced) error = nil")
	}
}

func TestConservativeFenceVerifierCanUseProviderDespiteUnreadableKubernetes(t *testing.T) {
	provider := &fakeProviderFence{}
	verifier, err := NewConservativeFenceVerifier(fakeNodeEvidence{err: ErrNormalNodeEvidenceUnavailable}, provider)
	if err != nil {
		t.Fatalf("NewConservativeFenceVerifier() error = %v", err)
	}
	if err := verifier.SafeToClear(context.Background(), "node", "parent"); err != nil {
		t.Fatalf("SafeToClear(provider proof after unreadable Kubernetes) error = %v", err)
	}
}
