package driverapp

import (
	"context"
	"errors"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

type recordedProviderErrors struct {
	counts map[observability.ProviderOperation]uint64
	err    error
}

func (metrics *recordedProviderErrors) AddProviderError(operation observability.ProviderOperation, count uint64) error {
	metrics.counts[operation] += count
	return metrics.err
}

func TestObservedScalewayAPICountsClosedProviderErrors(t *testing.T) {
	provider := scaleway.NewFakeAPI()
	metrics := &recordedProviderErrors{counts: make(map[observability.ProviderOperation]uint64)}
	failures := 0
	observed, err := newObservedScalewayAPI(provider, metrics, func(error) { failures++ })
	if err != nil {
		t.Fatalf("newObservedScalewayAPI() error = %v", err)
	}
	providerErr := errors.New("provider failed")

	provider.InjectFault("get-filesystem", providerErr)
	if _, err := observed.GetFilesystem(context.Background(), "fr-par", "11111111-1111-4111-8111-111111111111"); err != providerErr {
		t.Fatalf("GetFilesystem() error = %v, want exact provider error", err)
	}
	provider.InjectFault("list-attachments", providerErr)
	if _, err := observed.ListAttachments(context.Background(), scaleway.ListAttachmentsRequest{}); err != providerErr {
		t.Fatalf("ListAttachments() error = %v, want exact provider error", err)
	}
	provider.InjectFault("get-server", providerErr)
	if _, err := observed.GetServer(context.Background(), "fr-par-1", "22222222-2222-4222-8222-222222222222"); err != providerErr {
		t.Fatalf("GetServer() error = %v, want exact provider error", err)
	}
	provider.InjectFault("attach", providerErr)
	if err := observed.AttachServerFilesystem(context.Background(), "fr-par-1", "server", "filesystem"); err != providerErr {
		t.Fatalf("AttachServerFilesystem() error = %v, want exact provider error", err)
	}
	provider.InjectFault("detach", providerErr)
	if err := observed.DetachServerFilesystem(context.Background(), "fr-par-1", "server", "filesystem"); err != providerErr {
		t.Fatalf("DetachServerFilesystem() error = %v, want exact provider error", err)
	}
	for _, operation := range []observability.ProviderOperation{
		observability.ProviderGetFilesystem, observability.ProviderListAttachments,
		observability.ProviderGetServer, observability.ProviderAttachFilesystem,
		observability.ProviderDetachFilesystem,
	} {
		if metrics.counts[operation] != 1 {
			t.Errorf("provider metric %q = %d, want 1", operation, metrics.counts[operation])
		}
	}
	if failures != 0 {
		t.Fatalf("metric failure reports = %d", failures)
	}
}

func TestObservedScalewayAPIPreservesProviderErrorOnMetricFailure(t *testing.T) {
	provider := scaleway.NewFakeAPI()
	providerErr := errors.New("ambiguous attach response")
	provider.InjectFault("attach", providerErr)
	metricErr := errors.New("registry failure")
	metrics := &recordedProviderErrors{counts: make(map[observability.ProviderOperation]uint64), err: metricErr}
	var reported error
	observed, _ := newObservedScalewayAPI(provider, metrics, func(err error) { reported = err })
	if err := observed.AttachServerFilesystem(context.Background(), "fr-par-1", "server", "filesystem"); err != providerErr {
		t.Fatalf("AttachServerFilesystem() error = %v, want exact provider error", err)
	}
	if !errors.Is(reported, metricErr) {
		t.Fatalf("reported metric failure = %v", reported)
	}
}
