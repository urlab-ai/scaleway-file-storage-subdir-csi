package driverapp

import (
	"context"
	"fmt"
	"log/slog"

	"scaleway-sfs-subdir-csi/pkg/observability"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
)

type providerErrorMetric interface {
	AddProviderError(operation observability.ProviderOperation, count uint64) error
}

// observedScalewayAPI counts only failed authenticated provider calls with a
// closed operation label. A metrics failure is reported out of band and never
// replaces the provider result, especially for ambiguous attach/detach calls.
type observedScalewayAPI struct {
	delegate scaleway.API
	metrics  providerErrorMetric
	failure  func(error)
}

func newObservedScalewayAPI(delegate scaleway.API, metrics providerErrorMetric, failure func(error)) (*observedScalewayAPI, error) {
	if delegate == nil || metrics == nil || failure == nil {
		return nil, fmt.Errorf("observed provider API dependency is nil")
	}
	return &observedScalewayAPI{delegate: delegate, metrics: metrics, failure: failure}, nil
}

func (api *observedScalewayAPI) GetFilesystem(ctx context.Context, region, filesystemID string) (scaleway.Filesystem, error) {
	result, err := api.delegate.GetFilesystem(ctx, region, filesystemID)
	api.observe(ctx, observability.ProviderGetFilesystem, err, "region", region, "parent_filesystem_id", filesystemID)
	return result, err
}

func (api *observedScalewayAPI) ListAttachments(ctx context.Context, request scaleway.ListAttachmentsRequest) (scaleway.AttachmentPage, error) {
	result, err := api.delegate.ListAttachments(ctx, request)
	api.observe(ctx, observability.ProviderListAttachments, err, "region", request.Region, "parent_filesystem_id", request.FilesystemID)
	return result, err
}

func (api *observedScalewayAPI) GetServer(ctx context.Context, zone, serverID string) (scaleway.Server, error) {
	result, err := api.delegate.GetServer(ctx, zone, serverID)
	api.observe(ctx, observability.ProviderGetServer, err, "zone", zone, "instance_id", serverID)
	return result, err
}

func (api *observedScalewayAPI) AttachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	err := api.delegate.AttachServerFilesystem(ctx, zone, serverID, filesystemID)
	api.observe(ctx, observability.ProviderAttachFilesystem, err, "zone", zone, "instance_id", serverID, "parent_filesystem_id", filesystemID)
	return err
}

func (api *observedScalewayAPI) DetachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	err := api.delegate.DetachServerFilesystem(ctx, zone, serverID, filesystemID)
	api.observe(ctx, observability.ProviderDetachFilesystem, err, "zone", zone, "instance_id", serverID, "parent_filesystem_id", filesystemID)
	return err
}

func (api *observedScalewayAPI) observe(ctx context.Context, operation observability.ProviderOperation, providerErr error, attributes ...any) {
	attributes = append([]any{"provider_operation", operation}, attributes...)
	if providerErr != nil {
		attributes = append(attributes, "error", providerErr)
		slog.WarnContext(ctx, "Scaleway API operation failed", attributes...)
		if err := api.metrics.AddProviderError(operation, 1); err != nil {
			api.failure(fmt.Errorf("record provider %s failure: %w", operation, err))
		}
		return
	}
	if operation == observability.ProviderAttachFilesystem || operation == observability.ProviderDetachFilesystem {
		slog.InfoContext(ctx, "Scaleway API mutation completed", attributes...)
		return
	}
	slog.DebugContext(ctx, "Scaleway API read completed", attributes...)
}

var _ scaleway.API = (*observedScalewayAPI)(nil)
