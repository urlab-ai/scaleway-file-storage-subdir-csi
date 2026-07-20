package scaleway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"unicode/utf8"

	file "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instance "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
	"github.com/scaleway/scaleway-sdk-go/validation"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type fileSDK interface {
	GetFileSystem(*file.GetFileSystemRequest, ...scw.RequestOption) (*file.FileSystem, error)
	ListAttachments(*file.ListAttachmentsRequest, ...scw.RequestOption) (*file.ListAttachmentsResponse, error)
}

type instanceSDK interface {
	GetServer(*instance.GetServerRequest, ...scw.RequestOption) (*instance.GetServerResponse, error)
	ListServersTypes(*instance.ListServersTypesRequest, ...scw.RequestOption) (*instance.ListServersTypesResponse, error)
	AttachServerFileSystem(*instance.AttachServerFileSystemRequest, ...scw.RequestOption) (*instance.AttachServerFileSystemResponse, error)
	DetachServerFileSystem(*instance.DetachServerFileSystemRequest, ...scw.RequestOption) (*instance.DetachServerFileSystemResponse, error)
}

// SDKOptions contains the validated controller-only provider authority. Secret
// values are passed directly to the SDK clients and are never retained in
// durable state, errors, logs, or metrics.
type SDKOptions struct {
	Region    string
	ProjectID string
	Zone      string
	AccessKey string
	SecretKey string
	UserAgent string
}

// SDKAPI implements API with the pinned Scaleway SDK. It deliberately owns two
// clients: the File Storage client has no default zone, while the Instance
// client may have one for SDK initialization but every operation supplies its
// parsed target zone explicitly.
type SDKAPI struct {
	file     fileSDK
	instance instanceSDK
}

// NewSDKAPI constructs authenticated clients without consulting a config file
// or inheriting an environment-provided zone on regional File Storage reads.
func NewSDKAPI(options SDKOptions) (*SDKAPI, error) {
	if err := validateProviderScope(options.Region, options.ProjectID); err != nil {
		return nil, err
	}
	zone, err := scw.ParseZone(options.Zone)
	if err != nil {
		return nil, fmt.Errorf("provider default zone: %w", err)
	}
	region, err := zone.Region()
	if err != nil || region.String() != options.Region {
		return nil, fmt.Errorf("provider default zone %q is outside region %q", options.Zone, options.Region)
	}
	if !validation.IsAccessKey(options.AccessKey) || !validation.IsSecretKey(options.SecretKey) {
		return nil, fmt.Errorf("provider credential format is invalid")
	}
	if !utf8.ValidString(options.UserAgent) || len(options.UserAgent) == 0 || len(options.UserAgent) > 256 {
		return nil, fmt.Errorf("provider user agent must contain 1 to 256 UTF-8 bytes")
	}
	parsedRegion, _ := scw.ParseRegion(options.Region)
	common := []scw.ClientOption{
		scw.WithAuth(options.AccessKey, options.SecretKey),
		scw.WithDefaultProjectID(options.ProjectID),
		scw.WithDefaultRegion(parsedRegion),
		scw.WithUserAgent(options.UserAgent),
	}
	fileClient, err := scw.NewClient(common...)
	if err != nil {
		return nil, fmt.Errorf("construct regional File Storage SDK client")
	}
	instanceOptions := append([]scw.ClientOption{}, common...)
	instanceOptions = append(instanceOptions, scw.WithDefaultZone(zone))
	instanceClient, err := scw.NewClient(instanceOptions...)
	if err != nil {
		return nil, fmt.Errorf("construct Instance SDK client")
	}
	return newSDKAPI(file.NewAPI(fileClient), instance.NewAPI(instanceClient))
}

func newSDKAPI(fileAPI fileSDK, instanceAPI instanceSDK) (*SDKAPI, error) {
	if fileAPI == nil || instanceAPI == nil {
		return nil, fmt.Errorf("scaleway SDK API dependency is nil")
	}
	return &SDKAPI{file: fileAPI, instance: instanceAPI}, nil
}

// GetFilesystem returns normalized authoritative parent metadata.
func (api *SDKAPI) GetFilesystem(ctx context.Context, region, filesystemID string) (Filesystem, error) {
	if err := validateProviderRegion(region); err != nil {
		return Filesystem{}, fmt.Errorf("filesystem region: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(filesystemID); err != nil {
		return Filesystem{}, fmt.Errorf("filesystem ID: %v: %w", err, ErrInvalidArgument)
	}
	parsedRegion, _ := scw.ParseRegion(region)
	filesystem, err := api.file.GetFileSystem(&file.GetFileSystemRequest{Region: parsedRegion, FilesystemID: filesystemID}, scw.WithContext(ctx))
	if err != nil {
		return Filesystem{}, classifySDKError(ctx, err)
	}
	if filesystem == nil {
		return Filesystem{}, fmt.Errorf("file Storage API returned a nil filesystem: %w", ErrUnavailable)
	}
	return Filesystem{
		ID: filesystem.ID, ProjectID: filesystem.ProjectID, Region: filesystem.Region.String(),
		SizeBytes: uint64(filesystem.Size), Status: NormalizeFilesystemStatus(filesystem.Status.String()),
		NumberOfAttachments: filesystem.NumberOfAttachments,
	}, nil
}

// ListAttachments sends an explicit nil zone through a File Storage SDK client
// that has no default zone, preserving the required cross-zone regional view.
func (api *SDKAPI) ListAttachments(ctx context.Context, request ListAttachmentsRequest) (AttachmentPage, error) {
	if request.Zone != nil {
		return AttachmentPage{}, fmt.Errorf("attachment inventory zone filter must be nil: %w", ErrInvalidArgument)
	}
	if err := validateProviderRegion(request.Region); err != nil {
		return AttachmentPage{}, fmt.Errorf("attachment region: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(request.FilesystemID); err != nil {
		return AttachmentPage{}, fmt.Errorf("attachment filesystem ID: %v: %w", err, ErrInvalidArgument)
	}
	if request.PageSize == 0 || request.PageSize > 100 {
		return AttachmentPage{}, fmt.Errorf("attachment page size must be in [1,100]: %w", ErrInvalidArgument)
	}
	pageNumber := int32(1)
	if request.PageToken != "" {
		parsed, err := strconv.ParseInt(request.PageToken, 10, 32)
		if err != nil || parsed < 2 || strconv.FormatInt(parsed, 10) != request.PageToken {
			return AttachmentPage{}, fmt.Errorf("attachment page token is invalid: %w", ErrInvalidArgument)
		}
		pageNumber = int32(parsed)
	}
	region, _ := scw.ParseRegion(request.Region)
	filesystemID := request.FilesystemID
	pageSize := request.PageSize
	response, err := api.file.ListAttachments(&file.ListAttachmentsRequest{
		Region: region, FilesystemID: &filesystemID, Zone: nil,
		Page: &pageNumber, PageSize: &pageSize,
	}, scw.WithContext(ctx))
	if err != nil {
		return AttachmentPage{}, classifySDKError(ctx, err)
	}
	if response == nil {
		return AttachmentPage{}, fmt.Errorf("file Storage API returned a nil attachment page: %w", ErrUnavailable)
	}
	result := AttachmentPage{Attachments: make([]Attachment, 0, len(response.Attachments))}
	for index, value := range response.Attachments {
		if value == nil || value.Zone == nil {
			return AttachmentPage{}, fmt.Errorf("attachment page entry %d has nil identity or zone: %w", index, ErrUnavailable)
		}
		resourceType := AttachmentResourceUnknown
		if value.ResourceType == file.AttachmentResourceTypeInstanceServer {
			resourceType = AttachmentResourceServer
		}
		result.Attachments = append(result.Attachments, Attachment{
			ID: value.ID, FilesystemID: value.FilesystemID, ResourceID: value.ResourceID,
			ResourceType: resourceType, Zone: value.Zone.String(),
		})
	}
	offset := uint64(pageNumber-1) * uint64(pageSize)
	observedEnd := offset + uint64(len(response.Attachments))
	if observedEnd > response.TotalCount || (observedEnd < response.TotalCount && len(response.Attachments) == 0) {
		return AttachmentPage{}, fmt.Errorf("attachment pagination count is inconsistent: %w", ErrUnavailable)
	}
	if observedEnd < response.TotalCount {
		result.NextPageToken = strconv.FormatInt(int64(pageNumber)+1, 10)
	}
	return result, nil
}

// GetServer returns the complete Instance filesystem inventory plus the live
// commercial-type MaxFileSystems capability from the same explicit zone.
func (api *SDKAPI) GetServer(ctx context.Context, zone, serverID string) (Server, error) {
	target := Target{Zone: zone, ServerID: serverID}
	if err := validateSDKTarget(target); err != nil {
		return Server{}, fmt.Errorf("server target: %v: %w", err, ErrInvalidArgument)
	}
	parsedZone, _ := scw.ParseZone(zone)
	response, err := api.instance.GetServer(&instance.GetServerRequest{Zone: parsedZone, ServerID: serverID}, scw.WithContext(ctx))
	if err != nil {
		return Server{}, classifySDKError(ctx, err)
	}
	if response == nil || response.Server == nil {
		return Server{}, fmt.Errorf("instance API returned a nil server: %w", ErrUnavailable)
	}
	typesResponse, err := api.instance.ListServersTypes(&instance.ListServersTypesRequest{Zone: parsedZone}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return Server{}, classifySDKError(ctx, err)
	}
	if typesResponse == nil {
		return Server{}, fmt.Errorf("instance API returned nil server types: %w", ErrUnavailable)
	}
	serverType, present := typesResponse.Servers[response.Server.CommercialType]
	if !present || serverType == nil || serverType.Capabilities == nil {
		return Server{}, fmt.Errorf("instance commercial type %q has no live capability record: %w", response.Server.CommercialType, ErrFailedPrecondition)
	}
	region, err := parsedZone.Region()
	if err != nil {
		return Server{}, fmt.Errorf("derive server region: %v: %w", err, ErrUnavailable)
	}
	result := Server{
		ID: response.Server.ID, ProjectID: response.Server.Project, Zone: response.Server.Zone.String(), Region: region.String(),
		CommercialType: response.Server.CommercialType, State: NormalizeInstanceState(response.Server.State.String()),
		MaxFileSystems: serverType.Capabilities.MaxFileSystems,
		Filesystems:    make([]ServerFilesystem, 0, len(response.Server.Filesystems)),
	}
	for index, filesystem := range response.Server.Filesystems {
		if filesystem == nil {
			return Server{}, fmt.Errorf("server filesystem entry %d is nil: %w", index, ErrUnavailable)
		}
		result.Filesystems = append(result.Filesystems, ServerFilesystem{
			FilesystemID: filesystem.FilesystemID,
			State:        NormalizeServerFilesystemState(filesystem.State.String()),
		})
	}
	return result, nil
}

// AttachServerFilesystem issues exactly one SDK mutation. The caller owns all
// reread, ambiguity, polling, and one-call semantics.
func (api *SDKAPI) AttachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	return api.mutateAttachment(ctx, true, zone, serverID, filesystemID)
}

// DetachServerFilesystem issues exactly one authorized offline SDK mutation.
// It is never called by normal logical-volume unpublish.
func (api *SDKAPI) DetachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	return api.mutateAttachment(ctx, false, zone, serverID, filesystemID)
}

func (api *SDKAPI) mutateAttachment(ctx context.Context, attach bool, zone, serverID, filesystemID string) error {
	if err := validateSDKTarget(Target{Zone: zone, ServerID: serverID}); err != nil {
		return fmt.Errorf("attachment target: %v: %w", err, ErrInvalidArgument)
	}
	if err := volume.ValidateParentFilesystemID(filesystemID); err != nil {
		return fmt.Errorf("attachment filesystem ID: %v: %w", err, ErrInvalidArgument)
	}
	parsedZone, _ := scw.ParseZone(zone)
	var err error
	if attach {
		_, err = api.instance.AttachServerFileSystem(&instance.AttachServerFileSystemRequest{
			Zone: parsedZone, ServerID: serverID, FilesystemID: filesystemID,
		}, scw.WithContext(ctx))
	} else {
		_, err = api.instance.DetachServerFileSystem(&instance.DetachServerFileSystemRequest{
			Zone: parsedZone, ServerID: serverID, FilesystemID: filesystemID,
		}, scw.WithContext(ctx))
	}
	if err != nil {
		return classifySDKError(ctx, err)
	}
	return nil
}

func classifySDKError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	var notFound *scw.ResourceNotFoundError
	var permissions *scw.PermissionsDeniedError
	var deniedAuth *scw.DeniedAuthenticationError
	var quota *scw.QuotasExceededError
	var outOfStock *scw.OutOfStockError
	var transient *scw.TransientStateError
	var locked *scw.ResourceLockedError
	var precondition *scw.PreconditionFailedError
	var invalidArguments *scw.InvalidArgumentsError
	var response *scw.ResponseError
	switch {
	case errors.As(err, &notFound):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case errors.As(err, &permissions), errors.As(err, &deniedAuth):
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	case errors.As(err, &quota), errors.As(err, &outOfStock):
		return fmt.Errorf("%w: %v", ErrResourceExhausted, err)
	case errors.As(err, &precondition):
		return fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
	case errors.As(err, &transient), errors.As(err, &locked):
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	case errors.As(err, &invalidArguments):
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	case errors.As(err, &response):
		switch response.StatusCode {
		case http.StatusBadRequest:
			return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
		case http.StatusNotFound:
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case http.StatusConflict:
			return fmt.Errorf("%w: %v", ErrConflict, err)
		case http.StatusPreconditionFailed:
			return fmt.Errorf("%w: %v", ErrFailedPrecondition, err)
		case http.StatusRequestTimeout, http.StatusTooManyRequests:
			return fmt.Errorf("%w: %v", ErrUnavailable, err)
		default:
			if response.StatusCode >= 500 {
				return fmt.Errorf("%w: %v", ErrUnavailable, err)
			}
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		// A transport timeout is ambiguous provider availability while the
		// caller's operation context is still live. Only the operation context
		// itself authoritatively proves that its deadline was exhausted.
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}

var _ API = (*SDKAPI)(nil)

func validateSDKTarget(target Target) error {
	parsed, err := ParseNodeID(target.Zone + "/" + target.ServerID)
	if err != nil || parsed != target {
		return fmt.Errorf("provider target %q/%q is invalid", target.Zone, target.ServerID)
	}
	return nil
}
