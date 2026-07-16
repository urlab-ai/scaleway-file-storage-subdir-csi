package scaleway

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	file "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instance "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
)

func TestClassifySDKErrorKeepsConflictDistinctFromPrecondition(t *testing.T) {
	conflict := classifySDKError(context.Background(), &scw.ResponseError{
		StatusCode: http.StatusConflict,
		Status:     "409 Conflict",
		Message:    "concurrent attachment",
	})
	if !errors.Is(conflict, ErrConflict) || errors.Is(conflict, ErrFailedPrecondition) {
		t.Fatalf("classifySDKError(409) = %v, want only ErrConflict", conflict)
	}

	precondition := classifySDKError(context.Background(), &scw.ResponseError{
		StatusCode: http.StatusPreconditionFailed,
		Status:     "412 Precondition Failed",
		Message:    "definite precondition rejection",
	})
	if !errors.Is(precondition, ErrFailedPrecondition) || errors.Is(precondition, ErrConflict) {
		t.Fatalf("classifySDKError(412) = %v, want only ErrFailedPrecondition", precondition)
	}
}

type fakeFileSDK struct {
	filesystem      *file.FileSystem
	filesystemErr   error
	attachments     map[int32]*file.ListAttachmentsResponse
	attachmentErr   error
	attachmentCalls []*file.ListAttachmentsRequest
}

func (sdk *fakeFileSDK) GetFileSystem(_ *file.GetFileSystemRequest, _ ...scw.RequestOption) (*file.FileSystem, error) {
	return sdk.filesystem, sdk.filesystemErr
}

func (sdk *fakeFileSDK) ListAttachments(request *file.ListAttachmentsRequest, _ ...scw.RequestOption) (*file.ListAttachmentsResponse, error) {
	copyRequest := *request
	sdk.attachmentCalls = append(sdk.attachmentCalls, &copyRequest)
	if sdk.attachmentErr != nil {
		return nil, sdk.attachmentErr
	}
	return sdk.attachments[*request.Page], nil
}

type fakeInstanceSDK struct {
	server        *instance.GetServerResponse
	serverErr     error
	serverTypes   *instance.ListServersTypesResponse
	serverTypeErr error
	getRequests   []*instance.GetServerRequest
	attachRequest *instance.AttachServerFileSystemRequest
	detachRequest *instance.DetachServerFileSystemRequest
	mutationErr   error
}

func (sdk *fakeInstanceSDK) GetServer(request *instance.GetServerRequest, _ ...scw.RequestOption) (*instance.GetServerResponse, error) {
	copyRequest := *request
	sdk.getRequests = append(sdk.getRequests, &copyRequest)
	return sdk.server, sdk.serverErr
}

func (sdk *fakeInstanceSDK) ListServersTypes(_ *instance.ListServersTypesRequest, _ ...scw.RequestOption) (*instance.ListServersTypesResponse, error) {
	return sdk.serverTypes, sdk.serverTypeErr
}

func (sdk *fakeInstanceSDK) AttachServerFileSystem(request *instance.AttachServerFileSystemRequest, _ ...scw.RequestOption) (*instance.AttachServerFileSystemResponse, error) {
	copyRequest := *request
	sdk.attachRequest = &copyRequest
	return &instance.AttachServerFileSystemResponse{}, sdk.mutationErr
}

func (sdk *fakeInstanceSDK) DetachServerFileSystem(request *instance.DetachServerFileSystemRequest, _ ...scw.RequestOption) (*instance.DetachServerFileSystemResponse, error) {
	copyRequest := *request
	sdk.detachRequest = &copyRequest
	return &instance.DetachServerFileSystemResponse{}, sdk.mutationErr
}

func newFakeSDK(t *testing.T, fileAPI *fakeFileSDK, instanceAPI *fakeInstanceSDK) *SDKAPI {
	t.Helper()
	api, err := newSDKAPI(fileAPI, instanceAPI)
	if err != nil {
		t.Fatalf("newSDKAPI() error = %v", err)
	}
	return api
}

func TestSDKAPIListsCrossZoneAttachmentPagesWithoutZoneFilter(t *testing.T) {
	zoneOne, zoneTwo := scw.Zone("fr-par-1"), scw.Zone("fr-par-2")
	firstPage := make([]*file.Attachment, 0, 100)
	for index := 0; index < 100; index++ {
		firstPage = append(firstPage, &file.Attachment{
			ID: "attachment-a", FilesystemID: "11111111-1111-4111-8111-111111111111",
			ResourceID: "22222222-2222-4222-8222-222222222222", ResourceType: file.AttachmentResourceTypeInstanceServer, Zone: &zoneOne,
		})
	}
	fileAPI := &fakeFileSDK{attachments: map[int32]*file.ListAttachmentsResponse{
		1: {Attachments: firstPage, TotalCount: 101},
		2: {Attachments: []*file.Attachment{{
			ID: "attachment-b", FilesystemID: "11111111-1111-4111-8111-111111111111",
			ResourceID: "33333333-3333-4333-8333-333333333333", ResourceType: file.AttachmentResourceTypeInstanceServer, Zone: &zoneTwo,
		}}, TotalCount: 101},
	}}
	api := newFakeSDK(t, fileAPI, &fakeInstanceSDK{})
	first, err := api.ListAttachments(context.Background(), ListAttachmentsRequest{
		Region: "fr-par", FilesystemID: "11111111-1111-4111-8111-111111111111", PageSize: 100,
	})
	if err != nil || first.NextPageToken != "2" || len(first.Attachments) != 100 {
		t.Fatalf("ListAttachments(first) = %#v, %v", first, err)
	}
	second, err := api.ListAttachments(context.Background(), ListAttachmentsRequest{
		Region: "fr-par", FilesystemID: "11111111-1111-4111-8111-111111111111", PageSize: 100, PageToken: first.NextPageToken,
	})
	if err != nil || second.NextPageToken != "" || len(second.Attachments) != 1 || second.Attachments[0].Zone != "fr-par-2" {
		t.Fatalf("ListAttachments(second) = %#v, %v", second, err)
	}
	for _, request := range fileAPI.attachmentCalls {
		if request.Zone != nil || request.FilesystemID == nil || request.Page == nil || request.PageSize == nil {
			t.Fatalf("SDK ListAttachments request = %#v", request)
		}
	}
}

func TestSDKAPINormalizesFilesystemServerAndLiveCapabilities(t *testing.T) {
	fileAPI := &fakeFileSDK{filesystem: &file.FileSystem{
		ID: "11111111-1111-4111-8111-111111111111", ProjectID: "44444444-4444-4444-8444-444444444444",
		Region: scw.Region("fr-par"), Size: scw.Size(1000), Status: file.FileSystemStatusAvailable, NumberOfAttachments: 1,
	}}
	instanceAPI := &fakeInstanceSDK{
		server: &instance.GetServerResponse{Server: &instance.Server{
			ID: "22222222-2222-4222-8222-222222222222", Project: "44444444-4444-4444-8444-444444444444",
			Zone: scw.Zone("fr-par-2"), CommercialType: "TEST-TYPE-1", State: instance.ServerStateRunning,
			Filesystems: []*instance.ServerFilesystem{{FilesystemID: "11111111-1111-4111-8111-111111111111", State: instance.ServerFilesystemStateAvailable}},
		}},
		serverTypes: &instance.ListServersTypesResponse{Servers: map[string]*instance.ServerType{
			"TEST-TYPE-1": {Capabilities: &instance.ServerTypeCapabilities{MaxFileSystems: 4}},
		}},
	}
	api := newFakeSDK(t, fileAPI, instanceAPI)
	filesystem, err := api.GetFilesystem(context.Background(), "fr-par", "11111111-1111-4111-8111-111111111111")
	if err != nil || filesystem.SizeBytes != 1000 || filesystem.Status != FilesystemAvailable || filesystem.NumberOfAttachments != 1 {
		t.Fatalf("GetFilesystem() = %#v, %v", filesystem, err)
	}
	server, err := api.GetServer(context.Background(), "fr-par-2", "22222222-2222-4222-8222-222222222222")
	if err != nil || server.Region != "fr-par" || server.MaxFileSystems != 4 || server.State != InstanceRunning || len(server.Filesystems) != 1 || server.Filesystems[0].State != ServerFilesystemAvailable {
		t.Fatalf("GetServer() = %#v, %v", server, err)
	}
	if len(instanceAPI.getRequests) != 1 || instanceAPI.getRequests[0].Zone != scw.Zone("fr-par-2") {
		t.Fatalf("GetServer SDK requests = %#v", instanceAPI.getRequests)
	}
}

func TestSDKAPIMutationsUseExactTargetAndReturnMappedErrors(t *testing.T) {
	instanceAPI := &fakeInstanceSDK{}
	api := newFakeSDK(t, &fakeFileSDK{}, instanceAPI)
	serverID, filesystemID := "22222222-2222-4222-8222-222222222222", "11111111-1111-4111-8111-111111111111"
	if err := api.AttachServerFilesystem(context.Background(), "fr-par-2", serverID, filesystemID); err != nil {
		t.Fatalf("AttachServerFilesystem() error = %v", err)
	}
	if instanceAPI.attachRequest == nil || instanceAPI.attachRequest.Zone != scw.Zone("fr-par-2") || instanceAPI.attachRequest.ServerID != serverID || instanceAPI.attachRequest.FilesystemID != filesystemID {
		t.Fatalf("attach request = %#v", instanceAPI.attachRequest)
	}
	if err := api.DetachServerFilesystem(context.Background(), "fr-par-2", serverID, filesystemID); err != nil {
		t.Fatalf("DetachServerFilesystem() error = %v", err)
	}
	if instanceAPI.detachRequest == nil || instanceAPI.detachRequest.Zone != scw.Zone("fr-par-2") {
		t.Fatalf("detach request = %#v", instanceAPI.detachRequest)
	}
	instanceAPI.mutationErr = &scw.PermissionsDeniedError{}
	if err := api.AttachServerFilesystem(context.Background(), "fr-par-2", serverID, filesystemID); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("AttachServerFilesystem(permission) error = %v", err)
	}
	instanceAPI.mutationErr = &scw.QuotasExceededError{}
	if err := api.AttachServerFilesystem(context.Background(), "fr-par-2", serverID, filesystemID); !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("AttachServerFilesystem(quota) error = %v", err)
	}
}

func TestNewSDKAPIValidatesAuthorityWithoutExposingSecrets(t *testing.T) {
	base := SDKOptions{
		Region: "fr-par", ProjectID: "44444444-4444-4444-8444-444444444444", Zone: "fr-par-2",
		AccessKey: "SCW1234567890ABCDEFG", SecretKey: "7363616c-6577-6573-6862-6f7579616161", UserAgent: "sfs-subdir-test/1.0.0",
	}
	if _, err := NewSDKAPI(base); err != nil {
		t.Fatalf("NewSDKAPI() error = %v", err)
	}
	for name, mutate := range map[string]func(*SDKOptions){
		"region":      func(options *SDKOptions) { options.Region = "nl-ams" },
		"credentials": func(options *SDKOptions) { options.SecretKey = "" },
		"user agent":  func(options *SDKOptions) { options.UserAgent = "" },
	} {
		t.Run(name, func(t *testing.T) {
			options := base
			mutate(&options)
			if _, err := NewSDKAPI(options); err == nil {
				t.Fatal("NewSDKAPI() error = nil")
			} else if containsAny(err.Error(), []string{base.AccessKey, base.SecretKey}) {
				t.Fatalf("NewSDKAPI() exposed credential: %v", err)
			}
		})
	}
}

func containsAny(value string, candidates []string) bool {
	for _, candidate := range candidates {
		if candidate != "" && strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}
