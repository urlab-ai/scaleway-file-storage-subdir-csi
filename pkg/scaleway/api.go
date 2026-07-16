package scaleway

import "context"

// Filesystem is authoritative regional parent metadata.
type Filesystem struct {
	ID                  string
	ProjectID           string
	Region              string
	SizeBytes           uint64
	Status              FilesystemStatus
	NumberOfAttachments uint32
}

// AttachmentResourceType is normalized by the SDK adapter.
type AttachmentResourceType string

const (
	AttachmentResourceServer  AttachmentResourceType = "server"
	AttachmentResourceUnknown AttachmentResourceType = "unknown"
)

// Attachment is one regional File Storage attachment resource.
type Attachment struct {
	ID           string
	FilesystemID string
	ResourceID   string
	ResourceType AttachmentResourceType
	Zone         string
}

// ListAttachmentsRequest deliberately carries an optional zone pointer. The
// inventory implementation always leaves it nil so the pinned SDK cannot
// inherit SCW_DEFAULT_ZONE and silently hide cross-zone attachments.
type ListAttachmentsRequest struct {
	Region       string
	FilesystemID string
	PageToken    string
	PageSize     uint32
	Zone         *string
}

// AttachmentPage is one provider page and its opaque continuation token.
type AttachmentPage struct {
	Attachments   []Attachment
	NextPageToken string
}

// ServerFilesystem is one normalized entry from Server.Filesystems.
type ServerFilesystem struct {
	FilesystemID string
	State        ServerFilesystemState
}

// Server is the target zonal Instance and its complete attachment inventory.
type Server struct {
	ID             string
	ProjectID      string
	Zone           string
	Region         string
	CommercialType string
	State          InstanceState
	MaxFileSystems uint32
	Filesystems    []ServerFilesystem
}

// API is the narrow authenticated provider contract used by the controller.
type API interface {
	GetFilesystem(ctx context.Context, region, filesystemID string) (Filesystem, error)
	ListAttachments(ctx context.Context, request ListAttachmentsRequest) (AttachmentPage, error)
	GetServer(ctx context.Context, zone, serverID string) (Server, error)
	AttachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error
	DetachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error
}
