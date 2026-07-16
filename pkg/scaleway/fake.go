package scaleway

import (
	"context"
	"fmt"
	"maps"
	"sync"
)

// FakeAPI is a deterministic authenticated provider boundary for state-machine
// tests. Responses and faults are explicit; it never fabricates absence.
type FakeAPI struct {
	mu sync.Mutex

	Filesystems         map[string]Filesystem
	Servers             map[string]Server
	Pages               map[string]AttachmentPage
	FilesystemSequences map[string][]Filesystem
	ServerSequences     map[string][]Server
	PageSequences       map[string][]AttachmentPage

	ListRequests []ListAttachmentsRequest
	AttachCalls  []FakeAttachmentCall
	DetachCalls  []FakeAttachmentCall
	Faults       map[string][]error
}

// FakeAttachmentCall records one exact zonal mutation request.
type FakeAttachmentCall struct {
	Zone         string
	ServerID     string
	FilesystemID string
}

// NewFakeAPI returns an empty fake with isolated maps.
func NewFakeAPI() *FakeAPI {
	return &FakeAPI{
		Filesystems:         make(map[string]Filesystem),
		Servers:             make(map[string]Server),
		Pages:               make(map[string]AttachmentPage),
		FilesystemSequences: make(map[string][]Filesystem),
		ServerSequences:     make(map[string][]Server),
		PageSequences:       make(map[string][]AttachmentPage),
		Faults:              make(map[string][]error),
	}
}

// InjectFault appends a fault for get-filesystem, list-attachments, get-server,
// attach, or detach.
func (api *FakeAPI) InjectFault(operation string, err error) {
	api.mu.Lock()
	defer api.mu.Unlock()
	api.Faults[operation] = append(api.Faults[operation], err)
}

func (api *FakeAPI) GetFilesystem(ctx context.Context, region, filesystemID string) (Filesystem, error) {
	if err := ctx.Err(); err != nil {
		return Filesystem{}, err
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if err := api.takeFault("get-filesystem"); err != nil {
		return Filesystem{}, err
	}
	key := region + "/" + filesystemID
	if sequence := api.FilesystemSequences[key]; len(sequence) > 0 {
		filesystem := sequence[0]
		if len(sequence) > 1 {
			api.FilesystemSequences[key] = sequence[1:]
		}
		return filesystem, nil
	}
	filesystem, exists := api.Filesystems[key]
	if !exists {
		return Filesystem{}, ErrNotFound
	}
	return filesystem, nil
}

func (api *FakeAPI) ListAttachments(ctx context.Context, request ListAttachmentsRequest) (AttachmentPage, error) {
	if err := ctx.Err(); err != nil {
		return AttachmentPage{}, err
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	api.ListRequests = append(api.ListRequests, request)
	if err := api.takeFault("list-attachments"); err != nil {
		return AttachmentPage{}, err
	}
	key := request.FilesystemID + "/" + request.PageToken
	if sequence := api.PageSequences[key]; len(sequence) > 0 {
		page := sequence[0]
		if len(sequence) > 1 {
			api.PageSequences[key] = sequence[1:]
		}
		page.Attachments = append([]Attachment(nil), page.Attachments...)
		return page, nil
	}
	page, exists := api.Pages[key]
	if !exists {
		return AttachmentPage{}, fmt.Errorf("missing fake attachment page: %w", ErrUnavailable)
	}
	page.Attachments = append([]Attachment(nil), page.Attachments...)
	return page, nil
}

func (api *FakeAPI) GetServer(ctx context.Context, zone, serverID string) (Server, error) {
	if err := ctx.Err(); err != nil {
		return Server{}, err
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if err := api.takeFault("get-server"); err != nil {
		return Server{}, err
	}
	key := zone + "/" + serverID
	if sequence := api.ServerSequences[key]; len(sequence) > 0 {
		server := sequence[0]
		if len(sequence) > 1 {
			api.ServerSequences[key] = sequence[1:]
		}
		server.Filesystems = append([]ServerFilesystem(nil), server.Filesystems...)
		return server, nil
	}
	server, exists := api.Servers[key]
	if !exists {
		return Server{}, ErrNotFound
	}
	server.Filesystems = append([]ServerFilesystem(nil), server.Filesystems...)
	return server, nil
}

func (api *FakeAPI) AttachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	api.AttachCalls = append(api.AttachCalls, FakeAttachmentCall{Zone: zone, ServerID: serverID, FilesystemID: filesystemID})
	return api.takeFault("attach")
}

func (api *FakeAPI) DetachServerFilesystem(ctx context.Context, zone, serverID, filesystemID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	api.DetachCalls = append(api.DetachCalls, FakeAttachmentCall{Zone: zone, ServerID: serverID, FilesystemID: filesystemID})
	return api.takeFault("detach")
}

// SnapshotRequests returns isolated provider call evidence.
func (api *FakeAPI) SnapshotRequests() (lists []ListAttachmentsRequest, attaches, detaches []FakeAttachmentCall) {
	api.mu.Lock()
	defer api.mu.Unlock()
	return append([]ListAttachmentsRequest(nil), api.ListRequests...), append([]FakeAttachmentCall(nil), api.AttachCalls...), append([]FakeAttachmentCall(nil), api.DetachCalls...)
}

// SnapshotFilesystems returns an isolated metadata map for assertions.
func (api *FakeAPI) SnapshotFilesystems() map[string]Filesystem {
	api.mu.Lock()
	defer api.mu.Unlock()
	return maps.Clone(api.Filesystems)
}

func (api *FakeAPI) takeFault(operation string) error {
	faults := api.Faults[operation]
	if len(faults) == 0 {
		return nil
	}
	err := faults[0]
	api.Faults[operation] = faults[1:]
	return err
}
