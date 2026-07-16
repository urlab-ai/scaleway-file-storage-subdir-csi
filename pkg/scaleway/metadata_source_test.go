package scaleway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const metadataJSON = `{"id":"11111111-1111-4111-8111-111111111111","commercial_type":"TEST-TYPE-1","location":{"zone_id":"fr-par-2"},"future":{"accepted":true}}`

func TestLocalMetadataSourcePrefersExactRegularCloudInitFile(t *testing.T) {
	root := t.TempDir()
	cloudPath := filepath.Join(root, "instance-data.json")
	cloud := `{"ds":{"meta_data":` + metadataJSON + `},"future":"accepted"}`
	if err := os.WriteFile(cloudPath, []byte(cloud), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	var apiCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		apiCalls.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	source := &LocalMetadataSource{cloudInitPath: cloudPath, apiURL: server.URL, client: server.Client(), timeout: time.Second}
	identity, err := source.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if identity.InstanceID != "11111111-1111-4111-8111-111111111111" || identity.Zone != "fr-par-2" || identity.Region != "fr-par" || identity.CommercialType != "TEST-TYPE-1" {
		t.Fatalf("Load() = %#v", identity)
	}
	if apiCalls.Load() != 0 {
		t.Fatalf("metadata API calls = %d, want 0", apiCalls.Load())
	}
}

func TestLocalMetadataSourceFallsBackToBoundedAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Errorf("metadata method = %s", request.Method)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(metadataJSON))
	}))
	defer server.Close()
	source := &LocalMetadataSource{cloudInitPath: filepath.Join(t.TempDir(), "missing"), apiURL: server.URL, client: server.Client(), timeout: time.Second}
	identity, err := source.Load(context.Background())
	if err != nil || identity.Zone != "fr-par-2" || identity.Region != "fr-par" {
		t.Fatalf("Load(API) = %#v, %v", identity, err)
	}
}

func TestLocalMetadataSourceRejectsSymlinkDuplicateAndOversize(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.json")
	if err := os.WriteFile(target, []byte(`{"ds":{"meta_data":`+metadataJSON+`}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	link := filepath.Join(root, "link.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	source := &LocalMetadataSource{cloudInitPath: link}
	if _, err := source.loadCloudInit(context.Background()); err == nil {
		t.Fatal("loadCloudInit(symlink) error = nil")
	}

	for name, body := range map[string]string{
		"duplicate": `{"id":"11111111-1111-4111-8111-111111111111","id":"22222222-2222-4222-8222-222222222222","commercial_type":"TEST-TYPE-1","location":{"zone_id":"fr-par-2"}}`,
		"oversize":  strings.Repeat("x", maxMetadataBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(body))
			}))
			defer server.Close()
			source := &LocalMetadataSource{cloudInitPath: filepath.Join(root, "missing"), apiURL: server.URL, client: server.Client(), timeout: time.Second}
			if _, err := source.Load(context.Background()); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLocalMetadataSourceHonorsCancellationWithoutFallbackSuccess(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(started)
		<-request.Context().Done()
	}))
	defer server.Close()
	source := &LocalMetadataSource{cloudInitPath: filepath.Join(t.TempDir(), "missing"), apiURL: server.URL, client: server.Client(), timeout: metadataRequestTimeout}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := source.Load(ctx)
		result <- err
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Load(cancelled) error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Load() did not honor cancellation")
	}
}
