package observability

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestNodeMetrics(t *testing.T) *NodeMetrics {
	t.Helper()
	metrics, err := NewNodeMetrics([]string{"standard"})
	if err != nil {
		t.Fatalf("NewNodeMetrics() error = %v", err)
	}
	return metrics
}

func TestNodeMetricsExposeOnlyBoundedLabels(t *testing.T) {
	metrics := newTestNodeMetrics(t)
	if err := metrics.SetParentMounts("standard", 2); err != nil {
		t.Fatalf("SetParentMounts() error = %v", err)
	}
	if err := metrics.AddNodeStageVolume(3); err != nil {
		t.Fatalf("AddNodeStageVolume() error = %v", err)
	}
	if err := metrics.AddNodePublishVolume(4); err != nil {
		t.Fatalf("AddNodePublishVolume() error = %v", err)
	}
	if err := metrics.AddMountError(1); err != nil {
		t.Fatalf("AddMountError() error = %v", err)
	}
	if err := metrics.ObserveCSI(CSINodeStageVolume, CodeUnavailable, 2*time.Second); err != nil {
		t.Fatalf("ObserveCSI() error = %v", err)
	}
	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
	got := output.String()
	for _, want := range []string{
		`sfs_subdir_node_parent_mounts{pool="standard"} 2`,
		`sfs_subdir_node_stage_volume_total 3`,
		`sfs_subdir_node_publish_volume_total 4`,
		`sfs_subdir_mount_errors_total 1`,
		`sfs_subdir_csi_operations_total{operation="NodeStageVolume",code="Unavailable"} 1`,
	} {
		if !strings.Contains(got, want+"\n") {
			t.Errorf("node metrics missing %q\n%s", want, got)
		}
	}
	for _, forbiddenLabel := range []string{"node=", "logicalVolumeID=", "path="} {
		if strings.Contains(got, forbiddenLabel) {
			t.Errorf("node metrics contain forbidden label %q", forbiddenLabel)
		}
	}
	if err := metrics.SetParentMounts("tenant-input", 1); err == nil {
		t.Fatal("SetParentMounts(unconfigured pool) error = nil")
	}
}

func TestMetricsHTTPContract(t *testing.T) {
	metrics := newTestNodeMetrics(t)
	get := httptest.NewRecorder()
	metrics.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if get.Code != http.StatusOK || !strings.HasPrefix(get.Header().Get("Content-Type"), "text/plain; version=0.0.4") || get.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET status/headers = %d/%q/%q", get.Code, get.Header().Get("Content-Type"), get.Header().Get("Cache-Control"))
	}
	if !strings.Contains(get.Body.String(), "# TYPE sfs_subdir_node_parent_mounts gauge") {
		t.Fatalf("GET body missing metrics: %s", get.Body.String())
	}

	head := httptest.NewRecorder()
	metrics.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if head.Code != http.StatusOK || head.Body.Len() != 0 {
		t.Fatalf("HEAD status/body = %d/%q", head.Code, head.Body.String())
	}

	post := httptest.NewRecorder()
	metrics.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if post.Code != http.StatusMethodNotAllowed || post.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST status/Allow = %d/%q", post.Code, post.Header().Get("Allow"))
	}
}

func TestNodeMetricsConstructorAndWriterValidation(t *testing.T) {
	for name, pools := range map[string][]string{
		"empty":     nil,
		"duplicate": {"standard", "standard"},
		"invalid":   {"Bad_Pool"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewNodeMetrics(pools); err == nil {
				t.Fatal("NewNodeMetrics() error = nil")
			}
		})
	}
	metrics := newTestNodeMetrics(t)
	if err := metrics.WritePrometheus(nil); err == nil {
		t.Fatal("WritePrometheus(nil) error = nil")
	}
}
