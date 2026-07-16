package observability

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistryBatchAndCSIUpdatesAreAtomic(t *testing.T) {
	registry, err := newRegistry([]descriptor{
		{name: "good", help: "good gauge", kind: metricGauge},
		{name: "calls", help: "calls", kind: metricCounter, labelNames: []string{"operation", "code"}},
		{name: "duration", help: "duration", kind: metricHistogram, labelNames: []string{"operation", "code"}, buckets: []float64{1}},
	})
	if err != nil {
		t.Fatalf("newRegistry() error = %v", err)
	}
	if err := registry.setGauge("good", nil, 1); err != nil {
		t.Fatalf("setGauge() error = %v", err)
	}
	if err := registry.setGauges([]gaugeUpdate{
		{name: "good", value: 2},
		{name: "missing", value: 3},
	}); err == nil {
		t.Fatal("setGauges(partial invalid batch) error = nil")
	}

	labels := []string{"Probe", "OK"}
	registry.mu.Lock()
	callFamily, callSeries, seriesErr := registry.seriesLocked("calls", metricCounter, labels)
	if seriesErr != nil {
		registry.mu.Unlock()
		t.Fatalf("seriesLocked() error = %v", seriesErr)
	}
	callSeries.counter = ^uint64(0)
	callFamily.series[seriesKey(labels)] = callSeries
	registry.mu.Unlock()
	if err := registry.observeAndCount("duration", "calls", labels, 0.5); err == nil {
		t.Fatal("observeAndCount(counter overflow) error = nil")
	}

	var output bytes.Buffer
	if err := registry.writePrometheus(&output); err != nil {
		t.Fatalf("writePrometheus() error = %v", err)
	}
	got := output.String()
	if !strings.Contains(got, "good 1\n") {
		t.Fatalf("failed gauge batch changed prior value\n%s", got)
	}
	if strings.Contains(got, "duration_count") {
		t.Fatalf("failed CSI batch changed histogram\n%s", got)
	}
}

func TestSlowScrapeWriterDoesNotHoldRegistryLock(t *testing.T) {
	metrics := newTestControllerMetrics(t)
	writer := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	writeDone := make(chan error, 1)
	go func() { writeDone <- metrics.WritePrometheus(writer) }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("metrics writer did not start")
	}
	updateDone := make(chan error, 1)
	go func() { updateDone <- metrics.SetReady(true) }()
	select {
	case err := <-updateDone:
		if err != nil {
			t.Fatalf("SetReady() error = %v", err)
		}
	case <-time.After(time.Second):
		close(writer.release)
		t.Fatal("slow scrape held the registry lock")
	}
	close(writer.release)
	if err := <-writeDone; err != nil {
		t.Fatalf("WritePrometheus() error = %v", err)
	}
}

func TestRegistryValidatesDescriptorsAndPropagatesWriterError(t *testing.T) {
	tests := map[string][]descriptor{
		"empty": nil,
		"duplicate": {
			{name: "same", help: "one", kind: metricGauge},
			{name: "same", help: "two", kind: metricGauge},
		},
		"bad labels":  {{name: "bad", help: "bad", kind: metricGauge, labelNames: []string{"x", "x"}}},
		"bad buckets": {{name: "bad", help: "bad", kind: metricHistogram, buckets: []float64{1, 1}}},
	}
	for name, descriptors := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := newRegistry(descriptors); err == nil {
				t.Fatal("newRegistry() error = nil")
			}
		})
	}
	registry, err := newRegistry([]descriptor{{name: "good", help: "good", kind: metricGauge}})
	if err != nil {
		t.Fatalf("newRegistry() error = %v", err)
	}
	if err := registry.writePrometheus(errorWriter{}); !errors.Is(err, errWrite) {
		t.Fatalf("writePrometheus() error = %v, want %v", err, errWrite)
	}
}

var errWrite = errors.New("write failed")

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errWrite }

type blockingWriter struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (writer *blockingWriter) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.started) })
	<-writer.release
	return len(data), nil
}
