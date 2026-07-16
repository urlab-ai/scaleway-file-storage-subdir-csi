package observability

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type metricKind string

const (
	metricGauge     metricKind = "gauge"
	metricCounter   metricKind = "counter"
	metricHistogram metricKind = "histogram"
)

type descriptor struct {
	name       string
	help       string
	kind       metricKind
	labelNames []string
	buckets    []float64
}

type metricFamily struct {
	descriptor descriptor
	series     map[string]*metricSeries
}

type metricSeries struct {
	labels          []string
	gauge           float64
	counter         uint64
	histogramCounts []uint64
	histogramCount  uint64
	histogramSum    float64
}

// registry is deliberately private so callers can update metrics only through
// the typed, label-bounded controller and node surfaces.
type registry struct {
	mu       sync.Mutex
	order    []string
	families map[string]*metricFamily
}

type gaugeUpdate struct {
	name   string
	labels []string
	value  float64
}

func newRegistry(descriptors []descriptor) (*registry, error) {
	if len(descriptors) == 0 {
		return nil, fmt.Errorf("at least one metric descriptor is required")
	}
	result := &registry{
		order:    make([]string, 0, len(descriptors)),
		families: make(map[string]*metricFamily, len(descriptors)),
	}
	for _, candidate := range descriptors {
		if candidate.name == "" || candidate.help == "" {
			return nil, fmt.Errorf("metric descriptor name and help must be non-empty")
		}
		if candidate.kind != metricGauge && candidate.kind != metricCounter && candidate.kind != metricHistogram {
			return nil, fmt.Errorf("metric %q has unsupported kind %q", candidate.name, candidate.kind)
		}
		if _, duplicate := result.families[candidate.name]; duplicate {
			return nil, fmt.Errorf("metric descriptor %q is duplicated", candidate.name)
		}
		if err := validateDescriptorLabels(candidate.labelNames); err != nil {
			return nil, fmt.Errorf("metric %q labels: %w", candidate.name, err)
		}
		candidate.labelNames = slices.Clone(candidate.labelNames)
		candidate.buckets = slices.Clone(candidate.buckets)
		if candidate.kind == metricHistogram {
			if err := validateBuckets(candidate.buckets); err != nil {
				return nil, fmt.Errorf("metric %q buckets: %w", candidate.name, err)
			}
		} else if len(candidate.buckets) != 0 {
			return nil, fmt.Errorf("non-histogram metric %q defines buckets", candidate.name)
		}
		result.order = append(result.order, candidate.name)
		result.families[candidate.name] = &metricFamily{
			descriptor: candidate,
			series:     make(map[string]*metricSeries),
		}
	}
	return result, nil
}

func validateDescriptorLabels(labels []string) error {
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label == "" {
			return fmt.Errorf("label name is empty")
		}
		if _, duplicate := seen[label]; duplicate {
			return fmt.Errorf("label name %q is duplicated", label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func validateBuckets(buckets []float64) error {
	if len(buckets) == 0 {
		return fmt.Errorf("histogram requires at least one finite bucket")
	}
	for index, bucket := range buckets {
		if math.IsNaN(bucket) || math.IsInf(bucket, 0) || bucket <= 0 {
			return fmt.Errorf("bucket %d is not finite and positive", index)
		}
		if index > 0 && bucket <= buckets[index-1] {
			return fmt.Errorf("buckets are not strictly increasing")
		}
	}
	return nil
}

func (registry *registry) setGauge(name string, labels []string, value float64) error {
	return registry.setGauges([]gaugeUpdate{{name: name, labels: labels, value: value}})
}

func (registry *registry) setGauges(updates []gaugeUpdate) error {
	for _, update := range updates {
		if math.IsNaN(update.value) || math.IsInf(update.value, 0) || update.value < 0 {
			return fmt.Errorf("gauge %q value must be finite and non-negative", update.name)
		}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	type pendingUpdate struct {
		family *metricFamily
		series *metricSeries
		key    string
		value  float64
	}
	pending := make([]pendingUpdate, 0, len(updates))
	for _, update := range updates {
		family, series, err := registry.seriesLocked(update.name, metricGauge, update.labels)
		if err != nil {
			return err
		}
		pending = append(pending, pendingUpdate{
			family: family,
			series: series,
			key:    seriesKey(update.labels),
			value:  update.value,
		})
	}
	for _, update := range pending {
		update.series.gauge = update.value
		update.family.series[update.key] = update.series
	}
	return nil
}

func (registry *registry) addCounter(name string, labels []string, delta uint64) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	family, series, err := registry.seriesLocked(name, metricCounter, labels)
	if err != nil {
		return err
	}
	if ^uint64(0)-series.counter < delta {
		return fmt.Errorf("counter %q overflow", name)
	}
	series.counter += delta
	family.series[seriesKey(labels)] = series
	return nil
}

func (registry *registry) observeAndCount(histogramName, counterName string, labels []string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return fmt.Errorf("histogram %q observation must be finite and non-negative", histogramName)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	histogramFamily, histogramSeries, err := registry.seriesLocked(histogramName, metricHistogram, labels)
	if err != nil {
		return err
	}
	counterFamily, counterSeries, err := registry.seriesLocked(counterName, metricCounter, labels)
	if err != nil {
		return err
	}
	if histogramSeries.histogramCount == ^uint64(0) || counterSeries.counter == ^uint64(0) {
		return fmt.Errorf("CSI metric count overflow")
	}
	for index, bucket := range histogramFamily.descriptor.buckets {
		if value <= bucket && histogramSeries.histogramCounts[index] == ^uint64(0) {
			return fmt.Errorf("histogram %q bucket count overflow", histogramName)
		}
	}
	if math.IsInf(histogramSeries.histogramSum+value, 0) {
		return fmt.Errorf("histogram %q sum overflow", histogramName)
	}
	histogramSeries.histogramCount++
	histogramSeries.histogramSum += value
	for index, bucket := range histogramFamily.descriptor.buckets {
		if value <= bucket {
			histogramSeries.histogramCounts[index]++
		}
	}
	counterSeries.counter++
	histogramFamily.series[seriesKey(labels)] = histogramSeries
	counterFamily.series[seriesKey(labels)] = counterSeries
	return nil
}

func (registry *registry) seriesLocked(name string, want metricKind, labels []string) (*metricFamily, *metricSeries, error) {
	family, exists := registry.families[name]
	if !exists {
		return nil, nil, fmt.Errorf("metric %q is not registered", name)
	}
	if family.descriptor.kind != want {
		return nil, nil, fmt.Errorf("metric %q is %s, not %s", name, family.descriptor.kind, want)
	}
	if len(labels) != len(family.descriptor.labelNames) {
		return nil, nil, fmt.Errorf("metric %q has %d labels, want %d", name, len(labels), len(family.descriptor.labelNames))
	}
	key := seriesKey(labels)
	if current, present := family.series[key]; present {
		return family, current, nil
	}
	series := &metricSeries{labels: slices.Clone(labels)}
	if want == metricHistogram {
		series.histogramCounts = make([]uint64, len(family.descriptor.buckets))
	}
	return family, series, nil
}

func seriesKey(labels []string) string {
	var builder strings.Builder
	for _, value := range labels {
		builder.WriteString(strconv.Itoa(len(value)))
		builder.WriteByte(':')
		builder.WriteString(value)
	}
	return builder.String()
}

func (registry *registry) writePrometheus(writer io.Writer) error {
	var snapshot strings.Builder
	registry.mu.Lock()
	err := registry.writePrometheusLocked(&snapshot)
	registry.mu.Unlock()
	if err != nil {
		return err
	}
	_, err = io.WriteString(writer, snapshot.String())
	return err
}

// writePrometheusLocked renders into an in-memory bounded snapshot. The caller
// releases the registry lock before potentially blocking on a scrape client.
func (registry *registry) writePrometheusLocked(writer io.Writer) error {
	for _, name := range registry.order {
		family := registry.families[name]
		if _, err := fmt.Fprintf(writer, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(family.descriptor.help), name, family.descriptor.kind); err != nil {
			return err
		}
		keys := make([]string, 0, len(family.series))
		for key := range family.series {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			series := family.series[key]
			switch family.descriptor.kind {
			case metricGauge:
				if err := writeSample(writer, name, family.descriptor.labelNames, series.labels, "", formatFloat(series.gauge)); err != nil {
					return err
				}
			case metricCounter:
				if err := writeSample(writer, name, family.descriptor.labelNames, series.labels, "", strconv.FormatUint(series.counter, 10)); err != nil {
					return err
				}
			case metricHistogram:
				for index, bucket := range family.descriptor.buckets {
					if err := writeSample(writer, name+"_bucket", family.descriptor.labelNames, series.labels, formatFloat(bucket), strconv.FormatUint(series.histogramCounts[index], 10)); err != nil {
						return err
					}
				}
				if err := writeSample(writer, name+"_bucket", family.descriptor.labelNames, series.labels, "+Inf", strconv.FormatUint(series.histogramCount, 10)); err != nil {
					return err
				}
				if err := writeSample(writer, name+"_sum", family.descriptor.labelNames, series.labels, "", formatFloat(series.histogramSum)); err != nil {
					return err
				}
				if err := writeSample(writer, name+"_count", family.descriptor.labelNames, series.labels, "", strconv.FormatUint(series.histogramCount, 10)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeSample(writer io.Writer, name string, labelNames, labelValues []string, bucket, value string) error {
	if len(labelNames) != 0 || bucket != "" {
		if _, err := io.WriteString(writer, name+"{"); err != nil {
			return err
		}
		for index, labelName := range labelNames {
			if index != 0 {
				if _, err := io.WriteString(writer, ","); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(writer, "%s=\"%s\"", labelName, escapeLabel(labelValues[index])); err != nil {
				return err
			}
		}
		if bucket != "" {
			if len(labelNames) != 0 {
				if _, err := io.WriteString(writer, ","); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(writer, "le=\"%s\"", bucket); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(writer, "}"); err != nil {
			return err
		}
	} else if _, err := io.WriteString(writer, name); err != nil {
		return err
	}
	_, err := fmt.Fprintf(writer, " %s\n", value)
	return err
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func escapeHelp(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n").Replace(value)
}

func escapeLabel(value string) string {
	return strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"").Replace(value)
}

func serveMetrics(registry *registry, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method == http.MethodHead {
		writer.WriteHeader(http.StatusOK)
		return
	}
	if err := registry.writePrometheus(writer); err != nil {
		// A write error means the scrape client disconnected. There is no safe
		// second response to send after exposition bytes may have been written.
		return
	}
}
