package coordination

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

const (
	discoveryAnnotationPrefix = "sfs-subdir-discovery-"
	discoverySchemaVersion    = "1"
	discoveryStateProvisional = "Provisional"
)

var discoveryAnnotationKeys = map[string]struct{}{
	discoveryAnnotationPrefix + "schema-version":     {},
	discoveryAnnotationPrefix + "state":              {},
	discoveryAnnotationPrefix + "observed-at":        {},
	discoveryAnnotationPrefix + "installation-id":    {},
	discoveryAnnotationPrefix + "active-cluster-uid": {},
	discoveryAnnotationPrefix + "holder-pod-uid":     {},
}

// DiscoveryMarker durably prevents a same-Pod reacquisition from promoting a
// missing-Lease discovery holder into mutation leadership. It is cleared only
// by an exact fresh-installation proof or consumed missing-Lease approval CAS.
type DiscoveryMarker struct {
	SchemaVersion    string
	State            string
	ObservedAt       string
	InstallationID   string
	ActiveClusterUID string
	HolderPodUID     string
}

// NewDiscoveryMarker binds the provisional condition to runtime identity and
// the first coherent observation instant stored in the Lease.
func NewDiscoveryMarker(holder HolderEvidence, observedAt time.Time) (DiscoveryMarker, error) {
	marker := DiscoveryMarker{
		SchemaVersion: discoverySchemaVersion, State: discoveryStateProvisional,
		ObservedAt:     observedAt.UTC().Format(time.RFC3339Nano),
		InstallationID: holder.InstallationID, ActiveClusterUID: holder.ActiveClusterUID,
		HolderPodUID: holder.PodUID,
	}
	if err := marker.Validate(holder); err != nil {
		return DiscoveryMarker{}, err
	}
	return marker, nil
}

// Validate checks the complete marker and exact current holder binding.
func (marker DiscoveryMarker) Validate(holder HolderEvidence) error {
	if err := holder.Validate(); err != nil {
		return err
	}
	if marker.SchemaVersion != discoverySchemaVersion || marker.State != discoveryStateProvisional {
		return fmt.Errorf("discovery marker schema %q or state %q is unsupported", marker.SchemaVersion, marker.State)
	}
	parsed, err := time.Parse(time.RFC3339Nano, marker.ObservedAt)
	if err != nil || !strings.HasSuffix(marker.ObservedAt, "Z") || parsed.UTC().Format(time.RFC3339Nano) != marker.ObservedAt {
		return fmt.Errorf("discovery observation timestamp must be canonical RFC 3339 UTC")
	}
	if marker.InstallationID != holder.InstallationID || marker.ActiveClusterUID != holder.ActiveClusterUID || marker.HolderPodUID != holder.PodUID {
		return fmt.Errorf("discovery marker differs from current holder identity")
	}
	return nil
}

// ObservationTime returns the authenticated provisional condition instant.
func (marker DiscoveryMarker) ObservationTime() (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, marker.ObservedAt)
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func (marker DiscoveryMarker) annotations() map[string]string {
	return map[string]string{
		discoveryAnnotationPrefix + "schema-version":     marker.SchemaVersion,
		discoveryAnnotationPrefix + "state":              marker.State,
		discoveryAnnotationPrefix + "observed-at":        marker.ObservedAt,
		discoveryAnnotationPrefix + "installation-id":    marker.InstallationID,
		discoveryAnnotationPrefix + "active-cluster-uid": marker.ActiveClusterUID,
		discoveryAnnotationPrefix + "holder-pod-uid":     marker.HolderPodUID,
	}
}

// ApplyDiscoveryMarker installs one marker or accepts only its exact replay.
func ApplyDiscoveryMarker(annotations map[string]string, marker DiscoveryMarker, holder HolderEvidence) (map[string]string, error) {
	if err := marker.Validate(holder); err != nil {
		return nil, err
	}
	existing, present, err := ParseDiscoveryMarker(annotations, holder)
	if err != nil {
		return nil, err
	}
	if present && existing != marker {
		return nil, fmt.Errorf("another provisional discovery marker is already active")
	}
	result := maps.Clone(annotations)
	if result == nil {
		result = map[string]string{}
	}
	for key, value := range marker.annotations() {
		result[key] = value
	}
	return result, nil
}

// ParseDiscoveryMarker rejects partial and unknown schema extensions.
func ParseDiscoveryMarker(annotations map[string]string, holder HolderEvidence) (DiscoveryMarker, bool, error) {
	found := 0
	for key := range annotations {
		if !strings.HasPrefix(key, discoveryAnnotationPrefix) {
			continue
		}
		if _, known := discoveryAnnotationKeys[key]; !known {
			return DiscoveryMarker{}, true, fmt.Errorf("unknown discovery annotation %q", key)
		}
		found++
	}
	if found == 0 {
		return DiscoveryMarker{}, false, nil
	}
	if found != len(discoveryAnnotationKeys) {
		return DiscoveryMarker{}, true, fmt.Errorf("discovery marker has %d annotations, want %d", found, len(discoveryAnnotationKeys))
	}
	marker := DiscoveryMarker{
		SchemaVersion:    annotations[discoveryAnnotationPrefix+"schema-version"],
		State:            annotations[discoveryAnnotationPrefix+"state"],
		ObservedAt:       annotations[discoveryAnnotationPrefix+"observed-at"],
		InstallationID:   annotations[discoveryAnnotationPrefix+"installation-id"],
		ActiveClusterUID: annotations[discoveryAnnotationPrefix+"active-cluster-uid"],
		HolderPodUID:     annotations[discoveryAnnotationPrefix+"holder-pod-uid"],
	}
	if err := marker.Validate(holder); err != nil {
		return DiscoveryMarker{}, true, err
	}
	return marker, true, nil
}

// ClearDiscoveryMarker removes only the fixed marker keys.
func ClearDiscoveryMarker(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	for key := range discoveryAnnotationKeys {
		delete(result, key)
	}
	return result
}
