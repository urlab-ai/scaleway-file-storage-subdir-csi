package coordination

import (
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	holderSchemaVersionAnnotation = "coordinationSchemaVersion"
	holderPodUIDAnnotation        = "holderPodUID"
	holderNodeNameAnnotation      = "holderNodeName"
	holderCSINodeIDAnnotation     = "holderCSINodeID"
	holderInstanceIDAnnotation    = "holderInstanceID"
	holderZoneAnnotation          = "holderZone"
	holderInstallationAnnotation  = "holderInstallationID"
	holderClusterAnnotation       = "holderActiveClusterUID"

	gracefulSchemaAnnotation       = "gracefulReleaseSchemaVersion"
	gracefulStateAnnotation        = "gracefulReleaseState"
	gracefulHolderAnnotation       = "gracefulReleaseHolderPodUID"
	gracefulRequestAnnotation      = "gracefulReleaseRequestID"
	gracefulReleasedAtAnnotation   = "gracefulReleasedAt"
	gracefulLeaseUIDAnnotation     = "gracefulReleaseLeaseUID"
	gracefulInstallationAnnotation = "gracefulReleaseInstallationID"
	gracefulClusterAnnotation      = "gracefulReleaseActiveClusterUID"
)

var (
	kubernetesNodeNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
	holderAnnotationKeys      = map[string]struct{}{
		holderSchemaVersionAnnotation: {}, holderPodUIDAnnotation: {},
		holderNodeNameAnnotation: {}, holderCSINodeIDAnnotation: {},
		holderInstanceIDAnnotation: {}, holderZoneAnnotation: {},
		holderInstallationAnnotation: {}, holderClusterAnnotation: {},
	}
	gracefulAnnotationKeys = map[string]struct{}{
		gracefulSchemaAnnotation: {}, gracefulStateAnnotation: {},
		gracefulHolderAnnotation: {}, gracefulRequestAnnotation: {},
		gracefulReleasedAtAnnotation: {}, gracefulLeaseUIDAnnotation: {},
		gracefulInstallationAnnotation: {}, gracefulClusterAnnotation: {},
	}
)

// HolderEvidence is the fixed identity snapshot written atomically with every
// Lease acquisition. Renewals must preserve these values unchanged.
type HolderEvidence struct {
	SchemaVersion    string `json:"coordinationSchemaVersion"`
	PodUID           string `json:"holderPodUID"`
	NodeName         string `json:"holderNodeName"`
	CSINodeID        string `json:"holderCSINodeID"`
	InstanceID       string `json:"holderInstanceID"`
	Zone             string `json:"holderZone"`
	InstallationID   string `json:"holderInstallationID"`
	ActiveClusterUID string `json:"holderActiveClusterUID"`
}

// NewHolderEvidence constructs and validates one runtime identity snapshot.
func NewHolderEvidence(podUID, nodeName, csiNodeID, instanceID, zone, installationID, clusterUID string) (HolderEvidence, error) {
	evidence := HolderEvidence{
		SchemaVersion: volume.SchemaVersionV1, PodUID: podUID, NodeName: nodeName,
		CSINodeID: csiNodeID, InstanceID: instanceID, Zone: zone,
		InstallationID: installationID, ActiveClusterUID: clusterUID,
	}
	if err := evidence.Validate(); err != nil {
		return HolderEvidence{}, err
	}
	return evidence, nil
}

// Validate proves the complete holder identity and its cross-field node mapping.
func (evidence HolderEvidence) Validate() error {
	if evidence.SchemaVersion != volume.SchemaVersionV1 {
		return fmt.Errorf("coordination schema version %q is unsupported", evidence.SchemaVersion)
	}
	if err := volume.ValidateOperationID(evidence.PodUID); err != nil {
		return fmt.Errorf("holder Pod UID: %w", err)
	}
	if len(evidence.NodeName) == 0 || len(evidence.NodeName) > 253 || !kubernetesNodeNamePattern.MatchString(evidence.NodeName) {
		return fmt.Errorf("holder node name %q is not a bounded DNS name", evidence.NodeName)
	}
	if err := volume.ValidateNodeID(evidence.CSINodeID); err != nil {
		return fmt.Errorf("holder CSI node ID: %w", err)
	}
	parts := strings.Split(evidence.CSINodeID, "/")
	if evidence.Zone != parts[0] || evidence.InstanceID != parts[1] {
		return fmt.Errorf("holder CSI node ID disagrees with zone or Instance ID")
	}
	if err := volume.ValidateInstallationID(evidence.InstallationID); err != nil {
		return err
	}
	return volume.ValidateClusterUID(evidence.ActiveClusterUID)
}

// Annotations returns the complete fixed holder evidence set.
func (evidence HolderEvidence) Annotations() (map[string]string, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		holderSchemaVersionAnnotation: evidence.SchemaVersion,
		holderPodUIDAnnotation:        evidence.PodUID, holderNodeNameAnnotation: evidence.NodeName,
		holderCSINodeIDAnnotation: evidence.CSINodeID, holderInstanceIDAnnotation: evidence.InstanceID,
		holderZoneAnnotation: evidence.Zone, holderInstallationAnnotation: evidence.InstallationID,
		holderClusterAnnotation: evidence.ActiveClusterUID,
	}, nil
}

// ParseHolderEvidence rejects partial, malformed, or unknown holder fields. It
// leaves unrelated Lease annotations outside this closed namespace untouched.
func ParseHolderEvidence(annotations map[string]string) (HolderEvidence, bool, error) {
	found := 0
	for key := range annotations {
		if _, known := holderAnnotationKeys[key]; known {
			found++
			continue
		}
		if key == holderSchemaVersionAnnotation || strings.HasPrefix(key, "holder") {
			return HolderEvidence{}, true, fmt.Errorf("unknown holder evidence annotation %q", key)
		}
	}
	if found == 0 {
		return HolderEvidence{}, false, nil
	}
	if found != len(holderAnnotationKeys) {
		return HolderEvidence{}, true, fmt.Errorf("holder evidence has %d annotations, want %d", found, len(holderAnnotationKeys))
	}
	evidence := HolderEvidence{
		SchemaVersion: annotations[holderSchemaVersionAnnotation],
		PodUID:        annotations[holderPodUIDAnnotation], NodeName: annotations[holderNodeNameAnnotation],
		CSINodeID: annotations[holderCSINodeIDAnnotation], InstanceID: annotations[holderInstanceIDAnnotation],
		Zone: annotations[holderZoneAnnotation], InstallationID: annotations[holderInstallationAnnotation],
		ActiveClusterUID: annotations[holderClusterAnnotation],
	}
	if err := evidence.Validate(); err != nil {
		return HolderEvidence{}, true, err
	}
	return evidence, true, nil
}

// GracefulRelease is the one-time handoff marker persisted while clearing the
// exact current holder in a single Lease compare-and-swap.
type GracefulRelease struct {
	SchemaVersion    string
	State            string
	HolderPodUID     string
	RequestID        string
	ReleasedAt       string
	LeaseUID         string
	InstallationID   string
	ActiveClusterUID string
}

// NewGracefulRelease binds one release marker to the immutable Lease UID and
// the complete releasing holder evidence.
func NewGracefulRelease(holder HolderEvidence, leaseUID, requestID string, releasedAt time.Time) (GracefulRelease, error) {
	if err := holder.Validate(); err != nil {
		return GracefulRelease{}, err
	}
	release := GracefulRelease{
		SchemaVersion: volume.SchemaVersionV1, State: "Released",
		HolderPodUID: holder.PodUID, RequestID: requestID,
		ReleasedAt: releasedAt.UTC().Format(time.RFC3339Nano), LeaseUID: leaseUID,
		InstallationID: holder.InstallationID, ActiveClusterUID: holder.ActiveClusterUID,
	}
	if err := release.Validate(); err != nil {
		return GracefulRelease{}, err
	}
	return release, nil
}

// Validate checks the closed marker schema independently of current runtime.
func (release GracefulRelease) Validate() error {
	if release.SchemaVersion != volume.SchemaVersionV1 || release.State != "Released" {
		return fmt.Errorf("graceful release schema or state is unsupported")
	}
	if err := volume.ValidateOperationID(release.HolderPodUID); err != nil {
		return fmt.Errorf("graceful release holder Pod UID: %w", err)
	}
	if err := volume.ValidateOperationID(release.RequestID); err != nil {
		return fmt.Errorf("graceful release request ID: %w", err)
	}
	if err := volume.ValidateOperationID(release.LeaseUID); err != nil {
		return fmt.Errorf("graceful release Lease UID: %w", err)
	}
	if err := volume.ValidateInstallationID(release.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(release.ActiveClusterUID); err != nil {
		return err
	}
	return validateCoordinationTimestamp("graceful release", release.ReleasedAt)
}

// ValidateHandoff proves this marker was emitted by the preserved previous
// holder on this exact Lease and in this runtime identity.
func (release GracefulRelease) ValidateHandoff(leaseUID, installationID, clusterUID string, previous HolderEvidence) error {
	if err := release.Validate(); err != nil {
		return err
	}
	if err := previous.Validate(); err != nil {
		return err
	}
	if release.LeaseUID != leaseUID || release.InstallationID != installationID || release.ActiveClusterUID != clusterUID || release.HolderPodUID != previous.PodUID || previous.InstallationID != installationID || previous.ActiveClusterUID != clusterUID {
		return fmt.Errorf("graceful release marker disagrees with Lease, previous holder, or runtime identity")
	}
	return nil
}

// Annotations returns the complete graceful-release marker.
func (release GracefulRelease) Annotations() (map[string]string, error) {
	if err := release.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		gracefulSchemaAnnotation: release.SchemaVersion, gracefulStateAnnotation: release.State,
		gracefulHolderAnnotation: release.HolderPodUID, gracefulRequestAnnotation: release.RequestID,
		gracefulReleasedAtAnnotation: release.ReleasedAt, gracefulLeaseUIDAnnotation: release.LeaseUID,
		gracefulInstallationAnnotation: release.InstallationID, gracefulClusterAnnotation: release.ActiveClusterUID,
	}, nil
}

// ParseGracefulRelease distinguishes absence from one complete marker.
func ParseGracefulRelease(annotations map[string]string) (GracefulRelease, bool, error) {
	found := 0
	for key := range annotations {
		if _, known := gracefulAnnotationKeys[key]; known {
			found++
			continue
		}
		if strings.HasPrefix(key, "gracefulRelease") {
			return GracefulRelease{}, true, fmt.Errorf("unknown graceful release annotation %q", key)
		}
	}
	if found == 0 {
		return GracefulRelease{}, false, nil
	}
	if found != len(gracefulAnnotationKeys) {
		return GracefulRelease{}, true, fmt.Errorf("graceful release has %d annotations, want %d", found, len(gracefulAnnotationKeys))
	}
	release := GracefulRelease{
		SchemaVersion: annotations[gracefulSchemaAnnotation], State: annotations[gracefulStateAnnotation],
		HolderPodUID: annotations[gracefulHolderAnnotation], RequestID: annotations[gracefulRequestAnnotation],
		ReleasedAt: annotations[gracefulReleasedAtAnnotation], LeaseUID: annotations[gracefulLeaseUIDAnnotation],
		InstallationID: annotations[gracefulInstallationAnnotation], ActiveClusterUID: annotations[gracefulClusterAnnotation],
	}
	if err := release.Validate(); err != nil {
		return GracefulRelease{}, true, err
	}
	return release, true, nil
}

// ClearGracefulReleaseAnnotations consumes only the one-time marker and
// preserves previous-holder evidence until the new acquisition replaces it.
func ClearGracefulReleaseAnnotations(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	for key := range gracefulAnnotationKeys {
		delete(result, key)
	}
	return result
}

func validateCoordinationTimestamp(name, value string) error {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !strings.HasSuffix(value, "Z") || parsed.UTC().Format(time.RFC3339Nano) != value {
		return fmt.Errorf("%s timestamp must be canonical RFC 3339 UTC", name)
	}
	return nil
}
