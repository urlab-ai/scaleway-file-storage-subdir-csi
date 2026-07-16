package coordination

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	bootstrapSchemaVersion    = "1"
	bootstrapPhasePrepared    = "Prepared"
	bootstrapAnnotationPrefix = "sfs-subdir-bootstrap-"
)

var bootstrapAnnotationKeys = map[string]struct{}{
	bootstrapAnnotationPrefix + "schema-version":              {},
	bootstrapAnnotationPrefix + "attempt-id":                  {},
	bootstrapAnnotationPrefix + "installation-id":             {},
	bootstrapAnnotationPrefix + "active-cluster-uid":          {},
	bootstrapAnnotationPrefix + "parent-filesystem-id":        {},
	bootstrapAnnotationPrefix + "controller-node-id":          {},
	bootstrapAnnotationPrefix + "controller-instance-id":      {},
	bootstrapAnnotationPrefix + "controller-zone":             {},
	bootstrapAnnotationPrefix + "empty-inventory-observed-at": {},
	bootstrapAnnotationPrefix + "phase":                       {},
	bootstrapAnnotationPrefix + "claim-temp-path":             {},
}

// BootstrapAttempt is the fixed Lease-backed operation journal written after
// empty provider inventory is proven and before the first attach call. It is
// not parent ownership evidence; it authorizes only exact same-attempt resume
// or provider-fenced offline rollback.
type BootstrapAttempt struct {
	SchemaVersion            string
	AttemptID                string
	InstallationID           string
	ActiveClusterUID         string
	ParentFilesystemID       string
	ControllerNodeID         string
	ControllerInstanceID     string
	ControllerZone           string
	EmptyInventoryObservedAt string
	Phase                    string
	ClaimTempPath            string
}

// NewBootstrapAttempt constructs the deterministic temporary claim path.
func NewBootstrapAttempt(attemptID, installationID, clusterUID, parentID, controllerNodeID, controllerInstanceID, controllerZone string, observedAt time.Time) (BootstrapAttempt, error) {
	attempt := BootstrapAttempt{
		SchemaVersion:            bootstrapSchemaVersion,
		AttemptID:                attemptID,
		InstallationID:           installationID,
		ActiveClusterUID:         clusterUID,
		ParentFilesystemID:       parentID,
		ControllerNodeID:         controllerNodeID,
		ControllerInstanceID:     controllerInstanceID,
		ControllerZone:           controllerZone,
		EmptyInventoryObservedAt: observedAt.UTC().Format(time.RFC3339Nano),
		Phase:                    bootstrapPhasePrepared,
		ClaimTempPath:            "/.sfs-subdir-csi-owner." + attemptID + ".tmp",
	}
	if err := attempt.Validate(); err != nil {
		return BootstrapAttempt{}, err
	}
	return attempt, nil
}

// Validate enforces the exact prepared journal identity.
func (attempt BootstrapAttempt) Validate() error {
	if attempt.SchemaVersion != bootstrapSchemaVersion || attempt.Phase != bootstrapPhasePrepared {
		return fmt.Errorf("bootstrap schema %q or phase %q is unsupported", attempt.SchemaVersion, attempt.Phase)
	}
	if err := volume.ValidateOperationID(attempt.AttemptID); err != nil {
		return fmt.Errorf("bootstrap attempt ID: %w", err)
	}
	if err := volume.ValidateInstallationID(attempt.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(attempt.ActiveClusterUID); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(attempt.ParentFilesystemID); err != nil {
		return err
	}
	if err := volume.ValidateNodeID(attempt.ControllerNodeID); err != nil {
		return fmt.Errorf("controller node ID: %w", err)
	}
	parts := strings.Split(attempt.ControllerNodeID, "/")
	if attempt.ControllerZone != parts[0] || attempt.ControllerInstanceID != parts[1] {
		return fmt.Errorf("controller node ID disagrees with recorded zone or Instance")
	}
	parsed, err := time.Parse(time.RFC3339Nano, attempt.EmptyInventoryObservedAt)
	if err != nil || !strings.HasSuffix(attempt.EmptyInventoryObservedAt, "Z") || parsed.UTC().Format(time.RFC3339Nano) != attempt.EmptyInventoryObservedAt {
		return fmt.Errorf("empty inventory observation timestamp must be canonical RFC 3339 UTC")
	}
	wantPath := "/.sfs-subdir-csi-owner." + attempt.AttemptID + ".tmp"
	if attempt.ClaimTempPath != wantPath {
		return fmt.Errorf("bootstrap claim temp path %q does not match attempt; want %q", attempt.ClaimTempPath, wantPath)
	}
	return nil
}

// Annotations returns the complete fixed Lease annotation set.
func (attempt BootstrapAttempt) Annotations() (map[string]string, error) {
	if err := attempt.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		bootstrapAnnotationPrefix + "schema-version":              attempt.SchemaVersion,
		bootstrapAnnotationPrefix + "attempt-id":                  attempt.AttemptID,
		bootstrapAnnotationPrefix + "installation-id":             attempt.InstallationID,
		bootstrapAnnotationPrefix + "active-cluster-uid":          attempt.ActiveClusterUID,
		bootstrapAnnotationPrefix + "parent-filesystem-id":        attempt.ParentFilesystemID,
		bootstrapAnnotationPrefix + "controller-node-id":          attempt.ControllerNodeID,
		bootstrapAnnotationPrefix + "controller-instance-id":      attempt.ControllerInstanceID,
		bootstrapAnnotationPrefix + "controller-zone":             attempt.ControllerZone,
		bootstrapAnnotationPrefix + "empty-inventory-observed-at": attempt.EmptyInventoryObservedAt,
		bootstrapAnnotationPrefix + "phase":                       attempt.Phase,
		bootstrapAnnotationPrefix + "claim-temp-path":             attempt.ClaimTempPath,
	}, nil
}

// ParseBootstrapAttempt distinguishes a conclusive absence from a complete
// journal. Any partial, malformed, or unknown bootstrap annotation fails closed.
func ParseBootstrapAttempt(annotations map[string]string) (attempt BootstrapAttempt, present bool, err error) {
	found := 0
	for key := range annotations {
		if strings.HasPrefix(key, bootstrapAnnotationPrefix) {
			if _, known := bootstrapAnnotationKeys[key]; !known {
				return BootstrapAttempt{}, true, fmt.Errorf("unknown bootstrap annotation %q", key)
			}
			found++
		}
	}
	if found == 0 {
		return BootstrapAttempt{}, false, nil
	}
	if found != len(bootstrapAnnotationKeys) {
		return BootstrapAttempt{}, true, fmt.Errorf("bootstrap journal has %d annotations, want %d", found, len(bootstrapAnnotationKeys))
	}
	attempt = BootstrapAttempt{
		SchemaVersion:            annotations[bootstrapAnnotationPrefix+"schema-version"],
		AttemptID:                annotations[bootstrapAnnotationPrefix+"attempt-id"],
		InstallationID:           annotations[bootstrapAnnotationPrefix+"installation-id"],
		ActiveClusterUID:         annotations[bootstrapAnnotationPrefix+"active-cluster-uid"],
		ParentFilesystemID:       annotations[bootstrapAnnotationPrefix+"parent-filesystem-id"],
		ControllerNodeID:         annotations[bootstrapAnnotationPrefix+"controller-node-id"],
		ControllerInstanceID:     annotations[bootstrapAnnotationPrefix+"controller-instance-id"],
		ControllerZone:           annotations[bootstrapAnnotationPrefix+"controller-zone"],
		EmptyInventoryObservedAt: annotations[bootstrapAnnotationPrefix+"empty-inventory-observed-at"],
		Phase:                    annotations[bootstrapAnnotationPrefix+"phase"],
		ClaimTempPath:            annotations[bootstrapAnnotationPrefix+"claim-temp-path"],
	}
	if err := attempt.Validate(); err != nil {
		return BootstrapAttempt{}, true, err
	}
	return attempt, true, nil
}

// ClearBootstrapAnnotations removes only the exact journal keys and preserves
// holder, graceful-release, approval-consumption, and unrelated annotations.
func ClearBootstrapAnnotations(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	for key := range bootstrapAnnotationKeys {
		delete(result, key)
	}
	return result
}
