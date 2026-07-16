package volume

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	// SchemaVersionV1 is the only durable and wire schema understood by v1.
	SchemaVersionV1 = "1"
	// MaxHandleBytes is the CSI volume ID limit enforced by this driver.
	MaxHandleBytes = 128
	// MaxContextEntryBytes is the per-key and per-value CSI limit.
	MaxContextEntryBytes = 128
	// MaxContextBytes is the aggregate CSI volume_context limit.
	MaxContextBytes = 4 * 1024
	// MaxDirectoryNameBytes leaves room for the directory in volume_context.
	MaxDirectoryNameBytes = MaxContextEntryBytes
)

var (
	driverNamePattern       = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$`)
	uuidPattern             = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	poolNamePattern         = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)
	providerIDPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	nodeZonePattern         = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
	logicalVolumeIDPattern  = regexp.MustCompile(`^lv-[0-9a-f]{32}$`)
	mappingHashPattern      = regexp.MustCompile(`^mh-[0-9a-f]{32}$`)
	requestHashPattern      = regexp.MustCompile(`^rh-[0-9a-f]{32}$`)
	basePathHashPattern     = regexp.MustCompile(`^bp-[0-9a-f]{32}$`)
	volumeHandleHashPattern = regexp.MustCompile(
		`^vh-[0-9a-f]{32}$`,
	)
)

// ValidateDriverName validates the immutable CSI driver name contract.
func ValidateDriverName(name string) error {
	if !utf8.ValidString(name) || len(name) == 0 || len(name) > 63 {
		return fmt.Errorf("driver name must contain 1 to 63 UTF-8 bytes")
	}
	if !driverNamePattern.MatchString(name) {
		return fmt.Errorf("driver name %q must be a lowercase reverse-domain name", name)
	}
	return nil
}

// ValidateOperationID validates collision-resistant journal and lifecycle IDs.
func ValidateOperationID(id string) error {
	if !uuidPattern.MatchString(id) {
		return fmt.Errorf("operation ID must be a lowercase RFC 4122 UUID")
	}
	return nil
}

// ValidateNodeID validates the exact <zone>/<serverID> CSI node identity.
func ValidateNodeID(id string) error {
	if !utf8.ValidString(id) || len(id) == 0 || len(id) > MaxContextEntryBytes {
		return fmt.Errorf("node ID must contain 1 to %d UTF-8 bytes", MaxContextEntryBytes)
	}
	parts := strings.Split(id, "/")
	if len(parts) != 2 || !nodeZonePattern.MatchString(parts[0]) || !providerIDPattern.MatchString(parts[1]) {
		return fmt.Errorf("node ID %q must match <zone>/<serverID>", id)
	}
	return nil
}

// ValidateInstallationID validates the stable production installation UUID.
func ValidateInstallationID(id string) error {
	if !uuidPattern.MatchString(id) {
		return fmt.Errorf("installation ID must be a lowercase RFC 4122 UUID")
	}
	return nil
}

// ValidateClusterUID validates the immutable kube-system Namespace UID.
func ValidateClusterUID(uid string) error {
	if !utf8.ValidString(uid) || len(uid) == 0 || len(uid) > MaxContextEntryBytes {
		return fmt.Errorf("active cluster UID must contain 1 to %d UTF-8 bytes", MaxContextEntryBytes)
	}
	if strings.ContainsAny(uid, "/\x00\r\n") {
		return fmt.Errorf("active cluster UID contains a forbidden character")
	}
	return nil
}

// ValidatePoolName validates a bounded DNS label used in durable records.
func ValidatePoolName(name string) error {
	if len(name) == 0 || len(name) > 63 || !poolNamePattern.MatchString(name) {
		return fmt.Errorf("pool name %q must be a lowercase DNS label of at most 63 bytes", name)
	}
	return nil
}

// ValidateParentFilesystemID validates the bounded opaque provider identifier.
// The provider client performs the stricter SDK-specific UUID validation.
func ValidateParentFilesystemID(id string) error {
	if !utf8.ValidString(id) || len(id) == 0 || len(id) > MaxContextEntryBytes {
		return fmt.Errorf("parent filesystem ID must contain 1 to %d UTF-8 bytes", MaxContextEntryBytes)
	}
	if !providerIDPattern.MatchString(id) {
		return fmt.Errorf("parent filesystem ID %q contains unsafe characters", id)
	}
	return nil
}

// ValidateLogicalVolumeID validates a deterministic v1 logical volume ID.
func ValidateLogicalVolumeID(id string) error {
	if !logicalVolumeIDPattern.MatchString(id) {
		return fmt.Errorf("logical volume ID %q does not match the v1 format", id)
	}
	return nil
}

func validateMappingHash(hash string) error {
	if !mappingHashPattern.MatchString(hash) {
		return fmt.Errorf("mapping hash %q does not match the v1 format", hash)
	}
	return nil
}

func validateRequestHash(hash string) error {
	if !requestHashPattern.MatchString(hash) {
		return fmt.Errorf("request hash %q does not match the v1 format", hash)
	}
	return nil
}

func validateBasePathHash(hash string) error {
	if !basePathHashPattern.MatchString(hash) {
		return fmt.Errorf("base path hash %q does not match the v1 format", hash)
	}
	return nil
}

func validateVolumeHandleHash(hash string) error {
	if !volumeHandleHashPattern.MatchString(hash) {
		return fmt.Errorf("volume handle hash %q does not match the v1 format", hash)
	}
	return nil
}
