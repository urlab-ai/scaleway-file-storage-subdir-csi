package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
)

const handlePrefix = "sfs1"

var (
	// ErrForeignHandle identifies a non-empty volume ID that this driver could
	// never have emitted. DeleteVolume may return idempotent success without a
	// lookup for this class only.
	ErrForeignHandle = errors.New("foreign volume handle")
	// ErrInvalidHandle identifies a malformed handle in this driver's v1
	// namespace. It is evidence of bad input or corruption, not unknown state.
	ErrInvalidHandle = errors.New("invalid v1 volume handle")
)

// Handle is the compact v1 CSI volume ID.
type Handle struct {
	LogicalVolumeID string
	MappingHash     string
}

// Mapping is the complete immutable input to the compact mapping hash.
type Mapping struct {
	PoolName           string
	ParentFilesystemID string
	BasePath           string
	DirectoryName      string
	LogicalVolumeID    string
}

// LogicalVolumeID derives the permanent identity for one CreateVolume name.
func LogicalVolumeID(driverName, createVolumeRequestName string) (string, error) {
	if err := ValidateDriverName(driverName); err != nil {
		return "", err
	}
	if createVolumeRequestName == "" {
		return "", fmt.Errorf("create volume request name is empty")
	}
	if strings.IndexByte(createVolumeRequestName, 0) >= 0 {
		return "", fmt.Errorf("create volume request name contains NUL")
	}

	sum := sha256.Sum256([]byte(driverName + "\x00" + createVolumeRequestName))
	return "lv-" + hex.EncodeToString(sum[:16]), nil
}

// MappingHash returns the canonical truncated hash of the immutable mapping.
func MappingHash(mapping Mapping) (string, error) {
	if err := mapping.Validate(); err != nil {
		return "", err
	}
	payload := map[string]string{
		"basePath":           mapping.BasePath,
		"directoryName":      mapping.DirectoryName,
		"logicalVolumeID":    mapping.LogicalVolumeID,
		"parentFilesystemID": mapping.ParentFilesystemID,
		"poolName":           mapping.PoolName,
	}
	return truncatedCanonicalHash("mh-", payload)
}

// Validate verifies every mapping component before it can influence a path.
func (mapping Mapping) Validate() error {
	if err := ValidatePoolName(mapping.PoolName); err != nil {
		return err
	}
	if err := ValidateParentFilesystemID(mapping.ParentFilesystemID); err != nil {
		return err
	}
	if err := ValidateBasePath(mapping.BasePath); err != nil {
		return err
	}
	if err := ValidateDirectoryName(mapping.DirectoryName); err != nil {
		return err
	}
	if err := ValidateLogicalVolumeID(mapping.LogicalVolumeID); err != nil {
		return err
	}
	return nil
}

// NewHandle validates the mapping and returns its compact handle.
func NewHandle(mapping Mapping) (Handle, error) {
	hash, err := MappingHash(mapping)
	if err != nil {
		return Handle{}, err
	}
	return Handle{LogicalVolumeID: mapping.LogicalVolumeID, MappingHash: hash}, nil
}

// ParseHandle parses a compact v1 handle without consulting durable state.
func ParseHandle(encoded string) (Handle, error) {
	if encoded == "" {
		return Handle{}, fmt.Errorf("%w: handle is empty", ErrInvalidHandle)
	}
	if len(encoded) > MaxHandleBytes {
		return Handle{}, fmt.Errorf("%w: handle length %d exceeds %d bytes", ErrInvalidHandle, len(encoded), MaxHandleBytes)
	}
	if !strings.HasPrefix(encoded, handlePrefix+":") {
		return Handle{}, fmt.Errorf("%w: unsupported prefix", ErrForeignHandle)
	}

	parts := strings.Split(encoded, ":")
	if len(parts) != 3 || parts[0] != handlePrefix {
		return Handle{}, fmt.Errorf("%w: expected three colon-separated fields", ErrInvalidHandle)
	}
	handle := Handle{LogicalVolumeID: parts[1], MappingHash: parts[2]}
	if err := ValidateLogicalVolumeID(handle.LogicalVolumeID); err != nil {
		return Handle{}, fmt.Errorf("%w: %v", ErrInvalidHandle, err)
	}
	if err := validateMappingHash(handle.MappingHash); err != nil {
		return Handle{}, fmt.Errorf("%w: %v", ErrInvalidHandle, err)
	}
	return handle, nil
}

// String serializes a validated handle.
func (handle Handle) String() string {
	return handlePrefix + ":" + handle.LogicalVolumeID + ":" + handle.MappingHash
}

// ValidateMapping proves that the handle still names the supplied mapping.
func (handle Handle) ValidateMapping(mapping Mapping) error {
	if handle.LogicalVolumeID != mapping.LogicalVolumeID {
		return fmt.Errorf("logical volume ID mismatch: handle=%q mapping=%q", handle.LogicalVolumeID, mapping.LogicalVolumeID)
	}
	want, err := MappingHash(mapping)
	if err != nil {
		return err
	}
	if handle.MappingHash != want {
		return fmt.Errorf("mapping hash mismatch: handle=%q computed=%q", handle.MappingHash, want)
	}
	return nil
}

// BasePathHash returns the v1 hash projection of a normalized base path.
func BasePathHash(basePath string) (string, error) {
	if err := ValidateBasePath(basePath); err != nil {
		return "", err
	}
	return truncatedBytesHash("bp-", []byte(basePath)), nil
}

// VolumeHandleHash returns the v1 hash projection of a complete valid handle.
func VolumeHandleHash(encoded string) (string, error) {
	if _, err := ParseHandle(encoded); err != nil {
		return "", err
	}
	return truncatedBytesHash("vh-", []byte(encoded)), nil
}

func truncatedCanonicalHash(prefix string, value any) (string, error) {
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		return "", err
	}
	return truncatedBytesHash(prefix, encoded), nil
}

func truncatedBytesHash(prefix string, value []byte) string {
	sum := sha256.Sum256(value)
	return prefix + hex.EncodeToString(sum[:16])
}
