package volume

import (
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

var (
	// ErrInvalidContext identifies malformed or incomplete CSI wire context.
	// It is intentionally limited to inbound map parsing: validation failures
	// for durable records remain internal consistency errors and must not be
	// exposed to the CO as bad request data.
	ErrInvalidContext = errors.New("invalid volume context")
	// ErrContextMismatch identifies a syntactically valid inbound context or
	// handle that disagrees with an already-validated durable mapping. Durable
	// record validation errors remain internal and never wrap this sentinel.
	ErrContextMismatch = errors.New("volume context does not match durable mapping")
)

var immutableContextKeys = map[string]struct{}{
	"schemaVersion":      {},
	"installationID":     {},
	"activeClusterUID":   {},
	"poolName":           {},
	"parentFilesystemID": {},
	"basePath":           {},
	"basePathHash":       {},
	"directoryName":      {},
	"directoryMode":      {},
	"directoryUid":       {},
	"directoryGid":       {},
	"onDelete":           {},
	"logicalVolumeID":    {},
}

// ExternalProvisionerIdentityKey is the single CO-owned delivery field that
// external-provisioner adds to PersistentVolume volumeAttributes after
// CreateVolume. The driver never emits, hashes, persists, or authorizes from
// this value; it accepts and strips it when Kubernetes replays the PV context.
const ExternalProvisionerIdentityKey = "storage.kubernetes.io/csiProvisionerIdentity"

// ImmutableContext is the complete v1 CSI volume_context projection.
type ImmutableContext struct {
	SchemaVersion      string
	InstallationID     string
	ActiveClusterUID   string
	PoolName           string
	ParentFilesystemID string
	BasePath           string
	BasePathHash       string
	DirectoryName      string
	DirectoryMode      string
	DirectoryUID       uint32
	DirectoryGID       uint32
	DeletePolicy       DeletePolicy
	LogicalVolumeID    string
}

// Map returns the exact string map sent on the CSI wire.
func (context ImmutableContext) Map() (map[string]string, error) {
	if err := context.Validate(); err != nil {
		return nil, err
	}
	result := map[string]string{
		"schemaVersion":      context.SchemaVersion,
		"installationID":     context.InstallationID,
		"activeClusterUID":   context.ActiveClusterUID,
		"poolName":           context.PoolName,
		"parentFilesystemID": context.ParentFilesystemID,
		"basePath":           context.BasePath,
		"basePathHash":       context.BasePathHash,
		"directoryName":      context.DirectoryName,
		"directoryMode":      context.DirectoryMode,
		"directoryUid":       strconv.FormatUint(uint64(context.DirectoryUID), 10),
		"directoryGid":       strconv.FormatUint(uint64(context.DirectoryGID), 10),
		"onDelete":           string(context.DeletePolicy),
		"logicalVolumeID":    context.LogicalVolumeID,
	}
	if err := ValidateWireContext(result); err != nil {
		return nil, err
	}
	return result, nil
}

// Validate checks every immutable context field against the v1 contract.
func (context ImmutableContext) Validate() error {
	if context.SchemaVersion != SchemaVersionV1 {
		return fmt.Errorf("volume context schema version %q is unsupported", context.SchemaVersion)
	}
	if err := ValidateInstallationID(context.InstallationID); err != nil {
		return err
	}
	if err := ValidateClusterUID(context.ActiveClusterUID); err != nil {
		return err
	}
	if err := ValidatePoolName(context.PoolName); err != nil {
		return err
	}
	if err := ValidateParentFilesystemID(context.ParentFilesystemID); err != nil {
		return err
	}
	if err := ValidateBasePath(context.BasePath); err != nil {
		return err
	}
	wantBasePathHash, err := BasePathHash(context.BasePath)
	if err != nil {
		return err
	}
	if context.BasePathHash != wantBasePathHash {
		return fmt.Errorf("base path hash mismatch: stored=%q computed=%q", context.BasePathHash, wantBasePathHash)
	}
	if err := ValidateDirectoryName(context.DirectoryName); err != nil {
		return err
	}
	if err := ValidateDirectoryMode(context.DirectoryMode); err != nil {
		return err
	}
	if context.DirectoryUID > 2147483647 || context.DirectoryGID > 2147483647 {
		return fmt.Errorf("directory UID and GID must not exceed 2147483647")
	}
	if err := context.DeletePolicy.Validate(); err != nil {
		return err
	}
	return ValidateLogicalVolumeID(context.LogicalVolumeID)
}

// ParseImmutableContext validates the closed driver-owned map and returns typed
// state. Kubernetes delivery may add exactly ExternalProvisionerIdentityKey;
// its bounded non-empty value is ignored because it is not mapping identity.
func ParseImmutableContext(values map[string]string) (ImmutableContext, error) {
	immutable, err := parseImmutableContext(values)
	if err != nil {
		return ImmutableContext{}, fmt.Errorf("%w: %v", ErrInvalidContext, err)
	}
	return immutable, nil
}

func parseImmutableContext(values map[string]string) (ImmutableContext, error) {
	if err := ValidateWireContext(values); err != nil {
		return ImmutableContext{}, err
	}
	provisionerIdentity, hasProvisionerIdentity := values[ExternalProvisionerIdentityKey]
	wantFields := len(immutableContextKeys)
	if hasProvisionerIdentity {
		wantFields++
		if provisionerIdentity == "" {
			return ImmutableContext{}, fmt.Errorf("volume context field %q is empty", ExternalProvisionerIdentityKey)
		}
	}
	if len(values) != wantFields {
		return ImmutableContext{}, fmt.Errorf("volume context has %d fields, want %d", len(values), wantFields)
	}
	for key := range values {
		if key == ExternalProvisionerIdentityKey {
			continue
		}
		if _, known := immutableContextKeys[key]; !known {
			return ImmutableContext{}, fmt.Errorf("volume context field %q is unknown", key)
		}
	}
	for key := range immutableContextKeys {
		if _, present := values[key]; !present {
			return ImmutableContext{}, fmt.Errorf("volume context field %q is missing", key)
		}
	}

	uid, err := parseDirectoryIdentity("directoryUid", values["directoryUid"])
	if err != nil {
		return ImmutableContext{}, err
	}
	gid, err := parseDirectoryIdentity("directoryGid", values["directoryGid"])
	if err != nil {
		return ImmutableContext{}, err
	}
	context := ImmutableContext{
		SchemaVersion:      values["schemaVersion"],
		InstallationID:     values["installationID"],
		ActiveClusterUID:   values["activeClusterUID"],
		PoolName:           values["poolName"],
		ParentFilesystemID: values["parentFilesystemID"],
		BasePath:           values["basePath"],
		BasePathHash:       values["basePathHash"],
		DirectoryName:      values["directoryName"],
		DirectoryMode:      values["directoryMode"],
		DirectoryUID:       uid,
		DirectoryGID:       gid,
		DeletePolicy:       DeletePolicy(values["onDelete"]),
		LogicalVolumeID:    values["logicalVolumeID"],
	}
	if err := context.Validate(); err != nil {
		return ImmutableContext{}, err
	}
	return context, nil
}

// DriverOwnedContextMap validates a delivered CSI context and returns only the
// exact 13 driver-owned fields. Recovery and checkpoint projections use this
// form so a sidecar-generated identity never becomes durable mapping identity.
func DriverOwnedContextMap(values map[string]string) (map[string]string, error) {
	context, err := ParseImmutableContext(values)
	if err != nil {
		return nil, err
	}
	return context.Map()
}

// ValidateWireContext enforces the CSI per-entry and aggregate byte limits.
// CSI defines this size over the UTF-8 bytes of all keys and values.
func ValidateWireContext(values map[string]string) error {
	total := 0
	for key, value := range values {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return fmt.Errorf("volume context field %q is not valid UTF-8", key)
		}
		if len(key) > MaxContextEntryBytes {
			return fmt.Errorf("volume context key length %d exceeds %d bytes", len(key), MaxContextEntryBytes)
		}
		if len(value) > MaxContextEntryBytes {
			return fmt.Errorf("volume context value for %q has %d bytes, exceeds %d", key, len(value), MaxContextEntryBytes)
		}
		total += len(key) + len(value)
		if total > MaxContextBytes {
			return fmt.Errorf("volume context size %d exceeds %d bytes", total, MaxContextBytes)
		}
	}
	return nil
}

func parseDirectoryIdentity(field, value string) (uint32, error) {
	if value == "" {
		return 0, fmt.Errorf("volume context field %q is empty", field)
	}
	parsed, err := strconv.ParseUint(value, 10, 31)
	if err != nil {
		return 0, fmt.Errorf("volume context field %q is not a base-10 identity in [0,2147483647]: %w", field, err)
	}
	if strconv.FormatUint(parsed, 10) != value {
		return 0, fmt.Errorf("volume context field %q is not canonically encoded", field)
	}
	return uint32(parsed), nil
}

// ValidateDirectoryMode accepts one canonical four-digit octal mode.
func ValidateDirectoryMode(mode string) error {
	if len(mode) != 4 || mode[0] != '0' {
		return fmt.Errorf("directory mode %q must match 0[0-7]{3}", mode)
	}
	for _, digit := range mode[1:] {
		if digit < '0' || digit > '7' {
			return fmt.Errorf("directory mode %q must match 0[0-7]{3}", mode)
		}
	}
	return nil
}
