package volume

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const defaultLogicalReservationBytes = uint64(1 << 30)

var (
	// ErrCreateReplayIncompatible marks a same-name request whose immutable
	// semantics cannot reuse the durable mapping under CSI idempotency rules.
	ErrCreateReplayIncompatible = errors.New("create replay is incompatible with existing volume")
	// ErrCapacityOutOfRange identifies a valid CSI capacity range that cannot
	// contain the driver's default or requested logical reservation.
	ErrCapacityOutOfRange = errors.New("requested capacity range cannot be satisfied")
)

// DeletePolicy is the immutable logical-volume data disposition.
type DeletePolicy string

const (
	// DeletePolicyArchive moves data to the driver-owned archive directory.
	DeletePolicyArchive DeletePolicy = "archive"
	// DeletePolicyDelete removes data through the quarantine state machine.
	DeletePolicyDelete DeletePolicy = "delete"
	// DeletePolicyRetain leaves data at its original validated path.
	DeletePolicyRetain DeletePolicy = "retain"
)

// AccessMode is one of the two mounted-filesystem modes supported by v1.
type AccessMode string

const (
	// AccessModeSingleNodeWriter allows one published node and one node target.
	AccessModeSingleNodeWriter AccessMode = "SINGLE_NODE_WRITER"
	// AccessModeMultiNodeMultiWriter is the primary RWX access mode.
	AccessModeMultiNodeMultiWriter AccessMode = "MULTI_NODE_MULTI_WRITER"
)

// CreateParameters is the normalized immutable subset of CreateVolume input.
type CreateParameters struct {
	PoolName       string       `json:"poolName"`
	DeletePolicy   DeletePolicy `json:"deletePolicy"`
	DirectoryUID   uint32       `json:"directoryUid"`
	DirectoryGID   uint32       `json:"directoryGid"`
	DirectoryMode  string       `json:"directoryMode"`
	AccessType     string       `json:"accessType"`
	FilesystemType string       `json:"filesystemType"`
	AccessModes    []AccessMode `json:"accessModes"`
}

// CreateRequestIdentity contains exactly the fields covered by requestHash.
type CreateRequestIdentity struct {
	OriginalRequiredBytes uint64
	OriginalLimitBytes    uint64
	SelectedCapacityBytes uint64
	Parameters            CreateParameters
}

// Normalize validates parameters and sorts set-like access modes.
func (parameters CreateParameters) Normalize() (CreateParameters, error) {
	if err := ValidatePoolName(parameters.PoolName); err != nil {
		return CreateParameters{}, err
	}
	if err := parameters.DeletePolicy.Validate(); err != nil {
		return CreateParameters{}, err
	}
	if parameters.DirectoryUID > 2147483647 || parameters.DirectoryGID > 2147483647 {
		return CreateParameters{}, fmt.Errorf("directory UID and GID must not exceed 2147483647")
	}
	if err := ValidateDirectoryMode(parameters.DirectoryMode); err != nil {
		return CreateParameters{}, err
	}
	if parameters.AccessType != "mount" {
		return CreateParameters{}, fmt.Errorf("access type %q is unsupported; v1 requires mount", parameters.AccessType)
	}
	filesystemType := strings.ToLower(parameters.FilesystemType)
	if filesystemType != "" && filesystemType != "virtiofs" {
		return CreateParameters{}, fmt.Errorf("filesystem type %q is unsupported", parameters.FilesystemType)
	}
	if filesystemType == "" {
		filesystemType = "virtiofs"
	}

	modes := slices.Clone(parameters.AccessModes)
	if len(modes) == 0 {
		return CreateParameters{}, fmt.Errorf("at least one access mode is required")
	}
	slices.Sort(modes)
	modes = slices.Compact(modes)
	for _, mode := range modes {
		if err := mode.Validate(); err != nil {
			return CreateParameters{}, err
		}
	}

	parameters.FilesystemType = filesystemType
	parameters.AccessModes = modes
	return parameters, nil
}

// Validate rejects unknown delete policies.
func (policy DeletePolicy) Validate() error {
	switch policy {
	case DeletePolicyArchive, DeletePolicyDelete, DeletePolicyRetain:
		return nil
	default:
		return fmt.Errorf("delete policy %q is unsupported", policy)
	}
}

// Validate rejects access modes outside the closed v1 surface.
func (mode AccessMode) Validate() error {
	switch mode {
	case AccessModeSingleNodeWriter, AccessModeMultiNodeMultiWriter:
		return nil
	default:
		return fmt.Errorf("access mode %q is unsupported", mode)
	}
}

// RequestHash computes the canonical integrity hash for normalized creation.
func RequestHash(identity CreateRequestIdentity) (string, error) {
	parameters, err := identity.Parameters.Normalize()
	if err != nil {
		return "", err
	}
	if identity.SelectedCapacityBytes == 0 {
		return "", fmt.Errorf("selected capacity must be greater than zero")
	}
	if identity.OriginalLimitBytes > 0 && identity.SelectedCapacityBytes > identity.OriginalLimitBytes {
		return "", fmt.Errorf("selected capacity exceeds original limit")
	}

	payload := map[string]any{
		"accessModes":           parameters.AccessModes,
		"accessType":            parameters.AccessType,
		"deletePolicy":          parameters.DeletePolicy,
		"directoryGid":          parameters.DirectoryGID,
		"directoryMode":         parameters.DirectoryMode,
		"directoryUid":          parameters.DirectoryUID,
		"filesystemType":        parameters.FilesystemType,
		"originalLimitBytes":    identity.OriginalLimitBytes,
		"originalRequiredBytes": identity.OriginalRequiredBytes,
		"poolName":              parameters.PoolName,
		"selectedCapacityBytes": identity.SelectedCapacityBytes,
	}
	return truncatedCanonicalHash("rh-", payload)
}

// CapacityCompatible implements CSI range compatibility for a replay.
func CapacityCompatible(selected, required, limit uint64) bool {
	if selected < required {
		return false
	}
	return limit == 0 || selected <= limit
}

// SelectCapacity applies the v1 1-GiB default and capacity-range rules before
// placement or durable mutation.
func SelectCapacity(required, limit uint64) (uint64, error) {
	selected := required
	if selected == 0 {
		selected = defaultLogicalReservationBytes
	}
	if limit > 0 && selected > limit {
		return 0, fmt.Errorf("selected capacity %d exceeds limit %d: %w", selected, limit, ErrCapacityOutOfRange)
	}
	return selected, nil
}

// ValidateCreateReplay performs the CSI semantic compatibility check. The
// request hash is intentionally not compared: a different compatible capacity
// range must return the existing volume.
func ValidateCreateReplay(record *DetailedAllocationRecord, requestName string, required, limit uint64, parameters CreateParameters) error {
	if record == nil {
		return fmt.Errorf("existing allocation record is nil")
	}
	if err := record.Validate(); err != nil {
		return fmt.Errorf("existing allocation record: %w", err)
	}
	logicalID, err := LogicalVolumeID(record.DriverName, requestName)
	if err != nil {
		return err
	}
	if logicalID != record.LogicalVolumeID || requestName != record.CreateVolumeRequestName {
		return fmt.Errorf("request name resolves to different logical identity: %w", ErrCreateReplayIncompatible)
	}
	normalized, err := parameters.Normalize()
	if err != nil {
		return err
	}
	if !EqualCreateParameters(normalized, record.NormalizedCreateParameters) {
		return fmt.Errorf("normalized immutable creation parameters differ: %w", ErrCreateReplayIncompatible)
	}
	if !CapacityCompatible(record.SelectedCapacityBytes, required, limit) {
		return fmt.Errorf("persisted capacity %d is outside replay range required=%d limit=%d: %w", record.SelectedCapacityBytes, required, limit, ErrCreateReplayIncompatible)
	}
	return nil
}

// EqualCreateParameters compares already-normalized immutable parameters.
func EqualCreateParameters(left, right CreateParameters) bool {
	return left.PoolName == right.PoolName &&
		left.DeletePolicy == right.DeletePolicy &&
		left.DirectoryUID == right.DirectoryUID &&
		left.DirectoryGID == right.DirectoryGID &&
		left.DirectoryMode == right.DirectoryMode &&
		left.AccessType == right.AccessType &&
		left.FilesystemType == right.FilesystemType &&
		slices.Equal(left.AccessModes, right.AccessModes)
}
