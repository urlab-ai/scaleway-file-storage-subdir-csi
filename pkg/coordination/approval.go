package coordination

import (
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	// ApprovalSecretNameV1 is the fixed externally owned immutable Secret.
	ApprovalSecretNameV1 = "sfs-subdir-controller-approval"
	// CheckpointSecretNameV1 is the fixed externally owned immutable checkpoint.
	CheckpointSecretNameV1 = "sfs-subdir-checkpoint"
	// RecoveryFenceAllPreRecoveryInstances is the only missing-Lease scope that
	// can authorize recovery with durable prior state.
	RecoveryFenceAllPreRecoveryInstances = "all-pre-recovery-instances"

	approvalConsumptionSecretUIDAnnotation = "approvalConsumptionSecretUID"
	approvalConsumptionRequestAnnotation   = "approvalConsumptionRequestID"
	approvalConsumptionModeAnnotation      = "approvalConsumptionMode"
	approvalConsumptionPodAnnotation       = "approvalConsumptionPodUID"
	approvalConsumptionTimeAnnotation      = "approvalConsumedAt"
)

var (
	sha256DigestPattern               = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	approvalConsumptionAnnotationKeys = map[string]struct{}{
		approvalConsumptionSecretUIDAnnotation: {},
		approvalConsumptionRequestAnnotation:   {},
		approvalConsumptionModeAnnotation:      {},
		approvalConsumptionPodAnnotation:       {},
		approvalConsumptionTimeAnnotation:      {},
	}
)

// ApprovalMode is the closed operator authorization surface.
type ApprovalMode string

const (
	// ApprovalAbnormalTakeover authorizes a successor only after the exact
	// previous holder has been provider-fenced.
	ApprovalAbnormalTakeover ApprovalMode = "abnormal-takeover"
	// ApprovalMissingLeaseRecovery authorizes same-cluster checkpoint recovery
	// only after the all-pre-recovery-Instances fence.
	ApprovalMissingLeaseRecovery ApprovalMode = "missing-lease-recovery"
)

// OperatorApproval is the canonical value set read from the fixed immutable
// Secret. SecretUID and Immutable are Kubernetes metadata evidence, not data
// fields the controller may create or mutate.
type OperatorApproval struct {
	SecretUID                string
	Immutable                bool
	SchemaVersion            string
	Mode                     ApprovalMode
	RequestID                string
	InstallationID           string
	ActiveClusterUID         string
	PreviousHolderPodUID     string
	PreviousHolderNodeName   string
	PreviousHolderCSINodeID  string
	PreviousHolderInstanceID string
	PreviousHolderZone       string
	CheckpointRequestID      string
	CheckpointManifestSHA256 string
	RecoveryFenceScope       string
	Reason                   string
	ApprovedAt               string
	ExpiresAt                string
}

// ValidateAt checks schema, one-hour validity, freshness relative to the
// observed blocked condition, and mode-specific complete evidence.
func (approval OperatorApproval) ValidateAt(now, conditionObservedAt time.Time) error {
	if !approval.Immutable {
		return fmt.Errorf("operator approval Secret must be immutable")
	}
	if err := volume.ValidateOperationID(approval.SecretUID); err != nil {
		return fmt.Errorf("approval Secret UID: %w", err)
	}
	if approval.SchemaVersion != volume.SchemaVersionV1 {
		return fmt.Errorf("approval schema version %q is unsupported", approval.SchemaVersion)
	}
	if err := volume.ValidateOperationID(approval.RequestID); err != nil {
		return fmt.Errorf("approval request ID: %w", err)
	}
	if err := volume.ValidateInstallationID(approval.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(approval.ActiveClusterUID); err != nil {
		return err
	}
	if len(approval.Reason) == 0 || len(approval.Reason) > 512 || strings.ContainsAny(approval.Reason, "\x00\r\n") {
		return fmt.Errorf("approval reason must contain 1 to 512 single-line bytes")
	}
	approvedAt, err := parseCoordinationTimestamp("approvedAt", approval.ApprovedAt)
	if err != nil {
		return err
	}
	expiresAt, err := parseCoordinationTimestamp("expiresAt", approval.ExpiresAt)
	if err != nil {
		return err
	}
	if !expiresAt.After(approvedAt) || expiresAt.Sub(approvedAt) > time.Hour {
		return fmt.Errorf("approval validity must be positive and at most one hour")
	}
	if !approvedAt.After(conditionObservedAt) {
		return fmt.Errorf("approval predates the observed takeover or recovery condition")
	}
	if now.Before(approvedAt) || !now.Before(expiresAt) {
		return fmt.Errorf("approval is not currently valid")
	}

	switch approval.Mode {
	case ApprovalAbnormalTakeover:
		if err := approval.previousHolder().Validate(); err != nil {
			return fmt.Errorf("abnormal takeover previous holder: %w", err)
		}
		if approval.CheckpointRequestID != "" || approval.CheckpointManifestSHA256 != "" || approval.RecoveryFenceScope != "" {
			return fmt.Errorf("abnormal takeover approval contains missing-Lease recovery fields")
		}
	case ApprovalMissingLeaseRecovery:
		if err := volume.ValidateOperationID(approval.CheckpointRequestID); err != nil {
			return fmt.Errorf("checkpoint request ID: %w", err)
		}
		if !sha256DigestPattern.MatchString(approval.CheckpointManifestSHA256) {
			return fmt.Errorf("checkpoint manifest SHA-256 is malformed")
		}
		if approval.RecoveryFenceScope != RecoveryFenceAllPreRecoveryInstances {
			return fmt.Errorf("missing-Lease recovery fence scope %q is unsupported", approval.RecoveryFenceScope)
		}
		previousCount := 0
		for _, value := range []string{approval.PreviousHolderPodUID, approval.PreviousHolderNodeName, approval.PreviousHolderCSINodeID, approval.PreviousHolderInstanceID, approval.PreviousHolderZone} {
			if value != "" {
				previousCount++
			}
		}
		if previousCount != 0 && previousCount != 5 {
			return fmt.Errorf("historical previous holder fields must be all present or all absent")
		}
		if previousCount == 5 {
			if err := approval.previousHolder().Validate(); err != nil {
				return fmt.Errorf("historical previous holder: %w", err)
			}
		}
	default:
		return fmt.Errorf("approval mode %q is unsupported", approval.Mode)
	}
	return nil
}

// ValidateAbnormalHolder requires every previous-holder value to match the
// uncleared Lease exactly. Provider fencing remains a separate mandatory gate.
func (approval OperatorApproval) ValidateAbnormalHolder(holder HolderEvidence) error {
	if approval.Mode != ApprovalAbnormalTakeover {
		return fmt.Errorf("approval mode %q is not abnormal takeover", approval.Mode)
	}
	if approval.previousHolder() != holder {
		return fmt.Errorf("approval previous-holder evidence differs from Lease")
	}
	if approval.InstallationID != holder.InstallationID || approval.ActiveClusterUID != holder.ActiveClusterUID {
		return fmt.Errorf("approval runtime identity differs from previous holder evidence")
	}
	return nil
}

// ValidateCheckpoint binds missing-Lease recovery to the exact immutable
// checkpoint request and manifest digest.
func (approval OperatorApproval) ValidateCheckpoint(requestID, manifestSHA256 string) error {
	if approval.Mode != ApprovalMissingLeaseRecovery {
		return fmt.Errorf("approval mode %q is not missing-Lease recovery", approval.Mode)
	}
	if approval.CheckpointRequestID != requestID || approval.CheckpointManifestSHA256 != manifestSHA256 {
		return fmt.Errorf("approval does not match the immutable checkpoint")
	}
	return nil
}

func (approval OperatorApproval) previousHolder() HolderEvidence {
	return HolderEvidence{
		SchemaVersion: volume.SchemaVersionV1,
		PodUID:        approval.PreviousHolderPodUID, NodeName: approval.PreviousHolderNodeName,
		CSINodeID: approval.PreviousHolderCSINodeID, InstanceID: approval.PreviousHolderInstanceID,
		Zone: approval.PreviousHolderZone, InstallationID: approval.InstallationID,
		ActiveClusterUID: approval.ActiveClusterUID,
	}
}

// ApprovalConsumption is the permanent Lease audit evidence written in the
// same compare-and-swap that installs the successor holder.
type ApprovalConsumption struct {
	SecretUID       string
	RequestID       string
	Mode            ApprovalMode
	ConsumingPodUID string
	ConsumedAt      string
}

// NewApprovalConsumption constructs the one-time audit projection.
func NewApprovalConsumption(approval OperatorApproval, consumingPodUID string, consumedAt time.Time) (ApprovalConsumption, error) {
	if err := volume.ValidateOperationID(consumingPodUID); err != nil {
		return ApprovalConsumption{}, fmt.Errorf("consuming Pod UID: %w", err)
	}
	consumption := ApprovalConsumption{
		SecretUID: approval.SecretUID, RequestID: approval.RequestID, Mode: approval.Mode,
		ConsumingPodUID: consumingPodUID, ConsumedAt: consumedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := consumption.Validate(); err != nil {
		return ApprovalConsumption{}, err
	}
	return consumption, nil
}

// Validate checks the complete consumption audit record.
func (consumption ApprovalConsumption) Validate() error {
	if err := volume.ValidateOperationID(consumption.SecretUID); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(consumption.RequestID); err != nil {
		return err
	}
	if consumption.Mode != ApprovalAbnormalTakeover && consumption.Mode != ApprovalMissingLeaseRecovery {
		return fmt.Errorf("approval consumption mode %q is unsupported", consumption.Mode)
	}
	if err := volume.ValidateOperationID(consumption.ConsumingPodUID); err != nil {
		return err
	}
	return validateCoordinationTimestamp("approval consumption", consumption.ConsumedAt)
}

// Annotations returns the complete permanent Lease audit projection.
func (consumption ApprovalConsumption) Annotations() (map[string]string, error) {
	if err := consumption.Validate(); err != nil {
		return nil, err
	}
	return map[string]string{
		approvalConsumptionSecretUIDAnnotation: consumption.SecretUID,
		approvalConsumptionRequestAnnotation:   consumption.RequestID,
		approvalConsumptionModeAnnotation:      string(consumption.Mode),
		approvalConsumptionPodAnnotation:       consumption.ConsumingPodUID,
		approvalConsumptionTimeAnnotation:      consumption.ConsumedAt,
	}, nil
}

// ParseApprovalConsumption distinguishes absence from one complete, immutable
// audit tuple. Partial or future fields under the same namespace fail closed.
func ParseApprovalConsumption(annotations map[string]string) (ApprovalConsumption, bool, error) {
	found := 0
	for key := range annotations {
		if _, known := approvalConsumptionAnnotationKeys[key]; known {
			found++
			continue
		}
		if strings.HasPrefix(key, "approvalConsumption") || key == approvalConsumptionTimeAnnotation {
			return ApprovalConsumption{}, true, fmt.Errorf("unknown approval consumption annotation %q", key)
		}
	}
	if found == 0 {
		return ApprovalConsumption{}, false, nil
	}
	if found != len(approvalConsumptionAnnotationKeys) {
		return ApprovalConsumption{}, true, fmt.Errorf("approval consumption has %d annotations, want %d", found, len(approvalConsumptionAnnotationKeys))
	}
	consumption := ApprovalConsumption{
		SecretUID:       annotations[approvalConsumptionSecretUIDAnnotation],
		RequestID:       annotations[approvalConsumptionRequestAnnotation],
		Mode:            ApprovalMode(annotations[approvalConsumptionModeAnnotation]),
		ConsumingPodUID: annotations[approvalConsumptionPodAnnotation],
		ConsumedAt:      annotations[approvalConsumptionTimeAnnotation],
	}
	if err := consumption.Validate(); err != nil {
		return ApprovalConsumption{}, true, err
	}
	return consumption, true, nil
}

// ApplyApprovalConsumption preserves unrelated Lease annotations and refuses
// to overwrite any prior consumption, including a different approval.
func ApplyApprovalConsumption(annotations map[string]string, consumption ApprovalConsumption) (map[string]string, error) {
	if annotations == nil {
		return nil, fmt.Errorf("lease annotations must be an explicit map")
	}
	if existing, present, err := ParseApprovalConsumption(annotations); err != nil {
		return nil, err
	} else if present {
		return nil, fmt.Errorf("approval %s/%s is already consumed", existing.SecretUID, existing.RequestID)
	}
	encoded, err := consumption.Annotations()
	if err != nil {
		return nil, err
	}
	result := maps.Clone(annotations)
	for key, value := range encoded {
		result[key] = value
	}
	return result, nil
}

func parseCoordinationTimestamp(name, value string) (time.Time, error) {
	if err := validateCoordinationTimestamp(name, value); err != nil {
		return time.Time{}, err
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed, nil
}
