package coordination

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	freshBootstrapAnnotationPrefix = "sfs-subdir-fresh-bootstrap-"
	freshBootstrapPlanAnnotation   = freshBootstrapAnnotationPrefix + "plan"
	freshBootstrapSchemaVersion    = "1"
	freshBootstrapPhasePrepared    = "Prepared"
)

// FreshBootstrapParent is one exact parent authorization inside the durable
// fresh-installation plan. The attempt ID becomes the ordinary per-parent
// bootstrap journal before the immutable owner claim is installed.
type FreshBootstrapParent struct {
	ParentFilesystemID       string `json:"parentFilesystemID"`
	AttemptID                string `json:"attemptID"`
	EmptyInventoryObservedAt string `json:"emptyInventoryObservedAt"`
}

// FreshBootstrapPlan is the bounded Lease-backed authorization written after
// every configured parent has a conclusively empty provider inventory and
// before the first attach call. It is not ownership evidence. Its exact holder
// and controller-Instance binding permits only same-Pod, same-Instance resume;
// a replacement holder must follow the normal fencing and recovery policy.
type FreshBootstrapPlan struct {
	SchemaVersion        string                 `json:"schemaVersion"`
	Phase                string                 `json:"phase"`
	InstallationID       string                 `json:"installationID"`
	ActiveClusterUID     string                 `json:"activeClusterUID"`
	HolderPodUID         string                 `json:"holderPodUID"`
	ControllerNodeID     string                 `json:"controllerNodeID"`
	ControllerInstanceID string                 `json:"controllerInstanceID"`
	ControllerZone       string                 `json:"controllerZone"`
	Parents              []FreshBootstrapParent `json:"parents"`
}

// NewFreshBootstrapParent validates and canonicalizes one empty-inventory
// observation before it can authorize a provider attachment.
func NewFreshBootstrapParent(parentID, attemptID string, observedAt time.Time) (FreshBootstrapParent, error) {
	parent := FreshBootstrapParent{
		ParentFilesystemID:       parentID,
		AttemptID:                attemptID,
		EmptyInventoryObservedAt: observedAt.UTC().Format(time.RFC3339Nano),
	}
	if err := parent.Validate(); err != nil {
		return FreshBootstrapParent{}, err
	}
	return parent, nil
}

// Validate checks one exact parent authorization.
func (parent FreshBootstrapParent) Validate() error {
	if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(parent.AttemptID); err != nil {
		return fmt.Errorf("fresh bootstrap attempt ID: %w", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, parent.EmptyInventoryObservedAt)
	if err != nil || !strings.HasSuffix(parent.EmptyInventoryObservedAt, "Z") || parsed.UTC().Format(time.RFC3339Nano) != parent.EmptyInventoryObservedAt {
		return fmt.Errorf("fresh bootstrap empty-inventory timestamp must be canonical RFC 3339 UTC")
	}
	return nil
}

// NewFreshBootstrapPlan binds an exact, sorted parent set to the current
// provisional holder. Parent order is canonical so exact replay is stable.
func NewFreshBootstrapPlan(holder HolderEvidence, parents []FreshBootstrapParent) (FreshBootstrapPlan, error) {
	if err := holder.Validate(); err != nil {
		return FreshBootstrapPlan{}, err
	}
	canonicalParents := slices.Clone(parents)
	slices.SortFunc(canonicalParents, func(left, right FreshBootstrapParent) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	plan := FreshBootstrapPlan{
		SchemaVersion: freshBootstrapSchemaVersion, Phase: freshBootstrapPhasePrepared,
		InstallationID: holder.InstallationID, ActiveClusterUID: holder.ActiveClusterUID,
		HolderPodUID: holder.PodUID, ControllerNodeID: holder.CSINodeID,
		ControllerInstanceID: holder.InstanceID, ControllerZone: holder.Zone,
		Parents: canonicalParents,
	}
	if err := plan.ValidateForHolder(holder); err != nil {
		return FreshBootstrapPlan{}, err
	}
	return plan, nil
}

// Validate checks the closed plan independently of a running holder.
func (plan FreshBootstrapPlan) Validate() error {
	if plan.SchemaVersion != freshBootstrapSchemaVersion || plan.Phase != freshBootstrapPhasePrepared {
		return fmt.Errorf("fresh bootstrap schema %q or phase %q is unsupported", plan.SchemaVersion, plan.Phase)
	}
	if err := volume.ValidateInstallationID(plan.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(plan.ActiveClusterUID); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(plan.HolderPodUID); err != nil {
		return fmt.Errorf("fresh bootstrap holder Pod UID: %w", err)
	}
	if err := volume.ValidateNodeID(plan.ControllerNodeID); err != nil {
		return fmt.Errorf("fresh bootstrap controller node ID: %w", err)
	}
	parts := strings.Split(plan.ControllerNodeID, "/")
	if plan.ControllerZone != parts[0] || plan.ControllerInstanceID != parts[1] {
		return fmt.Errorf("fresh bootstrap controller node ID disagrees with recorded zone or Instance")
	}
	if len(plan.Parents) == 0 {
		return fmt.Errorf("fresh bootstrap plan has no parent")
	}
	previousID := ""
	attemptIDs := make(map[string]struct{}, len(plan.Parents))
	for index, parent := range plan.Parents {
		if err := parent.Validate(); err != nil {
			return fmt.Errorf("fresh bootstrap parent %d: %w", index, err)
		}
		if parent.ParentFilesystemID <= previousID {
			return fmt.Errorf("fresh bootstrap parents are not unique and strictly sorted")
		}
		if _, duplicate := attemptIDs[parent.AttemptID]; duplicate {
			return fmt.Errorf("fresh bootstrap attempt %q is duplicated", parent.AttemptID)
		}
		attemptIDs[parent.AttemptID] = struct{}{}
		previousID = parent.ParentFilesystemID
	}
	return nil
}

// ValidateForHolder additionally proves exact same-Pod and same-Instance
// identity. This is required before attach, resume, promotion, or conversion to
// the ordinary single-parent bootstrap journal.
func (plan FreshBootstrapPlan) ValidateForHolder(holder HolderEvidence) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if err := holder.Validate(); err != nil {
		return err
	}
	if plan.InstallationID != holder.InstallationID || plan.ActiveClusterUID != holder.ActiveClusterUID ||
		plan.HolderPodUID != holder.PodUID || plan.ControllerNodeID != holder.CSINodeID ||
		plan.ControllerInstanceID != holder.InstanceID || plan.ControllerZone != holder.Zone {
		return fmt.Errorf("fresh bootstrap plan differs from current holder identity")
	}
	return nil
}

// Parent returns the exact planned parent entry.
func (plan FreshBootstrapPlan) Parent(parentID string) (FreshBootstrapParent, bool) {
	index, present := slices.BinarySearchFunc(plan.Parents, parentID, func(parent FreshBootstrapParent, target string) int {
		return strings.Compare(parent.ParentFilesystemID, target)
	})
	if !present {
		return FreshBootstrapParent{}, false
	}
	return plan.Parents[index], true
}

func (plan FreshBootstrapPlan) annotationValue() (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("encode fresh bootstrap plan: %w", err)
	}
	return string(encoded), nil
}

// ParseFreshBootstrapPlan rejects partial, non-canonical, and unknown schema
// data instead of treating an unreadable authorization as absent.
func ParseFreshBootstrapPlan(annotations map[string]string) (FreshBootstrapPlan, bool, error) {
	found := 0
	for key := range annotations {
		if !strings.HasPrefix(key, freshBootstrapAnnotationPrefix) {
			continue
		}
		if key != freshBootstrapPlanAnnotation {
			return FreshBootstrapPlan{}, true, fmt.Errorf("unknown fresh bootstrap annotation %q", key)
		}
		found++
	}
	if found == 0 {
		return FreshBootstrapPlan{}, false, nil
	}
	if found != 1 {
		return FreshBootstrapPlan{}, true, fmt.Errorf("fresh bootstrap plan has %d annotations, want 1", found)
	}
	raw := annotations[freshBootstrapPlanAnnotation]
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var plan FreshBootstrapPlan
	if err := decoder.Decode(&plan); err != nil {
		return FreshBootstrapPlan{}, true, fmt.Errorf("decode fresh bootstrap plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return FreshBootstrapPlan{}, true, fmt.Errorf("decode fresh bootstrap plan trailing data: %w", err)
	}
	if err := plan.Validate(); err != nil {
		return FreshBootstrapPlan{}, true, err
	}
	canonical, err := plan.annotationValue()
	if err != nil {
		return FreshBootstrapPlan{}, true, err
	}
	if raw != canonical {
		return FreshBootstrapPlan{}, true, fmt.Errorf("fresh bootstrap plan is not canonical JSON")
	}
	return plan, true, nil
}

// ApplyFreshBootstrapPlan installs one plan or accepts only its exact replay.
func ApplyFreshBootstrapPlan(annotations map[string]string, plan FreshBootstrapPlan) (map[string]string, error) {
	value, err := plan.annotationValue()
	if err != nil {
		return nil, err
	}
	if existing, present, err := ParseFreshBootstrapPlan(annotations); err != nil {
		return nil, err
	} else if present {
		existingValue, encodeErr := existing.annotationValue()
		if encodeErr != nil {
			return nil, encodeErr
		}
		if existingValue != value {
			return nil, fmt.Errorf("another fresh bootstrap plan is already active")
		}
	}
	result := maps.Clone(annotations)
	if result == nil {
		result = map[string]string{}
	}
	result[freshBootstrapPlanAnnotation] = value
	return result, nil
}

// ClearFreshBootstrapPlan removes only the fixed plan annotation.
func ClearFreshBootstrapPlan(annotations map[string]string) map[string]string {
	result := maps.Clone(annotations)
	delete(result, freshBootstrapPlanAnnotation)
	return result
}
