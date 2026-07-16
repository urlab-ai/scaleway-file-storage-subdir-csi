package coordination

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

var (
	// ErrAbnormalTakeoverApprovalRequired preserves a different uncleared holder
	// regardless of Lease expiry.
	ErrAbnormalTakeoverApprovalRequired = errors.New("different uncleared Lease holder requires abnormal takeover approval")
	// ErrMissingLeaseRecoveryRequired prevents an empty/recreated Lease from
	// authorizing mutation when durable driver state exists.
	ErrMissingLeaseRecoveryRequired = errors.New("empty Lease with durable state requires missing-Lease recovery")
	// ErrGracefulReleaseUnsafe means active mutation, bootstrap, checkpoint, or
	// holder evidence makes a normal release unsafe.
	ErrGracefulReleaseUnsafe = errors.New("graceful Lease release preconditions are not satisfied")
)

// LeaseSnapshot is the minimal resource-versioned state a Kubernetes adapter
// must compare-and-swap atomically.
type LeaseSnapshot struct {
	UID             string
	ResourceVersion string
	HolderIdentity  string
	Annotations     map[string]string
}

// AcquisitionMode describes why an acquisition is authorized. Approval-based
// modes are deliberately not returned by the automatic planner.
type AcquisitionMode string

const (
	AcquisitionSameHolder          AcquisitionMode = "same-holder"
	AcquisitionGracefulHandoff     AcquisitionMode = "graceful-handoff"
	AcquisitionFreshInstallation   AcquisitionMode = "fresh-installation"
	AcquisitionProvisionalRecovery AcquisitionMode = "provisional-recovery"
	AcquisitionApprovedTakeover    AcquisitionMode = "approved-abnormal-takeover"
	AcquisitionApprovedRecovery    AcquisitionMode = "approved-missing-lease-recovery"
)

// AcquisitionPlan is a pure compare-and-swap projection. Provisional recovery
// is read-only and must not be treated as active mutation leadership.
type AcquisitionPlan struct {
	Mode            AcquisitionMode
	HolderIdentity  string
	Annotations     map[string]string
	MutationAllowed bool
}

// PlanAutomaticAcquisition enforces the v1 non-HA handoff rules without using
// Lease expiry or apparent Kubernetes-object absence as a storage fence. An
// empty Lease is always provisional until the separate all-parent discovery
// proof and promotion CAS succeed.
func PlanAutomaticAcquisition(snapshot LeaseSnapshot, candidate HolderEvidence, durableStateExists bool) (AcquisitionPlan, error) {
	if err := validateLeaseSnapshot(snapshot); err != nil {
		return AcquisitionPlan{}, err
	}
	if err := candidate.Validate(); err != nil {
		return AcquisitionPlan{}, err
	}
	previous, previousPresent, err := ParseHolderEvidence(snapshot.Annotations)
	if err != nil {
		return AcquisitionPlan{}, err
	}
	release, releasePresent, err := ParseGracefulRelease(snapshot.Annotations)
	if err != nil {
		return AcquisitionPlan{}, err
	}
	_, bootstrapPresent, err := ParseBootstrapAttempt(snapshot.Annotations)
	if err != nil {
		return AcquisitionPlan{}, err
	}

	if snapshot.HolderIdentity != "" {
		if !previousPresent || snapshot.HolderIdentity != previous.PodUID {
			return AcquisitionPlan{}, fmt.Errorf("non-empty holder identity lacks matching complete holder evidence")
		}
		_, discoveryPresent, err := ParseDiscoveryMarker(snapshot.Annotations, previous)
		if err != nil {
			return AcquisitionPlan{}, err
		}
		if snapshot.HolderIdentity != candidate.PodUID {
			return AcquisitionPlan{}, ErrAbnormalTakeoverApprovalRequired
		}
		if releasePresent {
			return AcquisitionPlan{}, fmt.Errorf("non-empty holder cannot carry an unconsumed graceful-release marker")
		}
		if discoveryPresent {
			return AcquisitionPlan{
				Mode: AcquisitionProvisionalRecovery, HolderIdentity: candidate.PodUID,
				Annotations: maps.Clone(snapshot.Annotations), MutationAllowed: false,
			}, ErrMissingLeaseRecoveryRequired
		}
		annotations, err := applyHolderEvidence(snapshot.Annotations, candidate, false)
		if err != nil {
			return AcquisitionPlan{}, err
		}
		return AcquisitionPlan{Mode: AcquisitionSameHolder, HolderIdentity: candidate.PodUID, Annotations: annotations, MutationAllowed: true}, nil
	}

	if releasePresent {
		if bootstrapPresent || !previousPresent {
			return AcquisitionPlan{}, fmt.Errorf("graceful handoff requires previous holder evidence and no bootstrap attempt")
		}
		if err := release.ValidateHandoff(snapshot.UID, candidate.InstallationID, candidate.ActiveClusterUID, previous); err != nil {
			return AcquisitionPlan{}, err
		}
		annotations, err := applyHolderEvidence(snapshot.Annotations, candidate, true)
		if err != nil {
			return AcquisitionPlan{}, err
		}
		return AcquisitionPlan{Mode: AcquisitionGracefulHandoff, HolderIdentity: candidate.PodUID, Annotations: annotations, MutationAllowed: true}, nil
	}
	if _, discoveryPresent, err := ParseDiscoveryMarker(snapshot.Annotations, candidate); err != nil {
		return AcquisitionPlan{}, err
	} else if discoveryPresent {
		return AcquisitionPlan{}, fmt.Errorf("empty Lease contains a provisional discovery marker")
	}

	if bootstrapPresent {
		return AcquisitionPlan{}, fmt.Errorf("empty Lease contains an unconsumed bootstrap attempt")
	}
	// Apparent absence is not conclusive until every configured parent has also
	// passed provider and filesystem discovery. Durable state and orphan holder
	// evidence use the same provisional state but can only follow the explicit
	// operator-approved recovery path because fresh discovery will reject them.
	_ = durableStateExists
	annotations, err := applyHolderEvidence(snapshot.Annotations, candidate, false)
	if err != nil {
		return AcquisitionPlan{}, err
	}
	return AcquisitionPlan{Mode: AcquisitionProvisionalRecovery, HolderIdentity: candidate.PodUID, Annotations: annotations, MutationAllowed: false}, ErrMissingLeaseRecoveryRequired
}

// PlanGracefulRelease clears the holder and installs the one-time marker only
// when the caller still owns the exact Lease and every local safety gate is
// drained.
func PlanGracefulRelease(snapshot LeaseSnapshot, current HolderEvidence, requestID string, releasedAt time.Time, inflightMutations int64, checkpointActive bool) (LeaseSnapshot, error) {
	if err := validateLeaseSnapshot(snapshot); err != nil {
		return LeaseSnapshot{}, err
	}
	if err := current.Validate(); err != nil {
		return LeaseSnapshot{}, err
	}
	preserved, present, err := ParseHolderEvidence(snapshot.Annotations)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	_, bootstrapPresent, err := ParseBootstrapAttempt(snapshot.Annotations)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	_, discoveryPresent, err := ParseDiscoveryMarker(snapshot.Annotations, current)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	if inflightMutations != 0 || checkpointActive || bootstrapPresent || discoveryPresent || !present || snapshot.HolderIdentity != current.PodUID || preserved != current {
		return LeaseSnapshot{}, ErrGracefulReleaseUnsafe
	}
	release, err := NewGracefulRelease(current, snapshot.UID, requestID, releasedAt)
	if err != nil {
		return LeaseSnapshot{}, err
	}
	releaseAnnotations, err := release.Annotations()
	if err != nil {
		return LeaseSnapshot{}, err
	}
	annotations := maps.Clone(snapshot.Annotations)
	for key, value := range releaseAnnotations {
		annotations[key] = value
	}
	return LeaseSnapshot{
		UID: snapshot.UID, ResourceVersion: snapshot.ResourceVersion,
		HolderIdentity: "", Annotations: annotations,
	}, nil
}

func validateLeaseSnapshot(snapshot LeaseSnapshot) error {
	if err := volume.ValidateOperationID(snapshot.UID); err != nil {
		return fmt.Errorf("lease UID: %w", err)
	}
	if snapshot.ResourceVersion == "" {
		return fmt.Errorf("lease resourceVersion is empty")
	}
	if snapshot.Annotations == nil {
		return fmt.Errorf("lease annotations must be an explicit map")
	}
	return nil
}

func applyHolderEvidence(annotations map[string]string, holder HolderEvidence, consumeGraceful bool) (map[string]string, error) {
	result := maps.Clone(annotations)
	if consumeGraceful {
		result = ClearGracefulReleaseAnnotations(result)
	}
	holderAnnotations, err := holder.Annotations()
	if err != nil {
		return nil, err
	}
	for key, value := range holderAnnotations {
		result[key] = value
	}
	return result, nil
}
