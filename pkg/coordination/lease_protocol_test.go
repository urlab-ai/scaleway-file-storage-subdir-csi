package coordination

import (
	"errors"
	"testing"
	"time"
)

func leaseWithHolder(t *testing.T, holder HolderEvidence) LeaseSnapshot {
	t.Helper()
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("holder.Annotations() error = %v", err)
	}
	return LeaseSnapshot{
		UID: "55555555-5555-4555-8555-555555555555", ResourceVersion: "10",
		HolderIdentity: holder.PodUID, Annotations: annotations,
	}
}

func TestAutomaticAcquisitionAllowsSameHolderButNeverDifferentExpiredHolder(t *testing.T) {
	holder := validHolderEvidence(t)
	snapshot := leaseWithHolder(t, holder)
	plan, err := PlanAutomaticAcquisition(snapshot, holder, true)
	if err != nil {
		t.Fatalf("PlanAutomaticAcquisition(same holder) error = %v", err)
	}
	if plan.Mode != AcquisitionSameHolder || !plan.MutationAllowed {
		t.Fatalf("same-holder plan = %#v", plan)
	}
	candidate := holder
	candidate.PodUID = "66666666-6666-4666-8666-666666666666"
	if _, err := PlanAutomaticAcquisition(snapshot, candidate, true); !errors.Is(err, ErrAbnormalTakeoverApprovalRequired) {
		t.Fatalf("PlanAutomaticAcquisition(different holder) error = %v", err)
	}
}

func TestAutomaticAcquisitionNeverPromotesSameHolderProvisionalMarker(t *testing.T) {
	holder := validHolderEvidence(t)
	snapshot := leaseWithHolder(t, holder)
	marker, err := NewDiscoveryMarker(holder, time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewDiscoveryMarker() error = %v", err)
	}
	snapshot.Annotations, err = ApplyDiscoveryMarker(snapshot.Annotations, marker, holder)
	if err != nil {
		t.Fatalf("ApplyDiscoveryMarker() error = %v", err)
	}
	plan, err := PlanAutomaticAcquisition(snapshot, holder, false)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || plan.Mode != AcquisitionProvisionalRecovery || plan.MutationAllowed {
		t.Fatalf("PlanAutomaticAcquisition(provisional same holder) = %#v, %v", plan, err)
	}
}

func TestGracefulReleaseAndOneTimeHandoff(t *testing.T) {
	holder := validHolderEvidence(t)
	snapshot := leaseWithHolder(t, holder)
	released, err := PlanGracefulRelease(snapshot, holder, "66666666-6666-4666-8666-666666666666", time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC), 0, false)
	if err != nil {
		t.Fatalf("PlanGracefulRelease() error = %v", err)
	}
	if released.HolderIdentity != "" {
		t.Fatal("graceful release did not clear holder identity")
	}
	candidate := holder
	candidate.PodUID = "77777777-7777-4777-8777-777777777777"
	plan, err := PlanAutomaticAcquisition(released, candidate, true)
	if err != nil {
		t.Fatalf("PlanAutomaticAcquisition(graceful handoff) error = %v", err)
	}
	if plan.Mode != AcquisitionGracefulHandoff || !plan.MutationAllowed {
		t.Fatalf("graceful handoff plan = %#v", plan)
	}
	if _, present, err := ParseGracefulRelease(plan.Annotations); err != nil || present {
		t.Fatalf("consumed graceful marker = present %v, error %v", present, err)
	}
	consumed := released
	consumed.Annotations = plan.Annotations
	if _, err := PlanAutomaticAcquisition(consumed, candidate, true); !errors.Is(err, ErrMissingLeaseRecoveryRequired) {
		t.Fatalf("reusing consumed marker error = %v", err)
	}
}

func TestGracefulReleaseRejectsInflightCheckpointAndBootstrap(t *testing.T) {
	holder := validHolderEvidence(t)
	snapshot := leaseWithHolder(t, holder)
	requestID := "66666666-6666-4666-8666-666666666666"
	now := time.Date(2026, 7, 13, 16, 0, 0, 0, time.UTC)
	if _, err := PlanGracefulRelease(snapshot, holder, requestID, now, 1, false); !errors.Is(err, ErrGracefulReleaseUnsafe) {
		t.Fatalf("PlanGracefulRelease(inflight) error = %v", err)
	}
	if _, err := PlanGracefulRelease(snapshot, holder, requestID, now, 0, true); !errors.Is(err, ErrGracefulReleaseUnsafe) {
		t.Fatalf("PlanGracefulRelease(checkpoint) error = %v", err)
	}
	attempt := validBootstrapAttempt(t)
	bootstrap, err := attempt.Annotations()
	if err != nil {
		t.Fatalf("bootstrap.Annotations() error = %v", err)
	}
	for key, value := range bootstrap {
		snapshot.Annotations[key] = value
	}
	if _, err := PlanGracefulRelease(snapshot, holder, requestID, now, 0, false); !errors.Is(err, ErrGracefulReleaseUnsafe) {
		t.Fatalf("PlanGracefulRelease(bootstrap) error = %v", err)
	}
}

func TestEmptyLeaseIsAlwaysProvisionalUntilCompleteDiscovery(t *testing.T) {
	candidate := validHolderEvidence(t)
	snapshot := LeaseSnapshot{
		UID: "55555555-5555-4555-8555-555555555555", ResourceVersion: "1",
		Annotations: map[string]string{},
	}
	plan, err := PlanAutomaticAcquisition(snapshot, candidate, false)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || plan.Mode != AcquisitionProvisionalRecovery || plan.MutationAllowed {
		t.Fatalf("apparently empty PlanAutomaticAcquisition() = %#v, %v", plan, err)
	}
	plan, err = PlanAutomaticAcquisition(snapshot, candidate, true)
	if !errors.Is(err, ErrMissingLeaseRecoveryRequired) || plan.Mode != AcquisitionProvisionalRecovery || plan.MutationAllowed {
		t.Fatalf("recovery PlanAutomaticAcquisition() = %#v, %v", plan, err)
	}
	evidence, present, evidenceErr := ParseHolderEvidence(plan.Annotations)
	if evidenceErr != nil || !present || evidence != candidate || plan.HolderIdentity != candidate.PodUID {
		t.Fatalf("provisional holder evidence = %#v, present=%v, error=%v", evidence, present, evidenceErr)
	}
}
