package coordination

import (
	"strings"
	"testing"
	"time"
)

func validFreshBootstrapPlan(t *testing.T) FreshBootstrapPlan {
	t.Helper()
	parent, err := NewFreshBootstrapParent(
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("NewFreshBootstrapParent() error = %v", err)
	}
	plan, err := NewFreshBootstrapPlan(validHolderEvidence(t), []FreshBootstrapParent{parent})
	if err != nil {
		t.Fatalf("NewFreshBootstrapPlan() error = %v", err)
	}
	return plan
}

func TestFreshBootstrapPlanRoundTripAndExactReplay(t *testing.T) {
	want := validFreshBootstrapPlan(t)
	annotations, err := ApplyFreshBootstrapPlan(map[string]string{"unrelated": "preserved"}, want)
	if err != nil {
		t.Fatalf("ApplyFreshBootstrapPlan() error = %v", err)
	}
	got, present, err := ParseFreshBootstrapPlan(annotations)
	if err != nil || !present || got.annotationMustEqual(want) == false {
		t.Fatalf("ParseFreshBootstrapPlan() = %#v, present=%v, error=%v", got, present, err)
	}
	if _, err := ApplyFreshBootstrapPlan(annotations, want); err != nil {
		t.Fatalf("ApplyFreshBootstrapPlan(exact replay) error = %v", err)
	}
	cleared := ClearFreshBootstrapPlan(annotations)
	if cleared["unrelated"] != "preserved" {
		t.Fatal("ClearFreshBootstrapPlan() removed an unrelated annotation")
	}
	if _, present, err := ParseFreshBootstrapPlan(cleared); err != nil || present {
		t.Fatalf("ParseFreshBootstrapPlan(cleared) = present=%v, error=%v", present, err)
	}
}

func TestFreshBootstrapPlanRejectsUnknownNonCanonicalAndChangedHolder(t *testing.T) {
	plan := validFreshBootstrapPlan(t)
	annotations, err := ApplyFreshBootstrapPlan(nil, plan)
	if err != nil {
		t.Fatalf("ApplyFreshBootstrapPlan() error = %v", err)
	}
	annotations[freshBootstrapAnnotationPrefix+"unknown"] = "x"
	if _, present, err := ParseFreshBootstrapPlan(annotations); err == nil || !present {
		t.Fatalf("ParseFreshBootstrapPlan(unknown) = present=%v, error=%v", present, err)
	}
	annotations, err = ApplyFreshBootstrapPlan(nil, plan)
	if err != nil {
		t.Fatalf("ApplyFreshBootstrapPlan() error = %v", err)
	}
	annotations[freshBootstrapPlanAnnotation] += " "
	if _, present, err := ParseFreshBootstrapPlan(annotations); err == nil || !present || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("ParseFreshBootstrapPlan(non-canonical) = present=%v, error=%v", present, err)
	}
	changed := validHolderEvidence(t)
	changed.PodUID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	if err := plan.ValidateForHolder(changed); err == nil {
		t.Fatal("ValidateForHolder(changed Pod) error = nil")
	}
}

func TestFreshBootstrapPlanCanonicalizesParentsAndRejectsDuplicateAttempts(t *testing.T) {
	first, err := NewFreshBootstrapParent(
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		time.Date(2026, 7, 14, 10, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFreshBootstrapParent(
		"99999999-9999-4999-8999-999999999999",
		"88888888-8888-4888-8888-888888888888",
		time.Date(2026, 7, 14, 10, 0, 1, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewFreshBootstrapPlan(validHolderEvidence(t), []FreshBootstrapParent{first, second})
	if err != nil {
		t.Fatalf("NewFreshBootstrapPlan() error = %v", err)
	}
	if plan.Parents[0].ParentFilesystemID != second.ParentFilesystemID {
		t.Fatalf("canonical parent order = %#v", plan.Parents)
	}
	plan.Parents[1].AttemptID = plan.Parents[0].AttemptID
	if err := plan.Validate(); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("Validate(duplicate attempt) error = %v", err)
	}
}

func (plan FreshBootstrapPlan) annotationMustEqual(other FreshBootstrapPlan) bool {
	left, leftErr := plan.annotationValue()
	right, rightErr := other.annotationValue()
	return leftErr == nil && rightErr == nil && left == right
}
