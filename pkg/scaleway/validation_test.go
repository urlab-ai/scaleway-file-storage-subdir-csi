package scaleway

import "testing"

func TestValidateProviderScopeAndTarget(t *testing.T) {
	projectID := "22222222-2222-4222-8222-222222222222"
	target := Target{Zone: "fr-par-1", ServerID: "33333333-3333-4333-8333-333333333333"}
	if err := validateProviderScope("fr-par", projectID); err != nil {
		t.Fatalf("validateProviderScope() error = %v", err)
	}
	if err := validateTargetInRegion(target, "fr-par"); err != nil {
		t.Fatalf("validateTargetInRegion() error = %v", err)
	}
	for _, scope := range [][2]string{{"fr/par", projectID}, {"fr-par", "project"}} {
		if err := validateProviderScope(scope[0], scope[1]); err == nil {
			t.Fatalf("validateProviderScope(%q, %q) error = nil", scope[0], scope[1])
		}
	}
	for _, candidate := range []Target{
		{Zone: "nl-ams-1", ServerID: target.ServerID},
		{Zone: "fr-par-1/other", ServerID: target.ServerID},
		{Zone: target.Zone, ServerID: "bad/id"},
	} {
		if err := validateTargetInRegion(candidate, "fr-par"); err == nil {
			t.Fatalf("validateTargetInRegion(%#v) error = nil", candidate)
		}
	}
}
