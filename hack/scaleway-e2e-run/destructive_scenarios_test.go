package main

import (
	"regexp"
	"testing"
)

func TestRandomUUIDV4IsCanonicalAndUnique(t *testing.T) {
	pattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	first, err := randomUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	second, err := randomUUIDv4()
	if err != nil {
		t.Fatal(err)
	}
	if !pattern.MatchString(first) || !pattern.MatchString(second) || first == second {
		t.Fatalf("random UUIDs are not distinct canonical v4 values: %q / %q", first, second)
	}
}
