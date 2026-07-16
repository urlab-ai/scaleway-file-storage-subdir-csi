package version

import "testing"

func TestStringIncludesEveryBuildField(t *testing.T) {
	originalVersion, originalCommit, originalBuildDate := Version, Commit, BuildDate
	t.Cleanup(func() {
		Version, Commit, BuildDate = originalVersion, originalCommit, originalBuildDate
	})

	Version = "1.2.3"
	Commit = "0123456789abcdef0123456789abcdef01234567"
	BuildDate = "2026-07-12T00:00:00Z"

	const want = "1.2.3 (commit=0123456789abcdef0123456789abcdef01234567, built=2026-07-12T00:00:00Z)"
	if got := String(); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
