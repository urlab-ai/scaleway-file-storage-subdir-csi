package version

import "fmt"

var (
	// Version is set by release builds to the semantic release version.
	Version = "0.0.0-dev"
	// Commit is set by release builds to the complete Git commit.
	Commit = "unknown"
	// BuildDate is set by release builds to an RFC 3339 UTC timestamp.
	BuildDate = "unknown"
)

// String returns a stable, human-readable build identity.
func String() string {
	return fmt.Sprintf("%s (commit=%s, built=%s)", Version, Commit, BuildDate)
}
