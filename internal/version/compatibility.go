package version

import (
	"fmt"

	releasecompat "scaleway-sfs-subdir-csi/internal/compatibility"
)

// QualifiedCommercialTypes is set by release builds to the canonical
// comma-separated real-E2E-qualified Scaleway Instance type allowlist.
var QualifiedCommercialTypes string

// CommercialTypes returns an isolated validated copy of the embedded release
// allowlist. Development builds intentionally have no embedded support claim.
func CommercialTypes() ([]string, error) {
	if QualifiedCommercialTypes == "" {
		return nil, fmt.Errorf("build has no embedded commercial type allowlist")
	}
	return releasecompat.ParseCommercialTypes(QualifiedCommercialTypes)
}
