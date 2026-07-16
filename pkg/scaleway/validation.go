package scaleway

import (
	"fmt"
	"regexp"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

var providerRegionPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

func validateProviderRegion(region string) error {
	if !providerRegionPattern.MatchString(region) {
		return fmt.Errorf("provider region %q is invalid", region)
	}
	return nil
}

func validateProviderScope(region, projectID string) error {
	if err := validateProviderRegion(region); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(projectID); err != nil {
		return fmt.Errorf("provider Project ID: %w", err)
	}
	return nil
}

func validateTargetInRegion(target Target, region string) error {
	parsed, err := ParseNodeID(target.Zone + "/" + target.ServerID)
	if err != nil || parsed != target {
		return fmt.Errorf("provider target %q/%q is invalid", target.Zone, target.ServerID)
	}
	if !strings.HasPrefix(target.Zone, region+"-") {
		return fmt.Errorf("provider target zone %q does not belong to region %q", target.Zone, region)
	}
	return nil
}
