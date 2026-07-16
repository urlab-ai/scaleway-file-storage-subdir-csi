package recovery

import (
	"fmt"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// RestoredCheckpointState is the live restore-stable projection recomputed
// after Kubernetes objects have been recreated and configured parents mounted.
// Source UIDs and resourceVersions are intentionally absent.
type RestoredCheckpointState struct {
	DriverName        string
	InstallationID    string
	ActiveClusterUID  string
	ChartVersion      string
	Images            []ImageDigest
	KubernetesObjects ObjectInventorySummary
	Parents           []ParentInventory
}

// VerifyRestoredCheckpoint proves that the restored logical object set and
// every current parent inventory exactly match one completed checkpoint. It
// sorts isolated copies because ordering is a canonical encoding concern, not
// live-state identity; duplicates remain invalid.
func VerifyRestoredCheckpoint(manifest CheckpointManifest, current RestoredCheckpointState) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	if err := volume.ValidateDriverName(current.DriverName); err != nil {
		return err
	}
	if err := volume.ValidateInstallationID(current.InstallationID); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(current.ActiveClusterUID); err != nil {
		return err
	}
	if current.DriverName != manifest.DriverName || current.ActiveClusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(current.InstallationID)) != manifest.InstallationIDHash {
		return fmt.Errorf("restored driver, installation, or cluster identity differs from checkpoint")
	}
	if current.ChartVersion != manifest.ChartVersion {
		return fmt.Errorf("restored chart version %q differs from checkpoint %q", current.ChartVersion, manifest.ChartVersion)
	}

	images := slices.Clone(current.Images)
	slices.SortFunc(images, func(left, right ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	parents := slices.Clone(current.Parents)
	slices.SortFunc(parents, func(left, right ParentInventory) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	// Reuse the closed manifest validator for runtime inventory field bounds,
	// digest syntax, and duplicate detection while preserving checkpoint holder
	// and Lease evidence unchanged.
	candidate := manifest
	candidate.Images = images
	candidate.KubernetesObjects = current.KubernetesObjects
	candidate.Parents = parents
	if err := candidate.Validate(); err != nil {
		return fmt.Errorf("restored checkpoint projection: %w", err)
	}
	if !slices.Equal(images, manifest.Images) {
		return fmt.Errorf("restored rendered image set differs from checkpoint")
	}
	if current.KubernetesObjects != manifest.KubernetesObjects {
		return fmt.Errorf("restored Kubernetes object count or aggregate differs from checkpoint")
	}
	if !slices.Equal(parents, manifest.Parents) {
		return fmt.Errorf("restored parent inventory set or aggregate differs from checkpoint")
	}
	return nil
}
