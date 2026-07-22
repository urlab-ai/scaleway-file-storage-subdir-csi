package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	releasecompat "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/compatibility"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// NodeConfigGeneration returns the fixed lowercase SHA-256 generation shared
// by Helm, controller preflight, and node-plugin Pod annotations. The immutable
// driver image digest is part of the generation because a new node binary may
// have a different durable-schema reader contract even when its values are
// unchanged. Development renders use the deliberate empty digest sentinel.
// Provider display names and placement lifecycle are excluded because they do
// not alter node mount authorization.
func NodeConfigGeneration(driverName, driverImageDigest, region, nodeParentMountRoot, kubeletPath string, commercialTypes []string, pools []pool.Config) (string, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return "", err
	}
	if region == "" {
		return "", fmt.Errorf("node configuration region is empty")
	}
	if err := validateAbsoluteNormalizedPath("node parent mount root", nodeParentMountRoot); err != nil {
		return "", err
	}
	if err := validateAbsoluteNormalizedPath("kubelet path", kubeletPath); err != nil {
		return "", err
	}
	if pathsOverlap(nodeParentMountRoot, kubeletPath) {
		return "", fmt.Errorf("node parent mount root overlaps kubelet path")
	}
	if err := releasecompat.ValidateCommercialTypes(commercialTypes); err != nil {
		return "", fmt.Errorf("node commercial types: %w", err)
	}
	if err := pool.ValidateConfigs(pools); err != nil {
		return "", err
	}
	parents := make(map[string]any)
	for _, configuredPool := range pools {
		for _, parent := range configuredPool.Filesystems {
			parents[parent.ID] = map[string]any{
				"basePath": configuredPool.BasePath,
				"pool":     configuredPool.Name,
			}
		}
	}
	projection := map[string]any{
		"accessModes":              []string{string(volume.AccessModeSingleNodeWriter), string(volume.AccessModeMultiNodeMultiWriter)},
		"driverImageDigest":        driverImageDigest,
		"driverName":               driverName,
		"kubeletPath":              kubeletPath,
		"nodeParentMountRoot":      nodeParentMountRoot,
		"ownershipSchema":          volume.SchemaVersionV1,
		"parents":                  parents,
		"qualifiedCommercialTypes": commercialTypes,
		"region":                   region,
	}
	encoded, err := canonicaljson.Marshal(projection)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
