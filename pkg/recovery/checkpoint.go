package recovery

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const maxCheckpointManifestBytes = 1024 * 1024

// ImageDigest binds one rendered workload image to immutable release content.
type ImageDigest struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// CheckpointManifest is the bounded in-cluster recovery envelope. Detailed
// per-object inventories are intentionally external to keep the Secret size
// O(number of parents plus images).
type CheckpointManifest struct {
	SchemaVersion       string                      `json:"schemaVersion"`
	CheckpointRequestID string                      `json:"checkpointRequestID"`
	DriverName          string                      `json:"driverName"`
	BackupTimestamp     string                      `json:"backupTimestamp"`
	ActiveClusterUID    string                      `json:"activeClusterUID"`
	InstallationIDHash  string                      `json:"installationIDHash"`
	ChartVersion        string                      `json:"chartVersion"`
	Images              []ImageDigest               `json:"images"`
	LeadershipLeaseName string                      `json:"leadershipLeaseName"`
	LeadershipLeaseUID  string                      `json:"leadershipLeaseUID"`
	HolderEvidence      coordination.HolderEvidence `json:"holderEvidence"`
	KubernetesObjects   ObjectInventorySummary      `json:"kubernetesObjects"`
	Parents             []ParentInventory           `json:"parents"`
}

// NewCheckpointManifest constructs a sorted, closed manifest after detailed
// inventories have already been captured in one uninterrupted quiesced window.
func NewCheckpointManifest(requestID, driverName, installationID, clusterUID, chartVersion, leaseUID string, holder coordination.HolderEvidence, backupTimestamp time.Time, images []ImageDigest, objects ObjectInventorySummary, parents []ParentInventory) (CheckpointManifest, error) {
	manifest := CheckpointManifest{
		SchemaVersion: volume.SchemaVersionV1, CheckpointRequestID: requestID,
		DriverName: driverName, BackupTimestamp: backupTimestamp.UTC().Format(time.RFC3339Nano),
		ActiveClusterUID: clusterUID, InstallationIDHash: SHA256Digest([]byte(installationID)),
		ChartVersion: chartVersion, Images: slices.Clone(images),
		LeadershipLeaseName: volume.LeadershipLeaseNameV1, LeadershipLeaseUID: leaseUID,
		HolderEvidence: holder, KubernetesObjects: objects, Parents: slices.Clone(parents),
	}
	slices.SortFunc(manifest.Images, func(left, right ImageDigest) int { return strings.Compare(left.Name, right.Name) })
	slices.SortFunc(manifest.Parents, func(left, right ParentInventory) int {
		return strings.Compare(left.ParentFilesystemID, right.ParentFilesystemID)
	})
	if err := manifest.Validate(); err != nil {
		return CheckpointManifest{}, err
	}
	if _, err := EncodeCheckpointManifest(manifest); err != nil {
		return CheckpointManifest{}, err
	}
	return manifest, nil
}

// Validate authenticates all bounded fields and deterministic ordering.
func (manifest CheckpointManifest) Validate() error {
	if manifest.SchemaVersion != volume.SchemaVersionV1 {
		return fmt.Errorf("checkpoint schema version %q is unsupported", manifest.SchemaVersion)
	}
	if err := volume.ValidateOperationID(manifest.CheckpointRequestID); err != nil {
		return fmt.Errorf("checkpoint request ID: %w", err)
	}
	if err := volume.ValidateDriverName(manifest.DriverName); err != nil {
		return err
	}
	if err := validateCheckpointTimestamp(manifest.BackupTimestamp); err != nil {
		return err
	}
	if err := volume.ValidateClusterUID(manifest.ActiveClusterUID); err != nil {
		return err
	}
	if !validSHA256Digest(manifest.InstallationIDHash) {
		return fmt.Errorf("installation ID hash is malformed")
	}
	if !utf8.ValidString(manifest.ChartVersion) || len(manifest.ChartVersion) == 0 || len(manifest.ChartVersion) > 128 || strings.ContainsAny(manifest.ChartVersion, "\x00\r\n") {
		return fmt.Errorf("chart version must contain 1 to 128 safe UTF-8 bytes")
	}
	if manifest.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("checkpoint Lease name %q differs from fixed v1 name", manifest.LeadershipLeaseName)
	}
	if err := volume.ValidateOperationID(manifest.LeadershipLeaseUID); err != nil {
		return fmt.Errorf("checkpoint Lease UID: %w", err)
	}
	if err := manifest.HolderEvidence.Validate(); err != nil {
		return fmt.Errorf("checkpoint holder evidence: %w", err)
	}
	if manifest.HolderEvidence.ActiveClusterUID != manifest.ActiveClusterUID || SHA256Digest([]byte(manifest.HolderEvidence.InstallationID)) != manifest.InstallationIDHash {
		return fmt.Errorf("checkpoint holder identity disagrees with manifest installation or cluster")
	}
	if !validSHA256Digest(manifest.KubernetesObjects.AggregateSHA256) {
		return fmt.Errorf("kubernetes object aggregate SHA-256 is malformed")
	}
	if len(manifest.Images) == 0 {
		return fmt.Errorf("checkpoint must include at least one rendered image digest")
	}
	for index, image := range manifest.Images {
		if !utf8.ValidString(image.Name) || len(image.Name) == 0 || len(image.Name) > 128 || strings.ContainsAny(image.Name, "\x00\r\n") || !validSHA256Digest(image.Digest) {
			return fmt.Errorf("checkpoint image %d is invalid", index)
		}
		if index > 0 && manifest.Images[index-1].Name >= image.Name {
			return fmt.Errorf("checkpoint images are unsorted or duplicated")
		}
	}
	if len(manifest.Parents) == 0 {
		return fmt.Errorf("checkpoint must include at least one configured parent inventory")
	}
	for index, parent := range manifest.Parents {
		if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
			return err
		}
		if !validSHA256Digest(parent.ParentOwnerSHA256) || !validSHA256Digest(parent.AggregateSHA256) {
			return fmt.Errorf("checkpoint parent %q has malformed digest", parent.ParentFilesystemID)
		}
		if index > 0 && manifest.Parents[index-1].ParentFilesystemID >= parent.ParentFilesystemID {
			return fmt.Errorf("checkpoint parents are unsorted or duplicated")
		}
	}
	return nil
}

// EncodeCheckpointManifest returns canonical JSON suitable for the fixed
// checkpoint Secret's sole checkpoint.json data key.
func EncodeCheckpointManifest(manifest CheckpointManifest) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	encoded, err := canonicaljson.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > maxCheckpointManifestBytes {
		return nil, fmt.Errorf("checkpoint manifest must contain 1 to %d bytes", maxCheckpointManifestBytes)
	}
	return encoded, nil
}

// DecodeCheckpointManifest rejects unknown fields and non-canonical or invalid
// recovery envelopes.
func DecodeCheckpointManifest(data []byte) (CheckpointManifest, error) {
	if len(data) == 0 || len(data) > maxCheckpointManifestBytes {
		return CheckpointManifest{}, fmt.Errorf("checkpoint manifest must contain 1 to %d bytes", maxCheckpointManifestBytes)
	}
	var manifest CheckpointManifest
	if err := strictjson.Decode(data, &manifest); err != nil {
		return CheckpointManifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return CheckpointManifest{}, err
	}
	canonical, err := canonicaljson.Marshal(manifest)
	if err != nil {
		return CheckpointManifest{}, err
	}
	if !bytes.Equal(canonical, data) {
		return CheckpointManifest{}, fmt.Errorf("checkpoint manifest JSON is not canonical")
	}
	return manifest, nil
}

// CheckpointManifestSHA256 validates and hashes the complete canonical bytes.
func CheckpointManifestSHA256(manifest CheckpointManifest) (string, error) {
	encoded, err := EncodeCheckpointManifest(manifest)
	if err != nil {
		return "", err
	}
	return SHA256Digest(encoded), nil
}

func validateCheckpointTimestamp(value string) error {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !strings.HasSuffix(value, "Z") || parsed.UTC().Format(time.RFC3339Nano) != value {
		return fmt.Errorf("checkpoint backup timestamp must be canonical RFC 3339 UTC")
	}
	return nil
}
