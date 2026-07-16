package recovery

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// ParentOwnershipExport is the exact live configured-parent evidence captured
// beside a checkpoint package. Records are keyed by normalized relative path;
// neither directory scanning heuristics nor missing historical parents are
// accepted by this verifier.
type ParentOwnershipExport struct {
	ParentFilesystemID string
	ParentOwnerBytes   []byte
	InventoryBytes     []byte
	Records            map[string][]byte
}

// CheckpointExportPackage is the complete controller-generated candidate plus
// the backup tool's exact Kubernetes and configured-parent export evidence.
type CheckpointExportPackage struct {
	ManifestBytes                  []byte
	KubernetesObjectInventoryBytes []byte
	KubernetesObjects              []ExportedKubernetesObject
	Parents                        []ParentOwnershipExport
}

// VerifyCheckpointExportPackage validates the complete package before it may
// be labelled completed or used to create the fixed restore Secret. The
// returned digest is over the exact canonical manifest bytes and is suitable
// for binding a missing-Lease recovery approval.
func VerifyCheckpointExportPackage(ctx context.Context, checkpoint CheckpointExportPackage) (CheckpointManifest, string, error) {
	if err := ctx.Err(); err != nil {
		return CheckpointManifest{}, "", err
	}
	manifest, err := DecodeCheckpointManifest(checkpoint.ManifestBytes)
	if err != nil {
		return CheckpointManifest{}, "", err
	}
	if err := VerifyKubernetesObjectExport(ctx, manifest.KubernetesObjects, checkpoint.KubernetesObjectInventoryBytes, checkpoint.KubernetesObjects); err != nil {
		return CheckpointManifest{}, "", fmt.Errorf("verify checkpoint Kubernetes export: %w", err)
	}
	parents := make(map[string]ParentOwnershipExport, len(checkpoint.Parents))
	for index, parent := range checkpoint.Parents {
		if err := ctx.Err(); err != nil {
			return CheckpointManifest{}, "", err
		}
		if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
			return CheckpointManifest{}, "", fmt.Errorf("checkpoint parent export %d: %w", index, err)
		}
		if _, duplicate := parents[parent.ParentFilesystemID]; duplicate {
			return CheckpointManifest{}, "", fmt.Errorf("checkpoint parent export %q is duplicated", parent.ParentFilesystemID)
		}
		parents[parent.ParentFilesystemID] = parent
	}
	if len(parents) != len(manifest.Parents) {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint has %d parent exports, manifest requires %d", len(parents), len(manifest.Parents))
	}
	for _, summary := range manifest.Parents {
		if err := ctx.Err(); err != nil {
			return CheckpointManifest{}, "", err
		}
		parent, present := parents[summary.ParentFilesystemID]
		if !present {
			return CheckpointManifest{}, "", fmt.Errorf("checkpoint is missing parent export %q", summary.ParentFilesystemID)
		}
		if err := VerifyParentInventoryExport(ctx, summary, parent.ParentOwnerBytes, parent.InventoryBytes, parent.Records); err != nil {
			return CheckpointManifest{}, "", fmt.Errorf("verify checkpoint parent %q: %w", summary.ParentFilesystemID, err)
		}
		delete(parents, summary.ParentFilesystemID)
	}
	for parentID := range parents {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint contains extra parent export %q", parentID)
	}
	return manifest, SHA256Digest(checkpoint.ManifestBytes), nil
}
