package recovery

import (
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

const checkpointDataKey = "checkpoint.json"

// CheckpointSecret is the narrow projection of the fixed externally owned
// Kubernetes Secret. The controller requires get-only access and never mutates
// this object.
type CheckpointSecret struct {
	Name      string
	Type      string
	Immutable bool
	Data      map[string][]byte
}

// ValidateCheckpointSecret requires the exact fixed name, Opaque type,
// immutability, and sole canonical manifest data key.
func ValidateCheckpointSecret(secret CheckpointSecret) (CheckpointManifest, string, error) {
	if secret.Name != coordination.CheckpointSecretNameV1 {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint Secret name %q differs from fixed v1 name", secret.Name)
	}
	if secret.Type != "Opaque" || !secret.Immutable {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint Secret must be immutable Opaque")
	}
	if len(secret.Data) != 1 {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint Secret must contain exactly one data key")
	}
	encoded, present := secret.Data[checkpointDataKey]
	if !present || len(encoded) == 0 {
		return CheckpointManifest{}, "", fmt.Errorf("checkpoint Secret data key %q is missing or empty", checkpointDataKey)
	}
	manifest, err := DecodeCheckpointManifest(encoded)
	if err != nil {
		return CheckpointManifest{}, "", err
	}
	return manifest, SHA256Digest(encoded), nil
}
