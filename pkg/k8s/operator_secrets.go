package k8s

import (
	"context"
	"fmt"
	"maps"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

const maxOperatorSecretFieldBytes = 4096

var approvalDataKeys = map[string]struct{}{
	"schemaVersion": {}, "mode": {}, "requestID": {},
	"installationID": {}, "activeClusterUID": {},
	"previousHolderPodUID": {}, "previousHolderNodeName": {},
	"previousHolderCSINodeID": {}, "previousHolderInstanceID": {},
	"previousHolderZone": {}, "checkpointRequestID": {},
	"checkpointManifestSHA256": {}, "recoveryFenceScope": {},
	"reason": {}, "approvedAt": {}, "expiresAt": {},
}

// ReadOperatorApproval gets only the fixed externally owned immutable Secret
// and projects its exact closed data key set. Temporal, holder, checkpoint, and
// provider-fence validation remains in the coordination acquisition CAS path.
func ReadOperatorApproval(ctx context.Context, core corev1client.CoreV1Interface, namespace string) (coordination.OperatorApproval, error) {
	secret, err := readFixedSecret(ctx, core, namespace, coordination.ApprovalSecretNameV1)
	if err != nil {
		return coordination.OperatorApproval{}, err
	}
	if len(secret.Data) != len(approvalDataKeys) {
		return coordination.OperatorApproval{}, fmt.Errorf("operator approval Secret has %d data keys, want %d", len(secret.Data), len(approvalDataKeys))
	}
	values := make(map[string]string, len(secret.Data))
	for key, encoded := range secret.Data {
		if _, known := approvalDataKeys[key]; !known {
			return coordination.OperatorApproval{}, fmt.Errorf("operator approval Secret contains unknown data key %q", key)
		}
		if len(encoded) > maxOperatorSecretFieldBytes || !utf8.Valid(encoded) {
			return coordination.OperatorApproval{}, fmt.Errorf("operator approval Secret data key %q is not bounded UTF-8", key)
		}
		values[key] = string(encoded)
	}
	return coordination.OperatorApproval{
		SecretUID: string(secret.UID), Immutable: true,
		SchemaVersion: values["schemaVersion"], Mode: coordination.ApprovalMode(values["mode"]),
		RequestID: values["requestID"], InstallationID: values["installationID"],
		ActiveClusterUID: values["activeClusterUID"], PreviousHolderPodUID: values["previousHolderPodUID"],
		PreviousHolderNodeName: values["previousHolderNodeName"], PreviousHolderCSINodeID: values["previousHolderCSINodeID"],
		PreviousHolderInstanceID: values["previousHolderInstanceID"], PreviousHolderZone: values["previousHolderZone"],
		CheckpointRequestID: values["checkpointRequestID"], CheckpointManifestSHA256: values["checkpointManifestSHA256"],
		RecoveryFenceScope: values["recoveryFenceScope"], Reason: values["reason"],
		ApprovedAt: values["approvedAt"], ExpiresAt: values["expiresAt"],
	}, nil
}

// FixedCheckpointSecret is the cycle-free Kubernetes projection consumed by
// the recovery package's closed manifest validator in the controller layer.
type FixedCheckpointSecret struct {
	Name      string
	Type      string
	Immutable bool
	Data      map[string][]byte
}

// ReadCheckpointSecret gets only the fixed restore Secret. Canonical manifest
// validation is deliberately left to recovery so the Kubernetes package does
// not depend back on the controller recovery graph.
func ReadCheckpointSecret(ctx context.Context, core corev1client.CoreV1Interface, namespace string) (FixedCheckpointSecret, error) {
	secret, err := readFixedSecret(ctx, core, namespace, coordination.CheckpointSecretNameV1)
	if err != nil {
		return FixedCheckpointSecret{}, err
	}
	return FixedCheckpointSecret{
		Name: secret.Name, Type: string(secret.Type), Immutable: true, Data: maps.Clone(secret.Data),
	}, nil
}

func readFixedSecret(ctx context.Context, core corev1client.CoreV1Interface, namespace, name string) (*corev1.Secret, error) {
	if ctx == nil {
		return nil, fmt.Errorf("operator Secret context is nil")
	}
	if core == nil {
		return nil, fmt.Errorf("operator Secret CoreV1 client is nil")
	}
	if namespace == "" || len(namespace) > 63 {
		return nil, fmt.Errorf("operator Secret namespace must contain 1 to 63 bytes")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	secret, err := core.Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, classifyClientGoError(ctx, err)
	}
	if secret == nil || secret.Name != name || secret.Namespace != namespace || secret.UID == "" {
		return nil, fmt.Errorf("operator Secret metadata is incomplete or has wrong identity")
	}
	if secret.DeletionTimestamp != nil {
		return nil, fmt.Errorf("operator Secret %q is pending deletion", name)
	}
	if secret.Immutable == nil || !*secret.Immutable || secret.Type != corev1.SecretTypeOpaque {
		return nil, fmt.Errorf("operator Secret %q must be immutable Opaque", name)
	}
	if len(secret.OwnerReferences) != 0 {
		return nil, fmt.Errorf("operator Secret %q must be externally owned without owner references", name)
	}
	return secret.DeepCopy(), nil
}
