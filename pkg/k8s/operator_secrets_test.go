package k8s

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

func TestReadOperatorApprovalProjectsExactImmutableSecret(t *testing.T) {
	immutable := true
	data := map[string][]byte{
		"schemaVersion": []byte("1"), "mode": []byte("abnormal-takeover"),
		"requestID":                []byte("11111111-1111-4111-8111-111111111111"),
		"installationID":           []byte("22222222-2222-4222-8222-222222222222"),
		"activeClusterUID":         []byte("33333333-3333-4333-8333-333333333333"),
		"previousHolderPodUID":     []byte("44444444-4444-4444-8444-444444444444"),
		"previousHolderNodeName":   []byte("worker-1"),
		"previousHolderCSINodeID":  []byte("fr-par-1/55555555-5555-4555-8555-555555555555"),
		"previousHolderInstanceID": []byte("55555555-5555-4555-8555-555555555555"),
		"previousHolderZone":       []byte("fr-par-1"),
		"checkpointRequestID":      {}, "checkpointManifestSHA256": {}, "recoveryFenceScope": {},
		"reason":     []byte("previous controller was fenced"),
		"approvedAt": []byte("2026-07-13T10:01:00Z"), "expiresAt": []byte("2026-07-13T10:31:00Z"),
	}
	client := fake.NewClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: coordination.ApprovalSecretNameV1, Namespace: "driver-system",
		UID: types.UID("66666666-6666-4666-8666-666666666666"),
	}, Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: data})
	approval, err := ReadOperatorApproval(context.Background(), client.CoreV1(), "driver-system")
	if err != nil {
		t.Fatalf("ReadOperatorApproval() error = %v", err)
	}
	if approval.SecretUID != "66666666-6666-4666-8666-666666666666" || !approval.Immutable || approval.Mode != coordination.ApprovalAbnormalTakeover || approval.PreviousHolderNodeName != "worker-1" {
		t.Fatalf("approval projection = %#v", approval)
	}
}

func TestReadOperatorApprovalRejectsUnknownMutableAndOwnedSecret(t *testing.T) {
	valid := true
	baseData := make(map[string][]byte, len(approvalDataKeys))
	for key := range approvalDataKeys {
		baseData[key] = []byte{}
	}
	for name, mutate := range map[string]func(*corev1.Secret){
		"unknown": func(secret *corev1.Secret) { secret.Data["unknown"] = []byte("x") },
		"mutable": func(secret *corev1.Secret) { secret.Immutable = nil },
		"owned": func(secret *corev1.Secret) {
			secret.OwnerReferences = []metav1.OwnerReference{{Name: "helm"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: coordination.ApprovalSecretNameV1, Namespace: "driver-system", UID: types.UID("11111111-1111-4111-8111-111111111111"),
			}, Type: corev1.SecretTypeOpaque, Immutable: &valid, Data: make(map[string][]byte, len(baseData))}
			for key, value := range baseData {
				secret.Data[key] = value
			}
			mutate(secret)
			client := fake.NewClientset(secret)
			if _, err := ReadOperatorApproval(context.Background(), client.CoreV1(), "driver-system"); err == nil {
				t.Fatalf("ReadOperatorApproval(%s) error = nil", name)
			}
		})
	}
}
