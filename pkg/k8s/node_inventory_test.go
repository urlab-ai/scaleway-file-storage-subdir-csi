package k8s

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	inventoryDriver  = "sfs-subdir.csi.example.com"
	inventoryNodeID  = "fr-par-1/11111111-1111-4111-8111-111111111111"
	inventoryNode    = "worker-a"
	inventoryNS      = "scaleway-sfs-subdir-csi"
	inventoryApp     = "scaleway-sfs-subdir-csi"
	inventoryRelease = "driver-release"
)

func TestClientGoNodeInventoryJoinsExactNodeCSINodeAndPod(t *testing.T) {
	client := fake.NewSimpleClientset(inventoryNodeObject(), inventoryCSINodeObject(), inventoryPodObject())
	inventory, err := NewClientGoNodeInventory(client.CoreV1(), client.StorageV1(), inventoryNS, inventoryDriver, inventoryApp, inventoryRelease)
	if err != nil {
		t.Fatalf("NewClientGoNodeInventory() error = %v", err)
	}
	observations, err := inventory.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(observations) != 1 {
		t.Fatalf("observations = %#v", observations)
	}
	got := observations[0]
	if got.NodeName != inventoryNode || got.CSINodeID != inventoryNodeID || !got.Ready || !got.Schedulable || !got.PluginPodReady || !got.DriverRegistered || got.NodeConfigGeneration != "generation" || got.FailureDomain != "fr-par-1" {
		t.Fatalf("observation = %#v", got)
	}
}

func TestClientGoNodeInventoryRejectsDuplicateActivePodAndUnknownNode(t *testing.T) {
	duplicate := inventoryPodObject()
	duplicate.Name = "node-plugin-b"
	client := fake.NewSimpleClientset(inventoryNodeObject(), inventoryCSINodeObject(), inventoryPodObject(), duplicate)
	inventory, _ := NewClientGoNodeInventory(client.CoreV1(), client.StorageV1(), inventoryNS, inventoryDriver, inventoryApp, inventoryRelease)
	if _, err := inventory.Snapshot(context.Background()); err == nil {
		t.Fatal("Snapshot(duplicate active Pod) error = nil")
	}

	unknown := inventoryPodObject()
	unknown.Spec.NodeName = "missing-node"
	client = fake.NewSimpleClientset(inventoryNodeObject(), inventoryCSINodeObject(), unknown)
	inventory, _ = NewClientGoNodeInventory(client.CoreV1(), client.StorageV1(), inventoryNS, inventoryDriver, inventoryApp, inventoryRelease)
	if _, err := inventory.Snapshot(context.Background()); err == nil {
		t.Fatal("Snapshot(unknown Pod node) error = nil")
	}
}

func TestClientGoNormalNodeEvidenceRequiresReadyIdentityWithoutOutOfServiceTaint(t *testing.T) {
	client := fake.NewSimpleClientset(inventoryNodeObject(), inventoryCSINodeObject())
	evidence, err := NewClientGoNormalNodeEvidence(client.CoreV1(), client.StorageV1(), inventoryDriver)
	if err != nil {
		t.Fatalf("NewClientGoNormalNodeEvidence() error = %v", err)
	}
	allowed, err := evidence.NormalUnpublishAllowed(context.Background(), inventoryNodeID)
	if err != nil || !allowed {
		t.Fatalf("NormalUnpublishAllowed() = %t, %v", allowed, err)
	}
	node, _ := client.CoreV1().Nodes().Get(context.Background(), inventoryNode, metav1.GetOptions{})
	node.Spec.Taints = []corev1.Taint{{Key: outOfServiceTaint, Effect: corev1.TaintEffectNoExecute}}
	_, _ = client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{})
	allowed, err = evidence.NormalUnpublishAllowed(context.Background(), inventoryNodeID)
	if err != nil || allowed {
		t.Fatalf("NormalUnpublishAllowed(out-of-service) = %t, %v", allowed, err)
	}
	allowed, err = evidence.NormalUnpublishAllowed(context.Background(), "fr-par-2/22222222-2222-4222-8222-222222222222")
	if err != nil || allowed {
		t.Fatalf("NormalUnpublishAllowed(absent) = %t, %v", allowed, err)
	}
}

func inventoryNodeObject() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: inventoryNode, Labels: map[string]string{standardZoneLabel: "fr-par-1"}},
		Status: corev1.NodeStatus{
			NodeInfo:   corev1.NodeSystemInfo{OperatingSystem: "linux"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func inventoryCSINodeObject() *storagev1.CSINode {
	return &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: inventoryNode},
		Spec:       storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: inventoryDriver, NodeID: inventoryNodeID}}},
	}
}

func inventoryPodObject() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: inventoryNS, Name: "node-plugin-a",
			Labels: map[string]string{
				applicationNameLabel: inventoryApp, applicationInstanceLabel: inventoryRelease,
				applicationComponentLabel: nodePluginComponentLabel,
			},
			Annotations: map[string]string{nodeGenerationAnnotation: "generation"},
		},
		Spec:   corev1.PodSpec{NodeName: inventoryNode},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}
