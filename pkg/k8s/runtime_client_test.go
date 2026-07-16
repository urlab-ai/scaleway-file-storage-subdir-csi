package k8s

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestReadActiveClusterUIDUsesOnlyKubeSystemNamespaceUID(t *testing.T) {
	uid := types.UID("11111111-1111-4111-8111-111111111111")
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: activeClusterNamespace, UID: uid}})
	got, err := ReadActiveClusterUID(context.Background(), client.CoreV1())
	if err != nil || got != string(uid) {
		t.Fatalf("ReadActiveClusterUID() = %q, %v", got, err)
	}
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "namespaces" {
		t.Fatalf("Kubernetes actions = %#v", actions)
	}
}

func TestReadActiveClusterUIDFailsClosed(t *testing.T) {
	client := fake.NewSimpleClientset()
	if _, err := ReadActiveClusterUID(context.Background(), client.CoreV1()); err == nil {
		t.Fatal("ReadActiveClusterUID(missing) error = nil")
	}
	client = fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: activeClusterNamespace, UID: "bad/uid"}})
	if _, err := ReadActiveClusterUID(context.Background(), client.CoreV1()); err == nil {
		t.Fatal("ReadActiveClusterUID(malformed) error = nil")
	}
	client = fake.NewSimpleClientset()
	client.PrependReactor("get", "namespaces", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	if _, err := ReadActiveClusterUID(context.Background(), client.CoreV1()); err == nil {
		t.Fatal("ReadActiveClusterUID(unavailable) error = nil")
	}
}
