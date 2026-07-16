package k8s

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestClientGoConfigMapsRoundTripAndResourceVersionCAS(t *testing.T) {
	clientset := fake.NewClientset()
	client, err := NewClientGoConfigMaps(clientset.CoreV1())
	if err != nil {
		t.Fatalf("NewClientGoConfigMaps() error = %v", err)
	}
	input := ConfigMap{Namespace: "driver", Name: "allocation", UID: "11111111-1111-4111-8111-111111111111", Labels: map[string]string{"app": "driver"}, Data: map[string]string{"record.json": "{}"}}
	created, err := client.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Namespace != input.Namespace || created.Name != input.Name || created.UID != input.UID {
		t.Fatalf("Create() = %#v", created)
	}
	created.Data["record.json"] = "changed"
	updated, err := client.Update(context.Background(), created)
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	got, err := client.Get(context.Background(), input.Namespace, input.Name)
	if err != nil || got.Data["record.json"] != "changed" || got.ResourceVersion != updated.ResourceVersion {
		t.Fatalf("Get() = %#v, %v", got, err)
	}
	listed, err := client.List(context.Background(), input.Namespace, map[string]string{"app": "driver"})
	if err != nil || len(listed) != 1 || listed[0].Name != input.Name {
		t.Fatalf("List() = %#v, %v", listed, err)
	}
	input.Data["record.json"] = "caller mutation"
	if got.Data["record.json"] != "changed" {
		t.Fatal("client-go projection aliases caller map")
	}
}

func TestClientGoConfigMapsPreservesServerAssignedUID(t *testing.T) {
	clientset := fake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: "driver", Name: "allocation", UID: types.UID("22222222-2222-4222-8222-222222222222"), ResourceVersion: "7",
	}})
	client, err := NewClientGoConfigMaps(clientset.CoreV1())
	if err != nil {
		t.Fatalf("NewClientGoConfigMaps() error = %v", err)
	}
	object, err := client.Get(context.Background(), "driver", "allocation")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if object.UID != "22222222-2222-4222-8222-222222222222" || object.ResourceVersion != "7" {
		t.Fatalf("server identity projection = %#v", object)
	}
}

func TestClientGoConfigMapsClassifiesOnlyConclusiveAbsence(t *testing.T) {
	clientset := fake.NewClientset()
	client, err := NewClientGoConfigMaps(clientset.CoreV1())
	if err != nil {
		t.Fatalf("NewClientGoConfigMaps() error = %v", err)
	}
	if _, err := client.Get(context.Background(), "driver", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) error = %v", err)
	}
	resource := schema.GroupResource{Group: "", Resource: "configmaps"}
	tests := map[string]struct {
		err  error
		want error
	}{
		"forbidden": {err: apierrors.NewForbidden(resource, "x", errors.New("denied")), want: ErrForbidden},
		"conflict":  {err: apierrors.NewConflict(resource, "x", errors.New("stale")), want: ErrConflict},
		"timeout":   {err: apierrors.NewServerTimeout(resource, "get", 1), want: ErrUnavailable},
		"unknown":   {err: errors.New("connection result ambiguous"), want: ErrUnavailable},
	}
	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			clientset.PrependReactor("get", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, testCase.err
			})
			_, err := client.Get(context.Background(), "driver", "x")
			if !errors.Is(err, testCase.want) || errors.Is(err, ErrNotFound) {
				t.Fatalf("Get() error = %v, want %v", err, testCase.want)
			}
		})
	}
}

func TestClientGoConfigMapsRejectsBinaryDataAndBoundsList(t *testing.T) {
	clientset := fake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "driver", Name: "binary"}, BinaryData: map[string][]byte{"x": {1}}})
	client, err := NewClientGoConfigMaps(clientset.CoreV1())
	if err != nil {
		t.Fatalf("NewClientGoConfigMaps() error = %v", err)
	}
	if _, err := client.Get(context.Background(), "driver", "binary"); err == nil {
		t.Fatal("Get(binaryData) error = nil")
	}
	objects := make([]runtime.Object, 0, maxListedConfigMaps+1)
	for index := 0; index <= maxListedConfigMaps; index++ {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "driver", Name: fmt.Sprintf("allocation-%04d", index), Labels: map[string]string{"app": "driver"}}})
	}
	clientset = fake.NewClientset(objects...)
	client, _ = NewClientGoConfigMaps(clientset.CoreV1())
	if _, err := client.List(context.Background(), "driver", map[string]string{"app": "driver"}); err == nil {
		t.Fatal("List(over bound) error = nil")
	}
}

func TestClientGoConfigMapsListsSupportedV1HistoryEnvelope(t *testing.T) {
	const supportedRecords = 1000 + 10000
	objects := make([]runtime.Object, 0, supportedRecords)
	for index := 0; index < supportedRecords; index++ {
		objects = append(objects, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Namespace: "driver", Name: fmt.Sprintf("allocation-%05d", index), Labels: map[string]string{"app": "driver"},
		}})
	}
	clientset := fake.NewClientset(objects...)
	client, err := NewClientGoConfigMaps(clientset.CoreV1())
	if err != nil {
		t.Fatalf("NewClientGoConfigMaps() error = %v", err)
	}
	listed, err := client.List(context.Background(), "driver", map[string]string{"app": "driver"})
	if err != nil {
		t.Fatalf("List(supported history envelope) error = %v", err)
	}
	if len(listed) != supportedRecords {
		t.Fatalf("List(supported history envelope) objects = %d, want %d", len(listed), supportedRecords)
	}
}
