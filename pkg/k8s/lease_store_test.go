package k8s

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"scaleway-sfs-subdir-csi/pkg/coordination"
)

const testLeaseUID = "11111111-1111-4111-8111-111111111111"

func TestClientGoLeaseStoreLoadsAndCASUpdatesExactEvidence(t *testing.T) {
	holder := "old-holder"
	clientset := fake.NewClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: "driver", Name: ControllerLeaseName, UID: types.UID(testLeaseUID), ResourceVersion: "7", Annotations: map[string]string{"preserved": "value"}},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder},
	})
	store, err := NewClientGoLeaseStore(clientset.CoordinationV1(), "driver")
	if err != nil {
		t.Fatalf("NewClientGoLeaseStore() error = %v", err)
	}
	current, err := store.Load(context.Background())
	if err != nil || current.UID != testLeaseUID || current.ResourceVersion != "7" || current.HolderIdentity != holder {
		t.Fatalf("Load() = %#v, %v", current, err)
	}
	next := current
	next.HolderIdentity = "new-holder"
	next.Annotations = map[string]string{"preserved": "value", "holder": "evidence"}
	updated, err := store.Update(context.Background(), current, next, time.Unix(123, 456000000), 30*time.Second)
	if err != nil || updated.HolderIdentity != "new-holder" || updated.Annotations["holder"] != "evidence" {
		t.Fatalf("Update() = %#v, %v", updated, err)
	}
	stored, err := clientset.CoordinationV1().Leases("driver").Get(context.Background(), ControllerLeaseName, metav1.GetOptions{})
	if err != nil || stored.Spec.RenewTime == nil || stored.Spec.RenewTime.Unix() != 123 || stored.Spec.LeaseDurationSeconds == nil || *stored.Spec.LeaseDurationSeconds != 30 {
		t.Fatalf("stored Lease = %#v, %v", stored, err)
	}
	if stored.OwnerReferences != nil {
		t.Fatal("runtime Lease unexpectedly has an owner reference")
	}
}

func TestClientGoLeaseStoreCreatesInitialLeaseOnlyAfterNotFound(t *testing.T) {
	clientset := fake.NewClientset()
	clientset.PrependReactor("create", "leases", func(action clienttesting.Action) (bool, runtime.Object, error) {
		create := action.(clienttesting.CreateAction)
		lease := create.GetObject().(*coordinationv1.Lease).DeepCopy()
		lease.UID = types.UID(testLeaseUID)
		lease.ResourceVersion = "1"
		return true, lease, nil
	})
	store, _ := NewClientGoLeaseStore(clientset.CoordinationV1(), "driver")
	snapshot, err := store.Load(context.Background())
	if err != nil || snapshot.UID != testLeaseUID || snapshot.ResourceVersion != "1" || snapshot.HolderIdentity != "" || snapshot.Annotations == nil {
		t.Fatalf("Load(create) = %#v, %v", snapshot, err)
	}
}

func TestClientGoLeaseStoreMapsConflictToLeaseLostAndPreservesOutage(t *testing.T) {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "driver", Name: ControllerLeaseName, UID: types.UID(testLeaseUID), ResourceVersion: "7", Annotations: map[string]string{}}}
	clientset := fake.NewClientset(lease)
	store, _ := NewClientGoLeaseStore(clientset.CoordinationV1(), "driver")
	snapshot, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	resource := schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}
	clientset.PrependReactor("update", "leases", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(resource, ControllerLeaseName, errors.New("stale"))
	})
	if _, err := store.Update(context.Background(), snapshot, snapshot, time.Now(), 30*time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Update(conflict) error = %v", err)
	}
}
