package k8s

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
)

const ControllerLeaseName = "scaleway-sfs-subdir-csi-controller"

// ClientGoLeaseStore owns the fixed controller Lease. Load creates the initial
// empty object only after a conclusive NotFound; Update is one resourceVersion
// CAS and maps a conclusive conflict to coordination.ErrLeaseLost.
type ClientGoLeaseStore struct {
	leases    coordinationv1client.LeaseInterface
	namespace string
}

// NewClientGoLeaseStore scopes the client to the dedicated driver namespace
// and the fixed non-configurable v1 Lease name.
func NewClientGoLeaseStore(api coordinationv1client.CoordinationV1Interface, namespace string) (*ClientGoLeaseStore, error) {
	if api == nil {
		return nil, fmt.Errorf("client-go CoordinationV1 interface is nil")
	}
	if namespace == "" || len(namespace) > 63 {
		return nil, fmt.Errorf("lease namespace must contain 1 to 63 bytes")
	}
	return &ClientGoLeaseStore{leases: api.Leases(namespace), namespace: namespace}, nil
}

// Load returns one coherent Lease generation, creating the first empty object
// without owner references so Helm can never delete or rewrite live evidence.
func (store *ClientGoLeaseStore) Load(ctx context.Context) (coordination.LeaseSnapshot, error) {
	lease, err := store.leases.Get(ctx, ControllerLeaseName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		lease, err = store.createInitial(ctx)
	}
	if err != nil {
		return coordination.LeaseSnapshot{}, classifyClientGoError(ctx, err)
	}
	return leaseSnapshot(lease)
}

func (store *ClientGoLeaseStore) createInitial(ctx context.Context) (*coordinationv1.Lease, error) {
	initial := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: store.namespace, Name: ControllerLeaseName,
		Labels:      map[string]string{"app.kubernetes.io/name": "scaleway-sfs-subdir-csi"},
		Annotations: map[string]string{},
	}}
	created, err := store.leases.Create(ctx, initial, metav1.CreateOptions{})
	if err == nil {
		return created, nil
	}
	if !apierrors.IsAlreadyExists(err) && !errors.Is(classifyClientGoError(ctx, err), ErrUnavailable) {
		return nil, err
	}
	// AlreadyExists and an ambiguous create are resolved only by a fresh exact
	// read. A failed reread preserves the original ambiguity.
	observed, readErr := store.leases.Get(ctx, ControllerLeaseName, metav1.GetOptions{})
	if readErr != nil {
		return nil, errors.Join(err, readErr)
	}
	return observed, nil
}

// Update atomically installs the next holder/evidence generation and exact
// renewal instant. It never retries a conflict with a fresh resourceVersion.
func (store *ClientGoLeaseStore) Update(ctx context.Context, expected, next coordination.LeaseSnapshot, renewedAt time.Time, leaseDuration time.Duration) (coordination.LeaseSnapshot, error) {
	if expected.UID == "" || expected.ResourceVersion == "" || next.UID != expected.UID || next.ResourceVersion != expected.ResourceVersion || next.Annotations == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("lease update identity or annotations are invalid")
	}
	if renewedAt.IsZero() || leaseDuration <= 0 || leaseDuration%time.Second != 0 || leaseDuration/time.Second > math.MaxInt32 {
		return coordination.LeaseSnapshot{}, fmt.Errorf("lease renewal instant or duration is invalid")
	}
	durationSeconds := int32(leaseDuration / time.Second)
	holder := next.HolderIdentity
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: store.namespace, Name: ControllerLeaseName, UID: types.UID(expected.UID),
			ResourceVersion: expected.ResourceVersion,
			Labels:          map[string]string{"app.kubernetes.io/name": "scaleway-sfs-subdir-csi"},
			Annotations:     maps.Clone(next.Annotations),
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &durationSeconds,
			RenewTime: &metav1.MicroTime{Time: renewedAt.UTC()},
		},
	}
	updated, err := store.leases.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return coordination.LeaseSnapshot{}, fmt.Errorf("%w: %v", coordination.ErrLeaseLost, err)
		}
		return coordination.LeaseSnapshot{}, classifyClientGoError(ctx, err)
	}
	return leaseSnapshot(updated)
}

func leaseSnapshot(lease *coordinationv1.Lease) (coordination.LeaseSnapshot, error) {
	if lease == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("kubernetes returned a nil Lease")
	}
	if lease.Name != ControllerLeaseName || lease.Namespace == "" || lease.UID == "" || lease.ResourceVersion == "" {
		return coordination.LeaseSnapshot{}, fmt.Errorf("controller Lease metadata is incomplete or has wrong identity")
	}
	if lease.DeletionTimestamp != nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("controller Lease is pending deletion")
	}
	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	annotations := maps.Clone(lease.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	return coordination.LeaseSnapshot{
		UID: string(lease.UID), ResourceVersion: lease.ResourceVersion,
		HolderIdentity: holder, Annotations: annotations,
	}, nil
}
