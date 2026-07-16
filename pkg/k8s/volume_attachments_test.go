package k8s

import (
	"context"
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const attachmentTestHandle = "sfs1:lv-11111111111111111111111111111111:mh-22222222222222222222222222222222"

func persistentVolume(name, driverName, handle string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: driverName, VolumeHandle: handle}},
	}}
}

func volumeAttachment(name, driverName, persistentVolumeName string) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: storagev1.VolumeAttachmentSpec{
		Attacher: driverName, NodeName: "worker-a",
		Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &persistentVolumeName},
	}}
}

func TestClientGoVolumeAttachmentsFindsExactHandleIncludingDeletingObject(t *testing.T) {
	otherHandle := "sfs1:lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:mh-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	deletion := metav1.Now()
	matching := volumeAttachment("matching", testDriverName, "pv-matching")
	matching.DeletionTimestamp = &deletion
	matching.Finalizers = []string{"external-attacher/test"}
	clientset := fake.NewClientset(
		persistentVolume("pv-other", testDriverName, otherHandle),
		persistentVolume("pv-matching", testDriverName, attachmentTestHandle),
		volumeAttachment("other", testDriverName, "pv-other"), matching,
	)
	client, err := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	if err != nil {
		t.Fatalf("NewClientGoVolumeAttachments() error = %v", err)
	}
	hasAttachment, err := client.HasAttachment(context.Background(), attachmentTestHandle)
	if err != nil || !hasAttachment {
		t.Fatalf("HasAttachment() = %v, %v", hasAttachment, err)
	}
}

func TestClientGoVolumeAttachmentsFailsClosedOnOrphanOrUnavailableInventory(t *testing.T) {
	clientset := fake.NewClientset(volumeAttachment("orphan", testDriverName, "missing-pv"))
	client, _ := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	if _, err := client.HasAttachment(context.Background(), attachmentTestHandle); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("HasAttachment(orphan) error = %v", err)
	}

	clientset = fake.NewClientset()
	clientset.PrependReactor("list", "persistentvolumes", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API response ambiguous")
	})
	client, _ = NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	if _, err := client.HasAttachment(context.Background(), attachmentTestHandle); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("HasAttachment(outage) error = %v", err)
	}
}

func TestClientGoVolumeAttachmentsIgnoresForeignDriverAndReportsAbsence(t *testing.T) {
	clientset := fake.NewClientset(
		persistentVolume("pv", "other.csi.example.com", attachmentTestHandle),
		volumeAttachment("attachment", "other.csi.example.com", "pv"),
	)
	client, _ := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	hasAttachment, err := client.HasAttachment(context.Background(), attachmentTestHandle)
	if err != nil || hasAttachment {
		t.Fatalf("HasAttachment(foreign) = %v, %v", hasAttachment, err)
	}
}

func TestClientGoVolumeAttachmentsChecksExactPVLogicalReference(t *testing.T) {
	clientset := fake.NewClientset(persistentVolume("pv", testDriverName, attachmentTestHandle))
	client, _ := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	parsed, err := volume.ParseHandle(attachmentTestHandle)
	if err != nil {
		t.Fatalf("ParseHandle() error = %v", err)
	}
	referenced, err := client.HasPVReference(context.Background(), parsed.LogicalVolumeID)
	if err != nil || !referenced {
		t.Fatalf("HasPVReference() = %t, %v", referenced, err)
	}
	other := "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	referenced, err = client.HasPVReference(context.Background(), other)
	if err != nil || referenced {
		t.Fatalf("HasPVReference(other) = %t, %v", referenced, err)
	}
}

func TestClientGoVolumeAttachmentsBoundsOnlyRelevantDriverObjects(t *testing.T) {
	objects := make([]runtime.Object, 0, 4102)
	for index := 0; index < 4100; index++ {
		objects = append(objects, persistentVolume(fmt.Sprintf("foreign-%05d", index), "foreign.csi.example.com", attachmentTestHandle))
	}
	objects = append(objects,
		persistentVolume("driver-pv", testDriverName, attachmentTestHandle),
		volumeAttachment("driver-attachment", testDriverName, "driver-pv"),
	)
	clientset := fake.NewClientset(objects...)
	client, err := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	if err != nil {
		t.Fatalf("NewClientGoVolumeAttachments() error = %v", err)
	}
	hasAttachment, err := client.HasAttachment(context.Background(), attachmentTestHandle)
	if err != nil || !hasAttachment {
		t.Fatalf("HasAttachment(after 4100 foreign PVs) = %t, %v", hasAttachment, err)
	}
}

func TestClientGoVolumeAttachmentsSupportsSixThousandRelevantAttachments(t *testing.T) {
	objects := make([]runtime.Object, 0, 6001)
	objects = append(objects, persistentVolume("driver-pv", testDriverName, attachmentTestHandle))
	for index := 0; index < 6000; index++ {
		objects = append(objects, volumeAttachment(fmt.Sprintf("driver-%05d", index), testDriverName, "driver-pv"))
	}
	clientset := fake.NewClientset(objects...)
	client, err := NewClientGoVolumeAttachments(clientset.CoreV1(), clientset.StorageV1(), testDriverName)
	if err != nil {
		t.Fatalf("NewClientGoVolumeAttachments() error = %v", err)
	}
	attachments, err := client.listVolumeAttachments(context.Background())
	if err != nil {
		t.Fatalf("listVolumeAttachments() error = %v", err)
	}
	if got := len(attachments.Items); got != 6000 {
		t.Fatalf("relevant VolumeAttachments = %d, want 6000", got)
	}
}
