package k8s

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	storagev1client "k8s.io/client-go/kubernetes/typed/storage/v1"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const maxInventoryObjects = 16384

// DriverPersistentVolume is the immutable CSI recovery projection retained
// with exact Kubernetes generation identity.
type DriverPersistentVolume struct {
	Name            string
	UID             string
	ResourceVersion string
	VolumeHandle    string
	VolumeContext   map[string]string
}

// ClientGoVolumeAttachments proves whether one exact CSI handle still has a
// VolumeAttachment. It lists coherent PV and attachment inventories instead of
// issuing an unbounded API call per attachment.
type ClientGoVolumeAttachments struct {
	core       corev1client.CoreV1Interface
	storage    storagev1client.StorageV1Interface
	driverName string
}

// NewClientGoVolumeAttachments validates its typed Kubernetes boundaries.
func NewClientGoVolumeAttachments(core corev1client.CoreV1Interface, storage storagev1client.StorageV1Interface, driverName string) (*ClientGoVolumeAttachments, error) {
	if core == nil || storage == nil {
		return nil, fmt.Errorf("client-go volume attachment dependency is nil")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	return &ClientGoVolumeAttachments{core: core, storage: storage, driverName: driverName}, nil
}

// HasAttachment counts deleting objects as live and fails closed if a
// driver-owned VolumeAttachment cannot be resolved to an exact CSI handle.
func (client *ClientGoVolumeAttachments) HasAttachment(ctx context.Context, volumeHandle string) (bool, error) {
	if _, err := volume.ParseHandle(volumeHandle); err != nil {
		return false, err
	}
	persistentVolumes, err := client.listPersistentVolumes(ctx)
	if err != nil {
		return false, err
	}
	handlesByPV := make(map[string]string)
	for index := range persistentVolumes.Items {
		persistentVolume := &persistentVolumes.Items[index]
		if persistentVolume.Spec.CSI != nil && persistentVolume.Spec.CSI.Driver == client.driverName {
			handlesByPV[persistentVolume.Name] = persistentVolume.Spec.CSI.VolumeHandle
		}
	}
	attachments, err := client.listVolumeAttachments(ctx)
	if err != nil {
		return false, err
	}
	for index := range attachments.Items {
		attachment := &attachments.Items[index]
		if attachment.Spec.Attacher != client.driverName {
			continue
		}
		if attachment.Spec.Source.PersistentVolumeName != nil {
			persistentVolumeName := *attachment.Spec.Source.PersistentVolumeName
			handle, present := handlesByPV[persistentVolumeName]
			if !present {
				return false, fmt.Errorf("driver VolumeAttachment %q references unresolved PersistentVolume %q: %w", attachment.Name, persistentVolumeName, ErrUnavailable)
			}
			if handle == volumeHandle {
				return true, nil
			}
			continue
		}
		if attachment.Spec.Source.InlineVolumeSpec != nil && attachment.Spec.Source.InlineVolumeSpec.CSI != nil {
			inline := attachment.Spec.Source.InlineVolumeSpec.CSI
			if inline.Driver != client.driverName {
				return false, fmt.Errorf("driver VolumeAttachment %q contains a conflicting inline CSI driver: %w", attachment.Name, ErrUnavailable)
			}
			if inline.VolumeHandle == volumeHandle {
				return true, nil
			}
			continue
		}
		return false, fmt.Errorf("driver VolumeAttachment %q has no resolvable source: %w", attachment.Name, ErrUnavailable)
	}
	return false, nil
}

// HasPVReference proves whether any current driver PersistentVolume references
// the exact logical ID. A malformed driver handle fails closed instead of being
// ignored as an unrelated object.
func (client *ClientGoVolumeAttachments) HasPVReference(ctx context.Context, logicalVolumeID string) (bool, error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return false, err
	}
	persistentVolumes, err := client.listPersistentVolumes(ctx)
	if err != nil {
		return false, err
	}
	for index := range persistentVolumes.Items {
		persistentVolume := &persistentVolumes.Items[index]
		if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != client.driverName {
			continue
		}
		handle, err := volume.ParseHandle(persistentVolume.Spec.CSI.VolumeHandle)
		if err != nil {
			return false, fmt.Errorf("driver PersistentVolume %q has invalid volume handle: %w", persistentVolume.Name, err)
		}
		if handle.LogicalVolumeID == logicalVolumeID {
			return true, nil
		}
	}
	return false, nil
}

// DriverPersistentVolumes returns every current PV for this exact driver in
// stable name order. Invalid or duplicate handle identity fails the complete
// inventory rather than being silently skipped.
func (client *ClientGoVolumeAttachments) DriverPersistentVolumes(ctx context.Context) ([]DriverPersistentVolume, error) {
	persistentVolumes, err := client.listPersistentVolumes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]DriverPersistentVolume, 0)
	seenHandles := make(map[string]string)
	for index := range persistentVolumes.Items {
		persistentVolume := &persistentVolumes.Items[index]
		if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != client.driverName {
			continue
		}
		if _, err := volume.ParseHandle(persistentVolume.Spec.CSI.VolumeHandle); err != nil {
			return nil, fmt.Errorf("driver PersistentVolume %q has invalid volume handle: %w", persistentVolume.Name, err)
		}
		if first, duplicate := seenHandles[persistentVolume.Spec.CSI.VolumeHandle]; duplicate {
			return nil, fmt.Errorf("driver PersistentVolumes %q and %q repeat one volume handle", first, persistentVolume.Name)
		}
		seenHandles[persistentVolume.Spec.CSI.VolumeHandle] = persistentVolume.Name
		result = append(result, DriverPersistentVolume{
			Name: persistentVolume.Name, UID: string(persistentVolume.UID), ResourceVersion: persistentVolume.ResourceVersion,
			VolumeHandle:  persistentVolume.Spec.CSI.VolumeHandle,
			VolumeContext: cloneMap(persistentVolume.Spec.CSI.VolumeAttributes),
		})
	}
	slices.SortFunc(result, func(left, right DriverPersistentVolume) int { return strings.Compare(left.Name, right.Name) })
	return result, nil
}

func (client *ClientGoVolumeAttachments) listPersistentVolumes(ctx context.Context) (*corev1.PersistentVolumeList, error) {
	result := &corev1.PersistentVolumeList{}
	seenTokens := map[string]struct{}{"": {}}
	continueToken := ""
	for {
		page, err := client.core.PersistentVolumes().List(ctx, metav1.ListOptions{Limit: configMapListPageSize, Continue: continueToken})
		if err != nil {
			return nil, classifyClientGoError(ctx, err)
		}
		for index := range page.Items {
			persistentVolume := page.Items[index]
			if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != client.driverName {
				continue
			}
			if len(result.Items) == maxInventoryObjects {
				return nil, fmt.Errorf("driver PersistentVolume inventory exceeds %d relevant objects", maxInventoryObjects)
			}
			result.Items = append(result.Items, persistentVolume)
		}
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seenTokens[continueToken]; duplicate {
			return nil, fmt.Errorf("PersistentVolume list repeated continue token")
		}
		seenTokens[continueToken] = struct{}{}
	}
}

func (client *ClientGoVolumeAttachments) listVolumeAttachments(ctx context.Context) (*storagev1.VolumeAttachmentList, error) {
	result := &storagev1.VolumeAttachmentList{}
	seenTokens := map[string]struct{}{"": {}}
	continueToken := ""
	for {
		page, err := client.storage.VolumeAttachments().List(ctx, metav1.ListOptions{Limit: configMapListPageSize, Continue: continueToken})
		if err != nil {
			return nil, classifyClientGoError(ctx, err)
		}
		for index := range page.Items {
			attachment := page.Items[index]
			if attachment.Spec.Attacher != client.driverName {
				continue
			}
			if len(result.Items) == maxInventoryObjects {
				return nil, fmt.Errorf("driver VolumeAttachment inventory exceeds %d relevant objects", maxInventoryObjects)
			}
			result.Items = append(result.Items, attachment)
		}
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seenTokens[continueToken]; duplicate {
			return nil, fmt.Errorf("VolumeAttachment list repeated continue token")
		}
		seenTokens[continueToken] = struct{}{}
	}
}
