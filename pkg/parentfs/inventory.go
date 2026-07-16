package parentfs

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const maxOwnershipInventoryEntries = safety.MaxRegularFileInventoryEntries

// OwnershipTemporary identifies one syntactically valid crash-retry file. It
// is not ownership authority and is excluded from the record set. Startup
// reconciliation may encounter it only through the exact allocation/ownership
// operation that recomputes and authenticates its complete bytes.
type OwnershipTemporary struct {
	Name            string
	LogicalVolumeID string
	OperationID     string
}

// ParentRecordSet is one complete bounded mounted-parent metadata inventory.
type ParentRecordSet struct {
	ParentOwner volume.ParentOwnerRecord
	Ownerships  []volume.OwnershipRecord
	Temporaries []OwnershipTemporary
}

// ReadParentRecordSet revalidates the controller parent mount, anchors both
// metadata and Linux mount-boundary descriptors, and returns every final
// ownership record in stable logical-ID order. Unknown names, special files,
// symlinks, nested mounts, and oversized inventories fail closed.
func (backend *Backend) ReadParentRecordSet(ctx context.Context, parentFilesystemID string) (result ParentRecordSet, returnErr error) {
	root, err := backend.parentRoot(ctx, parentFilesystemID)
	if err != nil {
		return ParentRecordSet{}, err
	}
	durable, err := safety.OpenOSDurableFS(root)
	if err != nil {
		return ParentRecordSet{}, err
	}
	lifecycle, err := safety.OpenOSLifecycleFS(root)
	if err != nil {
		return ParentRecordSet{}, errors.Join(err, durable.Close())
	}
	defer func() { returnErr = errors.Join(returnErr, lifecycle.Close(), durable.Close()) }()

	ownerBytes, err := durable.ReadFileNoFollow(ctx, strings.TrimPrefix(volume.ParentOwnerPath, "/"))
	if err != nil {
		return ParentRecordSet{}, fmt.Errorf("read parent %q owner during inventory: %w", parentFilesystemID, err)
	}
	owner, err := volume.DecodeParentOwnerRecord(ownerBytes)
	if err != nil {
		return ParentRecordSet{}, fmt.Errorf("decode parent %q owner during inventory: %w", parentFilesystemID, err)
	}
	if owner.ParentFilesystemID != parentFilesystemID {
		return ParentRecordSet{}, fmt.Errorf("parent inventory owner ID %q differs from mounted parent %q", owner.ParentFilesystemID, parentFilesystemID)
	}
	metadataDirectory, err := safety.RelativeToParent(path.Join(owner.BasePath, volume.OwnershipMetadataDirectory))
	if err != nil {
		return ParentRecordSet{}, err
	}
	names, err := lifecycle.ListRegularFiles(ctx, metadataDirectory, maxOwnershipInventoryEntries)
	if err != nil {
		return ParentRecordSet{}, fmt.Errorf("list parent %q ownership metadata: %w", parentFilesystemID, err)
	}
	ownerships, temporaries, err := decodeParentRecordEntries(ctx, durable, metadataDirectory, names)
	if err != nil {
		return ParentRecordSet{}, fmt.Errorf("decode parent %q ownership inventory: %w", parentFilesystemID, err)
	}
	return ParentRecordSet{ParentOwner: owner, Ownerships: ownerships, Temporaries: temporaries}, nil
}

func decodeParentRecordEntries(ctx context.Context, durable safety.DurableFS, directory string, names []string) ([]volume.OwnershipRecord, []OwnershipTemporary, error) {
	if durable == nil {
		return nil, nil, fmt.Errorf("ownership inventory durable filesystem is nil")
	}
	if err := safety.ValidateRelative(directory); err != nil {
		return nil, nil, err
	}
	if len(names) > maxOwnershipInventoryEntries {
		return nil, nil, fmt.Errorf("ownership inventory exceeds %d entries", maxOwnershipInventoryEntries)
	}
	orderedNames := slices.Clone(names)
	slices.Sort(orderedNames)
	for index := 1; index < len(orderedNames); index++ {
		if orderedNames[index-1] == orderedNames[index] {
			return nil, nil, fmt.Errorf("ownership inventory repeats entry %q", orderedNames[index])
		}
	}
	ownerships := make([]volume.OwnershipRecord, 0, len(orderedNames))
	temporaries := make([]OwnershipTemporary, 0)
	seenLogicalIDs := make(map[string]struct{})
	for _, name := range orderedNames {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if logicalID, operationID, temporary := parseOwnershipTemporaryName(name); temporary {
			if err := volume.ValidateLogicalVolumeID(logicalID); err != nil {
				return nil, nil, fmt.Errorf("ownership temporary %q logical ID: %w", name, err)
			}
			if err := volume.ValidateOperationID(operationID); err != nil {
				return nil, nil, fmt.Errorf("ownership temporary %q operation ID: %w", name, err)
			}
			temporaries = append(temporaries, OwnershipTemporary{Name: name, LogicalVolumeID: logicalID, OperationID: operationID})
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			return nil, nil, fmt.Errorf("ownership metadata entry %q has an unsupported name", name)
		}
		logicalID := strings.TrimSuffix(name, ".json")
		if err := volume.ValidateLogicalVolumeID(logicalID); err != nil {
			return nil, nil, fmt.Errorf("ownership metadata entry %q logical ID: %w", name, err)
		}
		if _, duplicate := seenLogicalIDs[logicalID]; duplicate {
			return nil, nil, fmt.Errorf("ownership logical ID %q is duplicated", logicalID)
		}
		encoded, err := durable.ReadFileNoFollow(ctx, path.Join(directory, name))
		if err != nil {
			return nil, nil, fmt.Errorf("read ownership metadata entry %q: %w", name, err)
		}
		record, err := volume.DecodeOwnershipRecord(encoded)
		if err != nil {
			return nil, nil, fmt.Errorf("decode ownership metadata entry %q: %w", name, err)
		}
		if record.LogicalID() != logicalID {
			return nil, nil, fmt.Errorf("ownership metadata entry %q contains logical ID %q", name, record.LogicalID())
		}
		seenLogicalIDs[logicalID] = struct{}{}
		ownerships = append(ownerships, record)
	}
	slices.SortFunc(ownerships, func(left, right volume.OwnershipRecord) int {
		return strings.Compare(left.LogicalID(), right.LogicalID())
	})
	return ownerships, temporaries, nil
}

func parseOwnershipTemporaryName(name string) (logicalID, operationID string, present bool) {
	if !strings.HasSuffix(name, ".tmp") {
		return "", "", false
	}
	withoutSuffix := strings.TrimSuffix(name, ".tmp")
	separator := strings.LastIndex(withoutSuffix, ".json.")
	if separator <= 0 || separator+len(".json.") >= len(withoutSuffix) {
		return "", "", false
	}
	return withoutSuffix[:separator], withoutSuffix[separator+len(".json."):], true
}
