package driverapp

import (
	"context"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type recoveryLeadershipGuard interface {
	RequireActiveLeadership(ctx context.Context) error
}

// recoveryMutationAdmission preserves the controller's global gate -> volume
// lock order for create-only reconstruction writes. Checkpoint resume passes a
// callback-scoped gate capability in ctx; ordinary startup and background
// repair use normal admission through the same method.
type recoveryMutationAdmission struct {
	leadership recoveryLeadershipGuard
	gate       *coordination.MutationGate
	volumeLock *coordination.KeyedLock
}

func newRecoveryMutationAdmission(leadership recoveryLeadershipGuard, gate *coordination.MutationGate, volumeLock *coordination.KeyedLock) (*recoveryMutationAdmission, error) {
	if leadership == nil || gate == nil || volumeLock == nil {
		return nil, fmt.Errorf("recovery mutation admission dependency is nil")
	}
	return &recoveryMutationAdmission{leadership: leadership, gate: gate, volumeLock: volumeLock}, nil
}

func (admission *recoveryMutationAdmission) enter(ctx context.Context, logicalVolumeID string) (func(), error) {
	if err := volume.ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return nil, err
	}
	if err := admission.leadership.RequireActiveLeadership(ctx); err != nil {
		return nil, err
	}
	releaseMutation, err := admission.gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	unlockVolume, err := admission.volumeLock.Lock(ctx, logicalVolumeID)
	if err != nil {
		releaseMutation()
		return nil, err
	}
	return func() {
		unlockVolume()
		releaseMutation()
	}, nil
}

type pvBackedReconstructor interface {
	Reconstruct(ctx context.Context, evidence recovery.PersistentVolumeEvidence, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error)
}

type guardedPVBackedReconstructor struct {
	admission *recoveryMutationAdmission
	delegate  pvBackedReconstructor
}

func newGuardedPVBackedReconstructor(admission *recoveryMutationAdmission, delegate pvBackedReconstructor) (*guardedPVBackedReconstructor, error) {
	if admission == nil || delegate == nil {
		return nil, fmt.Errorf("guarded PV-backed reconstructor dependency is nil")
	}
	return &guardedPVBackedReconstructor{admission: admission, delegate: delegate}, nil
}

func (reconstructor *guardedPVBackedReconstructor) Reconstruct(ctx context.Context, evidence recovery.PersistentVolumeEvidence, ownership *volume.DetailedOwnershipRecord) (k8s.StoredAllocation, error) {
	logicalVolumeID, err := evidenceLogicalVolumeID(evidence, ownership)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	release, err := reconstructor.admission.enter(ctx, logicalVolumeID)
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	defer release()
	return reconstructor.delegate.Reconstruct(ctx, evidence, ownership)
}

func evidenceLogicalVolumeID(evidence recovery.PersistentVolumeEvidence, ownership *volume.DetailedOwnershipRecord) (string, error) {
	if ownership == nil {
		return "", fmt.Errorf("PV-backed recovery ownership is nil")
	}
	if err := ownership.Validate(); err != nil {
		return "", err
	}
	context, err := evidence.Validate()
	if err != nil {
		return "", err
	}
	if context.LogicalVolumeID != ownership.LogicalVolumeID {
		return "", fmt.Errorf("PV-backed recovery evidence and ownership logical IDs differ")
	}
	return ownership.LogicalVolumeID, nil
}

type ownershipOnlyReconstructor interface {
	Reconstruct(ctx context.Context, ownership volume.OwnershipRecord) (k8s.StoredAllocation, error)
}

type guardedOwnershipOnlyReconstructor struct {
	admission *recoveryMutationAdmission
	delegate  ownershipOnlyReconstructor
}

func newGuardedOwnershipOnlyReconstructor(admission *recoveryMutationAdmission, delegate ownershipOnlyReconstructor) (*guardedOwnershipOnlyReconstructor, error) {
	if admission == nil || delegate == nil {
		return nil, fmt.Errorf("guarded ownership-only reconstructor dependency is nil")
	}
	return &guardedOwnershipOnlyReconstructor{admission: admission, delegate: delegate}, nil
}

func (reconstructor *guardedOwnershipOnlyReconstructor) Reconstruct(ctx context.Context, ownership volume.OwnershipRecord) (k8s.StoredAllocation, error) {
	if ownership == nil {
		return k8s.StoredAllocation{}, fmt.Errorf("ownership-only recovery record is nil")
	}
	if err := ownership.Validate(); err != nil {
		return k8s.StoredAllocation{}, err
	}
	release, err := reconstructor.admission.enter(ctx, ownership.LogicalID())
	if err != nil {
		return k8s.StoredAllocation{}, err
	}
	defer release()
	return reconstructor.delegate.Reconstruct(ctx, ownership)
}
