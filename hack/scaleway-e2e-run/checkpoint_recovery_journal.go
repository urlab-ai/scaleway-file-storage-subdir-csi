package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/releasequalification"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	checkpointRecoverySchemaVersion   = "1"
	checkpointPhaseWorkloadCreating   = "workload-creating"
	checkpointPhaseWorkloadReady      = "workload-ready"
	checkpointPhaseWorkloadStopped    = "workload-stopped"
	checkpointPhasePreparing          = "preparing"
	checkpointPhasePrepared           = "prepared"
	checkpointPhaseNamespaceDeleted   = "namespace-deleted"
	checkpointPhaseControllerRestored = "controller-restored"
)

// checkpointNodeRetirement normally binds one pre-recovery Kapsule node to the
// exact run-owned Instance and provider-created root volume that may have to be
// retired if Kapsule leaves DeleteNode stuck. The record is fsynced before the
// first provider mutation so an interrupted recovery never has to rediscover
// destructive authority from a name or from a partially deleted resource.
//
// AlreadyAbsent is a narrow compatibility state for a retained journal written
// by the older harness before root-volume capture existed. It records only the
// old Instance ID after both complete Kapsule inventory and Instance API prove
// absence. It supplies no destructive authority and is forbidden in journals
// created by the current harness.
type checkpointNodeRetirement struct {
	KapsuleNodeID string `json:"kapsuleNodeId,omitempty"`
	NodeName      string `json:"nodeName,omitempty"`
	InstanceID    string `json:"instanceId"`
	RootVolumeID  string `json:"rootVolumeId,omitempty"`
	AlreadyAbsent bool   `json:"alreadyAbsent,omitempty"`
}

// checkpointRecoveryJournal makes the namespace-delete checkpoint scenario
// restartable. Paths refer only to files inside the exact evidence directory;
// cloud and Kubernetes authority continues to come from the closed request and
// exact-ID cleanup inventory.
type checkpointRecoveryJournal struct {
	SchemaVersion                          string                     `json:"schemaVersion"`
	RunID                                  string                     `json:"runId"`
	Phase                                  string                     `json:"phase"`
	WorkloadNamespace                      string                     `json:"workloadNamespace"`
	WorkloadClaim                          string                     `json:"workloadClaim"`
	WorkloadDeployment                     string                     `json:"workloadDeployment"`
	Marker                                 string                     `json:"marker"`
	PersistentVolume                       string                     `json:"persistentVolume,omitempty"`
	WorkloadStoppedBeforeNamespaceDeletion bool                       `json:"workloadStoppedBeforeNamespaceDeletion,omitempty"`
	ValuesPath                             string                     `json:"valuesPath,omitempty"`
	ValuesSHA256                           string                     `json:"valuesSha256,omitempty"`
	CheckpointRequestID                    string                     `json:"checkpointRequestId,omitempty"`
	ArchivePath                            string                     `json:"archivePath,omitempty"`
	ArchiveSHA256                          string                     `json:"archiveSha256,omitempty"`
	ArchiveBytes                           uint64                     `json:"archiveBytes,omitempty"`
	ManifestSHA256                         string                     `json:"manifestSha256,omitempty"`
	OldInstanceIDs                         []string                   `json:"oldInstanceIds,omitempty"`
	NodeRetirements                        []checkpointNodeRetirement `json:"nodeRetirements,omitempty"`
}

func newCheckpointRecoveryJournal(plan e2eplan.Plan) checkpointRecoveryJournal {
	shortRun := plan.RunID[:8]
	return checkpointRecoveryJournal{
		SchemaVersion: checkpointRecoverySchemaVersion, RunID: plan.RunID,
		Phase:              checkpointPhaseWorkloadCreating,
		WorkloadNamespace:  "e2e-recovery-" + shortRun,
		WorkloadClaim:      "checkpoint-data-" + shortRun,
		WorkloadDeployment: "checkpoint-workload-" + shortRun,
		Marker:             "checkpoint-" + shortRun,
	}
}

func (backend *scalewayBackend) checkpointRecoveryPath(plan e2eplan.Plan) string {
	return filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "checkpoint-recovery-"+plan.RunID+".json")
}

func (journal checkpointRecoveryJournal) validate(plan e2eplan.Plan) error {
	expected := newCheckpointRecoveryJournal(plan)
	if journal.SchemaVersion != checkpointRecoverySchemaVersion || journal.RunID != plan.RunID ||
		journal.WorkloadNamespace != expected.WorkloadNamespace || journal.WorkloadClaim != expected.WorkloadClaim ||
		journal.WorkloadDeployment != expected.WorkloadDeployment || journal.Marker != expected.Marker {
		return fmt.Errorf("checkpoint recovery journal envelope is invalid")
	}
	switch journal.Phase {
	case checkpointPhaseWorkloadCreating:
		if journal.PersistentVolume != "" || journal.WorkloadStoppedBeforeNamespaceDeletion ||
			journal.ValuesPath != "" || journal.ValuesSHA256 != "" || journal.CheckpointRequestID != "" ||
			len(journal.OldInstanceIDs) != 0 || len(journal.NodeRetirements) != 0 {
			return fmt.Errorf("checkpoint workload-creating journal contains future authority")
		}
	case checkpointPhaseWorkloadReady:
		if !safeKubernetesObjectName(journal.PersistentVolume) || journal.ValuesPath != "" ||
			journal.WorkloadStoppedBeforeNamespaceDeletion ||
			journal.ValuesSHA256 != "" || journal.CheckpointRequestID != "" ||
			len(journal.OldInstanceIDs) != 0 || len(journal.NodeRetirements) != 0 {
			return fmt.Errorf("checkpoint workload-ready journal is incomplete")
		}
	case checkpointPhaseWorkloadStopped:
		if !safeKubernetesObjectName(journal.PersistentVolume) ||
			!journal.WorkloadStoppedBeforeNamespaceDeletion ||
			journal.ValuesPath != "" || journal.ValuesSHA256 != "" || journal.CheckpointRequestID != "" ||
			len(journal.OldInstanceIDs) != 0 || len(journal.NodeRetirements) != 0 {
			return fmt.Errorf("checkpoint workload-stopped journal is incomplete")
		}
	case checkpointPhasePreparing:
		if !safeKubernetesObjectName(journal.PersistentVolume) ||
			!safeEvidencePath(plan, journal.ValuesPath, "checkpoint-release-values-"+plan.RunID+".yaml") ||
			!validRecoveryDigest(journal.ValuesSHA256) ||
			!safeEvidencePath(plan, journal.ArchivePath, "checkpoint-"+journal.CheckpointRequestID+".tar") ||
			len(journal.OldInstanceIDs) != int(plan.NodePool.Count) {
			return fmt.Errorf("preparing checkpoint recovery journal is incomplete")
		}
		if err := volume.ValidateOperationID(journal.CheckpointRequestID); err != nil {
			return err
		}
	case checkpointPhasePrepared, checkpointPhaseNamespaceDeleted, checkpointPhaseControllerRestored:
		if !safeKubernetesObjectName(journal.PersistentVolume) ||
			!safeEvidencePath(plan, journal.ValuesPath, "checkpoint-release-values-"+plan.RunID+".yaml") ||
			!validRecoveryDigest(journal.ValuesSHA256) ||
			!safeEvidencePath(plan, journal.ArchivePath, "checkpoint-"+journal.CheckpointRequestID+".tar") ||
			!validRecoveryDigest(journal.ArchiveSHA256) || !validRecoveryDigest(journal.ManifestSHA256) ||
			journal.ArchiveBytes == 0 || len(journal.OldInstanceIDs) != int(plan.NodePool.Count) {
			return fmt.Errorf("prepared checkpoint recovery journal is incomplete")
		}
		if err := volume.ValidateOperationID(journal.CheckpointRequestID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("checkpoint recovery phase %q is unsupported", journal.Phase)
	}
	seen := map[string]struct{}{}
	for _, id := range journal.OldInstanceIDs {
		if err := volume.ValidateOperationID(id); err != nil {
			return err
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("checkpoint recovery journal repeats an old Instance ID")
		}
		seen[id] = struct{}{}
	}
	if journal.WorkloadStoppedBeforeNamespaceDeletion &&
		journal.Phase != checkpointPhaseWorkloadStopped &&
		len(journal.NodeRetirements) != int(plan.NodePool.Count) {
		return fmt.Errorf("checkpoint recovery journal lacks its exact node-retirement records")
	}
	if journal.WorkloadStoppedBeforeNamespaceDeletion {
		for _, retirement := range journal.NodeRetirements {
			if retirement.AlreadyAbsent {
				return fmt.Errorf("new checkpoint recovery cannot infer an already-absent retirement")
			}
		}
	}
	if err := validateCheckpointNodeRetirements(journal.OldInstanceIDs, journal.NodeRetirements); err != nil {
		return err
	}
	return nil
}

func validateCheckpointNodeRetirements(oldInstanceIDs []string, retirements []checkpointNodeRetirement) error {
	if len(retirements) == 0 {
		// Compatibility for the retained diagnostic journal created before
		// exact root-volume identities were added. Recovery must durably arm
		// those identities from still-observable exact resources before it may
		// perform another provider mutation.
		return nil
	}
	if len(retirements) != len(oldInstanceIDs) {
		return fmt.Errorf("checkpoint node-retirement records differ from the old Instance set")
	}
	old := make(map[string]struct{}, len(oldInstanceIDs))
	for _, id := range oldInstanceIDs {
		old[id] = struct{}{}
	}
	seenNodes := make(map[string]struct{}, len(retirements))
	seenInstances := make(map[string]struct{}, len(retirements))
	seenRoots := make(map[string]struct{}, len(retirements))
	for _, retirement := range retirements {
		if err := volume.ValidateOperationID(retirement.InstanceID); err != nil {
			return fmt.Errorf("checkpoint Instance retirement identity: %w", err)
		}
		if retirement.AlreadyAbsent {
			if retirement.KapsuleNodeID != "" || retirement.NodeName != "" || retirement.RootVolumeID != "" {
				return fmt.Errorf("already-absent checkpoint retirement contains invented provider authority")
			}
		} else {
			for label, id := range map[string]string{
				"Kapsule node": retirement.KapsuleNodeID,
				"root volume":  retirement.RootVolumeID,
			} {
				if err := volume.ValidateOperationID(id); err != nil {
					return fmt.Errorf("checkpoint %s retirement identity: %w", label, err)
				}
			}
			if !safeKubernetesObjectName(retirement.NodeName) {
				return fmt.Errorf("checkpoint node-retirement name is invalid")
			}
		}
		if _, present := old[retirement.InstanceID]; !present {
			return fmt.Errorf("checkpoint node-retirement record names a foreign Instance")
		}
		if _, duplicate := seenInstances[retirement.InstanceID]; duplicate {
			return fmt.Errorf("checkpoint node-retirement records repeat an Instance")
		}
		if !retirement.AlreadyAbsent {
			if _, duplicate := seenNodes[retirement.KapsuleNodeID]; duplicate {
				return fmt.Errorf("checkpoint node-retirement records repeat a Kapsule node")
			}
			if _, duplicate := seenRoots[retirement.RootVolumeID]; duplicate {
				return fmt.Errorf("checkpoint node-retirement records repeat a root volume")
			}
			seenNodes[retirement.KapsuleNodeID] = struct{}{}
			seenRoots[retirement.RootVolumeID] = struct{}{}
		}
		seenInstances[retirement.InstanceID] = struct{}{}
	}
	return nil
}

func safeKubernetesObjectName(value string) bool {
	return value != "" && len(value) <= 253 && !strings.ContainsAny(value, "\x00\r\n\t /")
}

func safeEvidencePath(plan e2eplan.Plan, path, basename string) bool {
	return path == filepath.Join(filepath.Dir(plan.CleanupInventoryPath), basename) &&
		filepath.IsAbs(path) && filepath.Clean(path) == path
}

func validRecoveryDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") &&
		strings.Trim(value[7:], "0123456789abcdef") == ""
}

func (backend *scalewayBackend) writeCheckpointRecoveryJournal(plan e2eplan.Plan, journal checkpointRecoveryJournal) error {
	if err := journal.validate(plan); err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(journal)
	if err != nil {
		return err
	}
	return replaceDurableFile(backend.checkpointRecoveryPath(plan), append(encoded, '\n'), 0o600)
}

func (backend *scalewayBackend) readCheckpointRecoveryJournal(plan e2eplan.Plan) (checkpointRecoveryJournal, error) {
	encoded, err := os.ReadFile(backend.checkpointRecoveryPath(plan))
	if err != nil {
		return checkpointRecoveryJournal{}, err
	}
	var journal checkpointRecoveryJournal
	if err := strictjson.Decode(encoded, &journal); err != nil {
		return checkpointRecoveryJournal{}, err
	}
	if err := journal.validate(plan); err != nil {
		return checkpointRecoveryJournal{}, err
	}
	return journal, nil
}

func validateCheckpointRecoveryArtifacts(journal checkpointRecoveryJournal) error {
	if journal.ValuesPath != "" {
		digest, err := releasequalification.DigestFile(journal.ValuesPath)
		if err != nil {
			return fmt.Errorf("checkpoint recovery values differ from the durable journal: %w", err)
		}
		if digest != journal.ValuesSHA256 {
			return fmt.Errorf("checkpoint recovery values differ from the durable journal")
		}
	}
	if journal.ArchiveSHA256 != "" {
		digest, err := releasequalification.DigestFile(journal.ArchivePath)
		if err != nil {
			return fmt.Errorf("checkpoint recovery archive differs from the durable journal: %w", err)
		}
		if digest != journal.ArchiveSHA256 {
			return fmt.Errorf("checkpoint recovery archive differs from the durable journal")
		}
		info, err := os.Lstat(journal.ArchivePath)
		if err != nil {
			return fmt.Errorf("checkpoint recovery archive size differs from the durable journal: %w", err)
		}
		if uint64(info.Size()) != journal.ArchiveBytes {
			return fmt.Errorf("checkpoint recovery archive size differs from the durable journal")
		}
	}
	return nil
}

func (backend *scalewayBackend) removeCheckpointRecoveryJournal(plan e2eplan.Plan) error {
	return removeDurableFile(backend.checkpointRecoveryPath(plan))
}

// recoverInterruptedCheckpoint runs before generic workload removal. Before
// the namespace deletion it only resumes the quiesced controller and removes
// the external workload. After deletion it deterministically replays the
// same-cluster restore on a freshly fenced run-owned pool.
func (backend *scalewayBackend) recoverInterruptedCheckpoint(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) error {
	journal, err := backend.readCheckpointRecoveryJournal(plan)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read interrupted checkpoint recovery journal: %w", err)
	}
	if err := validateCheckpointRecoveryArtifacts(journal); err != nil {
		return err
	}
	driverNamespacePresent, err := backend.exactRunNamespacePresent(ctx, request, plan, request.DriverNamespace)
	if err != nil {
		return err
	}
	if journal.Phase == checkpointPhaseControllerRestored && driverNamespacePresent {
		if err := backend.cleanupCheckpointNamespace(ctx, request, journal); err != nil {
			return err
		}
		if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
			"delete", "secret/sfs-subdir-checkpoint", "--ignore-not-found", "--wait=true", "--timeout=5m",
		); err != nil {
			return err
		}
		return backend.removeCheckpointRecoveryJournal(plan)
	}
	if journal.Phase != checkpointPhaseNamespaceDeleted && journal.Phase != checkpointPhaseControllerRestored && driverNamespacePresent {
		if journal.CheckpointRequestID != "" {
			if _, err := backend.runAdmin(ctx, request, "checkpoint", "resume",
				"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
				"--request-id="+journal.CheckpointRequestID, "--timeout=30m",
			); err != nil {
				controller, controllerErr := backend.singularPod(ctx, request, controllerSelector, "")
				if controllerErr != nil || !podReady(controller) {
					return fmt.Errorf("resume interrupted prepared checkpoint before cleanup: %w", errors.Join(err, controllerErr))
				}
			}
		}
		if err := backend.cleanupCheckpointNamespace(ctx, request, journal); err != nil {
			return err
		}
		return backend.removeCheckpointRecoveryJournal(plan)
	}
	if !checkpointRecoveryCanReplay(journal.Phase) {
		// The driver namespace disappeared before a complete checkpoint was
		// durably retained. There is no safe automated reconstruction.
		return fmt.Errorf("driver namespace disappeared before a complete checkpoint was retained")
	}
	return backend.replayCheckpointForCleanup(ctx, request, plan, inventory, journal)
}

func checkpointRecoveryCanReplay(phase string) bool {
	return phase == checkpointPhasePrepared ||
		phase == checkpointPhaseNamespaceDeleted ||
		phase == checkpointPhaseControllerRestored
}

func (backend *scalewayBackend) replayCheckpointForCleanup(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
	journal checkpointRecoveryJournal,
) error {
	clusterID := resourceID(inventory, e2ecleanup.ResourceKindCluster, 0)
	poolID := resourceID(inventory, e2ecleanup.ResourceKindNodePool, 0)
	parentIDs := []string{
		resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
		resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
	}
	if clusterID == "" || poolID == "" || parentIDs[0] == "" || parentIDs[1] == "" {
		return fmt.Errorf("checkpoint cleanup recovery lacks exact retained cloud identities")
	}
	if len(journal.NodeRetirements) == 0 {
		// Compatibility for the already-retained diagnostic journal. This is
		// still pre-mutation: every surviving exact Kapsule node, Instance, and
		// root volume remains observable. An old Instance already removed by
		// Kapsule is accepted only after exact node, Instance, and regional
		// attachment absence proof and carries no inferred root authority.
		retirements, err := backend.captureCheckpointNodeRetirements(
			ctx, plan, clusterID, poolID, journal.OldInstanceIDs, parentIDs,
		)
		if err != nil {
			return fmt.Errorf("arm legacy checkpoint node retirement: %w", err)
		}
		journal.NodeRetirements = retirements
		if err := backend.writeCheckpointRecoveryJournal(plan, journal); err != nil {
			return fmt.Errorf("retain exact legacy checkpoint node retirement: %w", err)
		}
	}

	if err := backend.deleteExactRunNamespaceIfPresent(ctx, request, plan); err != nil {
		return err
	}
	replacement, err := backend.replacePreRecoveryKapsuleNodes(
		ctx, plan, clusterID, poolID, parentIDs, journal.NodeRetirements,
	)
	if err != nil {
		return err
	}
	if err := backend.createRecoveryNamespaceAndSecrets(ctx, request, plan); err != nil {
		return err
	}
	if _, err := backend.runAdmin(ctx, request, "checkpoint", "restore",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+journal.CheckpointRequestID, "--archive-file="+journal.ArchivePath,
		"--identity-secret=scaleway-sfs-subdir-csi-identity", "--identity-key=installationID",
		"--mode=dry-run", "--timeout=30m",
	); err != nil {
		return err
	}
	executeBytes, err := backend.runAdmin(ctx, request, "checkpoint", "restore",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+journal.CheckpointRequestID, "--archive-file="+journal.ArchivePath,
		"--identity-secret=scaleway-sfs-subdir-csi-identity", "--identity-key=installationID",
		"--mode=execute", "--timeout=30m",
	)
	if err != nil {
		return err
	}
	restored, err := decodeCheckpointRestoreResult(executeBytes, "execute", journal.CheckpointRequestID, journal.PersistentVolume)
	if err != nil || restored.ArchiveSHA256 != journal.ArchiveSHA256 ||
		restored.ManifestSHA256 != journal.ManifestSHA256 {
		return fmt.Errorf("cleanup checkpoint restore differs from the retained journal: %w", err)
	}
	if err := backend.installRecoveryControllerOnly(ctx, request, journal.ValuesPath); err != nil {
		return err
	}
	_, provisionalLease, err := backend.waitForProvisionalRecovery(ctx, request)
	if err != nil {
		return err
	}
	if err := backend.verifyOnlyProvisionalParentAttachment(ctx, request, parentIDs, provisionalLease, replacement.InstanceIDs); err != nil {
		return err
	}
	approvalRequestID, approvalUID, err := backend.createMissingLeaseApproval(
		ctx, request, plan, journal.CheckpointRequestID, journal.ManifestSHA256, provisionalLease,
	)
	if err != nil {
		return err
	}
	// The immutable approval closes the pre-approval DaemonSet-absence proof.
	// Restore node plugins before waiting for the promoted controller so normal
	// rollout authorization can converge without a circular dependency.
	if err := backend.installFullRecoveredRelease(ctx, request, journal.ValuesPath); err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"rollout", "status", "deployment", "-l",
		"app.kubernetes.io/instance="+request.HelmRelease+","+controllerSelector, "--timeout=30m",
	); err != nil {
		return err
	}
	recoveredLease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return err
	}
	if recoveredLease.Metadata.UID != provisionalLease.Metadata.UID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionSecretUID"] != approvalUID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionRequestID"] != approvalRequestID {
		return fmt.Errorf("cleanup checkpoint replay lacks exact approval consumption")
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "secret/sfs-subdir-controller-approval", "--wait=true", "--timeout=5m",
	); err != nil {
		return err
	}
	journal.Phase = checkpointPhaseControllerRestored
	if err := backend.writeCheckpointRecoveryJournal(plan, journal); err != nil {
		return err
	}
	if err := backend.cleanupCheckpointNamespace(ctx, request, journal); err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "secret/sfs-subdir-checkpoint", "--ignore-not-found", "--wait=true", "--timeout=5m",
	); err != nil {
		return err
	}
	return backend.removeCheckpointRecoveryJournal(plan)
}

func (backend *scalewayBackend) cleanupCheckpointNamespace(
	ctx context.Context,
	request e2erunner.Request,
	journal checkpointRecoveryJournal,
) error {
	if _, err := backend.exactRunNamespacePresent(ctx, request, backend.plan, journal.WorkloadNamespace); err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil, "delete", "namespace/"+journal.WorkloadNamespace,
		"--ignore-not-found", "--wait=true", "--timeout=20m",
	); err != nil {
		return err
	}
	if journal.PersistentVolume == "" {
		return nil
	}
	return backend.waitPersistentVolumeAbsent(ctx, request, journal.PersistentVolume)
}

func (backend *scalewayBackend) waitPersistentVolumeAbsent(
	ctx context.Context,
	request e2erunner.Request,
	pvName string,
) error {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		encoded, err := backend.kubectl(waitCtx, request, nil,
			"get", "pv/"+pvName, "--ignore-not-found", "-o", "name",
		)
		if err == nil && strings.TrimSpace(string(encoded)) == "" {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for checkpoint workload PV cleanup: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) exactRunNamespacePresent(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
) (bool, error) {
	encoded, err := backend.kubectl(ctx, request, nil,
		"get", "namespace/"+namespace, "--ignore-not-found", "-o", "json",
	)
	if err != nil {
		return false, err
	}
	if len(encoded) == 0 {
		return false, nil
	}
	var observed struct {
		Metadata struct {
			Labels            map[string]string `json:"labels"`
			DeletionTimestamp *string           `json:"deletionTimestamp"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(encoded, &observed); err != nil {
		return false, err
	}
	if observed.Metadata.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		observed.Metadata.Labels["app.kubernetes.io/instance"] != request.HelmRelease {
		return false, fmt.Errorf("namespace %q is not owned by the exact run", namespace)
	}
	if observed.Metadata.DeletionTimestamp != nil {
		return false, nil
	}
	return true, nil
}

func (backend *scalewayBackend) deleteExactRunNamespaceIfPresent(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
) error {
	present, err := backend.exactRunNamespacePresent(ctx, request, plan, request.DriverNamespace)
	if err != nil {
		return err
	}
	_ = present
	_, err = backend.kubectl(ctx, request, nil,
		"delete", "namespace/"+request.DriverNamespace, "--ignore-not-found", "--wait=true", "--timeout=20m",
	)
	return err
}
