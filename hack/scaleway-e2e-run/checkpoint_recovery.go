package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type checkpointPrepareResult struct {
	SchemaVersion string `json:"schemaVersion"`
	RequestID     string `json:"requestID"`
	OutputFile    string `json:"outputFile"`
	Receipt       struct {
		RequestID      string `json:"requestID"`
		ManifestSHA256 string `json:"manifestSHA256"`
		ArchiveSHA256  string `json:"archiveSHA256"`
		ArchiveBytes   uint64 `json:"archiveBytes"`
		ArchiveFormat  string `json:"archiveFormat"`
	} `json:"receipt"`
}

type checkpointRestoreResult struct {
	SchemaVersion          string   `json:"schemaVersion"`
	CheckpointRequestID    string   `json:"checkpointRequestID"`
	Mode                   string   `json:"mode"`
	Ready                  bool     `json:"ready"`
	Completed              bool     `json:"completed"`
	ArchiveSHA256          string   `json:"archiveSHA256"`
	ArchiveBytes           uint64   `json:"archiveBytes"`
	ManifestSHA256         string   `json:"manifestSHA256"`
	PersistentVolumeNames  []string `json:"persistentVolumeNames"`
	CheckpointSecretStatus string   `json:"checkpointSecretStatus"`
}

type kapsuleNodeSet struct {
	Nodes       []*k8sapi.Node
	InstanceIDs []string
	NodeNames   []string
}

func (backend *scalewayBackend) runCheckpointRecoveryScenarios(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
	evidenceDirectory string,
) ([]e2erunner.ScenarioResult, error) {
	shortRun := plan.RunID[:8]
	workloadNamespace := "e2e-recovery-" + shortRun
	workloadClaim := "checkpoint-data-" + shortRun
	workloadDeployment := "checkpoint-workload-" + shortRun
	marker := "checkpoint-" + shortRun
	poolID := resourceID(inventory, e2ecleanup.ResourceKindNodePool, 0)
	clusterID := resourceID(inventory, e2ecleanup.ResourceKindCluster, 0)
	parentIDs := []string{
		resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
		resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
	}
	if poolID == "" || clusterID == "" || parentIDs[0] == "" || parentIDs[1] == "" {
		return nil, fmt.Errorf("checkpoint recovery lacks exact retained cloud identities")
	}

	oldNodes, err := backend.waitForKapsuleNodeSet(ctx, plan, clusterID, poolID, int(plan.NodePool.Count), nil)
	if err != nil {
		return nil, err
	}
	if err := backend.createCheckpointWorkload(ctx, request, plan, workloadNamespace, workloadClaim, workloadDeployment, marker); err != nil {
		return nil, err
	}
	persistentVolumeName, err := backend.workloadPersistentVolume(ctx, request, workloadNamespace, workloadClaim)
	if err != nil {
		return nil, err
	}

	valuesPath := filepath.Join(evidenceDirectory, "checkpoint-release-values-"+plan.RunID+".yaml")
	currentValues, err := backend.runHostCommand(ctx, nil, "helm", "get", "values", request.HelmRelease, "--namespace", request.DriverNamespace, "--output", "yaml")
	if err != nil {
		return nil, err
	}
	if err := replaceDurableFile(valuesPath, currentValues, 0o600); err != nil {
		return nil, fmt.Errorf("retain exact pre-recovery Helm values: %w", err)
	}

	checkpointRequestID, err := randomUUIDv4()
	if err != nil {
		return nil, err
	}
	archivePath := filepath.Join(evidenceDirectory, "checkpoint-"+checkpointRequestID+".tar")
	prepareBytes, err := backend.runAdmin(ctx, request, "checkpoint", "prepare",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+checkpointRequestID, "--output-file="+archivePath, "--timeout=30m")
	if err != nil {
		return nil, err
	}
	var prepared checkpointPrepareResult
	if err := strictjson.Decode(prepareBytes, &prepared); err != nil {
		return nil, fmt.Errorf("decode checkpoint prepare receipt: %w", err)
	}
	if prepared.SchemaVersion != "1" || prepared.RequestID != checkpointRequestID || prepared.OutputFile != archivePath ||
		prepared.Receipt.RequestID != checkpointRequestID || prepared.Receipt.ManifestSHA256 == "" ||
		prepared.Receipt.ArchiveSHA256 == "" || prepared.Receipt.ArchiveBytes == 0 || prepared.Receipt.ArchiveFormat != "checkpoint-tar-v1" {
		return nil, fmt.Errorf("checkpoint prepare receipt is incomplete")
	}
	if err := backend.waitForControllerUnready(ctx, request); err != nil {
		return nil, err
	}
	historicalHolderOnlyRejected, staleOwnershipRejected, differentClusterUIDRejected, err := verifyRecoveryNegativeProjections(ctx, archivePath, plan.RunID)
	if err != nil {
		return nil, err
	}

	if _, err := backend.kubectl(ctx, request, nil, "delete", "namespace/"+request.DriverNamespace, "--wait=true", "--timeout=20m"); err != nil {
		return nil, fmt.Errorf("delete exact driver namespace for checkpoint recovery: %w", err)
	}
	if err := backend.scalePoolAndWait(ctx, plan, clusterID, poolID, 0, oldNodes.InstanceIDs); err != nil {
		return nil, err
	}
	for _, parentID := range parentIDs {
		for _, instanceID := range oldNodes.InstanceIDs {
			if _, err := backend.waitRegionalAttachment(ctx, parentID, instanceID, false); err != nil {
				return nil, fmt.Errorf("wait for old Instance %s attachment cleanup on %s: %w", instanceID, parentID, err)
			}
		}
		listed, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{Region: scw.Region(plan.Region), FilesystemID: &parentID}, scw.WithAllPages(), scw.WithContext(ctx))
		if err != nil {
			return nil, fmt.Errorf("list parent %s attachments after the pool reached zero: %w", parentID, err)
		}
		if listed == nil || len(listed.Attachments) != 0 {
			return nil, fmt.Errorf("parent %s retains an old or unknown attachment after the pool reached zero", parentID)
		}
	}
	if err := backend.createRecoveryNamespaceAndSecrets(ctx, request, plan); err != nil {
		return nil, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "get", "pv/"+persistentVolumeName); err != nil {
		return nil, fmt.Errorf("surviving checkpoint PV is absent: %w", err)
	}
	archiveBytes, err := os.ReadFile(archivePath)
	if err != nil {
		return nil, fmt.Errorf("read completed checkpoint for interrupted-export probe: %w", err)
	}
	if len(archiveBytes) < 2 {
		return nil, fmt.Errorf("completed checkpoint is too short for the interrupted-export probe")
	}
	incompleteArchive := filepath.Join(evidenceDirectory, "checkpoint-incomplete-"+checkpointRequestID+".tar")
	if err := replaceDurableFile(incompleteArchive, archiveBytes[:len(archiveBytes)/2], 0o600); err != nil {
		return nil, err
	}
	if _, err := backend.runAdmin(ctx, request, "checkpoint", "restore",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+checkpointRequestID, "--archive-file="+incompleteArchive,
		"--identity-secret=scaleway-sfs-subdir-csi-identity", "--identity-key=installationID",
		"--mode=dry-run", "--timeout=30m"); err == nil {
		return nil, fmt.Errorf("checkpoint restore accepted an interrupted export")
	}
	exportInProgressRejected := true

	dryRunBytes, err := backend.runAdmin(ctx, request, "checkpoint", "restore",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+checkpointRequestID, "--archive-file="+archivePath,
		"--identity-secret=scaleway-sfs-subdir-csi-identity", "--identity-key=installationID",
		"--mode=dry-run", "--timeout=30m")
	if err != nil {
		return nil, err
	}
	executeBytes, err := backend.runAdmin(ctx, request, "checkpoint", "restore",
		"--namespace="+request.DriverNamespace, "--release="+request.HelmRelease,
		"--request-id="+checkpointRequestID, "--archive-file="+archivePath,
		"--identity-secret=scaleway-sfs-subdir-csi-identity", "--identity-key=installationID",
		"--mode=execute", "--timeout=30m")
	if err != nil {
		return nil, err
	}
	dryRun, err := decodeCheckpointRestoreResult(dryRunBytes, "dry-run", checkpointRequestID, persistentVolumeName)
	if err != nil {
		return nil, err
	}
	executed, err := decodeCheckpointRestoreResult(executeBytes, "execute", checkpointRequestID, persistentVolumeName)
	if err != nil {
		return nil, err
	}
	if dryRun.ArchiveSHA256 != prepared.Receipt.ArchiveSHA256 || executed.ArchiveSHA256 != prepared.Receipt.ArchiveSHA256 ||
		dryRun.ManifestSHA256 != prepared.Receipt.ManifestSHA256 || executed.ManifestSHA256 != prepared.Receipt.ManifestSHA256 ||
		executed.CheckpointSecretStatus != "created" {
		return nil, fmt.Errorf("checkpoint restore receipt differs from the prepared archive")
	}
	checkpointSecretImmutable, err := backend.verifyCheckpointSecretImmutable(ctx, request)
	if err != nil {
		return nil, err
	}

	if err := backend.scalePoolAndWait(ctx, plan, clusterID, poolID, plan.NodePool.Count, oldNodes.InstanceIDs); err != nil {
		return nil, err
	}
	replacementNodes, err := backend.waitForKapsuleNodeSet(ctx, plan, clusterID, poolID, int(plan.NodePool.Count), oldNodes.InstanceIDs)
	if err != nil {
		return nil, err
	}
	if err := backend.installRecoveryControllerOnly(ctx, request, valuesPath); err != nil {
		return nil, err
	}
	provisional, provisionalLease, err := backend.waitForProvisionalRecovery(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := backend.verifyOnlyProvisionalParentAttachment(ctx, request, parentIDs, provisionalLease, replacementNodes.InstanceIDs); err != nil {
		return nil, err
	}

	pendingClaim := "recovery-pending-" + shortRun
	if err := backend.applyNamespacedPVC(ctx, request, plan, request.DriverNamespace, pendingClaim, "sfs-subdir-rwx", "missing-lease-recovery"); err != nil {
		return nil, err
	}
	if err := backend.requirePVCUnbound(ctx, request, request.DriverNamespace, pendingClaim, 45*time.Second); err != nil {
		return nil, err
	}
	approvalRequestID, approvalUID, err := backend.createMissingLeaseApproval(
		ctx, request, plan, checkpointRequestID, prepared.Receipt.ManifestSHA256, provisionalLease,
	)
	if err != nil {
		return nil, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "status", "deployment", "-l", "app.kubernetes.io/instance="+request.HelmRelease+","+controllerSelector, "--timeout=30m"); err != nil {
		return nil, err
	}
	recovered, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return nil, fmt.Errorf("missing-Lease controller did not recover in the provisional Pod: %w", err)
	}
	if recovered.Metadata.UID != provisional.Metadata.UID || !podReady(recovered) {
		return nil, fmt.Errorf("missing-Lease controller did not recover in the provisional Pod")
	}
	recoveredLease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("read the recovered missing-Lease controller Lease: %w", err)
	}
	if recoveredLease.Metadata.UID != provisionalLease.Metadata.UID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionSecretUID"] != approvalUID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionRequestID"] != approvalRequestID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionMode"] != "missing-lease-recovery" ||
		recoveredLease.Metadata.Annotations["approvalConsumptionPodUID"] != recovered.Metadata.UID {
		return nil, fmt.Errorf("missing-Lease approval consumption evidence is incomplete")
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "wait", "pvc/"+pendingClaim, "--for=jsonpath={.status.phase}=Bound", "--timeout=10m"); err != nil {
		return nil, fmt.Errorf("new provisioning did not recover after missing-Lease approval: %w", err)
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "delete", "secret/sfs-subdir-controller-approval", "--wait=true", "--timeout=5m"); err != nil {
		return nil, err
	}
	if err := backend.installFullRecoveredRelease(ctx, request, valuesPath); err != nil {
		return nil, err
	}
	if err := backend.waitForCheckpointWorkloadMarker(ctx, request, workloadNamespace, workloadDeployment, marker); err != nil {
		return nil, err
	}
	archiveVerified, deleteVerified, tombstonesVerified, err := backend.verifyRecoveredLifecycles(ctx, request, plan, workloadNamespace, marker)
	if err != nil {
		return nil, err
	}
	if err := backend.cleanupCheckpointWorkload(ctx, request, workloadNamespace, persistentVolumeName); err != nil {
		return nil, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "delete", "secret/sfs-subdir-checkpoint", "--wait=true", "--timeout=5m"); err != nil {
		return nil, err
	}

	checkpointProof := e2erunner.CheckpointRestoreProof{
		SchemaVersion: "1", Scenario: "checkpoint-and-restore", RunID: plan.RunID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
		CheckpointRequestID: checkpointRequestID, ArchiveSHA256: prepared.Receipt.ArchiveSHA256,
		ArchiveBytes: prepared.Receipt.ArchiveBytes, ManifestSHA256: prepared.Receipt.ManifestSHA256,
		WorkloadNamespace: workloadNamespace, WorkloadClaimName: workloadClaim, PersistentVolumeName: persistentVolumeName,
		OldInstanceIDs: slices.Clone(oldNodes.InstanceIDs), ReplacementInstanceIDs: slices.Clone(replacementNodes.InstanceIDs),
		PrepareCompleted: true, ControllerQuiesced: true, DriverNamespaceDeleted: true, DriverNamespaceRecreated: true,
		PersistentVolumePreserved: true, RestoreDryRunCompleted: dryRun.Ready, RestoreExecuteCompleted: executed.Completed,
		CheckpointSecretImmutable: checkpointSecretImmutable, CheckpointSecretDeletedAfterAudit: true,
		OldPoolScaledToZero: true, AllOldInstancesAbsent: true,
		PoolRestoredWithFreshInstances: true, ExistingMarkerReadAfterRecovery: true, NewProvisioningSucceeded: true,
		ArchiveLifecycleVerified: archiveVerified, DeleteLifecycleVerified: deleteVerified,
		TombstoneInventoryVerified: tombstonesVerified, ExternalWorkloadCleanupCompleted: true,
	}
	missingProof := e2erunner.MissingLeaseRecoveryProof{
		SchemaVersion: "1", Scenario: "missing-lease-recovery", RunID: plan.RunID, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
		CheckpointRequestID: checkpointRequestID, CheckpointManifestSHA256: prepared.Receipt.ManifestSHA256,
		LeaseUID: provisionalLease.Metadata.UID, ProvisionalControllerPodUID: provisional.Metadata.UID,
		RecoveredControllerPodUID: recovered.Metadata.UID, ApprovalSecretUID: approvalUID, ApprovalRequestID: approvalRequestID,
		RecoveryFenceScope: "all-pre-recovery-instances", OldInstanceIDs: slices.Clone(oldNodes.InstanceIDs),
		ReplacementInstanceIDs: slices.Clone(replacementNodes.InstanceIDs), LeaseAbsentBeforeController: true,
		ProvisionalLeasePersisted: true, ControllerNonServingBeforeFence: true, NodeDaemonSetAbsentBeforeApproval: true,
		OldAttachmentsAbsent: true, OnlyProvisionalAttachmentPresent: true, ApprovalCreatedAfterCondition: true,
		ApprovalConsumed: true, LeaseUIDPreserved: true, ControllerServingAfterApproval: true,
		ApprovalSecretDeletedAfterAudit: true, HistoricalHolderOnlyRejected: historicalHolderOnlyRejected, ExportInProgressRejected: exportInProgressRejected,
		StaleOwnershipRejected: staleOwnershipRejected, DifferentClusterUIDRejected: differentClusterUIDRejected,
	}
	if err := checkpointProof.Validate(); err != nil {
		return nil, err
	}
	if err := missingProof.Validate(); err != nil {
		return nil, err
	}
	checkpointResult, err := writeScenarioJSON(evidenceDirectory, "checkpoint-and-restore", checkpointProof)
	if err != nil {
		return nil, err
	}
	missingResult, err := writeScenarioJSON(evidenceDirectory, "missing-lease-recovery", missingProof)
	if err != nil {
		return nil, err
	}
	return []e2erunner.ScenarioResult{checkpointResult, missingResult}, nil
}

func decodeCheckpointRestoreResult(encoded []byte, mode, requestID, pvName string) (checkpointRestoreResult, error) {
	var result checkpointRestoreResult
	if err := json.Unmarshal(encoded, &result); err != nil {
		return result, fmt.Errorf("decode checkpoint %s result: %w", mode, err)
	}
	if result.SchemaVersion != "1" || result.CheckpointRequestID != requestID || result.Mode != mode || !result.Ready ||
		result.ArchiveSHA256 == "" || result.ManifestSHA256 == "" || result.ArchiveBytes == 0 ||
		!slices.Contains(result.PersistentVolumeNames, pvName) {
		return result, fmt.Errorf("checkpoint %s result is incomplete", mode)
	}
	if mode == "execute" && (!result.Completed || result.CheckpointSecretStatus == "") {
		return result, fmt.Errorf("checkpoint execute did not complete")
	}
	return result, nil
}

func verifyRecoveryNegativeProjections(ctx context.Context, archivePath, installationID string) (bool, bool, bool, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return false, false, false, fmt.Errorf("open checkpoint negative-projection source: %w", err)
	}
	decoded, decodeErr := recovery.ReadCheckpointArchive(ctx, file)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		return false, false, false, fmt.Errorf("decode checkpoint negative-projection source: %w", errors.Join(decodeErr, closeErr))
	}
	manifest := decoded.Manifest
	exact := recovery.RestoredCheckpointState{
		DriverName: manifest.DriverName, InstallationID: installationID, ActiveClusterUID: manifest.ActiveClusterUID,
		ChartVersion: manifest.ChartVersion, Images: slices.Clone(manifest.Images),
		KubernetesObjects: manifest.KubernetesObjects, Parents: slices.Clone(manifest.Parents),
	}
	if err := recovery.VerifyRestoredCheckpoint(manifest, exact); err != nil {
		return false, false, false, fmt.Errorf("exact checkpoint projection did not verify: %w", err)
	}
	stale := exact
	stale.Parents = slices.Clone(exact.Parents)
	if len(stale.Parents) == 0 {
		return false, false, false, fmt.Errorf("checkpoint negative projection has no configured parent")
	}
	stale.Parents[0].AggregateSHA256 = "sha256:" + strings.Repeat("f", 64)
	staleRejected := recovery.VerifyRestoredCheckpoint(manifest, stale) != nil
	differentCluster := exact
	differentCluster.ActiveClusterUID = "different-synthetic-cluster"
	differentClusterRejected := recovery.VerifyRestoredCheckpoint(manifest, differentCluster) != nil
	if !staleRejected || !differentClusterRejected {
		return false, false, false, fmt.Errorf("checkpoint negative projection was unexpectedly authorized")
	}
	now := time.Now().UTC()
	holder := manifest.HolderEvidence
	historicalOnly := coordination.OperatorApproval{
		SecretUID: manifest.CheckpointRequestID, Immutable: true, SchemaVersion: "1",
		Mode: coordination.ApprovalMissingLeaseRecovery, RequestID: manifest.CheckpointRequestID,
		InstallationID: installationID, ActiveClusterUID: manifest.ActiveClusterUID,
		PreviousHolderPodUID: holder.PodUID, PreviousHolderNodeName: holder.NodeName,
		PreviousHolderCSINodeID: holder.CSINodeID, PreviousHolderInstanceID: holder.InstanceID,
		PreviousHolderZone: holder.Zone, CheckpointRequestID: manifest.CheckpointRequestID,
		CheckpointManifestSHA256: decoded.ManifestSHA256, RecoveryFenceScope: "",
		Reason: "historical checkpoint holder only", ApprovedAt: now.Add(-30 * time.Second).Format(time.RFC3339Nano),
		ExpiresAt: now.Add(30 * time.Minute).Format(time.RFC3339Nano),
	}
	historicalErr := historicalOnly.ValidateAt(now, now.Add(-time.Minute))
	historicalRejected := historicalErr != nil && strings.Contains(historicalErr.Error(), "fence scope")
	if !historicalRejected {
		return false, false, false, fmt.Errorf("historical-holder-only approval was not rejected by the production approval contract: %w", historicalErr)
	}
	return true, true, true, nil
}

func (backend *scalewayBackend) waitForControllerUnready(ctx context.Context, request e2erunner.Request) error {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		pod, err := backend.singularPod(waitCtx, request, controllerSelector, "")
		if err == nil && !podReady(pod) {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for checkpoint-quiesced controller readiness: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) runAdmin(ctx context.Context, request e2erunner.Request, arguments ...string) ([]byte, error) {
	arguments = append(arguments, "--kubeconfig="+backend.kubeconfig)
	return backend.runHostCommand(ctx, nil, request.AdminBinary, arguments...)
}

func (backend *scalewayBackend) runHostCommand(ctx context.Context, stdin *strings.Reader, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	// Helm and csi-admin must target only the kubeconfig retained for this
	// exact run. They never call Scaleway directly, so do not leak provider
	// credentials into their process environment.
	command.Env = append(environmentWithoutScalewayCredentials(), "KUBECONFIG="+backend.kubeconfig)
	if stdin != nil {
		command.Stdin = stdin
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if len(message) > 2048 {
			message = message[:2048]
		}
		return nil, fmt.Errorf("run %s: %w: %s", filepath.Base(name), err, message)
	}
	return output, nil
}

func (backend *scalewayBackend) createCheckpointWorkload(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, namespace, claim, deployment, marker string) error {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
  labels: {sfs-subdir-e2e-run: %q}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: sfs-subdir-rwx
  resources: {requests: {storage: 16Mi}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
spec:
  replicas: 1
  selector: {matchLabels: {sfs-subdir-e2e-workload: %s}}
  template:
    metadata:
      labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint, sfs-subdir-e2e-workload: %s}
    spec:
      containers:
        - name: workload
          image: %s
          command: ["sh", "-c", "test -e /data/checkpoint-marker || { printf %s > /data/checkpoint-marker; sync; }; test \"$(cat /data/checkpoint-marker)\" = %s; sleep 3600"]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %s}
`, namespace, plan.RunID, claim, namespace, plan.RunID, deployment, namespace, plan.RunID, deployment,
		plan.RunID, deployment, request.WorkloadImage, marker, marker, claim)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return err
	}
	_, err := backend.kubectl(ctx, request, nil, "-n", namespace, "rollout", "status", "deployment/"+deployment, "--timeout=15m")
	return err
}

func (backend *scalewayBackend) workloadPersistentVolume(ctx context.Context, request e2erunner.Request, namespace, claim string) (string, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", namespace, "get", "pvc/"+claim, "-o", "jsonpath={.spec.volumeName}")
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(encoded))
	if name == "" {
		return "", fmt.Errorf("checkpoint workload PVC has no bound PV")
	}
	return name, nil
}

func (backend *scalewayBackend) scalePoolAndWait(ctx context.Context, plan e2eplan.Plan, clusterID, poolID string, size uint32, oldInstanceIDs []string) error {
	if _, err := backend.kubernetes.UpdatePool(&k8sapi.UpdatePoolRequest{Region: scw.Region(plan.Region), PoolID: poolID, Size: &size}, scw.WithContext(ctx)); err != nil {
		return fmt.Errorf("scale exact recovery pool to %d: %w", size, err)
	}
	timeout := 30 * time.Minute
	if _, err := backend.kubernetes.WaitForPool(&k8sapi.WaitForPoolRequest{Region: scw.Region(plan.Region), PoolID: poolID, Timeout: &timeout}, scw.WithContext(ctx)); err != nil {
		return fmt.Errorf("wait for exact recovery pool size %d: %w", size, err)
	}
	if size == 0 {
		if _, err := backend.waitForKapsuleNodeSet(ctx, plan, clusterID, poolID, 0, nil); err != nil {
			return err
		}
		for _, instanceID := range oldInstanceIDs {
			if err := backend.waitInstanceAbsent(ctx, scw.Zone(backend.request.Zone), instanceID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (backend *scalewayBackend) waitForKapsuleNodeSet(ctx context.Context, plan e2eplan.Plan, clusterID, poolID string, count int, excluded []string) (kapsuleNodeSet, error) {
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, id := range excluded {
		excludedSet[id] = struct{}{}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{Region: scw.Region(plan.Region), ClusterID: clusterID, PoolID: &poolID}, scw.WithAllPages(), scw.WithContext(waitCtx))
		if err == nil && listed != nil && len(listed.Nodes) == count {
			result := kapsuleNodeSet{Nodes: slices.Clone(listed.Nodes)}
			valid := true
			seen := make(map[string]struct{}, count)
			for _, node := range listed.Nodes {
				if node == nil || node.ClusterID != clusterID || node.PoolID != poolID || node.Region.String() != plan.Region || node.Status != k8sapi.NodeStatusReady || node.Name == "" {
					valid = false
					break
				}
				instanceID := providerIDResource(node.ProviderID)
				if err := volume.ValidateOperationID(instanceID); err != nil {
					valid = false
					break
				}
				if _, old := excludedSet[instanceID]; old {
					valid = false
					break
				}
				if _, duplicate := seen[instanceID]; duplicate {
					valid = false
					break
				}
				seen[instanceID] = struct{}{}
				result.InstanceIDs = append(result.InstanceIDs, instanceID)
				result.NodeNames = append(result.NodeNames, node.Name)
			}
			if valid {
				slices.Sort(result.InstanceIDs)
				slices.Sort(result.NodeNames)
				return result, nil
			}
		}
		select {
		case <-waitCtx.Done():
			return kapsuleNodeSet{}, fmt.Errorf("wait for exact %d-node Kapsule recovery set: %w", count, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func providerIDResource(providerID string) string {
	index := strings.LastIndex(providerID, "/")
	if index < 0 || index == len(providerID)-1 {
		return ""
	}
	return providerID[index+1:]
}

func (backend *scalewayBackend) createRecoveryNamespaceAndSecrets(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan) error {
	accessKey, accessPresent := os.LookupEnv("SCW_ACCESS_KEY")
	secretKey, secretPresent := os.LookupEnv("SCW_SECRET_KEY")
	if !accessPresent || !secretPresent || accessKey == "" || secretKey == "" {
		return fmt.Errorf("recovery credentials are absent from the approved runner environment")
	}
	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "List", "items": []any{
			map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{
				"name": request.DriverNamespace, "labels": map[string]string{
					"sfs-subdir-e2e-run": plan.RunID, "pod-security.kubernetes.io/enforce": "privileged",
					"pod-security.kubernetes.io/audit": "privileged", "pod-security.kubernetes.io/warn": "privileged",
				},
			}},
			map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "scaleway-sfs-subdir-csi-identity", "namespace": request.DriverNamespace}, "type": "Opaque", "stringData": map[string]string{"installationID": plan.RunID}},
			map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "scaleway-sfs-subdir-csi-credentials", "namespace": request.DriverNamespace}, "type": "Opaque", "stringData": map[string]string{"SCW_ACCESS_KEY": accessKey, "SCW_SECRET_KEY": secretKey}},
		},
	})
	if err != nil {
		return err
	}
	_, err = backend.kubectl(ctx, request, strings.NewReader(string(manifest)), "create", "-f", "-")
	return err
}

func (backend *scalewayBackend) installRecoveryControllerOnly(ctx context.Context, request e2erunner.Request, valuesPath string) error {
	postRenderer := filepath.Join(filepath.Dir(backend.scenarioTool), "e2e-helm-controller-only-postrenderer.sh")
	leaseName, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "lease/scaleway-sfs-subdir-csi-controller", "--ignore-not-found", "-o", "name")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(leaseName)) != "" {
		return fmt.Errorf("controller Lease exists before provisional recovery install")
	}
	if _, err := backend.runHostCommand(ctx, nil, "helm", "upgrade", "--install", request.HelmRelease, request.ChartPackage,
		"--namespace", request.DriverNamespace, "--values", valuesPath, "--post-renderer", postRenderer, "--timeout", "30m"); err != nil {
		return err
	}
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "daemonset", "-l", "app.kubernetes.io/instance="+request.HelmRelease, "-o", "json")
	if err != nil {
		return err
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
	}
	if json.Unmarshal(encoded, &list) != nil || len(list.Items) != 0 {
		return fmt.Errorf("recovery Helm release started a node DaemonSet before approval")
	}
	return nil
}

func (backend *scalewayBackend) installFullRecoveredRelease(ctx context.Context, request e2erunner.Request, valuesPath string) error {
	if _, err := backend.runHostCommand(ctx, nil, "helm", "upgrade", request.HelmRelease, request.ChartPackage,
		"--namespace", request.DriverNamespace, "--values", valuesPath, "--wait", "--timeout", "30m"); err != nil {
		return err
	}
	_, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "status", "daemonset", "-l", "app.kubernetes.io/instance="+request.HelmRelease+","+nodeSelector, "--timeout=20m")
	return err
}

func (backend *scalewayBackend) waitForProvisionalRecovery(ctx context.Context, request e2erunner.Request) (kubernetesPod, kubernetesLease, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		pod, podErr := backend.singularPod(waitCtx, request, controllerSelector, "")
		lease, leaseErr := backend.readControllerLease(waitCtx, request)
		if podErr == nil && leaseErr == nil && !podReady(pod) && lease.Spec.HolderIdentity == pod.Metadata.UID &&
			lease.Metadata.Annotations["sfs-subdir-discovery-state"] == "Provisional" &&
			lease.Metadata.Annotations["sfs-subdir-discovery-holder-pod-uid"] == pod.Metadata.UID &&
			lease.Metadata.Annotations["sfs-subdir-discovery-observed-at"] != "" {
			return pod, lease, nil
		}
		select {
		case <-waitCtx.Done():
			return kubernetesPod{}, kubernetesLease{}, fmt.Errorf("wait for provisional missing-Lease controller: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) verifyOnlyProvisionalParentAttachment(ctx context.Context, request e2erunner.Request, parentIDs []string, lease kubernetesLease, replacementIDs []string) error {
	if len(parentIDs) != 2 {
		return fmt.Errorf("provisional recovery requires the exact retained two-parent inventory")
	}
	parentID := parentIDs[0]
	nodeID := lease.Metadata.Annotations["holderCSINodeID"]
	instanceID := lease.Metadata.Annotations["holderInstanceID"]
	zone := lease.Metadata.Annotations["holderZone"]
	if !slices.Contains(replacementIDs, instanceID) || !strings.HasSuffix(nodeID, "/"+instanceID) {
		return fmt.Errorf("provisional controller is outside the exact replacement pool")
	}
	listed, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{Region: scw.Region(backend.plan.Region), FilesystemID: &parentID}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return err
	}
	parent, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: scw.Region(backend.plan.Region), FilesystemID: parentID}, scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("configured recovery parent is not authoritatively available: %w", err)
	}
	if parent == nil || parent.Status != fileapi.FileSystemStatusAvailable || parent.NumberOfAttachments != 1 {
		return fmt.Errorf("configured recovery parent is not authoritatively available")
	}
	if listed == nil || len(listed.Attachments) != 1 || listed.Attachments[0] == nil || listed.Attachments[0].ResourceID != instanceID ||
		listed.Attachments[0].FilesystemID != parentID || listed.Attachments[0].ResourceType != fileapi.AttachmentResourceTypeInstanceServer ||
		listed.Attachments[0].Zone == nil || listed.Attachments[0].Zone.String() != zone {
		return fmt.Errorf("configured parent is not attached only to the provisional controller Instance")
	}
	serverResponse, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: scw.Zone(zone), ServerID: instanceID}, scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("read provisional controller Instance inventory: %w", err)
	}
	if serverResponse == nil || serverResponse.Server == nil || serverResponse.Server.Project != backend.plan.ProjectID ||
		serverResponse.Server.State != instanceapi.ServerStateRunning || len(serverResponse.Server.Filesystems) != 1 ||
		serverResponse.Server.Filesystems[0] == nil || serverResponse.Server.Filesystems[0].FilesystemID != parentID ||
		serverResponse.Server.Filesystems[0].State != instanceapi.ServerFilesystemStateAvailable {
		return fmt.Errorf("provisional controller Instance inventory does not exactly match the configured parent")
	}
	decommissioned, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{Region: scw.Region(backend.plan.Region), FilesystemID: &parentIDs[1]}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("historical decommissioned parent was reattached during recovery: %w", err)
	}
	if decommissioned == nil || len(decommissioned.Attachments) != 0 {
		return fmt.Errorf("historical decommissioned parent was reattached during recovery")
	}
	return nil
}

func (backend *scalewayBackend) verifyCheckpointSecretImmutable(ctx context.Context, request e2erunner.Request) (bool, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "secret/sfs-subdir-checkpoint", "-o", "json")
	if err != nil {
		return false, err
	}
	var secret struct {
		Immutable bool              `json:"immutable"`
		Type      string            `json:"type"`
		Data      map[string]string `json:"data"`
	}
	if err := json.Unmarshal(encoded, &secret); err != nil {
		return false, err
	}
	if !secret.Immutable || secret.Type != "Opaque" || len(secret.Data) != 1 || secret.Data["checkpoint.json"] == "" {
		return false, fmt.Errorf("restored checkpoint Secret is not the exact immutable envelope")
	}
	return true, nil
}

func (backend *scalewayBackend) applyNamespacedPVC(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, namespace, name, storageClass, scenario string) error {
	manifest := fmt.Sprintf("apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    sfs-subdir-e2e-run: %q\n    sfs-subdir-e2e-scenario: %s\nspec:\n  accessModes: [ReadWriteMany]\n  storageClassName: %s\n  resources: {requests: {storage: 16Mi}}\n", name, namespace, plan.RunID, scenario, storageClass)
	_, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-")
	return err
}

func (backend *scalewayBackend) requirePVCUnbound(ctx context.Context, request e2erunner.Request, namespace, name string, duration time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		encoded, err := backend.kubectl(waitCtx, request, nil, "-n", namespace, "get", "pvc/"+name, "-o", "jsonpath={.status.phase}")
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(encoded)) == "Bound" {
			return fmt.Errorf("controller provisioned PVC %s before missing-Lease approval", name)
		}
		select {
		case <-waitCtx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) createMissingLeaseApproval(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, checkpointRequestID, manifestSHA string, lease kubernetesLease) (string, string, error) {
	requestID, err := randomUUIDv4()
	if err != nil {
		return "", "", err
	}
	observedAt, err := time.Parse(time.RFC3339Nano, lease.Metadata.Annotations["sfs-subdir-discovery-observed-at"])
	if err != nil {
		return "", "", err
	}
	approvedAt := time.Now().UTC()
	if !approvedAt.After(observedAt) {
		return "", "", fmt.Errorf("approval clock did not advance beyond provisional observation")
	}
	stringData := map[string]string{
		"schemaVersion": "1", "mode": "missing-lease-recovery", "requestID": requestID,
		"installationID": plan.RunID, "activeClusterUID": lease.Metadata.Annotations["holderActiveClusterUID"],
		"previousHolderPodUID": "", "previousHolderNodeName": "", "previousHolderCSINodeID": "",
		"previousHolderInstanceID": "", "previousHolderZone": "", "checkpointRequestID": checkpointRequestID,
		"checkpointManifestSHA256": manifestSHA, "recoveryFenceScope": "all-pre-recovery-instances",
		"reason": "real E2E same-cluster checkpoint recovery", "approvedAt": approvedAt.Format(time.RFC3339Nano),
		"expiresAt": approvedAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	}
	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "Secret", "immutable": true, "type": "Opaque",
		"metadata": map[string]any{"name": "sfs-subdir-controller-approval", "namespace": request.DriverNamespace}, "stringData": stringData,
	})
	if err != nil {
		return "", "", err
	}
	if _, err := backend.kubectl(ctx, request, strings.NewReader(string(manifest)), "create", "-f", "-"); err != nil {
		return "", "", err
	}
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "secret/sfs-subdir-controller-approval", "-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return "", "", fmt.Errorf("read missing-Lease approval Secret UID: %w", err)
	}
	if strings.TrimSpace(string(encoded)) == "" {
		return "", "", fmt.Errorf("missing-Lease approval Secret has an empty UID")
	}
	return requestID, strings.TrimSpace(string(encoded)), nil
}

func (backend *scalewayBackend) waitForCheckpointWorkloadMarker(ctx context.Context, request e2erunner.Request, namespace, deployment, marker string) error {
	if _, err := backend.kubectl(ctx, request, nil, "-n", namespace, "rollout", "status", "deployment/"+deployment, "--timeout=20m"); err != nil {
		return err
	}
	pod, err := backend.singularNamespacedPod(ctx, request, namespace, "sfs-subdir-e2e-workload="+deployment)
	if err != nil {
		return err
	}
	_, err = backend.kubectl(ctx, request, nil, "-n", namespace, "exec", pod.Metadata.Name, "--", "sh", "-c", "test \"$(cat /data/checkpoint-marker)\" = "+marker)
	return err
}

func (backend *scalewayBackend) singularNamespacedPod(ctx context.Context, request e2erunner.Request, namespace, selector string) (kubernetesPod, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", namespace, "get", "pods", "-l", selector, "-o", "json")
	if err != nil {
		return kubernetesPod{}, err
	}
	var list kubernetesPodList
	if err := json.Unmarshal(encoded, &list); err != nil {
		return kubernetesPod{}, err
	}
	live := make([]kubernetesPod, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Metadata.DeletionTimestamp == nil && podReady(pod) {
			live = append(live, pod)
		}
	}
	if len(live) != 1 {
		return kubernetesPod{}, fmt.Errorf("selector %q in namespace %q returned %d Ready Pods", selector, namespace, len(live))
	}
	return live[0], nil
}

func (backend *scalewayBackend) verifyRecoveredLifecycles(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, namespace, marker string) (bool, bool, bool, error) {
	restoredTerminalRecords, err := backend.countTerminalAllocationRecords(ctx, request)
	if err != nil {
		return false, false, false, fmt.Errorf("restored checkpoint has no terminal tombstone inventory: %w", err)
	}
	if restoredTerminalRecords == 0 {
		return false, false, false, fmt.Errorf("restored checkpoint has no terminal tombstone inventory")
	}
	driver, err := backend.storageClassDriver(ctx, request)
	if err != nil {
		return false, false, false, err
	}
	shortRun := plan.RunID[:8]
	archiveClass := "e2e-recovery-archive-" + shortRun
	deleteClass := "e2e-recovery-delete-" + shortRun
	classes := fmt.Sprintf(`apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
provisioner: %s
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: false
parameters: {poolName: standard, onDelete: archive, directoryMode: "0770", directoryUid: "1000", directoryGid: "1000"}
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
provisioner: %s
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: false
parameters: {poolName: standard, onDelete: delete, directoryMode: "0770", directoryUid: "1000", directoryGid: "1000"}
`, archiveClass, plan.RunID, driver, deleteClass, plan.RunID, driver)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(classes), "apply", "-f", "-"); err != nil {
		return false, false, false, err
	}
	archiveClaim := "recovery-archive-" + shortRun
	deleteClaim := "recovery-delete-" + shortRun
	for _, item := range []struct{ name, class string }{{archiveClaim, archiveClass}, {deleteClaim, deleteClass}} {
		if err := backend.applyNamespacedPVC(ctx, request, plan, namespace, item.name, item.class, "checkpoint"); err != nil {
			return false, false, false, err
		}
		if _, err := backend.kubectl(ctx, request, nil, "-n", namespace, "wait", "pvc/"+item.name, "--for=jsonpath={.status.phase}=Bound", "--timeout=10m"); err != nil {
			return false, false, false, err
		}
	}
	lifecyclePods := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: recovery-archive-writer-%s
  namespace: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
spec:
  restartPolicy: Never
  containers:
    - name: workload
      image: %s
      command: ["sh", "-c", "printf recovered-archive > /data/lifecycle-marker; sync; test \"$(cat /data/lifecycle-marker)\" = recovered-archive; sleep 3600"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, persistentVolumeClaim: {claimName: %s}}]
---
apiVersion: v1
kind: Pod
metadata:
  name: recovery-delete-writer-%s
  namespace: %s
  labels: {sfs-subdir-e2e-run: %q, sfs-subdir-e2e-scenario: checkpoint}
spec:
  restartPolicy: Never
  containers:
    - name: workload
      image: %s
      command: ["sh", "-c", "printf recovered-delete > /data/lifecycle-marker; sync; test \"$(cat /data/lifecycle-marker)\" = recovered-delete; sleep 3600"]
      volumeMounts: [{name: data, mountPath: /data}]
  volumes: [{name: data, persistentVolumeClaim: {claimName: %s}}]
`, shortRun, namespace, plan.RunID, request.WorkloadImage, archiveClaim,
		shortRun, namespace, plan.RunID, request.WorkloadImage, deleteClaim)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(lifecyclePods), "apply", "-f", "-"); err != nil {
		return false, false, false, err
	}
	for _, pod := range []string{"recovery-archive-writer-" + shortRun, "recovery-delete-writer-" + shortRun} {
		if _, err := backend.kubectl(ctx, request, nil, "-n", namespace, "wait", "pod/"+pod, "--for=condition=Ready", "--timeout=10m"); err != nil {
			return false, false, false, err
		}
	}
	requestNames := make(map[string]string, 2)
	for _, name := range []string{archiveClaim, deleteClaim} {
		uid, err := backend.kubectl(ctx, request, nil, "-n", namespace, "get", "pvc/"+name, "-o", "jsonpath={.metadata.uid}")
		if err != nil {
			return false, false, false, err
		}
		requestNames[name] = "pvc-" + strings.TrimSpace(string(uid))
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", namespace, "delete", "pod/recovery-archive-writer-"+shortRun, "pod/recovery-delete-writer-"+shortRun, "--wait=true", "--timeout=10m"); err != nil {
		return false, false, false, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", namespace, "delete", "pvc/"+archiveClaim, "pvc/"+deleteClaim, "--wait=true", "--timeout=15m"); err != nil {
		return false, false, false, err
	}
	states, err := backend.waitForAllocationStates(ctx, request, map[string]string{requestNames[archiveClaim]: "Archived", requestNames[deleteClaim]: "Deleted"})
	if err != nil {
		return false, false, false, err
	}
	if err := backend.waitForCheckpointWorkloadMarker(ctx, request, namespace, "checkpoint-workload-"+shortRun, marker); err != nil {
		return false, false, false, err
	}
	return states[requestNames[archiveClaim]] == "Archived", states[requestNames[deleteClaim]] == "Deleted", len(states) == 2 && restoredTerminalRecords > 0, nil
}

func (backend *scalewayBackend) countTerminalAllocationRecords(ctx context.Context, request e2erunner.Request) (int, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "configmaps", "-l", "app.kubernetes.io/name=scaleway-sfs-subdir-csi", "-o", "json")
	if err != nil {
		return 0, err
	}
	var list struct {
		Items []struct {
			Data map[string]string `json:"data"`
		} `json:"items"`
	}
	if err := json.Unmarshal(encoded, &list); err != nil {
		return 0, err
	}
	count := 0
	for _, item := range list.Items {
		var record struct {
			State string `json:"state"`
		}
		if raw := item.Data["record.json"]; raw != "" && json.Unmarshal([]byte(raw), &record) == nil &&
			(record.State == "Archived" || record.State == "Retained" || record.State == "Deleted") {
			count++
		}
	}
	return count, nil
}

func (backend *scalewayBackend) waitForAllocationStates(ctx context.Context, request e2erunner.Request, expected map[string]string) (map[string]string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		encoded, err := backend.kubectl(waitCtx, request, nil, "-n", request.DriverNamespace, "get", "configmaps", "-l", "app.kubernetes.io/name=scaleway-sfs-subdir-csi", "-o", "json")
		if err == nil {
			var list struct {
				Items []struct {
					Data map[string]string `json:"data"`
				} `json:"items"`
			}
			if json.Unmarshal(encoded, &list) == nil {
				observed := make(map[string]string, len(expected))
				for _, item := range list.Items {
					var record struct {
						CreateVolumeRequestName string `json:"createVolumeRequestName"`
						State                   string `json:"state"`
					}
					if raw := item.Data["record.json"]; raw != "" && json.Unmarshal([]byte(raw), &record) == nil {
						if want, tracked := expected[record.CreateVolumeRequestName]; tracked && record.State == want {
							observed[record.CreateVolumeRequestName] = record.State
						}
					}
				}
				if len(observed) == len(expected) {
					return observed, nil
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("wait for post-recovery allocation lifecycles: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) cleanupCheckpointWorkload(ctx context.Context, request e2erunner.Request, namespace, pvName string) error {
	if _, err := backend.kubectl(ctx, request, nil, "delete", "namespace/"+namespace, "--wait=true", "--timeout=20m"); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		encoded, err := backend.kubectl(waitCtx, request, nil, "get", "pv/"+pvName, "--ignore-not-found", "-o", "name")
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
