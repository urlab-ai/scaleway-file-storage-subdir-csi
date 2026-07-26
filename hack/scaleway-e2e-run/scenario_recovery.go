package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	driverscaleway "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	controllerRecoverySchemaVersion = "1"
	controllerRecoveryPhaseArmed    = "armed"
	controllerRecoveryPhaseStopped  = "stopped"
)

// controllerRecoveryJournal is the minimum durable state needed to make
// --cleanup-only recover a controller-node failure after the provider stop has
// committed. It contains no credential and grants authority only over exact
// run-owned IDs already present in the cleanup inventory.
type controllerRecoveryJournal struct {
	SchemaVersion       string `json:"schemaVersion"`
	RunID               string `json:"runId"`
	Phase               string `json:"phase"`
	ClusterID           string `json:"clusterId"`
	PoolID              string `json:"poolId"`
	OldKapsuleNodeID    string `json:"oldKapsuleNodeId"`
	OldControllerPod    string `json:"oldControllerPod"`
	OldControllerPodUID string `json:"oldControllerPodUid"`
	OldNodeName         string `json:"oldNodeName"`
	OldCSINodeID        string `json:"oldCsiNodeId"`
	OldServerID         string `json:"oldServerId"`
	OldZone             string `json:"oldZone"`
	LeaseUID            string `json:"leaseUid"`
	InstallationID      string `json:"installationId"`
	ActiveClusterUID    string `json:"activeClusterUid"`
}

func (journal controllerRecoveryJournal) validateForRequest(
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) error {
	if journal.SchemaVersion != controllerRecoverySchemaVersion || journal.RunID != plan.RunID ||
		(journal.Phase != controllerRecoveryPhaseArmed && journal.Phase != controllerRecoveryPhaseStopped) {
		return fmt.Errorf("controller recovery journal envelope is invalid")
	}
	for name, id := range map[string]string{
		"cluster": journal.ClusterID, "pool": journal.PoolID, "Kapsule node": journal.OldKapsuleNodeID,
		"controller Pod UID": journal.OldControllerPodUID, "server": journal.OldServerID,
		"Lease UID": journal.LeaseUID, "active cluster UID": journal.ActiveClusterUID,
	} {
		if err := volume.ValidateOperationID(id); err != nil {
			return fmt.Errorf("controller recovery %s identity: %w", name, err)
		}
	}
	if journal.ClusterID != resourceID(inventory, e2ecleanup.ResourceKindCluster, 0) ||
		journal.PoolID != resourceID(inventory, e2ecleanup.ResourceKindNodePool, 0) ||
		journal.InstallationID != plan.RunID || journal.OldZone != request.Zone ||
		journal.OldControllerPod == "" || journal.OldNodeName == "" ||
		strings.ContainsAny(journal.OldControllerPod+journal.OldNodeName, "\x00\r\n\t /") {
		return fmt.Errorf("controller recovery journal differs from the exact run inventory")
	}
	target, err := driverscaleway.ParseNodeID(journal.OldCSINodeID)
	if err != nil || target.ServerID != journal.OldServerID || target.Zone != journal.OldZone {
		return fmt.Errorf("controller recovery CSI node identity is invalid: %w", err)
	}
	return nil
}

func (backend *scalewayBackend) controllerRecoveryPath(plan e2eplan.Plan) string {
	return filepath.Join(filepath.Dir(plan.CleanupInventoryPath), "controller-recovery-"+plan.RunID+".json")
}

func (backend *scalewayBackend) writeControllerRecoveryJournal(plan e2eplan.Plan, journal controllerRecoveryJournal) error {
	encoded, err := canonicaljson.Marshal(journal)
	if err != nil {
		return err
	}
	return replaceDurableFile(backend.controllerRecoveryPath(plan), append(encoded, '\n'), 0o600)
}

func (backend *scalewayBackend) readControllerRecoveryJournal(plan e2eplan.Plan) (controllerRecoveryJournal, error) {
	encoded, err := os.ReadFile(backend.controllerRecoveryPath(plan))
	if err != nil {
		return controllerRecoveryJournal{}, err
	}
	var journal controllerRecoveryJournal
	if err := strictjson.Decode(encoded, &journal); err != nil {
		return controllerRecoveryJournal{}, err
	}
	return journal, nil
}

func (backend *scalewayBackend) removeControllerRecoveryJournal(plan e2eplan.Plan) error {
	return removeDurableFile(backend.controllerRecoveryPath(plan))
}

func newControllerRecoveryJournal(
	plan e2eplan.Plan,
	clusterID, poolID string,
	controller kubernetesPod,
	lease kubernetesLease,
	oldNode *k8sapi.Node,
	oldTarget driverscaleway.Target,
) controllerRecoveryJournal {
	return controllerRecoveryJournal{
		SchemaVersion: controllerRecoverySchemaVersion, RunID: plan.RunID, Phase: controllerRecoveryPhaseArmed,
		ClusterID: clusterID, PoolID: poolID, OldKapsuleNodeID: oldNode.ID,
		OldControllerPod: controller.Metadata.Name, OldControllerPodUID: controller.Metadata.UID,
		OldNodeName: controller.Spec.NodeName, OldCSINodeID: lease.Metadata.Annotations["holderCSINodeID"],
		OldServerID: oldTarget.ServerID, OldZone: oldTarget.Zone, LeaseUID: lease.Metadata.UID,
		InstallationID:   lease.Metadata.Annotations["holderInstallationID"],
		ActiveClusterUID: lease.Metadata.Annotations["holderActiveClusterUID"],
	}
}

func (journal controllerRecoveryJournal) holderAnnotations() map[string]string {
	return map[string]string{
		"holderPodUID": journal.OldControllerPodUID, "holderNodeName": journal.OldNodeName,
		"holderCSINodeID": journal.OldCSINodeID, "holderInstanceID": journal.OldServerID,
		"holderZone": journal.OldZone, "holderInstallationID": journal.InstallationID,
		"holderActiveClusterUID": journal.ActiveClusterUID,
	}
}

// recoverInterruptedControllerFailure is deliberately called before the
// generic Kubernetes cleanup. A stopped controller holder must be fenced and
// replaced before csi-admin can produce a trustworthy safe-uninstall audit.
func (backend *scalewayBackend) recoverInterruptedControllerFailure(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) error {
	journal, err := backend.readControllerRecoveryJournal(plan)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read interrupted controller recovery journal: %w", err)
	}
	if err := journal.validateForRequest(request, plan, inventory); err != nil {
		return err
	}

	stopped, err := backend.controllerInstanceStopped(ctx, journal)
	if err != nil {
		return err
	}
	if !stopped {
		// The provider stop did not commit and the exact injector recovery
		// already resumed the process. Nothing destructive remains to recover.
		return backend.removeControllerRecoveryJournal(plan)
	}

	nodePresent, err := backend.controllerRecoveryNodePresent(ctx, plan, journal)
	if err != nil {
		return err
	}
	if !nodePresent {
		instanceAbsent, err := backend.controllerInstanceAbsent(ctx, journal)
		if err != nil {
			return err
		}
		if !instanceAbsent {
			return fmt.Errorf("journaled Kapsule node is absent while its stopped Instance still exists; refuse an unproven detach")
		}
	}
	if err := backend.deleteExactOldControllerPod(ctx, request, journal); err != nil {
		return err
	}
	parentIDs := []string{
		resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
		resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
	}
	for _, parentID := range parentIDs {
		if parentID == "" {
			return fmt.Errorf("controller recovery lacks one exact parent ID")
		}
		if err := backend.ensureRecoveryAttachmentAbsent(ctx, scw.Zone(journal.OldZone), journal.OldServerID, parentID); err != nil {
			return fmt.Errorf("fence interrupted controller from parent %s: %w", parentID, err)
		}
	}
	if err := backend.ensureStoppedKapsuleNodeReplacement(ctx, plan, journal); err != nil {
		return err
	}
	if err := backend.waitInstanceAbsent(ctx, scw.Zone(journal.OldZone), journal.OldServerID); err != nil {
		return err
	}
	if recovered, err := backend.controllerRecoveredFromJournal(ctx, request, journal); err != nil {
		return err
	} else if recovered {
		return backend.removeControllerRecoveryJournal(plan)
	}
	successor, err := backend.waitForReplacementPod(ctx, request, controllerSelector, "", journal.OldControllerPodUID)
	if err != nil {
		return fmt.Errorf("wait for fail-closed cleanup successor controller: %w", err)
	}
	if successor.Spec.NodeName == journal.OldNodeName || podReady(successor) {
		return fmt.Errorf("cleanup successor controller is not fail-closed on a distinct node")
	}

	if err := backend.removeMatchingApprovalSecret(ctx, request, journal); err != nil {
		return err
	}
	approvalRequestID, err := randomUUIDv4()
	if err != nil {
		return err
	}
	approvalUID, err := backend.createAbnormalTakeoverApproval(ctx, request, approvalRequestID, journal.holderAnnotations(), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("create cleanup recovery approval: %w", err)
	}
	deployment, err := backend.singularObjectName(ctx, request, "deployment", controllerSelector)
	if err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"rollout", "status", "deployment/"+deployment, "--timeout=20m",
	); err != nil {
		return fmt.Errorf("wait for cleanup controller recovery: %w", err)
	}
	lease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return err
	}
	if lease.Metadata.UID != journal.LeaseUID || lease.Spec.HolderIdentity == journal.OldControllerPodUID ||
		lease.Metadata.Annotations["approvalConsumptionSecretUID"] != approvalUID ||
		lease.Metadata.Annotations["approvalConsumptionRequestID"] != approvalRequestID {
		return fmt.Errorf("cleanup controller recovery lacks exact approval consumption")
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "secret/sfs-subdir-controller-approval", "--wait=true", "--timeout=5m",
	); err != nil {
		return err
	}
	return backend.removeControllerRecoveryJournal(plan)
}

func (backend *scalewayBackend) controllerInstanceStopped(ctx context.Context, journal controllerRecoveryJournal) (bool, error) {
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	if providerNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read interrupted controller Instance: %w", err)
	}
	if response == nil || response.Server == nil || response.Server.ID != journal.OldServerID {
		return false, fmt.Errorf("read interrupted controller Instance: provider returned an empty or mismatched Instance")
	}
	state := response.Server.State
	if state == instanceapi.ServerStateStopping {
		timeout := 20 * time.Minute
		server, waitErr := backend.instance.WaitForServer(&instanceapi.WaitForServerRequest{
			Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID, Timeout: &timeout,
		}, scw.WithContext(ctx))
		if waitErr != nil {
			return false, fmt.Errorf("wait for interrupted controller Instance terminal state: %w", waitErr)
		}
		if server == nil || server.ID != journal.OldServerID {
			return false, fmt.Errorf("wait for interrupted controller Instance returned an empty or mismatched Instance")
		}
		state = server.State
	}
	switch state {
	case instanceapi.ServerStateStopped, instanceapi.ServerStateStoppedInPlace:
		return true, nil
	case instanceapi.ServerStateRunning:
		return false, nil
	default:
		return false, fmt.Errorf("interrupted controller Instance has ambiguous state %q", state)
	}
}

func (backend *scalewayBackend) controllerInstanceAbsent(ctx context.Context, journal controllerRecoveryJournal) (bool, error) {
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{
		Zone: scw.Zone(journal.OldZone), ServerID: journal.OldServerID,
	}, scw.WithContext(ctx))
	if providerNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read interrupted controller Instance identity: %w", err)
	}
	if response == nil || response.Server == nil || response.Server.ID != journal.OldServerID {
		return false, fmt.Errorf("interrupted controller Instance identity is ambiguous")
	}
	return false, nil
}

func (backend *scalewayBackend) deleteExactOldControllerPod(
	ctx context.Context,
	request e2erunner.Request,
	journal controllerRecoveryJournal,
) error {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"get", "pod/"+journal.OldControllerPod, "--ignore-not-found", "-o", "json",
	)
	if err != nil || len(encoded) == 0 {
		return err
	}
	var pod kubernetesPod
	if err := json.Unmarshal(encoded, &pod); err != nil {
		return err
	}
	if pod.Metadata.UID != journal.OldControllerPodUID || pod.Spec.NodeName != journal.OldNodeName {
		return fmt.Errorf("old controller Pod identity changed before cleanup recovery")
	}
	_, err = backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "pod/"+journal.OldControllerPod, "--grace-period=0", "--force", "--wait=false",
	)
	return err
}

func (backend *scalewayBackend) ensureStoppedKapsuleNodeReplacement(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) error {
	present, err := backend.controllerRecoveryNodePresent(ctx, plan, journal)
	if err != nil || !present {
		return err
	}
	if _, err := backend.kubernetes.DeleteNode(&k8sapi.DeleteNodeRequest{
		Region: scw.Region(plan.Region), NodeID: journal.OldKapsuleNodeID, Replace: true,
	}, scw.WithContext(ctx)); err != nil {
		return fmt.Errorf("replace interrupted exact Kapsule node: %w", err)
	}
	return nil
}

func (backend *scalewayBackend) controllerRecoveryNodePresent(
	ctx context.Context,
	plan e2eplan.Plan,
	journal controllerRecoveryJournal,
) (bool, error) {
	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(plan.Region), ClusterID: journal.ClusterID, PoolID: &journal.PoolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return false, err
	}
	if listed == nil {
		return false, fmt.Errorf("cleanup recovery node inventory is empty")
	}
	return controllerRecoveryNodeIdentityPresent(listed.Nodes, journal)
}

func controllerRecoveryNodeIdentityPresent(nodes []*k8sapi.Node, journal controllerRecoveryJournal) (bool, error) {
	for _, node := range nodes {
		if node == nil || node.ClusterID != journal.ClusterID || node.PoolID != journal.PoolID {
			return false, fmt.Errorf("cleanup recovery node inventory is outside the exact run pool")
		}
		if node.ID != journal.OldKapsuleNodeID {
			continue
		}
		if node.Name != journal.OldNodeName || !strings.HasSuffix(node.ProviderID, "/"+journal.OldServerID) {
			return false, fmt.Errorf("stopped Kapsule node identity changed before cleanup recovery")
		}
		return true, nil
	}
	return false, nil
}

func (backend *scalewayBackend) controllerRecoveredFromJournal(
	ctx context.Context,
	request e2erunner.Request,
	journal controllerRecoveryJournal,
) (bool, error) {
	lease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return false, err
	}
	if lease.Metadata.UID != journal.LeaseUID {
		return false, fmt.Errorf("controller Lease identity changed during cleanup recovery")
	}
	if lease.Spec.HolderIdentity == journal.OldControllerPodUID {
		return false, nil
	}
	controller, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return false, err
	}
	return controller.Metadata.UID == lease.Spec.HolderIdentity && podReady(controller), nil
}

func (backend *scalewayBackend) removeMatchingApprovalSecret(
	ctx context.Context,
	request e2erunner.Request,
	journal controllerRecoveryJournal,
) error {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"get", "secret/sfs-subdir-controller-approval", "--ignore-not-found", "-o", "json",
	)
	if err != nil || len(encoded) == 0 {
		return err
	}
	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(encoded, &secret); err != nil {
		return err
	}
	required := map[string]string{
		"mode": "abnormal-takeover", "installationID": journal.InstallationID,
		"activeClusterUID":         journal.ActiveClusterUID,
		"previousHolderPodUID":     journal.OldControllerPodUID,
		"previousHolderNodeName":   journal.OldNodeName,
		"previousHolderCSINodeID":  journal.OldCSINodeID,
		"previousHolderInstanceID": journal.OldServerID,
		"previousHolderZone":       journal.OldZone,
	}
	for key, want := range required {
		raw, present := secret.Data[key]
		decoded, decodeErr := base64.StdEncoding.DecodeString(raw)
		if !present || decodeErr != nil || string(decoded) != want {
			return fmt.Errorf("refuse deletion of an unrelated controller approval Secret")
		}
	}
	_, err = backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "secret/sfs-subdir-controller-approval", "--wait=true", "--timeout=5m",
	)
	return err
}
