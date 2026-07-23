package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	driverscaleway "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const controllerSelector = "app.kubernetes.io/component=controller"
const nodeSelector = "app.kubernetes.io/component=node"

type kubernetesPod struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		DeletionTimestamp *string           `json:"deletionTimestamp"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

type kubernetesPodList struct {
	Items []kubernetesPod `json:"items"`
}

type kubernetesLease struct {
	Metadata struct {
		UID         string            `json:"uid"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		HolderIdentity string `json:"holderIdentity"`
	} `json:"spec"`
}

type kubernetesSecret struct {
	Metadata struct {
		UID string `json:"uid"`
	} `json:"metadata"`
}

type kubernetesCSINodeList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Drivers []struct {
				Name   string `json:"name"`
				NodeID string `json:"nodeID"`
			} `json:"drivers"`
		} `json:"spec"`
	} `json:"items"`
}

type nodeDrainState struct {
	Proof e2erunner.NodeDrainProof
}

func nodeDrainManifest(request e2erunner.Request, plan e2eplan.Plan, deployment, claim, shortRun string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/instance: %q
    sfs-subdir-e2e-run: %q
    sfs-subdir-e2e-scenario: node-drain
spec:
  replicas: 1
  selector:
    matchLabels: {sfs-subdir-e2e-workload: %q}
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: %q
        sfs-subdir-e2e-run: %q
        sfs-subdir-e2e-scenario: node-drain
        sfs-subdir-e2e-workload: %q
    spec:
      containers:
        - name: workload
          image: %s
          command: ["sh", "-c", "test -e /data/node-drain-marker || { printf node-drain-%s > /data/node-drain-marker; sync; }; sleep 3600"]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %s}
`, deployment, request.DriverNamespace, request.HelmRelease, plan.RunID, deployment,
		request.HelmRelease, plan.RunID, deployment, request.WorkloadImage, shortRun, claim)
}

func (backend *scalewayBackend) runDestructiveControllerAndNodeScenarios(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
	evidenceDirectory string,
) ([]e2erunner.ScenarioResult, error) {
	drain, err := backend.normalNodeDrainScenario(ctx, request, plan)
	if err != nil {
		return nil, fmt.Errorf("normal node drain and plugin restart: %w", err)
	}
	controllerProof, replacement, err := backend.controllerHardFailureScenario(ctx, request, plan, inventory)
	if err != nil {
		return nil, fmt.Errorf("controller hard failure: %w", err)
	}
	drain.Proof.ReplacedKapsuleNodeID = replacement.ReplacedKapsuleNodeID
	drain.Proof.ReplacementKapsuleID = replacement.ReplacementKapsuleID
	drain.Proof.ReplacementKapsuleName = replacement.ReplacementKapsuleName
	drain.Proof.ReplacementKapsuleNodeID = replacement.ReplacementKapsuleNodeID
	drain.Proof.CommercialType = replacement.CommercialType
	drain.Proof.MaxFileSystems = replacement.MaxFileSystems
	drain.Proof.NodeConfigGeneration = replacement.NodeConfigGeneration
	drain.Proof.ReplacementReady = true
	drain.Proof.ReplacementPluginReady = true
	drain.Proof.ReplacementRegistered = true
	drain.Proof.ReplacementCompatible = true
	drain.Proof.MarkerReadOnReplacement = true
	if err := drain.Proof.Validate(); err != nil {
		return nil, err
	}
	controllerResult, err := writeScenarioJSON(evidenceDirectory, "controller-hard-failure", controllerProof)
	if err != nil {
		return nil, err
	}
	nodeResult, err := writeScenarioJSON(evidenceDirectory, "node-drain-and-replacement", drain.Proof)
	if err != nil {
		return nil, err
	}
	return []e2erunner.ScenarioResult{controllerResult, nodeResult}, nil
}

func (backend *scalewayBackend) normalNodeDrainScenario(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan) (state nodeDrainState, returnErr error) {
	shortRun := plan.RunID[:8]
	claim := "e2e-scale-" + shortRun + "-000"
	deployment := "e2e-node-drain-" + shortRun
	nodes, err := backend.readyLinuxNodeNames(ctx, request)
	if err != nil {
		return state, err
	}
	if len(nodes) < 2 {
		return state, fmt.Errorf("normal drain requires at least two Ready Linux nodes")
	}
	manifest := nodeDrainManifest(request, plan, deployment, claim, shortRun)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return state, err
	}
	victim := ""
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var uncordonErr error
		if victim != "" {
			_, uncordonErr = backend.kubectl(cleanupCtx, request, nil, "uncordon", victim)
		}
		_, deleteErr := backend.kubectl(cleanupCtx, request, nil, "-n", request.DriverNamespace, "delete", "deployment/"+deployment, "--ignore-not-found", "--wait=true", "--timeout=5m")
		returnErr = errors.Join(returnErr, uncordonErr, deleteErr)
	}()
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "status", "deployment/"+deployment, "--timeout=15m"); err != nil {
		return state, err
	}
	original, err := backend.singularPod(ctx, request, "sfs-subdir-e2e-workload="+deployment, "")
	if err != nil {
		return state, fmt.Errorf("observe original drain workload: %w", err)
	}
	victim = original.Spec.NodeName
	if _, err := backend.kubectl(ctx, request, nil, "cordon", victim); err != nil {
		return state, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "drain", victim, "--ignore-daemonsets", "--delete-emptydir-data", "--force", "--timeout=20m"); err != nil {
		return state, err
	}
	replacement, err := backend.waitForReadyReplacementPod(ctx, request, "sfs-subdir-e2e-workload="+deployment, "", original.Metadata.UID)
	if err != nil {
		return state, fmt.Errorf("observe replacement drain workload: %w", err)
	}
	if replacement.Spec.NodeName == victim {
		return state, fmt.Errorf("replacement drain workload did not move Ready to a distinct node")
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "exec", replacement.Metadata.Name, "--", "sh", "-c", "test \"$(cat /data/node-drain-marker)\" = node-drain-"+shortRun); err != nil {
		return state, err
	}
	oldPlugin, err := backend.singularPod(ctx, request, nodeSelector, replacement.Spec.NodeName)
	if err != nil {
		return state, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "delete", "pod/"+oldPlugin.Metadata.Name, "--wait=true", "--timeout=10m"); err != nil {
		return state, err
	}
	newPlugin, err := backend.waitForReadyReplacementPod(ctx, request, nodeSelector, replacement.Spec.NodeName, oldPlugin.Metadata.UID)
	if err != nil {
		return state, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "exec", replacement.Metadata.Name, "--", "sh", "-c", "test \"$(cat /data/node-drain-marker)\" = node-drain-"+shortRun); err != nil {
		return state, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "uncordon", victim); err != nil {
		return state, err
	}
	state.Proof = e2erunner.NodeDrainProof{
		SchemaVersion: "1", Scenario: "node-drain-and-replacement", RunID: plan.RunID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), ClaimName: claim, DeploymentName: deployment,
		OriginalNodeName: victim, ReplacementNodeName: replacement.Spec.NodeName,
		OriginalPodUID: original.Metadata.UID, ReplacementPodUID: replacement.Metadata.UID,
		OldNodeDrained: true, MarkerReadAfterDrain: true, OldNodeUncordoned: true,
		OldNodePluginUID: oldPlugin.Metadata.UID, NewNodePluginUID: newPlugin.Metadata.UID,
		MarkerReadAfterRestart: true,
	}
	return state, nil
}

type nodeReplacementEvidence struct {
	ReplacedKapsuleNodeID    string
	ReplacementKapsuleID     string
	ReplacementKapsuleName   string
	ReplacementKapsuleNodeID string
	CommercialType           string
	MaxFileSystems           int
	NodeConfigGeneration     string
}

func (backend *scalewayBackend) controllerHardFailureScenario(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	inventory e2ecleanup.Inventory,
) (proof e2erunner.ControllerFailureProof, replacement nodeReplacementEvidence, returnErr error) {
	started := time.Now().UTC()
	controller, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return proof, replacement, err
	}
	lease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return proof, replacement, err
	}
	annotations := lease.Metadata.Annotations
	oldNodeID := annotations["holderCSINodeID"]
	oldTarget, err := driverscaleway.ParseNodeID(oldNodeID)
	if err != nil {
		return proof, replacement, fmt.Errorf("controller Lease holder evidence does not match the live controller: %w", err)
	}
	if lease.Spec.HolderIdentity != controller.Metadata.UID || annotations["holderPodUID"] != controller.Metadata.UID || annotations["holderNodeName"] != controller.Spec.NodeName || annotations["holderInstanceID"] != oldTarget.ServerID || annotations["holderZone"] != oldTarget.Zone {
		return proof, replacement, fmt.Errorf("controller Lease holder evidence does not match the live controller")
	}
	poolID := resourceID(inventory, e2ecleanup.ResourceKindNodePool, 0)
	clusterID := resourceID(inventory, e2ecleanup.ResourceKindCluster, 0)
	beforeNodes, oldKapsuleNode, err := backend.exactKapsuleNode(ctx, clusterID, poolID, controller.Spec.NodeName, oldTarget.ServerID)
	if err != nil {
		return proof, replacement, err
	}
	readyNodes, err := backend.readyLinuxNodeNames(ctx, request)
	if err != nil {
		return proof, replacement, err
	}
	survivor := ""
	for _, name := range readyNodes {
		if name != controller.Spec.NodeName {
			survivor = name
			break
		}
	}
	if survivor == "" {
		return proof, replacement, fmt.Errorf("controller hard failure has no distinct Ready survivor node")
	}
	shortRun := plan.RunID[:8]
	failurePod := "e2e-controller-survivor-" + shortRun
	failureClaim := "e2e-scale-" + shortRun + "-000"
	if err := backend.applyMarkerPod(ctx, request, plan, failurePod, failureClaim, survivor, "controller-hard-failure"); err != nil {
		return proof, replacement, err
	}
	if err := backend.instance.ServerActionAndWait(&instanceapi.ServerActionAndWaitRequest{
		Zone: scw.Zone(oldTarget.Zone), ServerID: oldTarget.ServerID, Action: instanceapi.ServerActionPoweroff,
	}, scw.WithContext(ctx)); err != nil {
		return proof, replacement, fmt.Errorf("hard-stop exact controller Instance: %w", err)
	}
	if _, err := backend.kubectl(ctx, request, nil, "cordon", controller.Spec.NodeName); err != nil {
		return proof, replacement, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "delete", "pod/"+controller.Metadata.Name, "--grace-period=0", "--force", "--wait=false"); err != nil {
		return proof, replacement, err
	}
	newController, err := backend.waitForReplacementPod(ctx, request, controllerSelector, "", controller.Metadata.UID)
	if err != nil {
		return proof, replacement, err
	}
	if newController.Spec.NodeName == controller.Spec.NodeName || podReady(newController) {
		return proof, replacement, fmt.Errorf("successor controller was not fail-closed on a distinct node")
	}
	blockedLease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return proof, replacement, fmt.Errorf("successor changed the uncleared Lease before approval: %w", err)
	}
	if blockedLease.Spec.HolderIdentity != controller.Metadata.UID || blockedLease.Metadata.UID != lease.Metadata.UID {
		return proof, replacement, fmt.Errorf("successor changed the uncleared Lease before approval")
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "exec", failurePod, "--", "sh", "-c", "printf recovered-"+shortRun+" > /data/controller-hard-failure; sync; test \"$(cat /data/controller-hard-failure)\" = recovered-"+shortRun); err != nil {
		return proof, replacement, fmt.Errorf("survivor workload I/O during controller failure: %w", err)
	}
	parentIDs := []string{resourceID(inventory, e2ecleanup.ResourceKindParent, 0), resourceID(inventory, e2ecleanup.ResourceKindParent, 1)}
	for _, parentID := range parentIDs {
		if err := backend.ensureForeignAttachmentAbsent(ctx, scw.Zone(oldTarget.Zone), oldTarget.ServerID, parentID); err != nil {
			return proof, replacement, fmt.Errorf("fence old controller from parent %s: %w", parentID, err)
		}
	}
	// Start the exact Kapsule replacement after the stopped Instance is detached
	// but before approval. This prevents pool auto-healing from racing a later
	// replacement request, while the successor still remains non-serving behind
	// the uncleared Lease.
	if _, err := backend.kubernetes.DeleteNode(&k8sapi.DeleteNodeRequest{
		Region: scw.Region(plan.Region), NodeID: oldKapsuleNode.ID, Replace: true,
	}, scw.WithContext(ctx)); err != nil {
		return proof, replacement, fmt.Errorf("replace exact stopped Kapsule node: %w", err)
	}
	if err := backend.waitInstanceAbsent(ctx, scw.Zone(oldTarget.Zone), oldTarget.ServerID); err != nil {
		return proof, replacement, err
	}
	approvalRequestID, err := randomUUIDv4()
	if err != nil {
		return proof, replacement, err
	}
	approvedAt := time.Now().UTC()
	approvalUID, err := backend.createAbnormalTakeoverApproval(ctx, request, approvalRequestID, annotations, approvedAt)
	if err != nil {
		return proof, replacement, err
	}
	controllerDeployment, err := backend.singularObjectName(ctx, request, "deployment", controllerSelector)
	if err != nil {
		return proof, replacement, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "status", "deployment/"+controllerDeployment, "--timeout=20m"); err != nil {
		return proof, replacement, err
	}
	recoveredController, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return proof, replacement, fmt.Errorf("observe approved successor controller: %w", err)
	}
	if recoveredController.Metadata.UID != newController.Metadata.UID || !podReady(recoveredController) {
		return proof, replacement, fmt.Errorf("approved successor controller identity or readiness is invalid")
	}
	recoveredLease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return proof, replacement, fmt.Errorf("read recovered controller Lease: %w", err)
	}
	if recoveredLease.Metadata.UID != lease.Metadata.UID || recoveredLease.Spec.HolderIdentity != recoveredController.Metadata.UID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionSecretUID"] != approvalUID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionRequestID"] != approvalRequestID ||
		recoveredLease.Metadata.Annotations["approvalConsumptionMode"] != "abnormal-takeover" ||
		recoveredLease.Metadata.Annotations["approvalConsumptionPodUID"] != recoveredController.Metadata.UID {
		return proof, replacement, fmt.Errorf("controller Lease lacks exact approval consumption evidence")
	}
	newNodeID := recoveredLease.Metadata.Annotations["holderCSINodeID"]
	if _, err := driverscaleway.ParseNodeID(newNodeID); err != nil {
		return proof, replacement, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "delete", "secret/sfs-subdir-controller-approval", "--wait=true", "--timeout=5m"); err != nil {
		return proof, replacement, err
	}
	newClaim := "e2e-after-hard-failure-" + shortRun
	if err := backend.applyPVC(ctx, request, plan, newClaim); err != nil {
		return proof, replacement, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		_, pvcErr := backend.kubectl(cleanupCtx, request, nil, "-n", request.DriverNamespace, "delete", "pvc/"+newClaim, "--ignore-not-found", "--wait=true", "--timeout=10m")
		returnErr = errors.Join(returnErr, pvcErr)
	}()
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "wait", "pvc/"+newClaim, "--for=jsonpath={.status.phase}=Bound", "--timeout=10m"); err != nil {
		return proof, replacement, err
	}
	controllerRecoverySeconds := max(int64(time.Since(started).Seconds()), 1)
	replacement, err = backend.replaceStoppedKapsuleNode(ctx, request, plan, clusterID, poolID, oldKapsuleNode, beforeNodes, failureClaim, shortRun)
	if err != nil {
		return proof, replacement, err
	}
	proof = e2erunner.ControllerFailureProof{
		SchemaVersion: "1", Scenario: "controller-hard-failure", RunID: plan.RunID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), LeaseUID: lease.Metadata.UID,
		OldPodUID: controller.Metadata.UID, NewPodUID: recoveredController.Metadata.UID,
		OldNodeName: controller.Spec.NodeName, NewNodeName: recoveredController.Spec.NodeName,
		OldNodeID: oldNodeID, NewNodeID: newNodeID, ParentFilesystemIDs: parentIDs,
		ApprovalSecretUID: approvalUID, ApprovalRequestID: approvalRequestID,
		OperatorSteps: []string{
			"stop-old-controller-instance", "cordon-old-kubernetes-node", "force-delete-old-controller-pod",
			"verify-successor-blocked-by-uncleared-lease", "detach-exact-parents-and-verify-dual-absence",
			"replace-stopped-kapsule-node",
			"create-immutable-abnormal-takeover-approval", "verify-approval-consumption-and-controller-recovery",
			"delete-consumed-approval-secret",
		},
		RecoverySeconds: controllerRecoverySeconds, OldHolderMatched: true,
		OldInstanceReachedStopped: true, SuccessorBlockedBeforeApproval: true,
		ServerAttachmentsAbsent: true, RegionalAttachmentsAbsent: true, ApprovalConsumed: true,
		ExistingVolumeReadWrite: true, NewPVCName: newClaim, NewPVCBound: true,
		LeaseUIDPreserved: true, ControllerAvailable: true, ApprovalSecretDeletedAfterAudit: true,
	}
	if err := proof.Validate(); err != nil {
		return proof, replacement, err
	}
	return proof, replacement, nil
}

func (backend *scalewayBackend) replaceStoppedKapsuleNode(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	clusterID, poolID string,
	oldNode *k8sapi.Node,
	before []*k8sapi.Node,
	claim, shortRun string,
) (nodeReplacementEvidence, error) {
	beforeIDs := make(map[string]struct{}, len(before))
	for _, node := range before {
		if node != nil {
			beforeIDs[node.ID] = struct{}{}
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var replacement *k8sapi.Node
	for {
		listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
			Region: scw.Region(plan.Region), ClusterID: clusterID, PoolID: &poolID,
		}, scw.WithAllPages(), scw.WithContext(waitCtx))
		if err != nil {
			return nodeReplacementEvidence{}, err
		}
		oldPresent := false
		for _, node := range listed.Nodes {
			if node == nil || node.PoolID != poolID || node.ClusterID != clusterID {
				return nodeReplacementEvidence{}, fmt.Errorf("replacement node inventory is incomplete or outside the exact pool")
			}
			if node.ID == oldNode.ID {
				oldPresent = true
			}
			if _, existed := beforeIDs[node.ID]; !existed && node.Status == k8sapi.NodeStatusReady {
				if replacement != nil && replacement.ID != node.ID {
					return nodeReplacementEvidence{}, fmt.Errorf("multiple replacement Kapsule nodes appeared")
				}
				copy := *node
				replacement = &copy
			}
		}
		if !oldPresent && replacement != nil && uint32(len(listed.Nodes)) == plan.NodePool.Count {
			break
		}
		select {
		case <-waitCtx.Done():
			return nodeReplacementEvidence{}, fmt.Errorf("wait for exact Kapsule node replacement: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
	driver, err := backend.storageClassDriver(ctx, request)
	if err != nil {
		return nodeReplacementEvidence{}, err
	}
	kubernetesNodeName, nodeID, err := backend.waitForCSINodeServer(ctx, request, driver, replacement.ProviderID)
	if err != nil {
		return nodeReplacementEvidence{}, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "wait", "node/"+kubernetesNodeName, "--for=condition=Ready", "--timeout=20m"); err != nil {
		return nodeReplacementEvidence{}, err
	}
	target, err := driverscaleway.ParseNodeID(nodeID)
	if err != nil {
		return nodeReplacementEvidence{}, err
	}
	serverResponse, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: scw.Zone(target.Zone), ServerID: target.ServerID}, scw.WithContext(ctx))
	if err != nil {
		return nodeReplacementEvidence{}, fmt.Errorf("read replacement Kapsule Instance: %w", err)
	}
	if serverResponse == nil || serverResponse.Server == nil {
		return nodeReplacementEvidence{}, fmt.Errorf("read replacement Kapsule Instance: provider returned an empty response")
	}
	server := serverResponse.Server
	serverTypes, err := backend.instance.ListServersTypes(&instanceapi.ListServersTypesRequest{Zone: scw.Zone(target.Zone)}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nodeReplacementEvidence{}, fmt.Errorf("read replacement commercial-type capabilities: %w", err)
	}
	serverType := serverTypes.Servers[server.CommercialType]
	if server.CommercialType != plan.NodePool.CommercialType || server.Project != plan.ProjectID ||
		server.State != instanceapi.ServerStateRunning || serverType == nil || serverType.Capabilities == nil ||
		serverType.Capabilities.MaxFileSystems < plan.Parents.Count {
		return nodeReplacementEvidence{}, fmt.Errorf("replacement Kapsule Instance is not release-compatible")
	}
	plugin, err := backend.waitForReadyPodOnNode(ctx, request, nodeSelector, kubernetesNodeName)
	if err != nil {
		return nodeReplacementEvidence{}, err
	}
	ds, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "daemonset", "-l", "app.kubernetes.io/instance="+request.HelmRelease+","+nodeSelector, "-o", "jsonpath={.items[0].spec.template.metadata.annotations.scaleway-sfs-subdir-csi\\.io/node-config-generation}")
	if err != nil {
		return nodeReplacementEvidence{}, err
	}
	generation := strings.TrimSpace(string(ds))
	if plugin.Metadata.Annotations["scaleway-sfs-subdir-csi.io/node-config-generation"] != generation {
		return nodeReplacementEvidence{}, fmt.Errorf("replacement node-plugin generation differs from DaemonSet")
	}
	probe := "e2e-replacement-node-" + shortRun
	if err := backend.applyReadMarkerPod(ctx, request, plan, probe, claim, kubernetesNodeName, "controller-hard-failure"); err != nil {
		return nodeReplacementEvidence{}, err
	}
	// Keep both the survivor workload and this replacement-node probe mounted
	// through the following provider inventory scenario. Their exact run labels
	// make them part of the final safe-uninstall cleanup, and together they prove
	// that the post-replacement data path still spans two physical Instances.
	return nodeReplacementEvidence{
		ReplacedKapsuleNodeID: oldNode.ID, ReplacementKapsuleID: replacement.ID,
		ReplacementKapsuleName: kubernetesNodeName, ReplacementKapsuleNodeID: nodeID,
		CommercialType: server.CommercialType, MaxFileSystems: int(serverType.Capabilities.MaxFileSystems),
		NodeConfigGeneration: generation,
	}, nil
}

func (backend *scalewayBackend) readyLinuxNodeNames(ctx context.Context, request e2erunner.Request) ([]string, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "get", "nodes", "-l", "kubernetes.io/os=linux", "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Unschedulable bool `json:"unschedulable"`
			} `json:"spec"`
			Status struct {
				Conditions []struct{ Type, Status string } `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(encoded, &list); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(list.Items))
	for _, node := range list.Items {
		ready := false
		for _, condition := range node.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				ready = true
			}
		}
		if !node.Spec.Unschedulable && ready {
			result = append(result, node.Metadata.Name)
		}
	}
	slices.Sort(result)
	return result, nil
}

func (backend *scalewayBackend) singularPod(ctx context.Context, request e2erunner.Request, componentSelector, nodeName string) (kubernetesPod, error) {
	selector := "app.kubernetes.io/instance=" + request.HelmRelease + "," + componentSelector
	arguments := []string{"-n", request.DriverNamespace, "get", "pods", "-l", selector}
	if nodeName != "" {
		arguments = append(arguments, "--field-selector=spec.nodeName="+nodeName)
	}
	arguments = append(arguments, "-o", "json")
	encoded, err := backend.kubectl(ctx, request, nil, arguments...)
	if err != nil {
		return kubernetesPod{}, err
	}
	var list kubernetesPodList
	if err := json.Unmarshal(encoded, &list); err != nil {
		return kubernetesPod{}, err
	}
	live := make([]kubernetesPod, 0, len(list.Items))
	for _, pod := range list.Items {
		if pod.Metadata.DeletionTimestamp == nil {
			live = append(live, pod)
		}
	}
	if len(live) != 1 || live[0].Metadata.Name == "" || live[0].Metadata.UID == "" || live[0].Spec.NodeName == "" {
		return kubernetesPod{}, fmt.Errorf("pod selector %q on node %q returned %d live Pods", selector, nodeName, len(live))
	}
	return live[0], nil
}

func (backend *scalewayBackend) singularObjectName(ctx context.Context, request e2erunner.Request, kind, componentSelector string) (string, error) {
	selector := "app.kubernetes.io/instance=" + request.HelmRelease + "," + componentSelector
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", kind, "-l", selector, "-o", "json")
	if err != nil {
		return "", err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(encoded, &list); err != nil {
		return "", err
	}
	if len(list.Items) != 1 || list.Items[0].Metadata.Name == "" {
		return "", fmt.Errorf("%s selector %q returned %d objects", kind, selector, len(list.Items))
	}
	return list.Items[0].Metadata.Name, nil
}

func (backend *scalewayBackend) waitForReplacementPod(ctx context.Context, request e2erunner.Request, componentSelector, nodeName, oldUID string) (kubernetesPod, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		pod, err := backend.singularPod(waitCtx, request, componentSelector, nodeName)
		if err == nil && pod.Metadata.UID != oldUID {
			return pod, nil
		}
		select {
		case <-waitCtx.Done():
			return kubernetesPod{}, fmt.Errorf("wait for replacement Pod: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) waitForReadyPodOnNode(ctx context.Context, request e2erunner.Request, componentSelector, nodeName string) (kubernetesPod, error) {
	return backend.waitForReadyReplacementPod(ctx, request, componentSelector, nodeName, "")
}

func (backend *scalewayBackend) waitForReadyReplacementPod(ctx context.Context, request e2erunner.Request, componentSelector, nodeName, oldUID string) (kubernetesPod, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		pod, err := backend.singularPod(waitCtx, request, componentSelector, nodeName)
		if err == nil && pod.Metadata.UID != oldUID && podReady(pod) {
			return pod, nil
		}
		select {
		case <-waitCtx.Done():
			return kubernetesPod{}, fmt.Errorf("wait for Ready replacement Pod: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func podReady(pod kubernetesPod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

func (backend *scalewayBackend) readControllerLease(ctx context.Context, request e2erunner.Request) (kubernetesLease, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "lease/scaleway-sfs-subdir-csi-controller", "-o", "json")
	if err != nil {
		return kubernetesLease{}, err
	}
	var lease kubernetesLease
	if err := json.Unmarshal(encoded, &lease); err != nil {
		return kubernetesLease{}, err
	}
	if lease.Metadata.UID == "" || lease.Spec.HolderIdentity == "" || lease.Metadata.Annotations == nil {
		return kubernetesLease{}, fmt.Errorf("controller Lease evidence is incomplete")
	}
	return lease, nil
}

func (backend *scalewayBackend) exactKapsuleNode(ctx context.Context, clusterID, poolID, nodeName, serverID string) ([]*k8sapi.Node, *k8sapi.Node, error) {
	listed, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: scw.Region(backend.plan.Region), ClusterID: clusterID, PoolID: &poolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, nil, err
	}
	var match *k8sapi.Node
	for _, node := range listed.Nodes {
		if node == nil || node.ClusterID != clusterID || node.PoolID != poolID {
			return nil, nil, fmt.Errorf("kapsule node inventory is incomplete or outside the exact pool")
		}
		if node.Name == nodeName && strings.HasSuffix(node.ProviderID, "/"+serverID) {
			if match != nil {
				return nil, nil, fmt.Errorf("kapsule controller node is ambiguous")
			}
			copy := *node
			match = &copy
		}
	}
	if match == nil {
		return nil, nil, fmt.Errorf("kapsule controller node is absent from the exact run pool")
	}
	return slices.Clone(listed.Nodes), match, nil
}

func (backend *scalewayBackend) createAbnormalTakeoverApproval(ctx context.Context, request e2erunner.Request, requestID string, holder map[string]string, approvedAt time.Time) (string, error) {
	stringData := map[string]string{
		"schemaVersion": "1", "mode": "abnormal-takeover", "requestID": requestID,
		"installationID": holder["holderInstallationID"], "activeClusterUID": holder["holderActiveClusterUID"],
		"previousHolderPodUID": holder["holderPodUID"], "previousHolderNodeName": holder["holderNodeName"],
		"previousHolderCSINodeID": holder["holderCSINodeID"], "previousHolderInstanceID": holder["holderInstanceID"],
		"previousHolderZone": holder["holderZone"], "checkpointRequestID": "", "checkpointManifestSHA256": "",
		"recoveryFenceScope": "", "reason": "real E2E abrupt controller-node stop",
		"approvedAt": approvedAt.Format(time.RFC3339Nano), "expiresAt": approvedAt.Add(30 * time.Minute).Format(time.RFC3339Nano),
	}
	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "Secret", "immutable": true, "type": "Opaque",
		"metadata":   map[string]any{"name": "sfs-subdir-controller-approval", "namespace": request.DriverNamespace},
		"stringData": stringData,
	})
	if err != nil {
		return "", err
	}
	if _, err := backend.kubectl(ctx, request, strings.NewReader(string(manifest)), "create", "-f", "-"); err != nil {
		return "", err
	}
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "secret/sfs-subdir-controller-approval", "-o", "json")
	if err != nil {
		return "", err
	}
	var secret kubernetesSecret
	if err := json.Unmarshal(encoded, &secret); err != nil || secret.Metadata.UID == "" {
		return "", fmt.Errorf("read immutable approval Secret UID: %w", err)
	}
	return secret.Metadata.UID, nil
}

func (backend *scalewayBackend) applyPVC(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, name string) error {
	manifest := fmt.Sprintf("apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    sfs-subdir-e2e-run: %q\n    sfs-subdir-e2e-scenario: controller-hard-failure\nspec:\n  accessModes: [ReadWriteMany]\n  storageClassName: sfs-subdir-rwx\n  resources: {requests: {storage: 16Mi}}\n", name, request.DriverNamespace, plan.RunID)
	_, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-")
	return err
}

func (backend *scalewayBackend) applyMarkerPod(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, name, claim, nodeName, scenario string) error {
	manifest := fmt.Sprintf("apiVersion: v1\nkind: Pod\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    sfs-subdir-e2e-run: %q\n    sfs-subdir-e2e-scenario: %s\nspec:\n  restartPolicy: Never\n  nodeSelector: {kubernetes.io/hostname: %q}\n  containers:\n    - name: workload\n      image: %s\n      command: [\"sh\", \"-c\", \"sleep 3600\"]\n      volumeMounts: [{name: data, mountPath: /data}]\n  volumes:\n    - name: data\n      persistentVolumeClaim: {claimName: %s}\n", name, request.DriverNamespace, plan.RunID, scenario, nodeName, request.WorkloadImage, claim)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return err
	}
	_, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "wait", "pod/"+name, "--for=condition=Ready", "--timeout=10m")
	return err
}

func (backend *scalewayBackend) applyReadMarkerPod(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, name, claim, nodeName, scenario string) error {
	if err := backend.applyMarkerPod(ctx, request, plan, name, claim, nodeName, scenario); err != nil {
		return err
	}
	_, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "exec", name, "--", "sh", "-c", "test \"$(cat /data/controller-hard-failure)\" = recovered-"+plan.RunID[:8])
	return err
}

func (backend *scalewayBackend) storageClassDriver(ctx context.Context, request e2erunner.Request) (string, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "get", "storageclass/sfs-subdir-rwx", "-o", "jsonpath={.provisioner}")
	if err != nil {
		return "", err
	}
	driver := strings.TrimSpace(string(encoded))
	if driver == "" {
		return "", fmt.Errorf("driver StorageClass provisioner is empty")
	}
	return driver, nil
}

func (backend *scalewayBackend) waitForCSINodeServer(ctx context.Context, request e2erunner.Request, driver, providerID string) (string, string, error) {
	serverID := providerID[strings.LastIndex(providerID, "/")+1:]
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		encoded, err := backend.kubectl(waitCtx, request, nil, "get", "csinodes", "-o", "json")
		if err == nil {
			var list kubernetesCSINodeList
			if json.Unmarshal(encoded, &list) == nil {
				for _, node := range list.Items {
					for _, registered := range node.Spec.Drivers {
						if registered.Name == driver && strings.HasSuffix(registered.NodeID, "/"+serverID) {
							return node.Metadata.Name, registered.NodeID, nil
						}
					}
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return "", "", fmt.Errorf("wait for replacement CSINode registration: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) waitInstanceAbsent(ctx context.Context, zone scw.Zone, serverID string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		_, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: zone, ServerID: serverID}, scw.WithContext(waitCtx))
		if providerNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("observe stopped Kapsule Instance deletion: %w", err)
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for stopped Kapsule Instance deletion: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func randomUUIDv4() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value)
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32], nil
}
