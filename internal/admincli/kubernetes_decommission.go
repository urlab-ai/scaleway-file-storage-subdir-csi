package admincli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const decommissionProgressSchemaV1 = "1"

// kubernetesDecommissionBackend owns only caller-authorized Kubernetes
// orchestration. The runtime controller retains provider credentials and the
// authority to validate records, unmount its target, and detach that target.
type kubernetesDecommissionBackend struct {
	mu sync.Mutex

	base         *kubernetesUninstallBackend
	executor     decommissionPodAdminExecutor
	client       kubernetes.Interface
	namespace    string
	release      string
	requestID    string
	parentID     string
	mode         admin.DecommissionMode
	buildVersion string
	now          func() time.Time

	current  *decommissionLiveInventory
	progress *storedDecommissionProgress
}

type decommissionLiveInventory struct {
	discovery            *uninstallDiscovery
	controllerInspection admin.ControllerDecommissionInspection
	blockers             []string
}

type decommissionProgress struct {
	SchemaVersion             string                                     `json:"schemaVersion"`
	Request                   admin.MutationRequest                      `json:"request"`
	Namespace                 string                                     `json:"namespace"`
	Release                   string                                     `json:"release"`
	ParentFilesystemID        string                                     `json:"parentFilesystemID"`
	ParentState               pool.ParentState                           `json:"parentState"`
	ChartVersion              string                                     `json:"chartVersion"`
	DriverVersion             string                                     `json:"driverVersion"`
	ConfigMap                 objectIdentity                             `json:"configMap"`
	Controller                objectIdentity                             `json:"controllerDeployment"`
	NodeDaemonSet             objectIdentity                             `json:"nodeDaemonSet"`
	ControllerPod             objectIdentity                             `json:"controllerPod"`
	ConfiguredParents         []string                                   `json:"configuredParentFilesystemIDs"`
	NodeTargets               []admin.UninstallNodeTarget                `json:"nodeTargets"`
	NodePods                  []nodePodIdentity                          `json:"nodePods"`
	CheckedInstanceIDs        []string                                   `json:"checkedInstanceIDs"`
	NodeParentMountRoot       string                                     `json:"nodeParentMountRoot"`
	ControllerParentMountRoot string                                     `json:"controllerParentMountRoot"`
	Quiesced                  bool                                       `json:"quiesced"`
	PostQuiesceValidated      bool                                       `json:"postQuiesceValidated"`
	NodeEvidence              []admin.NodeDecommissionUnmountResult      `json:"nodeEvidence"`
	NodeDaemonSetDeleted      bool                                       `json:"nodeDaemonSetDeleted"`
	NodePluginStopped         bool                                       `json:"nodePluginStopped"`
	ControllerCleanup         *admin.ControllerCleanupEvidence           `json:"controllerCleanup,omitempty"`
	Released                  *admin.ControllerDecommissionReleaseResult `json:"released,omitempty"`
	ControllerScaled          bool                                       `json:"controllerScaled"`
	ControllerStopped         bool                                       `json:"controllerStopped"`
	CompletedAt               string                                     `json:"completedAt,omitempty"`
}

type storedDecommissionProgress struct {
	value           decommissionProgress
	resourceVersion string
}

func newKubernetesDecommissionBackend(_ context.Context, invocation operatorDecommissionInvocation, buildVersion string) (*kubernetesDecommissionBackend, error) {
	client, kubectl, err := newCallerKubernetesClient(invocation.kubeconfig, invocation.context, buildVersion)
	if err != nil {
		return nil, err
	}
	executor := &kubectlPodAdminExecutor{binary: kubectl, kubeconfig: invocation.kubeconfig, context: invocation.context}
	return newKubernetesDecommissionBackendForClient(client, executor, invocation, buildVersion)
}

func newKubernetesDecommissionBackendForClient(client kubernetes.Interface, executor decommissionPodAdminExecutor, invocation operatorDecommissionInvocation, buildVersion string) (*kubernetesDecommissionBackend, error) {
	if client == nil || executor == nil {
		return nil, fmt.Errorf("parent decommission Kubernetes or kubectl dependency is nil")
	}
	if buildVersion == "" || len(buildVersion) > 128 || strings.ContainsAny(buildVersion, "\x00\r\n") {
		return nil, fmt.Errorf("parent decommission admin build version is invalid")
	}
	if err := volume.ValidateOperationID(invocation.requestID); err != nil {
		return nil, err
	}
	if err := volume.ValidateParentFilesystemID(invocation.parentFilesystemID); err != nil {
		return nil, err
	}
	base, err := newKubernetesUninstallBackendForClient(client, executor, operatorUninstallInvocation{
		namespace: invocation.namespace, release: invocation.release, requestID: invocation.requestID,
	}, buildVersion)
	if err != nil {
		return nil, err
	}
	return &kubernetesDecommissionBackend{
		base: base, executor: executor, client: client, namespace: invocation.namespace,
		release: invocation.release, requestID: invocation.requestID,
		parentID: invocation.parentFilesystemID, mode: invocation.mode, buildVersion: buildVersion, now: time.Now,
	}, nil
}

func (backend *kubernetesDecommissionBackend) ReadDecommissionInventory(ctx context.Context, request admin.MutationRequest, parentFilesystemID string) (admin.DecommissionInventory, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.validateRequest(request, parentFilesystemID); err != nil {
		return admin.DecommissionInventory{}, err
	}
	progress, err := backend.loadProgress(ctx)
	if err != nil {
		return admin.DecommissionInventory{}, err
	}
	if progress == nil {
		live, err := backend.collectLiveInventory(ctx, request, nil)
		if err != nil {
			return admin.DecommissionInventory{}, err
		}
		backend.current = live
		backend.progress = nil
		return backend.inventoryFromLive(request, live), nil
	}
	backend.progress = progress
	if !progress.value.PostQuiesceValidated {
		live, err := backend.collectLiveInventory(ctx, request, progress)
		if err != nil {
			return admin.DecommissionInventory{}, err
		}
		if err := backend.validateProgressAgainstLive(progress.value, live); err != nil {
			return admin.DecommissionInventory{}, err
		}
		if len(live.blockers) != 0 {
			return admin.DecommissionInventory{}, fmt.Errorf("parent decommission gained %d blocker(s) after quiesce; first is %s", len(live.blockers), live.blockers[0])
		}
		if backend.mode == admin.DecommissionDryRun {
			// A dry-run may inspect an existing interrupted execute request, but
			// it must never advance that request's durable recovery authority.
			// Return the freshly validated live view without the CAS below.
			backend.current = live
			return backend.inventoryFromLive(request, live), nil
		}
		updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
			next.PostQuiesceValidated = true
			return nil
		})
		if err != nil {
			return admin.DecommissionInventory{}, err
		}
		backend.progress = updated
		backend.current = live
	} else {
		// Even a fully cached retry revalidates the surviving Deployment,
		// ConfigMap, DaemonSet/Pod absence, and frozen UIDs. Cached phase
		// evidence substitutes only for operations whose Pods are gone.
		discovery, err := backend.base.discover(ctx, request, backend.syntheticUninstallProgress(progress.value))
		if err != nil {
			return admin.DecommissionInventory{}, err
		}
		if err := backend.validateFrozenDiscovery(progress.value, discovery); err != nil {
			return admin.DecommissionInventory{}, err
		}
	}
	return backend.inventoryFromProgress(progress.value), nil
}

func (backend *kubernetesDecommissionBackend) validateRequest(request admin.MutationRequest, parentFilesystemID string) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.RequestID != backend.requestID || request.AdminVersion != backend.buildVersion {
		return fmt.Errorf("parent decommission request differs from invocation")
	}
	if parentFilesystemID != backend.parentID {
		return fmt.Errorf("parent decommission target differs from invocation")
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) collectLiveInventory(ctx context.Context, request admin.MutationRequest, progress *storedDecommissionProgress) (*decommissionLiveInventory, error) {
	var synthetic *storedUninstallProgress
	if progress != nil {
		synthetic = backend.syntheticUninstallProgress(progress.value)
	}
	discovery, err := backend.base.discover(ctx, request, synthetic)
	if err != nil {
		return nil, err
	}
	state, configured := configuredParentState(discovery.loaded, backend.parentID)
	if !configured || state != pool.ParentDraining {
		return nil, fmt.Errorf("parent decommission target must remain configured and draining")
	}
	if discovery.controllerPod.Name == "" {
		return nil, fmt.Errorf("controller Pod is unavailable for decommission inventory")
	}
	controllerBytes, err := backend.executeDecommissionPhase(ctx, discovery.controllerPod.Name, "inspect", request.RequestID)
	if err != nil {
		return nil, fmt.Errorf("inspect parent through controller Pod: %w", err)
	}
	var controllerInspection admin.ControllerDecommissionInspection
	if err := strictjson.Decode(controllerBytes, &controllerInspection); err != nil {
		return nil, fmt.Errorf("decode controller decommission inspection: %w", err)
	}
	if err := controllerInspection.Validate(); err != nil {
		return nil, fmt.Errorf("validate controller decommission inspection: %w", err)
	}
	if controllerInspection.RequestID != request.RequestID || controllerInspection.ParentFilesystemID != backend.parentID {
		return nil, fmt.Errorf("controller decommission inspection differs from request")
	}
	nodeInstances := make([]string, 0, len(discovery.targets))
	blockers := slices.Clone(controllerInspection.Blockers)
	for _, target := range discovery.targets {
		parts := strings.SplitN(target.NodeID, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("decommission node ID %q has no Instance projection", target.NodeID)
		}
		nodeInstances = append(nodeInstances, parts[1])
		inspectionBytes, err := backend.executeDecommissionPhase(ctx, target.PodName, "inspect", request.RequestID)
		if err != nil {
			return nil, fmt.Errorf("inspect parent mounts through node Pod %q: %w", target.PodName, err)
		}
		var inspection admin.NodeDecommissionInspection
		if err := strictjson.Decode(inspectionBytes, &inspection); err != nil {
			return nil, fmt.Errorf("decode node decommission inspection from Pod %q: %w", target.PodName, err)
		}
		if err := validateNodeInspection(inspection, target, backend.parentID, discovery.loaded.Runtime.Node.ParentMountRoot); err != nil {
			return nil, err
		}
		for _, path := range inspection.StagingMountPaths {
			blockers = append(blockers, fmt.Sprintf("staging mount %q on node %q", path, target.NodeID))
		}
		for _, path := range inspection.WorkloadTargetPaths {
			blockers = append(blockers, fmt.Sprintf("workload target %q on node %q", path, target.NodeID))
		}
	}
	slices.Sort(nodeInstances)
	if !slices.Equal(nodeInstances, controllerInspection.CheckedInstanceIDs) {
		return nil, fmt.Errorf("controller Instance targets differ from release node targets")
	}
	kubernetesBlockers, err := backend.targetKubernetesBlockers(ctx, discovery.loaded)
	if err != nil {
		return nil, err
	}
	blockers = append(blockers, kubernetesBlockers...)
	slices.Sort(blockers)
	blockers = slices.Compact(blockers)
	return &decommissionLiveInventory{discovery: discovery, controllerInspection: controllerInspection, blockers: blockers}, nil
}

func configuredParentState(loaded config.Loaded, parentID string) (pool.ParentState, bool) {
	for _, configuredPool := range loaded.Runtime.Pools {
		for _, parent := range configuredPool.Filesystems {
			if parent.ID == parentID {
				return parent.State, true
			}
		}
	}
	return "", false
}

func validateNodeInspection(inspection admin.NodeDecommissionInspection, target admin.UninstallNodeTarget, parentID, root string) error {
	if inspection.NodeID != target.NodeID || inspection.ParentFilesystemID != parentID || inspection.ParentMountPath != root+"/"+parentID {
		return fmt.Errorf("node Pod %q returned mismatched decommission inspection", target.PodName)
	}
	for _, values := range [][]string{inspection.StagingMountPaths, inspection.WorkloadTargetPaths} {
		if !slices.IsSorted(values) || len(slices.Compact(slices.Clone(values))) != len(values) {
			return fmt.Errorf("node Pod %q returned unsorted or duplicate mount identities", target.PodName)
		}
		for _, value := range values {
			if value == "" || len(value) > 512 || !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\x00\r\n") {
				return fmt.Errorf("node Pod %q returned invalid mount identity", target.PodName)
			}
		}
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) executeDecommissionPhase(ctx context.Context, podName, phase, requestID string) ([]byte, error) {
	handshake, err := backend.executor.Handshake(ctx, backend.namespace, podName)
	if err != nil {
		return nil, fmt.Errorf("handshake with admin Pod %q before decommission %s: %w", podName, phase, err)
	}
	if err := admin.Negotiate(admin.HandshakeRequest{
		AdminVersion: backend.buildVersion,
		Protocol:     admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
	}, handshake); err != nil {
		return nil, err
	}
	return backend.executor.ExecuteDecommission(ctx, backend.namespace, podName, phase, requestID, backend.parentID)
}

func (backend *kubernetesDecommissionBackend) targetKubernetesBlockers(ctx context.Context, loaded config.Loaded) ([]string, error) {
	persistentVolumes, err := backend.base.listPersistentVolumes(ctx, loaded.Runtime.DriverName)
	if err != nil {
		return nil, err
	}
	targetPVs := make(map[string]struct{})
	blockers := make([]string, 0)
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI == nil || persistentVolume.Spec.CSI.Driver != loaded.Runtime.DriverName {
			continue
		}
		evidence := recovery.PersistentVolumeEvidence{
			Name: persistentVolume.Name, UID: string(persistentVolume.UID), ResourceVersion: persistentVolume.ResourceVersion,
			DriverName: persistentVolume.Spec.CSI.Driver, VolumeHandle: persistentVolume.Spec.CSI.VolumeHandle,
			VolumeContext: persistentVolume.Spec.CSI.VolumeAttributes,
		}
		immutable, err := evidence.Validate()
		if err != nil {
			return nil, fmt.Errorf("driver PersistentVolume %q: %w", persistentVolume.Name, err)
		}
		if immutable.InstallationID != loaded.Runtime.Installation.ID {
			return nil, fmt.Errorf("driver PersistentVolume %q belongs to another installation", persistentVolume.Name)
		}
		if immutable.ParentFilesystemID == backend.parentID {
			targetPVs[persistentVolume.Name] = struct{}{}
			blockers = append(blockers, fmt.Sprintf("PersistentVolume %q", persistentVolume.Name))
		}
	}
	claims, claimSet, err := backend.targetClaims(ctx, targetPVs)
	if err != nil {
		return nil, err
	}
	for _, claim := range claims {
		blockers = append(blockers, fmt.Sprintf("PersistentVolumeClaim %q", claim))
	}
	pods, err := backend.targetWorkloadPods(ctx, loaded.Runtime.DriverName, claimSet)
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		blockers = append(blockers, fmt.Sprintf("workload Pod %q", pod))
	}
	attachments, err := backend.targetVolumeAttachments(ctx, loaded.Runtime.DriverName, targetPVs)
	if err != nil {
		return nil, err
	}
	for _, attachment := range attachments {
		blockers = append(blockers, fmt.Sprintf("VolumeAttachment %q", attachment))
	}
	slices.Sort(blockers)
	return blockers, nil
}

func (backend *kubernetesDecommissionBackend) targetClaims(ctx context.Context, targetPVs map[string]struct{}) ([]string, map[string]struct{}, error) {
	result := make([]string, 0)
	set := make(map[string]struct{})
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, nil, fmt.Errorf("list PersistentVolumeClaims for parent decommission: %w", err)
		}
		for _, claim := range page.Items {
			if _, present := targetPVs[claim.Spec.VolumeName]; !present {
				continue
			}
			identity := claim.Namespace + "/" + claim.Name
			result = append(result, identity)
			set[identity] = struct{}{}
			if len(result) > adminMaxInventoryObjects {
				return nil, nil, fmt.Errorf("target PersistentVolumeClaim inventory exceeds %d objects", adminMaxInventoryObjects)
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, nil, fmt.Errorf("PersistentVolumeClaim inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
	slices.Sort(result)
	return result, set, nil
}

func (backend *kubernetesDecommissionBackend) targetWorkloadPods(ctx context.Context, driverName string, claims map[string]struct{}) ([]string, error) {
	result := make([]string, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list workload Pods for parent decommission: %w", err)
		}
		for _, pod := range page.Items {
			usesTarget := false
			for _, podVolume := range pod.Spec.Volumes {
				if podVolume.PersistentVolumeClaim != nil {
					_, usesTarget = claims[pod.Namespace+"/"+podVolume.PersistentVolumeClaim.ClaimName]
				}
				if podVolume.CSI != nil && podVolume.CSI.Driver == driverName {
					return nil, fmt.Errorf("inline CSI volume in Pod %q cannot be attributed safely to one parent", pod.Namespace+"/"+pod.Name)
				}
				if usesTarget {
					break
				}
			}
			if usesTarget {
				result = append(result, pod.Namespace+"/"+pod.Name)
				if len(result) > adminMaxInventoryObjects {
					return nil, fmt.Errorf("target workload Pod inventory exceeds %d objects", adminMaxInventoryObjects)
				}
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("workload Pod inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
	slices.Sort(result)
	return result, nil
}

func (backend *kubernetesDecommissionBackend) targetVolumeAttachments(ctx context.Context, driverName string, targetPVs map[string]struct{}) ([]string, error) {
	result := make([]string, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list VolumeAttachments for parent decommission: %w", err)
		}
		for _, attachment := range page.Items {
			if attachment.Spec.Attacher != driverName {
				continue
			}
			if attachment.Spec.Source.PersistentVolumeName == nil {
				return nil, fmt.Errorf("driver VolumeAttachment %q has no PersistentVolume source", attachment.Name)
			}
			if _, present := targetPVs[*attachment.Spec.Source.PersistentVolumeName]; present {
				result = append(result, attachment.Name)
				if len(result) > adminMaxInventoryObjects {
					return nil, fmt.Errorf("target VolumeAttachment inventory exceeds %d objects", adminMaxInventoryObjects)
				}
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("VolumeAttachment inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
	slices.Sort(result)
	return result, nil
}

func (backend *kubernetesDecommissionBackend) inventoryFromLive(request admin.MutationRequest, live *decommissionLiveInventory) admin.DecommissionInventory {
	return admin.DecommissionInventory{
		Complete: true, ParentFilesystemID: backend.parentID, ParentState: pool.ParentDraining,
		Blockers: slices.Clone(live.blockers), NodeTargets: slices.Clone(live.discovery.targets),
		NodeParentMountRoot:       live.discovery.loaded.Runtime.Node.ParentMountRoot,
		ControllerParentMountRoot: live.discovery.loaded.Runtime.Controller.ParentMountRoot,
		ChartVersion:              live.discovery.loaded.ChartVersion, DriverVersion: live.discovery.driverVersion,
	}
}

func (backend *kubernetesDecommissionBackend) inventoryFromProgress(progress decommissionProgress) admin.DecommissionInventory {
	return admin.DecommissionInventory{
		Complete: true, ParentFilesystemID: progress.ParentFilesystemID, ParentState: progress.ParentState,
		Blockers: []string{}, NodeTargets: slices.Clone(progress.NodeTargets),
		NodeParentMountRoot: progress.NodeParentMountRoot, ControllerParentMountRoot: progress.ControllerParentMountRoot,
		ChartVersion: progress.ChartVersion, DriverVersion: progress.DriverVersion,
	}
}

func (backend *kubernetesDecommissionBackend) QuiesceParent(ctx context.Context, requestID, parentFilesystemID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireRequest(requestID, parentFilesystemID); err != nil {
		return err
	}
	if backend.progress != nil && backend.progress.value.Quiesced {
		return nil
	}
	if backend.current == nil || len(backend.current.blockers) != 0 {
		return fmt.Errorf("parent decommission has no blocker-free current inventory")
	}
	if err := backend.base.requirePod(ctx, backend.current.discovery.controllerPod); err != nil {
		return err
	}
	output, err := backend.executeDecommissionPhase(ctx, backend.current.discovery.controllerPod.Name, "quiesce", requestID)
	if err != nil {
		return err
	}
	var result admin.ControllerDecommissionQuiesceResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID || result.ParentFilesystemID != parentFilesystemID || !result.Quiesced {
		return fmt.Errorf("controller returned invalid decommission quiesce evidence: %w", err)
	}
	progress := backend.progressFromCurrent()
	stored, err := backend.createProgress(ctx, progress)
	if err != nil {
		return err
	}
	backend.progress = stored
	return nil
}

func (backend *kubernetesDecommissionBackend) UnmountNodeParent(ctx context.Context, requestID, parentFilesystemID string, target admin.UninstallNodeTarget) (admin.NodeDecommissionUnmountResult, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, parentFilesystemID); err != nil {
		return admin.NodeDecommissionUnmountResult{}, err
	}
	if !backend.progress.value.PostQuiesceValidated {
		return admin.NodeDecommissionUnmountResult{}, fmt.Errorf("post-quiesce decommission inventory has not been durably validated")
	}
	for _, evidence := range backend.progress.value.NodeEvidence {
		if evidence.NodeID == target.NodeID {
			return cloneNodeDecommissionEvidence(evidence), nil
		}
	}
	identity, present := decommissionProgressNodePod(backend.progress.value, target)
	if !present {
		return admin.NodeDecommissionUnmountResult{}, fmt.Errorf("node target is outside frozen decommission progress")
	}
	if err := backend.base.requirePod(ctx, objectIdentity{Name: identity.PodName, UID: identity.PodUID}); err != nil {
		return admin.NodeDecommissionUnmountResult{}, err
	}
	output, err := backend.executeDecommissionPhase(ctx, target.PodName, "prepare", requestID)
	if err != nil {
		return admin.NodeDecommissionUnmountResult{}, err
	}
	var evidence admin.NodeDecommissionUnmountResult
	if err := strictjson.Decode(output, &evidence); err != nil {
		return admin.NodeDecommissionUnmountResult{}, fmt.Errorf("decode node decommission unmount evidence: %w", err)
	}
	if err := admin.ValidateNodeDecommissionUnmountEvidence(evidence, target.NodeID, parentFilesystemID, backend.progress.value.NodeParentMountRoot); err != nil {
		return admin.NodeDecommissionUnmountResult{}, err
	}
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.NodeEvidence = append(next.NodeEvidence, cloneNodeDecommissionEvidence(evidence))
		slices.SortFunc(next.NodeEvidence, func(left, right admin.NodeDecommissionUnmountResult) int {
			return strings.Compare(left.NodeID, right.NodeID)
		})
		return nil
	})
	if err != nil {
		return admin.NodeDecommissionUnmountResult{}, err
	}
	backend.progress = updated
	return cloneNodeDecommissionEvidence(evidence), nil
}

func (backend *kubernetesDecommissionBackend) DeleteNodePlugin(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, backend.parentID); err != nil {
		return err
	}
	if len(backend.progress.value.NodeEvidence) != len(backend.progress.value.NodeTargets) {
		return fmt.Errorf("cannot delete node DaemonSet before every target has durable target-parent unmount evidence")
	}
	if backend.progress.value.NodeDaemonSetDeleted {
		return nil
	}
	identity := backend.progress.value.NodeDaemonSet
	uid := types.UID(identity.UID)
	foreground := metav1.DeletePropagationForeground
	err := backend.client.AppsV1().DaemonSets(backend.namespace).Delete(ctx, identity.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid}, PropagationPolicy: &foreground,
	})
	if err != nil && !apierrors.IsNotFound(err) {
		observed, readErr := backend.client.AppsV1().DaemonSets(backend.namespace).Get(ctx, identity.Name, metav1.GetOptions{})
		if readErr == nil || !apierrors.IsNotFound(readErr) {
			return errors.Join(fmt.Errorf("delete exact node DaemonSet for decommission: %w", err), readErr, identityMismatch(observed, identity))
		}
	}
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.NodeDaemonSetDeleted = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesDecommissionBackend) WaitNodePluginStopped(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, backend.parentID); err != nil {
		return err
	}
	if !backend.progress.value.NodeDaemonSetDeleted {
		return fmt.Errorf("node DaemonSet deletion has not completed")
	}
	if backend.progress.value.NodePluginStopped {
		return nil
	}
	if err := backend.base.waitForPodAbsence(ctx, "node"); err != nil {
		return err
	}
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.NodePluginStopped = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesDecommissionBackend) CleanupControllerParent(ctx context.Context, requestID, parentFilesystemID string) (admin.ControllerCleanupEvidence, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, parentFilesystemID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if !backend.progress.value.NodePluginStopped {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("node plugin has not been proved stopped")
	}
	if backend.progress.value.ControllerCleanup != nil {
		return cloneControllerCleanup(*backend.progress.value.ControllerCleanup), nil
	}
	if err := backend.base.requirePod(ctx, backend.progress.value.ControllerPod); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	output, err := backend.executeDecommissionPhase(ctx, backend.progress.value.ControllerPod.Name, "cleanup", requestID)
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	var result admin.ControllerDecommissionCleanupResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID || result.ParentFilesystemID != parentFilesystemID {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("decode controller decommission cleanup evidence: %w", err)
	}
	if err := admin.ValidateDecommissionCleanupEvidence(result.Evidence, parentFilesystemID, backend.progress.value.NodeTargets, backend.progress.value.ControllerParentMountRoot); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	storedEvidence := cloneControllerCleanup(result.Evidence)
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.ControllerCleanup = &storedEvidence
		return nil
	})
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	backend.progress = updated
	return cloneControllerCleanup(storedEvidence), nil
}

func (backend *kubernetesDecommissionBackend) ReleaseControllerAfterDecommission(ctx context.Context, requestID, parentFilesystemID string) (coordination.LeaseSnapshot, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, parentFilesystemID); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	if backend.progress.value.ControllerCleanup == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("controller target cleanup evidence is absent")
	}
	if backend.progress.value.Released != nil {
		return backend.progress.value.Released.LeaseSnapshot(), nil
	}
	if err := backend.base.requirePod(ctx, backend.progress.value.ControllerPod); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	output, err := backend.executeDecommissionPhase(ctx, backend.progress.value.ControllerPod.Name, "release", requestID)
	if err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	var result admin.ControllerDecommissionReleaseResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID || result.ParentFilesystemID != parentFilesystemID {
		return coordination.LeaseSnapshot{}, fmt.Errorf("decode controller decommission release evidence: %w", err)
	}
	if err := result.Validate(); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	lease := result.LeaseSnapshot()
	if err := validateReleasedLease(requestID, lease); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	storedResult := result
	storedResult.Annotations = cloneStringMap(result.Annotations)
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.Released = &storedResult
		return nil
	})
	if err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	backend.progress = updated
	return lease, nil
}

func (backend *kubernetesDecommissionBackend) ScaleControllerToZero(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, backend.parentID); err != nil {
		return err
	}
	if backend.progress.value.Released == nil {
		return fmt.Errorf("controller leadership has not been released")
	}
	if backend.progress.value.ControllerScaled {
		return nil
	}
	identity := backend.progress.value.Controller
	deployment, err := backend.client.AppsV1().Deployments(backend.namespace).Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read exact controller Deployment before decommission scale: %w", err)
	}
	if string(deployment.UID) != identity.UID {
		return fmt.Errorf("controller Deployment UID changed before decommission scale")
	}
	scale, err := backend.client.AppsV1().Deployments(backend.namespace).GetScale(ctx, identity.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read controller Deployment scale: %w", err)
	}
	if scale.Spec.Replicas != 0 {
		scale.Spec.Replicas = 0
		if _, err := backend.client.AppsV1().Deployments(backend.namespace).UpdateScale(ctx, identity.Name, scale, metav1.UpdateOptions{}); err != nil {
			observed, readErr := backend.client.AppsV1().Deployments(backend.namespace).GetScale(ctx, identity.Name, metav1.GetOptions{})
			if readErr != nil || observed.Spec.Replicas != 0 {
				return errors.Join(fmt.Errorf("scale controller Deployment to zero for decommission: %w", err), readErr)
			}
		}
	}
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.ControllerScaled = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesDecommissionBackend) WaitControllerStopped(ctx context.Context, requestID string) (time.Time, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID, backend.parentID); err != nil {
		return time.Time{}, err
	}
	if backend.progress.value.ControllerStopped {
		return parseCompletionTime(backend.progress.value.CompletedAt)
	}
	if !backend.progress.value.ControllerScaled {
		return time.Time{}, fmt.Errorf("controller Deployment has not been scaled to zero")
	}
	if err := backend.base.waitForPodAbsence(ctx, "controller"); err != nil {
		return time.Time{}, err
	}
	completedAt := backend.now().UTC()
	completedText := completedAt.Format(time.RFC3339Nano)
	updated, err := backend.updateProgress(ctx, func(next *decommissionProgress) error {
		next.ControllerStopped = true
		next.CompletedAt = completedText
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	backend.progress = updated
	return completedAt, nil
}

func (backend *kubernetesDecommissionBackend) requireRequest(requestID, parentFilesystemID string) error {
	if requestID != backend.requestID || parentFilesystemID != backend.parentID {
		return fmt.Errorf("parent decommission request or target differs from invocation")
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) requireProgress(requestID, parentFilesystemID string) error {
	if err := backend.requireRequest(requestID, parentFilesystemID); err != nil {
		return err
	}
	if backend.progress == nil || !backend.progress.value.Quiesced {
		return fmt.Errorf("parent decommission request does not own durable quiesced progress")
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) progressFromCurrent() decommissionProgress {
	discovery := backend.current.discovery
	return decommissionProgress{
		SchemaVersion: decommissionProgressSchemaV1,
		Request:       admin.MutationRequest{RequestID: backend.requestID, AdminVersion: backend.buildVersion, Protocol: admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1}},
		Namespace:     backend.namespace, Release: backend.release, ParentFilesystemID: backend.parentID,
		ParentState: pool.ParentDraining, ChartVersion: discovery.loaded.ChartVersion, DriverVersion: discovery.driverVersion,
		ConfigMap:  objectIdentity{Name: discovery.configMapName, UID: discovery.configMapUID},
		Controller: discovery.controllerDeployment, NodeDaemonSet: discovery.nodeDaemonSet, ControllerPod: discovery.controllerPod,
		ConfiguredParents: slices.Clone(discovery.parents), NodeTargets: slices.Clone(discovery.targets), NodePods: slices.Clone(discovery.nodePods),
		CheckedInstanceIDs:        slices.Clone(backend.current.controllerInspection.CheckedInstanceIDs),
		NodeParentMountRoot:       discovery.loaded.Runtime.Node.ParentMountRoot,
		ControllerParentMountRoot: discovery.loaded.Runtime.Controller.ParentMountRoot,
		Quiesced:                  true,
	}
}

func decommissionProgressConfigMapName(namespace, release, requestID string) string {
	digest := sha256.Sum256([]byte(namespace + "\n" + release + "\n" + requestID))
	return "sfs-subdir-decommission-" + hex.EncodeToString(digest[:20])
}

func (backend *kubernetesDecommissionBackend) loadProgress(ctx context.Context) (*storedDecommissionProgress, error) {
	name := decommissionProgressConfigMapName(backend.namespace, backend.release, backend.requestID)
	object, err := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read parent decommission progress ConfigMap: %w", err)
	}
	return backend.decodeProgress(object)
}

func (backend *kubernetesDecommissionBackend) decodeProgress(object *corev1.ConfigMap) (*storedDecommissionProgress, error) {
	if object == nil || object.Name != decommissionProgressConfigMapName(backend.namespace, backend.release, backend.requestID) || len(object.BinaryData) != 0 || len(object.Data) != 1 {
		return nil, fmt.Errorf("parent decommission progress ConfigMap shape is invalid")
	}
	if object.Labels["app.kubernetes.io/name"] != adminApplicationName || object.Labels["app.kubernetes.io/instance"] != backend.release || object.Labels["scaleway-sfs-subdir-csi.io/decommission-request"] != backend.requestID || object.Labels["scaleway-sfs-subdir-csi.io/parent-filesystem-id"] != backend.parentID {
		return nil, fmt.Errorf("parent decommission progress ConfigMap labels disagree with request")
	}
	encoded, present := object.Data[adminProgressDataKey]
	if !present || len(encoded) == 0 || len(encoded) > config.MaxRuntimeFileBytes {
		return nil, fmt.Errorf("parent decommission progress document is absent or over limit")
	}
	var progress decommissionProgress
	if err := strictjson.Decode([]byte(encoded), &progress); err != nil {
		return nil, fmt.Errorf("decode parent decommission progress: %w", err)
	}
	if err := backend.validateProgress(progress); err != nil {
		return nil, err
	}
	if len(object.OwnerReferences) != 1 {
		return nil, fmt.Errorf("parent decommission progress must have exactly one controller Deployment owner")
	}
	owner := object.OwnerReferences[0]
	if owner.APIVersion != "apps/v1" || owner.Kind != "Deployment" || owner.Name != progress.Controller.Name || string(owner.UID) != progress.Controller.UID {
		return nil, fmt.Errorf("parent decommission progress owner differs from frozen controller Deployment")
	}
	return &storedDecommissionProgress{value: progress, resourceVersion: object.ResourceVersion}, nil
}

func (backend *kubernetesDecommissionBackend) validateProgress(progress decommissionProgress) error {
	if progress.SchemaVersion != decommissionProgressSchemaV1 || progress.Namespace != backend.namespace || progress.Release != backend.release || progress.ParentFilesystemID != backend.parentID || progress.ParentState != pool.ParentDraining || progress.Request.RequestID != backend.requestID || progress.Request.AdminVersion != backend.buildVersion {
		return fmt.Errorf("parent decommission progress identity disagrees with invocation")
	}
	if err := progress.Request.Validate(); err != nil {
		return err
	}
	if !progress.Quiesced || len(progress.ConfiguredParents) == 0 || !slices.Contains(progress.ConfiguredParents, backend.parentID) || len(progress.NodeTargets) == 0 || len(progress.NodeTargets) != len(progress.NodePods) || len(progress.CheckedInstanceIDs) != len(progress.NodeTargets) {
		return fmt.Errorf("parent decommission progress lacks frozen quiesced identities")
	}
	for name, value := range map[string]string{"chart version": progress.ChartVersion, "driver version": progress.DriverVersion} {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("parent decommission progress %s is invalid", name)
		}
	}
	if err := mount.ValidateAbsoluteNormalizedPath(progress.NodeParentMountRoot); err != nil {
		return fmt.Errorf("parent decommission progress node parent root: %w", err)
	}
	if err := mount.ValidateAbsoluteNormalizedPath(progress.ControllerParentMountRoot); err != nil {
		return fmt.Errorf("parent decommission progress controller parent root: %w", err)
	}
	if !slices.IsSorted(progress.ConfiguredParents) || len(slices.Compact(slices.Clone(progress.ConfiguredParents))) != len(progress.ConfiguredParents) {
		return fmt.Errorf("parent decommission progress configured parent set is not sorted and unique")
	}
	for _, parentID := range progress.ConfiguredParents {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return err
		}
	}
	if !slices.IsSortedFunc(progress.NodeTargets, func(left, right admin.UninstallNodeTarget) int { return strings.Compare(left.NodeID, right.NodeID) }) ||
		!slices.IsSortedFunc(progress.NodePods, func(left, right nodePodIdentity) int { return strings.Compare(left.NodeID, right.NodeID) }) ||
		!slices.IsSorted(progress.CheckedInstanceIDs) || len(slices.Compact(slices.Clone(progress.CheckedInstanceIDs))) != len(progress.CheckedInstanceIDs) {
		return fmt.Errorf("parent decommission progress node and Instance projections are not sorted and unique")
	}
	for _, instanceID := range progress.CheckedInstanceIDs {
		if err := volume.ValidateParentFilesystemID(instanceID); err != nil {
			return fmt.Errorf("parent decommission progress Instance ID: %w", err)
		}
	}
	for _, identity := range []objectIdentity{progress.ConfigMap, progress.Controller, progress.NodeDaemonSet, progress.ControllerPod} {
		if identity.Name == "" || identity.UID == "" || len(identity.Name) > 253 || len(identity.UID) > 128 || strings.ContainsAny(identity.Name+identity.UID, "\x00\r\n") {
			return fmt.Errorf("parent decommission progress contains an invalid Kubernetes identity")
		}
	}
	if progress.NodeDaemonSetDeleted && len(progress.NodeEvidence) != len(progress.NodeTargets) {
		return fmt.Errorf("parent decommission progress deleted node DaemonSet without complete node evidence")
	}
	if len(progress.NodeEvidence) != 0 && !progress.PostQuiesceValidated || progress.NodePluginStopped && !progress.NodeDaemonSetDeleted || progress.ControllerCleanup != nil && !progress.NodePluginStopped || progress.Released != nil && progress.ControllerCleanup == nil || progress.ControllerScaled && progress.Released == nil || progress.ControllerStopped && !progress.ControllerScaled {
		return fmt.Errorf("parent decommission progress phases are out of order")
	}
	if progress.ControllerStopped {
		if _, err := parseCompletionTime(progress.CompletedAt); err != nil {
			return err
		}
	} else if progress.CompletedAt != "" {
		return fmt.Errorf("parent decommission progress has premature completion time")
	}
	seenNodes := make(map[string]struct{}, len(progress.NodeTargets))
	seenPods := make(map[string]struct{}, len(progress.NodeTargets))
	instances := make([]string, 0, len(progress.NodeTargets))
	for index, target := range progress.NodeTargets {
		if err := volume.ValidateNodeID(target.NodeID); err != nil {
			return err
		}
		if _, duplicate := seenNodes[target.NodeID]; duplicate {
			return fmt.Errorf("parent decommission progress duplicates node %q", target.NodeID)
		}
		seenNodes[target.NodeID] = struct{}{}
		if progress.NodePods[index].NodeID != target.NodeID || progress.NodePods[index].PodName != target.PodName || progress.NodePods[index].PodUID == "" {
			return fmt.Errorf("parent decommission progress node Pod projection differs from targets")
		}
		if target.PodName == "" || len(target.PodName) > 253 || strings.ContainsAny(target.PodName, "\x00\r\n") || len(progress.NodePods[index].PodUID) > 128 || strings.ContainsAny(progress.NodePods[index].PodUID, "\x00\r\n") {
			return fmt.Errorf("parent decommission progress contains an invalid node Pod identity")
		}
		if _, duplicate := seenPods[target.PodName]; duplicate {
			return fmt.Errorf("parent decommission progress duplicates node Pod %q", target.PodName)
		}
		seenPods[target.PodName] = struct{}{}
		instances = append(instances, strings.SplitN(target.NodeID, "/", 2)[1])
	}
	slices.Sort(instances)
	if !slices.Equal(instances, progress.CheckedInstanceIDs) {
		return fmt.Errorf("parent decommission progress checked Instances differ from node targets")
	}
	if !slices.IsSortedFunc(progress.NodeEvidence, func(left, right admin.NodeDecommissionUnmountResult) int {
		return strings.Compare(left.NodeID, right.NodeID)
	}) {
		return fmt.Errorf("parent decommission progress node evidence is not sorted")
	}
	seenEvidence := make(map[string]struct{}, len(progress.NodeEvidence))
	for _, evidence := range progress.NodeEvidence {
		if err := admin.ValidateNodeDecommissionUnmountEvidence(evidence, evidence.NodeID, backend.parentID, progress.NodeParentMountRoot); err != nil {
			return err
		}
		if _, present := seenNodes[evidence.NodeID]; !present {
			return fmt.Errorf("parent decommission progress contains evidence outside frozen nodes")
		}
		if _, duplicate := seenEvidence[evidence.NodeID]; duplicate {
			return fmt.Errorf("parent decommission progress duplicates node evidence %q", evidence.NodeID)
		}
		seenEvidence[evidence.NodeID] = struct{}{}
	}
	if progress.ControllerCleanup != nil {
		if err := admin.ValidateDecommissionCleanupEvidence(*progress.ControllerCleanup, backend.parentID, progress.NodeTargets, progress.ControllerParentMountRoot); err != nil {
			return err
		}
	}
	if progress.Released != nil {
		if progress.Released.RequestID != backend.requestID || progress.Released.ParentFilesystemID != backend.parentID {
			return fmt.Errorf("parent decommission release progress differs from request")
		}
		if err := progress.Released.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) validateProgressAgainstLive(progress decommissionProgress, live *decommissionLiveInventory) error {
	if err := backend.validateFrozenDiscovery(progress, live.discovery); err != nil {
		return err
	}
	if !slices.Equal(progress.CheckedInstanceIDs, live.controllerInspection.CheckedInstanceIDs) {
		return fmt.Errorf("parent decommission Instance targets changed after quiesce")
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) validateFrozenDiscovery(progress decommissionProgress, discovery *uninstallDiscovery) error {
	if progress.ConfigMap != (objectIdentity{Name: discovery.configMapName, UID: discovery.configMapUID}) || progress.Controller != discovery.controllerDeployment || progress.ChartVersion != discovery.loaded.ChartVersion || progress.DriverVersion != discovery.driverVersion || progress.NodeParentMountRoot != discovery.loaded.Runtime.Node.ParentMountRoot || progress.ControllerParentMountRoot != discovery.loaded.Runtime.Controller.ParentMountRoot || !slices.Equal(progress.ConfiguredParents, discovery.parents) {
		return fmt.Errorf("parent decommission durable progress disagrees with current immutable release identities")
	}
	if discovery.nodeDaemonSet.Name != "" && progress.NodeDaemonSet != discovery.nodeDaemonSet {
		return fmt.Errorf("parent decommission node DaemonSet identity changed")
	}
	if discovery.controllerPod.Name != "" && progress.ControllerPod != discovery.controllerPod {
		return fmt.Errorf("parent decommission controller Pod identity changed")
	}
	if len(discovery.targets) != 0 && (!slices.Equal(progress.NodeTargets, discovery.targets) || !slices.Equal(progress.NodePods, discovery.nodePods)) {
		return fmt.Errorf("parent decommission node Pod identities changed")
	}
	return nil
}

func (backend *kubernetesDecommissionBackend) syntheticUninstallProgress(progress decommissionProgress) *storedUninstallProgress {
	return &storedUninstallProgress{value: uninstallProgress{
		SchemaVersion: adminProgressSchemaV1, Request: progress.Request, Namespace: progress.Namespace, Release: progress.Release,
		ChartVersion: progress.ChartVersion, DriverVersion: progress.DriverVersion,
		ConfigMap: progress.ConfigMap, Controller: progress.Controller, NodeDaemonSet: progress.NodeDaemonSet,
		ControllerPod: progress.ControllerPod, Parents: slices.Clone(progress.ConfiguredParents),
		NodeTargets: slices.Clone(progress.NodeTargets), NodePods: slices.Clone(progress.NodePods),
		NodeParentMountRoot: progress.NodeParentMountRoot, ControllerParentMountRoot: progress.ControllerParentMountRoot,
		Quiesced: true, NodeDaemonSetDeleted: progress.NodeDaemonSetDeleted, NodePluginStopped: progress.NodePluginStopped,
		Released: decommissionReleaseAsUninstall(progress.Released), ControllerScaled: progress.ControllerScaled,
		ControllerStopped: progress.ControllerStopped, CompletedAt: progress.CompletedAt,
	}}
}

func decommissionReleaseAsUninstall(result *admin.ControllerDecommissionReleaseResult) *admin.ControllerUninstallReleaseResult {
	if result == nil {
		return nil
	}
	return &admin.ControllerUninstallReleaseResult{
		RequestID: result.RequestID, LeaseUID: result.LeaseUID, ResourceVersion: result.ResourceVersion,
		HolderIdentity: result.HolderIdentity, Annotations: cloneStringMap(result.Annotations),
	}
}

func (backend *kubernetesDecommissionBackend) createProgress(ctx context.Context, progress decommissionProgress) (*storedDecommissionProgress, error) {
	encoded, err := canonicaljson.Marshal(progress)
	if err != nil {
		return nil, err
	}
	name := decommissionProgressConfigMapName(backend.namespace, backend.release, backend.requestID)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: backend.namespace, Name: name,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: progress.Controller.Name, UID: types.UID(progress.Controller.UID)}},
		Labels: map[string]string{
			"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": backend.release,
			"app.kubernetes.io/component":                     "decommission-progress",
			"scaleway-sfs-subdir-csi.io/decommission-request": backend.requestID,
			"scaleway-sfs-subdir-csi.io/parent-filesystem-id": backend.parentID,
		},
	}, Data: map[string]string{adminProgressDataKey: string(encoded)}}
	created, createErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Create(ctx, object, metav1.CreateOptions{})
	if createErr == nil {
		return backend.decodeProgress(created)
	}
	observed, readErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("create parent decommission progress: %w", createErr), readErr)
	}
	stored, decodeErr := backend.decodeProgress(observed)
	if decodeErr != nil {
		return nil, errors.Join(createErr, decodeErr)
	}
	observedBytes, _ := canonicaljson.Marshal(stored.value)
	if !bytes.Equal(observedBytes, encoded) {
		return nil, fmt.Errorf("parent decommission progress create collided with different state")
	}
	return stored, nil
}

func (backend *kubernetesDecommissionBackend) updateProgress(ctx context.Context, mutate func(*decommissionProgress) error) (*storedDecommissionProgress, error) {
	if backend.progress == nil || mutate == nil {
		return nil, fmt.Errorf("parent decommission progress update is not initialized")
	}
	next := cloneDecommissionProgress(backend.progress.value)
	if err := mutate(&next); err != nil {
		return nil, err
	}
	if err := backend.validateProgress(next); err != nil {
		return nil, err
	}
	encoded, err := canonicaljson.Marshal(next)
	if err != nil {
		return nil, err
	}
	name := decommissionProgressConfigMapName(backend.namespace, backend.release, backend.requestID)
	current, err := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read parent decommission progress for CAS: %w", err)
	}
	if current.ResourceVersion != backend.progress.resourceVersion {
		return nil, fmt.Errorf("parent decommission progress changed concurrently")
	}
	current.Data = map[string]string{adminProgressDataKey: string(encoded)}
	updated, updateErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Update(ctx, current, metav1.UpdateOptions{})
	if updateErr == nil {
		return backend.decodeProgress(updated)
	}
	observed, readErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("update parent decommission progress: %w", updateErr), readErr)
	}
	stored, decodeErr := backend.decodeProgress(observed)
	if decodeErr != nil {
		return nil, errors.Join(updateErr, decodeErr)
	}
	observedBytes, _ := canonicaljson.Marshal(stored.value)
	if !bytes.Equal(observedBytes, encoded) {
		return nil, fmt.Errorf("parent decommission progress update result is ambiguous: %w", updateErr)
	}
	return stored, nil
}

func cloneDecommissionProgress(value decommissionProgress) decommissionProgress {
	value.ConfiguredParents = slices.Clone(value.ConfiguredParents)
	value.NodeTargets = slices.Clone(value.NodeTargets)
	value.NodePods = slices.Clone(value.NodePods)
	value.CheckedInstanceIDs = slices.Clone(value.CheckedInstanceIDs)
	value.NodeEvidence = slices.Clone(value.NodeEvidence)
	for index := range value.NodeEvidence {
		value.NodeEvidence[index] = cloneNodeDecommissionEvidence(value.NodeEvidence[index])
	}
	if value.ControllerCleanup != nil {
		cleanup := cloneControllerCleanup(*value.ControllerCleanup)
		value.ControllerCleanup = &cleanup
	}
	if value.Released != nil {
		released := *value.Released
		released.Annotations = cloneStringMap(value.Released.Annotations)
		value.Released = &released
	}
	return value
}

func cloneNodeDecommissionEvidence(value admin.NodeDecommissionUnmountResult) admin.NodeDecommissionUnmountResult {
	value.RemainingStagingMountPaths = slices.Clone(value.RemainingStagingMountPaths)
	value.RemainingWorkloadTargetPaths = slices.Clone(value.RemainingWorkloadTargetPaths)
	return value
}

func decommissionProgressNodePod(progress decommissionProgress, target admin.UninstallNodeTarget) (nodePodIdentity, bool) {
	for _, identity := range progress.NodePods {
		if identity.NodeID == target.NodeID && identity.PodName == target.PodName {
			return identity, true
		}
	}
	return nodePodIdentity{}, false
}

var _ admin.DecommissionOperatorBackend = (*kubernetesDecommissionBackend)(nil)
