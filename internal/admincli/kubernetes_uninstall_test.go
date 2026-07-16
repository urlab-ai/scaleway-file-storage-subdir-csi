package admincli

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	operatorNamespace      = "driver-system"
	operatorRelease        = "driver"
	operatorDriverName     = "sfs-subdir.csi.example.com"
	operatorInstallationID = "11111111-1111-4111-8111-111111111111"
	operatorParentID       = "33333333-3333-4333-8333-333333333333"
	operatorInstanceID     = "55555555-5555-4555-8555-555555555555"
	operatorNodeID         = "fr-par-1/55555555-5555-4555-8555-555555555555"
	operatorControllerUID  = "66666666-6666-4666-8666-666666666666"
	operatorNodePodUID     = "77777777-7777-4777-8777-777777777777"
	operatorLeaseUID       = "88888888-8888-4888-8888-888888888888"
)

type fakePodExecutor struct {
	t        *testing.T
	calls    []string
	released admin.ControllerUninstallReleaseResult
}

func (executor *fakePodExecutor) Handshake(_ context.Context, namespace, podName string) (admin.HandshakeResponse, error) {
	executor.calls = append(executor.calls, "handshake:"+namespace+"/"+podName)
	return admin.HandshakeResponse{DriverVersion: "1.0.0", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0}, nil
}

func (executor *fakePodExecutor) Execute(_ context.Context, _, podName, phase, requestID string) (json.RawMessage, error) {
	executor.t.Helper()
	executor.calls = append(executor.calls, phase+":"+podName)
	var value any
	switch phase {
	case "inspect":
		value = admin.NodeUninstallInspection{NodeID: operatorNodeID}
	case "quiesce":
		value = admin.ControllerUninstallQuiesceResult{RequestID: requestID, Quiesced: true}
	case "prepare":
		value = admin.NodeUnmountEvidence{NodeID: operatorNodeID, UnmountedParents: []admin.ParentUnmountEvidence{{
			ParentFilesystemID: operatorParentID, MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + operatorParentID,
		}}}
	case "cleanup":
		value = admin.ControllerUninstallCleanupResult{RequestID: requestID, Evidence: admin.ControllerCleanupEvidence{
			UnmountedParents: []admin.ParentUnmountEvidence{{
				ParentFilesystemID: operatorParentID, MountPath: "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + operatorParentID,
			}},
			DetachedParentFilesystemIDs: []string{operatorParentID}, CheckedInstanceIDs: []string{operatorInstanceID},
			RegionalInventorySHA256: "sha256:" + strings.Repeat("a", 64),
			InstanceInventorySHA256: "sha256:" + strings.Repeat("b", 64), ProviderInventoriesFresh: true,
		}}
	case "release":
		value = executor.released
	default:
		executor.t.Fatalf("unexpected executor phase %q", phase)
	}
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		executor.t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	return encoded, nil
}

func (executor *fakePodExecutor) ExecuteDecommission(_ context.Context, _, podName, phase, requestID, parentFilesystemID string) (json.RawMessage, error) {
	if requestID != testRequestID || parentFilesystemID != operatorParentID {
		executor.t.Fatalf("decommission request/parent = %q/%q", requestID, parentFilesystemID)
	}
	executor.calls = append(executor.calls, "decommission."+phase+":"+podName)
	switch phase {
	case "inspect":
		if strings.Contains(podName, "controller") {
			return executor.encode(admin.ControllerDecommissionInspection{
				RequestID: requestID, ParentFilesystemID: parentFilesystemID,
				ParentState: pool.ParentDraining, Blockers: []string{},
				CheckedInstanceIDs: []string{operatorInstanceID},
			})
		}
		return executor.encode(admin.NodeDecommissionInspection{
			NodeID: operatorNodeID, ParentFilesystemID: parentFilesystemID,
			ParentMountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parentFilesystemID,
			ParentMounted:   true, StagingMountPaths: []string{}, WorkloadTargetPaths: []string{},
		})
	case "prepare":
		return executor.encode(admin.NodeDecommissionUnmountResult{
			NodeID:                     operatorNodeID,
			Unmounted:                  admin.ParentUnmountEvidence{ParentFilesystemID: parentFilesystemID, MountPath: "/var/lib/scaleway-sfs-subdir-csi/parents/" + parentFilesystemID},
			RemainingStagingMountPaths: []string{}, RemainingWorkloadTargetPaths: []string{},
		})
	case "quiesce":
		return executor.encode(admin.ControllerDecommissionQuiesceResult{RequestID: requestID, ParentFilesystemID: parentFilesystemID, Quiesced: true})
	case "cleanup":
		return executor.encode(admin.ControllerDecommissionCleanupResult{
			RequestID: requestID, ParentFilesystemID: parentFilesystemID,
			Evidence: admin.ControllerCleanupEvidence{
				UnmountedParents:            []admin.ParentUnmountEvidence{{ParentFilesystemID: parentFilesystemID, MountPath: "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + parentFilesystemID}},
				DetachedParentFilesystemIDs: []string{parentFilesystemID}, CheckedInstanceIDs: []string{operatorInstanceID},
				ProviderInventoriesFresh: true, RegionalAttachmentIDs: []string{}, InstanceAttachmentIDs: []string{},
				RegionalInventorySHA256: "sha256:" + strings.Repeat("a", 64), InstanceInventorySHA256: "sha256:" + strings.Repeat("b", 64),
				RemainingControllerMountPaths: []string{},
			},
		})
	case "release":
		released := operatorReleaseResult(executor.t)
		return executor.encode(admin.ControllerDecommissionReleaseResult{
			RequestID: requestID, ParentFilesystemID: parentFilesystemID,
			LeaseUID: released.LeaseUID, ResourceVersion: released.ResourceVersion,
			HolderIdentity: released.HolderIdentity, Annotations: released.Annotations,
		})
	default:
		executor.t.Fatalf("unexpected decommission phase %q", phase)
		return nil, nil
	}
}

func (executor *fakePodExecutor) encode(value any) (json.RawMessage, error) {
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		executor.t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	return encoded, nil
}

func operatorReleaseResult(t *testing.T) admin.ControllerUninstallReleaseResult {
	t.Helper()
	holder, err := coordination.NewHolderEvidence(
		operatorControllerUID, "worker-a", operatorNodeID, operatorInstanceID, "fr-par-1",
		operatorInstallationID, "99999999-9999-4999-8999-999999999999",
	)
	if err != nil {
		t.Fatalf("NewHolderEvidence() error = %v", err)
	}
	annotations, err := holder.Annotations()
	if err != nil {
		t.Fatalf("HolderEvidence.Annotations() error = %v", err)
	}
	current := coordination.LeaseSnapshot{
		UID: operatorLeaseUID, ResourceVersion: "1", HolderIdentity: holder.PodUID, Annotations: annotations,
	}
	released, err := coordination.PlanGracefulRelease(
		current, holder, testRequestID, time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC), 0, false,
	)
	if err != nil {
		t.Fatalf("PlanGracefulRelease() error = %v", err)
	}
	released.ResourceVersion = "2"
	return admin.ControllerUninstallReleaseResult{
		RequestID: testRequestID, LeaseUID: released.UID, ResourceVersion: released.ResourceVersion,
		HolderIdentity: released.HolderIdentity, Annotations: released.Annotations,
	}
}

func operatorRuntimeJSON(t *testing.T) (string, string) {
	t.Helper()
	ratio, err := pool.ParseRatio("1.0")
	if err != nil {
		t.Fatalf("ParseRatio() error = %v", err)
	}
	pools := []pool.Config{{
		Name: "standard", BasePath: "/kubernetes-volumes", SelectionPolicy: pool.SelectionLeastAllocated,
		MaxParentsPerEligibleNode: 1, MaxLogicalOvercommitRatio: ratio, MinFreeBytes: 1, MinFreePercent: 5,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryMode: "0770", DirectoryUID: 1000, DirectoryGID: 1000,
		Filesystems: []pool.ParentConfig{{ID: operatorParentID, Name: "parent-a", State: pool.ParentDraining}},
	}}
	generation, err := config.NodeConfigGeneration(
		operatorDriverName, "fr-par", "/var/lib/scaleway-sfs-subdir-csi/parents", "/var/lib/kubelet",
		[]string{"TEST-TYPE-1"}, pools,
	)
	if err != nil {
		t.Fatalf("NodeConfigGeneration() error = %v", err)
	}
	document := map[string]any{
		"schemaVersion": "1", "mode": "development", "driverName": operatorDriverName, "logLevel": "info",
		"controllerNamespace": operatorNamespace, "helmReleaseName": operatorRelease, "chartVersion": "1.0.0",
		"renderedImages": []map[string]string{
			{"name": "driver", "digest": ""}, {"name": "external-attacher", "digest": ""},
			{"name": "external-provisioner", "digest": ""}, {"name": "liveness-probe", "digest": ""},
			{"name": "node-driver-registrar", "digest": ""},
		},
		"nodeConfigGeneration": generation,
		"installation":         map[string]any{"existingSecretName": "driver-identity", "idKey": "installationID", "generateForDevelopmentOnly": false},
		"scaleway": map[string]any{
			"region": "fr-par", "defaultZone": "fr-par-1", "projectId": "22222222-2222-4222-8222-222222222222",
			"credentials": map[string]any{"existingSecretName": "driver-credentials", "accessKeyKey": "SCW_ACCESS_KEY", "secretKeyKey": "SCW_SECRET_KEY"},
		},
		"controller": map[string]any{
			"replicas": 1, "updateStrategy": "Recreate", "maxConcurrentMutations": 10,
			"shutdownDeadlineSeconds": 90, "terminationGracePeriodSeconds": 120,
			"progressDeadlineSeconds": 3900, "startupProbeBudgetSeconds": 3600,
			"attachReadyDeadlineSeconds": 600, "metadataRefreshIntervalSeconds": 300, "detailedTombstoneRetentionSeconds": 2592000,
			"parentMountRoot": "/var/lib/scaleway-sfs-subdir-csi/controller-parents",
			"leadership":      map[string]any{"enabled": true, "leaseName": volume.LeadershipLeaseNameV1, "leaseDurationSeconds": 30, "renewDeadlineSeconds": 20, "retryPeriodSeconds": 5},
		},
		"node":          map[string]any{"parentMountRoot": "/var/lib/scaleway-sfs-subdir-csi/parents", "kubeletPath": "/var/lib/kubelet"},
		"scheduling":    map[string]any{"allSchedulableLinuxNodesAreEligible": true, "requireHomogeneousEligibleNodes": true, "skipNodePreflightForDevelopmentOnly": false},
		"compatibility": map[string]any{"qualifiedCommercialTypes": []string{"TEST-TYPE-1"}},
		"pools": map[string]any{"standard": map[string]any{
			"basePath": "/kubernetes-volumes", "selectionPolicy": "least-allocated", "maxParentsPerEligibleNode": 1,
			"maxLogicalOvercommitRatio": "1.0", "minFreeBytes": 1, "minFreePercent": 5, "onDelete": "archive",
			"directoryMode": "0770", "directoryUid": "1000", "directoryGid": "1000",
			"filesystems": []map[string]any{{"id": operatorParentID, "name": "parent-a", "state": "draining"}},
		}},
		"storageClasses": []map[string]any{{
			"name": "sfs-subdir-rwx", "poolName": "standard", "defaultClass": false,
			"reclaimPolicy": "Delete", "allowVolumeExpansion": false, "volumeBindingMode": "Immediate",
		}},
	}
	encoded, err := canonicaljson.Marshal(document)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal(runtime) error = %v", err)
	}
	return string(encoded), generation
}

func readyPod(name, component, uid, generation string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: operatorNamespace, Name: name, UID: types.UID(uid),
			Labels: map[string]string{
				"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": operatorRelease,
				"app.kubernetes.io/component": component,
			},
			Annotations: map[string]string{"scaleway-sfs-subdir-csi.io/node-config-generation": generation},
		},
		Spec: corev1.PodSpec{NodeName: "worker-a", Containers: []corev1.Container{{Name: adminDriverContainer}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
		}}},
	}
}

func operatorObjects(t *testing.T) ([]runtime.Object, string) {
	t.Helper()
	runtimeJSON, generation := operatorRuntimeJSON(t)
	labels := map[string]string{"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": operatorRelease}
	controllerLabels := mapsWith(labels, "app.kubernetes.io/component", "controller")
	nodeLabels := mapsWith(labels, "app.kubernetes.io/component", "node")
	replicas := int32(1)
	objects := []runtime.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: "driver-identity"}, Data: map[string][]byte{"installationID": []byte(operatorInstallationID)}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: "driver-config", UID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Labels: labels}, Data: map[string]string{"config.json": runtimeJSON}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: "driver-controller", UID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", Labels: controllerLabels}, Spec: appsv1.DeploymentSpec{
			Replicas: &replicas, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
				Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "driver-config"}}},
			}}}},
		}},
		&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: "driver-node", UID: "cccccccc-cccc-4ccc-8ccc-cccccccccccc", Labels: nodeLabels}},
		readyPod("driver-controller-pod", "controller", operatorControllerUID, generation),
		readyPod("driver-node-pod", "node", operatorNodePodUID, generation),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Labels: map[string]string{"topology.kubernetes.io/zone": "fr-par-1"}}, Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			NodeInfo:   corev1.NodeSystemInfo{OperatingSystem: "linux"},
		}},
		&storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: "worker-a"}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: operatorDriverName, NodeID: operatorNodeID}}}},
		&storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "sfs-subdir-rwx"}, Provisioner: operatorDriverName},
	}
	return objects, generation
}

func mapsWith(source map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for sourceKey, sourceValue := range source {
		result[sourceKey] = sourceValue
	}
	result[key] = value
	return result
}

func newOperatorBackendHarness(t *testing.T) (*kubernetesUninstallBackend, *fake.Clientset, *fakePodExecutor) {
	t.Helper()
	objects, _ := operatorObjects(t)
	client := fake.NewClientset(objects...)
	installFakeScaleReactors(t, client)
	executor := &fakePodExecutor{t: t, released: operatorReleaseResult(t)}
	backend, err := newKubernetesUninstallBackendForClient(client, executor, operatorUninstallInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: testRequestID,
	}, "1.0.0")
	if err != nil {
		t.Fatalf("newKubernetesUninstallBackendForClient() error = %v", err)
	}
	backend.now = func() time.Time { return time.Date(2026, 7, 13, 20, 0, 0, 0, time.UTC) }
	return backend, client, executor
}

func installFakeScaleReactors(t *testing.T, client *fake.Clientset) {
	t.Helper()
	client.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		get := action.(clienttesting.GetAction)
		object, err := client.Tracker().Get(appsv1.SchemeGroupVersion.WithResource("deployments"), get.GetNamespace(), get.GetName(), metav1.GetOptions{})
		if err != nil {
			return true, nil, err
		}
		deployment := object.(*appsv1.Deployment)
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: deployment.Name, Namespace: deployment.Namespace, ResourceVersion: deployment.ResourceVersion},
			Spec:       autoscalingv1.ScaleSpec{Replicas: *deployment.Spec.Replicas},
		}, nil
	})
	client.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "scale" {
			return false, nil, nil
		}
		update := action.(clienttesting.UpdateAction)
		scale := update.GetObject().(*autoscalingv1.Scale)
		object, err := client.Tracker().Get(appsv1.SchemeGroupVersion.WithResource("deployments"), update.GetNamespace(), scale.Name, metav1.GetOptions{})
		if err != nil {
			return true, nil, err
		}
		deployment := object.(*appsv1.Deployment).DeepCopy()
		deployment.Spec.Replicas = new(int32)
		*deployment.Spec.Replicas = scale.Spec.Replicas
		if err := client.Tracker().Update(appsv1.SchemeGroupVersion.WithResource("deployments"), deployment, update.GetNamespace(), metav1.UpdateOptions{}); err != nil {
			return true, nil, err
		}
		return true, scale, nil
	})
}

func operatorMutationRequest() admin.MutationRequest {
	return admin.MutationRequest{
		RequestID: testRequestID, AdminVersion: "1.0.0",
		Protocol: admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
	}
}

func TestKubernetesUninstallStorageClassLimitCountsOnlyDriverObjects(t *testing.T) {
	objects := make([]runtime.Object, 0, adminMaxInventoryObjects+2)
	for index := 0; index <= adminMaxInventoryObjects; index++ {
		objects = append(objects, &storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: fmt.Sprintf("foreign-%05d", index)},
			Provisioner: "foreign.csi.example.com",
		})
	}
	objects = append(objects, &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: "driver-class"}, Provisioner: operatorDriverName,
	})
	client := fake.NewClientset(objects...)
	backend := &kubernetesUninstallBackend{client: client}
	classes, err := backend.listStorageClasses(context.Background(), operatorDriverName)
	if err != nil {
		t.Fatalf("listStorageClasses() error = %v", err)
	}
	if len(classes) != 1 || classes[0].Name != "driver-class" {
		t.Fatalf("driver StorageClasses = %#v", classes)
	}
}

func TestKubernetesUninstallBackendDryRunInventoryIsReadOnly(t *testing.T) {
	backend, client, executor := newOperatorBackendHarness(t)
	inventory, err := backend.ReadUninstallInventory(context.Background(), operatorMutationRequest())
	if err != nil {
		t.Fatalf("ReadUninstallInventory() error = %v", err)
	}
	if !inventory.Complete || inventory.DriverVersion != "1.0.0" || !slices.Equal(inventory.ParentFilesystemIDs, []string{operatorParentID}) || len(inventory.NodeTargets) != 1 || inventory.NodeTargets[0].NodeID != operatorNodeID {
		t.Fatalf("inventory = %#v", inventory)
	}
	if err := admin.ValidateUninstallPreflight(inventory.Preflight); err != nil {
		t.Fatalf("ValidateUninstallPreflight() error = %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(operatorNamespace).Get(context.Background(), progressConfigMapName(operatorNamespace, operatorRelease, testRequestID), metav1.GetOptions{}); err == nil {
		t.Fatal("read-only inventory created uninstall progress")
	}
	if !slices.Equal(executor.calls, []string{
		"handshake:driver-system/driver-controller-pod",
		"handshake:driver-system/driver-node-pod", "inspect:driver-node-pod",
	}) {
		t.Fatalf("executor calls = %#v", executor.calls)
	}
}

func TestUninstallPersistentVolumeInventoryIgnoresUnrelatedClusterScale(t *testing.T) {
	backend, client, _ := newOperatorBackendHarness(t)
	for index := 0; index < 4100; index++ {
		object := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foreign-pv-%04d", index)}}
		if err := client.Tracker().Add(object); err != nil {
			t.Fatalf("add unrelated PersistentVolume %d: %v", index, err)
		}
	}
	driverPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "driver-pv"},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{
			Driver: operatorDriverName, VolumeHandle: "sfs1:lv-11111111111111111111111111111111:mh-22222222222222222222222222222222",
		}}},
	}
	if err := client.Tracker().Add(driverPV); err != nil {
		t.Fatalf("add driver PersistentVolume: %v", err)
	}
	items, err := backend.listPersistentVolumes(context.Background(), operatorDriverName)
	if err != nil {
		t.Fatalf("listPersistentVolumes() error = %v", err)
	}
	if len(items) != 1 || items[0].Name != driverPV.Name {
		t.Fatalf("driver PersistentVolumes = %#v", items)
	}
}

func TestKubernetesUninstallBackendPersistsAndResumesOrderedCleanup(t *testing.T) {
	backend, client, executor := newOperatorBackendHarness(t)
	ctx := context.Background()
	request := operatorMutationRequest()
	if _, err := backend.ReadUninstallInventory(ctx, request); err != nil {
		t.Fatalf("initial ReadUninstallInventory() error = %v", err)
	}
	if err := backend.QuiesceController(ctx, testRequestID); err != nil {
		t.Fatalf("QuiesceController() error = %v", err)
	}
	if _, err := backend.ReadUninstallInventory(ctx, request); err != nil {
		t.Fatalf("quiesced ReadUninstallInventory() error = %v", err)
	}
	target := admin.UninstallNodeTarget{NodeID: operatorNodeID, PodName: "driver-node-pod"}
	evidence, err := backend.UnmountNodeParents(ctx, testRequestID, target)
	if err != nil || evidence.NodeID != operatorNodeID {
		t.Fatalf("UnmountNodeParents() evidence/error = %#v/%v", evidence, err)
	}
	if err := backend.DeleteNodePlugin(ctx, testRequestID); err != nil {
		t.Fatalf("DeleteNodePlugin() error = %v", err)
	}
	if err := client.CoreV1().Pods(operatorNamespace).Delete(ctx, "driver-node-pod", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete fake node Pod: %v", err)
	}
	if err := backend.WaitNodePluginStopped(ctx, testRequestID); err != nil {
		t.Fatalf("WaitNodePluginStopped() error = %v", err)
	}
	cleanup, err := backend.CleanupControllerParents(ctx, testRequestID)
	if err != nil || !cleanup.ProviderInventoriesFresh {
		t.Fatalf("CleanupControllerParents() result/error = %#v/%v", cleanup, err)
	}
	released, err := backend.ReleaseController(ctx, testRequestID)
	if err != nil || released.UID != operatorLeaseUID {
		t.Fatalf("ReleaseController() result/error = %#v/%v", released, err)
	}
	if err := backend.ScaleControllerToZero(ctx, testRequestID); err != nil {
		t.Fatalf("ScaleControllerToZero() error = %v", err)
	}
	if err := client.CoreV1().Pods(operatorNamespace).Delete(ctx, "driver-controller-pod", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete fake controller Pod: %v", err)
	}
	completed, err := backend.WaitControllerStopped(ctx, testRequestID)
	if err != nil || completed.Format(time.RFC3339Nano) != "2026-07-13T20:00:00Z" {
		t.Fatalf("WaitControllerStopped() time/error = %s/%v", completed, err)
	}

	resumedExecutor := &fakePodExecutor{t: t, released: operatorReleaseResult(t)}
	resumed, err := newKubernetesUninstallBackendForClient(client, resumedExecutor, operatorUninstallInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: testRequestID,
	}, "1.0.0")
	if err != nil {
		t.Fatalf("new resumed backend: %v", err)
	}
	if _, err := resumed.ReadUninstallInventory(ctx, request); err != nil {
		t.Fatalf("resumed ReadUninstallInventory() error = %v", err)
	}
	if err := resumed.QuiesceController(ctx, testRequestID); err != nil {
		t.Fatalf("resumed QuiesceController() error = %v", err)
	}
	if _, err := resumed.UnmountNodeParents(ctx, testRequestID, target); err != nil {
		t.Fatalf("resumed UnmountNodeParents() error = %v", err)
	}
	if err := resumed.DeleteNodePlugin(ctx, testRequestID); err != nil {
		t.Fatalf("resumed DeleteNodePlugin() error = %v", err)
	}
	if err := resumed.WaitNodePluginStopped(ctx, testRequestID); err != nil {
		t.Fatalf("resumed WaitNodePluginStopped() error = %v", err)
	}
	if _, err := resumed.CleanupControllerParents(ctx, testRequestID); err != nil {
		t.Fatalf("resumed CleanupControllerParents() error = %v", err)
	}
	if _, err := resumed.ReleaseController(ctx, testRequestID); err != nil {
		t.Fatalf("resumed ReleaseController() error = %v", err)
	}
	if err := resumed.ScaleControllerToZero(ctx, testRequestID); err != nil {
		t.Fatalf("resumed ScaleControllerToZero() error = %v", err)
	}
	resumedCompletion, err := resumed.WaitControllerStopped(ctx, testRequestID)
	if err != nil || !resumedCompletion.Equal(completed) || len(resumedExecutor.calls) != 0 {
		t.Fatalf("resumed completion/calls/error = %s/%#v/%v", resumedCompletion, resumedExecutor.calls, err)
	}
	if len(executor.calls) < 7 {
		t.Fatalf("initial executor calls = %#v", executor.calls)
	}
}

func TestParseOperatorUninstallIsClosedAndBounded(t *testing.T) {
	parsed, err := parseOperatorUninstall([]string{
		"uninstall", "prepare", "--namespace=" + operatorNamespace, "--release=" + operatorRelease,
		"--request-id=" + testRequestID, "--mode=dry-run", "--timeout=10m",
	})
	if err != nil {
		t.Fatalf("parseOperatorUninstall() error = %v", err)
	}
	if parsed.namespace != operatorNamespace || parsed.release != operatorRelease || parsed.mode != admin.UninstallDryRun || parsed.timeout != 10*time.Minute {
		t.Fatalf("parsed invocation = %#v", parsed)
	}
	for _, args := range [][]string{
		{"uninstall"},
		{"uninstall", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--mode=mutate"},
		{"uninstall", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--mode=dry-run", "--timeout=30s"},
		{"uninstall", "prepare", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--mode=dry-run", "--unknown=true"},
	} {
		if _, err := parseOperatorUninstall(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorUninstall(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}

func TestParseOperatorUpgradeIsClosedAndBounded(t *testing.T) {
	parsed, err := parseOperatorUpgrade([]string{
		"upgrade", "preflight", "--namespace=" + operatorNamespace, "--release=" + operatorRelease,
		"--request-id=" + testRequestID, "--candidate-file=/tmp/candidate.json", "--timeout=10m",
	})
	if err != nil {
		t.Fatalf("parseOperatorUpgrade() error = %v", err)
	}
	if parsed.namespace != operatorNamespace || parsed.release != operatorRelease || parsed.candidateFile != "/tmp/candidate.json" || parsed.timeout != 10*time.Minute {
		t.Fatalf("parsed upgrade = %#v", parsed)
	}
	for _, args := range [][]string{
		{"upgrade"},
		{"upgrade", "preflight", "--namespace=x", "--release=y", "--request-id=" + testRequestID},
		{"upgrade", "preflight", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--candidate-file=relative"},
		{"upgrade", "preflight", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--candidate-file=/tmp/candidate.json", "--timeout=30s"},
	} {
		if _, err := parseOperatorUpgrade(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorUpgrade(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}

func TestParseOperatorGCIsClosedAndBounded(t *testing.T) {
	logicalID := "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	parsed, err := parseOperatorGC([]string{
		"gc", "submit", "--namespace=" + operatorNamespace, "--release=" + operatorRelease,
		"--request-id=" + testRequestID, "--logical-volume-id=" + logicalID,
		"--mode=dry-run", "--expected-state=Archived", "--timeout=10m",
	})
	if err != nil {
		t.Fatalf("parseOperatorGC() error = %v", err)
	}
	if parsed.logicalVolumeID != logicalID || parsed.mode != "dry-run" || parsed.expectedState != volume.StateArchived || parsed.timeout != 10*time.Minute {
		t.Fatalf("parsed GC = %#v", parsed)
	}
	for _, args := range [][]string{
		{"gc"},
		{"gc", "submit", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--logical-volume-id=bad", "--mode=dry-run", "--expected-state=Archived"},
		{"gc", "submit", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--logical-volume-id=" + logicalID, "--mode=execute", "--expected-state=Deleted"},
		{"gc", "submit", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--logical-volume-id=" + logicalID, "--mode=dry-run", "--expected-state=Archived", "--timeout=30s"},
	} {
		if _, err := parseOperatorGC(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorGC(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}
