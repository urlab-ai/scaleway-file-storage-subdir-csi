package admincli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	driverk8s "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	adminApplicationName  = "scaleway-sfs-subdir-csi"
	adminDriverContainer  = "driver"
	adminProgressSchemaV1 = "1"
	adminProgressDataKey  = "progress.json"
	adminListPageSize     = int64(500)
	// adminMaxInventoryObjects matches the allocation adapter's 16,384-record
	// safety cap. Cluster-wide inventories count only driver-relevant objects;
	// unrelated tenants are paged through but never consume this bound.
	adminMaxInventoryObjects   = 16 * 1024
	adminMaxKubectlStderrBytes = 32 * 1024
)

type podAdminExecutor interface {
	Handshake(ctx context.Context, namespace, podName string) (admin.HandshakeResponse, error)
	Execute(ctx context.Context, namespace, podName, phase, requestID string) (json.RawMessage, error)
}

type decommissionPodAdminExecutor interface {
	podAdminExecutor
	ExecuteDecommission(ctx context.Context, namespace, podName, phase, requestID, parentFilesystemID string) (json.RawMessage, error)
}

type kubectlPodAdminExecutor struct {
	binary     string
	kubeconfig string
	context    string
}

func (executor *kubectlPodAdminExecutor) Handshake(ctx context.Context, namespace, podName string) (admin.HandshakeResponse, error) {
	output, err := executor.run(ctx, namespace, podName, []string{"local", "--timeout=5m", "handshake"}, nil)
	if err != nil {
		return admin.HandshakeResponse{}, err
	}
	var response admin.HandshakeResponse
	if err := strictjson.Decode(output, &response); err != nil {
		return admin.HandshakeResponse{}, fmt.Errorf("decode in-Pod admin handshake: %w", err)
	}
	if err := response.Validate(); err != nil {
		return admin.HandshakeResponse{}, err
	}
	return response, nil
}

func (executor *kubectlPodAdminExecutor) Execute(ctx context.Context, namespace, podName, phase, requestID string) (json.RawMessage, error) {
	allowed := map[string]struct{}{"inspect": {}, "prepare": {}, "quiesce": {}, "cleanup": {}, "release": {}}
	if _, present := allowed[phase]; !present {
		return nil, fmt.Errorf("unsupported in-Pod uninstall phase %q", phase)
	}
	if err := volume.ValidateOperationID(requestID); err != nil {
		return nil, err
	}
	output, err := executor.run(ctx, namespace, podName, []string{
		"local", "--timeout=5m", "uninstall", phase, "--request-id=" + requestID,
	}, nil)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := strictjson.Decode(output, &object); err != nil {
		return nil, fmt.Errorf("decode in-Pod uninstall %s result: %w", phase, err)
	}
	if object == nil {
		return nil, fmt.Errorf("in-Pod uninstall %s result is not an object", phase)
	}
	return append(json.RawMessage(nil), bytes.TrimSpace(output)...), nil
}

func (executor *kubectlPodAdminExecutor) ExecuteDecommission(ctx context.Context, namespace, podName, phase, requestID, parentFilesystemID string) (json.RawMessage, error) {
	allowed := map[string]struct{}{"inspect": {}, "prepare": {}, "quiesce": {}, "cleanup": {}, "release": {}}
	if _, present := allowed[phase]; !present {
		return nil, fmt.Errorf("unsupported in-Pod decommission phase %q", phase)
	}
	if err := volume.ValidateOperationID(requestID); err != nil {
		return nil, err
	}
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return nil, err
	}
	output, err := executor.run(ctx, namespace, podName, []string{
		"local", "--timeout=5m", "decommission", phase, "--request-id=" + requestID,
		"--parent-filesystem-id=" + parentFilesystemID,
	}, nil)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := strictjson.Decode(output, &object); err != nil {
		return nil, fmt.Errorf("decode in-Pod decommission %s result: %w", phase, err)
	}
	if object == nil {
		return nil, fmt.Errorf("in-Pod decommission %s result is not an object", phase)
	}
	return append(json.RawMessage(nil), bytes.TrimSpace(output)...), nil
}

func (executor *kubectlPodAdminExecutor) run(ctx context.Context, namespace, podName string, localArgs []string, input []byte) ([]byte, error) {
	if ctx == nil || executor == nil || executor.binary == "" {
		return nil, fmt.Errorf("kubectl admin executor is incomplete")
	}
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return nil, fmt.Errorf("kubectl namespace is invalid")
	}
	if problems := validation.IsDNS1123Subdomain(podName); len(problems) != 0 {
		return nil, fmt.Errorf("kubectl Pod name is invalid")
	}
	arguments := make([]string, 0, len(localArgs)+10)
	if executor.kubeconfig != "" {
		arguments = append(arguments, "--kubeconfig="+executor.kubeconfig)
	}
	if executor.context != "" {
		arguments = append(arguments, "--context="+executor.context)
	}
	arguments = append(arguments, "--namespace="+namespace, "exec")
	if input != nil {
		arguments = append(arguments, "--stdin=true")
	}
	arguments = append(arguments, podName, "--container="+adminDriverContainer, "--", "/usr/local/bin/csi-admin")
	arguments = append(arguments, localArgs...)
	command := exec.CommandContext(ctx, executor.binary, arguments...)
	stdout := &boundedCommandBuffer{maximum: admin.MaxWireMessageBytes + 1}
	stderr := &boundedCommandBuffer{maximum: adminMaxKubectlStderrBytes}
	command.Stdout = stdout
	command.Stderr = stderr
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	if err := command.Run(); err != nil {
		message := boundedCommandError(stderr.String())
		if message == "" {
			return nil, fmt.Errorf("kubectl exec in Pod %s/%s: %w", namespace, podName, err)
		}
		return nil, fmt.Errorf("kubectl exec in Pod %s/%s: %w: %s", namespace, podName, err, message)
	}
	if stdout.overflow {
		return nil, fmt.Errorf("kubectl exec output exceeds %d bytes", admin.MaxWireMessageBytes)
	}
	if len(bytes.TrimSpace(stdout.Bytes())) == 0 {
		return nil, fmt.Errorf("kubectl exec returned empty admin output")
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

func (executor *kubectlPodAdminExecutor) stream(ctx context.Context, namespace, podName string, localArgs []string, input []byte, destination io.Writer) error {
	if ctx == nil || executor == nil || executor.binary == "" || destination == nil {
		return fmt.Errorf("kubectl streaming admin executor is incomplete")
	}
	if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
		return fmt.Errorf("kubectl namespace is invalid")
	}
	if problems := validation.IsDNS1123Subdomain(podName); len(problems) != 0 {
		return fmt.Errorf("kubectl Pod name is invalid")
	}
	arguments := make([]string, 0, len(localArgs)+10)
	if executor.kubeconfig != "" {
		arguments = append(arguments, "--kubeconfig="+executor.kubeconfig)
	}
	if executor.context != "" {
		arguments = append(arguments, "--context="+executor.context)
	}
	arguments = append(arguments, "--namespace="+namespace, "exec", "--stdin=true")
	arguments = append(arguments, podName, "--container="+adminDriverContainer, "--", "/usr/local/bin/csi-admin")
	arguments = append(arguments, localArgs...)
	command := exec.CommandContext(ctx, executor.binary, arguments...)
	stderr := &boundedCommandBuffer{maximum: adminMaxKubectlStderrBytes}
	command.Stdin = bytes.NewReader(input)
	command.Stdout = destination
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		message := boundedCommandError(stderr.String())
		if message == "" {
			return fmt.Errorf("kubectl exec stream in Pod %s/%s: %w", namespace, podName, err)
		}
		return fmt.Errorf("kubectl exec stream in Pod %s/%s: %w: %s", namespace, podName, err, message)
	}
	return nil
}

type boundedCommandBuffer struct {
	buffer   bytes.Buffer
	maximum  int
	overflow bool
}

func (buffer *boundedCommandBuffer) Write(value []byte) (int, error) {
	written := len(value)
	remaining := buffer.maximum - buffer.buffer.Len()
	if remaining <= 0 {
		buffer.overflow = true
		return written, nil
	}
	if len(value) > remaining {
		value = value[:remaining]
		buffer.overflow = true
	}
	_, _ = buffer.buffer.Write(value)
	return written, nil
}

func (buffer *boundedCommandBuffer) Bytes() []byte  { return buffer.buffer.Bytes() }
func (buffer *boundedCommandBuffer) String() string { return buffer.buffer.String() }

func boundedCommandError(value string) string {
	value = strings.TrimSpace(strings.NewReplacer("\x00", " ", "\r", " ", "\n", " ").Replace(strings.ToValidUTF8(value, "?")))
	for len(value) > 1024 {
		_, size := utf8.DecodeLastRuneInString(value)
		value = value[:len(value)-size]
	}
	return value
}

type kubernetesUninstallBackend struct {
	mu sync.Mutex

	client       kubernetes.Interface
	executor     podAdminExecutor
	namespace    string
	release      string
	buildVersion string
	requestID    string
	now          func() time.Time

	current  *uninstallDiscovery
	progress *storedUninstallProgress
}

type uninstallDiscovery struct {
	loaded               config.Loaded
	configMapName        string
	configMapUID         string
	controllerDeployment objectIdentity
	nodeDaemonSet        objectIdentity
	controllerPod        objectIdentity
	nodePods             []nodePodIdentity
	parents              []string
	targets              []admin.UninstallNodeTarget
	driverVersion        string
	stagingMounts        []admin.NodeMountReference
	workloadTargets      []admin.NodeMountReference
}

type objectIdentity struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type nodePodIdentity struct {
	NodeID  string `json:"nodeID"`
	PodName string `json:"podName"`
	PodUID  string `json:"podUID"`
}

type uninstallProgress struct {
	SchemaVersion             string                                  `json:"schemaVersion"`
	Request                   admin.MutationRequest                   `json:"request"`
	Namespace                 string                                  `json:"namespace"`
	Release                   string                                  `json:"release"`
	ChartVersion              string                                  `json:"chartVersion"`
	DriverVersion             string                                  `json:"driverVersion"`
	ConfigMap                 objectIdentity                          `json:"configMap"`
	Controller                objectIdentity                          `json:"controllerDeployment"`
	NodeDaemonSet             objectIdentity                          `json:"nodeDaemonSet"`
	ControllerPod             objectIdentity                          `json:"controllerPod"`
	Parents                   []string                                `json:"parentFilesystemIDs"`
	NodeTargets               []admin.UninstallNodeTarget             `json:"nodeTargets"`
	NodePods                  []nodePodIdentity                       `json:"nodePods"`
	NodeParentMountRoot       string                                  `json:"nodeParentMountRoot"`
	ControllerParentMountRoot string                                  `json:"controllerParentMountRoot"`
	Quiesced                  bool                                    `json:"quiesced"`
	NodeEvidence              []admin.NodeUnmountEvidence             `json:"nodeEvidence"`
	NodeDaemonSetDeleted      bool                                    `json:"nodeDaemonSetDeleted"`
	NodePluginStopped         bool                                    `json:"nodePluginStopped"`
	ControllerCleanup         *admin.ControllerCleanupEvidence        `json:"controllerCleanup,omitempty"`
	Released                  *admin.ControllerUninstallReleaseResult `json:"released,omitempty"`
	ControllerScaled          bool                                    `json:"controllerScaled"`
	ControllerStopped         bool                                    `json:"controllerStopped"`
	CompletedAt               string                                  `json:"completedAt,omitempty"`
}

type storedUninstallProgress struct {
	value           uninstallProgress
	resourceVersion string
}

func newKubernetesUninstallBackend(_ context.Context, invocation operatorUninstallInvocation, buildVersion string) (*kubernetesUninstallBackend, error) {
	client, kubectl, err := newCallerKubernetesClient(invocation.kubeconfig, invocation.context, buildVersion)
	if err != nil {
		return nil, err
	}
	return newKubernetesUninstallBackendForClient(client, &kubectlPodAdminExecutor{
		binary: kubectl, kubeconfig: invocation.kubeconfig, context: invocation.context,
	}, invocation, buildVersion)
}

func newCallerKubernetesClient(kubeconfig, currentContext, buildVersion string) (kubernetes.Interface, string, error) {
	client, err := newCallerKubernetesClientOnly(kubeconfig, currentContext, buildVersion)
	if err != nil {
		return nil, "", err
	}
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		return nil, "", fmt.Errorf("find kubectl required for in-Pod admin execution: %w", err)
	}
	return client, kubectl, nil
}

func newCallerKubernetesClientOnly(kubeconfig, currentContext, buildVersion string) (kubernetes.Interface, error) {
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loading.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if currentContext != "" {
		overrides.CurrentContext = currentContext
	}
	clientConfiguration := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides)
	restConfiguration, err := clientConfiguration.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load caller kubeconfig: %w", err)
	}
	restConfiguration = rest.CopyConfig(restConfiguration)
	rest.AddUserAgent(restConfiguration, "scaleway-sfs-subdir-csi-admin/"+buildVersion)
	client, err := kubernetes.NewForConfig(restConfiguration)
	if err != nil {
		return nil, fmt.Errorf("construct caller-authorized Kubernetes client: %w", err)
	}
	return client, nil
}

func newKubernetesUninstallBackendForClient(client kubernetes.Interface, executor podAdminExecutor, invocation operatorUninstallInvocation, buildVersion string) (*kubernetesUninstallBackend, error) {
	if client == nil || executor == nil {
		return nil, fmt.Errorf("safe-uninstall Kubernetes or kubectl dependency is nil")
	}
	if buildVersion == "" || len(buildVersion) > 128 || strings.ContainsAny(buildVersion, "\x00\r\n") {
		return nil, fmt.Errorf("safe-uninstall admin build version is invalid")
	}
	if err := volume.ValidateOperationID(invocation.requestID); err != nil {
		return nil, err
	}
	if problems := validation.IsDNS1123Label(invocation.namespace); len(problems) != 0 {
		return nil, fmt.Errorf("safe-uninstall namespace is invalid")
	}
	if problems := validation.IsDNS1123Label(invocation.release); len(problems) != 0 {
		return nil, fmt.Errorf("safe-uninstall release is invalid")
	}
	return &kubernetesUninstallBackend{
		client: client, executor: executor, namespace: invocation.namespace,
		release: invocation.release, requestID: invocation.requestID, buildVersion: buildVersion,
		now: time.Now,
	}, nil
}

func (backend *kubernetesUninstallBackend) ReadUninstallInventory(ctx context.Context, request admin.MutationRequest) (admin.UninstallInventory, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.validateRequest(request); err != nil {
		return admin.UninstallInventory{}, err
	}
	progress, err := backend.loadProgress(ctx)
	if err != nil {
		return admin.UninstallInventory{}, err
	}
	discovery, err := backend.discover(ctx, request, progress)
	if err != nil {
		return admin.UninstallInventory{}, err
	}
	preflight, err := backend.readPreflight(ctx, request, discovery.loaded)
	if err != nil {
		return admin.UninstallInventory{}, err
	}
	preflight.StagingMounts = slices.Clone(discovery.stagingMounts)
	preflight.WorkloadTargets = slices.Clone(discovery.workloadTargets)
	backend.current = discovery
	backend.progress = progress
	return admin.UninstallInventory{
		Complete: true, Preflight: preflight, ParentFilesystemIDs: slices.Clone(discovery.parents),
		NodeTargets: slices.Clone(discovery.targets), NodeParentMountRoot: discovery.loaded.Runtime.Node.ParentMountRoot,
		ControllerParentMountRoot: discovery.loaded.Runtime.Controller.ParentMountRoot,
		ChartVersion:              discovery.loaded.ChartVersion, DriverVersion: discovery.driverVersion,
	}, nil
}

func (backend *kubernetesUninstallBackend) validateRequest(request admin.MutationRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.RequestID != backend.requestID || request.AdminVersion != backend.buildVersion || request.Protocol != (admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1}) {
		return fmt.Errorf("safe-uninstall request differs from the validated CLI invocation")
	}
	return nil
}

func releaseSelector(release, component string) string {
	return labels.SelectorFromSet(labels.Set{
		"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": release,
		"app.kubernetes.io/component": component,
	}).String()
}

func (backend *kubernetesUninstallBackend) discover(ctx context.Context, request admin.MutationRequest, progress *storedUninstallProgress) (*uninstallDiscovery, error) {
	deployments, err := backend.client.AppsV1().Deployments(backend.namespace).List(ctx, metav1.ListOptions{LabelSelector: releaseSelector(backend.release, "controller"), Limit: 2})
	if err != nil {
		return nil, fmt.Errorf("list release controller Deployment: %w", err)
	}
	if deployments.Continue != "" || len(deployments.Items) != 1 || deployments.Items[0].DeletionTimestamp != nil {
		return nil, fmt.Errorf("safe uninstall requires exactly one non-deleting controller Deployment")
	}
	deployment := &deployments.Items[0]
	configMapName, err := deploymentConfigMapName(deployment)
	if err != nil {
		return nil, err
	}
	configMap, err := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read release runtime ConfigMap %q: %w", configMapName, err)
	}
	loaded, err := backend.decodeRuntimeConfig(ctx, configMap)
	if err != nil {
		return nil, err
	}
	parents := configuredParentIDs(loaded)
	discovery := &uninstallDiscovery{
		loaded: loaded, configMapName: configMap.Name, configMapUID: string(configMap.UID),
		controllerDeployment: objectIdentity{Name: deployment.Name, UID: string(deployment.UID)}, parents: parents,
	}
	if progress != nil {
		if err := backend.validateProgressAgainstDiscovery(progress.value, request, discovery); err != nil {
			return nil, err
		}
		discovery.nodeDaemonSet = progress.value.NodeDaemonSet
		discovery.controllerPod = progress.value.ControllerPod
		discovery.nodePods = slices.Clone(progress.value.NodePods)
		discovery.targets = slices.Clone(progress.value.NodeTargets)
		discovery.driverVersion = progress.value.DriverVersion
	}

	daemonSets, err := backend.client.AppsV1().DaemonSets(backend.namespace).List(ctx, metav1.ListOptions{LabelSelector: releaseSelector(backend.release, "node"), Limit: 2})
	if err != nil {
		return nil, fmt.Errorf("list release node DaemonSet: %w", err)
	}
	if daemonSets.Continue != "" || len(daemonSets.Items) > 1 {
		return nil, fmt.Errorf("safe uninstall found an ambiguous node DaemonSet inventory")
	}
	nodeDaemonSetPresent := len(daemonSets.Items) != 0
	if !nodeDaemonSetPresent {
		if progress == nil || (!progress.value.NodeDaemonSetDeleted && len(progress.value.NodeEvidence) != len(progress.value.NodeTargets)) {
			return nil, fmt.Errorf("release node DaemonSet is absent without same-request deletion evidence")
		}
	} else {
		daemonSet := &daemonSets.Items[0]
		if daemonSet.DeletionTimestamp != nil {
			return nil, fmt.Errorf("release node DaemonSet deletion is still in progress")
		}
		observed := objectIdentity{Name: daemonSet.Name, UID: string(daemonSet.UID)}
		if progress != nil && observed != progress.value.NodeDaemonSet {
			return nil, fmt.Errorf("release node DaemonSet identity changed during safe uninstall")
		}
		discovery.nodeDaemonSet = observed
	}

	controllerScaleObserved := deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == 0
	if progress == nil || (!progress.value.ControllerScaled && (progress.value.Released == nil || !controllerScaleObserved)) {
		controllerPod, err := backend.singleActivePod(ctx, "controller")
		if err != nil {
			return nil, err
		}
		observed := objectIdentity{Name: controllerPod.Name, UID: string(controllerPod.UID)}
		if progress != nil && observed != progress.value.ControllerPod {
			return nil, fmt.Errorf("controller Pod identity changed during safe uninstall")
		}
		discovery.controllerPod = observed
		if progress == nil {
			handshake, err := backend.executor.Handshake(ctx, backend.namespace, controllerPod.Name)
			if err != nil {
				return nil, fmt.Errorf("handshake with controller Pod: %w", err)
			}
			if err := admin.Negotiate(admin.HandshakeRequest{
				AdminVersion: backend.buildVersion,
				Protocol:     admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
			}, handshake); err != nil {
				return nil, err
			}
			discovery.driverVersion = handshake.DriverVersion
		}
	} else if !controllerScaleObserved {
		return nil, fmt.Errorf("controller progress says scaled while Deployment replicas are not zero")
	}

	nodeStopObserved := false
	if progress != nil && !nodeDaemonSetPresent {
		pods, err := backend.listPodsIncludingDeleting(ctx, "node")
		if err != nil {
			return nil, err
		}
		if len(pods) == 0 {
			nodeStopObserved = true
		} else if progress.value.NodeDaemonSetDeleted {
			return nil, fmt.Errorf("node DaemonSet deletion still has %d terminating Pod(s)", len(pods))
		}
	}
	if progress == nil || (!progress.value.NodePluginStopped && !nodeStopObserved) {
		targets, nodePods, err := backend.discoverNodeTargets(ctx, loaded)
		if err != nil {
			return nil, err
		}
		if progress != nil && (!slices.Equal(targets, progress.value.NodeTargets) || !slices.Equal(nodePods, progress.value.NodePods)) {
			return nil, fmt.Errorf("node Pod identities changed during safe uninstall")
		}
		discovery.targets, discovery.nodePods = targets, nodePods
		for _, target := range targets {
			output, err := backend.executePhase(ctx, target.PodName, "inspect", request.RequestID)
			if err != nil {
				return nil, fmt.Errorf("inspect node mounts through Pod %q: %w", target.PodName, err)
			}
			var inspection admin.NodeUninstallInspection
			if err := strictjson.Decode(output, &inspection); err != nil {
				return nil, fmt.Errorf("decode node mount inspection from Pod %q: %w", target.PodName, err)
			}
			if inspection.NodeID != target.NodeID {
				return nil, fmt.Errorf("node Pod %q returned inspection for %q", target.PodName, inspection.NodeID)
			}
			for _, mountPath := range inspection.StagingMountPaths {
				discovery.stagingMounts = append(discovery.stagingMounts, admin.NodeMountReference{NodeID: target.NodeID, Path: mountPath})
			}
			for _, mountPath := range inspection.WorkloadTargetPaths {
				discovery.workloadTargets = append(discovery.workloadTargets, admin.NodeMountReference{NodeID: target.NodeID, Path: mountPath})
			}
		}
		slices.SortFunc(discovery.stagingMounts, compareNodeMountReferences)
		slices.SortFunc(discovery.workloadTargets, compareNodeMountReferences)
	} else {
		pods, err := backend.listPodsIncludingDeleting(ctx, "node")
		if err != nil {
			return nil, err
		}
		if len(pods) != 0 {
			return nil, fmt.Errorf("node plugin stop evidence disagrees with %d surviving Pods", len(pods))
		}
	}
	return discovery, nil
}

func (backend *kubernetesUninstallBackend) executePhase(ctx context.Context, podName, phase, requestID string) (json.RawMessage, error) {
	handshake, err := backend.executor.Handshake(ctx, backend.namespace, podName)
	if err != nil {
		return nil, fmt.Errorf("handshake with admin Pod %q before %s: %w", podName, phase, err)
	}
	if err := admin.Negotiate(admin.HandshakeRequest{
		AdminVersion: backend.buildVersion,
		Protocol:     admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
	}, handshake); err != nil {
		return nil, err
	}
	return backend.executor.Execute(ctx, backend.namespace, podName, phase, requestID)
}

func compareNodeMountReferences(left, right admin.NodeMountReference) int {
	if compared := strings.Compare(left.NodeID, right.NodeID); compared != 0 {
		return compared
	}
	return strings.Compare(left.Path, right.Path)
}

func deploymentConfigMapName(deployment *appsv1.Deployment) (string, error) {
	if deployment == nil {
		return "", fmt.Errorf("controller Deployment is nil")
	}
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == "config" && volume.ConfigMap != nil && volume.ConfigMap.Name != "" {
			return volume.ConfigMap.Name, nil
		}
	}
	return "", fmt.Errorf("controller Deployment does not reference the fixed runtime ConfigMap volume")
}

type runtimeIdentityProjection struct {
	Installation struct {
		ExistingSecretName string `json:"existingSecretName"`
		IDKey              string `json:"idKey"`
	} `json:"installation"`
}

func (backend *kubernetesUninstallBackend) decodeRuntimeConfig(ctx context.Context, object *corev1.ConfigMap) (config.Loaded, error) {
	if object == nil || len(object.BinaryData) != 0 || len(object.Data) != 1 {
		return config.Loaded{}, fmt.Errorf("runtime ConfigMap has an unsupported data shape")
	}
	data, present := object.Data["config.json"]
	if !present {
		return config.Loaded{}, fmt.Errorf("runtime ConfigMap lacks config.json")
	}
	var projection runtimeIdentityProjection
	if err := json.Unmarshal([]byte(data), &projection); err != nil {
		return config.Loaded{}, fmt.Errorf("read runtime identity projection: %w", err)
	}
	if projection.Installation.ExistingSecretName == "" || projection.Installation.IDKey == "" {
		return config.Loaded{}, fmt.Errorf("runtime identity Secret projection is incomplete")
	}
	secret, err := backend.client.CoreV1().Secrets(backend.namespace).Get(ctx, projection.Installation.ExistingSecretName, metav1.GetOptions{})
	if err != nil {
		return config.Loaded{}, fmt.Errorf("read installation identity Secret: %w", err)
	}
	installationID, present := secret.Data[projection.Installation.IDKey]
	if !present || len(installationID) == 0 {
		return config.Loaded{}, fmt.Errorf("installation identity Secret lacks configured key")
	}
	loaded, err := config.DecodeRuntimeFile([]byte(data), config.ComponentNode, func(key string) (string, bool) {
		if key == "INSTALLATION_ID" {
			return string(installationID), true
		}
		return "", false
	})
	if err != nil {
		return config.Loaded{}, fmt.Errorf("validate release runtime configuration: %w", err)
	}
	if loaded.ControllerNamespace != backend.namespace || loaded.HelmReleaseName != backend.release {
		return config.Loaded{}, fmt.Errorf("runtime namespace or Helm release differs from CLI scope")
	}
	return loaded, nil
}

func configuredParentIDs(loaded config.Loaded) []string {
	parents := make([]string, 0)
	for _, configuredPool := range loaded.Runtime.Pools {
		for _, parent := range configuredPool.Filesystems {
			parents = append(parents, parent.ID)
		}
	}
	slices.Sort(parents)
	return parents
}

func (backend *kubernetesUninstallBackend) singleActivePod(ctx context.Context, component string) (*corev1.Pod, error) {
	pods, err := backend.listPods(ctx, component)
	if err != nil {
		return nil, err
	}
	if len(pods) != 1 || !podAvailableForAdmin(&pods[0]) {
		return nil, fmt.Errorf("safe uninstall requires exactly one running %s Pod with its driver container", component)
	}
	return &pods[0], nil
}

func (backend *kubernetesUninstallBackend) listPods(ctx context.Context, component string) ([]corev1.Pod, error) {
	return backend.listReleasePods(ctx, component, false)
}

func (backend *kubernetesUninstallBackend) listPodsIncludingDeleting(ctx context.Context, component string) ([]corev1.Pod, error) {
	return backend.listReleasePods(ctx, component, true)
}

func (backend *kubernetesUninstallBackend) listReleasePods(ctx context.Context, component string, includeDeleting bool) ([]corev1.Pod, error) {
	result := make([]corev1.Pod, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().Pods(backend.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: releaseSelector(backend.release, component), Limit: adminListPageSize, Continue: continueToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list release %s Pods: %w", component, err)
		}
		if len(result)+len(page.Items) > adminMaxInventoryObjects {
			return nil, fmt.Errorf("release %s Pod inventory exceeds %d objects", component, adminMaxInventoryObjects)
		}
		for _, pod := range page.Items {
			if includeDeleting || pod.DeletionTimestamp == nil {
				result = append(result, pod)
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("release %s Pod inventory repeated continue token", component)
		}
		seen[continueToken] = struct{}{}
	}
	slices.SortFunc(result, func(left, right corev1.Pod) int { return strings.Compare(left.Name, right.Name) })
	return result, nil
}

func podReadyForAdmin(pod *corev1.Pod) bool {
	if !podAvailableForAdmin(pod) {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podAvailableForAdmin(pod *corev1.Pod) bool {
	if pod == nil || pod.Spec.NodeName == "" || pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == adminDriverContainer {
			return true
		}
	}
	return false
}

func (backend *kubernetesUninstallBackend) discoverNodeTargets(ctx context.Context, loaded config.Loaded) ([]admin.UninstallNodeTarget, []nodePodIdentity, error) {
	inventory, err := driverk8s.NewClientGoNodeInventory(
		backend.client.CoreV1(), backend.client.StorageV1(), backend.namespace,
		loaded.Runtime.DriverName, adminApplicationName, backend.release,
	)
	if err != nil {
		return nil, nil, err
	}
	observations, err := inventory.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	pods, err := backend.listPods(ctx, "node")
	if err != nil {
		return nil, nil, err
	}
	podByNode := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		if !podReadyForAdmin(&pod) {
			return nil, nil, fmt.Errorf("node Pod %q is not Ready for safe-uninstall admin execution", pod.Name)
		}
		if _, duplicate := podByNode[pod.Spec.NodeName]; duplicate {
			return nil, nil, fmt.Errorf("multiple node Pods target Kubernetes node %q", pod.Spec.NodeName)
		}
		podByNode[pod.Spec.NodeName] = pod
	}
	targets := make([]admin.UninstallNodeTarget, 0, len(pods))
	identities := make([]nodePodIdentity, 0, len(pods))
	for _, observation := range observations {
		pod, podPresent := podByNode[observation.NodeName]
		if observation.OperatingSystem != "linux" {
			if podPresent {
				return nil, nil, fmt.Errorf("node plugin Pod %q runs on non-Linux node", pod.Name)
			}
			continue
		}
		if observation.Schedulable || observation.DriverRegistered || podPresent {
			if observation.Deleting || !observation.Ready || !observation.PluginPodPresent || !observation.PluginPodReady || !observation.DriverRegistered || !podPresent {
				return nil, nil, fmt.Errorf("known Linux node %q is not Ready with one registered plugin Pod", observation.NodeName)
			}
			if observation.NodeConfigGeneration != loaded.NodeConfigGeneration {
				return nil, nil, fmt.Errorf("node %q configuration generation differs from runtime", observation.NodeName)
			}
			if err := volume.ValidateNodeID(observation.CSINodeID); err != nil {
				return nil, nil, fmt.Errorf("node %q CSI identity: %w", observation.NodeName, err)
			}
			if !strings.HasPrefix(observation.CSINodeID, loaded.Runtime.Provider.Region+"-") {
				return nil, nil, fmt.Errorf("node %q CSI identity is outside configured region", observation.NodeName)
			}
			targets = append(targets, admin.UninstallNodeTarget{NodeID: observation.CSINodeID, PodName: pod.Name})
			identities = append(identities, nodePodIdentity{NodeID: observation.CSINodeID, PodName: pod.Name, PodUID: string(pod.UID)})
		}
	}
	if len(targets) == 0 || len(targets) != len(pods) {
		return nil, nil, fmt.Errorf("safe-uninstall node target inventory is empty or incomplete")
	}
	slices.SortFunc(targets, func(left, right admin.UninstallNodeTarget) int { return strings.Compare(left.NodeID, right.NodeID) })
	slices.SortFunc(identities, func(left, right nodePodIdentity) int { return strings.Compare(left.NodeID, right.NodeID) })
	return targets, identities, nil
}

func (backend *kubernetesUninstallBackend) readPreflight(ctx context.Context, request admin.MutationRequest, loaded config.Loaded) (admin.UninstallPreflightSnapshot, error) {
	configMaps, err := driverk8s.NewClientGoConfigMaps(backend.client.CoreV1())
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	allocationStore, err := driverk8s.NewAllocationStore(configMaps, backend.namespace, loaded.Runtime.DriverName, loaded.Runtime.Installation.ID)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	stored, err := allocationStore.List(ctx)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	allocations := make([]volume.AllocationRecord, 0, len(stored))
	for _, value := range stored {
		allocations = append(allocations, value.Record)
	}
	persistentVolumes, err := backend.listPersistentVolumes(ctx, loaded.Runtime.DriverName)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	storageClasses, err := backend.listStorageClasses(ctx, loaded.Runtime.DriverName)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	claims, claimSet, err := backend.listDriverClaims(ctx, loaded.Runtime.DriverName, persistentVolumes, storageClasses)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	workloadPods, err := backend.listWorkloadPods(ctx, loaded.Runtime.DriverName, claimSet, storageClasses)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	attachments, err := backend.listDriverVolumeAttachments(ctx, loaded.Runtime.DriverName)
	if err != nil {
		return admin.UninstallPreflightSnapshot{}, err
	}
	pvNames := make([]string, 0)
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI != nil && persistentVolume.Spec.CSI.Driver == loaded.Runtime.DriverName {
			if _, err := volume.ParseHandle(persistentVolume.Spec.CSI.VolumeHandle); err != nil {
				return admin.UninstallPreflightSnapshot{}, fmt.Errorf("driver PersistentVolume %q handle: %w", persistentVolume.Name, err)
			}
			pvNames = append(pvNames, persistentVolume.Name)
		}
	}
	slices.Sort(pvNames)
	return admin.UninstallPreflightSnapshot{
		Request: request, Allocations: allocations, PersistentVolumeNames: pvNames,
		PersistentVolumeClaimNames: claims, VolumeAttachmentNames: attachments, WorkloadPodNames: workloadPods,
	}, nil
}

func (backend *kubernetesUninstallBackend) listPersistentVolumes(ctx context.Context, driverName string) ([]corev1.PersistentVolume, error) {
	result := make([]corev1.PersistentVolume, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list PersistentVolumes for safe uninstall: %w", err)
		}
		for _, object := range page.Items {
			if object.Spec.CSI == nil || object.Spec.CSI.Driver != driverName {
				continue
			}
			result = append(result, object)
			if len(result) > adminMaxInventoryObjects {
				return nil, fmt.Errorf("driver PersistentVolume inventory exceeds %d objects", adminMaxInventoryObjects)
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("PersistentVolume inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
}

func (backend *kubernetesUninstallBackend) listStorageClasses(ctx context.Context, driverName string) ([]storagev1.StorageClass, error) {
	result := make([]storagev1.StorageClass, 0)
	continueToken := ""
	seen := make(map[string]struct{})
	seen[""] = struct{}{}
	for {
		page, err := backend.client.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list StorageClasses for safe uninstall: %w", err)
		}
		for _, object := range page.Items {
			if object.Provisioner != driverName {
				continue
			}
			result = append(result, object)
			if len(result) > adminMaxInventoryObjects {
				return nil, fmt.Errorf("driver StorageClass inventory exceeds %d relevant objects", adminMaxInventoryObjects)
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("StorageClass inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
}

func (backend *kubernetesUninstallBackend) listDriverClaims(ctx context.Context, driverName string, persistentVolumes []corev1.PersistentVolume, storageClasses []storagev1.StorageClass) ([]string, map[string]struct{}, error) {
	driverPVs := make(map[string]struct{})
	for _, persistentVolume := range persistentVolumes {
		if persistentVolume.Spec.CSI != nil && persistentVolume.Spec.CSI.Driver == driverName {
			driverPVs[persistentVolume.Name] = struct{}{}
		}
	}
	driverClasses := make(map[string]struct{})
	for _, storageClass := range storageClasses {
		if storageClass.Provisioner == driverName {
			driverClasses[storageClass.Name] = struct{}{}
		}
	}
	claims := make([]string, 0)
	claimSet := make(map[string]struct{})
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, nil, fmt.Errorf("list PersistentVolumeClaims for safe uninstall: %w", err)
		}
		for _, claim := range page.Items {
			_, boundDriverPV := driverPVs[claim.Spec.VolumeName]
			_, driverClass := driverClasses[claimStorageClassName(&claim)]
			if !boundDriverPV && !driverClass {
				continue
			}
			identity := claim.Namespace + "/" + claim.Name
			claims = append(claims, identity)
			claimSet[identity] = struct{}{}
			if len(claims) > adminMaxInventoryObjects {
				return nil, nil, fmt.Errorf("driver PersistentVolumeClaim inventory exceeds %d objects", adminMaxInventoryObjects)
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
	slices.Sort(claims)
	return claims, claimSet, nil
}

func claimStorageClassName(claim *corev1.PersistentVolumeClaim) string {
	if claim == nil || claim.Spec.StorageClassName == nil {
		return ""
	}
	return *claim.Spec.StorageClassName
}

func (backend *kubernetesUninstallBackend) listWorkloadPods(ctx context.Context, driverName string, claims map[string]struct{}, storageClasses []storagev1.StorageClass) ([]string, error) {
	driverClasses := make(map[string]struct{})
	for _, storageClass := range storageClasses {
		if storageClass.Provisioner == driverName {
			driverClasses[storageClass.Name] = struct{}{}
		}
	}
	result := make([]string, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list workload Pods for safe uninstall: %w", err)
		}
		for _, pod := range page.Items {
			if podUsesDriver(&pod, driverName, claims, driverClasses) {
				result = append(result, pod.Namespace+"/"+pod.Name)
				if len(result) > adminMaxInventoryObjects {
					return nil, fmt.Errorf("driver workload Pod inventory exceeds %d objects", adminMaxInventoryObjects)
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

func podUsesDriver(pod *corev1.Pod, driverName string, claims, driverClasses map[string]struct{}) bool {
	for _, podVolume := range pod.Spec.Volumes {
		if podVolume.PersistentVolumeClaim != nil {
			if _, present := claims[pod.Namespace+"/"+podVolume.PersistentVolumeClaim.ClaimName]; present {
				return true
			}
		}
		if podVolume.CSI != nil && podVolume.CSI.Driver == driverName {
			return true
		}
		if podVolume.Ephemeral != nil && podVolume.Ephemeral.VolumeClaimTemplate != nil {
			name := claimStorageClassName(&corev1.PersistentVolumeClaim{Spec: podVolume.Ephemeral.VolumeClaimTemplate.Spec})
			if _, present := driverClasses[name]; present {
				return true
			}
		}
	}
	return false
}

func (backend *kubernetesUninstallBackend) listDriverVolumeAttachments(ctx context.Context, driverName string) ([]string, error) {
	result := make([]string, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := backend.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list VolumeAttachments for safe uninstall: %w", err)
		}
		for _, attachment := range page.Items {
			if attachment.Spec.Attacher == driverName {
				result = append(result, attachment.Name)
				if len(result) > adminMaxInventoryObjects {
					return nil, fmt.Errorf("driver VolumeAttachment inventory exceeds %d objects", adminMaxInventoryObjects)
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

// The ordered mutation methods and durable progress persistence follow below.

func (backend *kubernetesUninstallBackend) QuiesceController(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireCurrent(requestID); err != nil {
		return err
	}
	if backend.progress != nil && backend.progress.value.Quiesced {
		return nil
	}
	if err := backend.requirePod(ctx, backend.current.controllerPod); err != nil {
		return err
	}
	output, err := backend.executePhase(ctx, backend.current.controllerPod.Name, "quiesce", requestID)
	if err != nil {
		return err
	}
	var result admin.ControllerUninstallQuiesceResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID || !result.Quiesced {
		return fmt.Errorf("controller returned invalid uninstall quiesce evidence: %w", err)
	}
	progress := backend.progressFromCurrent(result.RequestID)
	stored, err := backend.createProgress(ctx, progress)
	if err != nil {
		return err
	}
	backend.progress = stored
	return nil
}

func (backend *kubernetesUninstallBackend) UnmountNodeParents(ctx context.Context, requestID string, target admin.UninstallNodeTarget) (admin.NodeUnmountEvidence, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return admin.NodeUnmountEvidence{}, err
	}
	for _, evidence := range backend.progress.value.NodeEvidence {
		if evidence.NodeID == target.NodeID {
			return cloneNodeEvidence(evidence), nil
		}
	}
	identity, present := progressNodePod(backend.progress.value, target)
	if !present {
		return admin.NodeUnmountEvidence{}, fmt.Errorf("node target is outside frozen safe-uninstall progress")
	}
	if err := backend.requirePod(ctx, objectIdentity{Name: identity.PodName, UID: identity.PodUID}); err != nil {
		return admin.NodeUnmountEvidence{}, err
	}
	output, err := backend.executePhase(ctx, target.PodName, "prepare", requestID)
	if err != nil {
		return admin.NodeUnmountEvidence{}, err
	}
	var evidence admin.NodeUnmountEvidence
	if err := strictjson.Decode(output, &evidence); err != nil {
		return admin.NodeUnmountEvidence{}, fmt.Errorf("decode node unmount evidence: %w", err)
	}
	if evidence.NodeID != target.NodeID {
		return admin.NodeUnmountEvidence{}, fmt.Errorf("node Pod returned evidence for %q, want %q", evidence.NodeID, target.NodeID)
	}
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.NodeEvidence = append(progress.NodeEvidence, cloneNodeEvidence(evidence))
		slices.SortFunc(progress.NodeEvidence, func(left, right admin.NodeUnmountEvidence) int { return strings.Compare(left.NodeID, right.NodeID) })
		return nil
	})
	if err != nil {
		return admin.NodeUnmountEvidence{}, err
	}
	backend.progress = updated
	return cloneNodeEvidence(evidence), nil
}

func (backend *kubernetesUninstallBackend) DeleteNodePlugin(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return err
	}
	if len(backend.progress.value.NodeEvidence) != len(backend.progress.value.NodeTargets) {
		return fmt.Errorf("cannot delete node DaemonSet before every target has durable unmount evidence")
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
			return errors.Join(fmt.Errorf("delete exact node DaemonSet: %w", err), readErr, identityMismatch(observed, identity))
		}
	}
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.NodeDaemonSetDeleted = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesUninstallBackend) WaitNodePluginStopped(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return err
	}
	if !backend.progress.value.NodeDaemonSetDeleted {
		return fmt.Errorf("node DaemonSet deletion has not completed")
	}
	if backend.progress.value.NodePluginStopped {
		return nil
	}
	if err := backend.waitForPodAbsence(ctx, "node"); err != nil {
		return err
	}
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.NodePluginStopped = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesUninstallBackend) CleanupControllerParents(ctx context.Context, requestID string) (admin.ControllerCleanupEvidence, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	if !backend.progress.value.NodePluginStopped {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("node plugin has not been proved stopped")
	}
	if backend.progress.value.ControllerCleanup != nil {
		return cloneControllerCleanup(*backend.progress.value.ControllerCleanup), nil
	}
	if err := backend.requirePod(ctx, backend.progress.value.ControllerPod); err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	output, err := backend.executePhase(ctx, backend.progress.value.ControllerPod.Name, "cleanup", requestID)
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	var result admin.ControllerUninstallCleanupResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID {
		return admin.ControllerCleanupEvidence{}, fmt.Errorf("decode controller cleanup evidence: %w", err)
	}
	storedEvidence := cloneControllerCleanup(result.Evidence)
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.ControllerCleanup = &storedEvidence
		return nil
	})
	if err != nil {
		return admin.ControllerCleanupEvidence{}, err
	}
	backend.progress = updated
	return cloneControllerCleanup(storedEvidence), nil
}

func (backend *kubernetesUninstallBackend) ReleaseController(ctx context.Context, requestID string) (coordination.LeaseSnapshot, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	if backend.progress.value.ControllerCleanup == nil {
		return coordination.LeaseSnapshot{}, fmt.Errorf("controller cleanup evidence is absent")
	}
	if backend.progress.value.Released != nil {
		return backend.progress.value.Released.LeaseSnapshot(), nil
	}
	if err := backend.requirePod(ctx, backend.progress.value.ControllerPod); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	output, err := backend.executePhase(ctx, backend.progress.value.ControllerPod.Name, "release", requestID)
	if err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	var result admin.ControllerUninstallReleaseResult
	if err := strictjson.Decode(output, &result); err != nil || result.RequestID != requestID {
		return coordination.LeaseSnapshot{}, fmt.Errorf("decode controller release evidence: %w", err)
	}
	lease := result.LeaseSnapshot()
	if err := validateReleasedLease(requestID, lease); err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	storedResult := result
	storedResult.Annotations = cloneStringMap(result.Annotations)
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.Released = &storedResult
		return nil
	})
	if err != nil {
		return coordination.LeaseSnapshot{}, err
	}
	backend.progress = updated
	return lease, nil
}

func (backend *kubernetesUninstallBackend) ScaleControllerToZero(ctx context.Context, requestID string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
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
		return fmt.Errorf("read exact controller Deployment before scale: %w", err)
	}
	if string(deployment.UID) != identity.UID {
		return fmt.Errorf("controller Deployment UID changed before scale")
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
				return errors.Join(fmt.Errorf("scale controller Deployment to zero: %w", err), readErr)
			}
		}
	}
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.ControllerScaled = true
		return nil
	})
	if err != nil {
		return err
	}
	backend.progress = updated
	return nil
}

func (backend *kubernetesUninstallBackend) WaitControllerStopped(ctx context.Context, requestID string) (time.Time, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if err := backend.requireProgress(requestID); err != nil {
		return time.Time{}, err
	}
	if backend.progress.value.ControllerStopped {
		return parseCompletionTime(backend.progress.value.CompletedAt)
	}
	if !backend.progress.value.ControllerScaled {
		return time.Time{}, fmt.Errorf("controller Deployment has not been scaled to zero")
	}
	if err := backend.waitForPodAbsence(ctx, "controller"); err != nil {
		return time.Time{}, err
	}
	completedAt := backend.now().UTC()
	completedText := completedAt.Format(time.RFC3339Nano)
	updated, err := backend.updateProgress(ctx, func(progress *uninstallProgress) error {
		progress.ControllerStopped = true
		progress.CompletedAt = completedText
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	backend.progress = updated
	return completedAt, nil
}

func (backend *kubernetesUninstallBackend) waitForPodAbsence(ctx context.Context, component string) error {
	return wait.ExponentialBackoffWithContext(ctx, wait.Backoff{
		Duration: 250 * time.Millisecond, Factor: 1.6, Jitter: 0.2, Steps: int(^uint(0) >> 1), Cap: 5 * time.Second,
	}, func(ctx context.Context) (bool, error) {
		pods, err := backend.listPodsIncludingDeleting(ctx, component)
		if err != nil {
			return false, err
		}
		return len(pods) == 0, nil
	})
}

func (backend *kubernetesUninstallBackend) requireCurrent(requestID string) error {
	if requestID != backend.requestID || backend.current == nil {
		return fmt.Errorf("safe-uninstall inventory has not been read for this request")
	}
	return nil
}

func (backend *kubernetesUninstallBackend) requireProgress(requestID string) error {
	if requestID != backend.requestID || backend.progress == nil || !backend.progress.value.Quiesced {
		return fmt.Errorf("safe-uninstall request does not own durable quiesced progress")
	}
	return nil
}

func (backend *kubernetesUninstallBackend) requirePod(ctx context.Context, identity objectIdentity) error {
	pod, err := backend.client.CoreV1().Pods(backend.namespace).Get(ctx, identity.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read exact admin Pod %q: %w", identity.Name, err)
	}
	if string(pod.UID) != identity.UID || !podAvailableForAdmin(pod) {
		return fmt.Errorf("admin Pod %q identity or readiness changed", identity.Name)
	}
	return nil
}

func progressNodePod(progress uninstallProgress, target admin.UninstallNodeTarget) (nodePodIdentity, bool) {
	for _, identity := range progress.NodePods {
		if identity.NodeID == target.NodeID && identity.PodName == target.PodName {
			return identity, true
		}
	}
	return nodePodIdentity{}, false
}

func cloneNodeEvidence(value admin.NodeUnmountEvidence) admin.NodeUnmountEvidence {
	value.UnmountedParents = slices.Clone(value.UnmountedParents)
	value.RemainingParentMountPaths = slices.Clone(value.RemainingParentMountPaths)
	value.RemainingChildMountPaths = slices.Clone(value.RemainingChildMountPaths)
	return value
}

func cloneControllerCleanup(value admin.ControllerCleanupEvidence) admin.ControllerCleanupEvidence {
	value.UnmountedParents = slices.Clone(value.UnmountedParents)
	value.DetachedParentFilesystemIDs = slices.Clone(value.DetachedParentFilesystemIDs)
	value.CheckedInstanceIDs = slices.Clone(value.CheckedInstanceIDs)
	value.RegionalAttachmentIDs = slices.Clone(value.RegionalAttachmentIDs)
	value.InstanceAttachmentIDs = slices.Clone(value.InstanceAttachmentIDs)
	value.RemainingControllerMountPaths = slices.Clone(value.RemainingControllerMountPaths)
	return value
}

func cloneStringMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func validateReleasedLease(requestID string, lease coordination.LeaseSnapshot) error {
	if lease.HolderIdentity != "" || lease.ResourceVersion == "" || lease.Annotations == nil {
		return fmt.Errorf("released controller Lease proof is incomplete")
	}
	holder, present, err := coordination.ParseHolderEvidence(lease.Annotations)
	if err != nil || !present {
		return fmt.Errorf("released controller Lease holder evidence: %w", err)
	}
	release, present, err := coordination.ParseGracefulRelease(lease.Annotations)
	if err != nil || !present || release.RequestID != requestID {
		return fmt.Errorf("released controller Lease marker: %w", err)
	}
	return release.ValidateHandoff(lease.UID, holder.InstallationID, holder.ActiveClusterUID, holder)
}

func parseCompletionTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != value {
		return time.Time{}, fmt.Errorf("durable safe-uninstall completion time is invalid")
	}
	return parsed, nil
}

func identityMismatch(object *appsv1.DaemonSet, identity objectIdentity) error {
	if object == nil {
		return nil
	}
	if object.Name != identity.Name || string(object.UID) != identity.UID {
		return fmt.Errorf("node DaemonSet identity changed after ambiguous delete")
	}
	return nil
}

func (backend *kubernetesUninstallBackend) progressFromCurrent(requestID string) uninstallProgress {
	return uninstallProgress{
		SchemaVersion: adminProgressSchemaV1,
		Request:       admin.MutationRequest{RequestID: requestID, AdminVersion: backend.buildVersion, Protocol: admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1}},
		Namespace:     backend.namespace, Release: backend.release,
		ChartVersion: backend.current.loaded.ChartVersion, DriverVersion: backend.current.driverVersion,
		ConfigMap:  objectIdentity{Name: backend.current.configMapName, UID: backend.current.configMapUID},
		Controller: backend.current.controllerDeployment, NodeDaemonSet: backend.current.nodeDaemonSet,
		ControllerPod: backend.current.controllerPod, Parents: slices.Clone(backend.current.parents),
		NodeTargets: slices.Clone(backend.current.targets), NodePods: slices.Clone(backend.current.nodePods),
		NodeParentMountRoot:       backend.current.loaded.Runtime.Node.ParentMountRoot,
		ControllerParentMountRoot: backend.current.loaded.Runtime.Controller.ParentMountRoot,
		Quiesced:                  true,
	}
}

func progressConfigMapName(namespace, release, requestID string) string {
	digest := sha256.Sum256([]byte(namespace + "\n" + release + "\n" + requestID))
	return "sfs-subdir-uninstall-" + hex.EncodeToString(digest[:20])
}

func (backend *kubernetesUninstallBackend) loadProgress(ctx context.Context) (*storedUninstallProgress, error) {
	name := progressConfigMapName(backend.namespace, backend.release, backend.requestID)
	object, err := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read safe-uninstall progress ConfigMap: %w", err)
	}
	return backend.decodeProgress(object)
}

func (backend *kubernetesUninstallBackend) decodeProgress(object *corev1.ConfigMap) (*storedUninstallProgress, error) {
	if object == nil || object.Name != progressConfigMapName(backend.namespace, backend.release, backend.requestID) || len(object.BinaryData) != 0 || len(object.Data) != 1 {
		return nil, fmt.Errorf("safe-uninstall progress ConfigMap shape is invalid")
	}
	if object.Labels["app.kubernetes.io/name"] != adminApplicationName || object.Labels["app.kubernetes.io/instance"] != backend.release || object.Labels["scaleway-sfs-subdir-csi.io/uninstall-request"] != backend.requestID {
		return nil, fmt.Errorf("safe-uninstall progress ConfigMap labels disagree with request")
	}
	encoded, present := object.Data[adminProgressDataKey]
	if !present || len(encoded) == 0 || len(encoded) > config.MaxRuntimeFileBytes {
		return nil, fmt.Errorf("safe-uninstall progress document is absent or over limit")
	}
	var progress uninstallProgress
	if err := strictjson.Decode([]byte(encoded), &progress); err != nil {
		return nil, fmt.Errorf("decode safe-uninstall progress: %w", err)
	}
	if err := backend.validateProgress(progress); err != nil {
		return nil, err
	}
	if len(object.OwnerReferences) != 1 {
		return nil, fmt.Errorf("safe-uninstall progress must have exactly one controller Deployment owner")
	}
	owner := object.OwnerReferences[0]
	if owner.APIVersion != "apps/v1" || owner.Kind != "Deployment" || owner.Name != progress.Controller.Name || string(owner.UID) != progress.Controller.UID {
		return nil, fmt.Errorf("safe-uninstall progress owner differs from frozen controller Deployment")
	}
	return &storedUninstallProgress{value: progress, resourceVersion: object.ResourceVersion}, nil
}

func (backend *kubernetesUninstallBackend) validateProgress(progress uninstallProgress) error {
	if progress.SchemaVersion != adminProgressSchemaV1 || progress.Namespace != backend.namespace || progress.Release != backend.release || progress.Request.RequestID != backend.requestID || progress.Request.AdminVersion != backend.buildVersion {
		return fmt.Errorf("safe-uninstall progress identity disagrees with invocation")
	}
	if err := progress.Request.Validate(); err != nil {
		return err
	}
	if !progress.Quiesced || len(progress.Parents) == 0 || len(progress.NodeTargets) == 0 || len(progress.NodeTargets) != len(progress.NodePods) {
		return fmt.Errorf("safe-uninstall progress lacks frozen quiesced identities")
	}
	for _, identity := range []objectIdentity{progress.ConfigMap, progress.Controller, progress.NodeDaemonSet, progress.ControllerPod} {
		if identity.Name == "" || identity.UID == "" || len(identity.Name) > 253 || len(identity.UID) > 128 || strings.ContainsAny(identity.Name+identity.UID, "\x00\r\n") {
			return fmt.Errorf("safe-uninstall progress contains an invalid Kubernetes identity")
		}
	}
	if progress.NodeDaemonSetDeleted && len(progress.NodeEvidence) != len(progress.NodeTargets) {
		return fmt.Errorf("safe-uninstall progress deleted node DaemonSet without complete node evidence")
	}
	if progress.NodePluginStopped && !progress.NodeDaemonSetDeleted || progress.ControllerCleanup != nil && !progress.NodePluginStopped || progress.Released != nil && progress.ControllerCleanup == nil || progress.ControllerScaled && progress.Released == nil || progress.ControllerStopped && !progress.ControllerScaled {
		return fmt.Errorf("safe-uninstall progress phases are out of order")
	}
	if progress.ControllerStopped {
		if _, err := parseCompletionTime(progress.CompletedAt); err != nil {
			return err
		}
	} else if progress.CompletedAt != "" {
		return fmt.Errorf("safe-uninstall progress has premature completion time")
	}
	return nil
}

func (backend *kubernetesUninstallBackend) validateProgressAgainstDiscovery(progress uninstallProgress, request admin.MutationRequest, discovery *uninstallDiscovery) error {
	if progress.Request != request || progress.ConfigMap != (objectIdentity{Name: discovery.configMapName, UID: discovery.configMapUID}) || progress.Controller != discovery.controllerDeployment || progress.ChartVersion != discovery.loaded.ChartVersion || progress.NodeParentMountRoot != discovery.loaded.Runtime.Node.ParentMountRoot || progress.ControllerParentMountRoot != discovery.loaded.Runtime.Controller.ParentMountRoot || !slices.Equal(progress.Parents, discovery.parents) {
		return fmt.Errorf("safe-uninstall durable progress disagrees with current immutable release identities")
	}
	return nil
}

func (backend *kubernetesUninstallBackend) createProgress(ctx context.Context, progress uninstallProgress) (*storedUninstallProgress, error) {
	encoded, err := canonicaljson.Marshal(progress)
	if err != nil {
		return nil, err
	}
	name := progressConfigMapName(backend.namespace, backend.release, backend.requestID)
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: backend.namespace, Name: name,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "Deployment", Name: progress.Controller.Name, UID: types.UID(progress.Controller.UID),
		}},
		Labels: map[string]string{
			"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": backend.release,
			"app.kubernetes.io/component": "uninstall-progress", "scaleway-sfs-subdir-csi.io/uninstall-request": backend.requestID,
		},
	}, Data: map[string]string{adminProgressDataKey: string(encoded)}}
	created, createErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Create(ctx, object, metav1.CreateOptions{})
	if createErr == nil {
		return backend.decodeProgress(created)
	}
	observed, readErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("create safe-uninstall progress: %w", createErr), readErr)
	}
	stored, decodeErr := backend.decodeProgress(observed)
	if decodeErr != nil {
		return nil, errors.Join(createErr, decodeErr)
	}
	observedBytes, _ := canonicaljson.Marshal(stored.value)
	if !bytes.Equal(observedBytes, encoded) {
		return nil, fmt.Errorf("safe-uninstall progress create collided with different state")
	}
	return stored, nil
}

func (backend *kubernetesUninstallBackend) updateProgress(ctx context.Context, mutate func(*uninstallProgress) error) (*storedUninstallProgress, error) {
	if backend.progress == nil || mutate == nil {
		return nil, fmt.Errorf("safe-uninstall progress update is not initialized")
	}
	next := cloneProgress(backend.progress.value)
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
	name := progressConfigMapName(backend.namespace, backend.release, backend.requestID)
	current, err := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read safe-uninstall progress for CAS: %w", err)
	}
	if current.ResourceVersion != backend.progress.resourceVersion {
		return nil, fmt.Errorf("safe-uninstall progress changed concurrently")
	}
	current.Data = map[string]string{adminProgressDataKey: string(encoded)}
	updated, updateErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Update(ctx, current, metav1.UpdateOptions{})
	if updateErr == nil {
		return backend.decodeProgress(updated)
	}
	observed, readErr := backend.client.CoreV1().ConfigMaps(backend.namespace).Get(ctx, name, metav1.GetOptions{})
	if readErr != nil {
		return nil, errors.Join(fmt.Errorf("update safe-uninstall progress: %w", updateErr), readErr)
	}
	stored, decodeErr := backend.decodeProgress(observed)
	if decodeErr != nil {
		return nil, errors.Join(updateErr, decodeErr)
	}
	observedBytes, _ := canonicaljson.Marshal(stored.value)
	if !bytes.Equal(observedBytes, encoded) {
		return nil, fmt.Errorf("safe-uninstall progress update result is ambiguous: %w", updateErr)
	}
	return stored, nil
}

func cloneProgress(value uninstallProgress) uninstallProgress {
	value.Parents = slices.Clone(value.Parents)
	value.NodeTargets = slices.Clone(value.NodeTargets)
	value.NodePods = slices.Clone(value.NodePods)
	value.NodeEvidence = slices.Clone(value.NodeEvidence)
	for index := range value.NodeEvidence {
		value.NodeEvidence[index] = cloneNodeEvidence(value.NodeEvidence[index])
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

var _ admin.UninstallOperatorBackend = (*kubernetesUninstallBackend)(nil)
