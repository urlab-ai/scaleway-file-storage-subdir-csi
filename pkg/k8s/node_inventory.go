package k8s

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	storagev1client "k8s.io/client-go/kubernetes/typed/storage/v1"

	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	nodeInventoryPageSize     = int64(500)
	maxNodeInventoryObjects   = 4096
	nodeGenerationAnnotation  = "scaleway-sfs-subdir-csi.io/node-config-generation"
	standardZoneLabel         = "topology.kubernetes.io/zone"
	outOfServiceTaint         = "node.kubernetes.io/out-of-service"
	nodePluginComponentLabel  = "node"
	applicationNameLabel      = "app.kubernetes.io/name"
	applicationInstanceLabel  = "app.kubernetes.io/instance"
	applicationComponentLabel = "app.kubernetes.io/component"
)

// NodeInventoryObservation is the complete Kubernetes half of one rollout
// decision. Provider commercial type and attachment capability are deliberately
// filled only after an authenticated Scaleway read by the controller runtime.
type NodeInventoryObservation struct {
	NodeName             string
	CSINodeID            string
	OperatingSystem      string
	Schedulable          bool
	Deleting             bool
	Ready                bool
	PluginPodPresent     bool
	PluginPodReady       bool
	DriverRegistered     bool
	NodeConfigGeneration string
	FailureDomain        string
}

// ClientGoNodeInventory joins complete Node, CSINode, and node-plugin Pod
// snapshots by exact Kubernetes node name. It performs no provider calls and
// never treats an incomplete list or duplicate identity as a partial success.
type ClientGoNodeInventory struct {
	core        corev1client.CoreV1Interface
	storage     storagev1client.StorageV1Interface
	namespace   string
	driverName  string
	application string
	release     string
}

// NewClientGoNodeInventory validates the immutable labels used to find exactly
// this Helm release's node-plugin Pods.
func NewClientGoNodeInventory(core corev1client.CoreV1Interface, storage storagev1client.StorageV1Interface, namespace, driverName, application, release string) (*ClientGoNodeInventory, error) {
	if core == nil || storage == nil {
		return nil, fmt.Errorf("node inventory Kubernetes client is nil")
	}
	if namespace == "" || application == "" || release == "" || strings.ContainsAny(namespace+application+release, "\x00\r\n") {
		return nil, fmt.Errorf("node inventory namespace or Helm labels are invalid")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	return &ClientGoNodeInventory{
		core: core, storage: storage, namespace: namespace, driverName: driverName,
		application: application, release: release,
	}, nil
}

// Snapshot reads and joins one complete rollout inventory.
func (inventory *ClientGoNodeInventory) Snapshot(ctx context.Context) ([]NodeInventoryObservation, error) {
	nodes, err := inventory.listNodes(ctx)
	if err != nil {
		return nil, err
	}
	csiNodes, err := inventory.listCSINodes(ctx)
	if err != nil {
		return nil, err
	}
	pods, err := inventory.listNodePods(ctx)
	if err != nil {
		return nil, err
	}
	csiByName := make(map[string]driverRegistration, len(csiNodes))
	for index := range csiNodes {
		registration, err := inventory.registration(csiNodes[index])
		if err != nil {
			return nil, err
		}
		csiByName[csiNodes[index].Name] = registration
	}
	podByNode := make(map[string]podRegistration, len(pods))
	for index := range pods {
		pod := &pods[index]
		if pod.Spec.NodeName == "" {
			return nil, fmt.Errorf("node-plugin Pod %s/%s has no spec.nodeName", pod.Namespace, pod.Name)
		}
		if _, duplicate := podByNode[pod.Spec.NodeName]; duplicate {
			return nil, fmt.Errorf("multiple active node-plugin Pods target node %q", pod.Spec.NodeName)
		}
		podByNode[pod.Spec.NodeName] = podRegistration{
			ready: podReady(pod), generation: pod.Annotations[nodeGenerationAnnotation],
		}
	}
	result := make([]NodeInventoryObservation, 0, len(nodes))
	knownNodes := make(map[string]struct{}, len(nodes))
	for index := range nodes {
		node := &nodes[index]
		knownNodes[node.Name] = struct{}{}
		registration := csiByName[node.Name]
		plugin, pluginPresent := podByNode[node.Name]
		result = append(result, NodeInventoryObservation{
			NodeName: node.Name, CSINodeID: registration.nodeID,
			OperatingSystem: node.Status.NodeInfo.OperatingSystem,
			Schedulable:     !node.Spec.Unschedulable, Deleting: node.DeletionTimestamp != nil,
			Ready: nodeReady(node), PluginPodPresent: pluginPresent,
			PluginPodReady: pluginPresent && plugin.ready, DriverRegistered: registration.present,
			NodeConfigGeneration: plugin.generation, FailureDomain: node.Labels[standardZoneLabel],
		})
	}
	for nodeName := range podByNode {
		if _, exists := knownNodes[nodeName]; !exists {
			return nil, fmt.Errorf("node-plugin Pod targets unknown node %q", nodeName)
		}
	}
	slices.SortFunc(result, func(left, right NodeInventoryObservation) int { return strings.Compare(left.NodeName, right.NodeName) })
	return result, nil
}

type driverRegistration struct {
	present bool
	nodeID  string
}

type podRegistration struct {
	ready      bool
	generation string
}

func (inventory *ClientGoNodeInventory) registration(node storagev1.CSINode) (driverRegistration, error) {
	var result driverRegistration
	for _, registered := range node.Spec.Drivers {
		if registered.Name != inventory.driverName {
			continue
		}
		if result.present {
			return driverRegistration{}, fmt.Errorf("CSINode %q repeats driver %q", node.Name, inventory.driverName)
		}
		result = driverRegistration{present: true, nodeID: registered.NodeID}
	}
	return result, nil
}

func (inventory *ClientGoNodeInventory) listNodes(ctx context.Context) ([]corev1.Node, error) {
	result := make([]corev1.Node, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := inventory.core.Nodes().List(ctx, metav1.ListOptions{Limit: nodeInventoryPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list Kubernetes Nodes: %w", classifyClientGoError(ctx, err))
		}
		if len(result)+len(page.Items) > maxNodeInventoryObjects {
			return nil, fmt.Errorf("node inventory exceeds %d objects", maxNodeInventoryObjects)
		}
		result = append(result, page.Items...)
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("node inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
}

func (inventory *ClientGoNodeInventory) listCSINodes(ctx context.Context) ([]storagev1.CSINode, error) {
	result := make([]storagev1.CSINode, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := inventory.storage.CSINodes().List(ctx, metav1.ListOptions{Limit: nodeInventoryPageSize, Continue: continueToken})
		if err != nil {
			return nil, fmt.Errorf("list Kubernetes CSINodes: %w", classifyClientGoError(ctx, err))
		}
		if len(result)+len(page.Items) > maxNodeInventoryObjects {
			return nil, fmt.Errorf("CSINode inventory exceeds %d objects", maxNodeInventoryObjects)
		}
		result = append(result, page.Items...)
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("CSINode inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
}

func (inventory *ClientGoNodeInventory) listNodePods(ctx context.Context) ([]corev1.Pod, error) {
	selector := labels.SelectorFromSet(labels.Set{
		applicationNameLabel: inventory.application, applicationInstanceLabel: inventory.release,
		applicationComponentLabel: nodePluginComponentLabel,
	}).String()
	result := make([]corev1.Pod, 0)
	continueToken := ""
	seen := map[string]struct{}{"": {}}
	for {
		page, err := inventory.core.Pods(inventory.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector, Limit: nodeInventoryPageSize, Continue: continueToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list node-plugin Pods: %w", classifyClientGoError(ctx, err))
		}
		if len(result)+len(page.Items) > maxNodeInventoryObjects {
			return nil, fmt.Errorf("node-plugin Pod inventory exceeds %d objects", maxNodeInventoryObjects)
		}
		for index := range page.Items {
			if page.Items[index].DeletionTimestamp == nil {
				result = append(result, page.Items[index])
			}
		}
		continueToken = page.Continue
		if continueToken == "" {
			return result, nil
		}
		if _, duplicate := seen[continueToken]; duplicate {
			return nil, fmt.Errorf("node-plugin Pod inventory repeated continue token")
		}
		seen[continueToken] = struct{}{}
	}
}

func nodeReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// ClientGoNormalNodeEvidence implements the normal ControllerUnpublish path
// from fresh Node and CSINode reads. Conclusive absence returns false without
// error so the independent provider-fence path can decide safely.
type ClientGoNormalNodeEvidence struct {
	core       corev1client.CoreV1Interface
	storage    storagev1client.StorageV1Interface
	driverName string
}

// NewClientGoNormalNodeEvidence constructs the fresh unpublish reader.
func NewClientGoNormalNodeEvidence(core corev1client.CoreV1Interface, storage storagev1client.StorageV1Interface, driverName string) (*ClientGoNormalNodeEvidence, error) {
	if core == nil || storage == nil {
		return nil, fmt.Errorf("normal node evidence Kubernetes client is nil")
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	return &ClientGoNormalNodeEvidence{core: core, storage: storage, driverName: driverName}, nil
}

// NormalUnpublishAllowed returns true only for one exact Ready Node/CSINode
// identity without the out-of-service taint.
func (evidence *ClientGoNormalNodeEvidence) NormalUnpublishAllowed(ctx context.Context, nodeID string) (bool, error) {
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return false, err
	}
	list, err := evidence.storage.CSINodes().List(ctx, metav1.ListOptions{Limit: maxNodeInventoryObjects})
	if err != nil {
		return false, fmt.Errorf("list CSINodes for normal unpublish: %w", classifyClientGoError(ctx, err))
	}
	if list.Continue != "" || len(list.Items) > maxNodeInventoryObjects {
		return false, fmt.Errorf("normal unpublish CSINode inventory is incomplete or over limit")
	}
	nodeName := ""
	for _, csiNode := range list.Items {
		for _, registered := range csiNode.Spec.Drivers {
			if registered.Name == evidence.driverName && registered.NodeID == nodeID {
				if nodeName != "" {
					return false, fmt.Errorf("CSI node ID %q is registered by multiple Nodes", nodeID)
				}
				nodeName = csiNode.Name
			}
		}
	}
	if nodeName == "" {
		return false, nil
	}
	node, err := evidence.core.Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		classified := classifyClientGoError(ctx, err)
		if errors.Is(classified, ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("read Node %q for normal unpublish: %w", nodeName, classified)
	}
	if !nodeReady(node) {
		return false, nil
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == outOfServiceTaint {
			return false, nil
		}
	}
	return true, nil
}
