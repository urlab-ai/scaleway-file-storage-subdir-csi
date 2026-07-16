package k8s

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const activeClusterNamespace = "kube-system"

// NewInClusterClientset constructs the official typed Kubernetes client from
// the projected ServiceAccount transport. It does not fall back to a local
// kubeconfig, which prevents a production Pod from silently selecting another
// cluster authority.
func NewInClusterClientset(userAgent string) (kubernetes.Interface, error) {
	if userAgent == "" {
		return nil, fmt.Errorf("kubernetes client user agent is empty")
	}
	configuration, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	configuration = rest.CopyConfig(configuration)
	rest.AddUserAgent(configuration, userAgent)
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return nil, fmt.Errorf("construct in-cluster Kubernetes client: %w", err)
	}
	return client, nil
}

// ReadActiveClusterUID reads the immutable kube-system Namespace UID through
// the narrow get-only controller RBAC rule. Missing, forbidden, malformed, or
// ambiguous results are never replaced with a generated identity.
func ReadActiveClusterUID(ctx context.Context, core corev1client.CoreV1Interface) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("active cluster identity context is nil")
	}
	if core == nil {
		return "", fmt.Errorf("active cluster identity CoreV1 client is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	namespace, err := core.Namespaces().Get(ctx, activeClusterNamespace, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read kube-system Namespace identity: %w", classifyClientGoError(ctx, err))
	}
	uid := string(namespace.UID)
	if err := volume.ValidateClusterUID(uid); err != nil {
		return "", fmt.Errorf("kube-system Namespace UID: %w", err)
	}
	return uid, nil
}
