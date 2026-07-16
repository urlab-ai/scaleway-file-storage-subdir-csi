package k8s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	configMapListPageSize = int64(500)
	// maxListedConfigMaps covers 1,000 active allocations, 10,000 permanent
	// tombstones, and a bounded margin for lifecycle transitions while still
	// failing closed if labels or RBAC expose an unexpected object population.
	maxListedConfigMaps = 16 * 1024
)

// ClientGoConfigMaps implements the allocation ConfigMap trust boundary using
// the official typed client-go API. It never uses a dynamic or unstructured
// object and never interprets an ambiguous error as NotFound.
type ClientGoConfigMaps struct {
	core corev1client.CoreV1Interface
}

// NewClientGoConfigMaps constructs the production ConfigMap boundary.
func NewClientGoConfigMaps(core corev1client.CoreV1Interface) (*ClientGoConfigMaps, error) {
	if core == nil {
		return nil, fmt.Errorf("client-go CoreV1 interface is nil")
	}
	return &ClientGoConfigMaps{core: core}, nil
}

// Create performs one Kubernetes create and returns the server-assigned
// resourceVersion. A lost response remains ErrUnavailable, never success.
func (client *ClientGoConfigMaps) Create(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	created, err := client.core.ConfigMaps(object.Namespace).Create(ctx, toCoreConfigMap(object), metav1.CreateOptions{})
	if err != nil {
		return ConfigMap{}, classifyClientGoError(ctx, err)
	}
	return fromCoreConfigMap(created)
}

// Get returns ErrNotFound only for the typed Kubernetes NotFound reason.
func (client *ClientGoConfigMaps) Get(ctx context.Context, namespace, name string) (ConfigMap, error) {
	object, err := client.core.ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return ConfigMap{}, classifyClientGoError(ctx, err)
	}
	return fromCoreConfigMap(object)
}

// Update performs a resourceVersion compare-and-swap through the API server.
func (client *ClientGoConfigMaps) Update(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	updated, err := client.core.ConfigMaps(object.Namespace).Update(ctx, toCoreConfigMap(object), metav1.UpdateOptions{})
	if err != nil {
		return ConfigMap{}, classifyClientGoError(ctx, err)
	}
	return fromCoreConfigMap(updated)
}

// List follows the API server's consistent continue-token snapshot, rejects a
// repeated token, and bounds the aggregate before returning stable name order.
func (client *ClientGoConfigMaps) List(ctx context.Context, namespace string, matchLabels map[string]string) ([]ConfigMap, error) {
	selector := labels.SelectorFromSet(labels.Set(matchLabels)).String()
	result := make([]ConfigMap, 0)
	seenTokens := map[string]struct{}{"": {}}
	continueToken := ""
	for {
		page, err := client.core.ConfigMaps(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: selector, Limit: configMapListPageSize, Continue: continueToken,
		})
		if err != nil {
			return nil, classifyClientGoError(ctx, err)
		}
		if len(result)+len(page.Items) > maxListedConfigMaps {
			return nil, fmt.Errorf("ConfigMap list exceeds bounded maximum of %d objects", maxListedConfigMaps)
		}
		for index := range page.Items {
			object, err := fromCoreConfigMap(&page.Items[index])
			if err != nil {
				return nil, err
			}
			result = append(result, object)
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seenTokens[continueToken]; duplicate {
			return nil, fmt.Errorf("kubernetes ConfigMap list repeated continue token")
		}
		seenTokens[continueToken] = struct{}{}
	}
	slices.SortFunc(result, func(left, right ConfigMap) int { return strings.Compare(left.Name, right.Name) })
	for index := 1; index < len(result); index++ {
		if result[index-1].Namespace == result[index].Namespace && result[index-1].Name == result[index].Name {
			return nil, fmt.Errorf("kubernetes ConfigMap list returned duplicate object %s/%s", result[index].Namespace, result[index].Name)
		}
	}
	return result, nil
}

func toCoreConfigMap(object ConfigMap) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Namespace: object.Namespace, Name: object.Name, ResourceVersion: object.ResourceVersion,
		UID:    types.UID(object.UID),
		Labels: cloneMap(object.Labels),
	}, Data: cloneMap(object.Data)}
}

func fromCoreConfigMap(object *corev1.ConfigMap) (ConfigMap, error) {
	if object == nil {
		return ConfigMap{}, fmt.Errorf("kubernetes returned a nil ConfigMap")
	}
	if len(object.BinaryData) != 0 {
		return ConfigMap{}, fmt.Errorf("ConfigMap %s/%s contains unsupported binaryData", object.Namespace, object.Name)
	}
	return ConfigMap{
		Namespace: object.Namespace, Name: object.Name, UID: string(object.UID), ResourceVersion: object.ResourceVersion,
		Labels: cloneMap(object.Labels), Data: cloneMap(object.Data),
	}, nil
}

func cloneMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func classifyClientGoError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	case apierrors.IsAlreadyExists(err):
		return fmt.Errorf("%w: %v", ErrAlreadyExists, err)
	case apierrors.IsConflict(err):
		return fmt.Errorf("%w: %v", ErrConflict, err)
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return fmt.Errorf("%w: %v", ErrForbidden, err)
	case apierrors.IsTimeout(err), apierrors.IsServerTimeout(err), apierrors.IsTooManyRequests(err), apierrors.IsServiceUnavailable(err):
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	// An unclassified client-go result is not conclusive absence and may have
	// been applied server-side. Preserve that ambiguity as unavailable.
	return fmt.Errorf("%w: %v", ErrUnavailable, err)
}
