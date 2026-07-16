package k8s

import (
	"context"
	"errors"
)

var (
	// ErrNotFound is returned only for a conclusive Kubernetes NotFound.
	ErrNotFound = errors.New("kubernetes object not found")
	// ErrAlreadyExists is returned for a conclusive create collision.
	ErrAlreadyExists = errors.New("kubernetes object already exists")
	// ErrConflict is returned for a resourceVersion compare-and-swap conflict.
	ErrConflict = errors.New("kubernetes resource version conflict")
	// ErrForbidden marks an authorization rejection and never means absence.
	ErrForbidden = errors.New("kubernetes operation forbidden")
	// ErrUnavailable marks a timeout, transport failure, or ambiguous result.
	ErrUnavailable = errors.New("kubernetes API unavailable or result ambiguous")
)

// ConfigMap is the minimal API projection needed by allocation state.
type ConfigMap struct {
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
	Labels          map[string]string
	Data            map[string]string
}

// ConfigMapClient is the narrow production/fake Kubernetes boundary.
type ConfigMapClient interface {
	Create(ctx context.Context, object ConfigMap) (ConfigMap, error)
	Get(ctx context.Context, namespace, name string) (ConfigMap, error)
	Update(ctx context.Context, object ConfigMap) (ConfigMap, error)
	List(ctx context.Context, namespace string, labels map[string]string) ([]ConfigMap, error)
}
