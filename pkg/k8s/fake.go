package k8s

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
)

// FakeOperation identifies one deterministic ConfigMap API call.
type FakeOperation string

const (
	FakeCreate FakeOperation = "create"
	FakeGet    FakeOperation = "get"
	FakeUpdate FakeOperation = "update"
	FakeList   FakeOperation = "list"
)

// FakeFault injects an error at the next matching operation. ApplyBeforeError
// models an ambiguous response where the API server committed the write before
// the client lost the result.
type FakeFault struct {
	Operation        FakeOperation
	Err              error
	ApplyBeforeError bool
}

// FakeConfigMapClient implements API create/CAS/list semantics in memory.
type FakeConfigMapClient struct {
	mu       sync.Mutex
	objects  map[string]ConfigMap
	revision uint64
	faults   []FakeFault
}

// NewFakeConfigMapClient returns an empty deterministic API fake.
func NewFakeConfigMapClient() *FakeConfigMapClient {
	return &FakeConfigMapClient{objects: make(map[string]ConfigMap)}
}

// InjectFault appends one fault consumed by the next matching operation.
func (client *FakeConfigMapClient) InjectFault(fault FakeFault) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.faults = append(client.faults, fault)
}

// Create implements atomic create-if-absent.
func (client *FakeConfigMapClient) Create(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	if err := ctx.Err(); err != nil {
		return ConfigMap{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	fault, hasFault := client.takeFault(FakeCreate)
	key := objectKey(object.Namespace, object.Name)
	if _, exists := client.objects[key]; exists {
		return ConfigMap{}, ErrAlreadyExists
	}
	if hasFault && !fault.ApplyBeforeError {
		return ConfigMap{}, fault.Err
	}
	client.revision++
	if object.UID == "" {
		object.UID = "fake-configmap-" + strconv.FormatUint(client.revision, 10)
	}
	object.ResourceVersion = strconv.FormatUint(client.revision, 10)
	client.objects[key] = cloneConfigMap(object)
	if hasFault {
		return ConfigMap{}, fault.Err
	}
	return cloneConfigMap(object), nil
}

// Get returns a conclusive fake NotFound only when the object is absent.
func (client *FakeConfigMapClient) Get(ctx context.Context, namespace, name string) (ConfigMap, error) {
	if err := ctx.Err(); err != nil {
		return ConfigMap{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if fault, ok := client.takeFault(FakeGet); ok {
		return ConfigMap{}, fault.Err
	}
	object, exists := client.objects[objectKey(namespace, name)]
	if !exists {
		return ConfigMap{}, ErrNotFound
	}
	return cloneConfigMap(object), nil
}

// Update implements exact resourceVersion compare-and-swap.
func (client *FakeConfigMapClient) Update(ctx context.Context, object ConfigMap) (ConfigMap, error) {
	if err := ctx.Err(); err != nil {
		return ConfigMap{}, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	fault, hasFault := client.takeFault(FakeUpdate)
	key := objectKey(object.Namespace, object.Name)
	current, exists := client.objects[key]
	if !exists {
		return ConfigMap{}, ErrNotFound
	}
	if object.ResourceVersion == "" || object.ResourceVersion != current.ResourceVersion {
		return ConfigMap{}, ErrConflict
	}
	if hasFault && !fault.ApplyBeforeError {
		return ConfigMap{}, fault.Err
	}
	client.revision++
	if object.UID == "" {
		object.UID = current.UID
	}
	object.ResourceVersion = strconv.FormatUint(client.revision, 10)
	client.objects[key] = cloneConfigMap(object)
	if hasFault {
		return ConfigMap{}, fault.Err
	}
	return cloneConfigMap(object), nil
}

// List returns objects matching every requested label.
func (client *FakeConfigMapClient) List(ctx context.Context, namespace string, labels map[string]string) ([]ConfigMap, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if fault, ok := client.takeFault(FakeList); ok {
		return nil, fault.Err
	}
	result := make([]ConfigMap, 0)
	for _, object := range client.objects {
		if object.Namespace != namespace || !labelsMatch(object.Labels, labels) {
			continue
		}
		result = append(result, cloneConfigMap(object))
	}
	slices.SortFunc(result, func(left, right ConfigMap) int {
		if left.Namespace != right.Namespace {
			if left.Namespace < right.Namespace {
				return -1
			}
			return 1
		}
		if left.Name < right.Name {
			return -1
		}
		if left.Name > right.Name {
			return 1
		}
		return 0
	})
	return result, nil
}

// Snapshot returns an isolated copy for assertions, never for mutation logic.
func (client *FakeConfigMapClient) Snapshot() []ConfigMap {
	client.mu.Lock()
	defer client.mu.Unlock()
	result := make([]ConfigMap, 0, len(client.objects))
	for _, object := range client.objects {
		result = append(result, cloneConfigMap(object))
	}
	return result
}

// Seed inserts a raw object for read-path corruption and recovery tests. It is
// intentionally unavailable through AllocationStore production workflows.
func (client *FakeConfigMapClient) Seed(object ConfigMap) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.revision++
	if object.UID == "" {
		object.UID = "fake-configmap-" + strconv.FormatUint(client.revision, 10)
	}
	if object.ResourceVersion == "" {
		object.ResourceVersion = strconv.FormatUint(client.revision, 10)
	}
	client.objects[objectKey(object.Namespace, object.Name)] = cloneConfigMap(object)
}

// RemoveForTest deletes one exact object without adding a production delete
// capability to ConfigMapClient. Durable-state tests use it to model external
// corruption that the driver must detect rather than repair.
func (client *FakeConfigMapClient) RemoveForTest(namespace, name string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	delete(client.objects, objectKey(namespace, name))
}

func (client *FakeConfigMapClient) takeFault(operation FakeOperation) (FakeFault, bool) {
	for index, fault := range client.faults {
		if fault.Operation == operation {
			client.faults = append(client.faults[:index], client.faults[index+1:]...)
			if fault.Err == nil {
				fault.Err = fmt.Errorf("injected %s fault", operation)
			}
			return fault, true
		}
	}
	return FakeFault{}, false
}

func objectKey(namespace, name string) string { return namespace + "\x00" + name }

func cloneConfigMap(object ConfigMap) ConfigMap {
	object.Labels = maps.Clone(object.Labels)
	object.Data = maps.Clone(object.Data)
	return object
}

func labelsMatch(object, selector map[string]string) bool {
	for key, value := range selector {
		if object[key] != value {
			return false
		}
	}
	return true
}
