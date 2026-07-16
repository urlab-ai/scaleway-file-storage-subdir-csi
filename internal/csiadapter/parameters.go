package csiadapter

import (
	"fmt"
	"strconv"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	parameterPoolName      = "poolName"
	parameterDeletePolicy  = "onDelete"
	parameterDirectoryMode = "directoryMode"
	parameterDirectoryUID  = "directoryUid"
	parameterDirectoryGID  = "directoryGid"
	parameterPVCName       = "csi.storage.k8s.io/pvc/name"
	parameterPVCNamespace  = "csi.storage.k8s.io/pvc/namespace"
	parameterPVName        = "csi.storage.k8s.io/pv/name"
)

var allowedCreateParameters = map[string]struct{}{
	parameterPoolName: {}, parameterDeletePolicy: {}, parameterDirectoryMode: {},
	parameterDirectoryUID: {}, parameterDirectoryGID: {}, parameterPVCName: {},
	parameterPVCNamespace: {}, parameterPVName: {},
}

type parameterResolver struct {
	pools map[string]pool.Config
}

func newParameterResolver(configs []pool.Config) (*parameterResolver, error) {
	if err := pool.ValidateConfigs(configs); err != nil {
		return nil, fmt.Errorf("CSI parameter pools: %w", err)
	}
	resolver := &parameterResolver{pools: make(map[string]pool.Config, len(configs))}
	for _, config := range configs {
		resolver.pools[config.Name] = config
	}
	return resolver, nil
}

func (resolver *parameterResolver) resolve(values map[string]string, capabilities []volume.Capability) (volume.CreateParameters, string, string, error) {
	for key := range values {
		if _, allowed := allowedCreateParameters[key]; !allowed {
			return volume.CreateParameters{}, "", "", fmt.Errorf("StorageClass parameter %q is unsupported", key)
		}
	}
	poolName := values[parameterPoolName]
	if poolName == "" {
		return volume.CreateParameters{}, "", "", fmt.Errorf("StorageClass parameter %q is required", parameterPoolName)
	}
	configured, present := resolver.pools[poolName]
	if !present {
		return volume.CreateParameters{}, "", "", fmt.Errorf("StorageClass pool %q is not configured", poolName)
	}
	deletePolicy := configured.DeletePolicy
	if value, present := values[parameterDeletePolicy]; present {
		deletePolicy = volume.DeletePolicy(value)
	}
	directoryMode := configured.DirectoryMode
	if value, present := values[parameterDirectoryMode]; present {
		directoryMode = value
	}
	directoryUID := configured.DirectoryUID
	if value, present := values[parameterDirectoryUID]; present {
		parsed, err := parseIdentityParameter(parameterDirectoryUID, value)
		if err != nil {
			return volume.CreateParameters{}, "", "", err
		}
		directoryUID = parsed
	}
	directoryGID := configured.DirectoryGID
	if value, present := values[parameterDirectoryGID]; present {
		parsed, err := parseIdentityParameter(parameterDirectoryGID, value)
		if err != nil {
			return volume.CreateParameters{}, "", "", err
		}
		directoryGID = parsed
	}
	accessModes := make([]volume.AccessMode, 0, len(capabilities))
	for _, capability := range capabilities {
		accessModes = append(accessModes, capability.AccessMode)
	}
	parameters, err := (volume.CreateParameters{
		PoolName: poolName, DeletePolicy: deletePolicy,
		DirectoryUID: directoryUID, DirectoryGID: directoryGID, DirectoryMode: directoryMode,
		AccessType: "mount", FilesystemType: "virtiofs", AccessModes: accessModes,
	}).Normalize()
	if err != nil {
		return volume.CreateParameters{}, "", "", err
	}
	return parameters, values[parameterPVCNamespace], values[parameterPVCName], nil
}

func parseIdentityParameter(name, value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 31)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, fmt.Errorf("StorageClass parameter %q must be a canonical base-10 integer in [0,2147483647]", name)
	}
	return uint32(parsed), nil
}
