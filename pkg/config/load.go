package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/pool"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	// RuntimeFileSchemaV1 is the closed non-secret process configuration schema.
	RuntimeFileSchemaV1 = "1"
	// MaxRuntimeFileBytes bounds ConfigMap projection reads before JSON decode.
	MaxRuntimeFileBytes = 1 << 20
)

// Component selects controller-only environment checks from node least
// privilege checks.
type Component string

const (
	// ComponentController loads authenticated provider controller settings.
	ComponentController Component = "controller"
	// ComponentNode loads local metadata and mount-only node settings.
	ComponentNode Component = "node"
)

// LookupEnv reads one process environment key. Supplying it explicitly keeps
// tests deterministic and prevents the loader from retaining a full environment.
type LookupEnv func(key string) (value string, present bool)

// Loaded is the validated process configuration plus non-secret release and
// ownership metadata used by checkpoint, upgrade, and parent claims.
type Loaded struct {
	Runtime              Runtime
	LogLevel             string
	ControllerNamespace  string
	HelmReleaseName      string
	ChartVersion         string
	RenderedImages       []RenderedImage
	NodeConfigGeneration string
}

// RenderedImage is one stable chart workload image identity. The digest is
// mandatory in production and deliberately empty only for development chart
// renders, which are not eligible to produce or restore a checkpoint.
type RenderedImage struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

type runtimeFile struct {
	SchemaVersion        string                     `json:"schemaVersion"`
	Mode                 Mode                       `json:"mode"`
	DriverName           string                     `json:"driverName"`
	LogLevel             string                     `json:"logLevel"`
	ControllerNamespace  string                     `json:"controllerNamespace"`
	HelmReleaseName      string                     `json:"helmReleaseName"`
	ChartVersion         string                     `json:"chartVersion"`
	RenderedImages       []RenderedImage            `json:"renderedImages"`
	NodeConfigGeneration string                     `json:"nodeConfigGeneration"`
	Installation         runtimeInstallationFile    `json:"installation"`
	Provider             runtimeProviderFile        `json:"scaleway"`
	Controller           runtimeControllerFile      `json:"controller"`
	Node                 runtimeNodeFile            `json:"node"`
	Scheduling           Scheduling                 `json:"scheduling"`
	Compatibility        runtimeCompatibilityFile   `json:"compatibility"`
	Pools                map[string]runtimePoolFile `json:"pools"`
	StorageClasses       []runtimeStorageClassFile  `json:"storageClasses"`
}

type runtimeInstallationFile struct {
	ExistingSecretName         string `json:"existingSecretName"`
	IDKey                      string `json:"idKey"`
	GenerateForDevelopmentOnly bool   `json:"generateForDevelopmentOnly"`
}

type runtimeProviderFile struct {
	Region      string                 `json:"region"`
	DefaultZone string                 `json:"defaultZone"`
	ProjectID   string                 `json:"projectId"`
	Credentials runtimeCredentialsFile `json:"credentials"`
}

type runtimeCredentialsFile struct {
	ExistingSecretName string `json:"existingSecretName"`
	AccessKeyKey       string `json:"accessKeyKey"`
	SecretKeyKey       string `json:"secretKeyKey"`
}

type runtimeControllerFile struct {
	Replicas                          uint32                `json:"replicas"`
	UpdateStrategy                    string                `json:"updateStrategy"`
	MaxConcurrentMutations            uint32                `json:"maxConcurrentMutations"`
	ShutdownDeadlineSeconds           uint64                `json:"shutdownDeadlineSeconds"`
	TerminationGracePeriodSeconds     uint64                `json:"terminationGracePeriodSeconds"`
	ProgressDeadlineSeconds           uint64                `json:"progressDeadlineSeconds"`
	StartupProbeBudgetSeconds         uint64                `json:"startupProbeBudgetSeconds"`
	AttachReadyDeadlineSeconds        uint64                `json:"attachReadyDeadlineSeconds"`
	MetadataRefreshIntervalSeconds    uint64                `json:"metadataRefreshIntervalSeconds"`
	DetailedTombstoneRetentionSeconds uint64                `json:"detailedTombstoneRetentionSeconds"`
	ParentMountRoot                   string                `json:"parentMountRoot"`
	Leadership                        runtimeLeadershipFile `json:"leadership"`
}

type runtimeLeadershipFile struct {
	Enabled              bool   `json:"enabled"`
	LeaseName            string `json:"leaseName"`
	LeaseDurationSeconds uint64 `json:"leaseDurationSeconds"`
	RenewDeadlineSeconds uint64 `json:"renewDeadlineSeconds"`
	RetryPeriodSeconds   uint64 `json:"retryPeriodSeconds"`
}

type runtimeNodeFile struct {
	ParentMountRoot string `json:"parentMountRoot"`
	KubeletPath     string `json:"kubeletPath"`
}

type runtimeCompatibilityFile struct {
	QualifiedCommercialTypes []string `json:"qualifiedCommercialTypes"`
}

type runtimePoolFile struct {
	BasePath                  string               `json:"basePath"`
	SelectionPolicy           pool.SelectionPolicy `json:"selectionPolicy"`
	MaxParentsPerEligibleNode uint32               `json:"maxParentsPerEligibleNode"`
	MaxLogicalOvercommitRatio string               `json:"maxLogicalOvercommitRatio"`
	MinFreeBytes              uint64               `json:"minFreeBytes"`
	MinFreePercent            uint32               `json:"minFreePercent"`
	DeletePolicy              volume.DeletePolicy  `json:"onDelete"`
	DirectoryMode             string               `json:"directoryMode"`
	DirectoryUID              string               `json:"directoryUid"`
	DirectoryGID              string               `json:"directoryGid"`
	Filesystems               []runtimeParentFile  `json:"filesystems"`
}

type runtimeParentFile struct {
	ID    string           `json:"id"`
	Name  string           `json:"name"`
	State pool.ParentState `json:"state"`
}

type runtimeStorageClassFile struct {
	Name                 string `json:"name"`
	PoolName             string `json:"poolName"`
	DefaultClass         bool   `json:"defaultClass"`
	ReclaimPolicy        string `json:"reclaimPolicy"`
	AllowVolumeExpansion bool   `json:"allowVolumeExpansion"`
	VolumeBindingMode    string `json:"volumeBindingMode"`
}

// LoadRuntimeFile reads one bounded regular ConfigMap projection and resolves
// its component-specific environment authority.
func LoadRuntimeFile(ctx context.Context, filename string, component Component, lookup LookupEnv) (loaded Loaded, returnErr error) {
	if ctx == nil {
		return Loaded{}, fmt.Errorf("runtime configuration context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Loaded{}, err
	}
	if filename == "" || !filepath.IsAbs(filename) || filepath.Clean(filename) != filename {
		return Loaded{}, fmt.Errorf("runtime configuration path %q must be absolute and normalized", filename)
	}
	file, err := os.Open(filename)
	if err != nil {
		return Loaded{}, fmt.Errorf("open runtime configuration: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return Loaded{}, fmt.Errorf("stat runtime configuration: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Loaded{}, fmt.Errorf("runtime configuration is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxRuntimeFileBytes+1))
	if err != nil {
		return Loaded{}, fmt.Errorf("read runtime configuration: %w", err)
	}
	if len(data) > MaxRuntimeFileBytes {
		return Loaded{}, fmt.Errorf("runtime configuration exceeds %d bytes", MaxRuntimeFileBytes)
	}
	if err := ctx.Err(); err != nil {
		return Loaded{}, err
	}
	return DecodeRuntimeFile(data, component, lookup)
}

// DecodeRuntimeFile strictly decodes and validates one complete non-secret
// runtime document. Secret values are checked through lookup and never stored.
func DecodeRuntimeFile(data []byte, component Component, lookup LookupEnv) (Loaded, error) {
	if len(data) == 0 || len(data) > MaxRuntimeFileBytes {
		return Loaded{}, fmt.Errorf("runtime configuration must contain 1 to %d bytes", MaxRuntimeFileBytes)
	}
	if component != ComponentController && component != ComponentNode {
		return Loaded{}, fmt.Errorf("runtime component %q is unsupported", component)
	}
	if lookup == nil {
		return Loaded{}, fmt.Errorf("runtime environment lookup is nil")
	}
	var file runtimeFile
	if err := strictjson.Decode(data, &file); err != nil {
		return Loaded{}, fmt.Errorf("decode runtime configuration: %w", err)
	}
	if err := validateRuntimeRequiredFields(data); err != nil {
		return Loaded{}, err
	}
	if file.SchemaVersion != RuntimeFileSchemaV1 {
		return Loaded{}, fmt.Errorf("runtime configuration schema %q is unsupported", file.SchemaVersion)
	}
	runtime, err := file.runtime(component, lookup)
	if err != nil {
		return Loaded{}, err
	}
	if err := runtime.Validate(); err != nil {
		return Loaded{}, fmt.Errorf("validate runtime configuration: %w", err)
	}
	if file.LogLevel != "debug" && file.LogLevel != "info" && file.LogLevel != "warn" && file.LogLevel != "error" {
		return Loaded{}, fmt.Errorf("runtime log level %q is unsupported", file.LogLevel)
	}
	if !validKubernetesName(file.ControllerNamespace) {
		return Loaded{}, fmt.Errorf("controller namespace %q is invalid", file.ControllerNamespace)
	}
	if !validKubernetesName(file.HelmReleaseName) {
		return Loaded{}, fmt.Errorf("helm release name %q is invalid", file.HelmReleaseName)
	}
	if err := validateReleaseProjection(file.Mode, file.ChartVersion, file.RenderedImages); err != nil {
		return Loaded{}, err
	}
	wantGeneration, err := NodeConfigGeneration(
		runtime.DriverName, file.RenderedImages[0].Digest, runtime.Provider.Region, runtime.Node.ParentMountRoot,
		runtime.Node.KubeletPath, runtime.Compatibility.QualifiedCommercialTypes, runtime.Pools,
	)
	if err != nil {
		return Loaded{}, fmt.Errorf("compute node configuration generation: %w", err)
	}
	if file.NodeConfigGeneration != wantGeneration {
		return Loaded{}, fmt.Errorf("node configuration generation mismatch")
	}
	return Loaded{
		Runtime: runtime, LogLevel: file.LogLevel,
		ControllerNamespace: file.ControllerNamespace, HelmReleaseName: file.HelmReleaseName,
		ChartVersion: file.ChartVersion, RenderedImages: slices.Clone(file.RenderedImages),
		NodeConfigGeneration: file.NodeConfigGeneration,
	}, nil
}

func validateReleaseProjection(mode Mode, chartVersion string, images []RenderedImage) error {
	if !utf8.ValidString(chartVersion) || len(chartVersion) == 0 || len(chartVersion) > 128 || strings.ContainsAny(chartVersion, "\x00\r\n") {
		return fmt.Errorf("chart version must contain 1 to 128 safe UTF-8 bytes")
	}
	wantNames := []string{"driver", "external-attacher", "external-provisioner", "liveness-probe", "node-driver-registrar"}
	if len(images) != len(wantNames) {
		return fmt.Errorf("rendered image projection must contain exactly %d chart images", len(wantNames))
	}
	for index, image := range images {
		if image.Name != wantNames[index] {
			return fmt.Errorf("rendered image %d name %q differs from fixed chart image %q", index, image.Name, wantNames[index])
		}
		if image.Digest == "" && mode == ModeDevelopment {
			continue
		}
		if len(image.Digest) != len("sha256:")+64 || !strings.HasPrefix(image.Digest, "sha256:") {
			return fmt.Errorf("rendered image %q digest must be an immutable sha256 digest", image.Name)
		}
		for _, digit := range strings.TrimPrefix(image.Digest, "sha256:") {
			if !strings.ContainsRune("0123456789abcdef", digit) {
				return fmt.Errorf("rendered image %q digest must be lowercase hexadecimal", image.Name)
			}
		}
	}
	return nil
}

func (file runtimeFile) runtime(component Component, lookup LookupEnv) (Runtime, error) {
	installationID, present := lookup("INSTALLATION_ID")
	if !present || installationID == "" {
		return Runtime{}, fmt.Errorf("INSTALLATION_ID is required from the installation identity Secret")
	}
	pools, err := file.runtimePools()
	if err != nil {
		return Runtime{}, err
	}
	controller, err := file.Controller.runtime()
	if err != nil {
		return Runtime{}, err
	}
	storageClasses := make([]StorageClass, 0, len(file.StorageClasses))
	for _, storageClass := range file.StorageClasses {
		storageClasses = append(storageClasses, StorageClass(storageClass))
	}
	runtime := Runtime{
		Mode: file.Mode, DriverName: file.DriverName,
		Installation: Installation{
			ExistingSecretName: file.Installation.ExistingSecretName, IDKey: file.Installation.IDKey,
			ID: installationID, GenerateForDevelopmentOnly: file.Installation.GenerateForDevelopmentOnly,
		},
		Provider: Provider{
			Region: file.Provider.Region, DefaultZone: file.Provider.DefaultZone, ProjectID: file.Provider.ProjectID,
			CredentialsExistingSecretName: file.Provider.Credentials.ExistingSecretName,
			AccessKeyKey:                  file.Provider.Credentials.AccessKeyKey, SecretKeyKey: file.Provider.Credentials.SecretKeyKey,
		},
		Controller: controller,
		Node:       Node{ParentMountRoot: file.Node.ParentMountRoot, KubeletPath: file.Node.KubeletPath},
		Scheduling: file.Scheduling,
		Compatibility: Compatibility{
			QualifiedCommercialTypes: slices.Clone(file.Compatibility.QualifiedCommercialTypes),
		},
		Pools: pools, StorageClasses: storageClasses,
	}
	if err := validateComponentEnvironment(component, runtime.Provider, lookup); err != nil {
		return Runtime{}, err
	}
	return runtime, nil
}

func (file runtimeFile) runtimePools() ([]pool.Config, error) {
	names := make([]string, 0, len(file.Pools))
	for name := range file.Pools {
		names = append(names, name)
	}
	slices.Sort(names)
	result := make([]pool.Config, 0, len(names))
	for _, name := range names {
		configured := file.Pools[name]
		ratio, err := pool.ParseRatio(configured.MaxLogicalOvercommitRatio)
		if err != nil {
			return nil, fmt.Errorf("pool %q overcommit ratio: %w", name, err)
		}
		uid, err := parseIdentity("directoryUid", configured.DirectoryUID)
		if err != nil {
			return nil, fmt.Errorf("pool %q: %w", name, err)
		}
		gid, err := parseIdentity("directoryGid", configured.DirectoryGID)
		if err != nil {
			return nil, fmt.Errorf("pool %q: %w", name, err)
		}
		parents := make([]pool.ParentConfig, 0, len(configured.Filesystems))
		for index, parent := range configured.Filesystems {
			if parent.Name == "" || len(parent.Name) > 128 || !utf8.ValidString(parent.Name) || strings.ContainsAny(parent.Name, "\x00\r\n") {
				return nil, fmt.Errorf("pool %q parent %d display name must be single-line UTF-8 containing 1 to 128 bytes", name, index)
			}
			parents = append(parents, pool.ParentConfig{ID: parent.ID, Name: parent.Name, State: parent.State})
		}
		result = append(result, pool.Config{
			Name: name, BasePath: configured.BasePath, SelectionPolicy: configured.SelectionPolicy,
			MaxParentsPerEligibleNode: configured.MaxParentsPerEligibleNode,
			MaxLogicalOvercommitRatio: ratio, MinFreeBytes: configured.MinFreeBytes,
			MinFreePercent: configured.MinFreePercent, DeletePolicy: configured.DeletePolicy,
			DirectoryMode: configured.DirectoryMode, DirectoryUID: uid, DirectoryGID: gid,
			Filesystems: parents,
		})
	}
	return result, nil
}

func (file runtimeControllerFile) runtime() (Controller, error) {
	shutdown, err := secondsDuration("shutdown deadline", file.ShutdownDeadlineSeconds)
	if err != nil {
		return Controller{}, err
	}
	termination, err := secondsDuration("termination grace period", file.TerminationGracePeriodSeconds)
	if err != nil {
		return Controller{}, err
	}
	progress, err := secondsDuration("progress deadline", file.ProgressDeadlineSeconds)
	if err != nil {
		return Controller{}, err
	}
	startup, err := secondsDuration("startup probe budget", file.StartupProbeBudgetSeconds)
	if err != nil {
		return Controller{}, err
	}
	attach, err := secondsDuration("attach readiness deadline", file.AttachReadyDeadlineSeconds)
	if err != nil {
		return Controller{}, err
	}
	refresh, err := secondsDuration("metadata refresh interval", file.MetadataRefreshIntervalSeconds)
	if err != nil {
		return Controller{}, err
	}
	retention, err := secondsDuration("detailed tombstone retention", file.DetailedTombstoneRetentionSeconds)
	if err != nil {
		return Controller{}, err
	}
	lease, err := secondsDuration("leadership lease duration", file.Leadership.LeaseDurationSeconds)
	if err != nil {
		return Controller{}, err
	}
	renew, err := secondsDuration("leadership renew deadline", file.Leadership.RenewDeadlineSeconds)
	if err != nil {
		return Controller{}, err
	}
	retry, err := secondsDuration("leadership retry period", file.Leadership.RetryPeriodSeconds)
	if err != nil {
		return Controller{}, err
	}
	if file.Leadership.LeaseName != volume.LeadershipLeaseNameV1 {
		return Controller{}, fmt.Errorf("leadership Lease name %q differs from fixed v1 name", file.Leadership.LeaseName)
	}
	return Controller{
		Replicas: file.Replicas, UpdateStrategy: file.UpdateStrategy,
		MaxConcurrentMutations: file.MaxConcurrentMutations,
		ShutdownDeadline:       shutdown, TerminationGracePeriod: termination,
		ProgressDeadline: progress, StartupProbeBudget: startup,
		AttachReadyDeadline: attach, MetadataRefreshInterval: refresh,
		DetailedTombstoneRetention: retention,
		ParentMountRoot:            file.ParentMountRoot,
		Leadership:                 Leadership{Enabled: file.Leadership.Enabled, LeaseDuration: lease, RenewDeadline: renew, RetryPeriod: retry},
	}, nil
}

func validateComponentEnvironment(component Component, provider Provider, lookup LookupEnv) error {
	access, accessPresent := lookup("SCW_ACCESS_KEY")
	secret, secretPresent := lookup("SCW_SECRET_KEY")
	if component == ComponentNode {
		if accessPresent || secretPresent {
			return fmt.Errorf("node process must not receive Scaleway API credentials")
		}
		return nil
	}
	if !accessPresent || !secretPresent || access == "" || secret == "" {
		return fmt.Errorf("controller Scaleway credentials are missing")
	}
	for _, authority := range []struct {
		key      string
		expected string
	}{
		{key: "SCW_DEFAULT_PROJECT_ID", expected: provider.ProjectID},
		{key: "SCW_DEFAULT_REGION", expected: provider.Region},
		{key: "SCW_DEFAULT_ZONE", expected: provider.DefaultZone},
	} {
		value, present := lookup(authority.key)
		if !present || value != authority.expected {
			return fmt.Errorf("controller %s does not match the validated Helm configuration", authority.key)
		}
	}
	return nil
}

func secondsDuration(name string, seconds uint64) (time.Duration, error) {
	if seconds == 0 || seconds > uint64(math.MaxInt64/int64(time.Second)) {
		return 0, fmt.Errorf("%s seconds are outside the positive time.Duration range", name)
	}
	return time.Duration(seconds) * time.Second, nil
}

func parseIdentity(name, value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 31)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, fmt.Errorf("%s must be a canonical base-10 integer in [0,2147483647]", name)
	}
	return uint32(parsed), nil
}

func validateRuntimeRequiredFields(data []byte) error {
	root, err := requiredObject(data, "$", []string{
		"schemaVersion", "mode", "driverName", "logLevel", "controllerNamespace",
		"helmReleaseName", "chartVersion", "renderedImages", "nodeConfigGeneration", "installation", "scaleway",
		"controller", "node", "scheduling", "compatibility", "pools", "storageClasses",
	})
	if err != nil {
		return err
	}
	if _, err := requiredObject(root["installation"], "$.installation", []string{
		"existingSecretName", "idKey", "generateForDevelopmentOnly",
	}); err != nil {
		return err
	}
	provider, err := requiredObject(root["scaleway"], "$.scaleway", []string{
		"region", "defaultZone", "projectId", "credentials",
	})
	if err != nil {
		return err
	}
	if _, err := requiredObject(provider["credentials"], "$.scaleway.credentials", []string{
		"existingSecretName", "accessKeyKey", "secretKeyKey",
	}); err != nil {
		return err
	}
	controller, err := requiredObject(root["controller"], "$.controller", []string{
		"replicas", "updateStrategy", "maxConcurrentMutations", "shutdownDeadlineSeconds",
		"terminationGracePeriodSeconds", "progressDeadlineSeconds", "startupProbeBudgetSeconds",
		"attachReadyDeadlineSeconds", "metadataRefreshIntervalSeconds", "detailedTombstoneRetentionSeconds", "parentMountRoot", "leadership",
	})
	if err != nil {
		return err
	}
	if _, err := requiredObject(controller["leadership"], "$.controller.leadership", []string{
		"enabled", "leaseName", "leaseDurationSeconds", "renewDeadlineSeconds", "retryPeriodSeconds",
	}); err != nil {
		return err
	}
	if _, err := requiredObject(root["node"], "$.node", []string{"parentMountRoot", "kubeletPath"}); err != nil {
		return err
	}
	if _, err := requiredObject(root["scheduling"], "$.scheduling", []string{
		"allSchedulableLinuxNodesAreEligible", "requireHomogeneousEligibleNodes", "skipNodePreflightForDevelopmentOnly",
	}); err != nil {
		return err
	}
	if _, err := requiredObject(root["compatibility"], "$.compatibility", []string{
		"qualifiedCommercialTypes",
	}); err != nil {
		return err
	}
	var pools map[string]json.RawMessage
	if err := json.Unmarshal(root["pools"], &pools); err != nil || pools == nil {
		return fmt.Errorf("runtime configuration $.pools must be an object")
	}
	poolNames := make([]string, 0, len(pools))
	for name := range pools {
		poolNames = append(poolNames, name)
	}
	slices.Sort(poolNames)
	for _, name := range poolNames {
		path := "$.pools." + name
		configured, err := requiredObject(pools[name], path, []string{
			"basePath", "selectionPolicy", "maxParentsPerEligibleNode", "maxLogicalOvercommitRatio",
			"minFreeBytes", "minFreePercent", "onDelete", "directoryMode", "directoryUid",
			"directoryGid", "filesystems",
		})
		if err != nil {
			return err
		}
		var parents []json.RawMessage
		if err := json.Unmarshal(configured["filesystems"], &parents); err != nil || parents == nil {
			return fmt.Errorf("runtime configuration %s.filesystems must be an array", path)
		}
		for index, parent := range parents {
			if _, err := requiredObject(parent, fmt.Sprintf("%s.filesystems[%d]", path, index), []string{"id", "name", "state"}); err != nil {
				return err
			}
		}
	}
	var storageClasses []json.RawMessage
	if err := json.Unmarshal(root["storageClasses"], &storageClasses); err != nil || storageClasses == nil {
		return fmt.Errorf("runtime configuration $.storageClasses must be an array")
	}
	for index, storageClass := range storageClasses {
		if _, err := requiredObject(storageClass, fmt.Sprintf("$.storageClasses[%d]", index), []string{
			"name", "poolName", "defaultClass", "reclaimPolicy", "allowVolumeExpansion", "volumeBindingMode",
		}); err != nil {
			return err
		}
	}
	return nil
}

func requiredObject(data []byte, path string, required []string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, fmt.Errorf("runtime configuration %s must be an object", path)
	}
	for _, field := range required {
		if _, present := object[field]; !present {
			return nil, fmt.Errorf("runtime configuration %s.%s is missing", path, field)
		}
	}
	return object, nil
}
