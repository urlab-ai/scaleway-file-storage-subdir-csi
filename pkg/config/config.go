package config

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	releasecompat "scaleway-sfs-subdir-csi/internal/compatibility"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const (
	// FixedNodeParentMountRoot is the only supported production v1 node root.
	FixedNodeParentMountRoot = "/var/lib/scaleway-sfs-subdir-csi/parents"
	// MaxConcurrentMutationsV1 is the tested process-wide mutation envelope.
	MaxConcurrentMutationsV1 = 10
)

var (
	regionPattern         = regexp.MustCompile(`^[a-z]{2}-[a-z]+$`)
	zonePattern           = regexp.MustCompile(`^[a-z]{2}-[a-z]+-[0-9]+$`)
	kubernetesNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$`)
	secretDataKeyPattern  = regexp.MustCompile(`^[-._A-Za-z0-9]+$`)
)

// Mode separates deliberately unsupported local development bypasses from
// production validation. Development mode never becomes a release default.
type Mode string

const (
	ModeDevelopment Mode = "development"
	ModeProduction  Mode = "production"
)

// Installation identifies the durable external identity Secret and value.
type Installation struct {
	ExistingSecretName         string
	IDKey                      string
	ID                         string
	GenerateForDevelopmentOnly bool
}

// Provider contains non-secret Scaleway scope and the credential Secret name.
type Provider struct {
	Region                        string
	DefaultZone                   string
	ProjectID                     string
	CredentialsExistingSecretName string
	AccessKeyKey                  string
	SecretKeyKey                  string
}

// Leadership contains the fixed Lease timing contract.
type Leadership struct {
	Enabled       bool
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Controller contains concurrency, lifecycle, and parent mount settings.
type Controller struct {
	Replicas                   uint32
	UpdateStrategy             string
	MaxConcurrentMutations     uint32
	ShutdownDeadline           time.Duration
	TerminationGracePeriod     time.Duration
	ProgressDeadline           time.Duration
	StartupProbeBudget         time.Duration
	AttachReadyDeadline        time.Duration
	MetadataRefreshInterval    time.Duration
	DetailedTombstoneRetention time.Duration
	ParentMountRoot            string
	Leadership                 Leadership
}

// Node contains host path settings validated before mount propagation starts.
type Node struct {
	ParentMountRoot string
	KubeletPath     string
}

// Scheduling defines the only production v1 homogeneous node model.
type Scheduling struct {
	AllSchedulableLinuxNodesAreEligible bool `json:"allSchedulableLinuxNodesAreEligible"`
	RequireHomogeneousEligibleNodes     bool `json:"requireHomogeneousEligibleNodes"`
	SkipNodePreflightForDevelopmentOnly bool `json:"skipNodePreflightForDevelopmentOnly"`
}

// Compatibility is the exact release-qualified provider matrix expected from
// the binary. Production startup compares it with the independently linked
// build identity so an operator cannot acknowledge an untested Instance type.
type Compatibility struct {
	QualifiedCommercialTypes []string
}

// QualifiedCommercialTypeSet returns the exact validated allowlist in the map
// form consumed by provider attachment and node preflight state machines.
func (compatibility Compatibility) QualifiedCommercialTypeSet() (map[string]struct{}, error) {
	if err := releasecompat.ValidateCommercialTypes(compatibility.QualifiedCommercialTypes); err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(compatibility.QualifiedCommercialTypes))
	for _, commercialType := range compatibility.QualifiedCommercialTypes {
		result[commercialType] = struct{}{}
	}
	return result, nil
}

// StorageClass is the validated chart/runtime projection for one class.
type StorageClass struct {
	Name                 string
	PoolName             string
	DefaultClass         bool
	ReclaimPolicy        string
	AllowVolumeExpansion bool
	VolumeBindingMode    string
}

// Runtime is the complete configuration authority consumed at process start.
type Runtime struct {
	Mode           Mode
	DriverName     string
	Installation   Installation
	Provider       Provider
	Controller     Controller
	Node           Node
	Scheduling     Scheduling
	Compatibility  Compatibility
	Pools          []pool.Config
	StorageClasses []StorageClass
}

// Validate checks all pure and cross-field v1 constraints.
func (runtime Runtime) Validate() error {
	if runtime.Mode != ModeDevelopment && runtime.Mode != ModeProduction {
		return fmt.Errorf("runtime mode %q is unsupported", runtime.Mode)
	}
	if err := volume.ValidateDriverName(runtime.DriverName); err != nil {
		return err
	}
	if err := runtime.validateInstallation(); err != nil {
		return err
	}
	if err := runtime.validateProvider(); err != nil {
		return err
	}
	if err := runtime.validateController(); err != nil {
		return err
	}
	if err := runtime.validateNode(); err != nil {
		return err
	}
	if runtime.Mode == ModeProduction {
		if !runtime.Scheduling.AllSchedulableLinuxNodesAreEligible || !runtime.Scheduling.RequireHomogeneousEligibleNodes || runtime.Scheduling.SkipNodePreflightForDevelopmentOnly {
			return fmt.Errorf("production mode requires the homogeneous all-schedulable-Linux-node preflight")
		}
	}
	if err := releasecompat.ValidateCommercialTypes(runtime.Compatibility.QualifiedCommercialTypes); err != nil {
		return fmt.Errorf("runtime compatibility: %w", err)
	}
	if err := pool.ValidateConfigs(runtime.Pools); err != nil {
		return err
	}
	return runtime.validateStorageClasses()
}

func (runtime Runtime) validateInstallation() error {
	installation := runtime.Installation
	if !validSecretDataKey(installation.IDKey) {
		return fmt.Errorf("installation identity Secret key must contain 1 to 128 Kubernetes Secret-key characters")
	}
	if installation.ID == "" {
		return fmt.Errorf("installation identity is empty")
	}
	if err := volume.ValidateInstallationID(installation.ID); err != nil {
		return err
	}
	if runtime.Mode == ModeProduction {
		if installation.ExistingSecretName == "" || !validKubernetesName(installation.ExistingSecretName) {
			return fmt.Errorf("production mode requires a valid existing installation identity Secret name")
		}
		if installation.GenerateForDevelopmentOnly {
			return fmt.Errorf("production mode forbids generated installation identity")
		}
	}
	return nil
}

func (runtime Runtime) validateProvider() error {
	provider := runtime.Provider
	if !regionPattern.MatchString(provider.Region) {
		return fmt.Errorf("scaleway region %q is invalid", provider.Region)
	}
	if !zonePattern.MatchString(provider.DefaultZone) || !strings.HasPrefix(provider.DefaultZone, provider.Region+"-") {
		return fmt.Errorf("default zone %q does not belong to region %q", provider.DefaultZone, provider.Region)
	}
	if err := volume.ValidateInstallationID(provider.ProjectID); err != nil {
		return fmt.Errorf("scaleway project ID: %w", err)
	}
	if provider.CredentialsExistingSecretName == "" || !validKubernetesName(provider.CredentialsExistingSecretName) {
		return fmt.Errorf("a valid existing Scaleway credential Secret name is required")
	}
	if !validSecretDataKey(provider.AccessKeyKey) || !validSecretDataKey(provider.SecretKeyKey) || provider.AccessKeyKey == provider.SecretKeyKey {
		return fmt.Errorf("credential access and secret key names must be distinct valid Kubernetes Secret data keys")
	}
	return nil
}

func (runtime Runtime) validateController() error {
	controller := runtime.Controller
	if controller.Replicas != 1 {
		return fmt.Errorf("v1 requires exactly one controller replica")
	}
	if controller.UpdateStrategy != "Recreate" {
		return fmt.Errorf("v1 requires controller update strategy Recreate")
	}
	if controller.MaxConcurrentMutations == 0 || controller.MaxConcurrentMutations > MaxConcurrentMutationsV1 {
		return fmt.Errorf("controller mutation limit must be in [1,%d]", MaxConcurrentMutationsV1)
	}
	if !durationMarginAtLeast(controller.TerminationGracePeriod, controller.ShutdownDeadline, 30*time.Second) {
		return fmt.Errorf("termination grace period must exceed shutdown deadline by at least 30 seconds")
	}
	if !durationMarginAtLeast(controller.ProgressDeadline, controller.StartupProbeBudget, 5*time.Minute) {
		return fmt.Errorf("deployment progress deadline must cover startup probe budget plus five minutes")
	}
	if controller.AttachReadyDeadline <= 0 || controller.MetadataRefreshInterval <= 0 || controller.DetailedTombstoneRetention <= 0 {
		return fmt.Errorf("attach readiness, metadata refresh, and detailed tombstone retention durations must be positive")
	}
	if err := validateAbsoluteNormalizedPath("controller parent mount root", controller.ParentMountRoot); err != nil {
		return err
	}
	leadership := controller.Leadership
	if !leadership.Enabled {
		return fmt.Errorf("controller leadership is mandatory in v1")
	}
	if leadership.RetryPeriod <= 0 || leadership.RenewDeadline <= 0 || leadership.LeaseDuration <= 0 || leadership.RetryPeriod >= leadership.RenewDeadline || leadership.RenewDeadline >= leadership.LeaseDuration {
		return fmt.Errorf("leadership timing must satisfy retryPeriod < renewDeadline < leaseDuration")
	}
	return nil
}

func durationMarginAtLeast(larger, smaller, margin time.Duration) bool {
	return larger > 0 && smaller > 0 && margin > 0 && larger >= smaller && larger-smaller >= margin
}

func (runtime Runtime) validateNode() error {
	if err := validateAbsoluteNormalizedPath("node parent mount root", runtime.Node.ParentMountRoot); err != nil {
		return err
	}
	if err := validateAbsoluteNormalizedPath("kubelet path", runtime.Node.KubeletPath); err != nil {
		return err
	}
	if runtime.Mode == ModeProduction && runtime.Node.ParentMountRoot != FixedNodeParentMountRoot {
		return fmt.Errorf("production node parent mount root must be %q", FixedNodeParentMountRoot)
	}
	if pathsOverlap(runtime.Node.ParentMountRoot, runtime.Node.KubeletPath) {
		return fmt.Errorf("node parent mount root %q overlaps kubelet path %q", runtime.Node.ParentMountRoot, runtime.Node.KubeletPath)
	}
	return nil
}

func (runtime Runtime) validateStorageClasses() error {
	if len(runtime.StorageClasses) == 0 {
		return fmt.Errorf("at least one StorageClass is required")
	}
	pools := make(map[string]struct{}, len(runtime.Pools))
	for _, configuredPool := range runtime.Pools {
		pools[configuredPool.Name] = struct{}{}
	}
	names := make(map[string]struct{}, len(runtime.StorageClasses))
	defaultCount := 0
	for _, storageClass := range runtime.StorageClasses {
		if !validKubernetesName(storageClass.Name) {
			return fmt.Errorf("StorageClass name %q is invalid", storageClass.Name)
		}
		if _, duplicate := names[storageClass.Name]; duplicate {
			return fmt.Errorf("duplicate StorageClass name %q", storageClass.Name)
		}
		names[storageClass.Name] = struct{}{}
		if _, exists := pools[storageClass.PoolName]; !exists {
			return fmt.Errorf("StorageClass %q references missing pool %q", storageClass.Name, storageClass.PoolName)
		}
		if storageClass.DefaultClass {
			defaultCount++
		}
		if storageClass.ReclaimPolicy != "Delete" || storageClass.AllowVolumeExpansion || storageClass.VolumeBindingMode != "Immediate" {
			return fmt.Errorf("StorageClass %q must use reclaimPolicy Delete, expansion disabled, and Immediate binding", storageClass.Name)
		}
	}
	if defaultCount > 1 {
		return fmt.Errorf("at most one StorageClass may be default")
	}
	return nil
}

func validKubernetesName(value string) bool {
	return len(value) > 0 && len(value) <= 253 && kubernetesNamePattern.MatchString(value)
}

func validSecretDataKey(value string) bool {
	return len(value) > 0 && len(value) <= 128 && secretDataKeyPattern.MatchString(value)
}

func validateAbsoluteNormalizedPath(name, value string) error {
	if value == "" || !strings.HasPrefix(value, "/") || path.Clean(value) != value || value == "/" {
		return fmt.Errorf("%s %q must be an absolute normalized non-root path", name, value)
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}
