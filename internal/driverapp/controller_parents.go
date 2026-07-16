package driverapp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"sync"

	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/observability"
	"scaleway-sfs-subdir-csi/pkg/pool"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type nodeInventorySource interface {
	Snapshot(ctx context.Context) ([]k8s.NodeInventoryObservation, error)
}

type controllerNodeAuthorizations struct {
	mu             sync.RWMutex
	inventory      nodeInventorySource
	provider       scaleway.API
	region         string
	projectID      string
	generation     string
	commercial     []string
	parents        map[string]struct{}
	production     bool
	latest         driver.NodeAuthorizationSet
	latestKnown    map[string]struct{}
	latestEligible map[string]struct{}
}

func newControllerNodeAuthorizations(inventory nodeInventorySource, provider scaleway.API, configured config.Loaded) (*controllerNodeAuthorizations, error) {
	if inventory == nil || provider == nil {
		return nil, fmt.Errorf("controller node authorization dependency is nil")
	}
	if configured.NodeConfigGeneration == "" {
		return nil, fmt.Errorf("controller node configuration generation is empty")
	}
	parents := make(map[string]struct{})
	for _, parentID := range pool.ParentIDs(configured.Runtime.Pools) {
		parents[parentID] = struct{}{}
	}
	if len(parents) == 0 {
		return nil, fmt.Errorf("controller node authorization parent set is empty")
	}
	return &controllerNodeAuthorizations{
		inventory: inventory, provider: provider,
		region: configured.Runtime.Provider.Region, projectID: configured.Runtime.Provider.ProjectID,
		generation: configured.NodeConfigGeneration,
		commercial: slices.Clone(configured.Runtime.Compatibility.QualifiedCommercialTypes),
		parents:    parents, production: configured.Runtime.Mode == config.ModeProduction,
	}, nil
}

func (authorizations *controllerNodeAuthorizations) Refresh(ctx context.Context) (map[string]scaleway.Target, map[string]struct{}, error) {
	refresh, err := authorizations.RefreshSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	return cloneTargetMap(refresh.KnownInstances), cloneSet(refresh.EligibleInstanceIDs), nil
}

// NodeExists implements driver.NodeExistenceReader without consulting
// Scaleway. CSI sanity's context-less unknown-node probe is allowed to observe
// a conclusive Kubernetes absence before immutable-context validation, but it
// must never trigger provider discovery, attachment, or rollout repair.
func (authorizations *controllerNodeAuthorizations) NodeExists(ctx context.Context, nodeID string) (bool, error) {
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return false, err
	}
	observed, err := authorizations.inventory.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	for _, node := range observed {
		if node.DriverRegistered && node.CSINodeID == nodeID {
			return true, nil
		}
	}
	return false, nil
}

type controllerNodeAuthorizationRefresh struct {
	KnownInstanceIDs    map[string]struct{}
	KnownInstances      map[string]scaleway.Target
	EligibleInstanceIDs map[string]struct{}
	Servers             map[string]scaleway.Server
	ExpectedNodes       uint64
	ReadyNodes          uint64
	GenerationMismatch  uint64
	AttachmentSlotsUsed uint64
	AttachmentSlotLimit uint64
	// ParentDegradations contains read-only attachment-inventory failures keyed
	// by configured parent. They do not invalidate unrelated parents.
	ParentDegradations map[string]error
	// UnknownAttachments contains bounded anomaly counts by pool and closed
	// classification. Exact identities remain available only in logs/events.
	UnknownAttachments map[string]map[observability.UnknownAttachmentClass]uint64
}

func (authorizations *controllerNodeAuthorizations) RefreshSnapshot(ctx context.Context) (controllerNodeAuthorizationRefresh, error) {
	observed, err := authorizations.inventory.Snapshot(ctx)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	rollout := make([]driver.NodeRolloutObservation, 0, len(observed))
	controllerCandidates := make([]driver.ControllerCandidateObservation, 0, len(observed))
	servers := make(map[string]scaleway.Server)
	for _, node := range observed {
		projection := driver.NodeRolloutObservation{
			NodeName: node.NodeName, CSINodeID: node.CSINodeID,
			OperatingSystem: node.OperatingSystem, Schedulable: node.Schedulable,
			Deleting: node.Deleting, Ready: node.Ready,
			PluginPodPresent: node.PluginPodPresent, PluginPodReady: node.PluginPodReady,
			DriverRegistered: node.DriverRegistered, NodeConfigGeneration: node.NodeConfigGeneration,
		}
		if node.OperatingSystem == "linux" && node.DriverRegistered {
			target, err := scaleway.ParseNodeID(node.CSINodeID)
			if err != nil {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("resolve Kubernetes node %q provider identity: %w", node.NodeName, err)
			}
			server, err := authorizations.provider.GetServer(ctx, target.Zone, target.ServerID)
			if err != nil {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("read Kubernetes node %q Instance: %w", node.NodeName, err)
			}
			if server.ID != target.ServerID || server.Zone != target.Zone || server.Region != authorizations.region || server.ProjectID != authorizations.projectID {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("kubernetes node %q Instance identity differs from configured provider scope", node.NodeName)
			}
			if _, duplicate := servers[server.ID]; duplicate {
				return controllerNodeAuthorizationRefresh{}, fmt.Errorf("multiple Kubernetes nodes resolve to Instance %q", server.ID)
			}
			servers[server.ID] = server
			projection.CommercialType = server.CommercialType
			projection.MaxFileSystems = server.MaxFileSystems
		}
		if node.OperatingSystem == "linux" && node.Schedulable {
			failureDomain := "unknown"
			compatible := false
			if target, parseErr := scaleway.ParseNodeID(node.CSINodeID); parseErr == nil {
				failureDomain = target.Zone
				if server, present := servers[target.ServerID]; present {
					compatible = slices.Contains(authorizations.commercial, server.CommercialType) && server.MaxFileSystems > 0
				}
			}
			controllerCandidates = append(controllerCandidates, driver.ControllerCandidateObservation{
				NodeName: node.NodeName, FailureDomain: failureDomain,
				Ready: node.Ready, Deleting: node.Deleting, Compatible: compatible,
			})
		}
		rollout = append(rollout, projection)
	}
	validated, err := driver.ValidateNodeRollout(rollout, authorizations.generation, authorizations.region, authorizations.commercial)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	if authorizations.production {
		if err := driver.ValidateControllerCandidates(controllerCandidates); err != nil {
			return controllerNodeAuthorizationRefresh{}, err
		}
	}
	knownInstances, err := nodeIDsToTargets(validated.KnownNodeIDs)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	known := make(map[string]struct{}, len(knownInstances))
	for instanceID := range knownInstances {
		known[instanceID] = struct{}{}
	}
	eligible, err := nodeIDsToInstanceIDs(validated.EligibleNodeIDs)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, err
	}
	var slotsUsed, slotLimit uint64
	for instanceID := range eligible {
		server, present := servers[instanceID]
		if !present {
			return controllerNodeAuthorizationRefresh{}, fmt.Errorf("eligible Instance %q has no coherent provider observation", instanceID)
		}
		if err := scaleway.ValidateExclusiveServerInventory(server, authorizations.parents); err != nil {
			return controllerNodeAuthorizationRefresh{}, err
		}
		if err := scaleway.ValidatePostAttachBudget(server, authorizations.parents); err != nil {
			return controllerNodeAuthorizationRefresh{}, err
		}
		attached, err := scaleway.ServerAttachmentMap(server)
		if err != nil {
			return controllerNodeAuthorizationRefresh{}, err
		}
		if uint64(len(attached)) > ^uint64(0)-slotsUsed || uint64(server.MaxFileSystems) > ^uint64(0)-slotLimit {
			return controllerNodeAuthorizationRefresh{}, fmt.Errorf("eligible node attachment slot aggregate overflow")
		}
		slotsUsed += uint64(len(attached))
		slotLimit += uint64(server.MaxFileSystems)
	}
	authorizations.mu.Lock()
	authorizations.latest = cloneNodeAuthorizationSet(validated)
	authorizations.latestKnown = known
	authorizations.latestEligible = eligible
	authorizations.mu.Unlock()
	return controllerNodeAuthorizationRefresh{
		KnownInstanceIDs: cloneSet(known), KnownInstances: cloneTargetMap(knownInstances), EligibleInstanceIDs: cloneSet(eligible),
		Servers:       cloneServerMap(servers),
		ExpectedNodes: uint64(len(eligible)), ReadyNodes: uint64(len(eligible)),
		AttachmentSlotsUsed: slotsUsed, AttachmentSlotLimit: slotLimit,
	}, nil
}

func cloneServerMap(input map[string]scaleway.Server) map[string]scaleway.Server {
	result := make(map[string]scaleway.Server, len(input))
	for instanceID, server := range input {
		server.Filesystems = slices.Clone(server.Filesystems)
		result[instanceID] = server
	}
	return result
}

func nodeIDsToInstanceIDs(nodeIDs map[string]struct{}) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(nodeIDs))
	for nodeID := range nodeIDs {
		target, err := scaleway.ParseNodeID(nodeID)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[target.ServerID]; duplicate {
			return nil, fmt.Errorf("multiple CSI nodes resolve to Instance %q", target.ServerID)
		}
		result[target.ServerID] = struct{}{}
	}
	return result, nil
}

func nodeIDsToTargets(nodeIDs map[string]struct{}) (map[string]scaleway.Target, error) {
	result := make(map[string]scaleway.Target, len(nodeIDs))
	for nodeID := range nodeIDs {
		target, err := scaleway.ParseNodeID(nodeID)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[target.ServerID]; duplicate {
			return nil, fmt.Errorf("multiple CSI nodes resolve to Instance %q", target.ServerID)
		}
		result[target.ServerID] = target
	}
	return result, nil
}

func cloneNodeAuthorizationSet(input driver.NodeAuthorizationSet) driver.NodeAuthorizationSet {
	return driver.NodeAuthorizationSet{
		EligibleNodeIDs: cloneSet(input.EligibleNodeIDs), KnownNodeIDs: cloneSet(input.KnownNodeIDs),
	}
}

func cloneSet(input map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(input))
	for key := range input {
		result[key] = struct{}{}
	}
	return result
}

func cloneTargetMap(input map[string]scaleway.Target) map[string]scaleway.Target {
	result := make(map[string]scaleway.Target, len(input))
	for instanceID, target := range input {
		result[instanceID] = target
	}
	return result
}

type controllerParentAccess struct {
	region            string
	projectID         string
	localNodeID       string
	parentRoot        string
	configuredParents map[string]struct{}
	parentPools       map[string]string
	qualifiedTypes    map[string]struct{}
	authorizations    *controllerNodeAuthorizations
	attachments       *scaleway.AttachmentManager
	provider          scaleway.API
	mounter           mount.Interface
	attachmentLocks   *coordination.KeyedLock
}

func newControllerParentAccess(configured config.Runtime, localNodeID string, authorizations *controllerNodeAuthorizations, attachments *scaleway.AttachmentManager, mounter mount.Interface) (*controllerParentAccess, error) {
	if authorizations == nil || attachments == nil || mounter == nil {
		return nil, fmt.Errorf("controller parent access dependency is nil")
	}
	if _, err := scaleway.ParseNodeID(localNodeID); err != nil {
		return nil, err
	}
	qualified, err := configured.Compatibility.QualifiedCommercialTypeSet()
	if err != nil {
		return nil, err
	}
	parents := make(map[string]struct{})
	parentPools := make(map[string]string)
	for _, configuredPool := range configured.Pools {
		for _, parent := range configuredPool.Filesystems {
			parents[parent.ID] = struct{}{}
			parentPools[parent.ID] = configuredPool.Name
		}
	}
	return &controllerParentAccess{
		region: configured.Provider.Region, projectID: configured.Provider.ProjectID,
		localNodeID: localNodeID, parentRoot: configured.Controller.ParentMountRoot,
		configuredParents: parents, parentPools: parentPools, qualifiedTypes: qualified,
		authorizations: authorizations, attachments: attachments, provider: authorizations.provider, mounter: mounter,
		attachmentLocks: coordination.NewKeyedLock(),
	}, nil
}

// ValidateInstallationInventory refreshes the complete homogeneous node view
// and every configured parent's paginated regional attachment inventory. It is
// read-only and fails closed for the affected parent on foreign, unknown, or
// mismatched attachment evidence while preserving unrelated parents.
func (access *controllerParentAccess) ValidateInstallationInventory(ctx context.Context) (controllerNodeAuthorizationRefresh, error) {
	refresh, err := access.authorizations.RefreshSnapshot(ctx)
	if err != nil {
		return controllerNodeAuthorizationRefresh{}, fmt.Errorf("refresh node authorization inventory: %w", err)
	}
	parentIDs := make([]string, 0, len(access.configuredParents))
	for parentID := range access.configuredParents {
		parentIDs = append(parentIDs, parentID)
	}
	slices.Sort(parentIDs)
	refresh.ParentDegradations = make(map[string]error)
	refresh.UnknownAttachments = make(map[string]map[observability.UnknownAttachmentClass]uint64)
	for _, poolName := range access.parentPools {
		if refresh.UnknownAttachments[poolName] == nil {
			refresh.UnknownAttachments[poolName] = make(map[observability.UnknownAttachmentClass]uint64)
		}
	}
	for _, parentID := range parentIDs {
		filesystem, err := access.provider.GetFilesystem(ctx, access.region, parentID)
		if err != nil {
			if ctx.Err() != nil {
				return controllerNodeAuthorizationRefresh{}, ctx.Err()
			}
			access.recordParentDegradation(&refresh, parentID, fmt.Errorf("read attachment inventory metadata: %w", err))
			continue
		}
		if filesystem.ID != parentID || filesystem.ProjectID != access.projectID || filesystem.Region != access.region || filesystem.SizeBytes == 0 {
			access.recordParentDegradation(&refresh, parentID, fmt.Errorf("attachment inventory metadata differs from configured scope"))
			continue
		}
		inventory, err := scaleway.ListRegionalInventory(ctx, access.provider, filesystem)
		if err != nil {
			if ctx.Err() != nil {
				return controllerNodeAuthorizationRefresh{}, ctx.Err()
			}
			access.recordParentDegradation(&refresh, parentID, err)
			continue
		}
		if err := scaleway.ValidateAuthorizedAttachments(inventory, refresh.KnownInstances); err != nil {
			access.recordParentDegradation(&refresh, parentID, err)
			continue
		}
		if err := scaleway.ValidateAttachmentInventoryAgreement(inventory, refresh.KnownInstances, refresh.Servers); err != nil {
			access.recordParentDegradation(&refresh, parentID, err)
		}
	}
	return refresh, nil
}

func (access *controllerParentAccess) recordParentDegradation(refresh *controllerNodeAuthorizationRefresh, parentID string, err error) {
	wrapped := fmt.Errorf("parent %q attachment inventory: %w", parentID, err)
	refresh.ParentDegradations[parentID] = wrapped
	class, classified := attachmentAnomalyClass(err)
	if !classified {
		return
	}
	poolName := access.parentPools[parentID]
	refresh.UnknownAttachments[poolName][class]++
}

func attachmentAnomalyClass(err error) (observability.UnknownAttachmentClass, bool) {
	switch {
	case errors.Is(err, scaleway.ErrUnknownAttachmentNode):
		return observability.UnknownAttachmentUnknownNode, true
	case errors.Is(err, scaleway.ErrForeignAttachmentType):
		return observability.UnknownAttachmentForeignType, true
	case errors.Is(err, scaleway.ErrAttachmentInventoryDisagreement):
		return observability.UnknownAttachmentDisagreement, true
	default:
		return "", false
	}
}

// EnsureAttached implements driver.AttachmentPublisher with a fresh complete
// node and provider authorization snapshot before any possible attach call.
func (access *controllerParentAccess) EnsureAttached(ctx context.Context, allocation *volume.DetailedAllocationRecord, nodeID string) error {
	if allocation == nil {
		return fmt.Errorf("attachment allocation is nil")
	}
	if err := allocation.Validate(); err != nil {
		return err
	}
	return access.ensureAttached(ctx, allocation.ParentFilesystemID, nodeID)
}

func (access *controllerParentAccess) ensureAttached(ctx context.Context, parentID, nodeID string) error {
	if _, configured := access.configuredParents[parentID]; !configured {
		return fmt.Errorf("parent %q is not configured", parentID)
	}
	target, err := scaleway.ParseNodeID(nodeID)
	if err != nil {
		return err
	}
	lockKey := nodeID + "\x00" + parentID
	unlock, err := access.attachmentLocks.Lock(ctx, lockKey)
	if err != nil {
		return err
	}
	defer unlock()
	known, eligible, err := access.authorizations.Refresh(ctx)
	if err != nil {
		return fmt.Errorf("refresh node attachment authorization: %w", err)
	}
	return access.attachments.EnsureAttached(ctx, scaleway.AttachRequest{
		Region: access.region, ProjectID: access.projectID, FilesystemID: parentID,
		Target: target, ConfiguredParentIDs: cloneSet(access.configuredParents),
		KnownInstances: known, EligibleInstanceIDs: eligible,
		QualifiedCommercialTypes: cloneSet(access.qualifiedTypes),
	})
}

// EnsureMounted implements parentfs.MountedParentAccess. The attachment lock
// serializes same-parent mount check-and-act after the enclosing global and
// per-volume/pool locks; no code path acquires those outer locks from here.
func (access *controllerParentAccess) EnsureMounted(ctx context.Context, parentID string) (string, error) {
	if err := access.ensureAttached(ctx, parentID, access.localNodeID); err != nil {
		return "", err
	}
	target := path.Join(access.parentRoot, parentID)
	// The attachment operation released its keyed lock after provider
	// readiness. A second key section serializes the kernel mount check itself.
	unlock, err := access.attachmentLocks.Lock(ctx, "mount\x00"+parentID)
	if err != nil {
		return "", err
	}
	defer unlock()
	table, err := access.mounter.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if _, err := table.Exact(target); err == nil {
		if _, err := mount.ValidateParent(table, target, parentID); err != nil {
			return "", err
		}
		return target, nil
	} else if err != nil && !errors.Is(err, mount.ErrNotMounted) {
		return "", err
	}
	if err := access.mounter.MountParent(ctx, parentID, target); err != nil {
		return "", err
	}
	table, err = access.mounter.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if _, err := mount.ValidateParent(table, target, parentID); err != nil {
		return "", err
	}
	return target, nil
}

// VerifiedMountedRoot returns only an already-mounted exact configured parent.
// Periodic observation uses this read-only path so a metadata refresh cannot
// silently become a provider attach or kernel mount repair.
func (access *controllerParentAccess) VerifiedMountedRoot(ctx context.Context, parentID string) (string, error) {
	if _, configured := access.configuredParents[parentID]; !configured {
		return "", fmt.Errorf("parent %q is not configured", parentID)
	}
	target := path.Join(access.parentRoot, parentID)
	unlock, err := access.attachmentLocks.Lock(ctx, "mount\x00"+parentID)
	if err != nil {
		return "", err
	}
	defer unlock()
	table, err := access.mounter.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	if _, err := mount.ValidateParent(table, target, parentID); err != nil {
		return "", err
	}
	return target, nil
}

// controllerReadOnlyParentAccess deliberately satisfies the parentfs mounted
// access boundary with verification only.  It exists for CSI operations, such
// as ValidateVolumeCapabilities, whose contract permits authoritative
// filesystem reads but forbids an implicit provider attach or kernel mount.
// Mutation-capable lifecycle adapters continue to use controllerParentAccess
// directly and therefore retain their normal repair behavior.
type controllerReadOnlyParentAccess struct {
	delegate *controllerParentAccess
}

func (access controllerReadOnlyParentAccess) EnsureMounted(ctx context.Context, parentID string) (string, error) {
	if access.delegate == nil {
		return "", fmt.Errorf("read-only controller parent access is nil")
	}
	return access.delegate.VerifiedMountedRoot(ctx, parentID)
}

var _ driver.AttachmentPublisher = (*controllerParentAccess)(nil)
