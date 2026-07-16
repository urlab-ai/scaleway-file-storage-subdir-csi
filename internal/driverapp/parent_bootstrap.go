package driverapp

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"scaleway-sfs-subdir-csi/internal/clock"
	internaluuid "scaleway-sfs-subdir-csi/internal/uuid"
	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/coordination"
	"scaleway-sfs-subdir-csi/pkg/parentfs"
	"scaleway-sfs-subdir-csi/pkg/safety"
	"scaleway-sfs-subdir-csi/pkg/scaleway"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

type parentBootstrapLeadership interface {
	Context() context.Context
	RequireActiveLeadership(ctx context.Context) error
	Snapshot() coordination.LeaseSnapshot
	SetBootstrapAttempt(ctx context.Context, attempt coordination.BootstrapAttempt) error
	ClearBootstrapAttempt(ctx context.Context, attemptID string) error
}

type parentBootstrapAccess interface {
	EnsureMounted(ctx context.Context, parentID string) (string, error)
}

type parentBootstrapEvidence interface {
	HasDurableReferences(ctx context.Context, parentID string) (bool, error)
}

type parentBootstrapFilesystem interface {
	Close() error
	InspectFreshRoot(ctx context.Context) error
	InspectUnclaimedRoot(ctx context.Context, attemptID string) (safety.BootstrapRootState, error)
	InspectClaimedBootstrapRoot(ctx context.Context, attemptID string) (safety.BootstrapRootState, error)
	ReadParentClaim(ctx context.Context) (volume.ParentOwnerRecord, bool, error)
	InstallParentClaim(ctx context.Context, attemptID string, claim volume.ParentOwnerRecord) error
	RemoveBootstrapTemporary(ctx context.Context, attemptID string) error
	EnsureLayout(ctx context.Context, basePath string) error
}

type parentBootstrapFilesystemFactory func(parentRoot string) (parentBootstrapFilesystem, error)

type configuredBootstrapParent struct {
	id       string
	basePath string
}

// parentBootstrapManager establishes every configured immutable parent claim
// before the controller serves CSI mutations. One cancellable gate covers all
// parents because the fixed leadership Lease stores at most one attempt.
type parentBootstrapManager struct {
	driverName          string
	installationID      string
	clusterUID          string
	controllerNamespace string
	helmReleaseName     string
	region              string
	projectID           string
	localNodeID         string
	localTarget         scaleway.Target
	parents             map[string]configuredBootstrapParent
	configuredParentIDs map[string]struct{}
	qualifiedTypes      map[string]struct{}
	leadership          parentBootstrapLeadership
	provider            scaleway.API
	authorizations      *controllerNodeAuthorizations
	access              parentBootstrapAccess
	evidence            parentBootstrapEvidence
	operationClock      clock.Clock
	ids                 internaluuid.Generator
	openFilesystem      parentBootstrapFilesystemFactory
	gate                chan struct{}

	// freshBootstrap is process-local evidence produced only by a complete
	// provisional discovery proof. It deliberately does not survive a restart:
	// after a crash, an attachment without a durable bootstrap journal must be
	// recovered through the operator-approved missing-Lease path.
	freshBootstrapMu sync.Mutex
	freshBootstrap   map[string]time.Time
}

func newParentBootstrapManager(
	configured config.Loaded,
	clusterUID, localNodeID string,
	leadership parentBootstrapLeadership,
	provider scaleway.API,
	authorizations *controllerNodeAuthorizations,
	access parentBootstrapAccess,
	evidence parentBootstrapEvidence,
	operationClock clock.Clock,
	ids internaluuid.Generator,
) (*parentBootstrapManager, error) {
	if leadership == nil || provider == nil || authorizations == nil || access == nil || evidence == nil || operationClock == nil || ids == nil {
		return nil, fmt.Errorf("parent bootstrap dependency is nil")
	}
	if err := volume.ValidateClusterUID(clusterUID); err != nil {
		return nil, err
	}
	target, err := scaleway.ParseNodeID(localNodeID)
	if err != nil {
		return nil, err
	}
	qualified, err := configured.Runtime.Compatibility.QualifiedCommercialTypeSet()
	if err != nil {
		return nil, err
	}
	parents := make(map[string]configuredBootstrapParent)
	configuredIDs := make(map[string]struct{})
	for _, configuredPool := range configured.Runtime.Pools {
		for _, parent := range configuredPool.Filesystems {
			if _, duplicate := parents[parent.ID]; duplicate {
				return nil, fmt.Errorf("parent %q appears in multiple bootstrap configurations", parent.ID)
			}
			parents[parent.ID] = configuredBootstrapParent{id: parent.ID, basePath: configuredPool.BasePath}
			configuredIDs[parent.ID] = struct{}{}
		}
	}
	if len(parents) == 0 {
		return nil, fmt.Errorf("parent bootstrap configuration is empty")
	}
	return &parentBootstrapManager{
		driverName: configured.Runtime.DriverName, installationID: configured.Runtime.Installation.ID,
		clusterUID: clusterUID, controllerNamespace: configured.ControllerNamespace,
		helmReleaseName: configured.HelmReleaseName,
		region:          configured.Runtime.Provider.Region, projectID: configured.Runtime.Provider.ProjectID,
		localNodeID: localNodeID, localTarget: target,
		parents: parents, configuredParentIDs: configuredIDs, qualifiedTypes: qualified,
		leadership: leadership, provider: provider, authorizations: authorizations, access: access,
		evidence:       evidence,
		operationClock: operationClock, ids: ids,
		openFilesystem: func(parentRoot string) (parentBootstrapFilesystem, error) {
			return parentfs.OpenBootstrapFilesystem(parentRoot)
		},
		gate: make(chan struct{}, 1), freshBootstrap: make(map[string]time.Time),
	}, nil
}

// EnsureAll resumes the journaled parent first, then processes every other
// configured parent in stable ID order. No CSI socket may be opened until this
// complete startup barrier succeeds.
func (manager *parentBootstrapManager) EnsureAll(ctx context.Context) error {
	if err := manager.lock(ctx); err != nil {
		return err
	}
	defer manager.unlock()
	attempt, present, err := coordination.ParseBootstrapAttempt(manager.leadership.Snapshot().Annotations)
	if err != nil {
		return fmt.Errorf("parse startup bootstrap journal: %w", err)
	}
	processed := ""
	if present {
		if _, configured := manager.parents[attempt.ParentFilesystemID]; !configured {
			return fmt.Errorf("active bootstrap attempt %q references unconfigured parent %q", attempt.AttemptID, attempt.ParentFilesystemID)
		}
		if err := manager.ensureClaimed(ctx, manager.parents[attempt.ParentFilesystemID]); err != nil {
			return err
		}
		processed = attempt.ParentFilesystemID
	}
	parentIDs := make([]string, 0, len(manager.parents))
	for parentID := range manager.parents {
		parentIDs = append(parentIDs, parentID)
	}
	slices.Sort(parentIDs)
	for _, parentID := range parentIDs {
		if parentID == processed {
			continue
		}
		if err := manager.ensureClaimed(ctx, manager.parents[parentID]); err != nil {
			return err
		}
	}
	return nil
}

// replaceLeadership transfers the same-process fresh-discovery manager to the
// promoted mutation session. It is a startup-only operation performed after
// the provisional renewal loop has joined and before bootstrap can run.
func (manager *parentBootstrapManager) replaceLeadership(leadership parentBootstrapLeadership) error {
	if leadership == nil {
		return fmt.Errorf("replacement parent bootstrap leadership is nil")
	}
	manager.leadership = leadership
	return nil
}

// DiscoverExistingReadOnly mounts every configured parent through the bounded
// provisional exception and validates only its immutable claim. It performs no
// journal, layout, ownership, or directory mutation. Missing-Lease recovery
// calls this before consuming operator approval and then performs complete
// startup inventory reconciliation only after mutation leadership is granted.
func (manager *parentBootstrapManager) DiscoverExistingReadOnly(ctx context.Context) error {
	parentIDs := make([]string, 0, len(manager.parents))
	for parentID := range manager.parents {
		parentIDs = append(parentIDs, parentID)
	}
	slices.Sort(parentIDs)
	for _, parentID := range parentIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		parent := manager.parents[parentID]
		root, err := manager.access.EnsureMounted(ctx, parent.id)
		if err != nil {
			return fmt.Errorf("attach and mount recovery parent %q: %w", parent.id, err)
		}
		if err := manager.validateReadOnlyClaim(ctx, parent, root); err != nil {
			return err
		}
	}
	return nil
}

func (manager *parentBootstrapManager) validateReadOnlyClaim(ctx context.Context, parent configuredBootstrapParent, root string) (returnErr error) {
	filesystem, err := manager.openFilesystem(root)
	if err != nil {
		return fmt.Errorf("open recovery parent %q filesystem: %w", parent.id, err)
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	claim, present, err := filesystem.ReadParentClaim(ctx)
	if err != nil {
		return fmt.Errorf("read recovery parent %q claim: %w", parent.id, err)
	}
	if !present {
		return fmt.Errorf("recovery parent %q has no immutable owner claim", parent.id)
	}
	if err := manager.validateClaim(claim, parent); err != nil {
		return fmt.Errorf("validate recovery parent %q claim: %w", parent.id, err)
	}
	return nil
}

// EnsureClaimed establishes or validates one configured parent under the same
// global bootstrap gate used by EnsureAll.
func (manager *parentBootstrapManager) EnsureClaimed(ctx context.Context, parentID string) error {
	if err := manager.lock(ctx); err != nil {
		return err
	}
	defer manager.unlock()
	parent, configured := manager.parents[parentID]
	if !configured {
		return fmt.Errorf("parent %q is not configured", parentID)
	}
	return manager.ensureClaimed(ctx, parent)
}

func (manager *parentBootstrapManager) ensureClaimed(ctx context.Context, parent configuredBootstrapParent) error {
	if err := manager.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	journaled, journalPresent, err := coordination.ParseBootstrapAttempt(manager.leadership.Snapshot().Annotations)
	if err != nil {
		return fmt.Errorf("parse parent bootstrap journal: %w", err)
	}
	if journalPresent {
		if journaled.ParentFilesystemID != parent.id {
			return fmt.Errorf("bootstrap attempt %q for parent %q must complete before parent %q", journaled.AttemptID, journaled.ParentFilesystemID, parent.id)
		}
		if err := manager.validateAttemptIdentity(journaled); err != nil {
			return err
		}
		hasReferences, err := manager.evidence.HasDurableReferences(ctx, parent.id)
		if err != nil {
			return fmt.Errorf("read bootstrap durable references for parent %q: %w", parent.id, err)
		}
		if hasReferences {
			return fmt.Errorf("bootstrap attempt %q cannot resume while parent %q has allocation or PersistentVolume references", journaled.AttemptID, parent.id)
		}
	}

	observation, err := manager.observeProvider(ctx, parent.id)
	if err != nil {
		return err
	}
	if journalPresent {
		if err := observation.requireCurrentAttemptOnly(manager.localTarget); err != nil {
			return fmt.Errorf("resume bootstrap attempt %q: %w", journaled.AttemptID, err)
		}
		// Exact replay is intentionally a Lease CAS, not a local no-op. It proves
		// this holder still owns the current resourceVersion before provider work.
		if err := manager.leadership.SetBootstrapAttempt(ctx, journaled); err != nil {
			return err
		}
		return manager.completeBootstrap(ctx, parent, journaled)
	}
	if observation.emptyFor(manager.localTarget) {
		hasReferences, err := manager.evidence.HasDurableReferences(ctx, parent.id)
		if err != nil {
			return fmt.Errorf("read parent %q durable references before bootstrap: %w", parent.id, err)
		}
		if hasReferences {
			// Empty provider attachment inventory does not imply an unclaimed
			// filesystem. Existing Kubernetes mappings require ordinary claim
			// validation and must never be overwritten by a new bootstrap claim.
			return manager.validateExistingClaim(ctx, parent)
		}
		return manager.beginBootstrap(ctx, parent, manager.operationClock.Now(), false)
	}
	if observedAt, authorized := manager.freshBootstrapObservation(parent.id); authorized {
		if err := observation.requireCurrentControllerOnly(manager.localTarget); err != nil {
			return fmt.Errorf("fresh-discovery bootstrap parent %q: %w", parent.id, err)
		}
		hasReferences, err := manager.evidence.HasDurableReferences(ctx, parent.id)
		if err != nil {
			return fmt.Errorf("read parent %q durable references after fresh discovery: %w", parent.id, err)
		}
		if hasReferences {
			return fmt.Errorf("fresh-discovery bootstrap parent %q acquired a durable Kubernetes reference", parent.id)
		}
		return manager.beginBootstrap(ctx, parent, observedAt, true)
	}
	return manager.validateExistingClaim(ctx, parent)
}

func (manager *parentBootstrapManager) beginBootstrap(ctx context.Context, parent configuredBootstrapParent, observedAt time.Time, consumeFresh bool) error {
	attemptID, err := manager.ids.New()
	if err != nil {
		return fmt.Errorf("generate parent bootstrap attempt ID: %w", err)
	}
	attempt, err := coordination.NewBootstrapAttempt(
		attemptID, manager.installationID, manager.clusterUID, parent.id,
		manager.localNodeID, manager.localTarget.ServerID, manager.localTarget.Zone,
		observedAt,
	)
	if err != nil {
		return err
	}
	if err := manager.leadership.SetBootstrapAttempt(ctx, attempt); err != nil {
		return err
	}
	if consumeFresh {
		manager.consumeFreshBootstrap(parent.id, observedAt)
	}
	return manager.completeBootstrap(ctx, parent, attempt)
}

// authorizeFreshBootstrap installs an all-parent, same-process handoff from
// provisional read-only discovery to the first mutating bootstrap journal.
// Partial discovery can never authorize a claim.
func (manager *parentBootstrapManager) authorizeFreshBootstrap(observed map[string]time.Time) error {
	if len(observed) != len(manager.parents) {
		return fmt.Errorf("fresh discovery observed %d parents, want %d", len(observed), len(manager.parents))
	}
	next := make(map[string]time.Time, len(observed))
	for parentID := range manager.parents {
		observedAt, present := observed[parentID]
		if !present || observedAt.IsZero() {
			return fmt.Errorf("fresh discovery has no empty-inventory observation for parent %q", parentID)
		}
		next[parentID] = observedAt
	}
	manager.freshBootstrapMu.Lock()
	manager.freshBootstrap = next
	manager.freshBootstrapMu.Unlock()
	return nil
}

func (manager *parentBootstrapManager) freshBootstrapObservation(parentID string) (time.Time, bool) {
	manager.freshBootstrapMu.Lock()
	defer manager.freshBootstrapMu.Unlock()
	observedAt, present := manager.freshBootstrap[parentID]
	return observedAt, present
}

func (manager *parentBootstrapManager) consumeFreshBootstrap(parentID string, observedAt time.Time) {
	manager.freshBootstrapMu.Lock()
	if current, present := manager.freshBootstrap[parentID]; present && current.Equal(observedAt) {
		delete(manager.freshBootstrap, parentID)
	}
	manager.freshBootstrapMu.Unlock()
}

func (manager *parentBootstrapManager) completeBootstrap(ctx context.Context, parent configuredBootstrapParent, attempt coordination.BootstrapAttempt) (returnErr error) {
	root, err := manager.access.EnsureMounted(ctx, parent.id)
	if err != nil {
		return fmt.Errorf("attach and mount bootstrap parent %q: %w", parent.id, err)
	}
	filesystem, err := manager.openFilesystem(root)
	if err != nil {
		return fmt.Errorf("open bootstrap parent %q filesystem: %w", parent.id, err)
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()

	claim, present, err := filesystem.ReadParentClaim(ctx)
	if err != nil {
		return err
	}
	if present {
		if err := manager.validateClaim(claim, parent); err != nil {
			return err
		}
		if claim.BootstrapAttemptID == attempt.AttemptID {
			expected, err := manager.claimForAttempt(parent, attempt)
			if err != nil {
				return err
			}
			if claim != expected {
				return fmt.Errorf("parent %q claim differs from exact bootstrap attempt %q", parent.id, attempt.AttemptID)
			}
			rootState, err := filesystem.InspectClaimedBootstrapRoot(ctx, attempt.AttemptID)
			if err != nil {
				return fmt.Errorf("inspect claimed bootstrap parent %q root: %w", parent.id, err)
			}
			if !rootState.ParentClaimPresent {
				return fmt.Errorf("claimed bootstrap parent %q root inspection did not observe the final claim", parent.id)
			}
			// The no-overwrite retry cleans an exact leftover temp without ever
			// replacing the already-authoritative final claim.
			if err := filesystem.InstallParentClaim(ctx, attempt.AttemptID, expected); err != nil {
				return err
			}
		}
	} else {
		if _, err := filesystem.InspectUnclaimedRoot(ctx, attempt.AttemptID); err != nil {
			return fmt.Errorf("inspect unclaimed parent %q root: %w", parent.id, err)
		}
		expected, err := manager.claimForAttempt(parent, attempt)
		if err != nil {
			return err
		}
		if err := filesystem.InstallParentClaim(ctx, attempt.AttemptID, expected); err != nil {
			return err
		}
		claim, present, err = filesystem.ReadParentClaim(ctx)
		if err != nil || !present || claim != expected {
			if err != nil {
				return err
			}
			return fmt.Errorf("parent %q installed claim readback differs from prepared generation", parent.id)
		}
	}
	if err := filesystem.RemoveBootstrapTemporary(ctx, attempt.AttemptID); err != nil {
		return err
	}
	if err := manager.leadership.ClearBootstrapAttempt(ctx, attempt.AttemptID); err != nil {
		return err
	}
	// Once the immutable claim is authoritative and the attempt journal is
	// durably cleared, ordinary startup can safely retry this layout after any
	// crash without reopening the first-claim adoption boundary.
	if err := filesystem.EnsureLayout(ctx, parent.basePath); err != nil {
		return fmt.Errorf("ensure claimed parent %q layout: %w", parent.id, err)
	}
	return nil
}

func (manager *parentBootstrapManager) validateExistingClaim(ctx context.Context, parent configuredBootstrapParent) (returnErr error) {
	root, err := manager.access.EnsureMounted(ctx, parent.id)
	if err != nil {
		return fmt.Errorf("attach and mount claimed parent %q: %w", parent.id, err)
	}
	filesystem, err := manager.openFilesystem(root)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, filesystem.Close()) }()
	claim, present, err := filesystem.ReadParentClaim(ctx)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("parent %q has provider attachments but no immutable owner claim", parent.id)
	}
	if err := manager.validateClaim(claim, parent); err != nil {
		return err
	}
	if err := filesystem.EnsureLayout(ctx, parent.basePath); err != nil {
		return fmt.Errorf("ensure claimed parent %q layout: %w", parent.id, err)
	}
	return nil
}

func (manager *parentBootstrapManager) claimForAttempt(parent configuredBootstrapParent, attempt coordination.BootstrapAttempt) (volume.ParentOwnerRecord, error) {
	basePathHash, err := volume.BasePathHash(parent.basePath)
	if err != nil {
		return volume.ParentOwnerRecord{}, err
	}
	return (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1,
		DriverName: manager.driverName, InstallationID: manager.installationID,
		ActiveClusterUID: manager.clusterUID, ParentFilesystemID: parent.id,
		BasePath: parent.basePath, BasePathHash: basePathHash,
		ControllerNamespace: manager.controllerNamespace, HelmReleaseName: manager.helmReleaseName,
		LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID:  attempt.AttemptID, CreatedAt: attempt.EmptyInventoryObservedAt,
	}).Seal()
}

func (manager *parentBootstrapManager) validateClaim(claim volume.ParentOwnerRecord, parent configuredBootstrapParent) error {
	if err := claim.Validate(); err != nil {
		return err
	}
	if claim.Revision != 1 || claim.DriverName != manager.driverName || claim.InstallationID != manager.installationID ||
		claim.ActiveClusterUID != manager.clusterUID || claim.ParentFilesystemID != parent.id ||
		claim.BasePath != parent.basePath || claim.ControllerNamespace != manager.controllerNamespace ||
		claim.HelmReleaseName != manager.helmReleaseName || claim.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("parent %q claim differs from active driver installation", parent.id)
	}
	return nil
}

func (manager *parentBootstrapManager) validateAttemptIdentity(attempt coordination.BootstrapAttempt) error {
	if err := attempt.Validate(); err != nil {
		return err
	}
	if attempt.InstallationID != manager.installationID || attempt.ActiveClusterUID != manager.clusterUID ||
		attempt.ControllerNodeID != manager.localNodeID || attempt.ControllerInstanceID != manager.localTarget.ServerID ||
		attempt.ControllerZone != manager.localTarget.Zone {
		return fmt.Errorf("bootstrap attempt %q belongs to another controller runtime identity", attempt.AttemptID)
	}
	return nil
}

type parentBootstrapProviderObservation struct {
	filesystem  scaleway.Filesystem
	inventory   scaleway.RegionalInventory
	server      scaleway.Server
	serverState map[string]scaleway.ServerFilesystemState
}

func (manager *parentBootstrapManager) observeProvider(ctx context.Context, parentID string) (parentBootstrapProviderObservation, error) {
	known, eligible, err := manager.authorizations.Refresh(ctx)
	if err != nil {
		return parentBootstrapProviderObservation{}, fmt.Errorf("refresh parent bootstrap node authorization: %w", err)
	}
	if target, ok := known[manager.localTarget.ServerID]; !ok || target != manager.localTarget {
		return parentBootstrapProviderObservation{}, fmt.Errorf("controller Instance %q is absent from known node evidence", manager.localTarget.ServerID)
	}
	if _, ok := eligible[manager.localTarget.ServerID]; !ok {
		return parentBootstrapProviderObservation{}, fmt.Errorf("controller Instance %q is not eligible for parent bootstrap", manager.localTarget.ServerID)
	}
	filesystem, err := manager.provider.GetFilesystem(ctx, manager.region, parentID)
	if err != nil {
		return parentBootstrapProviderObservation{}, fmt.Errorf("read bootstrap parent %q metadata: %w", parentID, err)
	}
	if filesystem.ID != parentID || filesystem.ProjectID != manager.projectID || filesystem.Region != manager.region {
		return parentBootstrapProviderObservation{}, fmt.Errorf("bootstrap parent %q provider identity differs from configured scope", parentID)
	}
	if err := filesystem.Status.PermitNewMutation(); err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	inventory, err := scaleway.ListRegionalInventory(ctx, manager.provider, filesystem)
	if err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	if err := scaleway.ValidateAuthorizedAttachments(inventory, known); err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	instanceIDs := make([]string, 0, len(known))
	for instanceID := range known {
		instanceIDs = append(instanceIDs, instanceID)
	}
	slices.Sort(instanceIDs)
	servers := make(map[string]scaleway.Server, len(instanceIDs))
	for _, instanceID := range instanceIDs {
		target := known[instanceID]
		observed, readErr := manager.provider.GetServer(ctx, target.Zone, target.ServerID)
		if readErr != nil {
			return parentBootstrapProviderObservation{}, fmt.Errorf("read known bootstrap Instance %q: %w", instanceID, readErr)
		}
		if observed.ID != target.ServerID || observed.Zone != target.Zone || observed.Region != manager.region || observed.ProjectID != manager.projectID {
			return parentBootstrapProviderObservation{}, fmt.Errorf("known bootstrap Instance %q identity differs from configured scope", instanceID)
		}
		if err := scaleway.ValidateExclusiveServerInventory(observed, manager.configuredParentIDs); err != nil {
			return parentBootstrapProviderObservation{}, err
		}
		servers[instanceID] = observed
	}
	// Bootstrap is an attachment mutation. It must use the same complete
	// regional-versus-Instance proof as the ordinary attach path rather than
	// checking only the controller Instance and overlooking disagreement on a
	// different known workload node.
	if err := scaleway.ValidateAttachmentInventoryAgreement(inventory, known, servers); err != nil {
		return parentBootstrapProviderObservation{}, fmt.Errorf("reconcile complete bootstrap attachment inventory: %w", err)
	}
	server := servers[manager.localTarget.ServerID]
	if err := server.State.PermitNewAttachment(); err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	if _, qualified := manager.qualifiedTypes[server.CommercialType]; !qualified {
		return parentBootstrapProviderObservation{}, fmt.Errorf("bootstrap controller commercial type %q is not release-qualified", server.CommercialType)
	}
	if err := scaleway.ValidatePostAttachBudget(server, manager.configuredParentIDs); err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	serverState, err := scaleway.ServerAttachmentMap(server)
	if err != nil {
		return parentBootstrapProviderObservation{}, err
	}
	return parentBootstrapProviderObservation{filesystem: filesystem, inventory: inventory, server: server, serverState: serverState}, nil
}

func (observation parentBootstrapProviderObservation) emptyFor(target scaleway.Target) bool {
	_, onServer := observation.serverState[observation.filesystem.ID]
	return len(observation.inventory.Attachments) == 0 && !onServer && observation.server.ID == target.ServerID && observation.server.Zone == target.Zone
}

func (observation parentBootstrapProviderObservation) requireCurrentAttemptOnly(target scaleway.Target) error {
	state, onServer := observation.serverState[observation.filesystem.ID]
	if len(observation.inventory.Attachments) == 0 {
		if onServer {
			return fmt.Errorf("controller Instance reports the parent but regional inventory is empty")
		}
		return nil
	}
	if len(observation.inventory.Attachments) != 1 {
		return fmt.Errorf("provider inventory contains %d attachments, want zero or the exact current controller attachment", len(observation.inventory.Attachments))
	}
	attachment := observation.inventory.Attachments[0]
	if attachment.ResourceID != target.ServerID || attachment.Zone != target.Zone || !onServer {
		return fmt.Errorf("provider inventory contains an attachment outside the journaled controller Instance")
	}
	if state != scaleway.ServerFilesystemAttaching && state != scaleway.ServerFilesystemAvailable {
		return fmt.Errorf("journaled controller attachment is in unsafe state %q", state)
	}
	return nil
}

func (observation parentBootstrapProviderObservation) requireCurrentControllerOnly(target scaleway.Target) error {
	state, onServer := observation.serverState[observation.filesystem.ID]
	if len(observation.inventory.Attachments) != 1 {
		return fmt.Errorf("provider inventory contains %d attachments, want the exact current controller attachment", len(observation.inventory.Attachments))
	}
	attachment := observation.inventory.Attachments[0]
	if attachment.ResourceID != target.ServerID || attachment.Zone != target.Zone || !onServer {
		return fmt.Errorf("provider inventory is not attached exclusively to the current controller Instance")
	}
	if state != scaleway.ServerFilesystemAvailable {
		return fmt.Errorf("current controller attachment is not available: state %q", state)
	}
	return nil
}

func (manager *parentBootstrapManager) lock(ctx context.Context) error {
	if err := manager.leadership.RequireActiveLeadership(ctx); err != nil {
		return err
	}
	select {
	case manager.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-manager.leadership.Context().Done():
		return coordination.ErrLeadershipNotActive
	}
	if err := manager.leadership.RequireActiveLeadership(ctx); err != nil {
		manager.unlock()
		return err
	}
	return nil
}

func (manager *parentBootstrapManager) unlock() { <-manager.gate }
