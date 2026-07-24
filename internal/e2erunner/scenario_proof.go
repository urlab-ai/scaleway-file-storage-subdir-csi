package e2erunner

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// VirtioFSMountProof records the real parent mount and immutable claim that
// cannot be established by a fake mounter. The same workload marker must remain
// readable after a normal singleton-controller replacement.
type VirtioFSMountProof struct {
	SchemaVersion          string                   `json:"schemaVersion"`
	Scenario               string                   `json:"scenario"`
	RunID                  string                   `json:"runId"`
	ObservedAt             string                   `json:"observedAt"`
	ClaimName              string                   `json:"claimName"`
	PersistentVolumeName   string                   `json:"persistentVolumeName"`
	VolumeHandle           string                   `json:"volumeHandle"`
	ParentFilesystemID     string                   `json:"parentFilesystemId"`
	ControllerMountPath    string                   `json:"controllerMountPath"`
	FilesystemType         string                   `json:"filesystemType"`
	ControllerPodUIDBefore string                   `json:"controllerPodUidBefore"`
	ControllerPodUIDAfter  string                   `json:"controllerPodUidAfter"`
	ParentClaim            volume.ParentOwnerRecord `json:"parentClaim"`
	StatFSSucceeded        bool                     `json:"statfsSucceeded"`
	MarkerReadBefore       bool                     `json:"markerReadBefore"`
	MarkerReadAfter        bool                     `json:"markerReadAfter"`
}

// SingleNodeWriterProof records the complete live handoff boundary for one
// SINGLE_NODE_WRITER volume. The first publication must remain the only
// durable fence while a second node is rejected, and normal unpublish must
// then permit an exact handoff to the second node.
type SingleNodeWriterProof struct {
	SchemaVersion                string   `json:"schemaVersion"`
	Scenario                     string   `json:"scenario"`
	RunID                        string   `json:"runId"`
	ObservedAt                   string   `json:"observedAt"`
	ClaimName                    string   `json:"claimName"`
	PersistentVolumeName         string   `json:"persistentVolumeName"`
	FirstPodName                 string   `json:"firstPodName"`
	SecondPodName                string   `json:"secondPodName"`
	FirstNodeName                string   `json:"firstNodeName"`
	SecondNodeName               string   `json:"secondNodeName"`
	FirstNodeID                  string   `json:"firstNodeId"`
	SecondNodeID                 string   `json:"secondNodeId"`
	FirstPodReady                bool     `json:"firstPodReady"`
	ConflictObserved             bool     `json:"conflictObserved"`
	RejectionEventCount          int      `json:"rejectionEventCount"`
	PublishedNodesDuringConflict []string `json:"publishedNodesDuringConflict"`
	SecondReadyAfterHandoff      bool     `json:"secondReadyAfterHandoff"`
	PublishedNodesAfterHandoff   []string `json:"publishedNodesAfterHandoff"`
	ReadWriteAfterHandoff        bool     `json:"readWriteAfterHandoff"`
}

// HundredPVCScaleProof records the bounded release-scale workload. Counts are
// exact so unrelated run-labelled claims or partial success cannot be hidden.
type HundredPVCScaleProof struct {
	SchemaVersion             string   `json:"schemaVersion"`
	Scenario                  string   `json:"scenario"`
	RunID                     string   `json:"runId"`
	ObservedAt                string   `json:"observedAt"`
	PVCCount                  int      `json:"pvcCount"`
	BoundPVCCount             int      `json:"boundPvcCount"`
	PVCNames                  []string `json:"pvcNames"`
	SingleParentFilesystemID  string   `json:"singleParentFilesystemId"`
	SameNodeName              string   `json:"sameNodeName"`
	MaxFileSystems            int      `json:"maxFileSystems"`
	SameNodeLogicalMounts     int      `json:"sameNodeLogicalMounts"`
	SameNodeClaimNames        []string `json:"sameNodeClaimNames"`
	IsolatedMarkerCount       int      `json:"isolatedMarkerCount"`
	SameNodeID                string   `json:"sameNodeId"`
	RegionalAttachmentCount   int      `json:"regionalAttachmentCount"`
	ServerFilesystemCount     int      `json:"serverFilesystemCount"`
	NodeMaxVolumesOmitted     bool     `json:"nodeMaxVolumesOmitted"`
	SampledPVCCount           int      `json:"sampledPvcCount"`
	SampledClaimNames         []string `json:"sampledClaimNames"`
	SampledReaderNodeName     string   `json:"sampledReaderNodeName"`
	SampledReaderNodeID       string   `json:"sampledReaderNodeId"`
	SuccessfulWriterCount     int      `json:"successfulWriterCount"`
	SuccessfulReaderCount     int      `json:"successfulReaderCount"`
	ReadOnlyWriteRejected     bool     `json:"readOnlyWriteRejected"`
	NodePluginsCredentialFree bool     `json:"nodePluginsCredentialFree"`
	SoakDurationSeconds       int64    `json:"soakDurationSeconds"`
	SoakSuccessfulWrites      int      `json:"soakSuccessfulWrites"`
	SoakSuccessfulReads       int      `json:"soakSuccessfulReads"`
	SoakChecksumFailures      int      `json:"soakChecksumFailures"`
	SoakControllerUIDBefore   string   `json:"soakControllerUidBefore"`
	SoakControllerUIDAfter    string   `json:"soakControllerUidAfter"`
	SoakNodePluginUIDBefore   string   `json:"soakNodePluginUidBefore"`
	SoakNodePluginUIDAfter    string   `json:"soakNodePluginUidAfter"`
}

// ParentGrowthProof binds one exact product-size step to a fresh available
// provider observation and a subsequent allocation on that parent.
type ParentGrowthProof struct {
	SchemaVersion          string `json:"schemaVersion"`
	Scenario               string `json:"scenario"`
	RunID                  string `json:"runId"`
	ObservedAt             string `json:"observedAt"`
	FilesystemID           string `json:"filesystemId"`
	PreviousSizeBytes      uint64 `json:"previousSizeBytes"`
	RequestedSizeBytes     uint64 `json:"requestedSizeBytes"`
	ObservedSizeBytes      uint64 `json:"observedSizeBytes"`
	GrowthStepBytes        uint64 `json:"growthStepBytes"`
	ObservedStatus         string `json:"observedStatus"`
	ControllerPodUIDBefore string `json:"controllerPodUidBefore"`
	ControllerPodUIDAfter  string `json:"controllerPodUidAfter"`
	ProbePVC               string `json:"probePvc"`
	ProbeRequestName       string `json:"probeRequestName"`
	AllocationParentID     string `json:"allocationParentId"`
	FreshAvailableRead     bool   `json:"freshAvailableRead"`
}

// ParentDecommissionProof retains the validated csi-admin audit and proves the
// historical parent stayed detached and unconfigured after the remaining
// single-parent release restarted.
type ParentDecommissionProof struct {
	SchemaVersion             string                  `json:"schemaVersion"`
	Scenario                  string                  `json:"scenario"`
	RunID                     string                  `json:"runId"`
	ObservedAt                string                  `json:"observedAt"`
	RequestID                 string                  `json:"requestId"`
	ParentFilesystemID        string                  `json:"parentFilesystemId"`
	RemainingParentIDs        []string                `json:"remainingParentIds"`
	PreservedTombstoneIDs     []string                `json:"preservedTombstoneIds"`
	Audit                     admin.DecommissionAudit `json:"audit"`
	DryRunReady               bool                    `json:"dryRunReady"`
	ExecuteCompleted          bool                    `json:"executeCompleted"`
	RemovedParentUnconfigured bool                    `json:"removedParentUnconfigured"`
	ParentMountsAbsent        bool                    `json:"parentMountsAbsent"`
	ServerAttachmentAbsent    bool                    `json:"serverAttachmentAbsent"`
	RegionalAttachmentAbsent  bool                    `json:"regionalAttachmentAbsent"`
	ControllerReady           bool                    `json:"controllerReady"`
	NodePluginsReady          bool                    `json:"nodePluginsReady"`
}

// ControllerFailureProof records one abrupt controller-node stop followed by
// the exact abnormal-takeover fencing and approval protocol.
type ControllerFailureProof struct {
	SchemaVersion                   string   `json:"schemaVersion"`
	Scenario                        string   `json:"scenario"`
	RunID                           string   `json:"runId"`
	ObservedAt                      string   `json:"observedAt"`
	LeaseUID                        string   `json:"leaseUid"`
	OldPodUID                       string   `json:"oldPodUid"`
	NewPodUID                       string   `json:"newPodUid"`
	OldNodeName                     string   `json:"oldNodeName"`
	NewNodeName                     string   `json:"newNodeName"`
	OldNodeID                       string   `json:"oldNodeId"`
	NewNodeID                       string   `json:"newNodeId"`
	ParentFilesystemIDs             []string `json:"parentFilesystemIds"`
	ApprovalSecretUID               string   `json:"approvalSecretUid"`
	ApprovalRequestID               string   `json:"approvalRequestId"`
	OperatorSteps                   []string `json:"operatorSteps"`
	RecoverySeconds                 int64    `json:"recoverySeconds"`
	OldHolderMatched                bool     `json:"oldHolderMatched"`
	OldInstanceReachedStopped       bool     `json:"oldInstanceReachedStopped"`
	SuccessorBlockedBeforeApproval  bool     `json:"successorBlockedBeforeApproval"`
	ServerAttachmentsAbsent         bool     `json:"serverAttachmentsAbsent"`
	RegionalAttachmentsAbsent       bool     `json:"regionalAttachmentsAbsent"`
	ApprovalConsumed                bool     `json:"approvalConsumed"`
	ExistingVolumeReadWrite         bool     `json:"existingVolumeReadWrite"`
	NewPVCName                      string   `json:"newPvcName"`
	NewPVCBound                     bool     `json:"newPvcBound"`
	LeaseUIDPreserved               bool     `json:"leaseUidPreserved"`
	ControllerAvailable             bool     `json:"controllerAvailable"`
	ApprovalSecretDeletedAfterAudit bool     `json:"approvalSecretDeletedAfterAudit"`
}

// NodeDrainProof records workload rescheduling followed by a node-plugin
// restart on the replacement node. Both operations must preserve the marker
// written through the logical volume.
type NodeDrainProof struct {
	SchemaVersion            string `json:"schemaVersion"`
	Scenario                 string `json:"scenario"`
	RunID                    string `json:"runId"`
	ObservedAt               string `json:"observedAt"`
	ClaimName                string `json:"claimName"`
	DeploymentName           string `json:"deploymentName"`
	OriginalNodeName         string `json:"originalNodeName"`
	ReplacementNodeName      string `json:"replacementNodeName"`
	OriginalPodUID           string `json:"originalPodUid"`
	ReplacementPodUID        string `json:"replacementPodUid"`
	OldNodeDrained           bool   `json:"oldNodeDrained"`
	MarkerReadAfterDrain     bool   `json:"markerReadAfterDrain"`
	OldNodeUncordoned        bool   `json:"oldNodeUncordoned"`
	OldNodePluginUID         string `json:"oldNodePluginUid"`
	NewNodePluginUID         string `json:"newNodePluginUid"`
	MarkerReadAfterRestart   bool   `json:"markerReadAfterRestart"`
	ReplacedKapsuleNodeID    string `json:"replacedKapsuleNodeId"`
	ReplacementKapsuleID     string `json:"replacementKapsuleNodeId"`
	ReplacementKapsuleName   string `json:"replacementKapsuleNodeName"`
	ReplacementKapsuleNodeID string `json:"replacementKapsuleCsiNodeId"`
	CommercialType           string `json:"commercialType"`
	MaxFileSystems           int    `json:"maxFileSystems"`
	NodeConfigGeneration     string `json:"nodeConfigGeneration"`
	ReplacementReady         bool   `json:"replacementReady"`
	ReplacementPluginReady   bool   `json:"replacementPluginReady"`
	ReplacementRegistered    bool   `json:"replacementRegistered"`
	ReplacementCompatible    bool   `json:"replacementCompatible"`
	MarkerReadOnReplacement  bool   `json:"markerReadOnReplacement"`
}

// ProviderAttachDetachProof records the complete regional attachment snapshot
// plus one deliberate foreign attachment to the run-owned disposable Instance.
// The driver must fail closed while that Instance is outside the Kubernetes
// inventory and recover only after both provider surfaces prove detachment.
type ProviderAttachDetachProof struct {
	SchemaVersion    string                        `json:"schemaVersion"`
	Scenario         string                        `json:"scenario"`
	RunID            string                        `json:"runId"`
	ObservedAt       string                        `json:"observedAt"`
	PlannedNodeIDs   []string                      `json:"plannedNodeIds"`
	Parents          []ProviderParentProof         `json:"parents"`
	BootstrapRestart ProviderBootstrapRestartProof `json:"bootstrapRestart"`
	ForeignTest      ProviderForeignAttachProof    `json:"foreignTest"`
}

// ProviderParentProof retains the agreeing regional attachment identities for
// one exact run-owned parent.
type ProviderParentProof struct {
	FilesystemID        string   `json:"filesystemId"`
	FilesystemStatus    string   `json:"filesystemStatus"`
	ReportedAttachments uint32   `json:"reportedAttachments"`
	AttachmentIDs       []string `json:"attachmentIds"`
	ResourceIDs         []string `json:"resourceIds"`
	ResourceTypes       []string `json:"resourceTypes"`
	Zones               []string `json:"zones"`
}

// ProviderForeignAttachProof retains the exact mutation and fail-closed
// observations for the run-owned disposable Instance.
type ProviderForeignAttachProof struct {
	DisposableInstanceID       string   `json:"disposableInstanceId"`
	FilesystemIDs              []string `json:"filesystemIds"`
	AttachmentIDs              []string `json:"attachmentIds"`
	InitialAttachmentAbsent    bool     `json:"initialAttachmentAbsent"`
	AttachmentReachedAvailable bool     `json:"attachmentReachedAvailable"`
	PendingPVCName             string   `json:"pendingPvcName"`
	ProvisioningFailureSeen    bool     `json:"provisioningFailureSeen"`
	PVCRemainedUnbound         bool     `json:"pvcRemainedUnbound"`
	ServerAttachmentAbsent     bool     `json:"serverAttachmentAbsent"`
	RegionalAttachmentAbsent   bool     `json:"regionalAttachmentAbsent"`
	PVCBoundAfterDetach        bool     `json:"pvcBoundAfterDetach"`
}

// ProviderBootstrapRestartProof records a real fresh-parent Helm addition,
// immutable claim, and complete controller restart after bootstrap. The exact
// after-attach/before-claim crash window is deliberately covered by the
// deterministic parent-bootstrap tests rather than a timing-sensitive cloud
// signal race.
type ProviderBootstrapRestartProof struct {
	ParentFilesystemID                string `json:"parentFilesystemId"`
	LeaseUID                          string `json:"leaseUid"`
	BootstrapAttemptID                string `json:"bootstrapAttemptId"`
	ActiveClusterUID                  string `json:"activeClusterUid"`
	ClaimTempPath                     string `json:"claimTempPath"`
	ControllerPodUIDBeforeRestart     string `json:"controllerPodUidBeforeRestart"`
	ControllerPodUIDAfterRestart      string `json:"controllerPodUidAfterRestart"`
	ControllerNodeNameBeforeRestart   string `json:"controllerNodeNameBeforeRestart"`
	ControllerNodeNameAfterRestart    string `json:"controllerNodeNameAfterRestart"`
	ControllerNodeIDBeforeRestart     string `json:"controllerNodeIdBeforeRestart"`
	ControllerNodeIDAfterRestart      string `json:"controllerNodeIdAfterRestart"`
	FinalClaimInstallationID          string `json:"finalClaimInstallationId"`
	FinalClaimActiveClusterUID        string `json:"finalClaimActiveClusterUid"`
	FinalClaimParentFilesystemID      string `json:"finalClaimParentFilesystemId"`
	FinalClaimBootstrapAttemptID      string `json:"finalClaimBootstrapAttemptId"`
	InitialAttachmentAbsent           bool   `json:"initialAttachmentAbsent"`
	HelmUpgradeCompleted              bool   `json:"helmUpgradeCompleted"`
	JournalClearedBeforeRestart       bool   `json:"journalClearedBeforeRestart"`
	ClaimValidBeforeRestart           bool   `json:"claimValidBeforeRestart"`
	TemporaryClaimAbsentBeforeRestart bool   `json:"temporaryClaimAbsentBeforeRestart"`
	ControllerRestarted               bool   `json:"controllerRestarted"`
	FinalClaimUnchangedAfterRestart   bool   `json:"finalClaimUnchangedAfterRestart"`
	JournalClearedAfterRestart        bool   `json:"journalClearedAfterRestart"`
	TemporaryClaimAbsentAfterRestart  bool   `json:"temporaryClaimAbsentAfterRestart"`
	ServerAttachmentAvailable         bool   `json:"serverAttachmentAvailable"`
	RegionalAttachmentAvailable       bool   `json:"regionalAttachmentAvailable"`
}

// CheckpointRestoreProof records one complete same-cluster namespace restore.
// The workload claim and cluster-scoped PV survive outside the deleted driver
// namespace, while every pre-recovery Instance is replaced before the restored
// controller is allowed to serve.
type CheckpointRestoreProof struct {
	SchemaVersion                     string   `json:"schemaVersion"`
	Scenario                          string   `json:"scenario"`
	RunID                             string   `json:"runId"`
	ObservedAt                        string   `json:"observedAt"`
	CheckpointRequestID               string   `json:"checkpointRequestId"`
	ArchiveSHA256                     string   `json:"archiveSha256"`
	ArchiveBytes                      uint64   `json:"archiveBytes"`
	ManifestSHA256                    string   `json:"manifestSha256"`
	WorkloadNamespace                 string   `json:"workloadNamespace"`
	WorkloadClaimName                 string   `json:"workloadClaimName"`
	PersistentVolumeName              string   `json:"persistentVolumeName"`
	OldInstanceIDs                    []string `json:"oldInstanceIds"`
	ReplacementInstanceIDs            []string `json:"replacementInstanceIds"`
	PrepareCompleted                  bool     `json:"prepareCompleted"`
	ControllerQuiesced                bool     `json:"controllerQuiesced"`
	DriverNamespaceDeleted            bool     `json:"driverNamespaceDeleted"`
	DriverNamespaceRecreated          bool     `json:"driverNamespaceRecreated"`
	PersistentVolumePreserved         bool     `json:"persistentVolumePreserved"`
	RestoreDryRunCompleted            bool     `json:"restoreDryRunCompleted"`
	RestoreExecuteCompleted           bool     `json:"restoreExecuteCompleted"`
	CheckpointSecretImmutable         bool     `json:"checkpointSecretImmutable"`
	CheckpointSecretDeletedAfterAudit bool     `json:"checkpointSecretDeletedAfterAudit"`
	OldPoolScaledToZero               bool     `json:"oldPoolScaledToZero"`
	AllOldInstancesAbsent             bool     `json:"allOldInstancesAbsent"`
	PoolRestoredWithFreshInstances    bool     `json:"poolRestoredWithFreshInstances"`
	ExistingMarkerReadAfterRecovery   bool     `json:"existingMarkerReadAfterRecovery"`
	NewProvisioningSucceeded          bool     `json:"newProvisioningSucceeded"`
	ArchiveLifecycleVerified          bool     `json:"archiveLifecycleVerified"`
	DeleteLifecycleVerified           bool     `json:"deleteLifecycleVerified"`
	TombstoneInventoryVerified        bool     `json:"tombstoneInventoryVerified"`
	ExternalWorkloadCleanupCompleted  bool     `json:"externalWorkloadCleanupCompleted"`
}

// MissingLeaseRecoveryProof records the fail-closed provisional Lease and its
// one-time checkpoint-bound recovery approval. Negative cases that must not be
// exercised unsafely against another real cluster are retained from the
// deterministic production-adapter harness in the same run.
type MissingLeaseRecoveryProof struct {
	SchemaVersion                     string   `json:"schemaVersion"`
	Scenario                          string   `json:"scenario"`
	RunID                             string   `json:"runId"`
	ObservedAt                        string   `json:"observedAt"`
	CheckpointRequestID               string   `json:"checkpointRequestId"`
	CheckpointManifestSHA256          string   `json:"checkpointManifestSha256"`
	LeaseUID                          string   `json:"leaseUid"`
	ProvisionalControllerPodUID       string   `json:"provisionalControllerPodUid"`
	RecoveredControllerPodUID         string   `json:"recoveredControllerPodUid"`
	ApprovalSecretUID                 string   `json:"approvalSecretUid"`
	ApprovalRequestID                 string   `json:"approvalRequestId"`
	RecoveryFenceScope                string   `json:"recoveryFenceScope"`
	OldInstanceIDs                    []string `json:"oldInstanceIds"`
	ReplacementInstanceIDs            []string `json:"replacementInstanceIds"`
	LeaseAbsentBeforeController       bool     `json:"leaseAbsentBeforeController"`
	ProvisionalLeasePersisted         bool     `json:"provisionalLeasePersisted"`
	ControllerNonServingBeforeFence   bool     `json:"controllerNonServingBeforeFence"`
	NodeDaemonSetAbsentBeforeApproval bool     `json:"nodeDaemonSetAbsentBeforeApproval"`
	OldAttachmentsAbsent              bool     `json:"oldAttachmentsAbsent"`
	OnlyProvisionalAttachmentPresent  bool     `json:"onlyProvisionalAttachmentPresent"`
	ApprovalCreatedAfterCondition     bool     `json:"approvalCreatedAfterCondition"`
	ApprovalConsumed                  bool     `json:"approvalConsumed"`
	LeaseUIDPreserved                 bool     `json:"leaseUidPreserved"`
	ControllerServingAfterApproval    bool     `json:"controllerServingAfterApproval"`
	ApprovalSecretDeletedAfterAudit   bool     `json:"approvalSecretDeletedAfterAudit"`
	HistoricalHolderOnlyRejected      bool     `json:"historicalHolderOnlyRejected"`
	ExportInProgressRejected          bool     `json:"exportInProgressRejected"`
	StaleOwnershipRejected            bool     `json:"staleOwnershipRejected"`
	DifferentClusterUIDRejected       bool     `json:"differentClusterUidRejected"`
}

// ArtifactInstallProof records that the exact candidate is installed with the
// production security and coordination boundaries on every schedulable node.
type ArtifactInstallProof struct {
	SchemaVersion                  string `json:"schemaVersion"`
	Scenario                       string `json:"scenario"`
	RunID                          string `json:"runId"`
	ObservedAt                     string `json:"observedAt"`
	DriverName                     string `json:"driverName"`
	StorageClassName               string `json:"storageClassName"`
	LeaseUID                       string `json:"leaseUid"`
	ControllerPodUID               string `json:"controllerPodUid"`
	SchedulableLinuxNodes          int    `json:"schedulableLinuxNodes"`
	ReadyNodePluginPods            int    `json:"readyNodePluginPods"`
	RegisteredCSINodes             int    `json:"registeredCsiNodes"`
	NamespacePrivileged            bool   `json:"namespacePrivileged"`
	LeaseHolderExact               bool   `json:"leaseHolderExact"`
	HolderEvidenceComplete         bool   `json:"holderEvidenceComplete"`
	AllImagesImmutable             bool   `json:"allImagesImmutable"`
	ProductionSecurityContexts     bool   `json:"productionSecurityContexts"`
	ControllerCannotMutatePods     bool   `json:"controllerCannotMutatePods"`
	StorageClassNonDefault         bool   `json:"storageClassNonDefault"`
	NodeConfigurationGenerationSet bool   `json:"nodeConfigurationGenerationSet"`
}

// OfficialCSICoexistenceProof binds coexistence to Scaleway's exact official
// File Storage CSI identity and preinstalled idle node DaemonSet.
type OfficialCSICoexistenceProof struct {
	SchemaVersion              string `json:"schemaVersion"`
	Scenario                   string `json:"scenario"`
	RunID                      string `json:"runId"`
	ObservedAt                 string `json:"observedAt"`
	DriverName                 string `json:"driverName"`
	OfficialDriverName         string `json:"officialDriverName"`
	StorageClassName           string `json:"storageClassName"`
	OfficialStorageClassName   string `json:"officialStorageClassName"`
	SchedulableLinuxNodes      int    `json:"schedulableLinuxNodes"`
	ReadyOfficialNodePods      int    `json:"readyOfficialNodePods"`
	OfficialVolumesInUse       int    `json:"officialVolumesInUse"`
	DistinctCSIDrivers         bool   `json:"distinctCsiDrivers"`
	DistinctStorageClasses     bool   `json:"distinctStorageClasses"`
	BothStorageClassesPresent  bool   `json:"bothStorageClassesPresent"`
	NeitherStorageClassDefault bool   `json:"neitherStorageClassDefault"`
	NoReleaseObjectCollision   bool   `json:"noReleaseObjectCollision"`
}

// SafeUninstallProof records the completed structured uninstall audit and the
// subsequent Helm/namespace absence. It is emitted only after the run-owned
// workloads and every configured parent attachment have been removed.
type SafeUninstallProof struct {
	SchemaVersion            string   `json:"schemaVersion"`
	Scenario                 string   `json:"scenario"`
	RunID                    string   `json:"runId"`
	ObservedAt               string   `json:"observedAt"`
	RequestID                string   `json:"requestId"`
	LeaseUID                 string   `json:"leaseUid"`
	ParentFilesystemIDs      []string `json:"parentFilesystemIds"`
	CheckedNodeIDs           []string `json:"checkedNodeIds"`
	DryRunReady              bool     `json:"dryRunReady"`
	ExecuteCompleted         bool     `json:"executeCompleted"`
	AuditValidated           bool     `json:"auditValidated"`
	WorkloadsAndPVsRemoved   bool     `json:"workloadsAndPvsRemoved"`
	PublishedFencesCleared   bool     `json:"publishedFencesCleared"`
	NodeAndControllerStopped bool     `json:"nodeAndControllerStopped"`
	ParentAttachmentsAbsent  bool     `json:"parentAttachmentsAbsent"`
	HelmReleaseAbsent        bool     `json:"helmReleaseAbsent"`
	NamespaceAbsent          bool     `json:"namespaceAbsent"`
}

// NMinusOneUpgradeProof records a deliberately staggered online upgrade from
// the previous public chart. The candidate controller must remain unable to
// create or publish while old and candidate node generations coexist, while
// already mounted data remains readable through the N-1 node plugin.
type NMinusOneUpgradeProof struct {
	SchemaVersion                     string `json:"schemaVersion"`
	Scenario                          string `json:"scenario"`
	RunID                             string `json:"runId"`
	ObservedAt                        string `json:"observedAt"`
	PreviousDriverImage               string `json:"previousDriverImage"`
	CandidateDriverImage              string `json:"candidateDriverImage"`
	PreviousNodeConfigGeneration      string `json:"previousNodeConfigGeneration"`
	CandidateNodeConfigGeneration     string `json:"candidateNodeConfigGeneration"`
	SchedulableLinuxNodes             int    `json:"schedulableLinuxNodes"`
	PreviousPodsBeforeUpgrade         int    `json:"previousPodsBeforeUpgrade"`
	PreviousPodsDuringStagger         int    `json:"previousPodsDuringStagger"`
	CandidatePodsDuringStagger        int    `json:"candidatePodsDuringStagger"`
	CandidatePodsAfterConvergence     int    `json:"candidatePodsAfterConvergence"`
	UpgradePreflightAccepted          bool   `json:"upgradePreflightAccepted"`
	NewNodeOldControllerBlocked       bool   `json:"newNodeOldControllerBlocked"`
	InterruptedNodeRolloutRolledBack  bool   `json:"interruptedNodeRolloutRolledBack"`
	ProvisioningResumedAfterRollback  bool   `json:"provisioningResumedAfterRollback"`
	OldNodeNewControllerBlocked       bool   `json:"oldNodeNewControllerBlocked"`
	ExistingReadDuringStagger         bool   `json:"existingReadDuringStagger"`
	CreateBlockedDuringStagger        bool   `json:"createBlockedDuringStagger"`
	PublishBlockedDuringStagger       bool   `json:"publishBlockedDuringStagger"`
	ControllerPodReplaced             bool   `json:"controllerPodReplaced"`
	LeaseUIDPreserved                 bool   `json:"leaseUidPreserved"`
	ExistingVolumeHandlePreserved     bool   `json:"existingVolumeHandlePreserved"`
	AllocationIdentityPreserved       bool   `json:"allocationIdentityPreserved"`
	OwnershipIdentityPreserved        bool   `json:"ownershipIdentityPreserved"`
	NewPVCBoundAfterConvergence       bool   `json:"newPvcBoundAfterConvergence"`
	PublishSucceededAfterConvergence  bool   `json:"publishSucceededAfterConvergence"`
	ArchiveLifecycleVerified          bool   `json:"archiveLifecycleVerified"`
	RetainLifecycleVerified           bool   `json:"retainLifecycleVerified"`
	DeleteLifecycleVerified           bool   `json:"deleteLifecycleVerified"`
	SiblingDataPreserved              bool   `json:"siblingDataPreserved"`
	ProductionRollingStrategyRestored bool   `json:"productionRollingStrategyRestored"`
}

// ValidateAvailableScenarioProofs validates every structured proof type
// implemented by this runner. A future scenario may leave the development
// interlock only after a mandatory proof case exists here.
func ValidateAvailableScenarioProofs(scenarios []ScenarioResult) error {
	for _, scenario := range scenarios {
		switch scenario.Name {
		case "virtiofs-mount-api":
			var proof VirtioFSMountProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "artifact-and-install-preflight":
			var proof ArtifactInstallProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "single-node-writer-conflict":
			var proof SingleNodeWriterProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "one-hundred-pvc-scale":
			var proof HundredPVCScaleProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "controller-hard-failure":
			var proof ControllerFailureProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "node-drain-and-replacement":
			var proof NodeDrainProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "provider-attach-detach":
			var proof ProviderAttachDetachProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "parent-growth":
			var proof ParentGrowthProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "checkpoint-and-restore":
			var proof CheckpointRestoreProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "missing-lease-recovery":
			var proof MissingLeaseRecoveryProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "official-csi-coexistence":
			var proof OfficialCSICoexistenceProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "safe-uninstall":
			var proof SafeUninstallProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "parent-decommission":
			var proof ParentDecommissionProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		case "n-minus-one-upgrade":
			var proof NMinusOneUpgradeProof
			if len(scenario.Proof) == 0 {
				return fmt.Errorf("scenario %q has no semantic proof", scenario.Name)
			}
			if err := strictjson.Decode(scenario.Proof, &proof); err != nil {
				return fmt.Errorf("decode scenario %q proof: %w", scenario.Name, err)
			}
			if err := proof.Validate(); err != nil {
				return fmt.Errorf("validate scenario %q proof: %w", scenario.Name, err)
			}
		default:
			if slices.Contains(RequiredScenarios, scenario.Name) {
				return fmt.Errorf("release scenario %q has no structured proof validator", scenario.Name)
			}
		}
	}
	return nil
}

// Validate verifies the real virtiofs mount, immutable parent claim, and
// controller-replacement data-path proof.
func (proof VirtioFSMountProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "virtiofs-mount-api", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if !validKubernetesName(proof.ClaimName) || !validKubernetesName(proof.PersistentVolumeName) {
		return fmt.Errorf("virtiofs claim or persistent-volume identity is invalid")
	}
	if _, err := volume.ParseHandle(proof.VolumeHandle); err != nil {
		return fmt.Errorf("virtiofs volume handle: %w", err)
	}
	if err := volume.ValidateParentFilesystemID(proof.ParentFilesystemID); err != nil {
		return fmt.Errorf("virtiofs parent filesystem ID: %w", err)
	}
	wantMountPath := "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + proof.ParentFilesystemID
	if proof.ControllerMountPath != wantMountPath || proof.FilesystemType != "virtiofs" {
		return fmt.Errorf("virtiofs controller mount identity is not exact")
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDBefore); err != nil {
		return fmt.Errorf("virtiofs original controller Pod UID: %w", err)
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDAfter); err != nil {
		return fmt.Errorf("virtiofs replacement controller Pod UID: %w", err)
	}
	if proof.ControllerPodUIDBefore == proof.ControllerPodUIDAfter {
		return fmt.Errorf("virtiofs proof did not replace the controller Pod")
	}
	if err := proof.ParentClaim.Validate(); err != nil {
		return fmt.Errorf("virtiofs parent claim: %w", err)
	}
	if proof.ParentClaim.InstallationID != proof.RunID || proof.ParentClaim.ParentFilesystemID != proof.ParentFilesystemID ||
		proof.ParentClaim.LeadershipLeaseName != volume.LeadershipLeaseNameV1 {
		return fmt.Errorf("virtiofs parent claim identity differs from the run")
	}
	if !proof.StatFSSucceeded || !proof.MarkerReadBefore || !proof.MarkerReadAfter {
		return fmt.Errorf("virtiofs statfs or marker recovery proof is incomplete")
	}
	return nil
}

// Validate verifies the exact provider growth step and fresh post-restart
// placement observation.
func (proof ParentGrowthProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "parent-growth", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if err := volume.ValidateParentFilesystemID(proof.FilesystemID); err != nil {
		return fmt.Errorf("growth filesystem ID: %w", err)
	}
	if proof.GrowthStepBytes != 100_000_000_000 || proof.PreviousSizeBytes == 0 ||
		proof.RequestedSizeBytes != proof.PreviousSizeBytes+proof.GrowthStepBytes ||
		proof.ObservedSizeBytes != proof.RequestedSizeBytes || proof.ObservedStatus != "available" || !proof.FreshAvailableRead {
		return fmt.Errorf("growth size or fresh available observation is incomplete")
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDBefore); err != nil {
		return fmt.Errorf("growth original controller Pod UID: %w", err)
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDAfter); err != nil {
		return fmt.Errorf("growth replacement controller Pod UID: %w", err)
	}
	if proof.ControllerPodUIDBefore == proof.ControllerPodUIDAfter {
		return fmt.Errorf("growth proof did not replace the controller Pod")
	}
	if !validKubernetesName(proof.ProbePVC) || !strings.HasPrefix(proof.ProbeRequestName, "pvc-") {
		return fmt.Errorf("growth probe identity is invalid")
	}
	if err := volume.ValidateOperationID(strings.TrimPrefix(proof.ProbeRequestName, "pvc-")); err != nil {
		return fmt.Errorf("growth probe request identity: %w", err)
	}
	if proof.AllocationParentID != proof.FilesystemID {
		return fmt.Errorf("growth probe allocation did not use the grown parent")
	}
	return nil
}

// Validate verifies the exact decommission audit and the restarted
// single-parent release boundary.
func (proof ParentDecommissionProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "parent-decommission", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(proof.RequestID); err != nil {
		return fmt.Errorf("decommission request ID: %w", err)
	}
	if err := volume.ValidateParentFilesystemID(proof.ParentFilesystemID); err != nil {
		return fmt.Errorf("decommission parent filesystem ID: %w", err)
	}
	if proof.Audit.RequestID != proof.RequestID || proof.Audit.ParentFilesystemID != proof.ParentFilesystemID {
		return fmt.Errorf("decommission audit identity differs from the proof")
	}
	if err := proof.Audit.Validate(); err != nil {
		return fmt.Errorf("decommission audit: %w", err)
	}
	if len(proof.RemainingParentIDs) != 1 || proof.RemainingParentIDs[0] == proof.ParentFilesystemID {
		return fmt.Errorf("decommission remaining parent inventory is not exact")
	}
	if err := volume.ValidateParentFilesystemID(proof.RemainingParentIDs[0]); err != nil {
		return fmt.Errorf("decommission remaining parent ID: %w", err)
	}
	if len(proof.PreservedTombstoneIDs) == 0 || !slices.IsSorted(proof.PreservedTombstoneIDs) {
		return fmt.Errorf("decommission preserved tombstone inventory is empty or unsorted")
	}
	for index, logicalID := range proof.PreservedTombstoneIDs {
		if err := volume.ValidateLogicalVolumeID(logicalID); err != nil {
			return fmt.Errorf("decommission tombstone ID: %w", err)
		}
		if index > 0 && logicalID == proof.PreservedTombstoneIDs[index-1] {
			return fmt.Errorf("decommission tombstone ID %q is duplicated", logicalID)
		}
	}
	if !proof.DryRunReady || !proof.ExecuteCompleted || !proof.RemovedParentUnconfigured || !proof.ParentMountsAbsent ||
		!proof.ServerAttachmentAbsent || !proof.RegionalAttachmentAbsent || !proof.ControllerReady || !proof.NodePluginsReady {
		return fmt.Errorf("decommission post-restart proof is incomplete")
	}
	return nil
}

// Validate verifies the complete namespace-loss checkpoint cycle and its
// post-recovery data/lifecycle evidence.
func (proof CheckpointRestoreProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "checkpoint-and-restore", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if err := volume.ValidateOperationID(proof.CheckpointRequestID); err != nil {
		return fmt.Errorf("checkpoint request ID: %w", err)
	}
	if !validDigest(proof.ArchiveSHA256) || !validDigest(proof.ManifestSHA256) || proof.ArchiveBytes == 0 || proof.ArchiveBytes > 1<<30 {
		return fmt.Errorf("checkpoint archive evidence is invalid")
	}
	for label, value := range map[string]string{
		"workload namespace": proof.WorkloadNamespace, "workload claim": proof.WorkloadClaimName,
		"persistent volume": proof.PersistentVolumeName,
	} {
		if !validKubernetesName(value) {
			return fmt.Errorf("%s is invalid", label)
		}
	}
	if err := validateInstanceReplacementSets(proof.OldInstanceIDs, proof.ReplacementInstanceIDs); err != nil {
		return fmt.Errorf("checkpoint worker replacement: %w", err)
	}
	if !proof.PrepareCompleted || !proof.ControllerQuiesced || !proof.DriverNamespaceDeleted ||
		!proof.DriverNamespaceRecreated || !proof.PersistentVolumePreserved || !proof.RestoreDryRunCompleted ||
		!proof.RestoreExecuteCompleted || !proof.CheckpointSecretImmutable || !proof.CheckpointSecretDeletedAfterAudit || !proof.OldPoolScaledToZero ||
		!proof.AllOldInstancesAbsent || !proof.PoolRestoredWithFreshInstances || !proof.ExistingMarkerReadAfterRecovery ||
		!proof.NewProvisioningSucceeded || !proof.ArchiveLifecycleVerified || !proof.DeleteLifecycleVerified ||
		!proof.TombstoneInventoryVerified || !proof.ExternalWorkloadCleanupCompleted {
		return fmt.Errorf("checkpoint restore proof is incomplete")
	}
	return nil
}

// Validate verifies the provisional non-serving state and exact one-time
// missing-Lease approval, including the deterministic unsupported-takeover
// negative cases.
func (proof MissingLeaseRecoveryProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "missing-lease-recovery", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"checkpoint request ID": proof.CheckpointRequestID, "Lease UID": proof.LeaseUID,
		"provisional controller Pod UID": proof.ProvisionalControllerPodUID,
		"recovered controller Pod UID":   proof.RecoveredControllerPodUID,
		"approval Secret UID":            proof.ApprovalSecretUID,
		"approval request ID":            proof.ApprovalRequestID,
	} {
		if err := volume.ValidateOperationID(value); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
	}
	if proof.ProvisionalControllerPodUID != proof.RecoveredControllerPodUID ||
		!validDigest(proof.CheckpointManifestSHA256) ||
		proof.RecoveryFenceScope != coordination.RecoveryFenceAllPreRecoveryInstances {
		return fmt.Errorf("missing-Lease identity or checkpoint binding is invalid")
	}
	if err := validateInstanceReplacementSets(proof.OldInstanceIDs, proof.ReplacementInstanceIDs); err != nil {
		return fmt.Errorf("missing-Lease worker replacement: %w", err)
	}
	if !proof.LeaseAbsentBeforeController || !proof.ProvisionalLeasePersisted || !proof.ControllerNonServingBeforeFence ||
		!proof.NodeDaemonSetAbsentBeforeApproval || !proof.OldAttachmentsAbsent || !proof.OnlyProvisionalAttachmentPresent ||
		!proof.ApprovalCreatedAfterCondition || !proof.ApprovalConsumed || !proof.LeaseUIDPreserved ||
		!proof.ControllerServingAfterApproval || !proof.ApprovalSecretDeletedAfterAudit ||
		!proof.HistoricalHolderOnlyRejected || !proof.ExportInProgressRejected || !proof.StaleOwnershipRejected ||
		!proof.DifferentClusterUIDRejected {
		return fmt.Errorf("missing-Lease recovery proof is incomplete")
	}
	return nil
}

func validateInstanceReplacementSets(oldIDs, replacementIDs []string) error {
	if len(oldIDs) < 2 || len(oldIDs) > 3 || len(replacementIDs) != len(oldIDs) {
		return fmt.Errorf("worker Instance sets must contain the exact two or three planned nodes")
	}
	seen := make(map[string]string, len(oldIDs)+len(replacementIDs))
	for set, values := range map[string][]string{"old": oldIDs, "replacement": replacementIDs} {
		for _, value := range values {
			if err := volume.ValidateOperationID(value); err != nil {
				return fmt.Errorf("%s Instance ID: %w", set, err)
			}
			if previous, duplicate := seen[value]; duplicate {
				return fmt.Errorf("instance ID %q appears in both or twice in %s/%s sets", value, previous, set)
			}
			seen[value] = set
		}
	}
	return nil
}

// Validate verifies the complete mixed-version barrier and post-convergence
// lifecycle compatibility evidence.
func (proof NMinusOneUpgradeProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "n-minus-one-upgrade", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if !immutableImageReference(proof.PreviousDriverImage) || !immutableImageReference(proof.CandidateDriverImage) ||
		proof.PreviousDriverImage == proof.CandidateDriverImage {
		return fmt.Errorf("N-1 upgrade images are not distinct immutable references")
	}
	if !validDigest("sha256:"+proof.PreviousNodeConfigGeneration) || !validDigest("sha256:"+proof.CandidateNodeConfigGeneration) ||
		proof.PreviousNodeConfigGeneration == proof.CandidateNodeConfigGeneration {
		return fmt.Errorf("N-1 upgrade node configuration generations are invalid or equal")
	}
	if proof.SchedulableLinuxNodes < 2 || proof.PreviousPodsBeforeUpgrade != proof.SchedulableLinuxNodes ||
		proof.PreviousPodsDuringStagger < 1 || proof.CandidatePodsDuringStagger < 1 ||
		proof.PreviousPodsDuringStagger+proof.CandidatePodsDuringStagger != proof.SchedulableLinuxNodes ||
		proof.CandidatePodsAfterConvergence != proof.SchedulableLinuxNodes {
		return fmt.Errorf("N-1 upgrade does not prove a complete staggered node rollout")
	}
	if !proof.UpgradePreflightAccepted || !proof.NewNodeOldControllerBlocked || !proof.InterruptedNodeRolloutRolledBack ||
		!proof.ProvisioningResumedAfterRollback || !proof.OldNodeNewControllerBlocked ||
		!proof.ExistingReadDuringStagger || !proof.CreateBlockedDuringStagger ||
		!proof.PublishBlockedDuringStagger || !proof.ControllerPodReplaced || !proof.LeaseUIDPreserved ||
		!proof.ExistingVolumeHandlePreserved || !proof.AllocationIdentityPreserved || !proof.OwnershipIdentityPreserved ||
		!proof.NewPVCBoundAfterConvergence || !proof.PublishSucceededAfterConvergence ||
		!proof.ArchiveLifecycleVerified || !proof.RetainLifecycleVerified || !proof.DeleteLifecycleVerified ||
		!proof.SiblingDataPreserved || !proof.ProductionRollingStrategyRestored {
		return fmt.Errorf("N-1 upgrade semantic proof is incomplete")
	}
	return nil
}

// Validate verifies the live production installation boundary.
func (proof ArtifactInstallProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "artifact-and-install-preflight", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if err := volume.ValidateDriverName(proof.DriverName); err != nil {
		return err
	}
	if !validKubernetesName(proof.StorageClassName) {
		return fmt.Errorf("artifact install StorageClass name is invalid")
	}
	if err := volume.ValidateOperationID(proof.LeaseUID); err != nil {
		return fmt.Errorf("artifact install Lease UID: %w", err)
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUID); err != nil {
		return fmt.Errorf("artifact install controller Pod UID: %w", err)
	}
	if proof.SchedulableLinuxNodes < 2 || proof.ReadyNodePluginPods != proof.SchedulableLinuxNodes ||
		proof.RegisteredCSINodes != proof.SchedulableLinuxNodes {
		return fmt.Errorf("artifact install does not cover every schedulable Linux node")
	}
	if !proof.NamespacePrivileged || !proof.LeaseHolderExact || !proof.HolderEvidenceComplete ||
		!proof.AllImagesImmutable || !proof.ProductionSecurityContexts || !proof.ControllerCannotMutatePods ||
		!proof.StorageClassNonDefault || !proof.NodeConfigurationGenerationSet {
		return fmt.Errorf("artifact install production boundary is incomplete")
	}
	return nil
}

// Validate verifies exact coexistence with the managed Scaleway File Storage
// CSI while it is installed but idle.
func (proof OfficialCSICoexistenceProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "official-csi-coexistence", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if proof.OfficialDriverName != "filestorage.csi.scaleway.com" || proof.DriverName == proof.OfficialDriverName ||
		proof.OfficialStorageClassName != "sfs-standard" || proof.StorageClassName != "sfs-subdir-rwx" {
		return fmt.Errorf("official CSI coexistence identities are invalid")
	}
	if proof.SchedulableLinuxNodes < 2 || proof.ReadyOfficialNodePods != proof.SchedulableLinuxNodes || proof.OfficialVolumesInUse != 0 {
		return fmt.Errorf("official CSI is not installed and idle on every node")
	}
	if !proof.DistinctCSIDrivers || !proof.DistinctStorageClasses || !proof.BothStorageClassesPresent ||
		!proof.NeitherStorageClassDefault || !proof.NoReleaseObjectCollision {
		return fmt.Errorf("official CSI coexistence proof is incomplete")
	}
	return nil
}

// Validate verifies a complete structured safe-uninstall result.
func (proof SafeUninstallProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "safe-uninstall", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if proof.RequestID != proof.RunID {
		return fmt.Errorf("safe-uninstall request differs from the run ID")
	}
	if err := volume.ValidateOperationID(proof.LeaseUID); err != nil {
		return fmt.Errorf("safe-uninstall Lease UID: %w", err)
	}
	if len(proof.ParentFilesystemIDs) < 1 || len(proof.ParentFilesystemIDs) > 2 || len(proof.CheckedNodeIDs) < 1 {
		return fmt.Errorf("safe-uninstall parent or node inventory is incomplete")
	}
	seenParents := make(map[string]struct{}, len(proof.ParentFilesystemIDs))
	for _, parentID := range proof.ParentFilesystemIDs {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return fmt.Errorf("safe-uninstall parent ID: %w", err)
		}
		if _, duplicate := seenParents[parentID]; duplicate {
			return fmt.Errorf("safe-uninstall parent ID %q is duplicated", parentID)
		}
		seenParents[parentID] = struct{}{}
	}
	seenNodes := make(map[string]struct{}, len(proof.CheckedNodeIDs))
	for _, nodeID := range proof.CheckedNodeIDs {
		if _, err := scaleway.ParseNodeID(nodeID); err != nil {
			return fmt.Errorf("safe-uninstall checked node ID: %w", err)
		}
		if _, duplicate := seenNodes[nodeID]; duplicate {
			return fmt.Errorf("safe-uninstall checked node ID %q is duplicated", nodeID)
		}
		seenNodes[nodeID] = struct{}{}
	}
	if !proof.DryRunReady || !proof.ExecuteCompleted || !proof.AuditValidated || !proof.WorkloadsAndPVsRemoved ||
		!proof.PublishedFencesCleared || !proof.NodeAndControllerStopped || !proof.ParentAttachmentsAbsent ||
		!proof.HelmReleaseAbsent || !proof.NamespaceAbsent {
		return fmt.Errorf("safe-uninstall proof is incomplete")
	}
	return nil
}

var controllerFailureOperatorSteps = []string{
	"stop-old-controller-instance",
	"cordon-old-kubernetes-node",
	"force-delete-old-controller-pod",
	"verify-successor-blocked-by-uncleared-lease",
	"detach-exact-parents-and-verify-dual-absence",
	"replace-stopped-kapsule-node",
	"create-immutable-abnormal-takeover-approval",
	"verify-approval-consumption-and-controller-recovery",
	"delete-consumed-approval-secret",
}

// Validate verifies the abrupt Instance stop, fail-closed successor, exact
// provider fence, immutable approval consumption, and recovered data path.
func (proof ControllerFailureProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "controller-hard-failure", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"Lease UID": proof.LeaseUID, "old Pod UID": proof.OldPodUID, "new Pod UID": proof.NewPodUID,
		"approval Secret UID": proof.ApprovalSecretUID, "approval request ID": proof.ApprovalRequestID,
	} {
		if err := volume.ValidateOperationID(value); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
	}
	if proof.OldPodUID == proof.NewPodUID || !validKubernetesName(proof.OldNodeName) ||
		!validKubernetesName(proof.NewNodeName) || proof.OldNodeName == proof.NewNodeName ||
		!validKubernetesName(proof.NewPVCName) {
		return fmt.Errorf("controller replacement identities are invalid")
	}
	if _, err := scaleway.ParseNodeID(proof.OldNodeID); err != nil {
		return fmt.Errorf("old controller node ID: %w", err)
	}
	if _, err := scaleway.ParseNodeID(proof.NewNodeID); err != nil {
		return fmt.Errorf("new controller node ID: %w", err)
	}
	if proof.OldNodeID == proof.NewNodeID || len(proof.ParentFilesystemIDs) != 2 {
		return fmt.Errorf("controller failure provider scope is incomplete")
	}
	seenParents := make(map[string]struct{}, len(proof.ParentFilesystemIDs))
	for _, parentID := range proof.ParentFilesystemIDs {
		if err := volume.ValidateParentFilesystemID(parentID); err != nil {
			return fmt.Errorf("controller failure parent ID: %w", err)
		}
		if _, duplicate := seenParents[parentID]; duplicate {
			return fmt.Errorf("controller failure repeats parent %q", parentID)
		}
		seenParents[parentID] = struct{}{}
	}
	if !slices.Equal(proof.OperatorSteps, controllerFailureOperatorSteps) || proof.RecoverySeconds <= 0 || proof.RecoverySeconds > 3600 {
		return fmt.Errorf("controller failure operator audit is incomplete")
	}
	if !proof.OldHolderMatched || !proof.OldInstanceReachedStopped || !proof.SuccessorBlockedBeforeApproval ||
		!proof.ServerAttachmentsAbsent || !proof.RegionalAttachmentsAbsent || !proof.ApprovalConsumed ||
		!proof.ExistingVolumeReadWrite || !proof.NewPVCBound || !proof.LeaseUIDPreserved ||
		!proof.ControllerAvailable || !proof.ApprovalSecretDeletedAfterAudit {
		return fmt.Errorf("controller hard-failure proof is incomplete")
	}
	return nil
}

// Validate verifies workload movement and node-plugin restart evidence.
func (proof NodeDrainProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "node-drain-and-replacement", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"claim": proof.ClaimName, "deployment": proof.DeploymentName,
		"original node": proof.OriginalNodeName, "replacement node": proof.ReplacementNodeName,
	} {
		if !validKubernetesName(value) {
			return fmt.Errorf("%s identity is invalid", label)
		}
	}
	for label, value := range map[string]string{
		"original Pod UID": proof.OriginalPodUID, "replacement Pod UID": proof.ReplacementPodUID,
		"old node-plugin Pod UID": proof.OldNodePluginUID, "new node-plugin Pod UID": proof.NewNodePluginUID,
	} {
		if err := volume.ValidateOperationID(value); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
	}
	if proof.OriginalNodeName == proof.ReplacementNodeName || proof.OriginalPodUID == proof.ReplacementPodUID ||
		proof.OldNodePluginUID == proof.NewNodePluginUID || !proof.OldNodeDrained || !proof.MarkerReadAfterDrain ||
		!proof.OldNodeUncordoned || !proof.MarkerReadAfterRestart {
		return fmt.Errorf("node drain or node-plugin restart proof is incomplete")
	}
	for label, value := range map[string]string{
		"replaced Kapsule node ID":    proof.ReplacedKapsuleNodeID,
		"replacement Kapsule node ID": proof.ReplacementKapsuleID,
	} {
		if !validBoundedIdentity(value) {
			return fmt.Errorf("%s is invalid", label)
		}
	}
	if proof.ReplacedKapsuleNodeID == proof.ReplacementKapsuleID || !validKubernetesName(proof.ReplacementKapsuleName) {
		return fmt.Errorf("kapsule replacement identities are invalid")
	}
	if _, err := scaleway.ParseNodeID(proof.ReplacementKapsuleNodeID); err != nil {
		return fmt.Errorf("replacement CSI node ID: %w", err)
	}
	if !validBoundedIdentity(proof.CommercialType) || proof.MaxFileSystems < 2 ||
		!validDigest("sha256:"+proof.NodeConfigGeneration) || !proof.ReplacementReady ||
		!proof.ReplacementPluginReady || !proof.ReplacementRegistered || !proof.ReplacementCompatible ||
		!proof.MarkerReadOnReplacement {
		return fmt.Errorf("real Kapsule node replacement proof is incomplete")
	}
	return nil
}

// Validate verifies the exact foreign-attachment fail-closed and recovery
// evidence. Provider-specific identity strings are bounded before they enter
// retained release evidence.
func (proof ProviderAttachDetachProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "provider-attach-detach", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if len(proof.PlannedNodeIDs) < 2 || len(proof.Parents) != 2 {
		return fmt.Errorf("provider proof does not cover two nodes and two parents")
	}
	plannedNodes := make(map[string]struct{}, len(proof.PlannedNodeIDs))
	plannedInstances := make(map[string]struct{}, len(proof.PlannedNodeIDs))
	for _, nodeID := range proof.PlannedNodeIDs {
		target, err := scaleway.ParseNodeID(nodeID)
		if err != nil {
			return fmt.Errorf("provider planned node ID: %w", err)
		}
		if _, duplicate := plannedNodes[nodeID]; duplicate {
			return fmt.Errorf("provider proof repeats planned node %q", nodeID)
		}
		plannedNodes[nodeID] = struct{}{}
		if _, duplicate := plannedInstances[target.ServerID]; duplicate {
			return fmt.Errorf("provider proof repeats planned Instance %q", target.ServerID)
		}
		plannedInstances[target.ServerID] = struct{}{}
	}
	parentIDs := make(map[string]struct{}, len(proof.Parents))
	for _, parent := range proof.Parents {
		if err := volume.ValidateParentFilesystemID(parent.FilesystemID); err != nil {
			return fmt.Errorf("provider parent ID: %w", err)
		}
		if _, duplicate := parentIDs[parent.FilesystemID]; duplicate {
			return fmt.Errorf("provider proof repeats parent %q", parent.FilesystemID)
		}
		parentIDs[parent.FilesystemID] = struct{}{}
		count := len(parent.AttachmentIDs)
		if parent.FilesystemStatus != "available" || int(parent.ReportedAttachments) != count ||
			len(parent.ResourceIDs) != count || len(parent.ResourceTypes) != count || len(parent.Zones) != count {
			return fmt.Errorf("provider surfaces disagree for parent %q", parent.FilesystemID)
		}
		for index, attachmentID := range parent.AttachmentIDs {
			if !validBoundedIdentity(attachmentID) || !validBoundedIdentity(parent.ResourceIDs[index]) ||
				parent.ResourceTypes[index] != "instance_server" || !validBoundedIdentity(parent.Zones[index]) {
				return fmt.Errorf("provider attachment for parent %q is invalid", parent.FilesystemID)
			}
			if _, planned := plannedInstances[parent.ResourceIDs[index]]; !planned {
				return fmt.Errorf("provider baseline contains foreign Instance %q", parent.ResourceIDs[index])
			}
		}
	}
	if err := proof.BootstrapRestart.validate(proof.RunID, parentIDs, plannedNodes); err != nil {
		return fmt.Errorf("provider bootstrap restart: %w", err)
	}
	foreign := proof.ForeignTest
	if !validBoundedIdentity(foreign.DisposableInstanceID) || !validKubernetesName(foreign.PendingPVCName) {
		return fmt.Errorf("provider foreign-attachment identities are invalid")
	}
	if _, planned := plannedInstances[foreign.DisposableInstanceID]; planned {
		return fmt.Errorf("provider disposable Instance is part of the Kubernetes node inventory")
	}
	if len(foreign.FilesystemIDs) != len(parentIDs) || len(foreign.AttachmentIDs) != len(foreign.FilesystemIDs) {
		return fmt.Errorf("provider foreign test does not cover the complete parent pool")
	}
	seenForeignParents := make(map[string]struct{}, len(foreign.FilesystemIDs))
	for index, filesystemID := range foreign.FilesystemIDs {
		if _, present := parentIDs[filesystemID]; !present {
			return fmt.Errorf("provider foreign test uses an unplanned parent")
		}
		if _, duplicate := seenForeignParents[filesystemID]; duplicate || !validBoundedIdentity(foreign.AttachmentIDs[index]) {
			return fmt.Errorf("provider foreign attachment identity is invalid or duplicated")
		}
		seenForeignParents[filesystemID] = struct{}{}
	}
	if !foreign.InitialAttachmentAbsent || !foreign.AttachmentReachedAvailable || !foreign.ProvisioningFailureSeen ||
		!foreign.PVCRemainedUnbound || !foreign.ServerAttachmentAbsent || !foreign.RegionalAttachmentAbsent ||
		!foreign.PVCBoundAfterDetach {
		return fmt.Errorf("provider foreign-attachment fail-closed or recovery proof is incomplete")
	}
	return nil
}

func (proof ProviderBootstrapRestartProof) validate(
	runID string,
	parentIDs map[string]struct{},
	plannedNodes map[string]struct{},
) error {
	if err := volume.ValidateParentFilesystemID(proof.ParentFilesystemID); err != nil {
		return fmt.Errorf("parent filesystem ID: %w", err)
	}
	if _, planned := parentIDs[proof.ParentFilesystemID]; !planned {
		return fmt.Errorf("bootstrap parent is outside the planned pool")
	}
	if err := volume.ValidateOperationID(proof.LeaseUID); err != nil {
		return fmt.Errorf("lease UID: %w", err)
	}
	if err := volume.ValidateOperationID(proof.BootstrapAttemptID); err != nil {
		return fmt.Errorf("bootstrap attempt ID: %w", err)
	}
	if err := volume.ValidateClusterUID(proof.ActiveClusterUID); err != nil {
		return fmt.Errorf("active cluster UID: %w", err)
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDBeforeRestart); err != nil {
		return fmt.Errorf("controller Pod UID before restart: %w", err)
	}
	if err := volume.ValidateOperationID(proof.ControllerPodUIDAfterRestart); err != nil {
		return fmt.Errorf("controller Pod UID after restart: %w", err)
	}
	if proof.ControllerPodUIDBeforeRestart == proof.ControllerPodUIDAfterRestart {
		return fmt.Errorf("controller Pod UID did not change across restart")
	}
	if !validKubernetesName(proof.ControllerNodeNameBeforeRestart) || !validKubernetesName(proof.ControllerNodeNameAfterRestart) {
		return fmt.Errorf("controller node name is invalid")
	}
	if _, err := scaleway.ParseNodeID(proof.ControllerNodeIDBeforeRestart); err != nil {
		return fmt.Errorf("controller node ID before restart: %w", err)
	}
	if _, planned := plannedNodes[proof.ControllerNodeIDBeforeRestart]; !planned {
		return fmt.Errorf("controller node before restart is outside the planned node inventory")
	}
	if _, err := scaleway.ParseNodeID(proof.ControllerNodeIDAfterRestart); err != nil {
		return fmt.Errorf("controller node ID after restart: %w", err)
	}
	if _, planned := plannedNodes[proof.ControllerNodeIDAfterRestart]; !planned {
		return fmt.Errorf("controller node after restart is outside the planned node inventory")
	}
	if proof.ClaimTempPath != "/.sfs-subdir-csi-owner."+proof.BootstrapAttemptID+".tmp" {
		return fmt.Errorf("claim temporary path does not match the attempt")
	}
	if proof.FinalClaimInstallationID != runID || proof.FinalClaimActiveClusterUID != proof.ActiveClusterUID ||
		proof.FinalClaimParentFilesystemID != proof.ParentFilesystemID ||
		proof.FinalClaimBootstrapAttemptID != proof.BootstrapAttemptID {
		return fmt.Errorf("final parent claim does not match the prepared attempt")
	}
	if !proof.InitialAttachmentAbsent || !proof.HelmUpgradeCompleted || !proof.JournalClearedBeforeRestart ||
		!proof.ClaimValidBeforeRestart || !proof.TemporaryClaimAbsentBeforeRestart || !proof.ControllerRestarted ||
		!proof.FinalClaimUnchangedAfterRestart || !proof.JournalClearedAfterRestart ||
		!proof.TemporaryClaimAbsentAfterRestart || !proof.ServerAttachmentAvailable ||
		!proof.RegionalAttachmentAvailable {
		return fmt.Errorf("bootstrap addition or restart proof is incomplete")
	}
	return nil
}

// Validate verifies the exact SINGLE_NODE_WRITER conflict and handoff proof.
func (proof SingleNodeWriterProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "single-node-writer-conflict", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"claim": proof.ClaimName, "persistent volume": proof.PersistentVolumeName,
		"first pod": proof.FirstPodName, "second pod": proof.SecondPodName,
		"first node": proof.FirstNodeName, "second node": proof.SecondNodeName,
	} {
		if value == "" || len(value) > 253 || strings.ContainsAny(value, "\t\r\n/") {
			return fmt.Errorf("%s identity is invalid", label)
		}
	}
	if proof.FirstNodeName == proof.SecondNodeName || proof.FirstNodeID == proof.SecondNodeID {
		return fmt.Errorf("SINGLE_NODE_WRITER proof does not span two distinct nodes")
	}
	if _, err := scaleway.ParseNodeID(proof.FirstNodeID); err != nil {
		return fmt.Errorf("first node ID: %w", err)
	}
	if _, err := scaleway.ParseNodeID(proof.SecondNodeID); err != nil {
		return fmt.Errorf("second node ID: %w", err)
	}
	if !proof.FirstPodReady || !proof.ConflictObserved || proof.RejectionEventCount < 1 ||
		!proof.SecondReadyAfterHandoff || !proof.ReadWriteAfterHandoff {
		return fmt.Errorf("SINGLE_NODE_WRITER conflict or handoff assertion is incomplete")
	}
	if !slices.Equal(proof.PublishedNodesDuringConflict, []string{proof.FirstNodeID}) ||
		!slices.Equal(proof.PublishedNodesAfterHandoff, []string{proof.SecondNodeID}) {
		return fmt.Errorf("SINGLE_NODE_WRITER durable published-node fence is not exact")
	}
	return nil
}

// Validate verifies the exact 100-PVC load and multiplex proof.
func (proof HundredPVCScaleProof) Validate() error {
	if err := validateProofEnvelope(proof.SchemaVersion, proof.Scenario, "one-hundred-pvc-scale", proof.RunID, proof.ObservedAt); err != nil {
		return err
	}
	if proof.PVCCount != 100 || proof.BoundPVCCount != 100 {
		return fmt.Errorf("scale proof covers %d PVCs and %d bound PVCs, want 100", proof.PVCCount, proof.BoundPVCCount)
	}
	if err := validateExactNames(proof.PVCNames, proof.PVCCount, nil); err != nil {
		return fmt.Errorf("scale PVC names: %w", err)
	}
	if err := volume.ValidateParentFilesystemID(proof.SingleParentFilesystemID); err != nil {
		return fmt.Errorf("scale parent filesystem ID: %w", err)
	}
	if proof.SameNodeName == "" || len(proof.SameNodeName) > 253 || proof.MaxFileSystems < 1 ||
		proof.SameNodeLogicalMounts < proof.MaxFileSystems+5 || proof.SameNodeLogicalMounts > proof.PVCCount ||
		proof.IsolatedMarkerCount != proof.SameNodeLogicalMounts {
		return fmt.Errorf("scale same-node multiplex proof is incomplete")
	}
	if err := validateExactNames(proof.SameNodeClaimNames, proof.SameNodeLogicalMounts, proof.PVCNames); err != nil {
		return fmt.Errorf("scale same-node claims: %w", err)
	}
	if _, err := scaleway.ParseNodeID(proof.SameNodeID); err != nil {
		return fmt.Errorf("scale same-node ID: %w", err)
	}
	if proof.RegionalAttachmentCount != 1 || proof.ServerFilesystemCount != 1 || !proof.NodeMaxVolumesOmitted {
		return fmt.Errorf("scale physical attachment or logical MaxVolumes proof is incomplete")
	}
	if proof.SampledPVCCount != 10 || proof.SuccessfulWriterCount != proof.SampledPVCCount || proof.SuccessfulReaderCount != proof.SampledPVCCount {
		return fmt.Errorf("scale sampled read/write proof is incomplete")
	}
	if err := validateExactNames(proof.SampledClaimNames, proof.SampledPVCCount, proof.SameNodeClaimNames); err != nil {
		return fmt.Errorf("scale sampled claims: %w", err)
	}
	if !validKubernetesName(proof.SampledReaderNodeName) || proof.SampledReaderNodeName == proof.SameNodeName || proof.SampledReaderNodeID == proof.SameNodeID {
		return fmt.Errorf("scale sampled readers do not span a distinct node")
	}
	if _, err := scaleway.ParseNodeID(proof.SampledReaderNodeID); err != nil {
		return fmt.Errorf("scale sampled reader node ID: %w", err)
	}
	if !proof.ReadOnlyWriteRejected || !proof.NodePluginsCredentialFree {
		return fmt.Errorf("scale read-only or node credential boundary is not proven")
	}
	if proof.SoakDurationSeconds < 20*60 || proof.SoakDurationSeconds > 60*60 ||
		proof.SoakSuccessfulWrites < proof.SampledPVCCount*100 ||
		proof.SoakSuccessfulReads < proof.SampledPVCCount*100 || proof.SoakChecksumFailures != 0 {
		return fmt.Errorf("scale correctness soak duration, operation counts, or checksum proof is incomplete")
	}
	for label, podUID := range map[string]string{
		"controller before":  proof.SoakControllerUIDBefore,
		"controller after":   proof.SoakControllerUIDAfter,
		"node plugin before": proof.SoakNodePluginUIDBefore,
		"node plugin after":  proof.SoakNodePluginUIDAfter,
	} {
		if err := volume.ValidateOperationID(podUID); err != nil {
			return fmt.Errorf("scale soak %s Pod UID: %w", label, err)
		}
	}
	if proof.SoakControllerUIDBefore == proof.SoakControllerUIDAfter || proof.SoakNodePluginUIDBefore == proof.SoakNodePluginUIDAfter {
		return fmt.Errorf("scale soak did not replace both required Pods")
	}
	return nil
}

func validateExactNames(names []string, expected int, allowed []string) error {
	if len(names) != expected {
		return fmt.Errorf("contains %d names, want %d", len(names), expected)
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" || len(name) > 253 || strings.ContainsAny(name, "\t\r\n/") {
			return fmt.Errorf("contains an invalid name")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("contains duplicate %q", name)
		}
		seen[name] = struct{}{}
		if allowed != nil {
			if _, present := allowedSet[name]; !present {
				return fmt.Errorf("contains out-of-scope name %q", name)
			}
		}
	}
	return nil
}

func validKubernetesName(value string) bool {
	return value != "" && len(value) <= 253 && !strings.ContainsAny(value, "\t\r\n/")
}

func validBoundedIdentity(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsAny(value, "\t\r\n")
}

func validateProofEnvelope(schemaVersion, scenario, expectedScenario, runID, observedAt string) error {
	if schemaVersion != SchemaVersionV1 || scenario != expectedScenario {
		return fmt.Errorf("scenario proof envelope is invalid")
	}
	if err := volume.ValidateOperationID(runID); err != nil {
		return fmt.Errorf("scenario proof run ID: %w", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, observedAt); err != nil {
		return fmt.Errorf("scenario proof observation time: %w", err)
	}
	return nil
}
