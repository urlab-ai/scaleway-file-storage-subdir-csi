package e2erunner

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

func TestVirtioFSMountProofRequiresValidClaimAndControllerReplacement(t *testing.T) {
	parentID := "44444444-4444-4444-8444-444444444444"
	proof := VirtioFSMountProof{
		SchemaVersion: SchemaVersionV1, Scenario: "virtiofs-mount-api", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		ClaimName: "claim", PersistentVolumeName: "pv", VolumeHandle: "sfs1:lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:mh-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ParentFilesystemID: parentID, ControllerMountPath: "/var/lib/scaleway-sfs-subdir-csi/controller-parents/" + parentID,
		FilesystemType: "virtiofs", ControllerPodUIDBefore: proofFirstNodeID[9:], ControllerPodUIDAfter: proofSecondNodeID[9:],
		ParentClaim: testParentClaim(t, parentID), StatFSSucceeded: true, MarkerReadBefore: true, MarkerReadAfter: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.ParentClaim.ContentChecksum = "broken"
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(corrupt parent claim) error = nil")
	}
}

const (
	proofRunID        = "11111111-1111-4111-8111-111111111111"
	proofFirstNodeID  = "fr-par-1/22222222-2222-4222-8222-222222222222"
	proofSecondNodeID = "fr-par-1/33333333-3333-4333-8333-333333333333"
)

func testParentClaim(t *testing.T, parentID string) volume.ParentOwnerRecord {
	t.Helper()
	basePathHash, err := volume.BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := (volume.ParentOwnerRecord{
		SchemaVersion: volume.SchemaVersionV1, Revision: 1, DriverName: "sfs-subdir.csi.urlab.ai",
		InstallationID: proofRunID, ActiveClusterUID: "77777777-7777-4777-8777-777777777777",
		ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes", BasePathHash: basePathHash,
		ControllerNamespace: "driver-system", HelmReleaseName: "driver", LeadershipLeaseName: volume.LeadershipLeaseNameV1,
		BootstrapAttemptID: "88888888-8888-4888-8888-888888888888", CreatedAt: "2026-07-21T18:00:00Z",
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func TestSingleNodeWriterProofRequiresConflictAndExactHandoff(t *testing.T) {
	proof := SingleNodeWriterProof{
		SchemaVersion: SchemaVersionV1, Scenario: "single-node-writer-conflict", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		ClaimName: "claim", PersistentVolumeName: "pv", FirstPodName: "first", SecondPodName: "second",
		FirstNodeName: "node-a", SecondNodeName: "node-b", FirstNodeID: proofFirstNodeID, SecondNodeID: proofSecondNodeID,
		FirstPodReady: true, ConflictObserved: true, RejectionEventCount: 1,
		PublishedNodesDuringConflict: []string{proofFirstNodeID}, SecondReadyAfterHandoff: true,
		PublishedNodesAfterHandoff: []string{proofSecondNodeID}, ReadWriteAfterHandoff: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.PublishedNodesDuringConflict = []string{proofSecondNodeID}
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(wrong conflict fence) error = nil")
	}
}

func TestHundredPVCScaleProofRequiresExactLoadAndBoundedMultiplex(t *testing.T) {
	pvcNames := make([]string, 100)
	for index := range pvcNames {
		pvcNames[index] = "claim-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
	}
	proof := HundredPVCScaleProof{
		SchemaVersion: SchemaVersionV1, Scenario: "one-hundred-pvc-scale", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		PVCCount: 100, BoundPVCCount: 100, SingleParentFilesystemID: "44444444-4444-4444-8444-444444444444",
		PVCNames:     pvcNames,
		SameNodeName: "node-a", MaxFileSystems: 2, SameNodeLogicalMounts: 10, IsolatedMarkerCount: 10,
		SameNodeID: proofFirstNodeID, RegionalAttachmentCount: 1, ServerFilesystemCount: 1, NodeMaxVolumesOmitted: true,
		SameNodeClaimNames: pvcNames[:10], SampledClaimNames: pvcNames[:10],
		SampledReaderNodeName: "node-b", SampledReaderNodeID: proofSecondNodeID,
		SampledPVCCount: 10, SuccessfulWriterCount: 10, SuccessfulReaderCount: 10,
		ReadOnlyWriteRejected: true, NodePluginsCredentialFree: true,
		SoakDurationSeconds: 1200, SoakSuccessfulWrites: 1000, SoakSuccessfulReads: 1000, SoakChecksumFailures: 0,
		SoakControllerUIDBefore: proofFirstNodeID[9:], SoakControllerUIDAfter: proofSecondNodeID[9:],
		SoakNodePluginUIDBefore: "44444444-4444-4444-8444-444444444444", SoakNodePluginUIDAfter: "55555555-5555-4555-8555-555555555555",
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.SameNodeLogicalMounts = 6
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(too few same-node mounts) error = nil")
	}
}

func TestHundredPVCScaleProofRejectsShortOrCorruptSoak(t *testing.T) {
	pvcNames := make([]string, 100)
	for index := range pvcNames {
		pvcNames[index] = "claim-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
	}
	proof := HundredPVCScaleProof{
		SchemaVersion: SchemaVersionV1, Scenario: "one-hundred-pvc-scale", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		PVCCount: 100, BoundPVCCount: 100, PVCNames: pvcNames, SingleParentFilesystemID: "44444444-4444-4444-8444-444444444444",
		SameNodeName: "node-a", MaxFileSystems: 2, SameNodeLogicalMounts: 10, SameNodeClaimNames: pvcNames[:10],
		IsolatedMarkerCount: 10, SameNodeID: proofFirstNodeID, RegionalAttachmentCount: 1, ServerFilesystemCount: 1,
		NodeMaxVolumesOmitted: true, SampledPVCCount: 10, SampledClaimNames: pvcNames[:10], SuccessfulWriterCount: 10,
		SampledReaderNodeName: "node-b", SampledReaderNodeID: proofSecondNodeID,
		SuccessfulReaderCount: 10, ReadOnlyWriteRejected: true, NodePluginsCredentialFree: true,
		SoakDurationSeconds: 1199, SoakSuccessfulWrites: 1000, SoakSuccessfulReads: 1000,
		SoakControllerUIDBefore: proofFirstNodeID[9:], SoakControllerUIDAfter: proofSecondNodeID[9:],
		SoakNodePluginUIDBefore: "44444444-4444-4444-8444-444444444444", SoakNodePluginUIDAfter: "55555555-5555-4555-8555-555555555555",
	}
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(short soak) error = nil")
	}
	proof.SoakDurationSeconds = 1200
	proof.SoakChecksumFailures = 1
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(checksum failure) error = nil")
	}
}

func TestParentGrowthProofRequiresExactStepAndFreshPlacement(t *testing.T) {
	proof := ParentGrowthProof{
		SchemaVersion: SchemaVersionV1, Scenario: "parent-growth", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		FilesystemID: "44444444-4444-4444-8444-444444444444", PreviousSizeBytes: 100_000_000_000,
		RequestedSizeBytes: 200_000_000_000, ObservedSizeBytes: 200_000_000_000, GrowthStepBytes: 100_000_000_000,
		ObservedStatus: "available", ControllerPodUIDBefore: proofFirstNodeID[9:], ControllerPodUIDAfter: proofSecondNodeID[9:],
		ProbePVC: "growth-probe", ProbeRequestName: "pvc-66666666-6666-4666-8666-666666666666",
		AllocationParentID: "44444444-4444-4444-8444-444444444444", FreshAvailableRead: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.ObservedStatus = "updating"
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(transient observation) error = nil")
	}
}

func TestParentDecommissionProofRequiresAuditAndPreservedTombstones(t *testing.T) {
	removed := "44444444-4444-4444-8444-444444444444"
	remaining := "55555555-5555-4555-8555-555555555555"
	requestID := "66666666-6666-4666-8666-666666666666"
	nodeRoot := "/var/lib/scaleway-sfs-subdir-csi/parents"
	controllerRoot := "/var/lib/scaleway-sfs-subdir-csi/controller-parents"
	audit := admin.DecommissionAudit{
		SchemaVersion: volume.SchemaVersionV1, RequestID: requestID, ChartVersion: "0.1.0-rc.11",
		DriverVersion: "0.1.0-rc.11", AdminVersion: "0.1.0-rc.11", LeaseName: volume.LeadershipLeaseNameV1,
		LeaseUID: proofRunID, ParentFilesystemID: removed, NodeParentMountRoot: nodeRoot,
		ControllerParentMountRoot: controllerRoot, CheckedNodeIDs: []string{proofFirstNodeID},
		CheckedInstanceIDs: []string{proofFirstNodeID[9:]},
		NodeUnmounts: []admin.NodeDecommissionUnmountResult{{
			NodeID: proofFirstNodeID, Unmounted: admin.ParentUnmountEvidence{ParentFilesystemID: removed, MountPath: nodeRoot + "/" + removed},
			RemainingStagingMountPaths: []string{}, RemainingWorkloadTargetPaths: []string{},
		}},
		ControllerUnmount: admin.ParentUnmountEvidence{ParentFilesystemID: removed, MountPath: controllerRoot + "/" + removed},
		Detached:          true, RegionalInventorySHA256: "sha256:" + strings.Repeat("a", 64),
		InstanceInventorySHA256: "sha256:" + strings.Repeat("b", 64), CompletedAt: "2026-07-21T18:00:00Z",
	}
	proof := ParentDecommissionProof{
		SchemaVersion: SchemaVersionV1, Scenario: "parent-decommission", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		RequestID: requestID, ParentFilesystemID: removed, RemainingParentIDs: []string{remaining},
		PreservedTombstoneIDs: []string{"lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, Audit: audit,
		DryRunReady: true, ExecuteCompleted: true, RemovedParentUnconfigured: true, ParentMountsAbsent: true,
		ServerAttachmentAbsent: true, RegionalAttachmentAbsent: true, ControllerReady: true, NodePluginsReady: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.PreservedTombstoneIDs = nil
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(missing tombstones) error = nil")
	}
}

func TestValidateAvailableScenarioProofsRejectsUnknownFields(t *testing.T) {
	proof := SingleNodeWriterProof{
		SchemaVersion: SchemaVersionV1, Scenario: "single-node-writer-conflict", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		ClaimName: "claim", PersistentVolumeName: "pv", FirstPodName: "first", SecondPodName: "second",
		FirstNodeName: "node-a", SecondNodeName: "node-b", FirstNodeID: proofFirstNodeID, SecondNodeID: proofSecondNodeID,
		FirstPodReady: true, ConflictObserved: true, RejectionEventCount: 1,
		PublishedNodesDuringConflict: []string{proofFirstNodeID}, SecondReadyAfterHandoff: true,
		PublishedNodesAfterHandoff: []string{proofSecondNodeID}, ReadWriteAfterHandoff: true,
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded[:len(encoded)-1], []byte(`,"unexpected":true}`)...)
	if err := ValidateAvailableScenarioProofs([]ScenarioResult{{Name: proof.Scenario, Proof: encoded}}); err == nil {
		t.Fatal("ValidateAvailableScenarioProofs(unknown field) error = nil")
	}
}

func TestValidateAvailableScenarioProofsRejectsRequiredScenarioWithoutValidator(t *testing.T) {
	original := slices.Clone(RequiredScenarios)
	RequiredScenarios = append(slices.Clone(original), "future-unvalidated-scenario")
	t.Cleanup(func() { RequiredScenarios = original })
	if err := ValidateAvailableScenarioProofs([]ScenarioResult{{Name: "future-unvalidated-scenario", Proof: []byte(`{}`)}}); err == nil {
		t.Fatal("ValidateAvailableScenarioProofs(required scenario without validator) error = nil")
	}
}

func TestControllerFailureProofRequiresLeaseBoundReplacement(t *testing.T) {
	proof := ControllerFailureProof{
		SchemaVersion: SchemaVersionV1, Scenario: "controller-hard-failure", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		LeaseUID: proofRunID, OldPodUID: proofFirstNodeID[9:], NewPodUID: proofSecondNodeID[9:],
		OldNodeName: "node-a", NewNodeName: "node-b", OldNodeID: proofFirstNodeID, NewNodeID: proofSecondNodeID,
		ParentFilesystemIDs: []string{"44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555"},
		ApprovalSecretUID:   "66666666-6666-4666-8666-666666666666", ApprovalRequestID: "77777777-7777-4777-8777-777777777777",
		OperatorSteps: slices.Clone(controllerFailureOperatorSteps), RecoverySeconds: 120,
		OldHolderMatched: true, OldInstanceReachedStopped: true, SuccessorBlockedBeforeApproval: true,
		ServerAttachmentsAbsent: true, RegionalAttachmentsAbsent: true, ApprovalConsumed: true, ExistingVolumeReadWrite: true,
		NewPVCName: "replacement-claim", NewPVCBound: true, LeaseUIDPreserved: true, ControllerAvailable: true,
		ApprovalSecretDeletedAfterAudit: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.LeaseUIDPreserved = false
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(changed Lease) error = nil")
	}
}

func TestNodeDrainProofRequiresDistinctWorkloadAndPluginIdentities(t *testing.T) {
	proof := NodeDrainProof{
		SchemaVersion: SchemaVersionV1, Scenario: "node-drain-and-replacement", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		ClaimName: "claim", DeploymentName: "workload", OriginalNodeName: "node-a", ReplacementNodeName: "node-b",
		OriginalPodUID: proofRunID, ReplacementPodUID: proofFirstNodeID[9:],
		OldNodeDrained: true, MarkerReadAfterDrain: true, OldNodeUncordoned: true,
		OldNodePluginUID: proofSecondNodeID[9:], NewNodePluginUID: "44444444-4444-4444-8444-444444444444",
		MarkerReadAfterRestart: true, ReplacedKapsuleNodeID: "kapsule-old", ReplacementKapsuleID: "kapsule-new",
		ReplacementKapsuleName: "node-c", ReplacementKapsuleNodeID: "fr-par-1/55555555-5555-4555-8555-555555555555",
		CommercialType: "POP2-HM-2C-16G", MaxFileSystems: 2, NodeConfigGeneration: strings.Repeat("a", 64),
		ReplacementReady: true, ReplacementPluginReady: true, ReplacementRegistered: true,
		ReplacementCompatible: true, MarkerReadOnReplacement: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.NewNodePluginUID = proof.OldNodePluginUID
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(reused node plugin) error = nil")
	}
}

func TestProviderAttachDetachProofRequiresFailClosedAndDualAbsence(t *testing.T) {
	proof := ProviderAttachDetachProof{
		SchemaVersion: SchemaVersionV1, Scenario: "provider-attach-detach", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		PlannedNodeIDs: []string{proofFirstNodeID, proofSecondNodeID},
		Parents: []ProviderParentProof{
			{FilesystemID: "44444444-4444-4444-8444-444444444444", FilesystemStatus: "available", ReportedAttachments: 1,
				AttachmentIDs: []string{"attach-a"}, ResourceIDs: []string{proofFirstNodeID[9:]}, ResourceTypes: []string{"instance_server"}, Zones: []string{"fr-par-1"}},
			{FilesystemID: "55555555-5555-4555-8555-555555555555", FilesystemStatus: "available", ReportedAttachments: 1,
				AttachmentIDs: []string{"attach-b"}, ResourceIDs: []string{proofSecondNodeID[9:]}, ResourceTypes: []string{"instance_server"}, Zones: []string{"fr-par-1"}},
		},
		BootstrapRestart: ProviderBootstrapRestartProof{
			ParentFilesystemID: "55555555-5555-4555-8555-555555555555", LeaseUID: "77777777-7777-4777-8777-777777777777",
			BootstrapAttemptID: "88888888-8888-4888-8888-888888888888", ActiveClusterUID: "99999999-9999-4999-8999-999999999999",
			ClaimTempPath:                   "/.sfs-subdir-csi-owner.88888888-8888-4888-8888-888888888888.tmp",
			ControllerPodUIDBeforeRestart:   "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			ControllerPodUIDAfterRestart:    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			ControllerNodeNameBeforeRestart: "node-a", ControllerNodeNameAfterRestart: "node-b",
			ControllerNodeIDBeforeRestart: proofFirstNodeID, ControllerNodeIDAfterRestart: proofSecondNodeID,
			FinalClaimInstallationID: proofRunID, FinalClaimActiveClusterUID: "99999999-9999-4999-8999-999999999999",
			FinalClaimParentFilesystemID: "55555555-5555-4555-8555-555555555555",
			FinalClaimBootstrapAttemptID: "88888888-8888-4888-8888-888888888888",
			InitialAttachmentAbsent:      true, HelmUpgradeCompleted: true, JournalClearedBeforeRestart: true,
			ClaimValidBeforeRestart: true, TemporaryClaimAbsentBeforeRestart: true, ControllerRestarted: true,
			FinalClaimUnchangedAfterRestart: true, JournalClearedAfterRestart: true,
			TemporaryClaimAbsentAfterRestart: true, ServerAttachmentAvailable: true,
			RegionalAttachmentAvailable: true,
		},
		ForeignTest: ProviderForeignAttachProof{
			DisposableInstanceID: "66666666-6666-4666-8666-666666666666",
			FilesystemIDs:        []string{"44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555"},
			AttachmentIDs:        []string{"foreign-attachment-a", "foreign-attachment-b"}, InitialAttachmentAbsent: true, AttachmentReachedAvailable: true,
			PendingPVCName: "foreign-probe", ProvisioningFailureSeen: true, PVCRemainedUnbound: true,
			ServerAttachmentAbsent: true, RegionalAttachmentAbsent: true, PVCBoundAfterDetach: true,
		},
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.BootstrapRestart.FinalClaimUnchangedAfterRestart = false
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(changed claim after restart) error = nil")
	}
	proof.BootstrapRestart.FinalClaimUnchangedAfterRestart = true
	proof.BootstrapRestart.ControllerNodeIDAfterRestart = "fr-par-1/cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(controller outside planned node inventory) error = nil")
	}
	proof.BootstrapRestart.ControllerNodeIDAfterRestart = proofSecondNodeID
	proof.ForeignTest.RegionalAttachmentAbsent = false
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(regional attachment retained) error = nil")
	}
}

func TestCheckpointAndMissingLeaseProofsRequireCompleteRecoveryFence(t *testing.T) {
	oldInstances := []string{
		"11111111-1111-4111-8111-111111111111",
		"22222222-2222-4222-8222-222222222222",
	}
	replacements := []string{
		"33333333-3333-4333-8333-333333333333",
		"44444444-4444-4444-8444-444444444444",
	}
	checkpoint := CheckpointRestoreProof{
		SchemaVersion: SchemaVersionV1, Scenario: "checkpoint-and-restore", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		CheckpointRequestID: proofRunID, ArchiveSHA256: "sha256:" + strings.Repeat("a", 64), ArchiveBytes: 4096,
		ManifestSHA256: "sha256:" + strings.Repeat("b", 64), WorkloadNamespace: "workload", WorkloadClaimName: "claim",
		PersistentVolumeName: "pv-checkpoint", OldInstanceIDs: slices.Clone(oldInstances), ReplacementInstanceIDs: slices.Clone(replacements),
		PrepareCompleted: true, ControllerQuiesced: true, DriverNamespaceDeleted: true, DriverNamespaceRecreated: true,
		PersistentVolumePreserved: true, RestoreDryRunCompleted: true, RestoreExecuteCompleted: true,
		CheckpointSecretImmutable: true, CheckpointSecretDeletedAfterAudit: true, OldPoolScaledToZero: true, AllOldInstancesAbsent: true,
		PoolRestoredWithFreshInstances: true, ExistingMarkerReadAfterRecovery: true, NewProvisioningSucceeded: true,
		ArchiveLifecycleVerified: true, DeleteLifecycleVerified: true, TombstoneInventoryVerified: true,
		ExternalWorkloadCleanupCompleted: true,
	}
	if err := checkpoint.Validate(); err != nil {
		t.Fatalf("checkpoint Validate() error = %v", err)
	}
	checkpoint.ReplacementInstanceIDs[0] = checkpoint.OldInstanceIDs[0]
	if err := checkpoint.Validate(); err == nil {
		t.Fatal("checkpoint Validate(reused Instance) error = nil")
	}
	checkpoint.ReplacementInstanceIDs = slices.Clone(replacements)

	missing := MissingLeaseRecoveryProof{
		SchemaVersion: SchemaVersionV1, Scenario: "missing-lease-recovery", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		CheckpointRequestID: proofRunID, CheckpointManifestSHA256: "sha256:" + strings.Repeat("b", 64),
		LeaseUID: proofRunID, ProvisionalControllerPodUID: proofFirstNodeID[9:], RecoveredControllerPodUID: proofFirstNodeID[9:],
		ApprovalSecretUID: proofSecondNodeID[9:], ApprovalRequestID: "55555555-5555-4555-8555-555555555555",
		RecoveryFenceScope: "all-pre-recovery-instances", OldInstanceIDs: slices.Clone(oldInstances), ReplacementInstanceIDs: slices.Clone(replacements),
		LeaseAbsentBeforeController: true, ProvisionalLeasePersisted: true, ControllerNonServingBeforeFence: true,
		NodeDaemonSetAbsentBeforeApproval: true, OldAttachmentsAbsent: true, OnlyProvisionalAttachmentPresent: true,
		ApprovalCreatedAfterCondition: true, ApprovalConsumed: true, LeaseUIDPreserved: true,
		ControllerServingAfterApproval: true, ApprovalSecretDeletedAfterAudit: true, HistoricalHolderOnlyRejected: true,
		ExportInProgressRejected: true, StaleOwnershipRejected: true, DifferentClusterUIDRejected: true,
	}
	if err := missing.Validate(); err != nil {
		t.Fatalf("missing-Lease Validate() error = %v", err)
	}
	missing.ControllerNonServingBeforeFence = false
	if err := missing.Validate(); err == nil {
		t.Fatal("missing-Lease Validate(served before fence) error = nil")
	}
}

func TestArtifactInstallProofRequiresEverySchedulableNode(t *testing.T) {
	proof := ArtifactInstallProof{
		SchemaVersion: SchemaVersionV1, Scenario: "artifact-and-install-preflight", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		DriverName: "sfs-subdir.csi.urlab.ai", StorageClassName: "sfs-subdir-rwx", LeaseUID: proofRunID, ControllerPodUID: proofFirstNodeID[9:],
		SchedulableLinuxNodes: 2, ReadyNodePluginPods: 2, RegisteredCSINodes: 2,
		NamespacePrivileged: true, LeaseHolderExact: true, HolderEvidenceComplete: true, AllImagesImmutable: true,
		ProductionSecurityContexts: true, ControllerCannotMutatePods: true, StorageClassNonDefault: true, NodeConfigurationGenerationSet: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.ReadyNodePluginPods = 1
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(partial node coverage) error = nil")
	}
}

func TestOfficialCSICoexistenceProofRequiresExactIdleDriver(t *testing.T) {
	proof := OfficialCSICoexistenceProof{
		SchemaVersion: SchemaVersionV1, Scenario: "official-csi-coexistence", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		DriverName: "sfs-subdir.csi.urlab.ai", OfficialDriverName: "filestorage.csi.scaleway.com",
		StorageClassName: "sfs-subdir-rwx", OfficialStorageClassName: "sfs-standard",
		SchedulableLinuxNodes: 2, ReadyOfficialNodePods: 2, OfficialVolumesInUse: 0,
		DistinctCSIDrivers: true, DistinctStorageClasses: true, BothStorageClassesPresent: true,
		NeitherStorageClassDefault: true, NoReleaseObjectCollision: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.OfficialVolumesInUse = 1
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(active official volume) error = nil")
	}
}

func TestSafeUninstallProofRequiresCompletedAuditAndAbsence(t *testing.T) {
	proof := SafeUninstallProof{
		SchemaVersion: SchemaVersionV1, Scenario: "safe-uninstall", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		RequestID: proofRunID, LeaseUID: proofFirstNodeID[9:],
		ParentFilesystemIDs: []string{"44444444-4444-4444-8444-444444444444"}, CheckedNodeIDs: []string{proofFirstNodeID, proofSecondNodeID},
		DryRunReady: true, ExecuteCompleted: true, AuditValidated: true, WorkloadsAndPVsRemoved: true,
		PublishedFencesCleared: true, NodeAndControllerStopped: true, ParentAttachmentsAbsent: true,
		HelmReleaseAbsent: true, NamespaceAbsent: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.ParentAttachmentsAbsent = false
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(parent attachment retained) error = nil")
	}
	proof.ParentAttachmentsAbsent = true
	proof.CheckedNodeIDs = []string{proofFirstNodeID, proofFirstNodeID}
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(duplicate checked node) error = nil")
	}
}

func TestNMinusOneUpgradeProofRequiresMixedGenerationAndLifecycleCompatibility(t *testing.T) {
	proof := NMinusOneUpgradeProof{
		SchemaVersion: SchemaVersionV1, Scenario: "n-minus-one-upgrade", RunID: proofRunID, ObservedAt: "2026-07-21T18:00:00Z",
		PreviousDriverImage:          "registry.example/driver@sha256:" + strings.Repeat("a", 64),
		CandidateDriverImage:         "registry.example/driver@sha256:" + strings.Repeat("b", 64),
		PreviousNodeConfigGeneration: strings.Repeat("c", 64), CandidateNodeConfigGeneration: strings.Repeat("d", 64),
		SchedulableLinuxNodes: 2, PreviousPodsBeforeUpgrade: 2, PreviousPodsDuringStagger: 1,
		CandidatePodsDuringStagger: 1, CandidatePodsAfterConvergence: 2,
		UpgradePreflightAccepted: true, NewNodeOldControllerBlocked: true, InterruptedNodeRolloutRolledBack: true,
		ProvisioningResumedAfterRollback: true, OldNodeNewControllerBlocked: true,
		ExistingReadDuringStagger: true, CreateBlockedDuringStagger: true,
		PublishBlockedDuringStagger: true, ControllerPodReplaced: true, LeaseUIDPreserved: true,
		ExistingVolumeHandlePreserved: true, AllocationIdentityPreserved: true, OwnershipIdentityPreserved: true,
		NewPVCBoundAfterConvergence: true, PublishSucceededAfterConvergence: true,
		ArchiveLifecycleVerified: true, RetainLifecycleVerified: true, DeleteLifecycleVerified: true,
		SiblingDataPreserved: true, ProductionRollingStrategyRestored: true,
	}
	if err := proof.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	proof.CandidatePodsDuringStagger = 0
	if err := proof.Validate(); err == nil {
		t.Fatal("Validate(no candidate during stagger) error = nil")
	}
}
