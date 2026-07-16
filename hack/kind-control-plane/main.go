// Command kind-control-plane exercises the production Kubernetes persistence
// and coordination adapters against the disposable kind API server. Provider
// and mount behavior remain covered by the chart's deliberately small fake.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	driverName     = "file-storage-subdir.csi.urlab.ai"
	installationID = "11111111-1111-4111-8111-111111111111"
	parentID       = "33333333-3333-4333-8333-333333333333"
)

func main() {
	var kubeconfig, namespace string
	flags := flag.NewFlagSet("kind-control-plane", flag.ContinueOnError)
	flags.StringVar(&kubeconfig, "kubeconfig", "", "absolute disposable kind kubeconfig")
	flags.StringVar(&namespace, "namespace", "", "driver namespace")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 || kubeconfig == "" || namespace == "" {
		fmt.Fprintln(os.Stderr, "kind-control-plane: --kubeconfig and --namespace are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx, kubeconfig, namespace); err != nil {
		fmt.Fprintln(os.Stderr, "kind-control-plane:", err)
		os.Exit(1)
	}
	fmt.Println("kind production Kubernetes adapters, Lease CAS, reservation recovery, scale, and immutable Secret verification passed")
}

func run(ctx context.Context, kubeconfig, namespace string) error {
	configuration, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("load kind kubeconfig: %w", err)
	}
	configuration.UserAgent = "scaleway-sfs-subdir-csi-kind-control-plane"
	client, err := kubernetes.NewForConfig(configuration)
	if err != nil {
		return fmt.Errorf("construct kind client: %w", err)
	}
	clusterNamespace, err := client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil || clusterNamespace.UID == "" {
		return fmt.Errorf("read active cluster UID: %w", err)
	}
	clusterUID := string(clusterNamespace.UID)
	leaseUID, err := verifyLeaseProtocol(ctx, client, namespace, clusterUID)
	if err != nil {
		return err
	}
	if err := verifyReservationProtocol(ctx, client, namespace, clusterUID); err != nil {
		return err
	}
	checkpointRequestID, checkpointDigest, err := verifyCheckpointSecret(ctx, client, namespace, clusterUID, leaseUID)
	if err != nil {
		return err
	}
	return verifyImmutableApproval(ctx, client, namespace, clusterUID, checkpointRequestID, checkpointDigest)
}

func verifyLeaseProtocol(ctx context.Context, client kubernetes.Interface, namespace, clusterUID string) (string, error) {
	store, err := k8s.NewClientGoLeaseStore(client.CoordinationV1(), namespace)
	if err != nil {
		return "", err
	}
	empty, err := store.Load(ctx)
	if err != nil {
		return "", err
	}
	first := mustHolder("44444444-4444-4444-8444-444444444444", "kind-worker", clusterUID)
	plan, planErr := coordination.PlanAutomaticAcquisition(empty, first, true)
	if !errors.Is(planErr, coordination.ErrMissingLeaseRecoveryRequired) || plan.MutationAllowed {
		return "", fmt.Errorf("empty Lease did not enter provisional recovery: %v", planErr)
	}
	provisional, err := store.Update(ctx, empty, coordination.LeaseSnapshot{
		UID: empty.UID, ResourceVersion: empty.ResourceVersion,
		HolderIdentity: plan.HolderIdentity, Annotations: plan.Annotations,
	}, time.Now().UTC(), coordination.DefaultLeaseTiming().LeaseDuration)
	if err != nil {
		return "", fmt.Errorf("persist provisional Lease: %w", err)
	}
	// The disposable probe has no provider state. Model the already-proven fresh
	// installation promotion by clearing only the discovery marker through the
	// production parser/annotation contract and one exact CAS.
	annotations := coordination.ClearDiscoveryMarker(provisional.Annotations)
	active, err := store.Update(ctx, provisional, coordination.LeaseSnapshot{
		UID: provisional.UID, ResourceVersion: provisional.ResourceVersion,
		HolderIdentity: first.PodUID, Annotations: annotations,
	}, time.Now().UTC(), coordination.DefaultLeaseTiming().LeaseDuration)
	if err != nil {
		return "", fmt.Errorf("promote fresh Lease: %w", err)
	}
	releaseRequest := "55555555-5555-4555-8555-555555555555"
	releasedPlan, err := coordination.PlanGracefulRelease(active, first, releaseRequest, time.Now().UTC(), 0, false)
	if err != nil {
		return "", fmt.Errorf("plan graceful handoff: %w", err)
	}
	released, err := store.Update(ctx, active, releasedPlan, time.Now().UTC(), coordination.DefaultLeaseTiming().LeaseDuration)
	if err != nil {
		return "", fmt.Errorf("persist graceful handoff: %w", err)
	}
	second := mustHolder("66666666-6666-4666-8666-666666666666", "kind-worker", clusterUID)
	handoff, err := coordination.PlanAutomaticAcquisition(released, second, true)
	if err != nil || handoff.Mode != coordination.AcquisitionGracefulHandoff || !handoff.MutationAllowed {
		return "", fmt.Errorf("plan graceful successor: %#v, %w", handoff, err)
	}
	successor, err := store.Update(ctx, released, coordination.LeaseSnapshot{
		UID: released.UID, ResourceVersion: released.ResourceVersion,
		HolderIdentity: handoff.HolderIdentity, Annotations: handoff.Annotations,
	}, time.Now().UTC(), coordination.DefaultLeaseTiming().LeaseDuration)
	if err != nil {
		return "", fmt.Errorf("persist graceful successor: %w", err)
	}
	staleNext := coordination.LeaseSnapshot{
		UID: released.UID, ResourceVersion: released.ResourceVersion,
		HolderIdentity: successor.HolderIdentity, Annotations: successor.Annotations,
	}
	if _, err := store.Update(ctx, released, staleNext, time.Now().UTC(), coordination.DefaultLeaseTiming().LeaseDuration); !errors.Is(err, coordination.ErrLeaseLost) {
		return "", fmt.Errorf("stale Lease CAS error = %v, want Lease lost", err)
	}
	if err := client.CoordinationV1().Leases(namespace).Delete(ctx, k8s.ControllerLeaseName, metav1.DeleteOptions{}); err != nil {
		return "", fmt.Errorf("delete disposable Lease: %w", err)
	}
	recreated, err := store.Load(ctx)
	if err != nil {
		return "", fmt.Errorf("recreate fixed Lease: %w", err)
	}
	if recreated.UID == successor.UID {
		return "", fmt.Errorf("recreated Lease retained old UID")
	}
	if _, err := coordination.PlanAutomaticAcquisition(recreated, second, true); !errors.Is(err, coordination.ErrMissingLeaseRecoveryRequired) {
		return "", fmt.Errorf("missing-Lease recovery was not fail-closed: %v", err)
	}
	return recreated.UID, nil
}

func verifyReservationProtocol(ctx context.Context, client kubernetes.Interface, namespace, clusterUID string) error {
	configMaps, err := k8s.NewClientGoConfigMaps(client.CoreV1())
	if err != nil {
		return err
	}
	allocations, err := k8s.NewAllocationStore(configMaps, namespace, driverName, installationID)
	if err != nil {
		return err
	}
	journal, err := k8s.NewReservationJournalStore(configMaps, namespace, driverName, installationID)
	if err != nil {
		return err
	}
	if err := journal.BootstrapFresh(ctx, []string{"standard"}, clusterUID); err != nil {
		return fmt.Errorf("bootstrap reservation journals: %w", err)
	}
	first, err := allocation("kind-control-000", clusterUID)
	if err != nil {
		return err
	}
	if _, err := journal.Begin(ctx, "standard", clusterUID, first); err != nil {
		return fmt.Errorf("begin takeover reservation: %w", err)
	}
	// A new store instance has no process-local knowledge and must recover the
	// exact Pending allocation before it can reopen this pool.
	successor, _ := k8s.NewReservationJournalStore(configMaps, namespace, driverName, installationID)
	created, err := successor.Reconcile(ctx, []string{"standard"}, clusterUID, allocations)
	if err != nil || !created {
		return fmt.Errorf("reconcile successor reservation = %t, %w", created, err)
	}
	for index := 1; index < 100; index++ {
		record, err := allocation(fmt.Sprintf("kind-control-%03d", index), clusterUID)
		if err != nil {
			return err
		}
		if _, err := successor.Begin(ctx, "standard", clusterUID, record); err != nil {
			return fmt.Errorf("begin reservation %d: %w", index, err)
		}
		if _, err := allocations.Create(ctx, record); err != nil {
			return fmt.Errorf("create allocation %d: %w", index, err)
		}
		if _, completed, err := successor.CompleteExact(ctx, "standard", clusterUID, record); err != nil {
			return fmt.Errorf("complete reservation %d: %w", index, err)
		} else if !completed {
			return fmt.Errorf("complete reservation %d did not resolve the matching Pending journal", index)
		}
	}
	listed, err := allocations.List(ctx)
	if err != nil || len(listed) != 100 {
		return fmt.Errorf("allocation scale inventory count = %d, %w", len(listed), err)
	}
	return nil
}

func verifyCheckpointSecret(ctx context.Context, client kubernetes.Interface, namespace, clusterUID, leaseUID string) (string, string, error) {
	holder := mustHolder("66666666-6666-4666-8666-666666666666", "kind-worker", clusterUID)
	requestID := "99999999-9999-4999-8999-999999999999"
	manifest, err := recovery.NewCheckpointManifest(
		requestID, driverName, installationID, clusterUID, "0.0.0-kind", leaseUID, holder, time.Now().UTC(),
		[]recovery.ImageDigest{{Name: "driver", Digest: "sha256:" + strings.Repeat("a", 64)}},
		recovery.ObjectInventorySummary{Count: 100, AggregateSHA256: "sha256:" + strings.Repeat("b", 64)},
		[]recovery.ParentInventory{{ParentFilesystemID: parentID, ParentOwnerSHA256: "sha256:" + strings.Repeat("c", 64), RecordCount: 100, AggregateSHA256: "sha256:" + strings.Repeat("d", 64)}},
	)
	if err != nil {
		return "", "", err
	}
	encoded, err := recovery.EncodeCheckpointManifest(manifest)
	if err != nil {
		return "", "", err
	}
	immutable := true
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: coordination.CheckpointSecretNameV1, Namespace: namespace},
		Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: map[string][]byte{"checkpoint.json": encoded}}
	created, err := client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return "", "", fmt.Errorf("create immutable checkpoint: %w", err)
	}
	projected, err := k8s.ReadCheckpointSecret(ctx, client.CoreV1(), namespace)
	if err != nil {
		return "", "", err
	}
	_, digest, err := recovery.ValidateCheckpointSecret(recovery.CheckpointSecret{
		Name: projected.Name, Type: projected.Type, Immutable: projected.Immutable, Data: projected.Data,
	})
	if err != nil {
		return "", "", fmt.Errorf("validate projected checkpoint: %w", err)
	}
	changed := created.DeepCopy()
	changed.Data["checkpoint.json"] = []byte("changed")
	if _, err := client.CoreV1().Secrets(namespace).Update(ctx, changed, metav1.UpdateOptions{}); err == nil {
		return "", "", fmt.Errorf("kubernetes accepted mutation of immutable checkpoint")
	}
	return requestID, digest, nil
}

func verifyImmutableApproval(ctx context.Context, client kubernetes.Interface, namespace, clusterUID, checkpointRequestID, checkpointDigest string) error {
	immutable := true
	now := time.Now().UTC()
	approvedAt := now.Add(-time.Minute).Format(time.RFC3339Nano)
	expiresAt := now.Add(20 * time.Minute).Format(time.RFC3339Nano)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: coordination.ApprovalSecretNameV1, Namespace: namespace},
		Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: map[string][]byte{
			"schemaVersion": []byte("1"), "mode": []byte("missing-lease-recovery"),
			"requestID": []byte("77777777-7777-4777-8777-777777777777"), "installationID": []byte(installationID),
			"activeClusterUID": []byte(clusterUID), "previousHolderPodUID": {}, "previousHolderNodeName": {}, "previousHolderCSINodeID": {},
			"previousHolderInstanceID": {}, "previousHolderZone": {}, "checkpointRequestID": []byte(checkpointRequestID),
			"checkpointManifestSHA256": []byte(checkpointDigest), "recoveryFenceScope": []byte(coordination.RecoveryFenceAllPreRecoveryInstances),
			"reason": []byte("disposable kind recovery proof"), "approvedAt": []byte(approvedAt), "expiresAt": []byte(expiresAt),
		}}
	created, err := client.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create immutable approval: %w", err)
	}
	approval, err := k8s.ReadOperatorApproval(ctx, client.CoreV1(), namespace)
	if err != nil || approval.SecretUID != string(created.UID) || !approval.Immutable {
		return fmt.Errorf("read immutable approval: %#v, %w", approval, err)
	}
	if err := approval.ValidateAt(now, now.Add(-2*time.Minute)); err != nil {
		return fmt.Errorf("validate recovery approval: %w", err)
	}
	if err := approval.ValidateCheckpoint(checkpointRequestID, checkpointDigest); err != nil {
		return err
	}
	changed := created.DeepCopy()
	changed.Data["reason"] = []byte("must fail")
	if _, err := client.CoreV1().Secrets(namespace).Update(ctx, changed, metav1.UpdateOptions{}); err == nil {
		return fmt.Errorf("kubernetes accepted mutation of immutable approval")
	}
	return nil
}

func mustHolder(podUID, nodeName, clusterUID string) coordination.HolderEvidence {
	holder, err := coordination.NewHolderEvidence(
		podUID, nodeName, "fr-par-1/88888888-8888-4888-8888-888888888888",
		"88888888-8888-4888-8888-888888888888", "fr-par-1", installationID, clusterUID,
	)
	if err != nil {
		panic(err)
	}
	return holder
}

func allocation(requestName, clusterUID string) (*volume.DetailedAllocationRecord, error) {
	logicalID, err := volume.LogicalVolumeID(driverName, requestName)
	if err != nil {
		return nil, err
	}
	directoryName, err := volume.DirectoryName("kind-control", requestName, logicalID)
	if err != nil {
		return nil, err
	}
	mapping := volume.Mapping{PoolName: "standard", ParentFilesystemID: parentID, BasePath: "/kubernetes-volumes", DirectoryName: directoryName, LogicalVolumeID: logicalID}
	handle, err := volume.NewHandle(mapping)
	if err != nil {
		return nil, err
	}
	handleHash, err := volume.VolumeHandleHash(handle.String())
	if err != nil {
		return nil, err
	}
	baseHash, err := volume.BasePathHash(mapping.BasePath)
	if err != nil {
		return nil, err
	}
	parameters, err := (volume.CreateParameters{PoolName: "standard", DeletePolicy: volume.DeletePolicyArchive, DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770", AccessType: "mount", FilesystemType: "virtiofs", AccessModes: []volume.AccessMode{volume.AccessModeMultiNodeMultiWriter}}).Normalize()
	if err != nil {
		return nil, err
	}
	requestHash, err := volume.RequestHash(volume.CreateRequestIdentity{OriginalRequiredBytes: 1, SelectedCapacityBytes: 1, Parameters: parameters})
	if err != nil {
		return nil, err
	}
	record := &volume.DetailedAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDetailed, RecordRevision: 1,
		DriverName: driverName, ActiveClusterUID: clusterUID, State: volume.StateReserved, InstallationID: installationID,
		CreateVolumeRequestName: requestName, RequestHash: requestHash, OriginalRequiredBytes: 1, SelectedCapacityBytes: 1,
		NormalizedCreateParameters: parameters, LogicalVolumeID: logicalID, VolumeHandle: handle.String(), VolumeHandleHash: handleHash,
		MappingHash: handle.MappingHash, PoolName: mapping.PoolName, ParentFilesystemID: mapping.ParentFilesystemID,
		BasePath: mapping.BasePath, BasePathHash: baseHash, DirectoryName: mapping.DirectoryName, ReservesCapacity: true,
		DeletePolicy: volume.DeletePolicyArchive, DirectoryUID: 1000, DirectoryGID: 1000, DirectoryMode: "0770",
		CreatedAt: "2026-07-15T10:00:00Z", UpdatedAt: "2026-07-15T10:00:00Z", PublishedNodeIDs: []string{},
	}
	return record, record.Validate()
}
