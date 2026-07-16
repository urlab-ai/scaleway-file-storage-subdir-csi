package admincli

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"scaleway-sfs-subdir-csi/pkg/coordination"
	driverk8s "scaleway-sfs-subdir-csi/pkg/k8s"
	"scaleway-sfs-subdir-csi/pkg/recovery"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func emptyCheckpointRestoreFixture(t *testing.T) (recovery.DecodedCheckpointArchive, recovery.CheckpointRestorePlan, string) {
	t.Helper()
	ticket := validLocalCheckpointTicket(t)
	manifest, err := recovery.DecodeCheckpointManifest(ticket.Manifest)
	if err != nil {
		t.Fatalf("DecodeCheckpointManifest() error = %v", err)
	}
	setRecord, journalRecords, journalObjects := checkpointRestoreJournalFixture(
		t, manifest.DriverName, manifest.HolderEvidence.InstallationID, manifest.ActiveClusterUID,
	)
	summary, err := recovery.BuildRestoreKubernetesObjectSummary(operatorNamespace, nil, journalObjects, nil)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary(journals) error = %v", err)
	}
	manifest.KubernetesObjects = summary
	manifestBytes, err := recovery.EncodeCheckpointManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	return recovery.DecodedCheckpointArchive{
			Package:  recovery.CheckpointExportPackage{ManifestBytes: manifestBytes},
			Manifest: manifest, ManifestSHA256: recovery.SHA256Digest(manifestBytes),
			ArchiveSHA256: "sha256:" + strings.Repeat("a", 64), ArchiveBytes: 1024,
		}, recovery.CheckpointRestorePlan{
			CheckpointRequestID: manifest.CheckpointRequestID,
			DriverName:          manifest.DriverName, ActiveClusterUID: manifest.ActiveClusterUID,
			InstallationIDHash:    manifest.InstallationIDHash,
			ReservationJournalSet: setRecord, ReservationJournals: journalRecords,
			Allocations: []recovery.RestoreAllocation{}, PersistentVolumes: []recovery.RestorePersistentVolume{},
		}, manifest.HolderEvidence.InstallationID
}

func checkpointRestoreJournalFixture(t *testing.T, driverName, installationID, clusterUID string) (driverk8s.ReservationJournalSetRecord, []driverk8s.ReservationJournalRecord, []driverk8s.StoredReservationJournalObject) {
	t.Helper()
	client := driverk8s.NewFakeConfigMapClient()
	store, err := driverk8s.NewReservationJournalStore(client, operatorNamespace, driverName, installationID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapFresh(context.Background(), []string{"standard"}, clusterUID); err != nil {
		t.Fatal(err)
	}
	set, err := store.GetSet(context.Background(), clusterUID)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := store.Get(context.Background(), "standard", clusterUID)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := store.CheckpointObjects(context.Background(), []string{"standard"}, clusterUID)
	if err != nil {
		t.Fatal(err)
	}
	return set.Record, []driverk8s.ReservationJournalRecord{journal.Record}, objects
}

func checkpointRestoreFixtureWithAllocation(t *testing.T) (recovery.DecodedCheckpointArchive, recovery.CheckpointRestorePlan, string) {
	t.Helper()
	archive, plan, installationID := emptyCheckpointRestoreFixture(t)
	logicalID, err := volume.LogicalVolumeID(plan.DriverName, "restored-deleted-volume")
	if err != nil {
		t.Fatalf("LogicalVolumeID() error = %v", err)
	}
	record := &volume.DeletedUnknownAllocationRecord{
		SchemaVersion: volume.SchemaVersionV1, RecordKind: volume.AllocationRecordDeletedUnknown,
		RecordRevision: 1, DriverName: plan.DriverName, InstallationID: installationID,
		ActiveClusterUID: plan.ActiveClusterUID, LogicalVolumeID: logicalID,
		VolumeHandleHash: "vh-" + strings.Repeat("a", 32), MappingHash: "mh-" + strings.Repeat("b", 32),
		State: volume.StateDeleted, ReservesCapacity: false,
		AbsenceReason: "all authoritative sources conclusively absent",
		CreatedAt:     "2026-07-13T17:00:00Z", UpdatedAt: "2026-07-13T17:00:00Z", DeletedAt: "2026-07-13T17:00:00Z",
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("DeletedUnknownAllocationRecord.Validate() error = %v", err)
	}
	name, err := driverk8s.AllocationName(logicalID)
	if err != nil {
		t.Fatalf("AllocationName() error = %v", err)
	}
	_, _, journalObjects := checkpointRestoreJournalFixture(t, plan.DriverName, installationID, plan.ActiveClusterUID)
	summary, err := recovery.BuildRestoreKubernetesObjectSummary(operatorNamespace, []driverk8s.StoredAllocation{{
		Record: record, ResourceVersion: "source-version",
	}}, journalObjects, nil)
	if err != nil {
		t.Fatalf("BuildRestoreKubernetesObjectSummary() error = %v", err)
	}
	archive.Manifest.KubernetesObjects = summary
	manifestBytes, err := recovery.EncodeCheckpointManifest(archive.Manifest)
	if err != nil {
		t.Fatalf("EncodeCheckpointManifest() error = %v", err)
	}
	archive.Package.ManifestBytes = manifestBytes
	archive.ManifestSHA256 = recovery.SHA256Digest(manifestBytes)
	projection, err := volume.EncodeAllocationRecord(record)
	if err != nil {
		t.Fatalf("EncodeAllocationRecord() error = %v", err)
	}
	plan.Allocations = []recovery.RestoreAllocation{{
		Namespace: operatorNamespace, Name: name, Record: record, Projection: projection,
	}}
	return archive, plan, installationID
}

func TestExecuteCheckpointRestoreCreatesSecretLastAndIsIdempotent(t *testing.T) {
	archive, plan, installationID := checkpointRestoreFixtureWithAllocation(t)
	client := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace, UID: types.UID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: types.UID(plan.ActiveClusterUID)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: "driver-identity"}, Data: map[string][]byte{"installationID": []byte(installationID)}},
	)
	client.PrependReactor("create", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID = types.UID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
		object.ResourceVersion = "1"
		return false, nil, nil
	})
	invocation := operatorCheckpointRestoreInvocation{
		namespace: operatorNamespace, release: operatorRelease, requestID: plan.CheckpointRequestID,
		archiveFile: "/tmp/checkpoint.tar", identitySecret: "driver-identity", identityKey: "installationID",
		mode: checkpointRestoreDryRun,
	}
	dryRun, err := executeCheckpointRestore(context.Background(), client, invocation, archive, plan)
	if err != nil {
		t.Fatalf("executeCheckpointRestore(dry-run) error = %v", err)
	}
	if !dryRun.Ready || dryRun.Completed || dryRun.CheckpointSecretStatus != "planned-create" || !slices.Equal(dryRun.PlannedAllocationNames, []string{plan.Allocations[0].Name}) {
		t.Fatalf("dry-run restore result = %#v", dryRun)
	}
	if _, err := client.CoreV1().Secrets(operatorNamespace).Get(context.Background(), coordination.CheckpointSecretNameV1, metav1.GetOptions{}); err == nil {
		t.Fatal("dry-run created checkpoint Secret")
	}

	invocation.mode = checkpointRestoreExecute
	executed, err := executeCheckpointRestore(context.Background(), client, invocation, archive, plan)
	if err != nil {
		t.Fatalf("executeCheckpointRestore(execute) error = %v", err)
	}
	if !executed.Ready || !executed.Completed || executed.CheckpointSecretStatus != "created" {
		t.Fatalf("execute restore result = %#v", executed)
	}
	if !slices.Equal(executed.CreatedAllocationNames, []string{plan.Allocations[0].Name}) {
		t.Fatalf("created allocations = %#v", executed.CreatedAllocationNames)
	}
	allocation, err := client.CoreV1().ConfigMaps(operatorNamespace).Get(context.Background(), plan.Allocations[0].Name, metav1.GetOptions{})
	if err != nil || allocation.Data["record.json"] != string(plan.Allocations[0].Projection) {
		t.Fatalf("restored allocation/error = %#v/%v", allocation, err)
	}
	secret, err := client.CoreV1().Secrets(operatorNamespace).Get(context.Background(), coordination.CheckpointSecretNameV1, metav1.GetOptions{})
	if err != nil || secret.Immutable == nil || !*secret.Immutable || !slices.Equal(secret.Data["checkpoint.json"], archive.Package.ManifestBytes) {
		t.Fatalf("created checkpoint Secret/error = %#v/%v", secret, err)
	}

	retried, err := executeCheckpointRestore(context.Background(), client, invocation, archive, plan)
	if err != nil {
		t.Fatalf("executeCheckpointRestore(retry) error = %v", err)
	}
	if !retried.Completed || retried.CheckpointSecretStatus != "verified-existing" {
		t.Fatalf("retried restore result = %#v", retried)
	}
	var configMapCreate, secretCreate int
	for index, action := range client.Actions() {
		if action.GetVerb() != "create" {
			continue
		}
		switch action.GetResource().Resource {
		case "configmaps":
			configMapCreate = index + 1
		case "secrets":
			secretCreate = index + 1
		}
	}
	if configMapCreate == 0 || secretCreate == 0 || configMapCreate >= secretCreate {
		t.Fatalf("restore create order ConfigMap/Secret = %d/%d", configMapCreate, secretCreate)
	}
}

func TestParseOperatorCheckpointRestoreIsClosedAndBounded(t *testing.T) {
	parsed, err := parseOperatorCheckpointRestore([]string{
		"checkpoint", "restore", "--namespace=" + operatorNamespace, "--release=" + operatorRelease,
		"--request-id=" + testRequestID, "--archive-file=/tmp/checkpoint.tar",
		"--identity-secret=driver-identity", "--identity-key=installationID", "--mode=dry-run", "--timeout=10m",
	})
	if err != nil {
		t.Fatalf("parseOperatorCheckpointRestore() error = %v", err)
	}
	if parsed.mode != checkpointRestoreDryRun || parsed.timeout != 10*time.Minute || parsed.identitySecret != "driver-identity" {
		t.Fatalf("parsed checkpoint restore = %#v", parsed)
	}
	for _, args := range [][]string{
		{"checkpoint", "restore"},
		{"checkpoint", "restore", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--archive-file=relative", "--identity-secret=id", "--identity-key=installationID", "--mode=dry-run"},
		{"checkpoint", "restore", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--archive-file=/tmp/checkpoint.tar", "--identity-secret=id", "--identity-key=bad/key", "--mode=dry-run"},
		{"checkpoint", "restore", "--namespace=x", "--release=y", "--request-id=" + testRequestID, "--archive-file=/tmp/checkpoint.tar", "--identity-secret=id", "--identity-key=installationID", "--mode=mutate"},
	} {
		if _, err := parseOperatorCheckpointRestore(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseOperatorCheckpointRestore(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}
