package admincli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/coordination"
	driverk8s "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type checkpointRestoreMode string

const (
	checkpointRestoreDryRun  checkpointRestoreMode = "dry-run"
	checkpointRestoreExecute checkpointRestoreMode = "execute"
)

type operatorCheckpointRestoreInvocation struct {
	namespace      string
	release        string
	requestID      string
	archiveFile    string
	identitySecret string
	identityKey    string
	mode           checkpointRestoreMode
	kubeconfig     string
	context        string
	timeout        time.Duration
}

type checkpointRestoreResult struct {
	SchemaVersion           string                `json:"schemaVersion"`
	CheckpointRequestID     string                `json:"checkpointRequestID"`
	Mode                    checkpointRestoreMode `json:"mode"`
	Ready                   bool                  `json:"ready"`
	Completed               bool                  `json:"completed"`
	Namespace               string                `json:"namespace"`
	Release                 string                `json:"release"`
	DriverName              string                `json:"driverName"`
	ArchiveFile             string                `json:"archiveFile"`
	ArchiveSHA256           string                `json:"archiveSHA256"`
	ArchiveBytes            uint64                `json:"archiveBytes"`
	ManifestSHA256          string                `json:"manifestSHA256"`
	PersistentVolumeNames   []string              `json:"persistentVolumeNames"`
	PlannedJournalNames     []string              `json:"plannedJournalNames"`
	CreatedJournalNames     []string              `json:"createdJournalNames"`
	PlannedAllocationNames  []string              `json:"plannedAllocationNames"`
	CreatedAllocationNames  []string              `json:"createdAllocationNames"`
	VerifiedAllocationNames []string              `json:"verifiedAllocationNames"`
	CheckpointSecretStatus  string                `json:"checkpointSecretStatus"`
	RestoreObjectAggregate  string                `json:"restoreObjectAggregateSHA256"`
}

func parseOperatorCheckpointRestore(args []string) (operatorCheckpointRestoreInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorCheckpointRestoreInvocation{}, usage(err)
	}
	if len(args) < 2 || args[0] != "checkpoint" || args[1] != "restore" {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore command is required"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "archive-file": {},
		"identity-secret": {}, "identity-key": {}, "mode": {},
		"kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorCheckpointRestoreInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore does not accept positional arguments"))
	}
	for _, required := range []string{"namespace", "release", "request-id", "archive-file", "identity-secret", "identity-key", "mode"} {
		if _, present := values[required]; !present {
			return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Subdomain(values["identity-secret"]); len(problems) != 0 {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("identity Secret name is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsConfigMapKey(values["identity-key"]); len(problems) != 0 || len(values["identity-key"]) > 128 {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("identity Secret key is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore request ID: %w", err))
	}
	if err := validateCheckpointArchiveInputPath(values["archive-file"]); err != nil {
		return operatorCheckpointRestoreInvocation{}, usage(err)
	}
	mode := checkpointRestoreMode(values["mode"])
	if mode != checkpointRestoreDryRun && mode != checkpointRestoreExecute {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore mode %q is unsupported", mode))
	}
	timeout := defaultOperatorCheckpointTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > 2*time.Hour {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("checkpoint restore timeout must be between 1 minute and 2 hours"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorCheckpointRestoreInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorCheckpointRestoreInvocation{
		namespace: values["namespace"], release: values["release"], requestID: values["request-id"],
		archiveFile: values["archive-file"], identitySecret: values["identity-secret"], identityKey: values["identity-key"],
		mode: mode, kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorCheckpointRestore(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	parsed, err := parseOperatorCheckpointRestore(args)
	if err != nil {
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, parsed.timeout)
	defer cancel()
	archive, err := readCheckpointArchiveFile(operationCtx, parsed.archiveFile)
	if err != nil {
		return err
	}
	if archive.Manifest.CheckpointRequestID != parsed.requestID {
		return fmt.Errorf("checkpoint archive request ID %q differs from --request-id", archive.Manifest.CheckpointRequestID)
	}
	plan, err := recovery.BuildCheckpointRestorePlan(parsed.namespace, archive)
	if err != nil {
		return fmt.Errorf("build checkpoint restore plan: %w", err)
	}
	client, err := newCallerKubernetesClientOnly(parsed.kubeconfig, parsed.context, buildVersion)
	if err != nil {
		return err
	}
	result, err := executeCheckpointRestore(operationCtx, client, parsed, archive, plan)
	if err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode checkpoint restore result: %w", err)
	}
	return writeResult(stdout, encoded)
}

func validateCheckpointArchiveInputPath(filename string) error {
	if filename == "" || !filepath.IsAbs(filename) || filepath.Clean(filename) != filename || filename == string(filepath.Separator) || strings.ContainsAny(filename, "\x00\r\n") {
		return fmt.Errorf("checkpoint archive file must be a clean absolute non-root path")
	}
	return nil
}

func readCheckpointArchiveFile(ctx context.Context, filename string) (result recovery.DecodedCheckpointArchive, returnErr error) {
	if err := validateCheckpointArchiveInputPath(filename); err != nil {
		return recovery.DecodedCheckpointArchive{}, err
	}
	before, err := os.Lstat(filename)
	if err != nil {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("inspect checkpoint archive file: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() <= 0 || uint64(before.Size()) > recovery.MaxCheckpointArchiveBytes {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive must be a non-symlink regular file bounded to %d bytes", recovery.MaxCheckpointArchiveBytes)
	}
	file, err := os.Open(filename)
	if err != nil {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("open checkpoint archive file: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive changed during open: %w", err)
	}
	archive, err := recovery.ReadCheckpointArchive(ctx, file)
	if err != nil {
		return recovery.DecodedCheckpointArchive{}, err
	}
	after, err := file.Stat()
	if err != nil {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("restat opened checkpoint archive: %w", err)
	}
	pathAfter, err := os.Lstat(filename)
	if err != nil || !os.SameFile(before, after) || !os.SameFile(before, pathAfter) || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
		return recovery.DecodedCheckpointArchive{}, fmt.Errorf("checkpoint archive changed while it was read: %w", err)
	}
	return archive, nil
}

func executeCheckpointRestore(ctx context.Context, client kubernetes.Interface, invocation operatorCheckpointRestoreInvocation, archive recovery.DecodedCheckpointArchive, plan recovery.CheckpointRestorePlan) (checkpointRestoreResult, error) {
	if client == nil {
		return checkpointRestoreResult{}, fmt.Errorf("checkpoint restore Kubernetes client is nil")
	}
	if err := requireRestoreWorkloadsStopped(ctx, client, invocation.namespace, invocation.release); err != nil {
		return checkpointRestoreResult{}, err
	}
	installationID, err := verifyRestoreClusterIdentity(ctx, client, invocation, plan)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	livePVs, pvNames, err := verifyRestorePersistentVolumes(ctx, client, plan)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	configMaps, err := driverk8s.NewClientGoConfigMaps(client.CoreV1())
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	store, err := driverk8s.NewAllocationStore(configMaps, invocation.namespace, plan.DriverName, installationID)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	journalStore, err := driverk8s.NewReservationJournalStore(configMaps, invocation.namespace, plan.DriverName, installationID)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	_, missingJournals, err := journalStore.RestoreCheckpointObjects(
		ctx, plan.ReservationJournalSet, plan.ReservationJournals, false,
	)
	if err != nil {
		return checkpointRestoreResult{}, fmt.Errorf("inspect restored reservation journals: %w", err)
	}
	existing, missing, err := inspectRestoreAllocations(ctx, client, store, invocation.namespace, plan.Allocations)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	secretStatus, secretExists, err := inspectRestoreCheckpointSecret(ctx, client, invocation.namespace, archive.Package.ManifestBytes)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	if secretExists && (len(missing) != 0 || len(missingJournals) != 0) {
		return checkpointRestoreResult{}, fmt.Errorf("immutable checkpoint Secret exists while %d allocation and %d journal ConfigMap(s) are missing", len(missing), len(missingJournals))
	}

	result := checkpointRestoreResult{
		SchemaVersion: volume.SchemaVersionV1, CheckpointRequestID: plan.CheckpointRequestID,
		Mode: invocation.mode, Ready: true, Namespace: invocation.namespace, Release: invocation.release,
		DriverName: plan.DriverName, ArchiveFile: invocation.archiveFile,
		ArchiveSHA256: archive.ArchiveSHA256, ArchiveBytes: archive.ArchiveBytes, ManifestSHA256: archive.ManifestSHA256,
		PersistentVolumeNames: pvNames, RestoreObjectAggregate: archive.Manifest.KubernetesObjects.AggregateSHA256,
		CheckpointSecretStatus: secretStatus,
		PlannedJournalNames:    slices.Clone(missingJournals),
	}
	for _, allocation := range missing {
		result.PlannedAllocationNames = append(result.PlannedAllocationNames, allocation.Name)
	}
	for _, stored := range existing {
		name, _ := driverk8s.AllocationName(stored.Record.LogicalID())
		result.VerifiedAllocationNames = append(result.VerifiedAllocationNames, name)
	}
	if invocation.mode == checkpointRestoreDryRun {
		result.Completed = secretExists && len(missing) == 0 && len(missingJournals) == 0
		if !secretExists {
			result.CheckpointSecretStatus = "planned-create"
		}
		return result, nil
	}

	createdJournals, _, err := journalStore.RestoreCheckpointObjects(
		ctx, plan.ReservationJournalSet, plan.ReservationJournals, true,
	)
	if err != nil {
		return checkpointRestoreResult{}, fmt.Errorf("restore reservation journals: %w", err)
	}
	result.CreatedJournalNames = createdJournals
	created, reverified, err := createRestoreAllocations(ctx, store, missing)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	result.CreatedAllocationNames = created
	result.VerifiedAllocationNames = append(result.VerifiedAllocationNames, reverified...)
	slices.Sort(result.VerifiedAllocationNames)
	allAllocations, remaining, err := inspectRestoreAllocations(ctx, client, store, invocation.namespace, plan.Allocations)
	if err != nil {
		return checkpointRestoreResult{}, err
	}
	if len(remaining) != 0 || len(allAllocations) != len(plan.Allocations) {
		return checkpointRestoreResult{}, fmt.Errorf("checkpoint allocation restore did not converge to the complete object set")
	}
	journalObjects, err := journalStore.CheckpointObjects(ctx, plan.ReservationJournalSet.Pools, plan.ActiveClusterUID)
	if err != nil {
		return checkpointRestoreResult{}, fmt.Errorf("reread restored reservation journals: %w", err)
	}
	summary, err := recovery.BuildRestoreKubernetesObjectSummary(invocation.namespace, allAllocations, journalObjects, livePVs)
	if err != nil {
		return checkpointRestoreResult{}, fmt.Errorf("recompute restored Kubernetes object aggregate: %w", err)
	}
	if summary != archive.Manifest.KubernetesObjects {
		return checkpointRestoreResult{}, fmt.Errorf("restored Kubernetes object aggregate differs from checkpoint")
	}
	if !secretExists {
		status, err := createRestoreCheckpointSecret(ctx, client, invocation.namespace, invocation.release, archive.Package.ManifestBytes)
		if err != nil {
			return checkpointRestoreResult{}, err
		}
		result.CheckpointSecretStatus = status
	}
	result.Completed = true
	return result, nil
}

func requireRestoreWorkloadsStopped(ctx context.Context, client kubernetes.Interface, namespace, release string) error {
	for _, component := range []string{"controller", "node"} {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: releaseSelector(release, component), Limit: adminMaxInventoryObjects})
		if err != nil {
			return fmt.Errorf("list release %s Pods before checkpoint restore: %w", component, err)
		}
		if pods.Continue != "" || len(pods.Items) != 0 {
			return fmt.Errorf("checkpoint restore requires every release %s Pod to be absent", component)
		}
	}
	daemonSets, err := client.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{LabelSelector: releaseSelector(release, "node"), Limit: 2})
	if err != nil {
		return fmt.Errorf("list release node DaemonSet before checkpoint restore: %w", err)
	}
	if daemonSets.Continue != "" || len(daemonSets.Items) != 0 {
		return fmt.Errorf("checkpoint restore requires the release node DaemonSet to be absent")
	}
	deployments, err := client.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{LabelSelector: releaseSelector(release, "controller"), Limit: 2})
	if err != nil {
		return fmt.Errorf("list release controller Deployment before checkpoint restore: %w", err)
	}
	if deployments.Continue != "" || len(deployments.Items) > 1 {
		return fmt.Errorf("checkpoint restore found an ambiguous controller Deployment inventory")
	}
	if len(deployments.Items) == 1 {
		deployment := &deployments.Items[0]
		if deployment.DeletionTimestamp != nil || deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
			return fmt.Errorf("checkpoint restore requires an existing controller Deployment to be stable at zero replicas")
		}
	}
	lease, err := client.CoordinationV1().Leases(namespace).Get(ctx, volume.LeadershipLeaseNameV1, metav1.GetOptions{})
	if err == nil {
		if lease.DeletionTimestamp != nil || lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
			return fmt.Errorf("checkpoint restore requires the controller Lease to be absent or empty")
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("read controller Lease before checkpoint restore: %w", err)
	}
	return nil
}

func verifyRestoreClusterIdentity(ctx context.Context, client kubernetes.Interface, invocation operatorCheckpointRestoreInvocation, plan recovery.CheckpointRestorePlan) (string, error) {
	namespace, err := client.CoreV1().Namespaces().Get(ctx, invocation.namespace, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read checkpoint restore namespace: %w", err)
	}
	if namespace.DeletionTimestamp != nil {
		return "", fmt.Errorf("checkpoint restore namespace is pending deletion")
	}
	clusterNamespace, err := client.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read kube-system identity: %w", err)
	}
	if string(clusterNamespace.UID) != plan.ActiveClusterUID {
		return "", fmt.Errorf("current kube-system UID differs from checkpoint activeClusterUID")
	}
	secret, err := client.CoreV1().Secrets(invocation.namespace).Get(ctx, invocation.identitySecret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read restored installation identity Secret: %w", err)
	}
	if secret.DeletionTimestamp != nil {
		return "", fmt.Errorf("restored installation identity Secret is pending deletion")
	}
	installationBytes, present := secret.Data[invocation.identityKey]
	if !present || len(installationBytes) == 0 || len(installationBytes) > 128 || bytes.ContainsAny(installationBytes, "\x00\r\n") {
		return "", fmt.Errorf("restored installation identity Secret key is missing or invalid")
	}
	installationID := string(installationBytes)
	if err := volume.ValidateInstallationID(installationID); err != nil {
		return "", err
	}
	if recovery.SHA256Digest(installationBytes) != plan.InstallationIDHash {
		return "", fmt.Errorf("restored installation identity differs from checkpoint")
	}
	return installationID, nil
}

func verifyRestorePersistentVolumes(ctx context.Context, client kubernetes.Interface, plan recovery.CheckpointRestorePlan) ([]recovery.PersistentVolumeEvidence, []string, error) {
	expected := make(map[string]recovery.RestorePersistentVolume, len(plan.PersistentVolumes))
	for _, persistentVolume := range plan.PersistentVolumes {
		expected[persistentVolume.Name] = persistentVolume
	}
	result := make([]recovery.PersistentVolumeEvidence, 0, len(expected))
	names := make([]string, 0, len(expected))
	continueToken := ""
	seenTokens := map[string]struct{}{"": {}}
	for {
		page, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, nil, fmt.Errorf("list PersistentVolumes for checkpoint restore: %w", err)
		}
		for _, object := range page.Items {
			if object.Spec.CSI == nil || object.Spec.CSI.Driver != plan.DriverName {
				continue
			}
			if len(result) >= adminMaxInventoryObjects {
				return nil, nil, fmt.Errorf("driver PersistentVolume inventory exceeds %d objects", adminMaxInventoryObjects)
			}
			want, present := expected[object.Name]
			if !present {
				return nil, nil, fmt.Errorf("live driver PersistentVolume %q is absent from checkpoint", object.Name)
			}
			driverContext, normalizeErr := volume.DriverOwnedContextMap(object.Spec.CSI.VolumeAttributes)
			if normalizeErr != nil {
				return nil, nil, fmt.Errorf("live PersistentVolume %q context: %w", object.Name, normalizeErr)
			}
			if object.Spec.CSI.VolumeHandle != want.VolumeHandle || !maps.Equal(driverContext, want.VolumeContext) {
				return nil, nil, fmt.Errorf("live PersistentVolume %q differs from checkpoint mapping", object.Name)
			}
			evidence := recovery.PersistentVolumeEvidence{
				Name: object.Name, UID: string(object.UID), ResourceVersion: object.ResourceVersion,
				DriverName: object.Spec.CSI.Driver, VolumeHandle: object.Spec.CSI.VolumeHandle,
				VolumeContext: maps.Clone(object.Spec.CSI.VolumeAttributes),
			}
			if _, err := evidence.Validate(); err != nil {
				return nil, nil, fmt.Errorf("live PersistentVolume %q: %w", object.Name, err)
			}
			result = append(result, evidence)
			names = append(names, object.Name)
			delete(expected, object.Name)
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seenTokens[continueToken]; duplicate {
			return nil, nil, fmt.Errorf("PersistentVolume inventory repeated continue token")
		}
		seenTokens[continueToken] = struct{}{}
	}
	if len(expected) != 0 {
		missing := make([]string, 0, len(expected))
		for name := range expected {
			missing = append(missing, name)
		}
		slices.Sort(missing)
		return nil, nil, fmt.Errorf("checkpoint PersistentVolume %q is missing", missing[0])
	}
	slices.SortFunc(result, func(left, right recovery.PersistentVolumeEvidence) int { return strings.Compare(left.Name, right.Name) })
	slices.Sort(names)
	return result, names, nil
}

func inspectRestoreAllocations(ctx context.Context, client kubernetes.Interface, store *driverk8s.AllocationStore, namespace string, expected []recovery.RestoreAllocation) ([]driverk8s.StoredAllocation, []recovery.RestoreAllocation, error) {
	expectedByName := make(map[string]recovery.RestoreAllocation, len(expected))
	for _, allocation := range expected {
		expectedByName[allocation.Name] = allocation
	}
	existing := make([]driverk8s.StoredAllocation, 0, len(expected))
	continueToken := ""
	seenTokens := map[string]struct{}{"": {}}
	for {
		page, err := client.CoreV1().ConfigMaps(namespace).List(ctx, metav1.ListOptions{Limit: adminListPageSize, Continue: continueToken})
		if err != nil {
			return nil, nil, fmt.Errorf("list ConfigMaps for checkpoint restore: %w", err)
		}
		for _, object := range page.Items {
			if !strings.HasPrefix(object.Name, "sfs-subdir-volume-") {
				continue
			}
			if len(existing) >= adminMaxInventoryObjects {
				return nil, nil, fmt.Errorf("allocation ConfigMap inventory exceeds %d objects", adminMaxInventoryObjects)
			}
			want, present := expectedByName[object.Name]
			if !present {
				return nil, nil, fmt.Errorf("live allocation ConfigMap %q is absent from checkpoint", object.Name)
			}
			stored, err := store.Get(ctx, want.Record.LogicalID())
			if err != nil {
				return nil, nil, err
			}
			if err := equalRestoreAllocation(stored.Record, want.Record); err != nil {
				return nil, nil, fmt.Errorf("live allocation ConfigMap %q: %w", object.Name, err)
			}
			existing = append(existing, stored)
			delete(expectedByName, object.Name)
		}
		continueToken = page.Continue
		if continueToken == "" {
			break
		}
		if _, duplicate := seenTokens[continueToken]; duplicate {
			return nil, nil, fmt.Errorf("ConfigMap inventory repeated continue token")
		}
		seenTokens[continueToken] = struct{}{}
	}
	missing := make([]recovery.RestoreAllocation, 0, len(expectedByName))
	for _, allocation := range expectedByName {
		missing = append(missing, allocation)
	}
	slices.SortFunc(existing, func(left, right driverk8s.StoredAllocation) int {
		return strings.Compare(left.Record.LogicalID(), right.Record.LogicalID())
	})
	slices.SortFunc(missing, func(left, right recovery.RestoreAllocation) int { return strings.Compare(left.Name, right.Name) })
	return existing, missing, nil
}

func equalRestoreAllocation(current, expected volume.AllocationRecord) error {
	currentBytes, err := volume.EncodeAllocationRecord(current)
	if err != nil {
		return err
	}
	expectedBytes, err := volume.EncodeAllocationRecord(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(currentBytes, expectedBytes) {
		return fmt.Errorf("durable record differs from checkpoint")
	}
	return nil
}

func createRestoreAllocations(ctx context.Context, store *driverk8s.AllocationStore, missing []recovery.RestoreAllocation) ([]string, []string, error) {
	created := make([]string, 0, len(missing))
	reverified := make([]string, 0)
	for _, allocation := range missing {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if _, err := store.Create(ctx, allocation.Record); err == nil {
			created = append(created, allocation.Name)
			continue
		} else {
			observed, readErr := store.Get(ctx, allocation.Record.LogicalID())
			if readErr != nil {
				return nil, nil, errors.Join(fmt.Errorf("create restored allocation %q: %w", allocation.Name, err), readErr)
			}
			if compareErr := equalRestoreAllocation(observed.Record, allocation.Record); compareErr != nil {
				return nil, nil, errors.Join(fmt.Errorf("create restored allocation %q was ambiguous: %w", allocation.Name, err), compareErr)
			}
			reverified = append(reverified, allocation.Name)
		}
	}
	slices.Sort(created)
	slices.Sort(reverified)
	return created, reverified, nil
}

func inspectRestoreCheckpointSecret(ctx context.Context, client kubernetes.Interface, namespace string, manifest []byte) (string, bool, error) {
	secret, err := client.CoreV1().Secrets(namespace).Get(ctx, coordination.CheckpointSecretNameV1, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "absent", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read checkpoint restore Secret: %w", err)
	}
	if err := validateRestoreCheckpointSecret(secret, manifest); err != nil {
		return "", false, err
	}
	return "verified-existing", true, nil
}

func validateRestoreCheckpointSecret(secret *corev1.Secret, manifest []byte) error {
	if secret == nil || secret.Name != coordination.CheckpointSecretNameV1 || secret.DeletionTimestamp != nil || secret.Type != corev1.SecretTypeOpaque || secret.Immutable == nil || !*secret.Immutable || len(secret.OwnerReferences) != 0 {
		return fmt.Errorf("existing checkpoint Secret metadata is invalid")
	}
	decoded, digest, err := recovery.ValidateCheckpointSecret(recovery.CheckpointSecret{
		Name: secret.Name, Type: string(secret.Type), Immutable: *secret.Immutable, Data: maps.Clone(secret.Data),
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(secret.Data["checkpoint.json"], manifest) || digest != recovery.SHA256Digest(manifest) || decoded.CheckpointRequestID == "" {
		return fmt.Errorf("existing checkpoint Secret differs from archive manifest")
	}
	return nil
}

func createRestoreCheckpointSecret(ctx context.Context, client kubernetes.Interface, namespace, release string, manifest []byte) (string, error) {
	immutable := true
	object := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace, Name: coordination.CheckpointSecretNameV1,
		Labels: map[string]string{
			"app.kubernetes.io/name": adminApplicationName, "app.kubernetes.io/instance": release,
			"app.kubernetes.io/component": "checkpoint-restore",
		},
	}, Type: corev1.SecretTypeOpaque, Immutable: &immutable, Data: map[string][]byte{"checkpoint.json": slices.Clone(manifest)}}
	created, createErr := client.CoreV1().Secrets(namespace).Create(ctx, object, metav1.CreateOptions{})
	if createErr == nil {
		if err := validateRestoreCheckpointSecret(created, manifest); err != nil {
			return "", err
		}
		return "created", nil
	}
	observed, readErr := client.CoreV1().Secrets(namespace).Get(ctx, coordination.CheckpointSecretNameV1, metav1.GetOptions{})
	if readErr != nil {
		return "", errors.Join(fmt.Errorf("create immutable checkpoint restore Secret: %w", createErr), readErr)
	}
	if err := validateRestoreCheckpointSecret(observed, manifest); err != nil {
		return "", errors.Join(createErr, err)
	}
	return "verified-after-ambiguous-create", nil
}
