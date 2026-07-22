package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	instanceapi "github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	driverscaleway "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const fileStorageSizeStep = uint64(100_000_000_000)

type providerAttachmentEvidence struct {
	SchemaVersion  string                     `json:"schemaVersion"`
	Scenario       string                     `json:"scenario"`
	RunID          string                     `json:"runId"`
	ObservedAt     string                     `json:"observedAt"`
	PlannedNodeIDs []string                   `json:"plannedNodeIds"`
	Parents        []providerParentAttachment `json:"parents"`
}

type providerParentAttachment struct {
	FilesystemID        string   `json:"filesystemId"`
	FilesystemStatus    string   `json:"filesystemStatus"`
	ReportedAttachments uint32   `json:"reportedAttachments"`
	AttachmentIDs       []string `json:"attachmentIds"`
	ResourceIDs         []string `json:"resourceIds"`
	ResourceTypes       []string `json:"resourceTypes"`
	Zones               []string `json:"zones"`
}

func (backend *scalewayBackend) runProviderScenarios(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory, evidenceDirectory string) ([]e2erunner.ScenarioResult, error) {
	attachmentResult, err := backend.providerAttachDetachScenario(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	growthResult, err := backend.providerGrowthScenario(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	return []e2erunner.ScenarioResult{attachmentResult, growthResult}, nil
}

func (backend *scalewayBackend) providerAttachmentScenario(ctx context.Context, request e2erunner.Request, inventory e2ecleanup.Inventory, evidenceDirectory, evidenceName string) (e2erunner.ScenarioResult, error) {
	evidence, err := backend.collectProviderAttachmentEvidence(ctx, request, inventory, evidenceName)
	if err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	return writeScenarioJSON(evidenceDirectory, evidenceName, evidence)
}

func (backend *scalewayBackend) collectProviderAttachmentEvidence(ctx context.Context, request e2erunner.Request, inventory e2ecleanup.Inventory, evidenceName string) (providerAttachmentEvidence, error) {
	region := scw.Region(backend.plan.Region)
	plannedNodeIDs, err := backend.plannedPoolCSINodeIDs(ctx, request, inventory)
	if err != nil {
		return providerAttachmentEvidence{}, err
	}
	evidence := providerAttachmentEvidence{
		SchemaVersion: "1", Scenario: evidenceName, RunID: backend.plan.RunID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), PlannedNodeIDs: plannedNodeIDs,
	}
	for ordinal := 0; ordinal < int(backend.plan.Parents.Count); ordinal++ {
		filesystemID := resourceID(inventory, e2ecleanup.ResourceKindParent, ordinal)
		filesystem, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
		if err != nil {
			return providerAttachmentEvidence{}, fmt.Errorf("read provider attachment parent %s: %w", filesystemID, err)
		}
		listed, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{Region: region, FilesystemID: &filesystemID}, scw.WithAllPages(), scw.WithContext(ctx))
		if err != nil {
			return providerAttachmentEvidence{}, fmt.Errorf("list provider attachments for %s: %w", filesystemID, err)
		}
		parent := providerParentAttachment{FilesystemID: filesystemID, FilesystemStatus: filesystem.Status.String(), ReportedAttachments: filesystem.NumberOfAttachments}
		for _, attachment := range listed.Attachments {
			if attachment == nil || attachment.FilesystemID != filesystemID || attachment.ID == "" || attachment.ResourceID == "" || attachment.Zone == nil {
				return providerAttachmentEvidence{}, fmt.Errorf("provider attachment inventory for %s is incomplete", filesystemID)
			}
			parent.AttachmentIDs = append(parent.AttachmentIDs, attachment.ID)
			parent.ResourceIDs = append(parent.ResourceIDs, attachment.ResourceID)
			parent.ResourceTypes = append(parent.ResourceTypes, attachment.ResourceType.String())
			parent.Zones = append(parent.Zones, attachment.Zone.String())
		}
		evidence.Parents = append(evidence.Parents, parent)
	}
	if err := validateProviderAttachmentEvidence(evidence, request.Zone, backend.plan.Parents.Count, backend.plan.Profile == e2eplan.ProfileBase); err != nil {
		return providerAttachmentEvidence{}, err
	}
	return evidence, nil
}

func (backend *scalewayBackend) providerAttachDetachScenario(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory, evidenceDirectory string) (result e2erunner.ScenarioResult, returnErr error) {
	bootstrapCrash, err := readProviderBootstrapCrashProof(evidenceDirectory)
	if err != nil {
		return result, err
	}
	baseline, err := backend.collectProviderAttachmentEvidence(ctx, request, inventory, "provider-attach-detach")
	if err != nil {
		return result, err
	}
	instanceID := resourceID(inventory, e2ecleanup.ResourceKindInstance, 0)
	filesystemIDs := []string{
		resourceID(inventory, e2ecleanup.ResourceKindParent, 0),
		resourceID(inventory, e2ecleanup.ResourceKindParent, 1),
	}
	instanceResource, present := inventoryResource(inventory, instanceID)
	if !present || instanceResource.Kind != e2ecleanup.ResourceKindInstance || !instanceResource.CreatedByRun {
		return result, fmt.Errorf("disposable Instance is absent from the exact run inventory")
	}
	zone := scw.Zone(request.Zone)
	server, err := backend.getExactDisposableServer(ctx, plan, instanceResource)
	if err != nil {
		return result, err
	}
	for _, filesystemID := range filesystemIDs {
		if serverHasFilesystem(server, filesystemID) {
			return result, fmt.Errorf("disposable Instance already has parent %s attached", filesystemID)
		}
		initialRegional, err := backend.regionalAttachmentForInstance(ctx, filesystemID, instanceID)
		if err != nil {
			return result, err
		}
		if initialRegional != nil {
			return result, fmt.Errorf("regional inventory already contains the disposable Instance attachment for parent %s", filesystemID)
		}
	}

	attachmentsMayExist := false
	defer func() {
		if !attachmentsMayExist {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cleanupErr := backend.ensureForeignAttachmentsAbsent(cleanupCtx, zone, instanceID, filesystemIDs)
		if cleanupErr != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("cleanup foreign provider attachment: %w", cleanupErr))
		}
	}()
	foreignAttachmentIDs := make([]string, 0, len(filesystemIDs))
	for _, filesystemID := range filesystemIDs {
		attachmentsMayExist = true
		if _, err := backend.instance.AttachServerFileSystem(&instanceapi.AttachServerFileSystemRequest{
			Zone: zone, ServerID: instanceID, FilesystemID: filesystemID,
		}, scw.WithContext(ctx)); err != nil {
			return result, fmt.Errorf("attach exact run-owned parent %s to disposable Instance: %w", filesystemID, err)
		}
		if err := backend.waitServerFilesystem(ctx, zone, instanceID, filesystemID, true); err != nil {
			return result, err
		}
		foreignAttachment, err := backend.waitRegionalAttachment(ctx, filesystemID, instanceID, true)
		if err != nil {
			return result, err
		}
		foreignAttachmentIDs = append(foreignAttachmentIDs, foreignAttachment.ID)
	}

	probePVC := fmt.Sprintf("e2e-foreign-attachment-%s", plan.RunID[:8])
	manifest := fmt.Sprintf("apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    sfs-subdir-e2e-run: %q\n    sfs-subdir-e2e-scenario: provider-attach-detach\nspec:\n  accessModes: [ReadWriteMany]\n  storageClassName: sfs-subdir-rwx\n  resources: {requests: {storage: 16Mi}}\n", probePVC, request.DriverNamespace, plan.RunID)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return result, err
	}
	if err := backend.waitProvisioningFailure(ctx, request, probePVC, instanceID); err != nil {
		return result, err
	}
	if err := backend.ensureForeignAttachmentsAbsent(ctx, zone, instanceID, filesystemIDs); err != nil {
		return result, err
	}
	attachmentsMayExist = false
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "wait", "pvc/"+probePVC, "--for=jsonpath={.status.phase}=Bound", "--timeout=10m"); err != nil {
		return result, fmt.Errorf("wait for provisioning recovery after exact detach: %w", err)
	}

	parents := make([]e2erunner.ProviderParentProof, 0, len(baseline.Parents))
	for _, parent := range baseline.Parents {
		parents = append(parents, e2erunner.ProviderParentProof{
			FilesystemID: parent.FilesystemID, FilesystemStatus: parent.FilesystemStatus,
			ReportedAttachments: parent.ReportedAttachments, AttachmentIDs: slices.Clone(parent.AttachmentIDs),
			ResourceIDs: slices.Clone(parent.ResourceIDs), ResourceTypes: slices.Clone(parent.ResourceTypes), Zones: slices.Clone(parent.Zones),
		})
	}
	proof := e2erunner.ProviderAttachDetachProof{
		SchemaVersion: "1", Scenario: "provider-attach-detach", RunID: plan.RunID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), PlannedNodeIDs: slices.Clone(baseline.PlannedNodeIDs), Parents: parents,
		BootstrapCrash: bootstrapCrash,
		ForeignTest: e2erunner.ProviderForeignAttachProof{
			DisposableInstanceID: instanceID, FilesystemIDs: slices.Clone(filesystemIDs), AttachmentIDs: foreignAttachmentIDs,
			InitialAttachmentAbsent: true, AttachmentReachedAvailable: true, PendingPVCName: probePVC,
			ProvisioningFailureSeen: true, PVCRemainedUnbound: true, ServerAttachmentAbsent: true,
			RegionalAttachmentAbsent: true, PVCBoundAfterDetach: true,
		},
	}
	if err := proof.Validate(); err != nil {
		return result, err
	}
	return writeScenarioJSON(evidenceDirectory, "provider-attach-detach", proof)
}

func readProviderBootstrapCrashProof(evidenceDirectory string) (e2erunner.ProviderBootstrapCrashProof, error) {
	path := filepath.Join(evidenceDirectory, "provider-bootstrap-crash.json")
	info, err := os.Lstat(path)
	if err != nil {
		return e2erunner.ProviderBootstrapCrashProof{}, fmt.Errorf("inspect provider bootstrap crash proof: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
		return e2erunner.ProviderBootstrapCrashProof{}, fmt.Errorf("provider bootstrap crash proof must be an exact regular file of 1 to 1 MiB")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return e2erunner.ProviderBootstrapCrashProof{}, fmt.Errorf("read provider bootstrap crash proof: %w", err)
	}
	var proof e2erunner.ProviderBootstrapCrashProof
	if err := strictjson.Decode(encoded, &proof); err != nil {
		return e2erunner.ProviderBootstrapCrashProof{}, fmt.Errorf("decode provider bootstrap crash proof: %w", err)
	}
	return proof, nil
}

func (backend *scalewayBackend) getExactDisposableServer(ctx context.Context, plan e2eplan.Plan, resource e2ecleanup.Resource) (*instanceapi.Server, error) {
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: scw.Zone(backend.request.Zone), ServerID: resource.ID}, scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("read exact disposable Instance: %w", err)
	}
	if response == nil || response.Server == nil || response.Server.ID != resource.ID || response.Server.Name != resource.Name ||
		response.Server.Project != plan.ProjectID || !slices.Contains(response.Server.Tags, plan.OwnershipTag) {
		return nil, fmt.Errorf("disposable Instance identity differs from the retained run inventory")
	}
	return response.Server, nil
}

func serverHasFilesystem(server *instanceapi.Server, filesystemID string) bool {
	if server == nil {
		return false
	}
	for _, filesystem := range server.Filesystems {
		if filesystem != nil && filesystem.FilesystemID == filesystemID {
			return true
		}
	}
	return false
}

func (backend *scalewayBackend) waitServerFilesystem(ctx context.Context, zone scw.Zone, instanceID, filesystemID string, wantPresent bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: zone, ServerID: instanceID}, scw.WithContext(waitCtx))
		if err != nil || response == nil || response.Server == nil {
			if err == nil {
				err = fmt.Errorf("provider returned an empty Instance")
			}
			return fmt.Errorf("observe disposable Instance filesystem transition: %w", err)
		}
		found := false
		for _, filesystem := range response.Server.Filesystems {
			if filesystem == nil {
				return fmt.Errorf("disposable Instance returned a nil filesystem entry")
			}
			if filesystem.FilesystemID != filesystemID {
				continue
			}
			found = true
			if wantPresent && filesystem.State == instanceapi.ServerFilesystemStateAvailable {
				return nil
			}
			if filesystem.State == instanceapi.ServerFilesystemStateUnknownState {
				return fmt.Errorf("disposable Instance parent entered unknown attachment state")
			}
		}
		if !wantPresent && !found {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for disposable Instance filesystem transition: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) regionalAttachmentForInstance(ctx context.Context, filesystemID, instanceID string) (*fileapi.Attachment, error) {
	response, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{
		Region: scw.Region(backend.plan.Region), FilesystemID: &filesystemID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list regional attachments for foreign-attachment proof: %w", err)
	}
	var match *fileapi.Attachment
	for _, attachment := range response.Attachments {
		if attachment == nil {
			return nil, fmt.Errorf("regional attachment inventory contains a nil entry")
		}
		if attachment.FilesystemID == filesystemID && attachment.ResourceID == instanceID {
			if match != nil {
				return nil, fmt.Errorf("regional inventory repeats the disposable Instance attachment")
			}
			copy := *attachment
			match = &copy
		}
	}
	return match, nil
}

func (backend *scalewayBackend) waitRegionalAttachment(ctx context.Context, filesystemID, instanceID string, wantPresent bool) (*fileapi.Attachment, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		attachment, err := backend.regionalAttachmentForInstance(waitCtx, filesystemID, instanceID)
		if err != nil {
			return nil, err
		}
		if wantPresent && attachment != nil {
			return attachment, nil
		}
		if !wantPresent && attachment == nil {
			return nil, nil
		}
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("wait for regional attachment transition: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (backend *scalewayBackend) ensureForeignAttachmentAbsent(ctx context.Context, zone scw.Zone, instanceID, filesystemID string) error {
	response, err := backend.instance.GetServer(&instanceapi.GetServerRequest{Zone: zone, ServerID: instanceID}, scw.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("read disposable Instance before exact detach: %w", err)
	}
	if response == nil || response.Server == nil {
		return fmt.Errorf("read disposable Instance before exact detach: provider returned an empty Instance")
	}
	if serverHasFilesystem(response.Server, filesystemID) {
		if _, err := backend.instance.DetachServerFileSystem(&instanceapi.DetachServerFileSystemRequest{
			Zone: zone, ServerID: instanceID, FilesystemID: filesystemID,
		}, scw.WithContext(ctx)); err != nil {
			return fmt.Errorf("detach exact run-owned parent from disposable Instance: %w", err)
		}
	}
	if err := backend.waitServerFilesystem(ctx, zone, instanceID, filesystemID, false); err != nil {
		return err
	}
	_, err = backend.waitRegionalAttachment(ctx, filesystemID, instanceID, false)
	return err
}

func (backend *scalewayBackend) ensureForeignAttachmentsAbsent(ctx context.Context, zone scw.Zone, instanceID string, filesystemIDs []string) error {
	var cleanupErr error
	for _, filesystemID := range filesystemIDs {
		if err := backend.ensureForeignAttachmentAbsent(ctx, zone, instanceID, filesystemID); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("parent %s: %w", filesystemID, err))
		}
	}
	return cleanupErr
}

func (backend *scalewayBackend) waitProvisioningFailure(ctx context.Context, request e2erunner.Request, pvcName, foreignInstanceID string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		pvcJSON, err := backend.kubectl(waitCtx, request, nil, "-n", request.DriverNamespace, "get", "pvc/"+pvcName, "-o", "json")
		if err != nil {
			return err
		}
		var pvc struct {
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		}
		if err := json.Unmarshal(pvcJSON, &pvc); err != nil {
			return fmt.Errorf("decode foreign-attachment probe PVC: %w", err)
		}
		if pvc.Status.Phase == "Bound" {
			return fmt.Errorf("foreign-attachment probe PVC bound before provider detachment")
		}
		eventsJSON, err := backend.kubectl(waitCtx, request, nil, "-n", request.DriverNamespace, "get", "events", "--field-selector=involvedObject.kind=PersistentVolumeClaim,involvedObject.name="+pvcName, "-o", "json")
		if err != nil {
			return err
		}
		var events struct {
			Items []struct {
				Type    string `json:"type"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"items"`
		}
		if err := json.Unmarshal(eventsJSON, &events); err != nil {
			return fmt.Errorf("decode foreign-attachment probe events: %w", err)
		}
		for _, event := range events.Items {
			// A generic provisioning error is not evidence for the foreign-
			// attachment invariant. Bind success to the exact disposable
			// Instance identity and the driver's fail-closed classification.
			if event.Type == "Warning" && event.Reason == "ProvisioningFailed" &&
				strings.Contains(event.Message, foreignInstanceID) && strings.Contains(event.Message, "unknown Instance") {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for fail-closed provisioning evidence: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// plannedPoolCSINodeIDs binds provider attachment evidence to the exact fresh
// node pool recorded in the cleanup inventory. Kapsule supplies the pool
// membership; CSINode supplies the driver-visible <zone>/<Instance ID> used by
// the CSI and provider attachment contracts.
func (backend *scalewayBackend) plannedPoolCSINodeIDs(ctx context.Context, request e2erunner.Request, inventory e2ecleanup.Inventory) ([]string, error) {
	region := scw.Region(backend.plan.Region)
	clusterID := resourceID(inventory, e2ecleanup.ResourceKindCluster, 0)
	poolID := resourceID(inventory, e2ecleanup.ResourceKindNodePool, 0)
	nodes, err := backend.kubernetes.ListNodes(&k8sapi.ListNodesRequest{
		Region: region, ClusterID: clusterID, PoolID: &poolID,
	}, scw.WithAllPages(), scw.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("list exact planned Kapsule pool nodes: %w", err)
	}
	observedNodes := 0
	if nodes != nil {
		observedNodes = len(nodes.Nodes)
	}
	if observedNodes != int(backend.plan.NodePool.Count) {
		return nil, fmt.Errorf("planned Kapsule pool exposes %d nodes, want %d", observedNodes, backend.plan.NodePool.Count)
	}
	plannedNames := make(map[string]struct{}, len(nodes.Nodes))
	for _, node := range nodes.Nodes {
		if node == nil || node.Name == "" || node.PoolID != poolID || node.ClusterID != clusterID || node.Region != region || node.Status != k8sapi.NodeStatusReady {
			return nil, fmt.Errorf("planned Kapsule pool contains an incomplete or non-ready node")
		}
		if _, duplicate := plannedNames[node.Name]; duplicate {
			return nil, fmt.Errorf("planned Kapsule pool repeats node %q", node.Name)
		}
		plannedNames[node.Name] = struct{}{}
	}

	driverObjects, err := backend.kubectl(ctx, request, nil, "get", "csidriver", "-l", "app.kubernetes.io/instance="+request.HelmRelease, "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list exact release CSIDriver: %w", err)
	}
	csiNodeObjects, err := backend.kubectl(ctx, request, nil, "get", "csinodes", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list CSINodes for planned pool: %w", err)
	}
	return decodePlannedCSINodeIDs(driverObjects, csiNodeObjects, plannedNames)
}

func decodePlannedCSINodeIDs(driverObjects, csiNodeObjects []byte, plannedNames map[string]struct{}) ([]string, error) {
	if len(plannedNames) == 0 {
		return nil, fmt.Errorf("planned Kapsule node-name set is empty")
	}
	var drivers struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(driverObjects, &drivers); err != nil {
		return nil, fmt.Errorf("decode exact release CSIDriver: %w", err)
	}
	if len(drivers.Items) != 1 || drivers.Items[0].Metadata.Name == "" {
		return nil, fmt.Errorf("exact release CSIDriver inventory must contain one named object")
	}
	driverName := drivers.Items[0].Metadata.Name

	var csiNodes struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Drivers []struct {
					Name   string `json:"name"`
					NodeID string `json:"nodeID"`
				} `json:"drivers"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(csiNodeObjects, &csiNodes); err != nil {
		return nil, fmt.Errorf("decode CSINodes for planned pool: %w", err)
	}
	plannedNodeIDs := make([]string, 0, len(plannedNames))
	seenNames := make(map[string]struct{}, len(plannedNames))
	for _, node := range csiNodes.Items {
		if _, planned := plannedNames[node.Metadata.Name]; !planned {
			continue
		}
		if _, duplicate := seenNames[node.Metadata.Name]; duplicate {
			return nil, fmt.Errorf("CSINode inventory repeats planned node %q", node.Metadata.Name)
		}
		seenNames[node.Metadata.Name] = struct{}{}
		matchingNodeID := ""
		for _, driver := range node.Spec.Drivers {
			if driver.Name != driverName {
				continue
			}
			if matchingNodeID != "" || driver.NodeID == "" {
				return nil, fmt.Errorf("CSINode %q has ambiguous %q registration", node.Metadata.Name, driverName)
			}
			matchingNodeID = driver.NodeID
		}
		if matchingNodeID == "" {
			return nil, fmt.Errorf("CSINode %q lacks %q registration", node.Metadata.Name, driverName)
		}
		if _, err := driverscaleway.ParseNodeID(matchingNodeID); err != nil {
			return nil, fmt.Errorf("parse planned CSINode %q identity: %w", node.Metadata.Name, err)
		}
		plannedNodeIDs = append(plannedNodeIDs, matchingNodeID)
	}
	if len(plannedNodeIDs) != len(plannedNames) {
		return nil, fmt.Errorf("CSINode inventory covers %d of %d planned pool nodes", len(plannedNodeIDs), len(plannedNames))
	}
	slices.Sort(plannedNodeIDs)
	return plannedNodeIDs, nil
}

// validateProviderAttachmentEvidence proves that the logical-volume smoke did
// not create an unbounded provider attachment fan-out. The same Instance may
// legitimately appear once for each parent, but never twice for one parent.
func validateProviderAttachmentEvidence(evidence providerAttachmentEvidence, expectedZone string, parentCount uint32, requireEveryPlannedNode bool) error {
	if len(evidence.Parents) != int(parentCount) || parentCount == 0 || len(evidence.PlannedNodeIDs) < 2 {
		return fmt.Errorf("provider attachment evidence does not cover the exact parent pool")
	}
	attachmentIDs := make(map[string]struct{})
	filesystemIDs := make(map[string]struct{})
	plannedResourceIDs := make(map[string]struct{}, len(evidence.PlannedNodeIDs))
	for _, nodeID := range evidence.PlannedNodeIDs {
		target, err := driverscaleway.ParseNodeID(nodeID)
		if err != nil || target.Zone != expectedZone {
			return fmt.Errorf("provider attachment evidence contains invalid planned node %q", nodeID)
		}
		if _, duplicate := plannedResourceIDs[target.ServerID]; duplicate {
			return fmt.Errorf("provider attachment evidence repeats planned Instance %s", target.ServerID)
		}
		plannedResourceIDs[target.ServerID] = struct{}{}
	}
	resourceIDs := make(map[string]struct{})
	totalAttachments := 0
	for _, parent := range evidence.Parents {
		if parent.FilesystemID == "" {
			return fmt.Errorf("provider attachment evidence contains an empty filesystem ID")
		}
		if parent.FilesystemStatus != fileapi.FileSystemStatusAvailable.String() {
			return fmt.Errorf("provider attachment parent %s has status %q", parent.FilesystemID, parent.FilesystemStatus)
		}
		if _, duplicate := filesystemIDs[parent.FilesystemID]; duplicate {
			return fmt.Errorf("provider attachment evidence repeats filesystem %s", parent.FilesystemID)
		}
		filesystemIDs[parent.FilesystemID] = struct{}{}
		count := len(parent.AttachmentIDs)
		if uint32(count) != parent.ReportedAttachments || len(parent.ResourceIDs) != count || len(parent.ResourceTypes) != count || len(parent.Zones) != count {
			return fmt.Errorf("provider attachment surfaces disagree for %s", parent.FilesystemID)
		}
		parentResources := make(map[string]struct{}, count)
		for index, attachmentID := range parent.AttachmentIDs {
			resourceID := parent.ResourceIDs[index]
			if attachmentID == "" || resourceID == "" {
				return fmt.Errorf("provider attachment evidence for %s is incomplete", parent.FilesystemID)
			}
			if _, duplicate := attachmentIDs[attachmentID]; duplicate {
				return fmt.Errorf("provider attachment evidence repeats attachment %s", attachmentID)
			}
			attachmentIDs[attachmentID] = struct{}{}
			if _, duplicate := parentResources[resourceID]; duplicate {
				return fmt.Errorf("provider attachment evidence repeats resource %s for parent %s", resourceID, parent.FilesystemID)
			}
			parentResources[resourceID] = struct{}{}
			if _, planned := plannedResourceIDs[resourceID]; !planned {
				return fmt.Errorf("provider attachment %s references foreign Instance %s", attachmentID, resourceID)
			}
			resourceIDs[resourceID] = struct{}{}
			if parent.ResourceTypes[index] != fileapi.AttachmentResourceTypeInstanceServer.String() {
				return fmt.Errorf("provider attachment %s has unsupported resource type %q", attachmentID, parent.ResourceTypes[index])
			}
			if parent.Zones[index] != expectedZone {
				return fmt.Errorf("provider attachment %s is in zone %q instead of %q", attachmentID, parent.Zones[index], expectedZone)
			}
		}
		totalAttachments += count
	}
	if len(resourceIDs) < 2 {
		return fmt.Errorf("provider inventory spans %d planned Instances after cross-node mounts; expected at least 2", len(resourceIDs))
	}
	if requireEveryPlannedNode && len(resourceIDs) != len(plannedResourceIDs) {
		return fmt.Errorf("provider inventory spans %d of %d planned Instances", len(resourceIDs), len(plannedResourceIDs))
	}
	if totalAttachments > int(parentCount)*len(plannedResourceIDs) {
		return fmt.Errorf("provider inventory contains %d attachments for %d parents and %d planned nodes", totalAttachments, parentCount, len(plannedResourceIDs))
	}
	return nil
}

func (backend *scalewayBackend) providerGrowthScenario(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory, evidenceDirectory string) (e2erunner.ScenarioResult, error) {
	region := scw.Region(plan.Region)
	filesystemID := resourceID(inventory, e2ecleanup.ResourceKindParent, 1)
	controllerBefore, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read Ready controller before parent growth: %w", err)
	}
	if !podReady(controllerBefore) {
		return e2erunner.ScenarioResult{}, fmt.Errorf("controller is not Ready before parent growth")
	}
	before, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
	if err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read parent before growth: %w", err)
	}
	if before == nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read parent before growth: provider returned an empty response")
	}
	if before.Status != fileapi.FileSystemStatusAvailable {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read parent before growth: provider status is %q", before.Status.String())
	}
	newSize := uint64(before.Size) + fileStorageSizeStep
	if newSize > 50_000_000_000_000 {
		return e2erunner.ScenarioResult{}, fmt.Errorf("parent growth would exceed the File Storage maximum")
	}
	if _, err := backend.file.UpdateFileSystem(&fileapi.UpdateFileSystemRequest{Region: region, FilesystemID: filesystemID, Size: &newSize}, scw.WithContext(ctx)); err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("grow exact run-owned parent: %w", err)
	}
	if _, err := backend.file.WaitForFileSystem(&fileapi.WaitForFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx)); err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("wait for grown parent availability: %w", err)
	}
	after, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
	if err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("reread grown parent size: %w", err)
	}
	if after == nil || uint64(after.Size) != newSize || after.Status != fileapi.FileSystemStatusAvailable {
		return e2erunner.ScenarioResult{}, fmt.Errorf("reread grown parent size: provider did not return the exact requested size")
	}

	// Restart only the singleton controller after the provider has returned to
	// available. This forces a fresh authoritative inventory before the probe
	// allocation and avoids waiting for a cache interval as a correctness rule.
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "restart", "deployment", "-l", "app.kubernetes.io/instance="+request.HelmRelease+",app.kubernetes.io/component=controller"); err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "rollout", "status", "deployment", "-l", "app.kubernetes.io/instance="+request.HelmRelease+",app.kubernetes.io/component=controller", "--timeout=20m"); err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	controllerAfter, err := backend.waitForReadyReplacementPod(ctx, request, controllerSelector, "", controllerBefore.Metadata.UID)
	if err != nil {
		return e2erunner.ScenarioResult{}, err
	}

	var probePVC, requestName, allocationParent string
	for index := 0; index < 10 && allocationParent != filesystemID; index++ {
		probePVC = fmt.Sprintf("e2e-growth-%s-%02d", plan.RunID[:8], index)
		manifest := fmt.Sprintf("apiVersion: v1\nkind: PersistentVolumeClaim\nmetadata:\n  name: %s\n  namespace: %s\n  labels:\n    sfs-subdir-e2e-run: %q\nspec:\n  accessModes: [ReadWriteMany]\n  storageClassName: sfs-subdir-rwx\n  resources: {requests: {storage: 16Mi}}\n", probePVC, request.DriverNamespace, plan.RunID)
		if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
			return e2erunner.ScenarioResult{}, err
		}
		if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "wait", "pvc/"+probePVC, "--for=jsonpath={.status.phase}=Bound", "--timeout=10m"); err != nil {
			return e2erunner.ScenarioResult{}, err
		}
		uid, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "pvc/"+probePVC, "-o", "jsonpath={.metadata.uid}")
		if err != nil {
			return e2erunner.ScenarioResult{}, fmt.Errorf("read growth probe PVC UID: %w", err)
		}
		if strings.TrimSpace(string(uid)) == "" {
			return e2erunner.ScenarioResult{}, fmt.Errorf("growth probe PVC has an empty UID")
		}
		requestName = "pvc-" + strings.TrimSpace(string(uid))
		allocationParent, err = backend.allocationParent(ctx, request, requestName)
		if err != nil {
			return e2erunner.ScenarioResult{}, err
		}
	}
	if allocationParent != filesystemID {
		return e2erunner.ScenarioResult{}, fmt.Errorf("new placements did not use the freshly observed grown parent within the bounded probe set")
	}
	proof := e2erunner.ParentGrowthProof{
		SchemaVersion: "1", Scenario: "parent-growth", RunID: plan.RunID,
		ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), FilesystemID: filesystemID,
		PreviousSizeBytes: uint64(before.Size), RequestedSizeBytes: newSize, ObservedSizeBytes: uint64(after.Size),
		GrowthStepBytes: fileStorageSizeStep, ObservedStatus: after.Status.String(),
		ControllerPodUIDBefore: controllerBefore.Metadata.UID, ControllerPodUIDAfter: controllerAfter.Metadata.UID,
		ProbePVC: probePVC, ProbeRequestName: requestName, AllocationParentID: allocationParent, FreshAvailableRead: true,
	}
	if err := proof.Validate(); err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	return writeScenarioJSON(evidenceDirectory, "parent-growth", proof)
}

func (backend *scalewayBackend) allocationParent(ctx context.Context, request e2erunner.Request, requestName string) (string, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace, "get", "configmaps", "-l", "app.kubernetes.io/name=scaleway-sfs-subdir-csi", "-o", "json")
	if err != nil {
		return "", err
	}
	var objects struct {
		Items []struct {
			Data map[string]string `json:"data"`
		} `json:"items"`
	}
	if err := json.Unmarshal(encoded, &objects); err != nil {
		return "", fmt.Errorf("decode allocation ConfigMaps: %w", err)
	}
	for _, object := range objects.Items {
		recordBytes, present := object.Data["record.json"]
		if !present {
			continue
		}
		var record struct {
			CreateVolumeRequestName string `json:"createVolumeRequestName"`
			ParentFilesystemID      string `json:"parentFilesystemID"`
		}
		if err := json.Unmarshal([]byte(recordBytes), &record); err != nil {
			return "", fmt.Errorf("decode allocation record during growth proof: %w", err)
		}
		if record.CreateVolumeRequestName == requestName {
			return record.ParentFilesystemID, nil
		}
	}
	return "", fmt.Errorf("growth probe allocation %q is absent", requestName)
}

func (backend *scalewayBackend) kubectl(ctx context.Context, request e2erunner.Request, stdin *strings.Reader, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "kubectl", arguments...)
	command.Env = append(environmentWithoutScalewayCredentials(), "KUBECONFIG="+backend.kubeconfig)
	if stdin != nil {
		command.Stdin = stdin
	}
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("kubectl %s failed: %w: %s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func writeScenarioJSON(directory, name string, value any) (e2erunner.ScenarioResult, error) {
	encoded, err := canonicaljson.Marshal(value)
	if err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	fileName := name + ".json"
	path := filepath.Join(directory, fileName)
	if err := replaceDurableFile(path, append(encoded, '\n'), 0o600); err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	digest, err := fileSHA256(path)
	if err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	return e2erunner.ScenarioResult{Name: name, Succeeded: true, EvidenceFile: fileName, EvidenceSHA: digest, Proof: encoded}, nil
}
