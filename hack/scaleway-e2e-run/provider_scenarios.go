package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	driverscaleway "github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/scaleway"
)

const fileStorageSizeStep = uint64(100_000_000_000)

type providerAttachmentEvidence struct {
	SchemaVersion  string                     `json:"schemaVersion"`
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

type providerGrowthEvidence struct {
	SchemaVersion      string `json:"schemaVersion"`
	ObservedAt         string `json:"observedAt"`
	FilesystemID       string `json:"filesystemId"`
	PreviousSizeBytes  uint64 `json:"previousSizeBytes"`
	ObservedSizeBytes  uint64 `json:"observedSizeBytes"`
	ProbePVC           string `json:"probePvc"`
	ProbeRequestName   string `json:"probeRequestName"`
	AllocationParentID string `json:"allocationParentId"`
}

func (backend *scalewayBackend) runProviderScenarios(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory, evidenceDirectory string) ([]e2erunner.ScenarioResult, error) {
	attachmentResult, err := backend.providerAttachmentScenario(ctx, request, inventory, evidenceDirectory, "provider-attach-detach")
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
	region := scw.Region(backend.plan.Region)
	plannedNodeIDs, err := backend.plannedPoolCSINodeIDs(ctx, request, inventory)
	if err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	evidence := providerAttachmentEvidence{
		SchemaVersion: "1", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), PlannedNodeIDs: plannedNodeIDs,
	}
	for ordinal := 0; ordinal < int(backend.plan.Parents.Count); ordinal++ {
		filesystemID := resourceID(inventory, e2ecleanup.ResourceKindParent, ordinal)
		filesystem, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
		if err != nil {
			return e2erunner.ScenarioResult{}, fmt.Errorf("read provider attachment parent %s: %w", filesystemID, err)
		}
		listed, err := backend.file.ListAttachments(&fileapi.ListAttachmentsRequest{Region: region, FilesystemID: &filesystemID}, scw.WithAllPages(), scw.WithContext(ctx))
		if err != nil {
			return e2erunner.ScenarioResult{}, fmt.Errorf("list provider attachments for %s: %w", filesystemID, err)
		}
		parent := providerParentAttachment{FilesystemID: filesystemID, FilesystemStatus: filesystem.Status.String(), ReportedAttachments: filesystem.NumberOfAttachments}
		for _, attachment := range listed.Attachments {
			if attachment == nil || attachment.FilesystemID != filesystemID || attachment.ID == "" || attachment.ResourceID == "" || attachment.Zone == nil {
				return e2erunner.ScenarioResult{}, fmt.Errorf("provider attachment inventory for %s is incomplete", filesystemID)
			}
			parent.AttachmentIDs = append(parent.AttachmentIDs, attachment.ID)
			parent.ResourceIDs = append(parent.ResourceIDs, attachment.ResourceID)
			parent.ResourceTypes = append(parent.ResourceTypes, attachment.ResourceType.String())
			parent.Zones = append(parent.Zones, attachment.Zone.String())
		}
		evidence.Parents = append(evidence.Parents, parent)
	}
	if err := validateProviderAttachmentEvidence(evidence, request.Zone, backend.plan.Parents.Count, backend.plan.Profile == e2eplan.ProfileBase); err != nil {
		return e2erunner.ScenarioResult{}, err
	}
	return writeScenarioJSON(evidenceDirectory, evidenceName, evidence)
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
	before, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
	if err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read parent before growth: %w", err)
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
	if err != nil || uint64(after.Size) != newSize {
		return e2erunner.ScenarioResult{}, fmt.Errorf("reread grown parent size: %w", err)
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
		if err != nil || strings.TrimSpace(string(uid)) == "" {
			return e2erunner.ScenarioResult{}, fmt.Errorf("read growth probe PVC UID: %w", err)
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
	evidence := providerGrowthEvidence{SchemaVersion: "1", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), FilesystemID: filesystemID,
		PreviousSizeBytes: uint64(before.Size), ObservedSizeBytes: uint64(after.Size), ProbePVC: probePVC,
		ProbeRequestName: requestName, AllocationParentID: allocationParent}
	return writeScenarioJSON(evidenceDirectory, "parent-growth", evidence)
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
	return e2erunner.ScenarioResult{Name: name, Succeeded: true, EvidenceFile: fileName, EvidenceSHA: digest}, nil
}
