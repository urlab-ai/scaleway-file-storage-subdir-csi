package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	fileapi "github.com/scaleway/scaleway-sdk-go/api/file/v1alpha1"
	"github.com/scaleway/scaleway-sdk-go/scw"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/e2ecleanup"
	"scaleway-sfs-subdir-csi/internal/e2eplan"
	"scaleway-sfs-subdir-csi/internal/e2erunner"
)

const fileStorageSizeStep = uint64(100_000_000_000)

type providerAttachmentEvidence struct {
	SchemaVersion string                     `json:"schemaVersion"`
	ObservedAt    string                     `json:"observedAt"`
	Parents       []providerParentAttachment `json:"parents"`
}

type providerParentAttachment struct {
	FilesystemID        string   `json:"filesystemId"`
	FilesystemStatus    string   `json:"filesystemStatus"`
	ReportedAttachments uint32   `json:"reportedAttachments"`
	AttachmentIDs       []string `json:"attachmentIds"`
	ResourceIDs         []string `json:"resourceIds"`
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
	attachmentResult, err := backend.providerAttachmentScenario(ctx, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	growthResult, err := backend.providerGrowthScenario(ctx, request, plan, inventory, evidenceDirectory)
	if err != nil {
		return nil, err
	}
	return []e2erunner.ScenarioResult{attachmentResult, growthResult}, nil
}

func (backend *scalewayBackend) providerAttachmentScenario(ctx context.Context, inventory e2ecleanup.Inventory, evidenceDirectory string) (e2erunner.ScenarioResult, error) {
	region := scw.Region(backend.plan.Region)
	evidence := providerAttachmentEvidence{SchemaVersion: "1", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	totalAttachments := 0
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
			if attachment == nil || attachment.FilesystemID != filesystemID || attachment.ID == "" || attachment.ResourceID == "" {
				return e2erunner.ScenarioResult{}, fmt.Errorf("provider attachment inventory for %s is incomplete", filesystemID)
			}
			parent.AttachmentIDs = append(parent.AttachmentIDs, attachment.ID)
			parent.ResourceIDs = append(parent.ResourceIDs, attachment.ResourceID)
		}
		if uint32(len(parent.AttachmentIDs)) != parent.ReportedAttachments {
			return e2erunner.ScenarioResult{}, fmt.Errorf("provider attachment surfaces disagree for %s", filesystemID)
		}
		totalAttachments += len(parent.AttachmentIDs)
		evidence.Parents = append(evidence.Parents, parent)
	}
	if totalAttachments == 0 {
		return e2erunner.ScenarioResult{}, fmt.Errorf("provider inventory contains no live parent attachment after mounted workloads")
	}
	return writeScenarioJSON(evidenceDirectory, "provider-attach-detach", evidence)
}

func (backend *scalewayBackend) providerGrowthScenario(ctx context.Context, request e2erunner.Request, plan e2eplan.Plan, inventory e2ecleanup.Inventory, evidenceDirectory string) (e2erunner.ScenarioResult, error) {
	region := scw.Region(plan.Region)
	filesystemID := resourceID(inventory, e2ecleanup.ResourceKindParent, 1)
	before, err := backend.file.GetFileSystem(&fileapi.GetFileSystemRequest{Region: region, FilesystemID: filesystemID}, scw.WithContext(ctx))
	if err != nil {
		return e2erunner.ScenarioResult{}, fmt.Errorf("read parent before growth: %w", err)
	}
	newSize := uint64(before.Size) + fileStorageSizeStep
	if newSize > 10_000_000_000_000 {
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
	command.Env = append(os.Environ(), "KUBECONFIG="+backend.kubeconfig)
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
