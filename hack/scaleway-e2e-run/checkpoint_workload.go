package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

// stopCheckpointWorkload cleanly releases the logical volume while the full
// CSI release still exists. Deleting the driver namespace with a live consumer
// can strand Kapsule node deletion behind a mount that no longer has a node
// plugin available to unpublish it. The PVC, PV, and on-disk marker deliberately
// remain; only the exact run-owned Deployment is scaled to zero.
func (backend *scalewayBackend) stopCheckpointWorkload(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
	persistentVolume string,
) error {
	if err := backend.validateCheckpointDeployment(ctx, request, plan, namespace, claim, deployment); err != nil {
		return err
	}
	_, scaleErr := backend.kubectl(ctx, request, nil,
		"-n", namespace, "scale", "deployment/"+deployment, "--replicas=0",
	)

	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	var lastObservationErr error
	for {
		stopped, err := backend.checkpointWorkloadStopped(
			waitCtx, request, plan, namespace, claim, deployment, persistentVolume,
		)
		switch {
		case err == nil && stopped:
			// The authoritative Kubernetes state resolves a lost scale
			// response without retrying the mutation.
			return nil
		case err != nil:
			lastObservationErr = err
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf(
				"wait for exact checkpoint workload unpublish before driver namespace deletion: %w",
				errors.Join(scaleErr, lastObservationErr, waitCtx.Err()),
			)
		case <-ticker.C:
		}
	}
}

// startCheckpointWorkload restores only the exact run-owned Deployment after
// the controller and node plugin have both recovered. The subsequent marker
// read proves that the preserved PV was remounted rather than reprovisioned.
func (backend *scalewayBackend) startCheckpointWorkload(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
) error {
	if err := backend.validateCheckpointDeployment(ctx, request, plan, namespace, claim, deployment); err != nil {
		return err
	}
	if _, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "scale", "deployment/"+deployment, "--replicas=1",
	); err != nil {
		// Scale can commit while its response is lost. Accept that ambiguity
		// only if the exact Deployment now reports the requested durable spec.
		encoded, readErr := backend.kubectl(ctx, request, nil,
			"-n", namespace, "get", "deployment/"+deployment, "-o", "json",
		)
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		var observed appsv1.Deployment
		if decodeErr := json.Unmarshal(encoded, &observed); decodeErr != nil {
			return errors.Join(err, decodeErr)
		}
		if validateErr := validateCheckpointDeploymentObject(
			observed, plan, namespace, claim, deployment,
		); validateErr != nil {
			return errors.Join(err, validateErr)
		}
		if observed.Spec.Replicas == nil || *observed.Spec.Replicas != 1 {
			return errors.Join(err, fmt.Errorf("checkpoint workload Deployment did not retain replicas=1"))
		}
	}
	return nil
}

func (backend *scalewayBackend) validateCheckpointDeployment(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
) error {
	encoded, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "deployment/"+deployment, "-o", "json",
	)
	if err != nil {
		return fmt.Errorf("read exact checkpoint workload Deployment: %w", err)
	}
	var observed appsv1.Deployment
	if err := json.Unmarshal(encoded, &observed); err != nil {
		return fmt.Errorf("decode exact checkpoint workload Deployment: %w", err)
	}
	return validateCheckpointDeploymentObject(observed, plan, namespace, claim, deployment)
}

func validateCheckpointDeploymentObject(
	observed appsv1.Deployment,
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
) error {
	if observed.Name != deployment || observed.Namespace != namespace ||
		observed.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		observed.Labels["sfs-subdir-e2e-scenario"] != "checkpoint" ||
		observed.Spec.Selector == nil ||
		observed.Spec.Selector.MatchLabels["sfs-subdir-e2e-workload"] != deployment ||
		observed.Spec.Template.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		observed.Spec.Template.Labels["sfs-subdir-e2e-scenario"] != "checkpoint" ||
		observed.Spec.Template.Labels["sfs-subdir-e2e-workload"] != deployment {
		return fmt.Errorf("checkpoint workload Deployment differs from the exact run-owned identity")
	}
	claimFound := false
	for _, volume := range observed.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil &&
			volume.PersistentVolumeClaim.ClaimName == claim {
			claimFound = true
		}
	}
	if !claimFound {
		return fmt.Errorf("checkpoint workload Deployment no longer references its exact PVC")
	}
	return nil
}

func (backend *scalewayBackend) checkpointWorkloadStopped(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
	persistentVolume string,
) (bool, error) {
	podsJSON, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "pods", "-l", "sfs-subdir-e2e-workload="+deployment, "-o", "json",
	)
	if err != nil {
		return false, fmt.Errorf("observe checkpoint workload Pods: %w", err)
	}
	deploymentJSON, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "deployment/"+deployment, "-o", "json",
	)
	if err != nil {
		return false, fmt.Errorf("observe checkpoint workload Deployment: %w", err)
	}
	pvcJSON, err := backend.kubectl(ctx, request, nil,
		"-n", namespace, "get", "pvc/"+claim, "-o", "json",
	)
	if err != nil {
		return false, fmt.Errorf("observe preserved checkpoint PVC: %w", err)
	}
	pvJSON, err := backend.kubectl(ctx, request, nil,
		"get", "pv/"+persistentVolume, "-o", "json",
	)
	if err != nil {
		return false, fmt.Errorf("observe preserved checkpoint PV: %w", err)
	}
	attachmentsJSON, err := backend.kubectl(ctx, request, nil,
		"get", "volumeattachments", "-o", "json",
	)
	if err != nil {
		return false, fmt.Errorf("observe checkpoint VolumeAttachments: %w", err)
	}

	var pods kubernetesPodList
	var observedDeployment appsv1.Deployment
	var pvc corev1.PersistentVolumeClaim
	var pv corev1.PersistentVolume
	var attachments storagev1.VolumeAttachmentList
	if err := json.Unmarshal(podsJSON, &pods); err != nil {
		return false, fmt.Errorf("decode checkpoint workload Pods: %w", err)
	}
	if err := json.Unmarshal(deploymentJSON, &observedDeployment); err != nil {
		return false, fmt.Errorf("decode checkpoint workload Deployment: %w", err)
	}
	if err := json.Unmarshal(pvcJSON, &pvc); err != nil {
		return false, fmt.Errorf("decode preserved checkpoint PVC: %w", err)
	}
	if err := json.Unmarshal(pvJSON, &pv); err != nil {
		return false, fmt.Errorf("decode preserved checkpoint PV: %w", err)
	}
	if err := json.Unmarshal(attachmentsJSON, &attachments); err != nil {
		return false, fmt.Errorf("decode checkpoint VolumeAttachments: %w", err)
	}
	return validateCheckpointWorkloadStopped(
		plan, namespace, claim, deployment, persistentVolume,
		observedDeployment, pods, pvc, pv, attachments,
	)
}

func validateCheckpointWorkloadStopped(
	plan e2eplan.Plan,
	namespace string,
	claim string,
	deployment string,
	persistentVolume string,
	observedDeployment appsv1.Deployment,
	pods kubernetesPodList,
	pvc corev1.PersistentVolumeClaim,
	pv corev1.PersistentVolume,
	attachments storagev1.VolumeAttachmentList,
) (bool, error) {
	if err := validateCheckpointDeploymentObject(
		observedDeployment, plan, namespace, claim, deployment,
	); err != nil {
		return false, err
	}
	if observedDeployment.Spec.Replicas == nil || *observedDeployment.Spec.Replicas != 0 {
		return false, nil
	}
	for _, pod := range pods.Items {
		if pod.Metadata.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
			pod.Metadata.Labels["sfs-subdir-e2e-scenario"] != "checkpoint" ||
			pod.Metadata.Labels["sfs-subdir-e2e-workload"] != deployment {
			return false, fmt.Errorf("checkpoint workload selector returned a foreign Pod")
		}
	}
	if pvc.Name != claim || pvc.Namespace != namespace ||
		pvc.UID == "" ||
		pvc.Labels["sfs-subdir-e2e-run"] != plan.RunID ||
		pvc.Labels["sfs-subdir-e2e-scenario"] != "checkpoint" ||
		pvc.Status.Phase != corev1.ClaimBound ||
		pvc.Spec.VolumeName != persistentVolume {
		return false, fmt.Errorf("checkpoint PVC changed while stopping its workload")
	}
	if pv.Name != persistentVolume || pv.Spec.ClaimRef == nil ||
		pv.Spec.ClaimRef.Namespace != namespace || pv.Spec.ClaimRef.Name != claim ||
		pv.Spec.ClaimRef.UID != pvc.UID ||
		pv.Spec.CSI == nil || pv.Spec.CSI.Driver == "" {
		return false, fmt.Errorf("checkpoint PV changed while stopping its workload")
	}
	if len(pods.Items) != 0 {
		return false, nil
	}
	for _, attachment := range attachments.Items {
		if attachment.Spec.Source.PersistentVolumeName == nil ||
			*attachment.Spec.Source.PersistentVolumeName != persistentVolume {
			continue
		}
		if attachment.Spec.Attacher != pv.Spec.CSI.Driver {
			return false, fmt.Errorf("checkpoint PV has a VolumeAttachment owned by another CSI driver")
		}
		return false, nil
	}
	return true, nil
}
