package main

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
)

func checkpointStoppedWorkloadFixture() (
	e2eplan.Plan,
	appsv1.Deployment,
	corev1.PersistentVolumeClaim,
	corev1.PersistentVolume,
) {
	plan := e2eplan.Plan{RunID: recoveryTestRunID}
	replicas := int32(0)
	labels := map[string]string{
		"sfs-subdir-e2e-run":      plan.RunID,
		"sfs-subdir-e2e-scenario": "checkpoint",
	}
	templateLabels := map[string]string{
		"sfs-subdir-e2e-run":      plan.RunID,
		"sfs-subdir-e2e-scenario": "checkpoint",
		"sfs-subdir-e2e-workload": "checkpoint-workload",
	}
	deployment := appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "checkpoint-workload", Namespace: "checkpoint-system", Labels: labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"sfs-subdir-e2e-workload": "checkpoint-workload"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: templateLabels},
				Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "checkpoint-data",
						},
					},
				}}},
			},
		},
	}
	claimUID := types.UID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "checkpoint-data", Namespace: "checkpoint-system", UID: claimUID, Labels: labels,
		},
		Spec:   corev1.PersistentVolumeClaimSpec{VolumeName: "pv-checkpoint"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-checkpoint"},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{
				Name: "checkpoint-data", Namespace: "checkpoint-system", UID: claimUID,
			},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "file-storage-subdir.csi.urlab.ai",
				},
			},
		},
	}
	return plan, deployment, pvc, pv
}

func TestValidateCheckpointWorkloadStoppedRequiresDurableUnpublish(t *testing.T) {
	plan, deployment, pvc, pv := checkpointStoppedWorkloadFixture()
	validate := func(
		deployment appsv1.Deployment,
		pods kubernetesPodList,
		pvc corev1.PersistentVolumeClaim,
		pv corev1.PersistentVolume,
		attachments storagev1.VolumeAttachmentList,
	) (bool, error) {
		return validateCheckpointWorkloadStopped(
			plan, "checkpoint-system", "checkpoint-data", "checkpoint-workload",
			"pv-checkpoint", deployment, pods, pvc, pv, attachments,
		)
	}

	stopped, err := validate(deployment, kubernetesPodList{}, pvc, pv, storagev1.VolumeAttachmentList{})
	if err != nil || !stopped {
		t.Fatalf("cleanly unpublished checkpoint workload = %t, %v", stopped, err)
	}

	t.Run("desired replica still present", func(t *testing.T) {
		one := int32(1)
		changed := deployment.DeepCopy()
		changed.Spec.Replicas = &one
		stopped, err := validate(*changed, kubernetesPodList{}, pvc, pv, storagev1.VolumeAttachmentList{})
		if err != nil || stopped {
			t.Fatalf("replicas=1 checkpoint workload = %t, %v; want pending", stopped, err)
		}
	})
	t.Run("Pod still exists", func(t *testing.T) {
		var pod kubernetesPod
		pod.Metadata.Labels = map[string]string{
			"sfs-subdir-e2e-run":      plan.RunID,
			"sfs-subdir-e2e-scenario": "checkpoint",
			"sfs-subdir-e2e-workload": "checkpoint-workload",
		}
		stopped, err := validate(
			deployment, kubernetesPodList{Items: []kubernetesPod{pod}},
			pvc, pv, storagev1.VolumeAttachmentList{},
		)
		if err != nil || stopped {
			t.Fatalf("existing checkpoint Pod = %t, %v; want pending", stopped, err)
		}
	})
	t.Run("VolumeAttachment still exists", func(t *testing.T) {
		pvName := "pv-checkpoint"
		attachments := storagev1.VolumeAttachmentList{Items: []storagev1.VolumeAttachment{{
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: "file-storage-subdir.csi.urlab.ai",
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
		}}}
		stopped, err := validate(deployment, kubernetesPodList{}, pvc, pv, attachments)
		if err != nil || stopped {
			t.Fatalf("attached checkpoint PV = %t, %v; want pending", stopped, err)
		}
		attachments.Items[0].Spec.Attacher = "foreign.csi.example"
		if _, err := validate(deployment, kubernetesPodList{}, pvc, pv, attachments); err == nil {
			t.Fatal("foreign VolumeAttachment for checkpoint PV was accepted")
		}
	})
	t.Run("PVC or Deployment identity changed", func(t *testing.T) {
		changedPVC := pvc.DeepCopy()
		changedPVC.Spec.VolumeName = "foreign-pv"
		if _, err := validate(
			deployment, kubernetesPodList{}, *changedPVC, pv, storagev1.VolumeAttachmentList{},
		); err == nil {
			t.Fatal("checkpoint PVC bound to another PV was accepted")
		}
		changedDeployment := deployment.DeepCopy()
		changedDeployment.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "foreign-claim"
		if _, err := validate(
			*changedDeployment, kubernetesPodList{}, pvc, pv, storagev1.VolumeAttachmentList{},
		); err == nil {
			t.Fatal("checkpoint Deployment referencing another PVC was accepted")
		}
	})
}
