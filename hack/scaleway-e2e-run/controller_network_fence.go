package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
)

const controllerFenceLabel = "sfs-subdir-e2e-controller-fence"

type controllerNetworkFence struct {
	PolicyName       string
	LabelValue       string
	ControllerPod    string
	ControllerPodUID string
	FencedLease      kubernetesLease
}

func newControllerNetworkFence(plan e2eplan.Plan, controller kubernetesPod) controllerNetworkFence {
	shortRun := plan.RunID[:8]
	return controllerNetworkFence{
		PolicyName:       "e2e-controller-fence-" + shortRun,
		LabelValue:       shortRun,
		ControllerPod:    controller.Metadata.Name,
		ControllerPodUID: controller.Metadata.UID,
	}
}

func controllerNetworkFenceManifest(
	request e2erunner.Request,
	plan e2eplan.Plan,
	fence controllerNetworkFence,
) string {
	return fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/instance: %q
    sfs-subdir-e2e-run: %q
    sfs-subdir-e2e-scenario: controller-hard-failure
spec:
  podSelector:
    matchLabels:
      %s: %q
  policyTypes:
    - Egress
  egress: []
`, fence.PolicyName, request.DriverNamespace, request.HelmRelease, plan.RunID,
		controllerFenceLabel, fence.LabelValue)
}

// applyControllerNetworkFence makes a later guest shutdown unable to turn the
// deliberately frozen controller into a graceful handoff. The policy selects
// only the already-created Pod through an ad-hoc label that is not present on
// the Deployment template. A replacement Pod therefore retains API access.
func (backend *scalewayBackend) applyControllerNetworkFence(
	ctx context.Context,
	request e2erunner.Request,
	plan e2eplan.Plan,
	controller kubernetesPod,
) (controllerNetworkFence, error) {
	fence := newControllerNetworkFence(plan, controller)
	manifest := controllerNetworkFenceManifest(request, plan, fence)
	if _, err := backend.kubectl(ctx, request, strings.NewReader(manifest), "apply", "-f", "-"); err != nil {
		return controllerNetworkFence{}, fmt.Errorf("create exact controller egress fence: %w", err)
	}
	applied := true
	defer func() {
		if applied {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			_ = backend.deleteControllerNetworkFence(cleanupCtx, request, fence)
			cancel()
		}
	}()

	if _, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"label", "pod/"+fence.ControllerPod, controllerFenceLabel+"="+fence.LabelValue, "--overwrite=false",
	); err != nil {
		return controllerNetworkFence{}, fmt.Errorf("select exact controller Pod for egress fencing: %w", err)
	}
	observed, err := backend.readExactPod(ctx, request, fence.ControllerPod)
	if err != nil {
		return controllerNetworkFence{}, err
	}
	if observed.Metadata.UID != fence.ControllerPodUID ||
		observed.Metadata.Labels[controllerFenceLabel] != fence.LabelValue {
		return controllerNetworkFence{}, fmt.Errorf("controller egress fence selected a changed Pod identity")
	}
	fencedLease, err := backend.waitControllerLeaseFenced(ctx, request, fence.ControllerPodUID)
	if err != nil {
		return controllerNetworkFence{}, err
	}
	fence.FencedLease = fencedLease
	applied = false
	return fence, nil
}

func (backend *scalewayBackend) waitControllerLeaseFenced(
	ctx context.Context,
	request e2erunner.Request,
	expectedHolder string,
) (kubernetesLease, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastRenewTime, lastResourceVersion string
	var unchangedSince time.Time
	for {
		lease, err := backend.readControllerLease(waitCtx, request)
		if err != nil {
			return kubernetesLease{}, fmt.Errorf("observe controller Lease during egress fencing: %w", err)
		}
		if lease.Spec.HolderIdentity != expectedHolder ||
			lease.Spec.RenewTime == "" ||
			lease.Spec.LeaseDurationSeconds <= 0 ||
			lease.Spec.LeaseDurationSeconds > 300 ||
			lease.Metadata.Annotations["gracefulReleaseState"] != "" {
			return kubernetesLease{}, fmt.Errorf("controller Lease identity changed while establishing the egress fence")
		}
		now := time.Now()
		if lease.Spec.RenewTime != lastRenewTime ||
			lease.Metadata.ResourceVersion != lastResourceVersion {
			lastRenewTime = lease.Spec.RenewTime
			lastResourceVersion = lease.Metadata.ResourceVersion
			unchangedSince = now
		} else if now.Sub(unchangedSince) >= 10*time.Second {
			return lease, nil
		}
		select {
		case <-waitCtx.Done():
			return kubernetesLease{}, fmt.Errorf("controller Lease kept renewing through the exact egress fence: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

// waitForBlockedController proves that the successor has remained non-serving
// across a complete old-Lease expiry window. A single early non-Ready
// observation is insufficient because every healthy Pod is briefly non-Ready
// during startup.
func (backend *scalewayBackend) waitForBlockedController(
	ctx context.Context,
	request e2erunner.Request,
	successor kubernetesPod,
	expectedLease kubernetesLease,
) (kubernetesLease, int64, int32, error) {
	initialLease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return kubernetesLease{}, 0, 0, fmt.Errorf("read initial uncleared Lease for blocked successor: %w", err)
	}
	if expectedLease.Metadata.UID == "" || expectedLease.Metadata.ResourceVersion == "" ||
		expectedLease.Spec.HolderIdentity == "" || expectedLease.Spec.RenewTime == "" ||
		expectedLease.Spec.LeaseDurationSeconds <= 0 ||
		initialLease.Metadata.UID != expectedLease.Metadata.UID ||
		initialLease.Metadata.ResourceVersion != expectedLease.Metadata.ResourceVersion ||
		initialLease.Spec.HolderIdentity != expectedLease.Spec.HolderIdentity ||
		initialLease.Spec.RenewTime != expectedLease.Spec.RenewTime ||
		initialLease.Spec.LeaseDurationSeconds != expectedLease.Spec.LeaseDurationSeconds ||
		initialLease.Spec.LeaseDurationSeconds <= 0 ||
		initialLease.Spec.LeaseDurationSeconds > 300 ||
		initialLease.Metadata.Annotations["holderPodUID"] != expectedLease.Spec.HolderIdentity ||
		initialLease.Metadata.Annotations["gracefulReleaseState"] != "" {
		return kubernetesLease{}, 0, 0, fmt.Errorf("initial uncleared Lease identity is invalid")
	}
	timeout := time.Duration(initialLease.Spec.LeaseDurationSeconds)*time.Second + 90*time.Second
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var stableSince time.Time
	expectedRenewTime := initialLease.Spec.RenewTime
	var expectedRestartCount int32
	for {
		current, err := backend.singularPod(waitCtx, request, controllerSelector, "")
		if err != nil {
			return kubernetesLease{}, 0, 0, fmt.Errorf("observe blocked successor controller: %w", err)
		}
		if current.Metadata.UID != successor.Metadata.UID ||
			current.Spec.NodeName != successor.Spec.NodeName || podReady(current) {
			return kubernetesLease{}, 0, 0, fmt.Errorf("successor controller did not remain fail-closed with one stable identity")
		}
		lease, err := backend.readControllerLease(waitCtx, request)
		if err != nil {
			return kubernetesLease{}, 0, 0, fmt.Errorf("observe uncleared Lease for blocked successor: %w", err)
		}
		if lease.Metadata.UID != expectedLease.Metadata.UID ||
			lease.Spec.HolderIdentity != expectedLease.Spec.HolderIdentity ||
			lease.Metadata.ResourceVersion != initialLease.Metadata.ResourceVersion ||
			lease.Spec.RenewTime != initialLease.Spec.RenewTime ||
			lease.Spec.LeaseDurationSeconds != initialLease.Spec.LeaseDurationSeconds ||
			lease.Metadata.Annotations["holderPodUID"] != expectedLease.Spec.HolderIdentity ||
			lease.Metadata.Annotations["gracefulReleaseState"] != "" {
			return kubernetesLease{}, 0, 0, fmt.Errorf("successor changed the uncleared Lease before approval")
		}
		if lease.Spec.RenewTime != expectedRenewTime {
			return kubernetesLease{}, 0, 0, fmt.Errorf("uncleared controller Lease renewed after the old Instance stopped")
		}
		restartCount, running, err := blockedControllerDriverState(current)
		if err != nil {
			return kubernetesLease{}, 0, 0, err
		}
		if running {
			if stableSince.IsZero() {
				stableSince = time.Now()
				expectedRestartCount = restartCount
			} else if restartCount != expectedRestartCount {
				return kubernetesLease{}, 0, 0, fmt.Errorf("blocked successor driver restarted during the proof window")
			}
			if time.Since(stableSince) >= time.Duration(initialLease.Spec.LeaseDurationSeconds)*time.Second+10*time.Second {
				return lease, max(int64(time.Since(stableSince).Seconds()), 1), expectedRestartCount, nil
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-waitCtx.Done():
			return kubernetesLease{}, 0, 0, fmt.Errorf("wait for stable fail-closed successor: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func blockedControllerDriverState(pod kubernetesPod) (int32, bool, error) {
	if pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
		return 0, false, fmt.Errorf("successor controller Pod entered terminal phase %q", pod.Status.Phase)
	}
	if pod.Status.Phase != "Running" {
		return 0, false, nil
	}
	var driverStatusCount int
	var restartCount int32
	var running, ready bool
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "driver" {
			continue
		}
		driverStatusCount++
		restartCount = status.RestartCount
		running = status.State.Running != nil
		ready = status.Ready
	}
	if driverStatusCount != 1 {
		return 0, false, fmt.Errorf("successor controller Pod has %d driver container statuses", driverStatusCount)
	}
	if ready {
		return 0, false, fmt.Errorf("successor driver container became Ready before approval")
	}
	return restartCount, running, nil
}

func (backend *scalewayBackend) assertControllerStillBlocked(
	ctx context.Context,
	request e2erunner.Request,
	successor kubernetesPod,
	expectedLease kubernetesLease,
	expectedRestartCount int32,
) error {
	current, err := backend.singularPod(ctx, request, controllerSelector, "")
	if err != nil {
		return err
	}
	if current.Metadata.UID != successor.Metadata.UID ||
		current.Spec.NodeName != successor.Spec.NodeName || podReady(current) {
		return fmt.Errorf("successor controller changed or became serving before approval")
	}
	restartCount, running, err := blockedControllerDriverState(current)
	if err != nil {
		return err
	}
	if !running || restartCount != expectedRestartCount {
		return fmt.Errorf("successor driver did not remain one stable running non-Ready process before approval")
	}
	lease, err := backend.readControllerLease(ctx, request)
	if err != nil {
		return err
	}
	if lease.Metadata.UID != expectedLease.Metadata.UID ||
		lease.Metadata.ResourceVersion != expectedLease.Metadata.ResourceVersion ||
		lease.Spec.HolderIdentity != expectedLease.Spec.HolderIdentity ||
		lease.Spec.RenewTime != expectedLease.Spec.RenewTime ||
		lease.Spec.LeaseDurationSeconds != expectedLease.Spec.LeaseDurationSeconds ||
		lease.Metadata.Annotations["holderPodUID"] != expectedLease.Metadata.Annotations["holderPodUID"] ||
		lease.Metadata.Annotations["gracefulReleaseState"] != "" {
		return fmt.Errorf("uncleared controller Lease changed immediately before approval")
	}
	return nil
}

func (backend *scalewayBackend) deleteControllerNetworkFence(
	ctx context.Context,
	request e2erunner.Request,
	fence controllerNetworkFence,
) error {
	if fence.PolicyName == "" {
		return nil
	}
	_, policyErr := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"delete", "networkpolicy/"+fence.PolicyName, "--ignore-not-found", "--wait=true", "--timeout=2m",
	)
	encoded, podErr := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"get", "pod/"+fence.ControllerPod, "--ignore-not-found", "-o", "json",
	)
	if podErr != nil || len(encoded) == 0 {
		return errors.Join(policyErr, podErr)
	}
	var pod kubernetesPod
	if err := json.Unmarshal(encoded, &pod); err != nil {
		return errors.Join(policyErr, err)
	}
	if pod.Metadata.UID != fence.ControllerPodUID {
		return errors.Join(policyErr, fmt.Errorf("refuse removal of a controller fence from a changed Pod identity"))
	}
	labelValue, labeled := pod.Metadata.Labels[controllerFenceLabel]
	if !labeled {
		// The journal is written before policy application. A crash in that
		// window legitimately leaves neither the selector label nor a policy.
		return policyErr
	}
	if labelValue != fence.LabelValue {
		return errors.Join(policyErr, fmt.Errorf("refuse removal of a controller fence with changed label identity"))
	}
	_, labelErr := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"label", "pod/"+fence.ControllerPod, controllerFenceLabel+"-",
	)
	return errors.Join(policyErr, labelErr)
}

func (backend *scalewayBackend) readExactPod(
	ctx context.Context,
	request e2erunner.Request,
	name string,
) (kubernetesPod, error) {
	encoded, err := backend.kubectl(ctx, request, nil, "-n", request.DriverNamespace,
		"get", "pod/"+name, "-o", "json",
	)
	if err != nil {
		return kubernetesPod{}, err
	}
	var pod kubernetesPod
	if err := json.Unmarshal(encoded, &pod); err != nil {
		return kubernetesPod{}, err
	}
	if pod.Metadata.Name != name || pod.Metadata.UID == "" {
		return kubernetesPod{}, fmt.Errorf("exact controller Pod identity is incomplete")
	}
	return pod, nil
}
