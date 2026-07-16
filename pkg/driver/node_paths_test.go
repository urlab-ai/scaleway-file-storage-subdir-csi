package driver

import "testing"

func TestNodePathPolicyAcceptsOnlyExactKubeletTrees(t *testing.T) {
	policy, err := NewNodePathPolicy(driverTestName, "/var/lib/kubelet", "/var/lib/scaleway-sfs-subdir-csi/parents")
	if err != nil {
		t.Fatalf("NewNodePathPolicy() error = %v", err)
	}
	if err := policy.ValidateStagingPath("/var/lib/kubelet/plugins/kubernetes.io/csi/file-storage-subdir.csi.urlab.ai/volume/globalmount"); err != nil {
		t.Fatalf("ValidateStagingPath(valid) error = %v", err)
	}
	if err := policy.ValidatePublishPath("/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/pv-a/mount"); err != nil {
		t.Fatalf("ValidatePublishPath(valid) error = %v", err)
	}
	for _, invalid := range []string{
		"/var/lib/kubelet/plugins/foreign/stage",
		"/var/lib/kubelet/pods/pod-a/volumes/foreign/pv-a/mount",
		"/var/lib/kubelet/pods/pod-a/volumes/kubernetes.io~csi/pv-a/other",
	} {
		if err := policy.ValidateStagingPath(invalid); err == nil {
			if err := policy.ValidatePublishPath(invalid); err == nil {
				t.Errorf("path %q accepted as both stage and publish", invalid)
			}
		}
	}
}

func TestNodePathPolicyAcceptsDisjointDevelopmentRootAfterRuntimeValidation(t *testing.T) {
	policy, err := NewNodePathPolicy(driverTestName, "/var/lib/kubelet", "/tmp/development-parent-root")
	if err != nil {
		t.Fatalf("NewNodePathPolicy(development root) error = %v", err)
	}
	if got, err := policy.ParentTarget("33333333-3333-4333-8333-333333333333"); err != nil || got != "/tmp/development-parent-root/33333333-3333-4333-8333-333333333333" {
		t.Fatalf("ParentTarget() = %q, %v", got, err)
	}
}
