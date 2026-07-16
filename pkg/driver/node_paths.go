package driver

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"scaleway-sfs-subdir-csi/pkg/mount"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

// ErrInvalidNodePath identifies an inbound CSI path outside the exact kubelet
// subtrees owned by the requested Node operation.
var ErrInvalidNodePath = errors.New("invalid CSI node path")

// NodePathPolicy validates only the exact kubelet subtrees owned by each CSI
// operation. Runtime PathManager checks additionally resolve symlinks no-follow.
type NodePathPolicy struct {
	driverName      string
	kubeletPath     string
	parentMountRoot string
}

// NewNodePathPolicy validates the already-authorized runtime roots. Production
// fixed-root policy is enforced by config.Runtime before this path boundary is
// constructed; keeping this layer mode-agnostic permits explicit development
// layouts without weakening production validation.
func NewNodePathPolicy(driverName, kubeletPath, parentMountRoot string) (*NodePathPolicy, error) {
	if err := volume.ValidateDriverName(driverName); err != nil {
		return nil, err
	}
	if err := mount.ValidateAbsoluteNormalizedPath(kubeletPath); err != nil {
		return nil, fmt.Errorf("kubelet path: %w", err)
	}
	if err := mount.ValidateAbsoluteNormalizedPath(parentMountRoot); err != nil {
		return nil, fmt.Errorf("parent mount root: %w", err)
	}
	if pathsLexicallyOverlap(kubeletPath, parentMountRoot) {
		return nil, fmt.Errorf("kubelet path and parent mount root overlap")
	}
	return &NodePathPolicy{driverName: driverName, kubeletPath: kubeletPath, parentMountRoot: parentMountRoot}, nil
}

// ParentTarget returns the deterministic driver-owned warm mount target.
func (policy *NodePathPolicy) ParentTarget(parentFilesystemID string) (string, error) {
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return "", err
	}
	return path.Join(policy.parentMountRoot, parentFilesystemID), nil
}

// ValidateStagingPath accepts only a descendant of this driver's exact kubelet
// CSI staging tree. The CO owns creation and removal of the path itself.
func (policy *NodePathPolicy) ValidateStagingPath(stagingPath string) error {
	if err := mount.ValidateAbsoluteNormalizedPath(stagingPath); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNodePath, err)
	}
	prefix := path.Join(policy.kubeletPath, "plugins/kubernetes.io/csi", policy.driverName)
	if stagingPath == prefix || !strings.HasPrefix(stagingPath, prefix+"/") {
		return fmt.Errorf("staging path %q is outside driver kubelet staging tree %q: %w", stagingPath, prefix, ErrInvalidNodePath)
	}
	return nil
}

// ValidatePublishPath accepts exactly
// pods/<pod>/volumes/kubernetes.io~csi/<pv>/mount below kubelet.
func (policy *NodePathPolicy) ValidatePublishPath(targetPath string) error {
	if err := mount.ValidateAbsoluteNormalizedPath(targetPath); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNodePath, err)
	}
	prefix := path.Join(policy.kubeletPath, "pods")
	if !strings.HasPrefix(targetPath, prefix+"/") {
		return fmt.Errorf("publish target %q is outside kubelet pod tree: %w", targetPath, ErrInvalidNodePath)
	}
	relative := strings.TrimPrefix(targetPath, prefix+"/")
	parts := strings.Split(relative, "/")
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[3] == "" || parts[4] != "mount" {
		return fmt.Errorf("publish target %q does not match the exact kubelet CSI target shape: %w", targetPath, ErrInvalidNodePath)
	}
	return nil
}

func pathsLexicallyOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}
