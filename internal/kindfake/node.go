package kindfake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

type nodeServiceCore interface {
	GetInfo() driver.NodeInfo
	Stage(context.Context, string, map[string]string, string, volume.Capability) error
	Unstage(context.Context, string, string) error
	Publish(context.Context, string, map[string]string, string, string, volume.Capability, bool) error
	Unpublish(context.Context, string, string) error
}

type nodeCore struct {
	nodeID      string
	driverName  string
	dataRoot    string
	kubeletPath string
	mounts      nodeMountBackend
}

type nodeMountBackend interface {
	EnsureBind(context.Context, string, string, bool) error
	UnmountBind(context.Context, string, string, bool) error
}

func newPortableNodeCore(options Options, mounts nodeMountBackend) (nodeServiceCore, error) {
	if mounts == nil {
		return nil, fmt.Errorf("kind fake node mount backend is nil")
	}
	nodeID := deterministicNodeID(options.NodeName)
	if err := volume.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	return &nodeCore{
		nodeID: nodeID, driverName: options.DriverName, dataRoot: options.DataRoot,
		kubeletPath: options.KubeletPath, mounts: mounts,
	}, nil
}

func (node *nodeCore) GetInfo() driver.NodeInfo { return driver.NodeInfo{NodeID: node.nodeID} }

func (node *nodeCore) Stage(ctx context.Context, encoded string, values map[string]string, staging string, capability volume.Capability) error {
	if _, err := volume.NormalizeCapability(capability); err != nil {
		return err
	}
	handle, err := node.validateRequest(encoded, values)
	if err != nil {
		return err
	}
	if err := node.validateStagePath(staging); err != nil {
		return err
	}
	return node.mounts.EnsureBind(ctx, node.source(handle), staging, false)
}

func (node *nodeCore) Unstage(ctx context.Context, encoded, staging string) error {
	handle, err := volume.ParseHandle(encoded)
	if err != nil {
		return err
	}
	if err := node.validateStagePath(staging); err != nil {
		return err
	}
	return node.mounts.UnmountBind(ctx, node.source(handle), staging, false)
}

func (node *nodeCore) Publish(ctx context.Context, encoded string, values map[string]string, staging, target string, capability volume.Capability, readOnly bool) error {
	if _, err := volume.NormalizeCapability(capability); err != nil {
		return err
	}
	handle, err := node.validateRequest(encoded, values)
	if err != nil {
		return err
	}
	if err := node.validateStagePath(staging); err != nil {
		return err
	}
	if err := node.validatePublishPath(target); err != nil {
		return err
	}
	// Requiring the staging bind to exist keeps the fake aligned with the CSI
	// STAGE_UNSTAGE_VOLUME capability instead of silently mounting its source.
	if err := node.mounts.EnsureBind(ctx, node.source(handle), staging, false); err != nil {
		return fmt.Errorf("verify kind fake staging bind: %w", err)
	}
	return node.mounts.EnsureBind(ctx, staging, target, readOnly)
}

func (node *nodeCore) Unpublish(ctx context.Context, encoded, target string) error {
	handle, err := volume.ParseHandle(encoded)
	if err != nil {
		return err
	}
	if err := node.validatePublishPath(target); err != nil {
		return err
	}
	// The live publish bind is sourced from the staging mount, but both paths
	// expose the same logical source inode. Comparing with the durable fake data
	// directory therefore also detects a foreign replacement.
	return node.mounts.UnmountBind(ctx, node.source(handle), target, true)
}

func (node *nodeCore) validateRequest(encoded string, values map[string]string) (volume.Handle, error) {
	if err := validateHandleContext(encoded, values); err != nil {
		return volume.Handle{}, err
	}
	return volume.ParseHandle(encoded)
}

func (node *nodeCore) source(handle volume.Handle) string {
	return filepath.Join(node.dataRoot, handle.LogicalVolumeID)
}

func (node *nodeCore) validateStagePath(candidate string) error {
	prefix := filepath.Join(node.kubeletPath, "plugins", "kubernetes.io", "csi", node.driverName)
	relative, err := cleanRelative(prefix, candidate)
	if err != nil {
		return fmt.Errorf("kind fake staging path: %w", err)
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 2 || parts[0] == "" || parts[1] != "globalmount" {
		return fmt.Errorf("kind fake staging path has an unexpected kubelet shape")
	}
	return nil
}

func (node *nodeCore) validatePublishPath(candidate string) error {
	prefix := filepath.Join(node.kubeletPath, "pods")
	relative, err := cleanRelative(prefix, candidate)
	if err != nil {
		return fmt.Errorf("kind fake publish path: %w", err)
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) != 5 || parts[0] == "" || parts[1] != "volumes" || parts[2] != "kubernetes.io~csi" || parts[3] == "" || parts[4] != "mount" {
		return fmt.Errorf("kind fake publish path has an unexpected kubelet shape")
	}
	return nil
}

func cleanRelative(root, candidate string) (string, error) {
	if candidate == "" || !filepath.IsAbs(candidate) || filepath.Clean(candidate) != candidate {
		return "", fmt.Errorf("path is not clean and absolute")
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path is outside the required root")
	}
	return relative, nil
}

func deterministicNodeID(nodeName string) string {
	digest := sha256.Sum256([]byte(nodeName))
	identifier := digest[:16]
	identifier[6] = (identifier[6] & 0x0f) | 0x40
	identifier[8] = (identifier[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(identifier)
	uuid := hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:32]
	return "fr-par-1/" + uuid
}
