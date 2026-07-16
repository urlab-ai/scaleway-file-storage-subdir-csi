package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

// NodePathLayout is the complete node-local path authority that must be proven
// safe before the process serves CSI or relies on Bidirectional propagation.
// Derived kubelet plugin, registry, staging, and pod target trees are not
// independently configurable in v1.
type NodePathLayout struct {
	DriverName      string
	ParentMountRoot string
	KubeletPath     string
	CSISocketPath   string
}

// NodePathPreflight is an opaque successful filesystem and mount-namespace
// proof. It deliberately exposes no constructor: callers must obtain it from
// PreflightNodePaths using a fresh mountinfo snapshot.
type NodePathPreflight struct {
	layout    NodePathLayout
	parent    resolvedNodePath
	protected []resolvedNodePath
	anchors   nodeMountAnchors
}

type resolvedNodePath struct {
	name       string
	configured string
	resolved   string
	exists     bool
}

type nodeMountAnchors struct {
	parentRoot string
	plugins    string
	pods       string
	csi        string
}

// PreflightNodePaths resolves all currently existing symlink components,
// rejects parent-root overlap, parses one bounded coherent mountinfo snapshot,
// and proves that the exact hostPath mounts are non-stacked, non-aliased, and
// propagated as required. The snapshot reader is normally
// /proc/self/mountinfo; accepting it explicitly keeps unit tests deterministic.
func PreflightNodePaths(ctx context.Context, layout NodePathLayout, mountInfo io.Reader) (NodePathPreflight, error) {
	if ctx == nil {
		return NodePathPreflight{}, fmt.Errorf("node path preflight context is nil")
	}
	if err := ctx.Err(); err != nil {
		return NodePathPreflight{}, err
	}
	proof, err := inspectNodePaths(ctx, layout)
	if err != nil {
		return NodePathPreflight{}, err
	}
	if err := ctx.Err(); err != nil {
		return NodePathPreflight{}, err
	}
	entries, err := ParseMountInfo(mountInfo)
	if err != nil {
		return NodePathPreflight{}, fmt.Errorf("read node startup mount topology: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return NodePathPreflight{}, err
	}
	if err := validateNodeMountTopology(entries, proof); err != nil {
		return NodePathPreflight{}, err
	}
	return proof, nil
}

func inspectNodePaths(ctx context.Context, layout NodePathLayout) (NodePathPreflight, error) {
	derived, err := validateNodePathLayout(layout)
	if err != nil {
		return NodePathPreflight{}, err
	}
	parent, err := resolveNodePath(ctx, "parent mount root", layout.ParentMountRoot, true)
	if err != nil {
		return NodePathPreflight{}, err
	}
	protectedSpecs := []struct {
		name      string
		value     string
		required  bool
		directory bool
	}{
		{name: "kubelet root", value: layout.KubeletPath, required: true, directory: true},
		{name: "CSI socket directory", value: derived.csi, required: true, directory: true},
		{name: "CSI socket", value: layout.CSISocketPath},
		{name: "kubelet plugin directory", value: derived.pluginDirectory, required: true, directory: true},
		{name: "kubelet registry tree", value: derived.registry, directory: true},
		{name: "kubelet pod target tree", value: derived.pods, required: true, directory: true},
		{name: "kubelet staging tree", value: derived.staging, directory: true},
	}
	protected := make([]resolvedNodePath, 0, len(protectedSpecs))
	for _, spec := range protectedSpecs {
		if err := ctx.Err(); err != nil {
			return NodePathPreflight{}, err
		}
		resolved, resolveErr := resolveNodePath(ctx, spec.name, spec.value, spec.required)
		if resolveErr != nil {
			return NodePathPreflight{}, resolveErr
		}
		if spec.directory && resolved.exists {
			info, statErr := os.Stat(resolved.resolved)
			if statErr != nil {
				return NodePathPreflight{}, fmt.Errorf("inspect resolved %s %q: %w", spec.name, resolved.resolved, statErr)
			}
			if !info.IsDir() {
				return NodePathPreflight{}, fmt.Errorf("resolved %s %q is not a directory", spec.name, resolved.resolved)
			}
		}
		if pathsOverlapMountRoots(parent.configured, resolved.configured) {
			return NodePathPreflight{}, fmt.Errorf("parent mount root %q lexically overlaps %s %q", parent.configured, spec.name, resolved.configured)
		}
		if pathsOverlapMountRoots(parent.resolved, resolved.resolved) {
			return NodePathPreflight{}, fmt.Errorf("resolved parent mount root %q overlaps %s %q", parent.resolved, spec.name, resolved.resolved)
		}
		protected = append(protected, resolved)
	}
	return NodePathPreflight{
		layout: layout, parent: parent, protected: protected,
		anchors: nodeMountAnchors{
			parentRoot: layout.ParentMountRoot,
			plugins:    derived.plugins,
			pods:       derived.pods,
			csi:        derived.csi,
		},
	}, nil
}

type derivedNodePaths struct {
	plugins         string
	pluginDirectory string
	registry        string
	pods            string
	staging         string
	csi             string
}

func validateNodePathLayout(layout NodePathLayout) (derivedNodePaths, error) {
	if err := volume.ValidateDriverName(layout.DriverName); err != nil {
		return derivedNodePaths{}, err
	}
	for _, candidate := range []struct {
		name  string
		value string
	}{
		{name: "parent mount root", value: layout.ParentMountRoot},
		{name: "kubelet path", value: layout.KubeletPath},
		{name: "CSI socket path", value: layout.CSISocketPath},
	} {
		if err := ValidateAbsoluteNormalizedPath(candidate.value); err != nil {
			return derivedNodePaths{}, fmt.Errorf("%s: %w", candidate.name, err)
		}
	}
	plugins := path.Join(layout.KubeletPath, "plugins")
	derived := derivedNodePaths{
		plugins:         plugins,
		pluginDirectory: path.Join(plugins, layout.DriverName),
		registry:        path.Join(layout.KubeletPath, "plugins_registry"),
		pods:            path.Join(layout.KubeletPath, "pods"),
		staging:         path.Join(plugins, "kubernetes.io/csi", layout.DriverName),
		csi:             path.Dir(layout.CSISocketPath),
	}
	if pathsOverlapMountRoots(layout.ParentMountRoot, layout.KubeletPath) {
		return derivedNodePaths{}, fmt.Errorf("parent mount root and kubelet path overlap")
	}
	return derived, nil
}

func resolveNodePath(ctx context.Context, name, configured string, required bool) (resolvedNodePath, error) {
	if err := ctx.Err(); err != nil {
		return resolvedNodePath{}, err
	}
	cursor := filepath.FromSlash(configured)
	missing := make([]string, 0, 4)
	for {
		resolved, err := filepath.EvalSymlinks(cursor)
		if err == nil {
			info, statErr := os.Stat(resolved)
			if statErr != nil {
				return resolvedNodePath{}, fmt.Errorf("inspect resolved %s %q: %w", name, filepath.ToSlash(resolved), statErr)
			}
			if len(missing) > 0 && !info.IsDir() {
				return resolvedNodePath{}, fmt.Errorf("existing ancestor %q of %s is not a directory", filepath.ToSlash(resolved), name)
			}
			for _, component := range missing {
				resolved = filepath.Join(resolved, component)
			}
			resolved = filepath.Clean(resolved)
			if !filepath.IsAbs(resolved) {
				return resolvedNodePath{}, fmt.Errorf("resolved %s %q is not absolute", name, filepath.ToSlash(resolved))
			}
			exists := len(missing) == 0
			if required && !exists {
				return resolvedNodePath{}, fmt.Errorf("required %s %q does not exist", name, configured)
			}
			if required && !info.IsDir() {
				return resolvedNodePath{}, fmt.Errorf("required %s %q is not a directory", name, configured)
			}
			return resolvedNodePath{
				name: name, configured: configured, resolved: filepath.ToSlash(resolved), exists: exists,
			}, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return resolvedNodePath{}, fmt.Errorf("resolve %s %q: %w", name, configured, err)
		}
		if info, lstatErr := os.Lstat(cursor); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return resolvedNodePath{}, fmt.Errorf("resolve %s %q: dangling symlink at %q", name, configured, filepath.ToSlash(cursor))
		} else if lstatErr != nil && !errors.Is(lstatErr, os.ErrNotExist) {
			return resolvedNodePath{}, fmt.Errorf("inspect missing %s component %q: %w", name, filepath.ToSlash(cursor), lstatErr)
		}
		if cursor == string(filepath.Separator) {
			return resolvedNodePath{}, fmt.Errorf("resolve %s %q: no existing ancestor", name, configured)
		}
		missing = append([]string{filepath.Base(cursor)}, missing...)
		cursor = filepath.Dir(cursor)
		if err := ctx.Err(); err != nil {
			return resolvedNodePath{}, err
		}
	}
}

func validateNodeMountTopology(entries []MountInfoEntry, proof NodePathPreflight) error {
	if err := proof.validate(); err != nil {
		return err
	}
	parent, err := exactStartupMount(entries, "parent mount root", proof.anchors.parentRoot, true)
	if err != nil {
		return err
	}
	protected := []struct {
		name       string
		target     string
		propagated bool
	}{
		{name: "kubelet plugins", target: proof.anchors.plugins, propagated: true},
		{name: "kubelet pods", target: proof.anchors.pods, propagated: true},
		{name: "CSI socket directory", target: proof.anchors.csi},
	}
	for _, candidate := range protected {
		entry, exactErr := exactStartupMount(entries, candidate.name, candidate.target, candidate.propagated)
		if exactErr != nil {
			return exactErr
		}
		overlaps, overlapErr := mountSourceRootsOverlap(parent, entry)
		if overlapErr != nil {
			return fmt.Errorf("compare parent mount root with %s: %w", candidate.name, overlapErr)
		}
		if overlaps {
			return fmt.Errorf("parent mount root and %s alias overlapping roots on device %s", candidate.name, parent.DeviceID)
		}
	}
	return nil
}

func (proof NodePathPreflight) validate() error {
	derived, err := validateNodePathLayout(proof.layout)
	if err != nil {
		return fmt.Errorf("node path preflight proof is invalid: %w", err)
	}
	if proof.parent.name != "parent mount root" || proof.parent.configured != proof.layout.ParentMountRoot || proof.parent.resolved == "" || !proof.parent.exists {
		return fmt.Errorf("node path preflight proof has no valid parent root")
	}
	if len(proof.protected) != 7 {
		return fmt.Errorf("node path preflight proof has an incomplete protected path set")
	}
	if proof.anchors != (nodeMountAnchors{
		parentRoot: proof.layout.ParentMountRoot, plugins: derived.plugins, pods: derived.pods, csi: derived.csi,
	}) {
		return fmt.Errorf("node path preflight proof mount anchors differ from layout")
	}
	for _, candidate := range proof.protected {
		if candidate.configured == "" || candidate.resolved == "" || pathsOverlapMountRoots(proof.parent.configured, candidate.configured) || pathsOverlapMountRoots(proof.parent.resolved, candidate.resolved) {
			return fmt.Errorf("node path preflight proof contains an invalid or overlapping %s", candidate.name)
		}
	}
	return nil
}

func exactStartupMount(entries []MountInfoEntry, name, target string, requireShared bool) (MountInfoEntry, error) {
	if err := ValidateAbsoluteNormalizedPath(target); err != nil {
		return MountInfoEntry{}, fmt.Errorf("%s target: %w", name, err)
	}
	matches := make([]MountInfoEntry, 0, 1)
	for _, entry := range entries {
		if entry.MountPoint == target {
			matches = append(matches, entry)
		}
	}
	if len(matches) == 0 {
		return MountInfoEntry{}, fmt.Errorf("%s %q has no exact mountinfo entry", name, target)
	}
	if len(matches) != 1 {
		return MountInfoEntry{}, fmt.Errorf("%s %q is a stacked mount with %d entries", name, target, len(matches))
	}
	entry := matches[0]
	if entry.MountID == 0 || entry.DeviceID == "" || entry.Root == "" || !path.IsAbs(entry.Root) || path.Clean(entry.Root) != entry.Root {
		return MountInfoEntry{}, fmt.Errorf("%s %q has incomplete mount identity", name, target)
	}
	if _, _, err := parseStartupDevice(entry.DeviceID); err != nil {
		return MountInfoEntry{}, fmt.Errorf("%s %q: %w", name, target, err)
	}
	if requireShared && !hasSharedPropagation(entry.Optional) {
		return MountInfoEntry{}, fmt.Errorf("%s %q is not a shared Bidirectional-propagation mount", name, target)
	}
	return entry, nil
}

func mountSourceRootsOverlap(left, right MountInfoEntry) (bool, error) {
	leftMajor, leftMinor, err := parseStartupDevice(left.DeviceID)
	if err != nil {
		return false, err
	}
	rightMajor, rightMinor, err := parseStartupDevice(right.DeviceID)
	if err != nil {
		return false, err
	}
	if leftMajor != rightMajor || leftMinor != rightMinor {
		return false, nil
	}
	if !path.IsAbs(left.Root) || path.Clean(left.Root) != left.Root || !path.IsAbs(right.Root) || path.Clean(right.Root) != right.Root {
		return false, fmt.Errorf("mount source root is not absolute and normalized")
	}
	return pathsOverlapMountRoots(left.Root, right.Root), nil
}

func parseStartupDevice(value string) (uint64, uint64, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, fmt.Errorf("device ID %q is malformed", value)
	}
	major, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("device ID %q has invalid major number", value)
	}
	minor, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("device ID %q has invalid minor number", value)
	}
	return major, minor, nil
}

func hasSharedPropagation(optional []string) bool {
	for _, value := range optional {
		if strings.HasPrefix(value, "shared:") && len(value) > len("shared:") {
			return true
		}
	}
	return false
}
