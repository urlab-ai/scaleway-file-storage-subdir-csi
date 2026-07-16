package mount

import (
	"bufio"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	maxMountInfoLineBytes  = 1024 * 1024
	maxMountInfoEntries    = 100000
	maxMountInfoTotalBytes = 64 * 1024 * 1024
)

// MountInfoEntry is one losslessly normalized /proc/self/mountinfo line. Linux
// exposes the bind's filesystem root and device, not the source pathname used
// by the original bind syscall.
type MountInfoEntry struct {
	MountID        uint64
	ParentMountID  uint64
	DeviceID       string
	Root           string
	MountPoint     string
	MountOptions   []string
	Optional       []string
	FilesystemType string
	MountSource    string
	SuperOptions   []string
}

// ParseMountInfo parses a bounded coherent mountinfo snapshot and rejects
// malformed escapes, IDs, separators, and paths.
func ParseMountInfo(reader io.Reader) ([]MountInfoEntry, error) {
	return parseMountInfoBounded(reader, maxMountInfoTotalBytes)
}

func parseMountInfoBounded(reader io.Reader, maximumBytes int) ([]MountInfoEntry, error) {
	if reader == nil {
		return nil, fmt.Errorf("mountinfo reader is nil")
	}
	if maximumBytes <= 0 || maximumBytes > maxMountInfoTotalBytes {
		return nil, fmt.Errorf("mountinfo byte bound is outside [1,%d]", maxMountInfoTotalBytes)
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxMountInfoLineBytes)
	entries := make([]MountInfoEntry, 0, 256)
	seenMountIDs := make(map[uint64]struct{}, 256)
	totalBytes := 0
	for scanner.Scan() {
		lineBytes := len(scanner.Bytes()) + 1
		if lineBytes > maximumBytes-totalBytes {
			return nil, fmt.Errorf("mountinfo exceeds %d bytes", maximumBytes)
		}
		totalBytes += lineBytes
		if len(entries) >= maxMountInfoEntries {
			return nil, fmt.Errorf("mountinfo exceeds %d entries", maxMountInfoEntries)
		}
		entry, err := parseMountInfoLine(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("mountinfo line %d: %w", len(entries)+1, err)
		}
		if _, duplicate := seenMountIDs[entry.MountID]; duplicate {
			return nil, fmt.Errorf("mountinfo line %d repeats mount ID %d", len(entries)+1, entry.MountID)
		}
		seenMountIDs[entry.MountID] = struct{}{}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read mountinfo: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("mountinfo snapshot is empty")
	}
	return entries, nil
}

// BuildTableFromMountInfo retains every mount at a driver-owned parent,
// staging, or pod target, including foreign filesystem types. Keeping foreign
// entries ensures an existing target can never be mistaken for absence and
// over-mounted as a retry.
func BuildTableFromMountInfo(entries []MountInfoEntry, parentMountRoot, kubeletPath, driverName string) (Table, error) {
	if err := ValidateAbsoluteNormalizedPath(parentMountRoot); err != nil {
		return Table{}, fmt.Errorf("parent mount root: %w", err)
	}
	if err := ValidateAbsoluteNormalizedPath(kubeletPath); err != nil {
		return Table{}, fmt.Errorf("kubelet path: %w", err)
	}
	if err := volume.ValidateDriverName(driverName); err != nil {
		return Table{}, err
	}
	if pathsOverlapMountRoots(parentMountRoot, kubeletPath) {
		return Table{}, fmt.Errorf("parent mount root and kubelet path overlap")
	}
	result := Table{Entries: make([]Entry, 0)}
	for _, raw := range entries {
		kind, parentID, relevant := classifyMountTarget(raw.MountPoint, raw.MountSource, parentMountRoot, kubeletPath, driverName)
		if !relevant {
			continue
		}
		result.Entries = append(result.Entries, Entry{
			MountID: raw.MountID, MountInfoID: raw.MountID, ParentMountID: raw.ParentMountID, DeviceID: raw.DeviceID,
			Kind: kind, Target: raw.MountPoint,
			FilesystemType: raw.FilesystemType, FilesystemSource: raw.MountSource,
			ParentFilesystemID: parentID, BackingRelativePath: raw.Root,
			ReadOnly: mountOptionsContain(raw.MountOptions, "ro"),
		})
	}
	return result, nil
}

func parseMountInfoLine(line string) (MountInfoEntry, error) {
	fields := strings.Fields(line)
	if len(fields) < 10 {
		return MountInfoEntry{}, fmt.Errorf("expected at least 10 fields")
	}
	separator := -1
	for index := 6; index < len(fields); index++ {
		if fields[index] == "-" {
			separator = index
			break
		}
	}
	if separator < 6 || len(fields)-separator != 4 {
		return MountInfoEntry{}, fmt.Errorf("mountinfo separator or post-separator field count is invalid")
	}
	mountID, err := parsePositiveMountID("mount", fields[0])
	if err != nil {
		return MountInfoEntry{}, err
	}
	parentID, err := parsePositiveMountID("parent", fields[1])
	if err != nil {
		return MountInfoEntry{}, err
	}
	if !validDeviceID(fields[2]) {
		return MountInfoEntry{}, fmt.Errorf("device ID %q is malformed", fields[2])
	}
	root, err := decodeMountInfoPath(fields[3])
	if err != nil {
		return MountInfoEntry{}, fmt.Errorf("root: %w", err)
	}
	mountPoint, err := decodeMountInfoPath(fields[4])
	if err != nil {
		return MountInfoEntry{}, fmt.Errorf("mount point: %w", err)
	}
	if root == "" || root[0] != '/' || path.Clean(root) != root {
		return MountInfoEntry{}, fmt.Errorf("root %q is not absolute and normalized", root)
	}
	if mountPoint != "/" {
		if err := ValidateAbsoluteNormalizedPath(mountPoint); err != nil {
			return MountInfoEntry{}, err
		}
	}
	mountSource, err := decodeMountInfoField(fields[separator+2])
	if err != nil {
		return MountInfoEntry{}, fmt.Errorf("mount source: %w", err)
	}
	if fields[separator+1] == "" || mountSource == "" {
		return MountInfoEntry{}, fmt.Errorf("filesystem type or mount source is empty")
	}
	return MountInfoEntry{
		MountID: mountID, ParentMountID: parentID, DeviceID: fields[2],
		Root: root, MountPoint: mountPoint,
		MountOptions:   splitMountOptions(fields[5]),
		Optional:       append([]string(nil), fields[6:separator]...),
		FilesystemType: fields[separator+1], MountSource: mountSource,
		SuperOptions: splitMountOptions(fields[separator+3]),
	}, nil
}

func parsePositiveMountID(name, value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s mount ID %q is not positive base-10", name, value)
	}
	return parsed, nil
}

func validDeviceID(value string) bool {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}

func decodeMountInfoPath(value string) (string, error) {
	decoded, err := decodeMountInfoField(value)
	if err != nil {
		return "", err
	}
	if strings.ContainsRune(decoded, 0) {
		return "", fmt.Errorf("decoded path contains NUL")
	}
	return decoded, nil
}

func decodeMountInfoField(value string) (string, error) {
	var builder strings.Builder
	for index := 0; index < len(value); {
		if value[index] != '\\' {
			builder.WriteByte(value[index])
			index++
			continue
		}
		if index+3 >= len(value) {
			return "", fmt.Errorf("truncated mountinfo escape")
		}
		escape := value[index+1 : index+4]
		switch escape {
		case "040":
			builder.WriteByte(' ')
		case "011":
			builder.WriteByte('\t')
		case "012":
			builder.WriteByte('\n')
		case "134":
			builder.WriteByte('\\')
		default:
			return "", fmt.Errorf("unsupported mountinfo escape \\%s", escape)
		}
		index += 4
	}
	return builder.String(), nil
}

func splitMountOptions(value string) []string {
	if value == "" {
		return []string{}
	}
	return strings.Split(value, ",")
}

func mountOptionsContain(options []string, wanted string) bool {
	for _, option := range options {
		if option == wanted {
			return true
		}
	}
	return false
}

func classifyMountTarget(target, source, parentRoot, kubeletPath, driverName string) (Kind, string, bool) {
	if path.Dir(target) == parentRoot {
		return KindParent, path.Base(target), true
	}
	if strings.HasPrefix(target, parentRoot+"/") {
		relative := strings.TrimPrefix(target, parentRoot+"/")
		parentID := strings.SplitN(relative, "/", 2)[0]
		return KindForeign, parentID, true
	}
	stagePrefix := path.Join(kubeletPath, "plugins/kubernetes.io/csi", driverName)
	if strings.HasPrefix(target, stagePrefix+"/") {
		return KindStage, source, true
	}
	if exact, descendant := classifyKubeletPublishTarget(target, kubeletPath); exact {
		return KindPublish, source, true
	} else if descendant {
		return KindForeign, source, true
	}
	return "", "", false
}

func classifyKubeletPublishTarget(target, kubeletPath string) (exact, descendant bool) {
	podPrefix := path.Join(kubeletPath, "pods")
	if !strings.HasPrefix(target, podPrefix+"/") {
		return false, false
	}
	relative := strings.TrimPrefix(target, podPrefix+"/")
	parts := strings.Split(relative, "/")
	shape := len(parts) >= 5 && parts[0] != "" && parts[1] == "volumes" && parts[2] == "kubernetes.io~csi" && parts[3] != "" && parts[4] == "mount"
	return shape && len(parts) == 5, shape && len(parts) > 5
}

func pathsOverlapMountRoots(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/")
}
