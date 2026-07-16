package volume

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

const directorySuffixBytes = 12

const maxFilesystemComponentBytes = 255

var safeDirectoryPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

var reservedDirectoryNames = map[string]struct{}{
	".archived":       {},
	".deleted":        {},
	".sfs-subdir-csi": {},
}

const parentOwnerNamespace = ".sfs-subdir-csi-owner"

// ValidateBasePath validates the immutable Linux parent-relative base path.
func ValidateBasePath(basePath string) error {
	if !utf8.ValidString(basePath) || basePath == "" {
		return fmt.Errorf("base path must be non-empty valid UTF-8")
	}
	if !strings.HasPrefix(basePath, "/") {
		return fmt.Errorf("base path %q must be absolute", basePath)
	}
	if basePath == "/" {
		return fmt.Errorf("base path must not be the filesystem root")
	}
	if path.Clean(basePath) != basePath {
		return fmt.Errorf("base path %q must be normalized", basePath)
	}
	if strings.ContainsRune(basePath, 0) {
		return fmt.Errorf("base path contains NUL")
	}
	if len(basePath) > MaxContextEntryBytes {
		return fmt.Errorf("base path contains %d bytes, exceeds CSI context limit %d", len(basePath), MaxContextEntryBytes)
	}
	components := strings.Split(strings.TrimPrefix(basePath, "/"), "/")
	// The immutable parent claim and its bootstrap temporary files live in the
	// root-level .sfs-subdir-csi-owner namespace. Allowing a configured base
	// path to occupy that namespace would make first claim creation ambiguous
	// and could turn a bootstrap artifact into a traversable directory tree.
	if strings.HasPrefix(components[0], parentOwnerNamespace) {
		return fmt.Errorf("base path %q uses the reserved parent-owner namespace", basePath)
	}
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("base path %q contains an unsafe component", basePath)
		}
	}
	return nil
}

// ValidateDirectoryName validates one untrusted logical-volume path component.
func ValidateDirectoryName(name string) error {
	if !utf8.ValidString(name) || len(name) == 0 || len(name) > MaxDirectoryNameBytes {
		return fmt.Errorf("directory name must contain 1 to %d UTF-8 bytes", MaxDirectoryNameBytes)
	}
	if !safeDirectoryPattern.MatchString(name) {
		return fmt.Errorf("directory name %q contains unsupported characters", name)
	}
	if strings.Contains(name, "..") || strings.ContainsRune(name, '/') {
		return fmt.Errorf("directory name %q contains path traversal", name)
	}
	if _, reserved := reservedDirectoryNames[name]; reserved {
		return fmt.Errorf("directory name %q is reserved for driver state", name)
	}
	return nil
}

// DirectoryName derives a deterministic human-readable directory component.
// If PVC metadata is absent, the logical volume ID itself is used.
func DirectoryName(namespace, pvcName, logicalVolumeID string) (string, error) {
	if err := ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return "", err
	}
	if namespace == "" || pvcName == "" {
		return logicalVolumeID, nil
	}

	suffix := strings.TrimPrefix(logicalVolumeID, "lv-")[:directorySuffixBytes]
	prefix := sanitizeDirectorySegment(namespace) + "--" + sanitizeDirectorySegment(pvcName)
	maxPrefix := MaxDirectoryNameBytes - len("--") - len(suffix)
	if len(prefix) > maxPrefix {
		prefix = strings.TrimRight(prefix[:maxPrefix], ".-_")
	}
	if prefix == "" {
		prefix = "volume"
	}
	name := prefix + "--" + suffix
	if err := ValidateDirectoryName(name); err != nil {
		return "", err
	}
	return name, nil
}

// ManagedLifecycleTarget derives the only v1 archive or quarantine path. Its
// collision-resistant component binds the immutable directory and logical
// volume identities to the persisted operation ID and canonical start time.
func ManagedLifecycleTarget(basePath, managedDirectory, directoryName, logicalVolumeID, timestamp, operationID string) (string, error) {
	if err := ValidateBasePath(basePath); err != nil {
		return "", err
	}
	if managedDirectory != ".archived" && managedDirectory != ".deleted" {
		return "", fmt.Errorf("managed lifecycle directory %q is unsupported", managedDirectory)
	}
	if err := ValidateDirectoryName(directoryName); err != nil {
		return "", err
	}
	if err := ValidateLogicalVolumeID(logicalVolumeID); err != nil {
		return "", err
	}
	if err := validateRequiredTimestamp("lifecycle timestamp", timestamp); err != nil {
		return "", err
	}
	if err := ValidateOperationID(operationID); err != nil {
		return "", err
	}
	componentTimestamp := strings.NewReplacer(":", "", ".", "", "-", "").Replace(strings.ToLower(timestamp))
	component := directoryName + "-" + logicalVolumeID + "-" + componentTimestamp + "-" + operationID
	if len(component) > maxFilesystemComponentBytes {
		return "", fmt.Errorf("managed lifecycle component exceeds %d bytes", maxFilesystemComponentBytes)
	}
	return path.Join(basePath, managedDirectory, component), nil
}

func sanitizeDirectorySegment(input string) string {
	var builder strings.Builder
	lastSeparator := false
	for _, r := range strings.ToLower(input) {
		safe := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.'
		if safe && r != '/' {
			if r == '.' && lastSeparator {
				continue
			}
			builder.WriteRune(r)
			lastSeparator = r == '.'
			continue
		}
		if unicode.IsSpace(r) || !safe {
			if builder.Len() > 0 && !lastSeparator {
				builder.WriteByte('-')
				lastSeparator = true
			}
		}
	}
	return strings.Trim(builder.String(), ".-_")
}
