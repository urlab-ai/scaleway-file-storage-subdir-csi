package safety

import (
	"errors"
	"fmt"
	"path"
	"strings"
)

var (
	// ErrUnsafeLivePath marks a syntactically valid CSI path whose live
	// filesystem object is absent, replaced, aliased, non-directory, crosses a
	// mount boundary, or otherwise cannot safely satisfy the requested action.
	ErrUnsafeLivePath = errors.New("live filesystem path is unsafe")
	// ErrTargetConflict marks an existing mount target that contains foreign
	// entries and therefore must not be hidden by a new CSI mount.
	ErrTargetConflict = errors.New("CSI mount target conflicts with existing content")
)

// ValidateRelative rejects paths that could escape or ambiguously reference a
// Root operation. The canonical root itself is represented only by ".".
func ValidateRelative(value string) error {
	if value == "" || strings.HasPrefix(value, "/") || strings.ContainsRune(value, 0) {
		return fmt.Errorf("relative path %q is empty, absolute, or contains NUL", value)
	}
	if path.Clean(value) != value {
		return fmt.Errorf("relative path %q is not normalized", value)
	}
	if value == ".." || strings.HasPrefix(value, "../") {
		return fmt.Errorf("relative path %q escapes its root", value)
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == ".." {
			return fmt.Errorf("relative path %q contains an unsafe component", value)
		}
	}
	return nil
}

// RelativeToParent converts a normalized absolute in-parent path into a Root
// path without accepting filesystem root or traversal.
func RelativeToParent(absolute string) (string, error) {
	if absolute == "" || !strings.HasPrefix(absolute, "/") || absolute == "/" || path.Clean(absolute) != absolute {
		return "", fmt.Errorf("parent path %q must be absolute, normalized, and non-root", absolute)
	}
	relative := strings.TrimPrefix(absolute, "/")
	if err := ValidateRelative(relative); err != nil {
		return "", err
	}
	return relative, nil
}

// JoinRelative appends untrusted path components without string interpolation.
func JoinRelative(base string, components ...string) (string, error) {
	if err := ValidateRelative(base); err != nil {
		return "", err
	}
	joined := base
	for _, component := range components {
		if component == "" || strings.Contains(component, "/") || component == "." || component == ".." {
			return "", fmt.Errorf("path component %q is unsafe", component)
		}
		joined = path.Join(joined, component)
	}
	if err := ValidateRelative(joined); err != nil {
		return "", err
	}
	return joined, nil
}
