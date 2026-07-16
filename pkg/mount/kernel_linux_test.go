//go:build linux

package mount

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

func TestParentMountReadinessUnavailableUsesClosedErrnoSet(t *testing.T) {
	for _, err := range []error{unix.EAGAIN, unix.ENODEV, unix.ENOENT, unix.ETIMEDOUT} {
		if !parentMountReadinessUnavailable(err) {
			t.Fatalf("parentMountReadinessUnavailable(%v) = false", err)
		}
	}
	for _, err := range []error{unix.EINVAL, unix.ENOSYS, unix.EOPNOTSUPP, unix.EPERM} {
		if parentMountReadinessUnavailable(err) {
			t.Fatalf("parentMountReadinessUnavailable(%v) = true", err)
		}
	}
	if !parentMountReadinessUnavailable(errors.Join(errors.New("wrapped"), unix.ENODEV)) {
		t.Fatal("wrapped ENODEV was not classified as transient")
	}
}
