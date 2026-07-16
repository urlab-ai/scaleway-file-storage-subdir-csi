package safety

import (
	"context"
	"reflect"
	"testing"
)

func TestValidateBootstrapRootEntriesAllowsOnlyExactRegularTemporary(t *testing.T) {
	const attemptID = "11111111-1111-4111-8111-111111111111"
	temporary := ".sfs-subdir-csi-owner." + attemptID + ".tmp"

	for name, test := range map[string]struct {
		entries []bootstrapRootEntry
		present bool
		valid   bool
	}{
		"empty":     {valid: true},
		"temporary": {entries: []bootstrapRootEntry{{name: temporary, regular: true}}, present: true, valid: true},
		"claim":     {entries: []bootstrapRootEntry{{name: ".sfs-subdir-csi-owner.json", regular: true}}},
		"directory": {entries: []bootstrapRootEntry{{name: temporary}}},
		"foreign":   {entries: []bootstrapRootEntry{{name: "workload-data", regular: true}}},
		"duplicate": {entries: []bootstrapRootEntry{{name: temporary, regular: true}, {name: temporary, regular: true}}},
	} {
		t.Run(name, func(t *testing.T) {
			state, err := validateBootstrapRootEntries(test.entries, attemptID, false)
			if test.valid && err != nil {
				t.Fatalf("validateBootstrapRootEntries() error = %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("validateBootstrapRootEntries(unsafe) error = nil")
			}
			if test.valid && state.AttemptTemporaryPresent != test.present {
				t.Fatalf("temporary present = %v, want %v", state.AttemptTemporaryPresent, test.present)
			}
		})
	}
	if _, err := validateBootstrapRootEntries(nil, "not-a-uuid", false); err == nil {
		t.Fatal("validateBootstrapRootEntries(invalid attempt) error = nil")
	}
	claimed, err := validateBootstrapRootEntries([]bootstrapRootEntry{
		{name: ".sfs-subdir-csi-owner.json", regular: true},
		{name: temporary, regular: true},
	}, attemptID, true)
	if err != nil || !claimed.ParentClaimPresent || !claimed.AttemptTemporaryPresent {
		t.Fatalf("validateBootstrapRootEntries(claimed retry) = %#v, %v", claimed, err)
	}
}

func TestEnsureParentLayoutCreatesRootOwnedDurableHierarchyAndRetriesBarriers(t *testing.T) {
	filesystem := NewFakeLifecycleFS()
	if err := filesystem.SeedDirectory(".", FakeLifecycleEntry{Mode: 0o700}); err != nil {
		t.Fatalf("SeedDirectory(root) error = %v", err)
	}
	if err := EnsureParentLayout(context.Background(), filesystem, "/tenants/kubernetes-volumes"); err != nil {
		t.Fatalf("EnsureParentLayout() error = %v", err)
	}
	wantDirectories := []string{
		"tenants",
		"tenants/kubernetes-volumes",
		"tenants/kubernetes-volumes/.archived",
		"tenants/kubernetes-volumes/.deleted",
		"tenants/kubernetes-volumes/.sfs-subdir-csi",
		"tenants/kubernetes-volumes/.sfs-subdir-csi/volumes",
	}
	entries := filesystem.Entries()
	for _, relative := range wantDirectories {
		entry, present := entries[relative]
		if !present || entry.Mode != 0o700 || entry.UID != 0 || entry.GID != 0 {
			t.Fatalf("layout entry %q = %#v, present=%v", relative, entry, present)
		}
	}
	firstOperations := filesystem.Operations()
	for _, relative := range wantDirectories {
		wantPrefix := []string{"mkdir:" + relative, "chown:" + relative, "chmod:" + relative, "sync-node:" + relative, "sync-dir:" + pathParent(relative)}
		if !containsOrderedSubsequence(firstOperations, wantPrefix) {
			t.Fatalf("operations %#v do not contain ordered barriers %#v", firstOperations, wantPrefix)
		}
	}
	if err := EnsureParentLayout(context.Background(), filesystem, "/tenants/kubernetes-volumes"); err != nil {
		t.Fatalf("EnsureParentLayout(retry) error = %v", err)
	}
	retryOperations := filesystem.Operations()[len(firstOperations):]
	wantRetry := make([]string, 0, len(wantDirectories)*2)
	for _, relative := range wantDirectories {
		wantRetry = append(wantRetry, "sync-node:"+relative, "sync-dir:"+pathParent(relative))
	}
	if !reflect.DeepEqual(retryOperations, wantRetry) {
		t.Fatalf("retry operations = %#v, want barriers %#v", retryOperations, wantRetry)
	}
}

func TestEnsureParentLayoutRejectsUnsafeExistingComponent(t *testing.T) {
	filesystem := NewFakeLifecycleFS()
	if err := filesystem.SeedDirectory(".", FakeLifecycleEntry{Mode: 0o700}); err != nil {
		t.Fatalf("SeedDirectory(root) error = %v", err)
	}
	if err := filesystem.SeedDirectory("kubernetes-volumes", FakeLifecycleEntry{Symlink: true}); err != nil {
		t.Fatalf("SeedDirectory(symlink) error = %v", err)
	}
	if err := EnsureParentLayout(context.Background(), filesystem, "/kubernetes-volumes"); err == nil {
		t.Fatal("EnsureParentLayout(symlink) error = nil")
	}
	if operations := filesystem.Operations(); len(operations) != 0 {
		t.Fatalf("unsafe layout inspection mutated filesystem: %#v", operations)
	}
}

func pathParent(relative string) string {
	for index := len(relative) - 1; index >= 0; index-- {
		if relative[index] == '/' {
			return relative[:index]
		}
	}
	return "."
}

func containsOrderedSubsequence(values, want []string) bool {
	index := 0
	for _, value := range values {
		if index < len(want) && value == want[index] {
			index++
		}
	}
	return index == len(want)
}
