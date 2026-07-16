package volume

import (
	"fmt"
	"strings"
	"testing"
)

func testImmutableContext(t *testing.T) ImmutableContext {
	t.Helper()
	basePathHash, err := BasePathHash("/kubernetes-volumes")
	if err != nil {
		t.Fatalf("BasePathHash() error = %v", err)
	}
	return ImmutableContext{
		SchemaVersion:      SchemaVersionV1,
		InstallationID:     "11111111-1111-4111-8111-111111111111",
		ActiveClusterUID:   "22222222-2222-4222-8222-222222222222",
		PoolName:           "standard",
		ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
		BasePath:           "/kubernetes-volumes",
		BasePathHash:       basePathHash,
		DirectoryName:      "tenant--claim--0123456789ab",
		DirectoryMode:      "0770",
		DirectoryUID:       1000,
		DirectoryGID:       1000,
		DeletePolicy:       DeletePolicyArchive,
		LogicalVolumeID:    "lv-cba6af669a8d67780b6f36aecd3c58af",
	}
}

func TestImmutableContextRoundTrip(t *testing.T) {
	want := testImmutableContext(t)
	values, err := want.Map()
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	got, err := ParseImmutableContext(values)
	if err != nil {
		t.Fatalf("ParseImmutableContext() error = %v", err)
	}
	if got != want {
		t.Fatalf("ParseImmutableContext() = %#v, want %#v", got, want)
	}
}

func TestImmutableContextAcceptsOnlyExternalProvisionerDeliveryField(t *testing.T) {
	want := testImmutableContext(t)
	values, err := want.Map()
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	values[ExternalProvisionerIdentityKey] = "1783952485991-2771-sfs-subdir.csi.example.com"
	got, err := ParseImmutableContext(values)
	if err != nil || got != want {
		t.Fatalf("ParseImmutableContext(provisioner identity) = %#v, %v", got, err)
	}
	driverOwned, err := DriverOwnedContextMap(values)
	if err != nil {
		t.Fatalf("DriverOwnedContextMap() error = %v", err)
	}
	if len(driverOwned) != len(immutableContextKeys) {
		t.Fatalf("driver-owned context fields = %d", len(driverOwned))
	}
	if _, present := driverOwned[ExternalProvisionerIdentityKey]; present {
		t.Fatal("driver-owned context retained external-provisioner identity")
	}
	values[ExternalProvisionerIdentityKey] = ""
	if _, err := ParseImmutableContext(values); err == nil {
		t.Fatal("ParseImmutableContext(empty provisioner identity) error = nil")
	}
}

func TestParseImmutableContextRejectsMissingUnknownAndNonCanonicalFields(t *testing.T) {
	valid, err := testImmutableContext(t).Map()
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}

	tests := map[string]func(map[string]string){
		"missing":          func(values map[string]string) { delete(values, "poolName") },
		"unknown":          func(values map[string]string) { values["futureField"] = "value" },
		"noncanonical uid": func(values map[string]string) { values["directoryUid"] = "01000" },
		"wrong base hash":  func(values map[string]string) { values["basePathHash"] = "bp-00000000000000000000000000000000" },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(valid)+1)
			for key, value := range valid {
				values[key] = value
			}
			mutate(values)
			if _, err := ParseImmutableContext(values); err == nil {
				t.Fatal("ParseImmutableContext() error = nil, want rejection")
			}
		})
	}
}

func TestValidateWireContextBoundaries(t *testing.T) {
	exact := make(map[string]string, 32)
	for index := range 32 {
		key := strings.Repeat("k", 62) + fmt.Sprintf("%02d", index)
		exact[key] = strings.Repeat("v", 64)
	}
	if err := ValidateWireContext(exact); err != nil {
		t.Fatalf("ValidateWireContext(exact 4 KiB) error = %v", err)
	}

	for key := range exact {
		exact[key] += "x"
		break
	}
	if err := ValidateWireContext(exact); err == nil {
		t.Fatal("ValidateWireContext(4 KiB + 1) error = nil")
	}
}

func TestValidateWireContextUTF8ByteBoundaries(t *testing.T) {
	if err := ValidateWireContext(map[string]string{"key": strings.Repeat("é", 64)}); err != nil {
		t.Fatalf("ValidateWireContext(128-byte UTF-8 value) error = %v", err)
	}
	if err := ValidateWireContext(map[string]string{"key": strings.Repeat("é", 64) + "x"}); err == nil {
		t.Fatal("ValidateWireContext(129-byte UTF-8 value) error = nil")
	}
	if err := ValidateWireContext(map[string]string{strings.Repeat("é", 64): "value"}); err != nil {
		t.Fatalf("ValidateWireContext(128-byte UTF-8 key) error = %v", err)
	}
}
