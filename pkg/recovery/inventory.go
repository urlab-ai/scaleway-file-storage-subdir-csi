package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/safety"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const (
	maxRecoverableObjectProjectionBytes = 2 * 1024 * 1024
	maxExternalInventoryBytes           = 32 * 1024 * 1024
)

// OwnershipInventoryEntry is the exact canonical projection of one detailed
// or compact filesystem ownership record in an external checkpoint inventory.
type OwnershipInventoryEntry struct {
	Path         string                     `json:"path"`
	RecordSHA256 string                     `json:"recordSHA256"`
	Revision     uint64                     `json:"revision"`
	RecordKind   volume.OwnershipRecordKind `json:"recordKind"`
	State        volume.AllocationState     `json:"state"`
}

// ParentInventory is the O(1)-per-parent summary embedded in the in-cluster
// checkpoint manifest. Detailed entries remain in the external package.
type ParentInventory struct {
	ParentFilesystemID string `json:"parentFilesystemID"`
	ParentOwnerSHA256  string `json:"parentOwnerSHA256"`
	RecordCount        uint64 `json:"recordCount"`
	AggregateSHA256    string `json:"aggregateSHA256"`
}

// BuildParentInventory validates and sorts detailed entries, authenticates the
// parent claim bytes, and returns the manifest summary plus canonical external
// inventory bytes.
func BuildParentInventory(parentFilesystemID string, parentOwnerBytes []byte, entries []OwnershipInventoryEntry) (ParentInventory, []byte, error) {
	if err := volume.ValidateParentFilesystemID(parentFilesystemID); err != nil {
		return ParentInventory{}, nil, err
	}
	claim, err := volume.DecodeParentOwnerRecord(parentOwnerBytes)
	if err != nil {
		return ParentInventory{}, nil, fmt.Errorf("decode parent owner: %w", err)
	}
	if claim.ParentFilesystemID != parentFilesystemID {
		return ParentInventory{}, nil, fmt.Errorf("parent owner claim ID differs from inventory parent")
	}
	// Preserve an explicit empty array. A configured parent with no ownership
	// records is a complete inventory, while JSON null cannot prove that the
	// enumeration completed and is deliberately rejected by the decoder.
	ordered := make([]OwnershipInventoryEntry, len(entries))
	copy(ordered, entries)
	slices.SortFunc(ordered, func(left, right OwnershipInventoryEntry) int {
		return strings.Compare(left.Path, right.Path)
	})
	for index, entry := range ordered {
		if err := entry.Validate(); err != nil {
			return ParentInventory{}, nil, fmt.Errorf("ownership inventory entry %d: %w", index, err)
		}
		if index > 0 && ordered[index-1].Path == entry.Path {
			return ParentInventory{}, nil, fmt.Errorf("duplicate ownership inventory path %q", entry.Path)
		}
	}
	encoded, err := canonicaljson.Marshal(ordered)
	if err != nil {
		return ParentInventory{}, nil, err
	}
	if len(encoded) == 0 || len(encoded) > maxExternalInventoryBytes {
		return ParentInventory{}, nil, fmt.Errorf("ownership inventory must contain 1 to %d bytes", maxExternalInventoryBytes)
	}
	return ParentInventory{
		ParentFilesystemID: parentFilesystemID,
		ParentOwnerSHA256:  SHA256Digest(parentOwnerBytes),
		RecordCount:        uint64(len(ordered)),
		AggregateSHA256:    SHA256Digest(encoded),
	}, encoded, nil
}

// DecodeOwnershipInventory accepts only the sorted canonical closed external
// inventory representation. It does not authenticate record bytes; callers use
// VerifyParentInventoryExport for that complete boundary.
func DecodeOwnershipInventory(data []byte) ([]OwnershipInventoryEntry, error) {
	if len(data) == 0 || len(data) > maxExternalInventoryBytes {
		return nil, fmt.Errorf("ownership inventory must contain 1 to %d bytes", maxExternalInventoryBytes)
	}
	var entries []OwnershipInventoryEntry
	if err := strictjson.Decode(data, &entries); err != nil {
		return nil, err
	}
	if entries == nil {
		return nil, fmt.Errorf("ownership inventory must be an explicit JSON array")
	}
	for index, entry := range entries {
		if err := entry.Validate(); err != nil {
			return nil, fmt.Errorf("ownership inventory entry %d: %w", index, err)
		}
		if index > 0 && entries[index-1].Path >= entry.Path {
			return nil, fmt.Errorf("ownership inventory paths are unsorted or duplicated at %q", entry.Path)
		}
	}
	canonical, err := canonicaljson.Marshal(entries)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, data) {
		return nil, fmt.Errorf("ownership inventory JSON is not canonical")
	}
	return entries, nil
}

// Validate checks one closed inventory projection without reading the record.
// The caller separately verifies RecordSHA256 against the complete bytes and
// decodes those bytes through volume.DecodeOwnershipRecord.
func (entry OwnershipInventoryEntry) Validate() error {
	if entry.Path == "." {
		return fmt.Errorf("ownership record path cannot be the parent root")
	}
	if err := safety.ValidateRelative(entry.Path); err != nil {
		return err
	}
	if !strings.HasSuffix(entry.Path, ".json") {
		return fmt.Errorf("ownership record path %q is not a JSON metadata path", entry.Path)
	}
	if !validSHA256Digest(entry.RecordSHA256) {
		return fmt.Errorf("ownership record SHA-256 is malformed")
	}
	if entry.Revision == 0 {
		return fmt.Errorf("ownership record revision must be positive")
	}
	switch entry.RecordKind {
	case volume.OwnershipRecordDetailed:
		if entry.State != volume.StateReady && entry.State != volume.StateDeleting && entry.State != volume.StateArchived && entry.State != volume.StateRetained {
			return fmt.Errorf("detailed ownership inventory state %q is unsupported", entry.State)
		}
	case volume.OwnershipRecordCompactDeleted:
		if entry.State != volume.StateDeleted {
			return fmt.Errorf("compact ownership inventory state must be Deleted")
		}
	default:
		return fmt.Errorf("ownership inventory kind %q is unsupported", entry.RecordKind)
	}
	return nil
}

// VerifyOwnershipInventoryEntry authenticates the full record bytes against one
// inventory entry and its closed schema projection.
func VerifyOwnershipInventoryEntry(entry OwnershipInventoryEntry, recordBytes []byte) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	if SHA256Digest(recordBytes) != entry.RecordSHA256 {
		return fmt.Errorf("ownership record content digest mismatch")
	}
	record, err := volume.DecodeOwnershipRecord(recordBytes)
	if err != nil {
		return err
	}
	var revision uint64
	switch typed := record.(type) {
	case *volume.DetailedOwnershipRecord:
		revision = typed.Revision
	case *volume.CompactDeletedOwnershipRecord:
		revision = typed.Revision
	default:
		return fmt.Errorf("unsupported ownership record type %T", record)
	}
	if record.Kind() != entry.RecordKind || record.LifecycleState() != entry.State || revision != entry.Revision {
		return fmt.Errorf("ownership record projection differs from inventory entry")
	}
	return nil
}

// VerifyParentInventoryExport authenticates a complete configured-parent
// inventory against its manifest summary, immutable parent claim, and exact
// ownership bytes. Missing, extra, renamed, duplicated, or changed records all
// fail before the snapshot can be labelled complete.
func VerifyParentInventoryExport(ctx context.Context, summary ParentInventory, parentOwnerBytes, inventoryBytes []byte, records map[string][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := DecodeOwnershipInventory(inventoryBytes)
	if err != nil {
		return err
	}
	computed, canonical, err := BuildParentInventory(summary.ParentFilesystemID, parentOwnerBytes, entries)
	if err != nil {
		return err
	}
	if computed != summary || !bytes.Equal(canonical, inventoryBytes) {
		return fmt.Errorf("parent inventory summary differs from exported inventory")
	}
	claim, err := volume.DecodeParentOwnerRecord(parentOwnerBytes)
	if err != nil {
		return fmt.Errorf("decode exported parent owner: %w", err)
	}
	if len(records) != len(entries) {
		return fmt.Errorf("parent ownership export has %d records, inventory requires %d", len(records), len(entries))
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		recordBytes, present := records[entry.Path]
		if !present {
			return fmt.Errorf("parent ownership export is missing inventory path %q", entry.Path)
		}
		if err := verifyOwnershipEntryForParent(entry, recordBytes, claim); err != nil {
			return fmt.Errorf("verify parent ownership path %q: %w", entry.Path, err)
		}
		seen[entry.Path] = struct{}{}
	}
	for recordPath := range records {
		if _, present := seen[recordPath]; !present {
			return fmt.Errorf("parent ownership export contains extra path %q", recordPath)
		}
	}
	return nil
}

func verifyOwnershipEntryForParent(entry OwnershipInventoryEntry, recordBytes []byte, claim volume.ParentOwnerRecord) error {
	if err := VerifyOwnershipInventoryEntry(entry, recordBytes); err != nil {
		return err
	}
	record, err := volume.DecodeOwnershipRecord(recordBytes)
	if err != nil {
		return err
	}
	var driverName, installationID, clusterUID, parentID, basePathHash, logicalID string
	switch typed := record.(type) {
	case *volume.DetailedOwnershipRecord:
		driverName, installationID, clusterUID = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
		parentID, basePathHash, logicalID = typed.ParentFilesystemID, typed.BasePathHash, typed.LogicalVolumeID
		if typed.BasePath != claim.BasePath {
			return fmt.Errorf("detailed ownership base path differs from parent claim")
		}
	case *volume.CompactDeletedOwnershipRecord:
		driverName, installationID, clusterUID = typed.DriverName, typed.InstallationID, typed.ActiveClusterUID
		parentID, basePathHash, logicalID = typed.ParentFilesystemID, typed.BasePathHash, typed.LogicalVolumeID
	default:
		return fmt.Errorf("unsupported ownership record type %T", record)
	}
	if driverName != claim.DriverName || installationID != claim.InstallationID || clusterUID != claim.ActiveClusterUID || parentID != claim.ParentFilesystemID || basePathHash != claim.BasePathHash {
		return fmt.Errorf("ownership identity differs from immutable parent claim")
	}
	expectedPath := path.Join(strings.TrimPrefix(claim.BasePath, "/"), volume.OwnershipMetadataDirectory, logicalID+".json")
	if entry.Path != expectedPath {
		return fmt.Errorf("ownership inventory path %q differs from deterministic path %q", entry.Path, expectedPath)
	}
	return nil
}

// KubernetesObjectInventoryEntry records source consistency evidence alongside
// the hash of the schema-defined recoverable projection. Server-assigned values
// are excluded from the recoverable aggregate used after restore.
type KubernetesObjectInventoryEntry struct {
	APIVersion            string `json:"apiVersion"`
	Kind                  string `json:"kind"`
	Namespace             string `json:"namespace,omitempty"`
	Name                  string `json:"name"`
	SourceUID             string `json:"sourceUID"`
	SourceResourceVersion string `json:"sourceResourceVersion"`
	RecoverableSHA256     string `json:"recoverableSHA256"`
}

type recoverableObjectProjection struct {
	APIVersion        string `json:"apiVersion"`
	Kind              string `json:"kind"`
	Namespace         string `json:"namespace,omitempty"`
	Name              string `json:"name"`
	RecoverableSHA256 string `json:"recoverableSHA256"`
}

// ObjectInventorySummary is embedded in the checkpoint manifest.
type ObjectInventorySummary struct {
	Count           uint64 `json:"count"`
	AggregateSHA256 string `json:"aggregateSHA256"`
}

// RestoredKubernetesObject is one live schema-defined recoverable projection
// after restore. Kubernetes-assigned UID and resourceVersion are intentionally
// absent: they prove export consistency at checkpoint time but cannot survive
// namespace object recreation.
type RestoredKubernetesObject struct {
	APIVersion            string
	Kind                  string
	Namespace             string
	Name                  string
	RecoverableProjection []byte
}

// ExportedKubernetesObject is the backup tool's live object projection. The
// caller must derive RecoverableProjection from the schema-defined fields of
// the exact object carrying SourceUID and SourceResourceVersion; this verifier
// hashes those bytes and binds them to the quiesced detailed inventory.
type ExportedKubernetesObject struct {
	APIVersion            string
	Kind                  string
	Namespace             string
	Name                  string
	SourceUID             string
	SourceResourceVersion string
	RecoverableProjection []byte
}

// BuildKubernetesObjectInventory returns canonical external bytes and the
// restore-stable aggregate over logical identity and recoverable content only.
func BuildKubernetesObjectInventory(entries []KubernetesObjectInventoryEntry) (ObjectInventorySummary, []byte, error) {
	ordered := slices.Clone(entries)
	slices.SortFunc(ordered, compareObjectEntries)
	projections := make([]recoverableObjectProjection, 0, len(ordered))
	for index, entry := range ordered {
		if err := entry.Validate(); err != nil {
			return ObjectInventorySummary{}, nil, fmt.Errorf("kubernetes object inventory entry %d: %w", index, err)
		}
		if index > 0 && compareObjectEntries(ordered[index-1], entry) == 0 {
			return ObjectInventorySummary{}, nil, fmt.Errorf("duplicate Kubernetes object inventory identity %s/%s/%s", entry.Kind, entry.Namespace, entry.Name)
		}
		projections = append(projections, recoverableObjectProjection{
			APIVersion: entry.APIVersion, Kind: entry.Kind, Namespace: entry.Namespace,
			Name: entry.Name, RecoverableSHA256: entry.RecoverableSHA256,
		})
	}
	external, err := canonicaljson.Marshal(ordered)
	if err != nil {
		return ObjectInventorySummary{}, nil, err
	}
	if len(external) == 0 || len(external) > maxExternalInventoryBytes {
		return ObjectInventorySummary{}, nil, fmt.Errorf("kubernetes object inventory must contain 1 to %d bytes", maxExternalInventoryBytes)
	}
	recoverable, err := canonicaljson.Marshal(projections)
	if err != nil {
		return ObjectInventorySummary{}, nil, err
	}
	return ObjectInventorySummary{Count: uint64(len(ordered)), AggregateSHA256: SHA256Digest(recoverable)}, external, nil
}

// BuildRestoredKubernetesObjectSummary recomputes the exact restore-stable
// aggregate embedded in a checkpoint manifest. Each projection must already be
// canonical JSON produced from the closed schema for that object kind.
func BuildRestoredKubernetesObjectSummary(objects []RestoredKubernetesObject) (ObjectInventorySummary, error) {
	projections := make([]recoverableObjectProjection, 0, len(objects))
	for index, object := range objects {
		if err := validateKubernetesObjectIdentity(object.APIVersion, object.Kind, object.Namespace, object.Name); err != nil {
			return ObjectInventorySummary{}, fmt.Errorf("restored Kubernetes object %d: %w", index, err)
		}
		if len(object.RecoverableProjection) == 0 || len(object.RecoverableProjection) > maxRecoverableObjectProjectionBytes {
			return ObjectInventorySummary{}, fmt.Errorf("restored Kubernetes object %d recoverable projection must contain 1 to %d bytes", index, maxRecoverableObjectProjectionBytes)
		}
		if err := validateCanonicalProjection(object.RecoverableProjection); err != nil {
			return ObjectInventorySummary{}, fmt.Errorf("restored Kubernetes object %d recoverable projection: %w", index, err)
		}
		projections = append(projections, recoverableObjectProjection{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace,
			Name: object.Name, RecoverableSHA256: SHA256Digest(object.RecoverableProjection),
		})
	}
	slices.SortFunc(projections, compareRecoverableObjectProjections)
	for index := 1; index < len(projections); index++ {
		if compareRecoverableObjectProjections(projections[index-1], projections[index]) == 0 {
			object := projections[index]
			return ObjectInventorySummary{}, fmt.Errorf("duplicate restored Kubernetes object identity %s/%s/%s", object.Kind, object.Namespace, object.Name)
		}
	}
	encoded, err := canonicaljson.Marshal(projections)
	if err != nil {
		return ObjectInventorySummary{}, err
	}
	return ObjectInventorySummary{Count: uint64(len(projections)), AggregateSHA256: SHA256Digest(encoded)}, nil
}

// DecodeKubernetesObjectInventory accepts only the sorted canonical closed
// external inventory generated during the quiesced capture.
func DecodeKubernetesObjectInventory(data []byte) ([]KubernetesObjectInventoryEntry, error) {
	if len(data) == 0 || len(data) > maxExternalInventoryBytes {
		return nil, fmt.Errorf("kubernetes object inventory must contain 1 to %d bytes", maxExternalInventoryBytes)
	}
	var entries []KubernetesObjectInventoryEntry
	if err := strictjson.Decode(data, &entries); err != nil {
		return nil, err
	}
	if entries == nil {
		return nil, fmt.Errorf("kubernetes object inventory must be an explicit JSON array")
	}
	for index, entry := range entries {
		if err := entry.Validate(); err != nil {
			return nil, fmt.Errorf("kubernetes object inventory entry %d: %w", index, err)
		}
		if index > 0 && compareObjectEntries(entries[index-1], entry) >= 0 {
			return nil, fmt.Errorf("kubernetes object inventory identities are unsorted or duplicated at %s/%s/%s", entry.Kind, entry.Namespace, entry.Name)
		}
	}
	canonical, err := canonicaljson.Marshal(entries)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, data) {
		return nil, fmt.Errorf("kubernetes object inventory JSON is not canonical")
	}
	return entries, nil
}

// VerifyKubernetesObjectExport proves that the exported objects have the exact
// logical identities, source generations, and recoverable bytes captured while
// quiesced, and that the detailed inventory matches the manifest summary.
func VerifyKubernetesObjectExport(ctx context.Context, summary ObjectInventorySummary, inventoryBytes []byte, objects []ExportedKubernetesObject) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	expected, err := DecodeKubernetesObjectInventory(inventoryBytes)
	if err != nil {
		return err
	}
	computedSummary, canonical, err := BuildKubernetesObjectInventory(expected)
	if err != nil {
		return err
	}
	if computedSummary != summary || !bytes.Equal(canonical, inventoryBytes) {
		return fmt.Errorf("kubernetes object inventory summary differs from manifest")
	}
	actual := make([]KubernetesObjectInventoryEntry, 0, len(objects))
	for index, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(object.RecoverableProjection) == 0 || len(object.RecoverableProjection) > maxRecoverableObjectProjectionBytes {
			return fmt.Errorf("exported Kubernetes object %d recoverable projection must contain 1 to %d bytes", index, maxRecoverableObjectProjectionBytes)
		}
		if err := validateCanonicalProjection(object.RecoverableProjection); err != nil {
			return fmt.Errorf("exported Kubernetes object %d recoverable projection: %w", index, err)
		}
		actual = append(actual, KubernetesObjectInventoryEntry{
			APIVersion: object.APIVersion, Kind: object.Kind, Namespace: object.Namespace,
			Name: object.Name, SourceUID: object.SourceUID,
			SourceResourceVersion: object.SourceResourceVersion,
			RecoverableSHA256:     SHA256Digest(object.RecoverableProjection),
		})
	}
	actualSummary, actualBytes, err := BuildKubernetesObjectInventory(actual)
	if err != nil {
		return err
	}
	if actualSummary != summary || !bytes.Equal(actualBytes, inventoryBytes) {
		return fmt.Errorf("exported Kubernetes objects differ from quiesced inventory")
	}
	return nil
}

func validateCanonicalProjection(data []byte) error {
	var projection any
	if err := strictjson.Decode(data, &projection); err != nil {
		return err
	}
	canonical, err := canonicaljson.Marshal(projection)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return fmt.Errorf("projection JSON is not canonical")
	}
	return nil
}

// Validate checks one bounded object identity and source export generation.
func (entry KubernetesObjectInventoryEntry) Validate() error {
	if err := validateKubernetesObjectIdentity(entry.APIVersion, entry.Kind, entry.Namespace, entry.Name); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"sourceUID": entry.SourceUID, "sourceResourceVersion": entry.SourceResourceVersion,
	} {
		if !utf8.ValidString(value) || len(value) == 0 || len(value) > 253 || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s must contain 1 to 253 safe UTF-8 bytes", name)
		}
	}
	if !validSHA256Digest(entry.RecoverableSHA256) {
		return fmt.Errorf("recoverable object SHA-256 is malformed")
	}
	return nil
}

func validateKubernetesObjectIdentity(apiVersion, kind, namespace, name string) error {
	for field, value := range map[string]string{"apiVersion": apiVersion, "kind": kind, "name": name} {
		if !utf8.ValidString(value) || len(value) == 0 || len(value) > 253 || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s must contain 1 to 253 safe UTF-8 bytes", field)
		}
	}
	if !utf8.ValidString(namespace) || len(namespace) > 253 || strings.ContainsAny(namespace, "\x00\r\n") {
		return fmt.Errorf("namespace is invalid")
	}
	return nil
}

// SHA256Digest returns the canonical prefixed lowercase digest used by every
// checkpoint inventory.
func SHA256Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validSHA256Digest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	if strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func compareObjectEntries(left, right KubernetesObjectInventoryEntry) int {
	for _, compared := range []int{
		strings.Compare(left.APIVersion, right.APIVersion),
		strings.Compare(left.Kind, right.Kind),
		strings.Compare(left.Namespace, right.Namespace),
		strings.Compare(left.Name, right.Name),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}

func compareRecoverableObjectProjections(left, right recoverableObjectProjection) int {
	for _, compared := range []int{
		strings.Compare(left.APIVersion, right.APIVersion),
		strings.Compare(left.Kind, right.Kind),
		strings.Compare(left.Namespace, right.Namespace),
		strings.Compare(left.Name, right.Name),
	} {
		if compared != 0 {
			return compared
		}
	}
	return 0
}
