package recovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const maxCheckpointTicketBytes = 2 * 1024 * 1024

// CheckpointInventoryCommitment authenticates one exact detailed inventory
// without copying its potentially large contents through the local control
// socket. Size is checked before the backup tool hashes or decodes a file.
type CheckpointInventoryCommitment struct {
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"sizeBytes"`
}

// CheckpointParentCommitment binds one configured parent ID to the exact
// canonical detailed inventory captured under quiesce.
type CheckpointParentCommitment struct {
	ParentFilesystemID string `json:"parentFilesystemID"`
	CheckpointInventoryCommitment
}

// CheckpointTicket is the bounded prepare result transported to csi-admin. The
// canonical manifest is included because it becomes checkpoint.json; detailed
// Kubernetes and parent inventories stay outside the control socket and are
// represented by exact SHA-256 and byte-size commitments.
type CheckpointTicket struct {
	SchemaVersion             string                        `json:"schemaVersion"`
	CheckpointRequestID       string                        `json:"checkpointRequestID"`
	Manifest                  json.RawMessage               `json:"manifest"`
	ManifestSHA256            string                        `json:"manifestSHA256"`
	KubernetesObjectInventory CheckpointInventoryCommitment `json:"kubernetesObjectInventory"`
	Parents                   []CheckpointParentCommitment  `json:"parents"`
}

// BuildCheckpointTicket derives a small immutable control result from a
// complete captured candidate. The ticket remains O(number of parents) even
// when detailed inventories contain the supported volume envelope.
func BuildCheckpointTicket(candidate CheckpointCandidate) (CheckpointTicket, error) {
	if err := candidate.Validate(); err != nil {
		return CheckpointTicket{}, err
	}
	manifestBytes, err := EncodeCheckpointManifest(candidate.Manifest)
	if err != nil {
		return CheckpointTicket{}, err
	}
	ticket := CheckpointTicket{
		SchemaVersion:       volume.SchemaVersionV1,
		CheckpointRequestID: candidate.Manifest.CheckpointRequestID,
		Manifest:            bytes.Clone(manifestBytes),
		ManifestSHA256:      SHA256Digest(manifestBytes),
		KubernetesObjectInventory: CheckpointInventoryCommitment{
			SHA256:    SHA256Digest(candidate.KubernetesObjectInventoryBytes),
			SizeBytes: uint64(len(candidate.KubernetesObjectInventoryBytes)),
		},
		Parents: make([]CheckpointParentCommitment, 0, len(candidate.Manifest.Parents)),
	}
	for _, parent := range candidate.Manifest.Parents {
		inventory := candidate.ParentInventoryBytes[parent.ParentFilesystemID]
		ticket.Parents = append(ticket.Parents, CheckpointParentCommitment{
			ParentFilesystemID: parent.ParentFilesystemID,
			CheckpointInventoryCommitment: CheckpointInventoryCommitment{
				SHA256: SHA256Digest(inventory), SizeBytes: uint64(len(inventory)),
			},
		})
	}
	if err := ticket.Validate(); err != nil {
		return CheckpointTicket{}, err
	}
	return ticket, nil
}

// Validate proves that all commitments are sorted, bounded, and consistent
// with the embedded canonical manifest.
func (ticket CheckpointTicket) Validate() error {
	if ticket.SchemaVersion != volume.SchemaVersionV1 {
		return fmt.Errorf("checkpoint ticket schema version %q is unsupported", ticket.SchemaVersion)
	}
	if err := volume.ValidateOperationID(ticket.CheckpointRequestID); err != nil {
		return fmt.Errorf("checkpoint ticket request ID: %w", err)
	}
	manifest, err := DecodeCheckpointManifest(ticket.Manifest)
	if err != nil {
		return fmt.Errorf("checkpoint ticket manifest: %w", err)
	}
	if manifest.CheckpointRequestID != ticket.CheckpointRequestID {
		return fmt.Errorf("checkpoint ticket request ID differs from embedded manifest")
	}
	if ticket.ManifestSHA256 != SHA256Digest(ticket.Manifest) {
		return fmt.Errorf("checkpoint ticket manifest SHA-256 differs from embedded manifest")
	}
	if err := ticket.KubernetesObjectInventory.validate("Kubernetes object"); err != nil {
		return err
	}
	if len(ticket.Parents) != len(manifest.Parents) {
		return fmt.Errorf("checkpoint ticket has %d parent commitments, manifest requires %d", len(ticket.Parents), len(manifest.Parents))
	}
	for index, parent := range ticket.Parents {
		if err := volume.ValidateParentFilesystemID(parent.ParentFilesystemID); err != nil {
			return err
		}
		if index > 0 && ticket.Parents[index-1].ParentFilesystemID >= parent.ParentFilesystemID {
			return fmt.Errorf("checkpoint ticket parent commitments are unsorted or duplicated")
		}
		if parent.ParentFilesystemID != manifest.Parents[index].ParentFilesystemID {
			return fmt.Errorf("checkpoint ticket parent %d differs from manifest parent", index)
		}
		if err := parent.validate("parent " + parent.ParentFilesystemID); err != nil {
			return err
		}
		if parent.SHA256 != manifest.Parents[index].AggregateSHA256 {
			return fmt.Errorf("checkpoint ticket parent %q digest differs from manifest aggregate", parent.ParentFilesystemID)
		}
	}
	return nil
}

func (commitment CheckpointInventoryCommitment) validate(name string) error {
	if !validSHA256Digest(commitment.SHA256) {
		return fmt.Errorf("checkpoint %s inventory SHA-256 is malformed", name)
	}
	if commitment.SizeBytes == 0 || commitment.SizeBytes > maxExternalInventoryBytes {
		return fmt.Errorf("checkpoint %s inventory size must be between 1 and %d bytes", name, maxExternalInventoryBytes)
	}
	return nil
}

// VerifyDetailedInventories proves that externally built canonical inventory
// bytes are exactly the ones captured under quiesce. The caller must then pass
// those same bytes and exported records to VerifyCheckpointExportPackage,
// which authenticates every object and ownership record rather than trusting
// commitments alone.
func (ticket CheckpointTicket) VerifyDetailedInventories(kubernetesInventory []byte, parentInventories map[string][]byte) error {
	if err := ticket.Validate(); err != nil {
		return err
	}
	if err := verifyInventoryCommitment("Kubernetes object", ticket.KubernetesObjectInventory, kubernetesInventory); err != nil {
		return err
	}
	if _, err := DecodeKubernetesObjectInventory(kubernetesInventory); err != nil {
		return fmt.Errorf("checkpoint Kubernetes object inventory: %w", err)
	}
	if len(parentInventories) != len(ticket.Parents) {
		return fmt.Errorf("checkpoint export has %d parent inventories, ticket requires %d", len(parentInventories), len(ticket.Parents))
	}
	seen := make(map[string]struct{}, len(ticket.Parents))
	for _, parent := range ticket.Parents {
		inventory, present := parentInventories[parent.ParentFilesystemID]
		if !present {
			return fmt.Errorf("checkpoint export is missing parent inventory %q", parent.ParentFilesystemID)
		}
		if err := verifyInventoryCommitment("parent "+parent.ParentFilesystemID, parent.CheckpointInventoryCommitment, inventory); err != nil {
			return err
		}
		if _, err := DecodeOwnershipInventory(inventory); err != nil {
			return fmt.Errorf("checkpoint parent %q inventory: %w", parent.ParentFilesystemID, err)
		}
		seen[parent.ParentFilesystemID] = struct{}{}
	}
	for parentID := range parentInventories {
		if _, present := seen[parentID]; !present {
			return fmt.Errorf("checkpoint export contains extra parent inventory %q", parentID)
		}
	}
	return nil
}

func verifyInventoryCommitment(name string, commitment CheckpointInventoryCommitment, inventory []byte) error {
	if uint64(len(inventory)) != commitment.SizeBytes {
		return fmt.Errorf("checkpoint %s inventory size differs from prepare ticket", name)
	}
	if SHA256Digest(inventory) != commitment.SHA256 {
		return fmt.Errorf("checkpoint %s inventory SHA-256 differs from prepare ticket", name)
	}
	return nil
}

// Clone returns an isolated ticket for retry-safe admin results.
func (ticket CheckpointTicket) Clone() CheckpointTicket {
	clone := ticket
	clone.Manifest = bytes.Clone(ticket.Manifest)
	clone.Parents = slices.Clone(ticket.Parents)
	return clone
}

// EncodeCheckpointTicket returns canonical JSON bounded for the admin control
// response. Detailed inventories are excluded by construction.
func EncodeCheckpointTicket(ticket CheckpointTicket) ([]byte, error) {
	if err := ticket.Validate(); err != nil {
		return nil, err
	}
	encoded, err := canonicaljson.Marshal(ticket)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > maxCheckpointTicketBytes {
		return nil, fmt.Errorf("checkpoint ticket must contain 1 to %d bytes", maxCheckpointTicketBytes)
	}
	return encoded, nil
}

// DecodeCheckpointTicket accepts only the canonical closed prepare result.
func DecodeCheckpointTicket(data []byte) (CheckpointTicket, error) {
	if len(data) == 0 || len(data) > maxCheckpointTicketBytes {
		return CheckpointTicket{}, fmt.Errorf("checkpoint ticket must contain 1 to %d bytes", maxCheckpointTicketBytes)
	}
	var ticket CheckpointTicket
	if err := strictjson.Decode(data, &ticket); err != nil {
		return CheckpointTicket{}, err
	}
	if err := ticket.Validate(); err != nil {
		return CheckpointTicket{}, err
	}
	canonical, err := canonicaljson.Marshal(ticket)
	if err != nil {
		return CheckpointTicket{}, err
	}
	if !bytes.Equal(canonical, data) {
		return CheckpointTicket{}, fmt.Errorf("checkpoint ticket JSON is not canonical")
	}
	return ticket.Clone(), nil
}
