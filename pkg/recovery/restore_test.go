package recovery

import (
	"strings"
	"testing"
)

func restoredStateFromManifest(manifest CheckpointManifest) RestoredCheckpointState {
	return RestoredCheckpointState{
		DriverName: manifest.DriverName, InstallationID: manifest.HolderEvidence.InstallationID,
		ActiveClusterUID: manifest.ActiveClusterUID, ChartVersion: manifest.ChartVersion,
		Images: append([]ImageDigest(nil), manifest.Images...), KubernetesObjects: manifest.KubernetesObjects,
		Parents: append([]ParentInventory(nil), manifest.Parents...),
	}
}

func TestVerifyRestoredCheckpointAcceptsExactUnorderedProjection(t *testing.T) {
	manifest := validCheckpointManifest(t)
	current := restoredStateFromManifest(manifest)
	current.Images[0], current.Images[1] = current.Images[1], current.Images[0]
	if err := VerifyRestoredCheckpoint(manifest, current); err != nil {
		t.Fatalf("VerifyRestoredCheckpoint() error = %v", err)
	}
}

func TestVerifyRestoredCheckpointRejectsEveryChangedRecoveryDimension(t *testing.T) {
	manifest := validCheckpointManifest(t)
	tests := map[string]func(*RestoredCheckpointState){
		"driver": func(state *RestoredCheckpointState) {
			state.DriverName = "other.csi.example.com"
		},
		"installation": func(state *RestoredCheckpointState) {
			state.InstallationID = "99999999-9999-4999-8999-999999999999"
		},
		"cluster": func(state *RestoredCheckpointState) {
			state.ActiveClusterUID = "different-cluster"
		},
		"chart": func(state *RestoredCheckpointState) {
			state.ChartVersion = "1.0.1"
		},
		"image": func(state *RestoredCheckpointState) {
			state.Images[0].Digest = "sha256:" + strings.Repeat("f", 64)
		},
		"object count": func(state *RestoredCheckpointState) {
			state.KubernetesObjects.Count++
		},
		"object digest": func(state *RestoredCheckpointState) {
			state.KubernetesObjects.AggregateSHA256 = "sha256:" + strings.Repeat("f", 64)
		},
		"parent owner": func(state *RestoredCheckpointState) {
			state.Parents[0].ParentOwnerSHA256 = "sha256:" + strings.Repeat("f", 64)
		},
		"parent count": func(state *RestoredCheckpointState) {
			state.Parents[0].RecordCount++
		},
		"parent digest": func(state *RestoredCheckpointState) {
			state.Parents[0].AggregateSHA256 = "sha256:" + strings.Repeat("f", 64)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			current := restoredStateFromManifest(manifest)
			mutate(&current)
			if err := VerifyRestoredCheckpoint(manifest, current); err == nil {
				t.Fatal("VerifyRestoredCheckpoint(changed state) error = nil")
			}
		})
	}
}

func TestVerifyRestoredCheckpointRejectsDuplicateRuntimeInventory(t *testing.T) {
	manifest := validCheckpointManifest(t)
	current := restoredStateFromManifest(manifest)
	current.Images = append(current.Images, current.Images[0])
	if err := VerifyRestoredCheckpoint(manifest, current); err == nil {
		t.Fatal("VerifyRestoredCheckpoint(duplicate image) error = nil")
	}

	current = restoredStateFromManifest(manifest)
	current.Parents = append(current.Parents, current.Parents[0])
	if err := VerifyRestoredCheckpoint(manifest, current); err == nil {
		t.Fatal("VerifyRestoredCheckpoint(duplicate parent) error = nil")
	}
}
