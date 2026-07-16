package driverapp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/k8s"
)

type controllerReadyMetric interface {
	SetReady(ready bool) error
}

// controllerAvailability is the single authority for cached controller
// readiness. Independent startup, maintenance, checkpoint, and shutdown states
// cannot accidentally overwrite each other with a last-writer-wins boolean.
type controllerAvailability struct {
	mu sync.RWMutex

	readiness *driver.Readiness
	metrics   controllerReadyMetric

	startupComplete    bool
	maintenanceHealthy bool
	maintenanceReason  string
	checkpointActive   bool
	decommissionActive bool
	uninstallActive    bool
	shuttingDown       bool
}

func newControllerAvailability(readiness *driver.Readiness, metrics controllerReadyMetric) (*controllerAvailability, error) {
	if readiness == nil || metrics == nil {
		return nil, fmt.Errorf("controller availability dependency is nil")
	}
	ready, reason := readiness.Snapshot()
	if ready || reason == "" {
		return nil, fmt.Errorf("controller availability requires initialized unready startup state")
	}
	return &controllerAvailability{readiness: readiness, metrics: metrics}, nil
}

func (availability *controllerAvailability) CompleteStartup() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.startupComplete = true
	availability.maintenanceHealthy = true
	availability.maintenanceReason = ""
	return availability.publishLocked()
}

func (availability *controllerAvailability) SetMaintenance(err error) error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	if err == nil {
		availability.maintenanceHealthy = true
		availability.maintenanceReason = ""
	} else {
		availability.maintenanceHealthy = false
		availability.maintenanceReason = boundedReadinessReason("controller maintenance is degraded: " + err.Error())
	}
	return availability.publishLocked()
}

func (availability *controllerAvailability) BeginCheckpoint() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.checkpointActive = true
	return availability.publishLocked()
}

func (availability *controllerAvailability) CompleteCheckpoint() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.checkpointActive = false
	return availability.publishLocked()
}

func (availability *controllerAvailability) BeginUninstall() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.uninstallActive = true
	return availability.publishLocked()
}

func (availability *controllerAvailability) BeginDecommission() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.decommissionActive = true
	return availability.publishLocked()
}

func (availability *controllerAvailability) Shutdown() error {
	availability.mu.Lock()
	defer availability.mu.Unlock()
	availability.shuttingDown = true
	return availability.publishLocked()
}

// RequireProvisioning blocks only operations that can allocate new logical or
// physical resources. Delete and Unpublish deliberately do not call this guard
// so a global inventory degradation cannot prevent safety-improving cleanup.
func (availability *controllerAvailability) RequireProvisioning(context.Context) error {
	availability.mu.RLock()
	ready, reason := availability.stateLocked()
	availability.mu.RUnlock()
	if ready {
		return nil
	}
	return fmt.Errorf("%w: %s", k8s.ErrUnavailable, reason)
}

func (availability *controllerAvailability) publishLocked() error {
	ready, reason := availability.stateLocked()
	if err := availability.metrics.SetReady(ready); err != nil {
		return err
	}
	return availability.readiness.Set(ready, reason)
}

func (availability *controllerAvailability) stateLocked() (bool, string) {
	switch {
	case availability.shuttingDown:
		return false, "controller process is shutting down"
	case availability.checkpointActive:
		return false, "controller checkpoint is quiesced"
	case availability.decommissionActive:
		return false, "controller parent decommission is quiesced"
	case availability.uninstallActive:
		return false, "controller safe uninstall is quiesced"
	case !availability.startupComplete:
		return false, controllerStartupUnready
	case !availability.maintenanceHealthy:
		return false, availability.maintenanceReason
	default:
		return true, ""
	}
}

func boundedReadinessReason(reason string) string {
	reason = strings.NewReplacer("\x00", " ", "\r", " ", "\n", " ").Replace(strings.ToValidUTF8(reason, "?"))
	if len(reason) <= 512 {
		return reason
	}
	reason = reason[:512]
	for !utf8.ValidString(reason) {
		reason = reason[:len(reason)-1]
	}
	return reason
}
