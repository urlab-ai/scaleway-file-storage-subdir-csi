package driverapp

import (
	"fmt"
	"time"

	"scaleway-sfs-subdir-csi/pkg/observability"
)

// controllerCSIObserver couples the generic code-labelled CSI metrics with the
// legacy operation counters. Both count completed RPCs, including failures;
// the bounded canonical code label carries the outcome.
type controllerCSIObserver struct {
	metrics *observability.ControllerMetrics
}

func (observer controllerCSIObserver) ObserveCSI(operation observability.CSIOperation, code observability.RPCCode, duration time.Duration) error {
	if observer.metrics == nil {
		return fmt.Errorf("controller metrics registry is nil")
	}
	if err := observer.metrics.ObserveCSI(operation, code, duration); err != nil {
		return err
	}
	switch operation {
	case observability.CSICreateVolume:
		return observer.metrics.AddCreateVolume(1)
	case observability.CSIDeleteVolume:
		return observer.metrics.AddDeleteVolume(1)
	default:
		return nil
	}
}

// nodeCSIObserver couples the generic code-labelled CSI metrics with the
// operation counters retained by the v1 public metric contract.
type nodeCSIObserver struct {
	metrics *observability.NodeMetrics
}

func (observer nodeCSIObserver) ObserveCSI(operation observability.CSIOperation, code observability.RPCCode, duration time.Duration) error {
	if observer.metrics == nil {
		return fmt.Errorf("node metrics registry is nil")
	}
	if err := observer.metrics.ObserveCSI(operation, code, duration); err != nil {
		return err
	}
	switch operation {
	case observability.CSINodeStageVolume:
		return observer.metrics.AddNodeStageVolume(1)
	case observability.CSINodePublishVolume:
		return observer.metrics.AddNodePublishVolume(1)
	default:
		return nil
	}
}

// firstRuntimeFailureReporter retains the first internal observation failure
// without blocking an RPC completion path. The serving supervisor consumes the
// signal and terminates the process after the already-fixed RPC result returns.
func firstRuntimeFailureReporter(failures chan<- error) func(error) {
	return func(err error) {
		if err == nil {
			return
		}
		select {
		case failures <- err:
		default:
		}
	}
}
