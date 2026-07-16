package driverapp

import (
	"context"
	"fmt"

	"scaleway-sfs-subdir-csi/pkg/config"
)

// Run constructs and serves the selected production component until the
// process context is canceled. Startup remains fail-closed: the controller may
// expose its local Identity/Probe shell while dependencies initialize, but its
// deferred Controller and admin cores reject every operation until the complete
// active runtime is installed. The node opens its socket only after local
// initialization is complete.
func Run(ctx context.Context, startup Startup) (returnErr error) {
	if ctx == nil {
		return fmt.Errorf("driver runtime context is nil")
	}
	logger, err := configureRuntimeLogging(startup.Config.LogLevel, startup.Options.Component, startup.Config.Runtime.DriverName)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "starting driver component")
	defer func() { logRuntimeCompletion(ctx, logger, returnErr) }()
	switch startup.Options.Component {
	case config.ComponentNode:
		returnErr = runNode(ctx, startup)
	case config.ComponentController:
		returnErr = runController(ctx, startup)
	default:
		returnErr = fmt.Errorf("unsupported driver runtime component %q", startup.Options.Component)
	}
	return returnErr
}
