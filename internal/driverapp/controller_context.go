package driverapp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
)

// controllerOperationContext combines the caller lifetime with every
// independent authority that permits a controller operation to continue.
//
// A request context alone is insufficient: the external-provisioner may keep
// it alive after this process has conclusively lost its Lease, and the local
// admin transport may still have an open connection while pod termination has
// begun.  Conversely, replacing the caller context would lose its deadline and
// values.  Context.AfterFunc lets us retain the caller as the parent while
// cancelling the derived context as soon as either controller authority ends,
// without leaving one goroutine behind per completed RPC.
func controllerOperationContext(request context.Context, authorities ...context.Context) (context.Context, context.CancelFunc, error) {
	if request == nil {
		return nil, nil, fmt.Errorf("controller operation request context is nil")
	}
	operation, cancel := context.WithCancel(request)
	stops := make([]func() bool, 0, len(authorities))
	for _, authority := range authorities {
		if authority == nil {
			cancel()
			for _, stop := range stops {
				stop()
			}
			return nil, nil, fmt.Errorf("controller operation authority context is nil")
		}
		stops = append(stops, context.AfterFunc(authority, cancel))
		if authority.Err() != nil {
			cancel()
		}
	}
	cleanup := func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
	return operation, cleanup, nil
}

// authorityBoundAdminHandler applies the same authority lifetime to operator
// workflows as CSI mutations. Individual workflows still perform their own
// command-specific leadership checks; this wrapper only guarantees that a
// workflow which has already started observes Lease loss or pod shutdown.
//
// The two terminal release commands are deliberately different. They first
// prove active leadership inside their workflow and then stop that exact
// leadership session before re-reading and CAS-updating the Lease. Binding the
// rest of the command to the leadership context would cancel its own final CAS
// as soon as Session.Stop executes. Those two commands therefore retain the
// request and process-shutdown lifetimes, while the LeaseRuntime's exact UID,
// holder, generation, and CAS checks remain the authority proof for release.
type authorityBoundAdminHandler struct {
	delegate   admin.OperationHandler
	leadership context.Context
	shutdown   context.Context
}

func newAuthorityBoundAdminHandler(delegate admin.OperationHandler, leadership, shutdown context.Context) (*authorityBoundAdminHandler, error) {
	if delegate == nil || leadership == nil || shutdown == nil {
		return nil, fmt.Errorf("authority-bound admin dependency is nil")
	}
	return &authorityBoundAdminHandler{delegate: delegate, leadership: leadership, shutdown: shutdown}, nil
}

func (handler *authorityBoundAdminHandler) HandleAdminOperation(ctx context.Context, command admin.Command, request admin.MutationRequest, payload json.RawMessage) (json.RawMessage, error) {
	authorities := []context.Context{handler.leadership, handler.shutdown}
	if command == admin.CommandUninstallRelease || command == admin.CommandDecommissionRelease {
		authorities = []context.Context{handler.shutdown}
	}
	operationCtx, cancel, err := controllerOperationContext(ctx, authorities...)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return handler.delegate.HandleAdminOperation(operationCtx, command, request, payload)
}
