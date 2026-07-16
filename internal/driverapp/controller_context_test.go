package driverapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
)

type adminContextProbe struct {
	cancelAuthority func()
	wantCanceled    bool
}

func (probe adminContextProbe) HandleAdminOperation(ctx context.Context, _ admin.Command, _ admin.MutationRequest, _ json.RawMessage) (json.RawMessage, error) {
	probe.cancelAuthority()
	if probe.wantCanceled {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return nil, fmt.Errorf("admin operation context did not observe authority cancellation")
		}
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("terminal release context was canceled by the leadership session it intentionally stops: %w", ctx.Err())
	default:
		return json.RawMessage(`{"released":true}`), nil
	}
}

func TestAuthorityBoundAdminHandlerKeepsOrdinaryCommandsBoundToLeadership(t *testing.T) {
	leadershipCtx, cancelLeadership := context.WithCancel(context.Background())
	defer cancelLeadership()
	handler, err := newAuthorityBoundAdminHandler(
		adminContextProbe{cancelAuthority: cancelLeadership, wantCanceled: true},
		leadershipCtx, context.Background(),
	)
	if err != nil {
		t.Fatalf("newAuthorityBoundAdminHandler() error = %v", err)
	}
	_, err = handler.HandleAdminOperation(context.Background(), admin.CommandGCSubmit, admin.MutationRequest{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary admin command error = %v, want leadership cancellation", err)
	}
}

func TestAuthorityBoundAdminHandlerLetsTerminalReleaseFinishAfterLeadershipStops(t *testing.T) {
	for _, command := range []admin.Command{admin.CommandUninstallRelease, admin.CommandDecommissionRelease} {
		t.Run(string(command), func(t *testing.T) {
			leadershipCtx, cancelLeadership := context.WithCancel(context.Background())
			defer cancelLeadership()
			handler, err := newAuthorityBoundAdminHandler(
				adminContextProbe{cancelAuthority: cancelLeadership},
				leadershipCtx, context.Background(),
			)
			if err != nil {
				t.Fatalf("newAuthorityBoundAdminHandler() error = %v", err)
			}
			if _, err := handler.HandleAdminOperation(context.Background(), command, admin.MutationRequest{}, nil); err != nil {
				t.Fatalf("terminal release error = %v", err)
			}
		})
	}
}

func TestAuthorityBoundAdminHandlerKeepsTerminalReleaseBoundToShutdown(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	defer cancelShutdown()
	handler, err := newAuthorityBoundAdminHandler(
		adminContextProbe{cancelAuthority: cancelShutdown, wantCanceled: true},
		context.Background(), shutdownCtx,
	)
	if err != nil {
		t.Fatalf("newAuthorityBoundAdminHandler() error = %v", err)
	}
	_, err = handler.HandleAdminOperation(context.Background(), admin.CommandUninstallRelease, admin.MutationRequest{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("terminal release shutdown error = %v, want cancellation", err)
	}
}
