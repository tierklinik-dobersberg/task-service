package services

import (
	"context"
	"errors"
	"fmt"
	"os"

	connect "github.com/bufbuild/connect-go"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/task-service/internal/config"
	"github.com/tierklinik-dobersberg/task-service/internal/permission"
)

type Common struct {
	Resolver *permission.Resolver
	Config   config.Config
}

func (svc *Common) IsAllowed(ctx context.Context, board *tasksv1.Board, op string) error {
	// In debug mode or without a IdmURL we don't perform any
	// permisssion checks at all
	if svc.Resolver == nil {
		return nil
	}

	var permissions *tasksv1.BoardPermission

	switch op {
	case "read":
		permissions = board.ReadPermission
	case "write":
		permissions = board.WritePermission

	default:
		return fmt.Errorf("invalid operation %q", op)
	}

	// if there are not permissions set we allow the operation
	if permissions == nil {
		return nil
	}

	remoteUser := auth.From(ctx)
	if remoteUser == nil {
		if os.Getenv("DEBUG") != "" {
			return nil
		}

		return connect.NewError(connect.CodePermissionDenied, errors.New("authentication required"))
	}

	// the board owner is always allowed
	if board.OwnerId == remoteUser.ID {
		return nil
	}

	if !svc.Resolver.IsAllowed(ctx, remoteUser.ID, permissions) {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("you are not allowed to perform this operation"))
	}

	return nil
}
