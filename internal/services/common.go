package services

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"os"

	connect "github.com/bufbuild/connect-go"
	eventsv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1/eventsv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/task-service/internal/config"
	"github.com/tierklinik-dobersberg/task-service/internal/permission"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Common struct {
	Resolver *permission.Resolver
	Config   config.Config

	Mails embed.FS
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

func (svc *Common) PublishEvent(event proto.Message) {
	if svc.Config.EventsServiceUrl == "" {
		return
	}

	go func() {
		cli := eventsv1connect.NewEventServiceClient(cli.NewInsecureHttp2Client(), svc.Config.EventsServiceUrl)

		anypb, err := anypb.New(event)
		if err != nil {
			slog.Error("failed to prepare google.protobuf.Any for publishing", "error", err, "typeUrl", proto.MessageName(event))

			return
		}

		cli.Publish(context.Background(), connect.NewRequest(&eventsv1.Event{
			Event: anypb,
		}))
	}()
}
