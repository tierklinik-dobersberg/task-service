package services

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"

	connect "github.com/bufbuild/connect-go"
	eventsv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/events/v1/eventsv1connect"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/task-service/internal/config"
	"github.com/tierklinik-dobersberg/task-service/internal/permission"
	"github.com/tierklinik-dobersberg/task-service/mails/src/templates"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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

	if remoteUser.ID == "service-account" {
		return nil
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

func (svc *Common) SendNotification(board *tasksv1.Board, task *tasksv1.Task, comment *tasksv1.TaskComment, subject, senderId string, templateName string) {
	subscriptions := make(map[string]*tasksv1.Subscription)
	for user, sub := range board.Subscriptions {
		// TODO(ppacher): merge notification types?
		subscriptions[user] = sub
	}
	for user, sub := range task.Subscriptions {
		subscriptions[user] = sub
	}

	tmplCtx, err := templates.NewTemplateContext(board, task, comment)
	if err != nil {
		slog.Error("failed to prepare template context for comment notification", "error", err.Error())
		return
	}

	value, err := structpb.NewStruct(tmplCtx)
	if err != nil {
		slog.Error("failed to prepare template context for comment notification", "error", err.Error())
		return
	}

	body, err := svc.Mails.ReadFile(path.Join("mails", templateName))
	if err != nil {
		slog.Error("failed to read comment notification template", "error", err.Error())
		return
	}

	cli := idmv1connect.NewNotifyServiceClient(cli.NewInsecureHttp2Client(), svc.Config.IdmURL)
	for _, sub := range subscriptions {

		// Do not send notification updates to the user that performed that action
		if sub.UserId == senderId {
			continue
		}

		res, err := cli.SendNotification(context.Background(), connect.NewRequest(&idmv1.SendNotificationRequest{
			SenderUserId: senderId,
			TargetUsers:  []string{sub.UserId},
			PerUserTemplateContext: map[string]*structpb.Struct{
				sub.UserId: value,
			},
			Message: &idmv1.SendNotificationRequest_Email{
				Email: &idmv1.EMailMessage{
					Subject: subject,
					Body:    string(body),
				},
			},
		}))

		if err != nil {
			slog.Error("failed to send comment notification", "user", sub.UserId, "error", err.Error())
		} else {
			err := false
			for _, d := range res.Msg.Deliveries {
				if d.Error != "" {
					err = true
					slog.Error("failed to send comment notification", "user", sub.UserId, "error", d.Error, "kind", d.ErrorKind.String())
				}
			}

			if !err {
				slog.Info("successfully send comment notification", "user", sub.UserId)
			}
		}
	}
}
