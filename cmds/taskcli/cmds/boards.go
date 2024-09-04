package cmds

import (
	"strconv"
	"strings"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

func GetBoardsCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use: "boards [id]",
		Run: func(cmd *cobra.Command, args []string) {
			cli := root.Boards()

			if len(args) > 0 {
				res, err := cli.GetBoard(root.Context(), connect.NewRequest(&tasksv1.GetBoardRequest{
					Id: args[1],
				}))
				if err != nil {
					logrus.Fatal(err.Error())
				}

				root.Print(res.Msg)
				return
			}

			res, err := cli.ListBoards(root.Context(), connect.NewRequest(new(tasksv1.ListBoardsRequest)))
			if err != nil {
				logrus.Fatal(err.Error())
			}

			root.Print(res.Msg)
		},
	}

	cmd.AddCommand(
		CreateBoardCommand(root),
		DeleteBoardCommand(root),
		AddNotificationCommand(root),
		DeleteNotificationCommand(root),
	)

	return cmd
}

func CreateBoardCommand(root *cli.Root) *cobra.Command {
	var req = &tasksv1.CreateBoardRequest{
		WritePermission: &tasksv1.BoardPermission{},
		ReadPermission:  &tasksv1.BoardPermission{},
	}

	cmd := &cobra.Command{
		Use: "create",
		Run: func(cmd *cobra.Command, args []string) {
			req.WritePermission.AllowRoles = root.MustResolveRoleIds(req.WritePermission.AllowRoles)
			req.WritePermission.DenyRoles = root.MustResolveRoleIds(req.WritePermission.DenyRoles)
			req.WritePermission.AllowUsers = root.MustResolveUserIds(req.WritePermission.AllowUsers)
			req.WritePermission.DenyUsers = root.MustResolveUserIds(req.WritePermission.DenyUsers)

			req.ReadPermission.AllowRoles = root.MustResolveRoleIds(req.ReadPermission.AllowRoles)
			req.ReadPermission.DenyRoles = root.MustResolveRoleIds(req.ReadPermission.DenyRoles)
			req.ReadPermission.AllowUsers = root.MustResolveUserIds(req.ReadPermission.AllowUsers)
			req.ReadPermission.DenyUsers = root.MustResolveUserIds(req.ReadPermission.DenyUsers)

			cli := root.Boards()

			res, err := cli.CreateBoard(root.Context(), connect.NewRequest(req))
			if err != nil {
				logrus.Fatal(err.Error())
			}

			root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&req.DisplayName, "display-name", "", "The display name for the new board")
		f.StringVar(&req.Description, "description", "", "An optional description for the board")

		f.StringSliceVar(&req.WritePermission.AllowRoles, "write-allow-roles", nil, "")
		f.StringSliceVar(&req.WritePermission.AllowUsers, "write-allow-users", nil, "")
		f.StringSliceVar(&req.WritePermission.DenyRoles, "write-deny-roles", nil, "")
		f.StringSliceVar(&req.WritePermission.DenyUsers, "write-deny-users", nil, "")

		f.StringSliceVar(&req.ReadPermission.AllowRoles, "read-allow-roles", nil, "")
		f.StringSliceVar(&req.ReadPermission.AllowUsers, "read-allow-users", nil, "")
		f.StringSliceVar(&req.ReadPermission.DenyRoles, "read-deny-roles", nil, "")
		f.StringSliceVar(&req.ReadPermission.DenyUsers, "read-deny-users", nil, "")
	}

	return cmd
}

func DeleteBoardCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "delete [id]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cli := root.Boards()

			res, err := cli.DeleteBoard(root.Context(), connect.NewRequest(&tasksv1.DeleteBoardRequest{
				Id: args[0],
			}))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	return cmd
}

func AddNotificationCommand(root *cli.Root) *cobra.Command {
	var (
		req              = &tasksv1.BoardNotification{}
		sendTimes        []string
		eventTypes       []string
		notificationType string
	)

	cmd := &cobra.Command{
		Use:  "add-notification [board-id]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			for _, t := range sendTimes {
				parts := strings.Split(t, ":")
				if len(parts) != 2 {
					logrus.Fatalf("invalid value %q for --send-at", t)
				}

				hour, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "0"), 10, 0)
				if err != nil {
					logrus.Fatalf("invalid value %q for --send-at", t)
				}

				min, err := strconv.ParseInt(strings.TrimPrefix(parts[1], "0"), 10, 0)
				if err != nil {
					logrus.Fatalf("invalid value %q for --send-at", t)
				}

				req.SendTimes = append(req.SendTimes, &commonv1.DayTime{
					Hour:   int32(hour),
					Minute: int32(min),
				})
			}

			switch notificationType {
			case "sms":
				req.NotificationType = tasksv1.NotificationType_NOTIFICATION_TYPE_SMS
			case "mail", "email":
				req.NotificationType = tasksv1.NotificationType_NOTIFICATION_TYPE_MAIL
			case "webpush", "push":
				req.NotificationType = tasksv1.NotificationType_NOTIFICATION_TYPE_WEBPUSH

			default:
				logrus.Fatalf("invalid notification type %q", notificationType)
			}

			for _, evt := range eventTypes {
				var e tasksv1.EventType

				switch evt {
				case "create":
					e = tasksv1.EventType_EVENT_TYPE_CREATED
				case "change":
					e = tasksv1.EventType_EVENT_TYPE_CHANGED
				case "update":
					e = tasksv1.EventType_EVENT_TYPE_UPDATED
				case "assign":
					e = tasksv1.EventType_EVENT_TYPE_ASSIGNEE_CHANGED
				case "completed":
					e = tasksv1.EventType_EVENT_TYPE_COMPLETED
				case "all":
					e = tasksv1.EventType_EVENT_TYPE_UNSPECIFIED

				default:
					logrus.Fatalf("invalid value for --event %q", evt)
				}

				req.EventTypes = append(req.EventTypes, e)
			}

			req.RecipientRoleIds = root.MustResolveRoleIds(req.RecipientRoleIds)
			req.RecipientUserIds = root.MustResolveUserIds(req.RecipientUserIds)

			cli := root.Boards()

			res, err := cli.SaveNotification(root.Context(), connect.NewRequest(&tasksv1.SaveNotificationRequest{
				BoardId:      args[0],
				Notification: req,
			}))

			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&req.Name, "name", "", "The name for the new notification")
		f.StringVar(&req.SubjectTemplate, "subject", "", "The subject template")
		f.StringVar(&req.MessageTemplate, "message", "", "The message template")
		f.StringSliceVar(&req.RecipientRoleIds, "to-role", nil, "A list of recipient roles")
		f.StringSliceVar(&req.RecipientUserIds, "to-user", nil, "A list of recipient users")
		f.StringSliceVar(&sendTimes, "send-at", nil, "A list of time-of-day at which to send notifications")
		f.StringSliceVar(&eventTypes, "event", nil, "A list of event type to send notifications for")
		f.StringVar(&notificationType, "type", "sms", "Which notification type to use: sms, mail or webpush")
	}

	return cmd
}

func DeleteNotificationCommand(root *cli.Root) *cobra.Command {
	return &cobra.Command{
		Use:  "delete-notification [board-id] [notification-name]",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cli := root.Boards()

			res, err := cli.DeleteNotification(root.Context(), connect.NewRequest(&tasksv1.DeleteNotificationRequest{
				BoardId:          args[0],
				NotificationName: args[1],
			}))

			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}
}
