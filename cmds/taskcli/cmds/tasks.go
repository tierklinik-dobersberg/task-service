package cmds

import (
	"strings"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TasksCommand(root *cli.Root) *cobra.Command {
	var (
		// Pagination
		pageSize int
		page     int
		sort     []string

		// Filter
		query     = &tasksv1.TaskQuery{}
		noResolve bool
	)

	cmd := &cobra.Command{
		Use: "tasks [id]",
		Run: func(cmd *cobra.Command, args []string) {
			cli := root.Tasks()

			if len(args) == 1 {
				res, err := cli.GetTask(root.Context(), connect.NewRequest(&tasksv1.GetTaskRequest{
					TaskId: args[0],
				}))
				if err != nil {
					logrus.Fatal(err)
				}

				root.Print(res.Msg)
				return
			}

			var pagination *commonv1.Pagination

			if pageSize > 0 || len(sort) > 0 {
				pagination = &commonv1.Pagination{
					PageSize: int32(pageSize),
					Kind: &commonv1.Pagination_Page{
						Page: int32(page),
					},
				}

				for _, field := range sort {
					direction := commonv1.SortDirection_SORT_DIRECTION_ASC
					if strings.HasPrefix(field, "-") {
						direction = commonv1.SortDirection_SORT_DIRECTION_DESC
						field = strings.TrimPrefix(field, "-")
					}

					pagination.SortBy = append(pagination.SortBy, &commonv1.Sort{
						FieldName: field,
						Direction: direction,
					})
				}
			}

			if !noResolve {
				query.AssignedTo = root.MustResolveUserIds(query.AssignedTo)
				query.CreatedBy = root.MustResolveUserIds(query.CreatedBy)
			}

			res, err := cli.ListTasks(root.Context(), connect.NewRequest(&tasksv1.ListTasksRequest{
				Queries:    []*tasksv1.TaskQuery{query},
				Pagination: pagination,
			}))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		// Pagination
		f.IntVar(&pageSize, "page-size", 0, "")
		f.IntVar(&page, "page", 0, "")
		f.StringSliceVar(&sort, "sort", nil, "")

		// Query
		f.StringSliceVar(&query.Tags, "tag", nil, "")
		f.StringSliceVar(&query.AssignedTo, "assignee", nil, "")
		f.StringSliceVar(&query.Statuses, "status", nil, "")
		f.StringSliceVar(&query.CreatedBy, "created-by", nil, "")
		f.StringSliceVar(&query.BoardId, "board", nil, "")

		f.BoolVar(&noResolve, "no-resolve-ids", false, "")
	}

	cmd.AddCommand(
		CreateTaskCommand(root),
		CompleteTaskCommand(root),
		DeleteTaskCommand(root),
		CompleteTaskCommand(root),
		AssignTaskCommand(root),
	)

	return cmd
}

func CreateTaskCommand(root *cli.Root) *cobra.Command {
	var (
		dueTime   string
		noResolve bool
	)

	req := &tasksv1.CreateTaskRequest{}

	cmd := &cobra.Command{
		Use: "create",
		Run: func(cmd *cobra.Command, args []string) {
			if dueTime != "" {
				t, err := time.Parse(time.RFC3339, dueTime)
				if err != nil {
					logrus.Fatalf("invalid value for --due-time: %s", err)
				}

				req.DueTime = timestamppb.New(t)
			}

			cli := root.Tasks()

			if req.AssigneeId != "" && !noResolve {
				user, err := root.ResolveUser(req.AssigneeId)
				if err != nil {
					logrus.Fatal(err)
				}

				req.AssigneeId = user.Id
			}

			res, err := cli.CreateTask(root.Context(), connect.NewRequest(req))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	f := cmd.Flags()
	{
		f.StringVar(&req.BoardId, "board", "", "The ID of the board")
		f.StringVar(&req.Title, "title", "", "The task title")
		f.StringVar(&req.Description, "description", "", "An optional task description")
		f.StringVar(&req.AssigneeId, "assignee", "", "")
		f.StringVar(&dueTime, "due-time", "", "")
		f.StringVar(&req.Status, "status", "", "")
		f.StringSliceVar(&req.Tags, "tag", nil, "")
		f.BoolVar(&noResolve, "no-resolve-ids", false, "Do not resolve user ids")
	}

	return cmd
}

func CompleteTaskCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "complete id",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			res, err := root.Tasks().CompleteTask(root.Context(), connect.NewRequest(&tasksv1.CompleteTaskRequest{
				TaskId: args[0],
			}))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	return cmd
}

func DeleteTaskCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "delete id",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			res, err := root.Tasks().DeleteTask(root.Context(), connect.NewRequest(&tasksv1.DeleteTaskRequest{
				TaskId: args[0],
			}))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	return cmd
}

func AssignTaskCommand(root *cli.Root) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "assign",
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			res, err := root.Tasks().AssignTask(root.Context(), connect.NewRequest(&tasksv1.AssignTaskRequest{
				TaskId:     args[0],
				AssigneeId: args[1],
			}))
			if err != nil {
				logrus.Fatal(err)
			}

			root.Print(res.Msg)
		},
	}

	return cmd
}
