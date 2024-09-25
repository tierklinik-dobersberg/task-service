package cmds

import (
	"github.com/bufbuild/connect-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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
	)

	return cmd
}

func CreateBoardCommand(root *cli.Root) *cobra.Command {
	var req = &tasksv1.CreateBoardRequest{
		WritePermission: &tasksv1.BoardPermission{},
		ReadPermission:  &tasksv1.BoardPermission{},
		Kind:            &tasksv1.CreateBoardRequest_List{},
	}

	var (
		noResolve bool
	)

	cmd := &cobra.Command{
		Use: "create",
		Run: func(cmd *cobra.Command, args []string) {
			if !noResolve {
				req.WritePermission.AllowRoles = root.MustResolveRoleIds(req.WritePermission.AllowRoles)
				req.WritePermission.DenyRoles = root.MustResolveRoleIds(req.WritePermission.DenyRoles)
				req.WritePermission.AllowUsers = root.MustResolveUserIds(req.WritePermission.AllowUsers)
				req.WritePermission.DenyUsers = root.MustResolveUserIds(req.WritePermission.DenyUsers)

				req.ReadPermission.AllowRoles = root.MustResolveRoleIds(req.ReadPermission.AllowRoles)
				req.ReadPermission.DenyRoles = root.MustResolveRoleIds(req.ReadPermission.DenyRoles)
				req.ReadPermission.AllowUsers = root.MustResolveUserIds(req.ReadPermission.AllowUsers)
				req.ReadPermission.DenyUsers = root.MustResolveUserIds(req.ReadPermission.DenyUsers)
			}

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

		f.BoolVar(&noResolve, "no-resolve-ids", false, "Do not try to resolve user or role ids")
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
