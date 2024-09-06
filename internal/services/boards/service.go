package boards

import (
	"context"
	"errors"
	"fmt"
	"os"
	"text/template"

	"github.com/bufbuild/connect-go"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"github.com/tierklinik-dobersberg/task-service/internal/services"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	tasksv1connect.UnimplementedBoardServiceHandler

	*services.Common

	repo repo.Repo
}

func New(ctx context.Context, repo repo.Repo, common *services.Common) (*Service, error) {
	return &Service{
		repo:   repo,
		Common: common,
	}, nil
}

func (svc *Service) CreateBoard(ctx context.Context, req *connect.Request[tasksv1.CreateBoardRequest]) (*connect.Response[tasksv1.CreateBoardResponse], error) {
	remoteUser := auth.From(ctx)

	if remoteUser == nil {
		if os.Getenv("DEBUG") != "" {
			remoteUser = &auth.RemoteUser{}
		} else {
			return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authentication required"))
		}
	}

	for _, n := range req.Msg.Notifications {
		if _, err := template.New("").Parse(n.MessageTemplate); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%q: invalid message template: %w", n.Name, err))
		}

		if _, err := template.New("").Parse(n.SubjectTemplate); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%q: invalid subject template: %w", n.Name, err))
		}
	}

	cr := req.Msg

	model := &tasksv1.Board{
		DisplayName:       cr.DisplayName,
		Description:       cr.Description,
		ReadPermission:    cr.ReadPermission,
		WritePermission:   cr.WritePermission,
		Notifications:     cr.Notifications,
		OwnerId:           remoteUser.ID,
		AllowedTaskStatus: cr.AllowedTaskStatus,
	}
	if err := svc.repo.CreateBoard(ctx, model); err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.CreateBoardResponse{
		Board: model,
	}), nil
}

func (svc *Service) ListBoards(ctx context.Context, req *connect.Request[tasksv1.ListBoardsRequest]) (*connect.Response[tasksv1.ListBoardsResponse], error) {
	list, err := svc.repo.ListBoards(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]*tasksv1.Board, 0, len(list))
	for _, b := range list {
		if err := svc.IsAllowed(ctx, b, "read"); err == nil {
			result = append(result, b)
		}
	}

	return connect.NewResponse(&tasksv1.ListBoardsResponse{
		Boards: result,
	}), nil
}

func (svc *Service) DeleteBoard(ctx context.Context, req *connect.Request[tasksv1.DeleteBoardRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := svc.ensureBoardOwner(ctx, req.Msg.Id); err != nil {
		return nil, err
	}

	// Delete all tasks associated with the board to delete
	if err := svc.repo.DeleteTasksMatchingQuery(ctx, []*tasksv1.TaskQuery{
		{
			BoardId: []string{req.Msg.Id},
		},
	}); err != nil {
		return nil, err
	}

	// Finally, delete the board itself.
	if err := svc.repo.DeleteBoard(ctx, req.Msg.Id); err != nil {
		return nil, err
	}

	return connect.NewResponse(new(emptypb.Empty)), nil
}

func (svc *Service) GetBoard(ctx context.Context, req *connect.Request[tasksv1.GetBoardRequest]) (*connect.Response[tasksv1.GetBoardResponse], error) {
	res, err := svc.repo.GetBoard(ctx, req.Msg.Id)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	if err := svc.IsAllowed(ctx, res, "read"); err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.GetBoardResponse{
		Board: res,
	}), nil
}

func (svc *Service) SaveNotification(ctx context.Context, req *connect.Request[tasksv1.SaveNotificationRequest]) (*connect.Response[tasksv1.SaveNotificationResponse], error) {
	if err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	res, err := svc.repo.SaveNotification(ctx, req.Msg.BoardId, req.Msg.Notification)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	return connect.NewResponse(&tasksv1.SaveNotificationResponse{
		Board: res,
	}), nil
}

func (svc *Service) DeleteNotification(ctx context.Context, req *connect.Request[tasksv1.DeleteNotificationRequest]) (*connect.Response[tasksv1.DeleteNotificationResponse], error) {
	if err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	res, err := svc.repo.DeleteNotification(ctx, req.Msg.BoardId, req.Msg.NotificationName)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	return connect.NewResponse(&tasksv1.DeleteNotificationResponse{
		Board: res,
	}), nil
}

func (svc *Service) ensureBoardOwner(ctx context.Context, boardID string) error {
	remoteUser := auth.From(ctx)

	if remoteUser == nil {
		if os.Getenv("DEBUG") != "" {
			return nil
		}

		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authentication required"))
	}

	board, err := svc.repo.GetBoard(ctx, boardID)
	if err != nil {
		return err
	}

	if board.OwnerId != remoteUser.ID {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("you are not allowed to perform this operation"))
	}

	return nil
}
