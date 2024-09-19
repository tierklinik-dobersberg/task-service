package boards

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

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

	cr := req.Msg

	model := &tasksv1.Board{
		DisplayName:           cr.DisplayName,
		Description:           cr.Description,
		ReadPermission:        cr.ReadPermission,
		WritePermission:       cr.WritePermission,
		OwnerId:               remoteUser.ID,
		AllowedTaskStatus:     cr.AllowedTaskStatus,
		AllowedTaskTags:       cr.AllowedTaskTags,
		AllowedTaskPriorities: cr.AllowedTaskPriorities,
		HelpText:              cr.HelpText,
		EligibleRoleIds:       cr.EligibleRoleIds,
		EligibleUserIds:       cr.EligibleUserIds,
		InitialStatus:         cr.InitialStatus,
		Subscriptions:         make(map[string]*tasksv1.Subscription),
		Views:                 cr.Views,
	}

	// validate the initial and done status values exists in the list of allowed
	// task statuses
	if cr.InitialStatus != "" {
		if err := validateStatusExists(cr.InitialStatus, model.AllowedTaskStatus); err != nil {
			return nil, err
		}
	}
	if cr.DoneStatus != "" {
		if err := validateStatusExists(cr.DoneStatus, model.AllowedTaskStatus); err != nil {
			return nil, err
		}
	}

	// validate uniqueness of status, tag and priorities
	if err := repo.EnsureUniqueField(cr.AllowedTaskStatus, func(s *tasksv1.TaskStatus) string {
		return s.Status
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("allowed_task_status: %w", err))
	}

	if err := repo.EnsureUniqueField(cr.AllowedTaskTags, func(s *tasksv1.TaskTag) string {
		return s.Tag
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("allowed_task_tags: %w", err))
	}

	if err := repo.EnsureUniqueField(cr.AllowedTaskPriorities, func(s *tasksv1.TaskPriority) string {
		return s.Name
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("allowed_task_tags: name: %w", err))
	}

	if err := repo.EnsureUniqueField(cr.AllowedTaskPriorities, func(s *tasksv1.TaskPriority) int32 {
		return s.Priority
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("allowed_task_priorities: priority: %w", err))
	}

	if err := repo.EnsureUniqueField(cr.Views, func(s *tasksv1.View) string {
		return s.Name
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("views: name: %w", err))
	}

	// ensure the board owner is automatically subscribed to updates
	model.Subscriptions[remoteUser.ID] = &tasksv1.Subscription{
		UserId:            remoteUser.ID,
		NotificationTypes: []tasksv1.NotificationType{}, // server prefered default
		Unsubscribed:      false,
	}

	// finally, create the board
	if err := svc.repo.CreateBoard(ctx, model); err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardCreated{
			BoardCreated: model,
		},
	})

	return connect.NewResponse(&tasksv1.CreateBoardResponse{
		Board: model,
	}), nil
}

func (svc *Service) UpdateBoard(ctx context.Context, req *connect.Request[tasksv1.UpdateBoardRequest]) (*connect.Response[tasksv1.UpdateBoardResponse], error) {
	board, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId)
	if err != nil {
		return nil, err
	}

	// validate the initial and done status values exists in the list of allowed
	// task statuses
	if req.Msg.InitialStatus != "" {
		if err := validateStatusExists(req.Msg.InitialStatus, board.AllowedTaskStatus); err != nil {
			return nil, err
		}
	}
	if req.Msg.DoneStatus != "" {
		if err := validateStatusExists(req.Msg.DoneStatus, board.AllowedTaskStatus); err != nil {
			return nil, err
		}
	}

	res, err := svc.repo.UpdateBoard(ctx, req.Msg)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: res,
		},
	})

	return connect.NewResponse(&tasksv1.UpdateBoardResponse{
		Board: res,
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
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.Id); err != nil {
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

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardDeleted{
			BoardDeleted: req.Msg.Id,
		},
	})

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

func (svc *Service) AddTaskStatus(ctx context.Context, req *connect.Request[tasksv1.AddTaskStatusRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.AddTaskStatus(ctx, req.Msg.BoardId, req.Msg.Status)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) DeleteTaskStatus(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskStatusRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.DeleteTaskStatus(ctx, req.Msg.BoardId, req.Msg.Status)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) AddTaskTag(ctx context.Context, req *connect.Request[tasksv1.AddTaskTagRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.AddTaskTag(ctx, req.Msg.BoardId, req.Msg.Tag)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) DeleteTaskTag(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskTagRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.DeleteTaskTag(ctx, req.Msg.BoardId, req.Msg.Tag)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) AddView(ctx context.Context, req *connect.Request[tasksv1.AddViewRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.AddView(ctx, req.Msg.BoardId, req.Msg.View)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) DeleteView(ctx context.Context, req *connect.Request[tasksv1.DeleteViewRequest]) (*connect.Response[tasksv1.Board], error) {
	if _, err := svc.ensureBoardOwner(ctx, req.Msg.BoardId); err != nil {
		return nil, err
	}

	b, err := svc.repo.DeleteView(ctx, req.Msg.BoardId, req.Msg.ViewName)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(b), nil
}

func (svc *Service) ManageSubscription(ctx context.Context, req *connect.Request[tasksv1.ManageSubscriptionRequest]) (*connect.Response[emptypb.Empty], error) {
	remoteUser := auth.From(ctx)
	if remoteUser == nil {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authentication required"))
	}

	isAdmin := slices.Contains(remoteUser.RoleIDs, "idm_superuser")

	if !isAdmin && req.Msg.UserId != "" && req.Msg.UserId != remoteUser.ID {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("only administrators are allowed to configure subscriptions for other users"))
	} else {
		req.Msg.UserId = remoteUser.ID
	}

	if err := svc.repo.UpdateBoardSubscription(ctx, req.Msg.Id, &tasksv1.Subscription{
		UserId:            req.Msg.UserId,
		NotificationTypes: req.Msg.Types,
		Unsubscribed:      req.Msg.Unsubscribe,
	}); err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	b, err := svc.repo.GetBoard(ctx, req.Msg.Id)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.BoardEvent{
		Kind: &tasksv1.BoardEvent_BoardUpdated{
			BoardUpdated: b,
		},
	})

	return connect.NewResponse(new(emptypb.Empty)), nil

}

func (svc *Service) ensureBoardOwner(ctx context.Context, boardID string) (*tasksv1.Board, error) {
	remoteUser := auth.From(ctx)

	if remoteUser == nil {
		if os.Getenv("DEBUG") != "" {
			return nil, nil
		}

		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authentication required"))
	}

	board, err := svc.repo.GetBoard(ctx, boardID)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, err
	}

	if board.OwnerId != remoteUser.ID {
		return board, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("you are not allowed to perform this operation"))
	}

	return board, nil
}

func validateStatusExists(status string, list []*tasksv1.TaskStatus) error {
	for _, s := range list {
		if s.Status == status {
			return nil
		}
	}

	return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("status value %q is not defined", status))
}
