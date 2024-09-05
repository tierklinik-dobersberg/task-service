package tasks

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/bufbuild/connect-go"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/task-service/internal/permission"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	tasksv1connect.UnimplementedTaskServiceHandler

	repo     repo.Backend
	resolver *permission.Resolver
}

func New(ctx context.Context, repo repo.Backend, resolver *permission.Resolver) (*Service, error) {
	return &Service{repo: repo, resolver: resolver}, nil
}

func (svc *Service) CreateTask(ctx context.Context, req *connect.Request[tasksv1.CreateTaskRequest]) (*connect.Response[tasksv1.CreateTaskResponse], error) {
	if err := svc.ensureBoardPermissions(ctx, req.Msg.BoardId, "write"); err != nil {
		return nil, err
	}

	var id string
	if user := auth.From(ctx); user != nil {
		id = user.ID
	}

	r := req.Msg
	model := &tasksv1.Task{
		BoardId:     r.BoardId,
		Title:       r.Title,
		Description: r.Description,
		CreatorId:   id,
		Tags:        r.Tags,
		DueTime:     r.DueTime,
		CreateTime:  timestamppb.Now(),
		UpdateTime:  timestamppb.Now(),
		Status:      r.Status,
		Attachments: r.Attachments,
	}

	switch v := r.Location.(type) {
	case *tasksv1.CreateTaskRequest_Address:
		model.Location = &tasksv1.Task_Address{
			Address: v.Address,
		}
	case *tasksv1.CreateTaskRequest_GeoLocation:
		model.Location = &tasksv1.Task_GeoLocation{
			GeoLocation: v.GeoLocation,
		}
	}

	if len(r.Attachments) > 0 {
		return nil, fmt.Errorf("attachments are not yet supported")
	}

	if r.AssigneeId != "" {
		model.AssigneeId = r.AssigneeId
		model.AssignedBy = id
		model.AssignTime = timestamppb.Now()
	}

	if err := svc.repo.CreateTask(ctx, model); err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	return connect.NewResponse(&tasksv1.CreateTaskResponse{
		Task: model,
	}), nil
}

func (svc *Service) DeleteTask(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskRequest]) (*connect.Response[emptypb.Empty], error) {
	if _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
		return nil, err
	}

	if err := svc.repo.DeleteTask(ctx, req.Msg.TaskId); err != nil {
		return nil, err
	}

	return connect.NewResponse(new(emptypb.Empty)), nil
}

func (svc *Service) CompleteTask(ctx context.Context, req *connect.Request[tasksv1.CompleteTaskRequest]) (*connect.Response[tasksv1.CompleteTaskResponse], error) {
	if _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
		return nil, err
	}

	t, err := svc.repo.CompleteTask(ctx, req.Msg.TaskId)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.CompleteTaskResponse{
		Task: t,
	}), nil
}

func (svc *Service) AssignTask(ctx context.Context, req *connect.Request[tasksv1.AssignTaskRequest]) (*connect.Response[tasksv1.AssignTaskResponse], error) {
	if _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
		return nil, err
	}

	var id string
	if r := auth.From(ctx); r != nil {
		id = r.ID
	}

	task, err := svc.repo.AssignTask(ctx, req.Msg.TaskId, req.Msg.AssigneeId, id)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.AssignTaskResponse{
		Task: task,
	}), nil
}

func (svc *Service) UpdateTask(ctx context.Context, req *connect.Request[tasksv1.UpdateTaskRequest]) (*connect.Response[tasksv1.UpdateTaskResponse], error) {
	if _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
		return nil, err
	}

	var id string
	if r := auth.From(ctx); r != nil {
		id = r.ID
	}

	t, err := svc.repo.UpdateTask(ctx, id, req.Msg)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.UpdateTaskResponse{
		Task: t,
	}), nil
}

func (svc *Service) GetTask(ctx context.Context, req *connect.Request[tasksv1.GetTaskRequest]) (*connect.Response[tasksv1.GetTaskResponse], error) {
	task, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "read")
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.GetTaskResponse{
		Task: task,
	}), nil
}

func (svc *Service) ListTasks(ctx context.Context, req *connect.Request[tasksv1.ListTasksRequest]) (*connect.Response[tasksv1.ListTasksResponse], error) {
	res, _, err := svc.repo.ListTasks(ctx, req.Msg.Queries, req.Msg.Pagination)
	if err != nil {
		return nil, err
	}

	boards := make(map[string]error)

	results := make([]*tasksv1.Task, 0, len(res))
	for _, r := range res {
		err, ok := boards[r.BoardId]
		if !ok {
			err = svc.ensureBoardPermissions(ctx, r.BoardId, "read")
			boards[r.BoardId] = err
		}

		if err == nil {
			results = append(results, r)
		}
	}

	return connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks: results,
	}), nil
}

func (svc *Service) ensureBoardPermissions(ctx context.Context, boardID string, op string) error {
	board, err := svc.repo.GetBoard(ctx, boardID)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return connect.NewError(connect.CodeNotFound, err)
		}

		return err
	}

	// In debug mode or without a IdmURL we don't perform any
	// permisssion checks at all
	if svc.resolver == nil {
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

	if !svc.resolver.IsAllowed(ctx, remoteUser.ID, permissions) {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("you are not allowed to perform this operation"))
	}

	return nil
}

func (svc *Service) ensureTaskPermissions(ctx context.Context, taskID string, op string) (*tasksv1.Task, error) {
	task, err := svc.repo.GetTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, repo.ErrTaskNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	return task, svc.ensureBoardPermissions(ctx, task.BoardId, op)
}
