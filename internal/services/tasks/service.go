package tasks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/bufbuild/connect-go"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"github.com/tierklinik-dobersberg/task-service/internal/services"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	tasksv1connect.UnimplementedTaskServiceHandler

	repo repo.Backend

	*services.Common
}

func New(ctx context.Context, repo repo.Backend, common *services.Common) (*Service, error) {
	return &Service{repo: repo, Common: common}, nil
}

func (svc *Service) CreateTask(ctx context.Context, req *connect.Request[tasksv1.CreateTaskRequest]) (*connect.Response[tasksv1.CreateTaskResponse], error) {
	board, err := svc.ensureBoardPermissions(ctx, req.Msg.BoardId, "write")
	if err != nil {
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

	// validate tags and status
	if r.Status != "" {
		if err := validateBoardStatus(board, r.Status); err != nil {
			return nil, err
		}
	}

	if err := validateBoardTags(board, r.Tags); err != nil {
		return nil, err
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

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      model,
		EventType: tasksv1.EventType_EVENT_TYPE_CREATED,
	})

	return connect.NewResponse(&tasksv1.CreateTaskResponse{
		Task: model,
	}), nil
}

func (svc *Service) DeleteTask(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskRequest]) (*connect.Response[emptypb.Empty], error) {
	task, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	if err := svc.repo.DeleteTask(ctx, req.Msg.TaskId); err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      task,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(new(emptypb.Empty)), nil
}

func (svc *Service) CompleteTask(ctx context.Context, req *connect.Request[tasksv1.CompleteTaskRequest]) (*connect.Response[tasksv1.CompleteTaskResponse], error) {
	if _, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
		return nil, err
	}

	t, err := svc.repo.CompleteTask(ctx, req.Msg.TaskId)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      t,
		EventType: tasksv1.EventType_EVENT_TYPE_COMPLETED,
	})

	return connect.NewResponse(&tasksv1.CompleteTaskResponse{
		Task: t,
	}), nil
}

func (svc *Service) AssignTask(ctx context.Context, req *connect.Request[tasksv1.AssignTaskRequest]) (*connect.Response[tasksv1.AssignTaskResponse], error) {
	if _, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write"); err != nil {
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

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      task,
		EventType: tasksv1.EventType_EVENT_TYPE_ASSIGNEE_CHANGED,
	})

	return connect.NewResponse(&tasksv1.AssignTaskResponse{
		Task: task,
	}), nil
}

func (svc *Service) UpdateTask(ctx context.Context, req *connect.Request[tasksv1.UpdateTaskRequest]) (*connect.Response[tasksv1.UpdateTaskResponse], error) {
	_, board, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	var id string
	if r := auth.From(ctx); r != nil {
		id = r.ID
	}

	if err := validateBoardTags(board, req.Msg.AddTags); err != nil {
		return nil, err
	}

	if req.Msg.Status != "" {
		if err := validateBoardStatus(board, req.Msg.Status); err != nil {
			return nil, err
		}
	}

	t, err := svc.repo.UpdateTask(ctx, id, req.Msg)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      t,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(&tasksv1.UpdateTaskResponse{
		Task: t,
	}), nil
}

func (svc *Service) GetTask(ctx context.Context, req *connect.Request[tasksv1.GetTaskRequest]) (*connect.Response[tasksv1.GetTaskResponse], error) {
	task, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "read")
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.GetTaskResponse{
		Task: task,
	}), nil
}

func (svc *Service) ListTasks(ctx context.Context, req *connect.Request[tasksv1.ListTasksRequest]) (*connect.Response[tasksv1.ListTasksResponse], error) {
	res, count, err := svc.repo.ListTasks(ctx, req.Msg.Queries, req.Msg.Pagination)
	if err != nil {
		return nil, err
	}

	boards := make(map[string]error)

	results := make([]*tasksv1.Task, 0, len(res))
	for _, r := range res {
		err, ok := boards[r.BoardId]
		if !ok {
			_, err = svc.ensureBoardPermissions(ctx, r.BoardId, "read")
			boards[r.BoardId] = err
		}

		if err == nil {
			results = append(results, r)
		}
	}

	return connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks:      results,
		TotalCount: int64(count),
	}), nil
}

func (svc *Service) AddTaskAttachment(ctx context.Context, req *connect.Request[tasksv1.AddTaskAttachmentRequest]) (*connect.Response[tasksv1.AddTaskAttachmentResponse], error) {
	task, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	file := filepath.Join(
		svc.Config.UploadDirectory,
		fmt.Sprintf("%s-%s", task.Id, req.Msg.Name),
	)

	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := io.Copy(f, bytes.NewReader(req.Msg.Content)); err != nil {
		return nil, err
	}

	task, err = svc.repo.AddTaskAttachment(ctx, task.Id, file, &tasksv1.Attachment{
		Name:        req.Msg.Name,
		ContentType: req.Msg.ContentType,
	})
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      task,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(&tasksv1.AddTaskAttachmentResponse{
		Task: task,
	}), nil
}

func (svc *Service) DeleteTaskAttachment(ctx context.Context, req *connect.Request[tasksv1.DeleteTaskAttachmentRequest]) (*connect.Response[tasksv1.DeleteTaskAttachmentResponse], error) {
	task, _, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	task, err = svc.repo.DeleteTaskAttachment(ctx, task.Id, req.Msg.Name)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      task,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(&tasksv1.DeleteTaskAttachmentResponse{
		Task: task,
	}), nil
}

func (svc *Service) ensureBoardPermissions(ctx context.Context, boardID string, op string) (*tasksv1.Board, error) {
	board, err := svc.repo.GetBoard(ctx, boardID)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	return board, svc.IsAllowed(ctx, board, op)
}

func (svc *Service) ensureTaskPermissions(ctx context.Context, taskID string, op string) (*tasksv1.Task, *tasksv1.Board, error) {
	task, err := svc.repo.GetTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, repo.ErrTaskNotFound) {
			return nil, nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, nil, err
	}

	board, err := svc.ensureBoardPermissions(ctx, task.BoardId, op)
	return task, board, err
}

func validateBoardTags(board *tasksv1.Board, tags []string) error {
	lm := make(map[string]struct{})
	for _, t := range board.AllowedTaskTags {
		lm[t.Tag] = struct{}{}
	}

	for _, t := range tags {
		if _, ok := lm[t]; !ok {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("tag %q not allowed for this board", t))
		}
	}

	return nil
}

func validateBoardStatus(board *tasksv1.Board, status string) error {
	for _, allowed := range board.AllowedTaskStatus {
		if allowed.Status == status {
			return nil
		}
	}

	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("status %q not allowed for this board", status))
}
