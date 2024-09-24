package tasks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"github.com/bufbuild/connect-go"
	"github.com/davecgh/go-spew/spew"
	"github.com/mennanov/fmutils"
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"github.com/tierklinik-dobersberg/task-service/internal/services"
	"github.com/tierklinik-dobersberg/task-service/internal/taskql"
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
	remoteUser := auth.From(ctx)

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
		BoardId:       r.BoardId,
		Title:         r.Title,
		Description:   r.Description,
		CreatorId:     id,
		Tags:          r.Tags,
		DueTime:       r.DueTime,
		CreateTime:    timestamppb.Now(),
		UpdateTime:    timestamppb.Now(),
		Status:        r.Status,
		Attachments:   r.Attachments,
		Priority:      r.Priority,
		Subscriptions: map[string]*tasksv1.Subscription{},
	}

	// Automatically subscribe the creator to task updates
	if remoteUser != nil {
		model.Subscriptions[remoteUser.ID] = &tasksv1.Subscription{
			UserId:            remoteUser.ID,
			NotificationTypes: []tasksv1.NotificationType{},
			Unsubscribed:      false,
		}
	}

	// validate tags and status
	if r.Status != "" {
		if err := validateBoardStatus(board, r.Status); err != nil {
			return nil, err
		}
	} else {
		r.Status = board.InitialStatus
	}

	if err := validateBoardTags(board, r.Tags); err != nil {
		return nil, err
	}

	if r.Priority != nil {
		if err := validateBoardPriority(board, r.Priority.Value); err != nil {
			return nil, err
		}
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

		// verify the user is acutally assignable
		if err := svc.canAssignUser(ctx, r.AssigneeId, board); err != nil {
			return nil, err
		}
	}

	if err := svc.repo.CreateTask(ctx, model); err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      model,
		EventType: tasksv1.EventType_EVENT_TYPE_CREATED,
	})

	if user := auth.From(ctx); svc.Config.IdmURL != "" && user != nil {
		go func() {
			name := user.DisplayName
			if name == "" {
				name = user.Username
			}

			subj := fmt.Sprintf("%s: %s hat eine neue Aufgabe %q erstellt", board.DisplayName, name, model.Title)
			svc.SendNotification(board, model, nil, subj, user.ID, "creation-notification.html")
		}()
	}

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
	task, board, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	t, err := svc.repo.CompleteTask(ctx, req.Msg.TaskId)
	if err != nil {
		return nil, err
	}

	if user := auth.From(ctx); svc.Config.IdmURL != "" && user != nil {
		go func() {
			name := user.DisplayName
			if name == "" {
				name = user.Username
			}

			subj := fmt.Sprintf("%s: %s hat die Aufgabe %q fertig gestellt", board.DisplayName, name, task.Title)
			svc.SendNotification(board, task, nil, subj, user.ID, "done-notification.html")
		}()
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      t,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(&tasksv1.CompleteTaskResponse{
		Task: t,
	}), nil
}

func (svc *Service) UpdateTaskComment(ctx context.Context, req *connect.Request[tasksv1.UpdateTaskCommentRequest]) (*connect.Response[emptypb.Empty], error) {
	if user := auth.From(ctx); user == nil {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("authentication required"))
	}

	e, err := svc.repo.UpdateTaskComment(ctx, req.Msg)
	if err != nil {
		if errors.Is(err, repo.ErrTaskNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, err
	}

	task, err := svc.repo.GetTask(ctx, e.TaskId)
	if err != nil {
		if errors.Is(err, repo.ErrTaskNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
		Task:      task,
	})

	return connect.NewResponse(new(emptypb.Empty)), nil
}

func (svc *Service) GetTimeline(ctx context.Context, req *connect.Request[tasksv1.GetTimelineRequest]) (*connect.Response[tasksv1.GetTimelineResponse], error) {
	for _, id := range req.Msg.TaskIds {
		if _, _, err := svc.ensureTaskPermissions(ctx, id, "read"); err != nil {
			return nil, err
		}
	}

	result, err := svc.repo.GetTaskTimeline(ctx, req.Msg.TaskIds)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.GetTimelineResponse{
		Timeline: result,
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
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
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

	switch v := req.Msg.Tags.(type) {
	case *tasksv1.UpdateTaskRequest_AddTags:
		if err := validateBoardTags(board, v.AddTags.Values); err != nil {
			return nil, err
		}

	case *tasksv1.UpdateTaskRequest_ReplaceTags:
		if err := validateBoardTags(board, v.ReplaceTags.Values); err != nil {
			return nil, err
		}

	}

	if req.Msg.AssigneeId != "" {
		if err := svc.canAssignUser(ctx, req.Msg.AssigneeId, board); err != nil {
			return nil, err
		}
	}

	if req.Msg.Status != "" {
		if err := validateBoardStatus(board, req.Msg.Status); err != nil {
			return nil, err
		}
	} else {
		// just set it to the initial status as it won't be updated anyway
		// if "status" is not part of update_mask.paths
		req.Msg.Status = board.InitialStatus
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

func (svc *Service) ParseFilter(ctx context.Context, req *connect.Request[tasksv1.ParseFilterRequest]) (*connect.Response[tasksv1.ParseFilterResponse], error) {
	board, err := svc.repo.GetBoard(ctx, req.Msg.BoardId)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	userCli := idmv1connect.NewUserServiceClient(cli.NewInsecureHttp2Client(), svc.Config.IdmURL)

	l := taskql.New(userCli, board)

	if err := l.Process(req.Msg.Query); err != nil {
		return nil, err
	}

	token, fieldName, values, err := l.ExpectedNextToken(ctx)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&tasksv1.ParseFilterResponse{
		ExpectedToken:   string(token),
		Values:          values,
		LastFieldName:   fieldName,
		NormalizedQuery: l.String(),
	}), nil
}

func (svc *Service) FilterTasks(ctx context.Context, req *connect.Request[tasksv1.FilterTasksRequest]) (*connect.Response[tasksv1.ListTasksResponse], error) {
	board, err := svc.repo.GetBoard(ctx, req.Msg.BoardId)
	if err != nil {
		if errors.Is(err, repo.ErrBoardNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	var query map[taskql.Field]taskql.Query

	if req.Msg.Query != "" {
		userCli := idmv1connect.NewUserServiceClient(cli.NewInsecureHttp2Client(), svc.Config.IdmURL)

		l := taskql.New(userCli, board)

		if err := l.Process(req.Msg.Query); err != nil {
			return nil, err
		}

		var err error
		query, err = l.Query(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Dump query for debugging purposes
	spew.Dump(query)

	res, count, err := svc.repo.FilterTasks(ctx, req.Msg.BoardId, query, "", req.Msg.Pagination)
	if err != nil {
		return nil, err
	}

	boards := make(map[string]error)

	results := make([]*tasksv1.Task, 0, len(res))
	for _, grp := range res {
		for _, r := range grp.Tasks {
			err, ok := boards[r.BoardId]
			if !ok {
				_, err = svc.ensureBoardPermissions(ctx, r.BoardId, "read")
				boards[r.BoardId] = err
			}

			if err == nil {
				results = append(results, r)
			}
		}
	}

	// TODO(ppacher): support for the tkd.common.v1.View field

	return connect.NewResponse(&tasksv1.ListTasksResponse{
		Tasks:      results,
		TotalCount: int64(count),
	}), nil
}

func (svc *Service) QueryView(ctx context.Context, req *connect.Request[tasksv1.QueryViewRequest]) (*connect.Response[tasksv1.QueryViewResponse], error) {
	boardIds := req.Msg.BoardIds

	allBoards, err := svc.repo.ListBoards(ctx)
	if err != nil {
		return nil, err
	}
	boardMap := make(map[string]*tasksv1.Board)

	for _, b := range allBoards {
		boardMap[b.Id] = b
	}

	if len(boardIds) == 0 {
		boardIds = slices.Collect(maps.Keys(boardMap))
	}

	var allGroups []*tasksv1.TaskGroup

	found := false
	for _, id := range boardIds {
		board, ok := boardMap[id]
		if !ok {
			return nil, connect.NewError(connect.CodeNotFound, repo.ErrBoardNotFound)
		}

		// ensure the user is actually allowed to query that board.
		if err := svc.IsAllowed(ctx, board, "read"); err != nil {
			continue
		}

		found = true

		var query map[taskql.Field]taskql.Query
		if req.Msg.View.Filter != "" {
			userCli := idmv1connect.NewUserServiceClient(cli.NewInsecureHttp2Client(), svc.Config.IdmURL)

			l := taskql.New(userCli, board)

			if err := l.Process(req.Msg.View.Filter); err != nil {
				return nil, err
			}

			var err error
			query, err = l.Query(ctx)
			if err != nil {
				return nil, err
			}
		}

		if req.Msg.View.Sort != nil {
			if req.Msg.Pagination == nil {
				req.Msg.Pagination = &commonv1.Pagination{}
			}

			req.Msg.Pagination.SortBy = []*commonv1.Sort{req.Msg.View.Sort}
		}

		res, _, err := svc.repo.FilterTasks(ctx, id, query, req.Msg.View.GroupByField, req.Msg.Pagination)
		if err != nil {
			return nil, err
		}

		allGroups = append(allGroups, res...)
	}

	// the use does not have permission to query at least one board
	if !found {
		return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("you are not allowed to read the task boards"))
	}

	response := &tasksv1.QueryViewResponse{
		Groups:       allGroups,
		GroupByField: req.Msg.View.GroupByField,
		Boards:       allBoards,
	}

	if fmp := req.Msg.GetReadMask().GetPaths(); len(fmp) > 0 {
		fmutils.Filter(response, fmp)
	}

	return connect.NewResponse(response), nil
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

func (svc *Service) CreateTaskComment(ctx context.Context, req *connect.Request[tasksv1.CreateTaskCommentRequest]) (*connect.Response[emptypb.Empty], error) {
	task, board, err := svc.ensureTaskPermissions(ctx, req.Msg.TaskId, "write")
	if err != nil {
		return nil, err
	}

	if err := svc.repo.CreateTaskComment(ctx, req.Msg.TaskId, task.BoardId, req.Msg.Comment); err != nil {
		return nil, err
	}

	if user := auth.From(ctx); svc.Config.IdmURL != "" && user != nil {
		go func() {
			name := user.DisplayName
			if name == "" {
				name = user.Username
			}

			subj := fmt.Sprintf("%s hat einen neuen Kommentar zur Aufgabe %q erstellt", name, task.Title)
			svc.SendNotification(board, task, &tasksv1.TaskComment{
				Comment: req.Msg.Comment,
			}, subj, user.ID, "comment-notification.html")
		}()
	}

	return connect.NewResponse(new(emptypb.Empty)), nil
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

func validateBoardPriority(board *tasksv1.Board, priority int32) error {
	for _, allowed := range board.AllowedTaskPriorities {
		if allowed.Priority == priority {
			return nil
		}
	}

	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("priorty %v not allowed for this board", priority))
}

func validateBoardStatus(board *tasksv1.Board, status string) error {
	for _, allowed := range board.AllowedTaskStatus {
		if allowed.Status == status {
			return nil
		}
	}

	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("status %q not allowed for this board", status))
}

func (svc *Service) canAssignUser(ctx context.Context, userId string, board *tasksv1.Board) error {
	if len(board.EligibleRoleIds) == 0 && len(board.EligibleUserIds) == 0 {
		return nil
	}

	if slices.Contains(board.EligibleUserIds, userId) {
		return nil
	}

	// fetch user roles
	cli := idmv1connect.NewUserServiceClient(cli.NewInsecureHttp2Client(), svc.Common.Config.IdmURL)

	profile, err := cli.GetUser(ctx, connect.NewRequest(&idmv1.GetUserRequest{
		Search: &idmv1.GetUserRequest_Id{
			Id: userId,
		},
	}))

	if err != nil {
		return fmt.Errorf("failed to fetch user profile: %w", err)
	}

	for _, role := range profile.Msg.Profile.Roles {
		if slices.Contains(board.EligibleRoleIds, role.Id) {
			return nil
		}
	}

	return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("the selected user is not eligible for assignment"))
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

	if err := svc.repo.UpdateTaskSubscription(ctx, req.Msg.Id, &tasksv1.Subscription{
		UserId:            req.Msg.UserId,
		NotificationTypes: req.Msg.Types,
		Unsubscribed:      req.Msg.Unsubscribe,
	}); err != nil {
		if errors.Is(err, repo.ErrTaskNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}

		return nil, err
	}

	t, err := svc.repo.GetTask(ctx, req.Msg.Id)
	if err != nil {
		return nil, err
	}

	svc.PublishEvent(&tasksv1.TaskEvent{
		Task:      t,
		EventType: tasksv1.EventType_EVENT_TYPE_UPDATED,
	})

	return connect.NewResponse(new(emptypb.Empty)), nil

}
