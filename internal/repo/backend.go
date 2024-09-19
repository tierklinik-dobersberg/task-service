package repo

import (
	"context"
	"fmt"

	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/taskql"
	"google.golang.org/protobuf/proto"
)

func Clone[T proto.Message](a T) T {
	return proto.Clone(a).(T)
}

type BoardBackend interface {
	// CreateBoard creates a new board instance and returns any errors encounterd.
	// If a new board has been created successfully, the ID of the passed board model
	// is updated to the newly generated ID.
	CreateBoard(context.Context, *tasksv1.Board) error

	// UpdateBoard updates board settings.
	UpdateBoard(context.Context, *tasksv1.UpdateBoardRequest) (*tasksv1.Board, error)

	// ListBoards returns all boards.
	ListBoards(context.Context) ([]*tasksv1.Board, error)

	// GetBoard returns a single board identified by id.
	GetBoard(context.Context, string) (*tasksv1.Board, error)

	// DeleteBoard deletes a board an all associated tasks
	// by id.
	DeleteBoard(context.Context, string) error

	// AddTaskStatus adds a new task status value to a board
	AddTaskStatus(context.Context, string, *tasksv1.TaskStatus) (*tasksv1.Board, error)

	// DeleteTaskStatus deletes a task status value from a board
	// and resets all tasks with that status to board.InitialStatus
	DeleteTaskStatus(context.Context, string, string) (*tasksv1.Board, error)

	// AddTaskTag adds a new task tag to the board.
	AddTaskTag(context.Context, string, *tasksv1.TaskTag) (*tasksv1.Board, error)

	// DeleteTaskTag deletes a task tag from the board and removes that
	// tag from any task that has it assigned.
	DeleteTaskTag(context.Context, string, string) (*tasksv1.Board, error)

	// AddView adds a new task view to the board.
	AddView(context.Context, string, *tasksv1.View) (*tasksv1.Board, error)

	// DeleteView deletes a task view from the board.
	DeleteView(context.Context, string, string) (*tasksv1.Board, error)

	UpdateBoardSubscription(ctx context.Context, boardId string, subscription *tasksv1.Subscription) error
}

type TaskBackend interface {
	// CreateTask creates a new task at the specified board id.
	// If the task has been created succesfully, the newly created task ID
	// is assigned to the passed pointer model.
	CreateTask(context.Context, *tasksv1.Task) error

	// DeleteTask deletes a task from a board, identified by the
	// unique task id.
	DeleteTask(context.Context, string) error

	// AssignTask assigns a task identified by it's ID to assigneeID.
	// The AssignedBy field is set to assignedByUserId.
	AssignTask(ctx context.Context, taskID, assigneeID, assignedByUserId string) (*tasksv1.Task, error)

	// CompleteTask completes a task identified by id.
	CompleteTask(context.Context, string) (*tasksv1.Task, error)

	// GetTask returns a task by it's ID.
	GetTask(ctx context.Context, taskID string) (*tasksv1.Task, error)

	// UpdateTask updates a task by the update-task-request.
	// The request is passed here as is as different backend implementations
	// might be able to perform multiple task field updates at once.
	UpdateTask(ctx context.Context, authenticatedUserId string, update *tasksv1.UpdateTaskRequest) (*tasksv1.Task, error)

	// ListTasks searches for task entries that match one of the provided
	// task queriers. The queries are passes as the are since different backend implementations
	// might be able to apply optimizations when search for multiple tasks.
	ListTasks(context.Context, []*tasksv1.TaskQuery, *commonv1.Pagination) ([]*tasksv1.Task, int, error)

	// FilterTasks is like ListTasks but filters based on taskql queries.
	FilterTasks(context.Context, string, map[taskql.Field]taskql.Query, *commonv1.Pagination) ([]*tasksv1.Task, int, error)

	// AddTaskAttachment adds a new attachment to a task
	AddTaskAttachment(context.Context, string, string, *tasksv1.Attachment) (*tasksv1.Task, error)

	// DeleteTaskAttachment deletes an attachment from a task.
	DeleteTaskAttachment(context.Context, string, string) (*tasksv1.Task, error)

	// DeleteTaskMatchingQuery deletes all tasks that match one of queries.
	// DANGEROURS: use with care!
	DeleteTasksMatchingQuery(ctx context.Context, queries []*tasksv1.TaskQuery) error

	DeleteTagsFromTasks(ctx context.Context, boardId, tag string) error

	DeleteStatusFromTasks(ctx context.Context, boardId, status, replacement string) error

	UpdateTaskSubscription(ctx context.Context, boardId string, subscription *tasksv1.Subscription) error

	GetTaskTimeline(ctx context.Context, ids []string) ([]*tasksv1.TaskTimelineEntry, error)

	CreateTaskComment(ctx context.Context, taskId, boardId string, comment string) error

	UpdateTaskComment(ctx context.Context, update *tasksv1.UpdateTaskCommentRequest) (*tasksv1.TaskTimelineEntry, error)
}

type Backend interface {
	BoardBackend
	TaskBackend
}

type Repo interface {
	Backend
}

type repo struct {
	Backend
}

func New(backend Backend) Repo {
	return &repo{
		Backend: backend,
	}
}

func EnsureUniqueField[T any, E comparable, F func(T) E](list []T, fn F) error {
	m := make(map[E]struct{}, len(list))

	for _, v := range list {
		key := fn(v)
		if _, ok := m[key]; ok {
			return fmt.Errorf("duplicate value: %v", key)
		}

		m[key] = struct{}{}
	}

	return nil
}
