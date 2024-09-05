package repo

import (
	"context"

	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
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

	// ListBoards returns all boards.
	ListBoards(context.Context) ([]*tasksv1.Board, error)

	// GetBoard returns a single board identified by id.
	GetBoard(context.Context, string) (*tasksv1.Board, error)

	// DeleteBoard deletes a board an all associated tasks
	// by id.
	DeleteBoard(context.Context, string) error

	// SaveNotification updates or creates a new board notification.
	SaveNotification(context.Context, string, *tasksv1.BoardNotification) (*tasksv1.Board, error)

	// DeleteNotification deletes a board notification identified by the board ID and
	// the notification name.
	DeleteNotification(context.Context, string, string) (*tasksv1.Board, error)
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
