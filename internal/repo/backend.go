package repo

import (
	"context"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"google.golang.org/protobuf/proto"
)

func Clone[T proto.Message](a T) T {
	return proto.Clone(a).(T)
}

type BoardBackend interface {
	CreateBoard(context.Context, *tasksv1.Board) error
	ListBoards(context.Context) ([]*tasksv1.Board, error)
	GetBoard(context.Context, string) (*tasksv1.Board, error)
	DeleteBoard(context.Context, string) error

	SaveNotification(context.Context, string, *tasksv1.BoardNotification) (*tasksv1.Board, error)
	DeleteNotification(context.Context, string, string) (*tasksv1.Board, error)
}

type Backend interface {
	BoardBackend
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
