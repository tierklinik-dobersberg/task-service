package repo

import "errors"

var (
	ErrBoardNotFound = errors.New("board not found")
	ErrTaskNotFound  = errors.New("task not found")
	ErrTaskCompleted = errors.New("task already completed")
)
