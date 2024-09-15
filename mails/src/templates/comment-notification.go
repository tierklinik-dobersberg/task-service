package templates

import (
	"encoding/json"
	"time"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
)

type Task struct {
	Status      *tasksv1.TaskStatus
	Tags        []*tasksv1.TaskTag
	Title       string
	Description string
	DueTime     *time.Time
}

type TaskCommentNotificationContext struct {
	Comment *tasksv1.TaskComment
	Task    Task
}

func NewCommentNotificationContext(board *tasksv1.Board, task *tasksv1.Task, comment *tasksv1.TaskComment) (map[string]any, error) {
	ctx := &TaskCommentNotificationContext{
		Comment: comment,
		Task: Task{
			Title:       task.Title,
			Description: task.Description,
		},
	}

	if task.DueTime.IsValid() {
		t := task.DueTime.AsTime()
		ctx.Task.DueTime = &t
	}

	if task.Status != "" {
		for _, s := range board.AllowedTaskStatus {
			if s.Status == task.Status {
				ctx.Task.Status = s
				break
			}
		}
	}

	for _, tag := range task.Tags {
		for _, t := range board.AllowedTaskTags {
			if t.Tag == tag {
				ctx.Task.Tags = append(ctx.Task.Tags, t)
				break
			}
		}
	}

	blob, err := json.Marshal(ctx)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	if err := json.Unmarshal(blob, &m); err != nil {
		return nil, err
	}

	return m, nil
}
