package templates

import (
	"encoding/json"
	"time"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/colorutil"
)

type Task struct {
	Status      *TaskStatus
	Tags        []TaskTag
	Title       string
	Description string
	DueTime     *time.Time
}

type TaskStatus struct {
	*tasksv1.TaskStatus
	Foreground string
}

type TaskTag struct {
	*tasksv1.TaskTag
	Foreground string
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
				ctx.Task.Status = &TaskStatus{
					TaskStatus: s,
				}

				if s.Color != "" {
					r, g, b := colorutil.HexToRBGColor(s.Color)
					if colorutil.UseLightTextOnBackground(r, g, b) {
						ctx.Task.Status.Foreground = "white"
					} else {
						ctx.Task.Status.Foreground = "black"
					}
				}

				break
			}
		}
	}

	for _, tag := range task.Tags {
		for _, t := range board.AllowedTaskTags {
			if t.Tag == tag {
				ct := TaskTag{
					TaskTag: t,
				}

				if t.Color != "" {
					r, g, b := colorutil.HexToRBGColor(t.Color)

					if colorutil.UseLightTextOnBackground(r, g, b) {
						ct.Foreground = "white"
					} else {
						ct.Foreground = "black"
					}
				}
				ctx.Task.Tags = append(ctx.Task.Tags, ct)
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
