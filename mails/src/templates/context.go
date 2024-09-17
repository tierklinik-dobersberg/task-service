package templates

import (
	"bytes"
	"encoding/json"
	"time"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/colorutil"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer/html"
)

type Task struct {
	ID          string
	Status      *TaskStatus
	Tags        []TaskTag
	Title       string
	Description string
	DueTime     *time.Time
	Assignee    string
	Creator     string
}

type Board struct {
	DisplayName string
	ID          string
}

type TaskStatus struct {
	*tasksv1.TaskStatus
	Foreground string
}

type TaskTag struct {
	*tasksv1.TaskTag
	Foreground string
}

type TemplateContext struct {
	Comment *tasksv1.TaskComment
	Task    Task
	Board   Board
}

func markdownToHTML(comment string) (string, error) {
	md := goldmark.New(
		goldmark.WithExtensions(extension.GFM),
		goldmark.WithParserOptions(
			parser.WithAutoHeadingID(),
		),
		goldmark.WithRendererOptions(
			html.WithHardWraps(),
			html.WithXHTML(),
		),
	)

	var buf bytes.Buffer
	if err := md.Convert([]byte(comment), &buf); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func NewTemplateContext(board *tasksv1.Board, task *tasksv1.Task, comment *tasksv1.TaskComment) (map[string]any, error) {
	var descriptionHTML string

	if task.Description != "" {
		var err error
		descriptionHTML, err = markdownToHTML(task.Description)
		if err != nil {
			return nil, err
		}
	}

	ctx := &TemplateContext{
		Task: Task{
			ID:          task.Id,
			Title:       task.Title,
			Description: descriptionHTML,
			Assignee:    task.AssigneeId,
			Creator:     task.CreatorId,
		},
		Board: Board{
			DisplayName: board.DisplayName,
			ID:          board.Id,
		},
	}

	if comment != nil {
		commentHTML, err := markdownToHTML(comment.Comment)
		if err != nil {
			return nil, err
		}
		ctx.Comment = &tasksv1.TaskComment{
			Comment: commentHTML,
		}
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
