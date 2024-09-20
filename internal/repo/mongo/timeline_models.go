package mongo

import (
	"time"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ValueChange struct {
		FieldName string `bson:"fieldName"`
		OldValue  any    `bson:"oldValue,omitempty"`
		NewValue  any    `bson:"newValue,omitempty"`
	}

	Reaction struct {
		UserID   string `bson:"userId"`
		Reaction string `bson:"reaction"`
	}

	TaskComment struct {
		Comment    string     `bson:"comment"`
		Reactions  []Reaction `bson:"reactions"`
		EditTime   time.Time  `bson:"editTime,omitempty"`
		DeleteTime time.Time  `bson:"deleteTime,omitempty"`
		Offtopic   time.Time  `bson:"offTopicTime,omitempty"`
	}

	Timeline struct {
		ID          primitive.ObjectID `bson:"_id,omitempty"`
		TaskID      primitive.ObjectID `bson:"taskId"`
		BoardID     primitive.ObjectID `bson:"boardId"`
		CreateTime  time.Time          `bson:"createTime"`
		UserID      string             `bson:"userId"`
		ValueChange *ValueChange       `bson:"valueChange,omitempty"`
		Comment     *TaskComment       `bson:"comment,omitempty"`
	}

	timelineValue interface {
		timeline()
	}
)

func (*TaskComment) timeline() {}
func (*ValueChange) timeline() {}

func (vc *ValueChange) ValueFrom(t *Task) any {
	var n any
	switch vc.FieldName {
	case "title":
		n = t.Title
	case "description":
		n = t.Description
	case "assignee_id":
		n = t.Assignee
	case "completeTime":
		n = t.CompleteTime
	case "priority":
		n = t.Priority
	case "status":
		n = t.Status
	case "tags":
		n = t.Tags
	case "due_time":
		if !t.DueTime.IsZero() {
			n = t.DueTime.Format(time.RFC3339)
		}
	case "complete_time":
		if !t.CompleteTime.IsZero() {
			n = t.CompleteTime.Format(time.RFC3339)
		}
	}

	return n
}

func (valueChange *ValueChange) ToProto() (*tasksv1.TaskValueChange, error) {
	var (
		oldValue *structpb.Value
		newValue *structpb.Value
	)

	if valueChange.OldValue != nil {
		var err error
		oldValue, err = structpb.NewValue(mayConvertPrimitives(valueChange.OldValue))
		if err != nil {
			return nil, err
		}
	}

	if valueChange.NewValue != nil {
		var err error
		newValue, err = structpb.NewValue(mayConvertPrimitives((valueChange.NewValue)))
		if err != nil {
			return nil, err
		}
	}

	pb := &tasksv1.TaskValueChange{
		FieldName: valueChange.FieldName,
		OldValue:  oldValue,
		NewValue:  newValue,
	}

	return pb, nil
}

func (reaction *Reaction) ToProto() *tasksv1.TaskReaction {
	return &tasksv1.TaskReaction{
		UserId:   reaction.UserID,
		Reaction: reaction.Reaction,
	}
}

func reactionListToProto(list []Reaction) []*tasksv1.TaskReaction {
	result := make([]*tasksv1.TaskReaction, len(list))

	for idx, r := range list {
		result[idx] = r.ToProto()
	}

	return result
}

func (c *TaskComment) ToProto() *tasksv1.TaskComment {
	pb := &tasksv1.TaskComment{
		Comment:   c.Comment,
		Reactions: reactionListToProto(c.Reactions),
	}

	if !c.EditTime.IsZero() {
		pb.EditTime = timestamppb.New(c.EditTime)
	}

	if !c.DeleteTime.IsZero() {
		pb.DeleteTime = timestamppb.New(c.DeleteTime)
	}

	if !c.Offtopic.IsZero() {
		pb.MarkAsOfftopicTime = timestamppb.New(c.Offtopic)
	}

	return pb
}

func (t *Timeline) ToProto() (*tasksv1.TaskTimelineEntry, error) {
	pb := &tasksv1.TaskTimelineEntry{
		Id:         t.ID.Hex(),
		TaskId:     t.TaskID.Hex(),
		CreateTime: timestamppb.New(t.CreateTime),
		UserId:     t.UserID,
	}

	if t.Comment != nil {
		pb.Kind = &tasksv1.TaskTimelineEntry_Comment{
			Comment: t.Comment.ToProto(),
		}
	} else if t.ValueChange != nil {
		v, err := t.ValueChange.ToProto()
		if err != nil {
			return pb, err
		}

		pb.Kind = &tasksv1.TaskTimelineEntry_ValueChange{
			ValueChange: v,
		}
	}

	return pb, nil
}
