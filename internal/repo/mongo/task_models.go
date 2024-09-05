package mongo

import (
	"time"

	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	customerv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/customer/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	GeoLocation struct {
		Latitude  float64 `bson:"lat"`
		Longitude float64 `bson:"long"`
	}

	Address struct {
		PostalCode string `bson:"postalCode"`
		City       string `bson:"city"`
		Street     string `bson:"street"`
		Extra      string `bson:"extra,omitempty"`
	}

	Task struct {
		ID           primitive.ObjectID `bson:"_id,omitempty"`
		BoardID      string             `bson:"boardId"`
		Title        string             `bson:"title"`
		Description  string             `bson:"description"`
		Creator      string             `bson:"creator"`
		Assignee     string             `bson:"assignee,omitempty"`
		GeoLocation  *GeoLocation       `bson:"location,omitempty"`
		Address      *Address           `bson:"address,omitempty"`
		Tags         []string           `bson:"tags"`
		Status       string             `bson:"status"`
		AssignedBy   string             `bson:"assignedBy,omitempty"`
		DueTime      time.Time          `bson:"dueTime,omitempty"`
		CreateTime   time.Time          `bson:"createTime"`
		UpdateTime   time.Time          `bson:"updateTime"`
		AssignTime   time.Time          `bson:"assignTime,omitempty"`
		CompleteTime time.Time          `bson:"completeTime,omitempty"`
	}
)

func (geo *GeoLocation) ToProto() *commonv1.GeoLocation {
	if geo == nil {
		return nil
	}
	return &commonv1.GeoLocation{
		Latitude:  geo.Latitude,
		Longitude: geo.Longitude,
	}
}

func geoLocationFromProto(pb *commonv1.GeoLocation) *GeoLocation {
	if pb == nil {
		return nil
	}

	return &GeoLocation{
		Latitude:  pb.Latitude,
		Longitude: pb.Longitude,
	}
}

func (addr *Address) ToProto() *customerv1.Address {
	if addr == nil {
		return nil
	}

	return &customerv1.Address{
		PostalCode: addr.PostalCode,
		City:       addr.City,
		Street:     addr.Street,
		Extra:      addr.Extra,
	}
}

func addrFromProto(pb *customerv1.Address) *Address {
	if pb == nil {
		return nil
	}

	return &Address{
		PostalCode: pb.PostalCode,
		City:       pb.City,
		Street:     pb.Street,
		Extra:      pb.Extra,
	}
}

func (task *Task) ToProto() *tasksv1.Task {
	pb := &tasksv1.Task{
		Id:          task.ID.Hex(),
		BoardId:     task.BoardID,
		Title:       task.Title,
		Description: task.Description,
		CreatorId:   task.Creator,
		AssigneeId:  task.Assignee,
		Tags:        task.Tags,
		Status:      task.Status,
		AssignedBy:  task.AssignedBy,
		CreateTime:  timestamppb.New(task.CreateTime),
		UpdateTime:  timestamppb.New(task.UpdateTime),
	}

	if task.GeoLocation != nil {
		pb.Location = &tasksv1.Task_GeoLocation{
			GeoLocation: task.GeoLocation.ToProto(),
		}
	} else if task.Address != nil {
		pb.Location = &tasksv1.Task_Address{
			Address: task.Address.ToProto(),
		}
	}

	if !task.AssignTime.IsZero() {
		pb.AssignTime = timestamppb.New(task.AssignTime)
	}

	if !task.CompleteTime.IsZero() {
		pb.CompleteTime = timestamppb.New(task.CompleteTime)
	}

	if !task.DueTime.IsZero() {
		pb.DueTime = timestamppb.New(task.DueTime)
	}

	return pb
}

func taskFromProto(pb *tasksv1.Task) (*Task, error) {
	var oid primitive.ObjectID

	if pb.Id != "" {
		var err error
		oid, err = primitive.ObjectIDFromHex(pb.Id)
		if err != nil {
			return nil, err
		}
	}

	t := &Task{
		ID:          oid,
		BoardID:     pb.BoardId,
		Title:       pb.Title,
		Description: pb.Description,
		Creator:     pb.CreatorId,
		Assignee:    pb.AssigneeId,
		Tags:        pb.Tags,
		Status:      pb.Status,
		AssignedBy:  pb.AssignedBy,
		CreateTime:  pb.CreateTime.AsTime(),
		UpdateTime:  pb.UpdateTime.AsTime(),
	}

	if pb.CompleteTime.IsValid() {
		t.CompleteTime = pb.CompleteTime.AsTime()
	}

	if pb.AssignTime.IsValid() {
		t.AssignTime = pb.AssignTime.AsTime()
	}

	if pb.DueTime.IsValid() {
		t.DueTime = pb.DueTime.AsTime()
	}

	switch v := pb.Location.(type) {
	case *tasksv1.Task_Address:
		t.Address = addrFromProto(v.Address)
	case *tasksv1.Task_GeoLocation:
		t.GeoLocation = geoLocationFromProto(v.GeoLocation)
	}

	return t, nil
}
