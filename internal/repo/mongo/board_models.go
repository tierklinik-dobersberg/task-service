package mongo

import (
	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	BoardPermission struct {
		AllowRoles []string `bson:"allowRoles,omitempty"`
		AllowUsers []string `bson:"allowUsers,omitempty"`
		DenyRoles  []string `bson:"denyRoles,omitempty"`
		DenyUsers  []string `bson:"denyUsers,omitempty"`
	}

	BoardNotification struct {
		Name             string                   `bson:"name"`
		SubjectTemplate  string                   `bson:"subjectTemplate"`
		MessageTemplate  string                   `bson:"messageTemplate"`
		UserIds          []string                 `bson:"userIds,omitempty"`
		RoleIds          []string                 `bson:"roleIds,omitempty"`
		DayTime          []*commonv1.DayTime      `bson:"sendTimes"`
		EventTypes       []tasksv1.EventType      `bson:"eventTypes"`
		NotificationType tasksv1.NotificationType `bson:"notificationType"`
	}

	TaskStatus struct {
		Status      string `bson:"status"`
		Description string `bson:"description,omitempty"`
		Color       string `bson:"color"`
	}

	TaskTag struct {
		Tag         string `bson:"tag"`
		Description string `bson:"description,omitempty"`
		Color       string `bson:"color"`
	}

	Board struct {
		ID               primitive.ObjectID  `bson:"_id,omitempty"`
		DisplayName      string              `bson:"displayName,omitempty"`
		Description      string              `bson:"description,omitempty"`
		WritePermissions *BoardPermission    `bson:"writePermissions,omitempty"`
		ReadPermissions  *BoardPermission    `bson:"readPermissions,omitempty"`
		Notifications    []BoardNotification `bson:"notifications"`
		OwnerID          string              `bson:"ownerId"`
		TaskStatuses     []TaskStatus        `bson:"statuses,omitempty"`
		TaskTags         []TaskTag           `bson:"tags,omitempty"`
	}
)

func (tag *TaskTag) ToProto() *tasksv1.TaskTag {
	return &tasksv1.TaskTag{
		Tag:         tag.Tag,
		Description: tag.Description,
		Color:       tag.Color,
	}
}

func tagFromProto(pb *tasksv1.TaskTag) *TaskTag {
	return &TaskTag{
		Tag:         pb.Tag,
		Description: pb.Description,
		Color:       pb.Color,
	}
}

func (status *TaskStatus) ToProto() *tasksv1.TaskStatus {
	return &tasksv1.TaskStatus{
		Status:      status.Status,
		Description: status.Description,
		Color:       status.Color,
	}
}

func statusFromProto(pb *tasksv1.TaskStatus) *TaskStatus {
	return &TaskStatus{
		Status:      pb.Status,
		Description: pb.Description,
	}
}

func (perm *BoardPermission) ToProto() *tasksv1.BoardPermission {
	if perm == nil {
		return nil
	}

	return &tasksv1.BoardPermission{
		AllowRoles: perm.AllowRoles,
		AllowUsers: perm.AllowUsers,
		DenyRoles:  perm.DenyRoles,
		DenyUsers:  perm.DenyUsers,
	}
}

func permissionsFromProto(pb *tasksv1.BoardPermission) *BoardPermission {
	if pb == nil {
		return nil
	}

	return &BoardPermission{
		AllowRoles: pb.AllowRoles,
		AllowUsers: pb.AllowUsers,
		DenyRoles:  pb.DenyRoles,
		DenyUsers:  pb.DenyUsers,
	}
}

func (not *BoardNotification) ToProto() *tasksv1.BoardNotification {
	pb := &tasksv1.BoardNotification{
		Name:             not.Name,
		SubjectTemplate:  not.SubjectTemplate,
		MessageTemplate:  not.MessageTemplate,
		RecipientUserIds: not.UserIds,
		RecipientRoleIds: not.RoleIds,
		SendTimes:        not.DayTime,
		NotificationType: not.NotificationType,
		EventTypes:       not.EventTypes,
	}

	return pb
}

func notificationFromProto(pb *tasksv1.BoardNotification) *BoardNotification {
	return &BoardNotification{
		Name:             pb.Name,
		SubjectTemplate:  pb.SubjectTemplate,
		MessageTemplate:  pb.MessageTemplate,
		UserIds:          pb.RecipientUserIds,
		RoleIds:          pb.RecipientRoleIds,
		DayTime:          pb.SendTimes,
		EventTypes:       pb.EventTypes,
		NotificationType: pb.NotificationType,
	}
}

func (b *Board) ToProto() *tasksv1.Board {
	pb := &tasksv1.Board{
		Id:                b.ID.Hex(),
		DisplayName:       b.DisplayName,
		Description:       b.Description,
		Kind:              &tasksv1.Board_List{},
		ReadPermission:    b.ReadPermissions.ToProto(),
		WritePermission:   b.WritePermissions.ToProto(),
		OwnerId:           b.OwnerID,
		Notifications:     make([]*tasksv1.BoardNotification, len(b.Notifications)),
		AllowedTaskStatus: make([]*tasksv1.TaskStatus, len(b.TaskStatuses)),
		AllowedTaskTags:   make([]*tasksv1.TaskTag, len(b.TaskTags)),
	}

	for idx, n := range b.Notifications {
		pb.Notifications[idx] = n.ToProto()
	}

	for idx, s := range b.TaskStatuses {
		pb.AllowedTaskStatus[idx] = s.ToProto()
	}

	for idx, t := range b.TaskTags {
		pb.AllowedTaskTags[idx] = t.ToProto()
	}

	return pb
}

func boardFromProto(pb *tasksv1.Board) (*Board, error) {
	var oid primitive.ObjectID
	if pb.Id != "" {
		var err error
		oid, err = primitive.ObjectIDFromHex(pb.Id)
		if err != nil {
			return nil, err
		}
	}

	b := &Board{
		ID:               oid,
		DisplayName:      pb.DisplayName,
		Description:      pb.Description,
		WritePermissions: permissionsFromProto(pb.WritePermission),
		ReadPermissions:  permissionsFromProto(pb.ReadPermission),
		Notifications:    make([]BoardNotification, len(pb.Notifications)),
		TaskStatuses:     make([]TaskStatus, len(pb.AllowedTaskStatus)),
		TaskTags:         make([]TaskTag, len(pb.AllowedTaskTags)),
		OwnerID:          pb.OwnerId,
	}

	for idx, n := range pb.Notifications {
		b.Notifications[idx] = *notificationFromProto(n)
	}

	for idx, s := range pb.AllowedTaskStatus {
		b.TaskStatuses[idx] = *statusFromProto(s)
	}

	for idx, t := range pb.AllowedTaskTags {
		b.TaskTags[idx] = *tagFromProto(t)
	}

	return b, nil
}
