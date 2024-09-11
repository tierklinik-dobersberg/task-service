package mongo

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	BoardPermission struct {
		AllowRoles []string `bson:"allowRoles,omitempty"`
		AllowUsers []string `bson:"allowUsers,omitempty"`
		DenyRoles  []string `bson:"denyRoles,omitempty"`
		DenyUsers  []string `bson:"denyUsers,omitempty"`
	}

	TaskStatus struct {
		Status      string `bson:"status"`
		Description string `bson:"description,omitempty"`
		Color       string `bson:"color"`
	}

	TaskPriority struct {
		Name        string `bson:"name"`
		Description string `bson:"description,omitempty"`
		Color       string `bson:"color"`
		Priority    int32  `bson:"priority"`
	}

	TaskTag struct {
		Tag         string `bson:"tag"`
		Description string `bson:"description,omitempty"`
		Color       string `bson:"color"`
	}

	Subscription struct {
		UserId       string                     `bson:"userId"`
		Types        []tasksv1.NotificationType `bson:"types"`
		Unsubscribed bool                       `bson:"unsubscribed"`
	}

	Board struct {
		ID               primitive.ObjectID      `bson:"_id,omitempty"`
		DisplayName      string                  `bson:"displayName,omitempty"`
		Description      string                  `bson:"description,omitempty"`
		TaskStatuses     []TaskStatus            `bson:"statuses"`
		TaskTags         []TaskTag               `bson:"tags"`
		TaskPriorities   []TaskPriority          `bson:"priorities"`
		HelpText         string                  `bson:"helpText,omitempty"`
		WritePermissions *BoardPermission        `bson:"writePermissions,omitempty"`
		ReadPermissions  *BoardPermission        `bson:"readPermissions,omitempty"`
		OwnerID          string                  `bson:"ownerId"`
		EligibleUserIds  []string                `bson:"eligibleUserIds,omitempty"`
		EligibleRoleIds  []string                `bson:"eligibleRoleIds,omitempty"`
		Subscriptions    map[string]Subscription `bson:"subscriptions"`
		InitialStatus    string                  `bson:"initialStatus"`
	}
)

func boardTagFromProtoFieldName(name string) string {
	switch name {
	case "id":
		return "_id"
	case "display_name":
		return "displayName"
	case "description":
		return "description"
	case "allowed_task_status":
		return "statuses"
	case "allowed_task_tags":
		return "tags"
	case "allowed_task_priorities":
		return "priorities"
	case "help_text":
		return "helpText"
	case "write_permission":
		return "writePermissions"
	case "read_permission":
		return "readPermissions"
	case "owner_id":
		return "ownerId"
	case "initial_status":
		return "initialStatus"
	case "eligible_user_ids":
		return "eligibleUserIds"
	case "eligible_role_ids":
		return "eligibleRoleIds"
	case "subscriptions":
		return "subscriptions"
	}

	return name // fallback
}

func (s *Subscription) ToProto() *tasksv1.Subscription {
	return &tasksv1.Subscription{
		UserId:            s.UserId,
		NotificationTypes: s.Types,
		Unsubscribed:      s.Unsubscribed,
	}
}

func subscriptionFromProto(s *tasksv1.Subscription) *Subscription {
	return &Subscription{
		UserId:       s.UserId,
		Types:        s.NotificationTypes,
		Unsubscribed: s.Unsubscribed,
	}
}

func subscriptionMapFromProto(s map[string]*tasksv1.Subscription) map[string]Subscription {
	r := make(map[string]Subscription, len(s))
	for key, value := range s {
		r[key] = *subscriptionFromProto(value)
	}

	return r
}

func subscriptionMapToProto(s map[string]Subscription) map[string]*tasksv1.Subscription {
	r := make(map[string]*tasksv1.Subscription, len(s))

	for key, value := range s {
		r[key] = value.ToProto()
	}

	return r
}

func (p *TaskPriority) ToProto() *tasksv1.TaskPriority {
	return &tasksv1.TaskPriority{
		Name:        p.Name,
		Descritpion: p.Description,
		Color:       p.Color,
		Priority:    p.Priority,
	}
}

func priorityFromProto(p *tasksv1.TaskPriority) *TaskPriority {
	return &TaskPriority{
		Name:        p.Name,
		Description: p.Descritpion,
		Color:       p.Color,
		Priority:    p.Priority,
	}
}

func priorityListFromProto(pb []*tasksv1.TaskPriority) []TaskPriority {
	result := make([]TaskPriority, len(pb))
	for idx, p := range pb {
		result[idx] = *priorityFromProto(p)
	}

	return result
}

func priorityListToProto(list []TaskPriority) []*tasksv1.TaskPriority {
	result := make([]*tasksv1.TaskPriority, len(list))
	for idx, p := range list {
		result[idx] = p.ToProto()
	}

	return result
}

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
		Color:       pb.Color,
	}
}

func statusListFromProto(pb []*tasksv1.TaskStatus) []TaskStatus {
	result := make([]TaskStatus, len(pb))
	for idx, p := range pb {
		result[idx] = *statusFromProto(p)
	}
	return result
}

func tagListFromProto(pb []*tasksv1.TaskTag) []TaskTag {
	result := make([]TaskTag, len(pb))
	for idx, p := range pb {
		result[idx] = *tagFromProto(p)
	}
	return result
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

func (b *Board) ToProto() *tasksv1.Board {
	pb := &tasksv1.Board{
		Id:                    b.ID.Hex(),
		DisplayName:           b.DisplayName,
		Description:           b.Description,
		Kind:                  &tasksv1.Board_List{},
		ReadPermission:        b.ReadPermissions.ToProto(),
		WritePermission:       b.WritePermissions.ToProto(),
		OwnerId:               b.OwnerID,
		AllowedTaskStatus:     make([]*tasksv1.TaskStatus, len(b.TaskStatuses)),
		AllowedTaskTags:       make([]*tasksv1.TaskTag, len(b.TaskTags)),
		AllowedTaskPriorities: priorityListToProto(b.TaskPriorities),
		EligibleRoleIds:       b.EligibleRoleIds,
		Subscriptions:         subscriptionMapToProto(b.Subscriptions),
		EligibleUserIds:       b.EligibleUserIds,
		HelpText:              b.HelpText,
		InitialStatus:         b.InitialStatus,
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
		TaskStatuses:     statusListFromProto(pb.AllowedTaskStatus),
		TaskTags:         tagListFromProto(pb.AllowedTaskTags),
		TaskPriorities:   priorityListFromProto(pb.AllowedTaskPriorities),
		OwnerID:          pb.OwnerId,
		EligibleUserIds:  pb.EligibleUserIds,
		EligibleRoleIds:  pb.EligibleRoleIds,
		Subscriptions:    subscriptionMapFromProto(pb.Subscriptions),
		InitialStatus:    pb.InitialStatus,
		HelpText:         pb.HelpText,
		// TODO(ppacher): kind
	}

	return b, nil
}

func (b *Board) Validate() error {
	errs := new(multierror.Error)

	if err := repo.EnsureUniqueField(b.TaskStatuses, func(s TaskStatus) string {
		return s.Status
	}); err != nil {
		errs.Errors = append(errs.Errors, fmt.Errorf("allowed_task_status: %w", err))
	}

	if err := repo.EnsureUniqueField(b.TaskTags, func(s TaskTag) string {
		return s.Tag
	}); err != nil {
		errs.Errors = append(errs.Errors, fmt.Errorf("allowed_task_tags: %w", err))
	}

	if err := repo.EnsureUniqueField(b.TaskPriorities, func(s TaskPriority) string {
		return s.Name
	}); err != nil {
		errs.Errors = append(errs.Errors, fmt.Errorf("allowed_task_priorities: name: %w", err))
	}

	if err := repo.EnsureUniqueField(b.TaskPriorities, func(s TaskPriority) int32 {
		return s.Priority
	}); err != nil {
		errs.Errors = append(errs.Errors, fmt.Errorf("allowed_task_priorities: priority: %w", err))
	}

	if b.InitialStatus != "" {
		found := false
		for _, s := range b.TaskStatuses {
			if s.Status == b.InitialStatus {
				found = true
				break
			}
		}

		if !found {
			errs.Errors = append(errs.Errors, fmt.Errorf("initial_status: status value %q is not allowed", b.InitialStatus))
		}
	}

	return errs.ErrorOrNil()
}
