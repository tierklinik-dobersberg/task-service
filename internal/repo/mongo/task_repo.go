package mongo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	commonv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/common/v1"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

func (db *Repository) CreateTask(ctx context.Context, task *tasksv1.Task) error {
	model, err := taskFromProto(task)
	if err != nil {
		return err
	}

	res, err := db.tasks.InsertOne(ctx, model)
	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	task.Id = res.InsertedID.(primitive.ObjectID).Hex()

	return nil
}

func (db *Repository) DeleteTask(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("failed to parse task id: %w", err)
	}

	res, err := db.tasks.DeleteOne(ctx, bson.M{"_id": oid})
	if err != nil {
		return fmt.Errorf("failed to perform delete operation: %w", err)
	}

	if res.DeletedCount == 0 {
		return repo.ErrTaskNotFound
	}

	if _, err := db.timeline.DeleteMany(ctx, bson.M{
		"taskId": oid,
	}); err != nil {
		slog.Error("failed to remove time-line entries", "taskId", id, "error", err)
	}

	return nil
}

func (db *Repository) AssignTask(ctx context.Context, taskID, assigneeID, assignedByUserId string) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	update := bson.M{
		"$set": bson.M{
			"assignedBy": assignedByUserId,
			"assignee":   assigneeID,
			"assignTime": time.Now(),
		},
	}

	session, err := db.client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		old, err := db.GetTask(ctx, taskID)
		if err != nil {
			return nil, err
		}

		db.recordChange(ctx, taskID, old.BoardId, &ValueChange{
			FieldName: "assignee_id",
			OldValue:  old.AssigneeId,
			NewValue:  assigneeID,
		})

		res := db.tasks.FindOneAndUpdate(ctx, bson.M{"_id": oid}, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrTaskNotFound
			}

			return nil, fmt.Errorf("failed to perform findAndModify: %w", err)
		}

		var t Task
		if err := res.Decode(&t); err != nil {
			return nil, fmt.Errorf("failed to decode task document: %w", err)
		}

		return t.ToProto(), nil
	})
	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Task), nil
}

func (db *Repository) CompleteTask(ctx context.Context, taskID string) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	now := time.Now()

	update := bson.M{
		"$set": bson.M{
			"completeTime": now,
		},
	}

	session, err := db.client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		old, err := db.GetTask(ctx, taskID)
		if err != nil {
			return nil, err
		}

		db.recordChange(ctx, taskID, old.BoardId, &ValueChange{
			FieldName: "complete_time",
			NewValue:  now.Format(time.RFC3339),
		})

		res := db.tasks.FindOneAndUpdate(ctx, bson.M{"_id": oid}, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrTaskNotFound
			}

			return nil, fmt.Errorf("failed to perform findAndModify: %w", err)
		}

		var t Task
		if err := res.Decode(&t); err != nil {
			return nil, fmt.Errorf("failed to decode task document: %w", err)
		}

		return t.ToProto(), nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Task), nil
}

func (db *Repository) GetTask(ctx context.Context, taskID string) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	res := db.tasks.FindOne(ctx, bson.M{"_id": oid})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, repo.ErrTaskNotFound
		}

		return nil, fmt.Errorf("failed to perform find operation: %w", err)
	}

	var t Task
	if err := res.Decode(&t); err != nil {
		return nil, fmt.Errorf("failed to decode task document: %w", err)
	}

	return t.ToProto(), nil
}

func (db *Repository) UpdateTask(ctx context.Context, authenticatedUserId string, update *tasksv1.UpdateTaskRequest) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(update.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	task, err := db.GetTask(ctx, update.TaskId)
	if err != nil {
		return nil, err
	}

	board, err := db.GetBoard(ctx, task.BoardId)
	if err != nil {
		return nil, err
	}

	setModel := bson.M{
		"updateTime": time.Now(),
	}
	unsetModel := bson.M{}
	pushModel := bson.M{}
	pullModel := bson.M{}

	paths := []string{}

	if p := update.GetUpdateMask().GetPaths(); len(p) > 0 {
		paths = p
	}

	if len(paths) == 0 {
		return task, nil
	}

	session, err := db.client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		changes := make([]*ValueChange, 0, len(paths))

		for _, p := range paths {
			switch p {
			case "title":
				setModel["title"] = update.Title
				changes = append(changes, &ValueChange{
					FieldName: "title",
					OldValue:  task.Title,
				})

			case "description":
				setModel["description"] = update.Description
				changes = append(changes, &ValueChange{
					FieldName: "description",
					OldValue:  task.Description,
				})
			case "assignee_id":
				setModel["assignee"] = update.AssigneeId
				setModel["assignTime"] = time.Now()
				setModel["assignedBy"] = authenticatedUserId

				changes = append(changes, &ValueChange{
					FieldName: "assignee_id",
					OldValue:  task.AssigneeId,
				})

				// if the user does not have a subscription placed, subscribe the new assignee
				// automatically
				if _, ok := task.Subscriptions[authenticatedUserId]; !ok {
					setModel["subscriptions."+authenticatedUserId] = Subscription{
						UserId:       authenticatedUserId,
						Types:        make([]tasksv1.NotificationType, 0),
						Unsubscribed: false,
					}
				}

			case "location":
				switch v := update.Location.(type) {
				case *tasksv1.UpdateTaskRequest_Address:
					setModel["address"] = addrFromProto(v.Address)
					unsetModel["location"] = ""
				case *tasksv1.UpdateTaskRequest_GeoLocation:
					setModel["location"] = geoLocationFromProto(v.GeoLocation)
					unsetModel["address"] = ""
				default:
					unsetModel["address"] = ""
					unsetModel["location"] = ""
				}

				changes = append(changes, &ValueChange{
					FieldName: "location",
				})

			case "priority":
				if update.Priority != nil {
					setModel["priority"] = update.Priority.Value
				} else {
					unsetModel["priority"] = ""
				}

				var old any
				if task.Priority != nil {
					old = task.Priority.Value
				}

				changes = append(changes, &ValueChange{
					FieldName: "priority",
					OldValue:  old,
				})

			case "tags":
				switch v := update.Tags.(type) {
				case *tasksv1.UpdateTaskRequest_AddTags:
					pushModel["tags"] = bson.M{
						"$each": v.AddTags.Values,
					}
					changes = append(changes, &ValueChange{
						FieldName: "tags",
						NewValue:  v.AddTags.Values,
					})

				case *tasksv1.UpdateTaskRequest_DeleteTags:
					pullModel["tags"] = bson.M{
						"$in": v.DeleteTags.Values,
					}
					changes = append(changes, &ValueChange{
						FieldName: "tags",
						OldValue:  v.DeleteTags.Values,
					})

				case *tasksv1.UpdateTaskRequest_ReplaceTags:
					setModel["tags"] = v.ReplaceTags.Values

					changes = append(changes, &ValueChange{
						FieldName: "tags",
						NewValue:  v.ReplaceTags.Values,
						OldValue:  task.Tags,
					})
				}

			case "status":
				if update.Status == "" {
					update.Status = board.InitialStatus
				}

				setModel["status"] = update.Status

				changes = append(changes, &ValueChange{
					FieldName: "status",
					OldValue:  task.Status,
				})

			case "due_time":
				if update.DueTime.IsValid() {
					setModel["dueTime"] = update.DueTime.AsTime()
				} else {
					unsetModel["dueTime"] = ""
				}

				var old any
				if task.DueTime != nil && task.DueTime.IsValid() {
					old = task.DueTime.AsTime().Format(time.RFC3339)
				}

				changes = append(changes, &ValueChange{
					FieldName: "due_time",
					OldValue:  old,
				})

			case "properties":
				switch v := update.Properties.(type) {
				case *tasksv1.UpdateTaskRequest_AddProperties:
					for _, property := range v.AddProperties.Properties {
						blob, err := proto.Marshal(property.Value)
						if err != nil {
							return nil, fmt.Errorf("failed to marshal property value for %q: %w", property.Key, err)
						}

						setModel["properties."+property.Key] = blob
					}

				case *tasksv1.UpdateTaskRequest_DeleteProperties:
					for _, prop := range v.DeleteProperties.Values {
						unsetModel["properties."+prop] = ""
					}
				}

			default:
				return nil, fmt.Errorf("invalid path %q in update_mask", p)
			}
		}

		updateModel := bson.M{}

		if len(setModel) > 1 {
			updateModel["$set"] = setModel
		}

		if len(unsetModel) > 0 {
			updateModel["$unset"] = unsetModel
		}

		if len(pullModel) > 0 {
			updateModel["$pull"] = pullModel
		}

		if len(pushModel) > 0 {
			updateModel["$addToSet"] = pushModel
		}

		if len(updateModel) == 0 {
			return nil, fmt.Errorf("nothing to update")
		}

		res := db.tasks.FindOneAndUpdate(
			ctx,
			bson.M{"_id": oid},
			updateModel,
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		)
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrTaskNotFound
			}

			return nil, fmt.Errorf("failed to perform findAndModify: %w", err)
		}

		var t Task
		if err := res.Decode(&t); err != nil {
			return nil, fmt.Errorf("failed to decode task document: %w", err)
		}

		// finnaly, create change records for each change
		for _, change := range changes {
			if change.NewValue == nil && change.FieldName != "tags" {
				change.NewValue = change.ValueFrom(&t)
			}

			db.recordChange(ctx, update.TaskId, t.BoardID, change)
		}

		return t.ToProto(), nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Task), nil
}

func (db *Repository) buildQueryFilter(queries []*tasksv1.TaskQuery) (bson.M, error) {
	mongoQueries := bson.A{}

	for _, q := range queries {
		filter := bson.M{}

		if len(q.AssignedTo) > 0 {
			filter["assignee"] = bson.M{
				"$in": q.AssignedTo,
			}
		}

		if len(q.BoardId) > 0 {
			filter["boardId"] = bson.M{
				"$in": q.BoardId,
			}
		}

		if len(q.CreatedBy) > 0 {
			filter["creator"] = bson.M{
				"$in": q.CreatedBy,
			}
		}

		if len(q.Statuses) > 0 {
			filter["status"] = bson.M{
				"$in": q.Statuses,
			}
		}

		if len(q.Tags) > 0 {
			filter["tags"] = bson.M{
				"$all": q.Tags,
			}
		}

		if q.DueBetween != nil {
			dueFilter := bson.M{}

			if f := q.DueBetween.From; f.IsValid() {
				dueFilter["$gte"] = f.AsTime()
			}

			if t := q.DueBetween.To; t.IsValid() {
				dueFilter["$lte"] = t.AsTime()
			}

			if len(dueFilter) > 0 {
				filter["dueTime"] = dueFilter
			}
		}

		if q.Completed != nil {
			filter["completeTime"] = bson.M{
				"$exists": q.Completed.Value,
			}
		}

		mongoQueries = append(mongoQueries, filter)
	}

	filter := bson.M{}

	switch len(mongoQueries) {
	case 0:
		// we load all tasks
	case 1:
		filter = mongoQueries[0].(bson.M)
	default:
		filter["$or"] = mongoQueries
	}

	return filter, nil
}

func (db *Repository) ListTasks(ctx context.Context, queries []*tasksv1.TaskQuery, pagination *commonv1.Pagination) ([]*tasksv1.Task, int, error) {
	filter, err := db.buildQueryFilter(queries)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to build task query: %w", err)
	}

	paginationPipeline := mongo.Pipeline{}

	if pagination != nil {
		if len(pagination.SortBy) > 0 {
			sort := bson.D{}
			for _, field := range pagination.SortBy {
				var dir int
				switch field.Direction {
				case commonv1.SortDirection_SORT_DIRECTION_ASC:
					dir = 1
				default:
					dir = -1
				}

				// FIXME(ppacher): convert from proto field-name to mongoDB document key
				sort = append(sort, bson.E{Key: field.FieldName, Value: dir})
			}

			paginationPipeline = append(paginationPipeline, bson.D{
				{Key: "$sort", Value: sort},
			})
		}

		if pagination.PageSize > 0 {
			paginationPipeline = append(paginationPipeline, bson.D{{Key: "$skip", Value: pagination.PageSize * pagination.GetPage()}})
			paginationPipeline = append(paginationPipeline, bson.D{{Key: "$limit", Value: pagination.PageSize}})
		}
	}

	pipeline := mongo.Pipeline{}

	if len(filter) > 0 {
		pipeline = append(pipeline, bson.D{
			{
				Key:   "$match",
				Value: filter,
			},
		})
	}

	pipeline = append(pipeline, bson.D{
		{
			Key: "$facet",
			Value: bson.M{
				"metadata": []bson.D{
					{{
						Key:   "$count",
						Value: "totalCount",
					}},
				},
				"data": paginationPipeline,
			},
		},
	})

	blob, err := json.MarshalIndent(pipeline, "", "   ")
	if err == nil {
		log.Println(string(blob))
	}

	res, err := db.tasks.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to perform find operation: %w", err)
	}

	var result []struct {
		Metadata []struct {
			TotalCount int `bson:"totalCount"`
		} `bson:"metadata"`
		Data []Task
	}

	if err := res.All(ctx, &result); err != nil {
		return nil, 0, fmt.Errorf("failed to decode result: %w", err)
	}

	// nothing found
	if len(result) == 0 {
		return nil, 0, nil
	}

	if len(result) > 1 {
		slog.Warn("received unexpected result count for aggregation state", "count", len(result))
	}

	pbResult := make([]*tasksv1.Task, len(result[0].Data))
	for idx, r := range result[0].Data {
		pbResult[idx] = r.ToProto()
	}

	var count int
	if len(result[0].Metadata) == 1 {
		count = result[0].Metadata[0].TotalCount
	}

	return pbResult, count, nil
}

func (db *Repository) AddTaskAttachment(ctx context.Context, taskID, filePath string, attachment *tasksv1.Attachment) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	filter := bson.M{
		"_id": oid,
	}

	update := bson.M{
		"$push": bson.M{
			"attachments": attachmentFromProto(attachment),
		},
	}

	res := db.tasks.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, repo.ErrTaskNotFound
		}

		return nil, err
	}

	var t Task
	if err := res.Decode(&t); err != nil {
		return nil, fmt.Errorf("failed to decode task document: %w", err)
	}

	return t.ToProto(), nil
}

func (db *Repository) DeleteTaskAttachment(ctx context.Context, taskID, attachmentName string) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	filter := bson.M{
		"_id": oid,
	}

	update := bson.M{
		"$pull": bson.M{
			"attachments": bson.M{
				"name": attachmentName,
			},
		},
	}

	res := db.tasks.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, repo.ErrTaskNotFound
		}

		return nil, err
	}

	var t Task
	if err := res.Decode(&t); err != nil {
		return nil, fmt.Errorf("failed to decode task document: %w", err)
	}

	return t.ToProto(), nil
}

func (db *Repository) DeleteTasksMatchingQuery(ctx context.Context, queries []*tasksv1.TaskQuery) error {
	filter, err := db.buildQueryFilter(queries)
	if err != nil {
		return fmt.Errorf("failed to build task query: %w", err)
	}

	_, err = db.tasks.DeleteMany(ctx, filter)

	return err
}

func (db *Repository) UpdateTaskSubscription(ctx context.Context, taskId string, subscription *tasksv1.Subscription) error {
	oid, err := primitive.ObjectIDFromHex(taskId)
	if err != nil {
		return fmt.Errorf("failed to parse task id: %w", err)
	}

	filter := bson.M{
		"_id": oid,
	}

	res, err := db.tasks.UpdateOne(
		ctx,
		filter,
		bson.M{
			"$set": bson.M{
				"subscriptions." + subscription.UserId: subscriptionFromProto(subscription),
			},
		},
	)
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return repo.ErrTaskNotFound
	}

	return err
}

func (db *Repository) DeleteTagsFromTasks(ctx context.Context, boardId string, tag string) error {
	_, err := db.tasks.UpdateMany(
		ctx,
		bson.M{"boardId": boardId},
		bson.M{
			"$pull": bson.M{
				"tags": tag,
			},
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func (db *Repository) DeletesPriorityFromTasks(ctx context.Context, boardId string, priority, replacement int32) error {
	update := bson.M{
		"$unset": bson.M{
			"priority": "",
		},
	}

	if replacement > 0 {
		update = bson.M{
			"$set": bson.M{
				"priority": replacement,
			},
		}
	}
	_, err := db.tasks.UpdateMany(
		ctx,
		bson.M{"boardId": boardId, "priority": priority},
		update,
	)

	if err != nil {
		return err
	}

	return nil
}

func (db *Repository) DeleteStatusFromTasks(ctx context.Context, boardId string, status string, replacement string) error {
	update := bson.M{
		"$unset": bson.M{
			"status": "",
		},
	}

	if replacement != "" {
		update = bson.M{
			"$set": bson.M{
				"status": replacement,
			},
		}
	}
	_, err := db.tasks.UpdateMany(
		ctx,
		bson.M{"boardId": boardId, "status": status},
		update,
	)

	if err != nil {
		return err
	}

	return nil
}

func (db *Repository) GetTaskTimeline(ctx context.Context, ids []string) ([]*tasksv1.TaskTimelineEntry, error) {
	oids := make([]primitive.ObjectID, len(ids))

	for idx, i := range ids {
		oid, err := primitive.ObjectIDFromHex(i)
		if err != nil {
			return nil, fmt.Errorf("failed to parse task id: %w", err)
		}

		oids[idx] = oid
	}

	filter := bson.M{}

	if len(oids) > 0 {
		filter["taskId"] = bson.M{
			"$in": oids,
		}
	}

	blob, err := json.MarshalIndent(filter, "", "   ")
	if err == nil {
		log.Println(string(blob))
	}

	result, err := db.timeline.Find(ctx, filter, options.Find().SetSort(bson.D{
		{Key: "createTime", Value: 1},
	}))

	if err != nil {
		return nil, err
	}

	var entries []Timeline
	if err := result.All(ctx, &entries); err != nil {
		return nil, err
	}

	pb := make([]*tasksv1.TaskTimelineEntry, 0, len(entries))

	for _, e := range entries {
		pe, err := e.ToProto()
		if err != nil {
			slog.Error("failed to convert time-line entry to protobuf", "error", err, "id", e.ID.Hex())
			continue
		}

		pb = append(pb, pe)
	}

	return pb, nil
}

func (db *Repository) recordChange(ctx context.Context, taskId, boardId string, change timelineValue) {
	oid, err := primitive.ObjectIDFromHex(taskId)
	if err != nil {
		slog.Error("failed to parse task id", "id", taskId, "error", err)
		return
	}

	boardOid, err := primitive.ObjectIDFromHex(boardId)
	if err != nil {
		slog.Error("failed to parse board id", "id", boardId, "error", err)
		return
	}

	var id string
	if user := auth.From(ctx); user != nil {
		id = user.ID
	}

	r := &Timeline{
		TaskID:     oid,
		BoardID:    boardOid,
		CreateTime: time.Now(),
		UserID:     id,
	}

	switch v := change.(type) {
	case *TaskComment:
		r.Comment = v
	case *ValueChange:
		r.ValueChange = v
	}

	if _, err := db.timeline.InsertOne(ctx, r); err != nil {
		slog.Error("failed to insert timeline record", "id", taskId, "error", err)
	}
}

var _ repo.TaskBackend = (*Repository)(nil)
