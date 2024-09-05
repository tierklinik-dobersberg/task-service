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
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
}

func (db *Repository) CompleteTask(ctx context.Context, taskID string) (*tasksv1.Task, error) {
	oid, err := primitive.ObjectIDFromHex(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %w", err)
	}

	update := bson.M{
		"$set": bson.M{
			"completeTime": time.Now(),
		},
	}

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

	setModel := bson.M{
		"updateTime": time.Now(),
	}
	unsetModel := bson.M{}
	pushModel := bson.M{}
	pullModel := bson.M{}

	paths := []string{
		"title",
		"description",
		"assignee_id",
		"location",
		"add_tags",
		"delete_tags",
		"status",
		"due_time",
	}

	if p := update.GetUpdateMask().GetPaths(); len(p) > 0 {
		paths = p
	}

	for _, p := range paths {
		switch p {
		case "title":
			setModel["title"] = update.Title
		case "description":
			setModel["description"] = update.Description
		case "assignee_id":
			setModel["assignee"] = update.AssigneeId
			setModel["assignTime"] = time.Now()
			setModel["assignedBy"] = authenticatedUserId
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
		case "add_tags":
			pushModel["tags"] = bson.M{
				"$each": update.AddTags,
			}
		case "delete_tags":
			pullModel["tags"] = bson.M{
				"$in": update.DeleteTags,
			}
		case "status":
			setModel["status"] = update.Status
		case "due_time":
			if update.DueTime.IsValid() {
				setModel["dueTime"] = update.DueTime.AsTime()
			} else {
				unsetModel["dueTime"] = ""
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

	return t.ToProto(), nil
}

func (db *Repository) ListTasks(ctx context.Context, queries []*tasksv1.TaskQuery, pagination *commonv1.Pagination) ([]*tasksv1.Task, int, error) {
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

		if q.DueBefore.IsValid() {
			filter["dueTime"] = bson.M{
				"$lte": q.DueBefore.AsTime(),
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

var _ repo.TaskBackend = (*Repository)(nil)
