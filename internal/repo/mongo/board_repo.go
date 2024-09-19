package mongo

import (
	"context"
	"errors"
	"fmt"

	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

type Repository struct {
	client   *mongo.Client
	boards   *mongo.Collection
	tasks    *mongo.Collection
	timeline *mongo.Collection
}

func New(ctx context.Context, uri, dbName string) (*Repository, error) {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to create mongodb client: %w", err)
	}

	if err := cli.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping mongodb server: %w", err)
	}

	db := cli.Database(dbName)

	repo := &Repository{
		client:   cli,
		boards:   db.Collection("boards"),
		tasks:    db.Collection("tasks"),
		timeline: db.Collection("timeline"),
	}

	if err := repo.setup(ctx); err != nil {
		return nil, fmt.Errorf("failed to setup collection: %w", err)
	}

	return repo, nil
}

func (db *Repository) setup(ctx context.Context) error {
	if _, err := db.boards.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "displayName", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		},
	}); err != nil {
		return fmt.Errorf("failed to create board indexes: %w", err)
	}

	if _, err := db.tasks.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "boardId", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "creator", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "assignee", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "completeTime", Value: 1},
			},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{Key: "dueTime", Value: 1},
			},
			Options: options.Index().SetSparse(true),
		},
	}); err != nil {
		return fmt.Errorf("failed to create timeline indexes")
	}

	if _, err := db.timeline.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "taskId", Value: 1},
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to create timeline indexes")
	}

	return nil
}

func (db *Repository) CreateBoard(ctx context.Context, board *tasksv1.Board) error {
	model, err := boardFromProto(board)
	if err != nil {
		return err
	}

	res, err := db.boards.InsertOne(ctx, model)
	if err != nil {
		return err
	}

	board.Id = res.InsertedID.(primitive.ObjectID).Hex()

	return nil
}

func (db *Repository) UpdateBoard(ctx context.Context, update *tasksv1.UpdateBoardRequest) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(update.BoardId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse board id: %w", err)
	}

	filter := bson.M{
		"_id": oid,
	}

	setModel := bson.M{}
	unsetModel := bson.M{}

	paths := []string{}

	if p := update.GetUpdateMask().GetPaths(); len(p) > 0 {
		paths = p
	}

	if len(paths) == 0 {
		return db.GetBoard(ctx, update.BoardId)
	}

	for _, p := range paths {
		field := boardTagFromProtoFieldName(p)

		switch p {
		case "display_name":
			setModel[field] = update.DisplayName

		case "description":
			setModel[field] = update.Description

		case "allowed_task_status":
			setModel[field] = statusListFromProto(update.AllowedTaskStatus)

		case "allowed_task_tags":
			setModel[field] = tagListFromProto(update.AllowedTaskTags)

		case "allowed_task_priorities":
			setModel[field] = priorityListFromProto(update.AllowedTaskPriorities)

		case "help_text":
			setModel[field] = update.HelpText

		case "owner_id":
			setModel[field] = update.OwnerId

		case "initial_status":
			setModel[field] = update.InitialStatus

		case "read_permission":
			setModel[field] = permissionsFromProto(update.ReadPermission)

		case "write_permission":
			setModel[field] = permissionsFromProto(update.WritePermission)

		case "eligible_role_ids":
			setModel[field] = update.EligibleRoleIds

		case "eligible_user_ids":
			setModel[field] = update.EligibleUserIds

		case "done_status":
			setModel[field] = update.DoneStatus

		case "views":
			setModel[field] = viewListFromProto(update.Views)

		default:
			return nil, fmt.Errorf("invalid path %q in update_mask", p)
		}
	}

	// FIXME(ppacher): update task tags, statuses and priorites if we deleted some

	updateModel := bson.M{}

	if len(setModel) > 0 {
		updateModel["$set"] = setModel
	}

	if len(unsetModel) > 0 {
		updateModel["$unset"] = unsetModel
	}

	if len(updateModel) == 0 {
		return nil, fmt.Errorf("nothing to update")
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		res := db.boards.FindOneAndUpdate(
			ctx,
			filter,
			updateModel,
			options.FindOneAndUpdate().SetReturnDocument(options.After),
		)
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrTaskNotFound
			}

			return nil, fmt.Errorf("failed to perform findAndModify: %w", err)
		}

		var b Board
		if err := res.Decode(&b); err != nil {
			return nil, fmt.Errorf("failed to decode board document: %w", err)
		}

		// validate the board
		if err := b.Validate(); err != nil {
			return nil, err
		}

		return b.ToProto(), nil
	}, txnOptions)

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) ListBoards(ctx context.Context) ([]*tasksv1.Board, error) {
	res, err := db.boards.Find(ctx, bson.M{})

	if err != nil {
		return nil, err
	}

	var results []*Board
	if err := res.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode boards: %w", err)
	}

	pbBoards := make([]*tasksv1.Board, len(results))
	for idx, b := range results {
		pbBoards[idx] = b.ToProto()
	}

	return pbBoards, nil
}

func (db *Repository) DeleteBoard(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}

	res, err := db.boards.DeleteOne(ctx, bson.M{"_id": oid})
	if err != nil {
		return fmt.Errorf("failed to perform delete operation: %w", err)
	}

	if res.DeletedCount == 0 {
		return repo.ErrBoardNotFound
	}

	return nil
}

func (db *Repository) GetBoard(ctx context.Context, id string) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("invalid id: %w", err)
	}

	res := db.boards.FindOne(ctx, bson.M{"_id": oid})
	if res.Err() != nil {
		return nil, convertErr(res.Err())
	}

	var b Board
	if err := res.Decode(&b); err != nil {
		return nil, fmt.Errorf("failed to decode board: %w", err)
	}

	return b.ToProto(), nil
}

func (db *Repository) AddTaskStatus(ctx context.Context, boardId string, status *tasksv1.TaskStatus) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse board id: %w", err)
	}

	model := statusFromProto(status)

	filter := bson.M{
		"_id": oid,
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		replaceResult, err := db.boards.UpdateOne(ctx, filter, bson.M{
			"$set": bson.M{
				"statuses.$[filter]": model,
			},
		}, options.Update().SetArrayFilters(options.ArrayFilters{
			Filters: []any{
				bson.M{
					"filter.status": model.Status,
				},
			},
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to perform replace operation: %w", err)
		}

		if replaceResult.ModifiedCount == 0 {
			update := bson.M{
				"$push": bson.M{
					"statuses": model,
				},
			}

			res, err := db.boards.UpdateOne(ctx, filter, update)
			if err != nil {
				return nil, fmt.Errorf("failed to perform update operation: %w", err)
			}

			if res.MatchedCount == 0 {
				return nil, repo.ErrBoardNotFound
			}
		}

		b, err := db.GetBoard(ctx, boardId)
		if err != nil {
			return nil, err
		}

		model, err := boardFromProto(b)
		if err != nil {
			return nil, err
		}

		if err := model.Validate(); err != nil {
			return nil, err
		}

		return b, nil
	}, txnOptions)

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) DeleteTaskStatus(ctx context.Context, boardID, status string) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardID)
	if err != nil {
		return nil, fmt.Errorf("invalid board id: %w", err)
	}

	session, err := db.client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	txOpts := options.Transaction().SetWriteConcern(writeconcern.Majority())

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		board, err := db.GetBoard(ctx, boardID)
		if err != nil {
			return nil, err
		}

		// first, unset the status from all tasks
		if err := db.DeleteStatusFromTasks(ctx, boardID, status, board.InitialStatus); err != nil {
			return nil, err
		}

		filter := bson.M{
			"_id": oid,
		}

		update := bson.M{
			"$pull": bson.M{
				"statuses": bson.M{
					"status": status,
				},
			},
		}

		res := db.boards.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrBoardNotFound
			}

			return nil, fmt.Errorf("failed to perform update operation: %w", err)
		}

		var b Board
		if err := res.Decode(&b); err != nil {
			return nil, fmt.Errorf("failed to decode board: %w", err)
		}

		if err := b.Validate(); err != nil {
			return nil, err
		}

		return b.ToProto(), nil
	}, txOpts)

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) AddTaskTag(ctx context.Context, boardId string, tag *tasksv1.TaskTag) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse board id: %w", err)
	}

	model := tagFromProto(tag)

	filter := bson.M{
		"_id": oid,
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		replaceResult, err := db.boards.UpdateOne(ctx, filter, bson.M{
			"$set": bson.M{
				"tags.$[filter]": model,
			},
		}, options.Update().SetArrayFilters(options.ArrayFilters{
			Filters: []any{
				bson.M{
					"filter.tag": model.Tag,
				},
			},
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to perform replace operation: %w", err)
		}

		if replaceResult.ModifiedCount == 0 {
			update := bson.M{
				"$push": bson.M{
					"tags": model,
				},
			}

			res, err := db.boards.UpdateOne(ctx, filter, update)
			if err != nil {
				return nil, fmt.Errorf("failed to perform update operation: %w", err)
			}

			if res.MatchedCount == 0 {
				return nil, repo.ErrBoardNotFound
			}
		}

		b, err := db.GetBoard(ctx, boardId)
		if err != nil {
			return nil, err
		}

		model, err := boardFromProto(b)
		if err != nil {
			return nil, err
		}

		if err := model.Validate(); err != nil {
			return nil, err
		}

		return b, nil
	}, txnOptions)

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) DeleteTaskTag(ctx context.Context, boardID, tag string) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardID)
	if err != nil {
		return nil, fmt.Errorf("invalid board id: %w", err)
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		// first, delete the tag from all tasks
		if err := db.DeleteTagsFromTasks(ctx, boardID, tag); err != nil {
			return nil, err
		}

		filter := bson.M{
			"_id": oid,
		}

		update := bson.M{
			"$pull": bson.M{
				"tags": bson.M{
					"tag": tag,
				},
			},
		}

		res := db.boards.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrBoardNotFound
			}

			return nil, fmt.Errorf("failed to perform update operation: %w", err)
		}

		var b Board
		if err := res.Decode(&b); err != nil {
			return nil, fmt.Errorf("failed to decode board: %w", err)
		}

		if err := b.Validate(); err != nil {
			return nil, err
		}

		return b.ToProto(), nil
	}, txnOptions)
	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) AddView(ctx context.Context, boardId string, view *tasksv1.View) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse board id: %w", err)
	}

	model := viewFromProto(view)

	filter := bson.M{
		"_id": oid,
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		replaceResult, err := db.boards.UpdateOne(ctx, filter, bson.M{
			"$set": bson.M{
				"views.$[filter]": model,
			},
		}, options.Update().SetArrayFilters(options.ArrayFilters{
			Filters: []any{
				bson.M{
					"filter.name": model.Name,
				},
			},
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to perform replace operation: %w", err)
		}

		if replaceResult.ModifiedCount == 0 {
			update := bson.M{
				"$push": bson.M{
					"views": model,
				},
			}

			res, err := db.boards.UpdateOne(ctx, filter, update)
			if err != nil {
				return nil, fmt.Errorf("failed to perform update operation: %w", err)
			}

			if res.MatchedCount == 0 {
				return nil, repo.ErrBoardNotFound
			}
		}

		b, err := db.GetBoard(ctx, boardId)
		if err != nil {
			return nil, err
		}

		model, err := boardFromProto(b)
		if err != nil {
			return nil, err
		}

		if err := model.Validate(); err != nil {
			return nil, err
		}

		return b, nil
	}, txnOptions)

	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) DeleteView(ctx context.Context, boardID, view string) (*tasksv1.Board, error) {
	oid, err := primitive.ObjectIDFromHex(boardID)
	if err != nil {
		return nil, fmt.Errorf("invalid board id: %w", err)
	}

	wc := writeconcern.Majority()
	txnOptions := options.Transaction().SetWriteConcern(wc)

	session, err := db.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(ctx mongo.SessionContext) (interface{}, error) {
		filter := bson.M{
			"_id": oid,
		}

		update := bson.M{
			"$pull": bson.M{
				"views": bson.M{
					"name": view,
				},
			},
		}

		res := db.boards.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(options.After))
		if err := res.Err(); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, repo.ErrBoardNotFound
			}

			return nil, fmt.Errorf("failed to perform update operation: %w", err)
		}

		var b Board
		if err := res.Decode(&b); err != nil {
			return nil, fmt.Errorf("failed to decode board: %w", err)
		}

		if err := b.Validate(); err != nil {
			return nil, err
		}

		return b.ToProto(), nil
	}, txnOptions)
	if err != nil {
		return nil, err
	}

	return result.(*tasksv1.Board), nil
}

func (db *Repository) UpdateBoardSubscription(ctx context.Context, boardId string, subscription *tasksv1.Subscription) error {
	oid, err := primitive.ObjectIDFromHex(boardId)
	if err != nil {
		return fmt.Errorf("failed to parse board id: %w", err)
	}

	filter := bson.M{
		"_id": oid,
	}

	res, err := db.boards.UpdateOne(
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
		return repo.ErrBoardNotFound
	}

	return err
}

// Compile-time check
var _ repo.BoardBackend = (*Repository)(nil)

func convertErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		return repo.ErrBoardNotFound
	}

	return err
}
