package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func mayConvertPrimitives(val any) any {
	if arr, ok := val.(primitive.A); ok {
		result := make([]any, len(arr))
		for idx, v := range arr {
			result[idx] = mayConvertPrimitives(v)
		}

		return result
	}

	if m, ok := val.(primitive.M); ok {
		result := make(map[string]any, len(m))
		for key, value := range m {
			result[key] = mayConvertPrimitives(value)
		}
	}

	if d, ok := val.(primitive.DateTime); ok {
		return d.Time()
	}

	if o, ok := val.(primitive.ObjectID); ok {
		return o.Hex()
	}

	return val
}

func anyToAnyPb(value any) (*anypb.Any, error) {
	value = mayConvertPrimitives(value)

	var msg proto.Message

	if t, ok := value.(time.Time); ok {
		msg = timestamppb.New(t)
	} else {
		val, err := structpb.NewValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to create google.protobuf.Value from type %T: %w", value, err)
		}

		msg = val
	}

	return anypb.New(msg)
}
