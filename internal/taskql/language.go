package taskql

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/bufbuild/connect-go"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/data"
)

type Query struct {
	In    []string
	NotIn []string
}

func (q Query) Len() int {
	return len(q.In) + len(q.NotIn)
}

type Language struct {
	userClient idmv1connect.UserServiceClient
	board      *tasksv1.Board

	l          sync.Mutex
	conditions []*Condition
	lastToken  Token
	queries    map[Field]Query
}

func New(userClient idmv1connect.UserServiceClient, board *tasksv1.Board) *Language {
	return &Language{
		userClient: userClient,
		board:      board,
	}
}

func (l *Language) String() string {
	l.l.Lock()
	defer l.l.Unlock()

	strs := make([]string, 0, len(l.conditions))

	for idx, value := range l.conditions {
		if idx == len(l.conditions)-1 {
			// skip the last empty condition returned by the lexer
			if value.FieldName == "" && value.Value == "" {
				break
			}

			fn := Field(value.FieldName)
			if fn.IsValid() {
				strs = append(strs, value.FieldName + ":" + "\"" + value.Value)
				break
			} else {
				strs = append(strs, value.FieldName)
			}
		}

		strs = append(strs, value.String())
	}

	return strings.Join(strs, " ")
}

func (l *Language) Process(input string) error {
	conditions, lastToken, err := Parse(input)
	if err != nil {
		return err
	}

	l.l.Lock()
	defer l.l.Unlock()

	l.queries = make(map[Field]Query)
	l.conditions = conditions
	l.lastToken = lastToken

	for _, c := range conditions {
		// skip empty conditions
		if c.FieldName == "" && c.Value == "" {
			continue
		}

		// construct the field name, validation happens at a later stage
		fn := Field(c.FieldName)

		// ensure there's already a query entry for the field name
		query, ok := l.queries[fn]
		if !ok {
			query = Query{}
		}

		// Append the value to the correct query slice.
		if c.Not {
			query.NotIn = append(query.NotIn, c.Value)
		} else {
			query.In = append(query.In, c.Value)
		}

		l.queries[fn] = query
	}

	return nil
}

func (l *Language) Query(ctx context.Context) (map[Field]Query, error) {
	l.l.Lock()
	defer l.l.Unlock()

	copy := maps.Clone(l.queries)

	if copy == nil {
		return nil, nil
	}

	for fn := range copy {
		if !fn.IsValid() {
			return nil, fmt.Errorf("unsupported field name %q", fn)
		}
	}

	// resolve all user ids if there are some
	if copy[FieldCreator].Len() > 0 || copy[FieldAssignee].Len() > 0 {
		users, err := l.userClient.ListUsers(ctx, connect.NewRequest(&idmv1.ListUsersRequest{}))
		if err != nil {
			return nil, err
		}

		userMap := data.IndexSlice(users.Msg.Users, func(p *idmv1.Profile) string {
			return p.User.Username
		})

		replace := func(list []string) {
			for idx, u := range list {
				// Special value
				if u == "me" {
					user := auth.From(ctx)
					if user != nil {
						list[idx] = user.ID
						continue
					}
				}

				user, ok := userMap[u]
				if ok {
					list[idx] = user.User.Id
				}
			}
		}

		replace(copy[FieldCreator].In)
		replace(copy[FieldCreator].NotIn)
		replace(copy[FieldAssignee].In)
		replace(copy[FieldAssignee].NotIn)
	}

	priorityMap := data.IndexSlice(l.board.AllowedTaskPriorities, func(p *tasksv1.TaskPriority) string {
		return p.Name
	})

	// resolve priority values
	for idx, v := range copy[FieldPriority].In {
		p, ok := priorityMap[v]
		if ok {
			copy[FieldPriority].In[idx] = strconv.Itoa(int(p.Priority))
		}
	}

	return copy, nil
}

func (l *Language) ExpectedNextToken(ctx context.Context) (Token, string, []string, error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.board == nil {
		return TokenStart, "", nil, fmt.Errorf("expected next token cannot be retrieved without a specified board")
	}

	// there isn't even a single token
	if len(l.conditions) == 0 {
		values, err := l.getPossibleValues(ctx, nil)
		if err != nil {
			return TokenStart, "", nil, err
		}

		return TokenStart, "", values, nil
	}

	last := l.conditions[len(l.conditions)-1]

	values, err := l.getPossibleValues(ctx, last)
	if err != nil {
		return TokenStart, last.FieldName, nil, err
	}

	return l.lastToken, last.FieldName, values, nil
}

func (l *Language) getPossibleValues(ctx context.Context, last *Condition) ([]string, error) {
	switch {
	case last == nil:
		result := make([]string, len(allFields))
		for idx, fn := range allFields {
			result[idx] = string(fn)
		}

		return result, nil

	case !Field(last.FieldName).IsValid():
		result := make([]string, 0)
		for _, fn := range allFields {
			if strings.HasPrefix(
				string(fn),
				last.FieldName,
			) {
				result = append(result, string(fn))
			}
		}

		return result, nil

	default:
		return l.getFieldValues(ctx, Field(last.FieldName))
	}
}

func (l *Language) getFieldValues(ctx context.Context, fieldName Field) ([]string, error) {
	if !fieldName.IsValid() {
		return nil, fmt.Errorf("unsupported or invalid field name %q", fieldName)
	}

	users, err := l.userClient.ListUsers(ctx, connect.NewRequest(&idmv1.ListUsersRequest{}))
	if err != nil {
		return nil, err
	}

	switch fieldName {
	case FieldAssignee:
		return mapValues(
			filterValues(
				users.Msg.Users,
				func(p *idmv1.Profile) bool {
					if len(l.board.EligibleRoleIds) == 0 && len(l.board.EligibleUserIds) == 0 {
						return true
					}

					if slices.Contains(l.board.EligibleUserIds, p.User.Id) {
						return true
					}

					for _, r := range p.Roles {
						if slices.Contains(l.board.EligibleRoleIds, r.Id) {
							return true
						}
					}

					return false
				},
			),
			func(p *idmv1.Profile) string {
				return p.User.Username
			},
		), nil

	case FieldCompleted:
		return []string{
			"true",
			"false",
		}, nil

	case FieldCreator:
		return mapValues(
			users.Msg.Users,
			func(p *idmv1.Profile) string {
				return p.User.Username
			},
		), nil

	case FieldPriority:
		return mapValues(l.board.AllowedTaskPriorities, func(e *tasksv1.TaskPriority) string {
			return e.Name
		}), nil

	case FieldStatus:
		return mapValues(l.board.AllowedTaskStatus, func(e *tasksv1.TaskStatus) string {
			return e.Status
		}), nil

	case FieldTag:
		return mapValues(l.board.AllowedTaskTags, func(e *tasksv1.TaskTag) string {
			return e.Tag
		}), nil
	}

	return nil, fmt.Errorf("unsupported or invalid field name: %q", fieldName)
}

func mapValues[T any, E any](list []T, fn func(T) E) []E {
	result := make([]E, len(list))

	for idx, e := range list {
		result[idx] = fn(e)
	}

	return result
}

func filterValues[T any](list []T, fn func(T) bool) []T {
	result := make([]T, 0, len(list))

	for _, e := range list {
		if fn(e) {
			result = append(result, e)
		}
	}

	return result
}
