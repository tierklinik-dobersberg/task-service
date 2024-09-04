package permission

import (
	"context"
	"log/slog"
	"slices"

	"github.com/bufbuild/connect-go"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type Resolver struct {
	userClient idmv1connect.UserServiceClient
	roleClient idmv1connect.RoleServiceClient
}

func NewResolver(httpCli connect.HTTPClient, baseURL string) *Resolver {
	return &Resolver{
		userClient: idmv1connect.NewUserServiceClient(httpCli, baseURL),
		roleClient: idmv1connect.NewRoleServiceClient(httpCli, baseURL),
	}
}

func (r *Resolver) IsAllowed(ctx context.Context, userId string, perm *tasksv1.BoardPermission) bool {
	// quick check if the user is immediately denied
	if slices.Contains(perm.DenyUsers, userId) {
		return false
	}

	// fetch user roles
	profileResponse, err := r.userClient.GetUser(ctx, connect.NewRequest(&idmv1.GetUserRequest{
		Search: &idmv1.GetUserRequest_Id{
			Id: userId,
		},
		FieldMask: &fieldmaskpb.FieldMask{
			Paths: []string{"profile.roles"},
		},
	}))

	if err != nil {
		slog.Error("failed to resolve user roles", "userID", userId, "error", err)

		return false
	}

	for _, r := range profileResponse.Msg.Profile.Roles {
		if slices.Contains(perm.DenyRoles, r.Id) {
			return false
		}
	}

	// check if the user is allowed
	if slices.Contains(perm.AllowUsers, userId) {
		return true
	}

	for _, r := range profileResponse.Msg.Profile.Roles {
		if slices.Contains(perm.AllowRoles, r.Id) {
			return true
		}
	}

	// if no "allow" values are set, this is a deny-list only.
	return len(perm.AllowRoles) == 0 && len(perm.AllowUsers) == 0
}
