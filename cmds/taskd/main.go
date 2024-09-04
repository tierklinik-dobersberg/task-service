package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/bufbuild/connect-go"
	"github.com/bufbuild/protovalidate-go"
	"github.com/sirupsen/logrus"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"github.com/tierklinik-dobersberg/apis/pkg/auth"
	"github.com/tierklinik-dobersberg/apis/pkg/cors"
	"github.com/tierklinik-dobersberg/apis/pkg/log"
	"github.com/tierklinik-dobersberg/apis/pkg/server"
	"github.com/tierklinik-dobersberg/apis/pkg/validator"
	"github.com/tierklinik-dobersberg/task-service/internal/config"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"github.com/tierklinik-dobersberg/task-service/internal/repo/mongo"
	"github.com/tierklinik-dobersberg/task-service/internal/services/boards"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type resolver map[string]int

func (r resolver) IsAllowed(importer string, owners []string) bool {
	p := r[importer]

	for _, o := range owners {
		if r[o] > p {
			return false
		}
	}

	return true
}

var serverContextKey = struct{ S string }{S: "serverContextKey"}

func main() {
	ctx := context.Background()

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		logrus.Fatalf("failed to load config: %s", err)
	}

	roleServiceClient := idmv1connect.NewRoleServiceClient(http.DefaultClient, cfg.IdmURL)

	protoValidator, err := protovalidate.New()
	if err != nil {
		logrus.Fatalf("failed to prepare protovalidator: %s", err)
	}

	authInterceptor := auth.NewAuthAnnotationInterceptor(
		protoregistry.GlobalFiles,
		auth.NewIDMRoleResolver(roleServiceClient),
		func(ctx context.Context, req connect.AnyRequest) (auth.RemoteUser, error) {
			serverKey, _ := ctx.Value(serverContextKey).(string)

			if serverKey == "admin" {
				return auth.RemoteUser{
					ID:          "service-account",
					DisplayName: req.Peer().Addr,
					RoleIDs:     []string{"idm_superuser"}, // FIXME(ppacher): use a dedicated manager role for this
					Admin:       true,
				}, nil
			}

			return auth.RemoteHeaderExtractor(ctx, req)
		},
	)

	interceptors := []connect.Interceptor{
		log.NewLoggingInterceptor(),
		validator.NewInterceptor(protoValidator),
	}

	slog.SetLogLoggerLevel(slog.LevelDebug)

	if os.Getenv("DEBUG") == "" {
		interceptors = append(interceptors, authInterceptor)
	}

	corsConfig := cors.Config{
		AllowedOrigins:   cfg.AllowedOrigins,
		AllowCredentials: true,
	}

	// Prepare our servemux and add handlers.
	serveMux := http.NewServeMux()

	var backend repo.Backend

	if cfg.MongoDBURL != "" {
		var err error
		backend, err = mongo.New(ctx, cfg.MongoDBURL, cfg.MongoDatabaseName)

		if err != nil {
			logrus.Fatalf("failed to create repository: %s", err)
		}
	} else {
		logrus.Fatal("missing MONGO_URL and MONGO_DATABASE")
	}

	repo := repo.New(backend)

	svc, err := boards.New(ctx, repo)
	if err != nil {
		logrus.Fatalf("failed to create board service: %w", err)
	}

	// create a new BoardService and add it to the mux.
	path, handler := tasksv1connect.NewBoardServiceHandler(svc, connect.WithInterceptors(interceptors...))
	serveMux.Handle(path, handler)

	loggingHandler := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logrus.Infof("received request: %s %s %s%s", r.Proto, r.Method, r.Host, r.URL.String())

			next.ServeHTTP(w, r)
		})
	}

	wrapWithKey := func(key string, next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(context.WithValue(r.Context(), serverContextKey, key))

			next.ServeHTTP(w, r)
		})
	}

	// Create the server
	srv, err := server.CreateWithOptions(cfg.ListenAddress, wrapWithKey("public", loggingHandler(serveMux)), server.WithCORS(corsConfig))
	if err != nil {
		logrus.Fatalf("failed to setup server: %s", err)
	}

	adminServer, err := server.CreateWithOptions(cfg.AdminListenAddress, wrapWithKey("admin", loggingHandler(serveMux)), server.WithCORS(corsConfig))
	if err != nil {
		logrus.Fatalf("failed to setup server: %s", err)
	}

	logrus.Infof("HTTP/2 server (h2c) prepared successfully, startin to listen ...")

	if err := server.Serve(ctx, srv, adminServer); err != nil {
		logrus.Fatalf("failed to serve: %s", err)
	}
}
