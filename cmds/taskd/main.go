package main

import (
	"context"
	"embed"
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
	"github.com/tierklinik-dobersberg/task-service/internal/permission"
	"github.com/tierklinik-dobersberg/task-service/internal/repo"
	"github.com/tierklinik-dobersberg/task-service/internal/repo/mongo"
	"github.com/tierklinik-dobersberg/task-service/internal/services"
	"github.com/tierklinik-dobersberg/task-service/internal/services/boards"
	"github.com/tierklinik-dobersberg/task-service/internal/services/tasks"
	"google.golang.org/protobuf/reflect/protoregistry"
)

//go:embed mails
var mails embed.FS

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

	slog.Info("task service starting")

	cfg, err := config.LoadConfig(ctx)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)

		os.Exit(1)
	}

	slog.Info("configuration loaded successfully")

	roleServiceClient := idmv1connect.NewRoleServiceClient(http.DefaultClient, cfg.IdmURL)

	protoValidator, err := protovalidate.New()
	if err != nil {
		slog.Error("failed to prepare protovalidator", "error", err)

		os.Exit(1)
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

	files, err := mails.ReadDir("mails")
	if err != nil {
		slog.Error("failed to check content os embedded mail file-system", "error", err)
	} else {
		for _, f := range files {
			slog.Info("mail file-system", "name", f.Name(), "is-directory", f.IsDir())
		}
	}

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

	var resolver *permission.Resolver
	if cfg.IdmURL != "" {
		resolver = permission.NewResolver(http.DefaultClient, cfg.IdmURL)
	}
	common := &services.Common{
		Resolver: resolver,
		Config:   *cfg,
		Mails:    mails,
	}

	boardService, err := boards.New(ctx, repo, common)
	if err != nil {
		logrus.Fatalf("failed to create board service: %s", err)
	}

	// create a new BoardService and add it to the mux.
	path, handler := tasksv1connect.NewBoardServiceHandler(boardService, connect.WithInterceptors(interceptors...))
	serveMux.Handle(path, handler)

	taskService, err := tasks.New(ctx, repo, common)
	if err != nil {
		logrus.Fatalf("failed to create task service: %s", err)
	}

	path, handler = tasksv1connect.NewTaskServiceHandler(taskService, connect.WithInterceptors(interceptors...))
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
