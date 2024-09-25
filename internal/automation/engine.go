package automation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/bufbuild/connect-go"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/eventloop"
	"github.com/dop251/goja_nodejs/require"
	"github.com/elazarl/goproxy"
	"github.com/olebedev/gojax/fetch"
	idmv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/idm/v1/idmv1connect"
	tasksv1 "github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1"
	"github.com/tierklinik-dobersberg/apis/gen/go/tkd/tasks/v1/tasksv1connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type schedule struct {
	cron string
	fn   goja.Callable
}

type Engine struct {
	rt        *eventloop.EventLoop
	r         *goja.Runtime
	this      *goja.Object
	schedule  *schedule
	providers Providers
}

type Providers struct {
	TaskClient   tasksv1connect.TaskServiceClient
	BoardClient  tasksv1connect.BoardServiceClient
	UserClient   idmv1connect.UserServiceClient
	RoleClient   idmv1connect.RoleServiceClient
	NotifyClient idmv1connect.NotifyServiceClient

	FS fs.FS
}

func New(script string, providers Providers) (*Engine, error) {
	r := require.NewRegistryWithLoader(func(path string) ([]byte, error) {
		return sourceLoader(providers.FS, path)
	})

	e := &Engine{
		rt:        eventloop.NewEventLoop(eventloop.EnableConsole(true), eventloop.WithRegistry(r)),
		providers: providers,
	}

	proxy := goproxy.NewProxyHttpServer()
	fetch.Enable(e.rt, proxy)

	var err error
	e.rt.Run(func(rt *goja.Runtime) {
		e.this = rt.NewObject()
		e.r = rt

		rt.SetFieldNameMapper(goja.TagFieldNameMapper("json", false))

		if err = rt.Set("schedule", e.Schedule); err != nil {
			return
		}

		if err = rt.Set("tasks", e.getTaskObject()); err != nil {
			return
		}

		if err = rt.Set("users", e.getUsersClient()); err != nil {
			return
		}

		if err = rt.Set("roles", e.getRolesClient()); err != nil {
			return
		}

		if err = rt.Set("notify", e.getNotifyClient()); err != nil {
			return
		}

		_, err = rt.RunString(script)
		if err != nil {
			return
		}
	})
	if err != nil {
		return nil, err
	}

	e.rt.Start()

	return e, nil
}

func (e *Engine) Wait() {
	e.rt.Stop()
}

func (e *Engine) RunSchedule() error {
	if e.schedule == nil {
		return nil
	}

	_, err := e.schedule.fn(e.this)

	return err
}

func (e *Engine) Schedule(cron string, fn goja.Callable) {
	e.schedule = &schedule{
		cron: cron,
		fn:   fn,
	}
}

func parseMessage(obj *goja.Object, msg proto.Message) error {
	blob, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	if err := protojson.Unmarshal(blob, msg); err != nil {
		return err
	}

	return nil
}

func (e *Engine) getTaskObject() *goja.Object {
	obj := e.r.NewObject()

	obj.Set("query", func(req *goja.Object) (any, error) {
		var r tasksv1.QueryViewRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.QueryView(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("get", func(req *goja.Object) (any, error) {
		var r tasksv1.GetTaskRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.GetTask(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("update", func(req *goja.Object) (any, error) {
		var r tasksv1.UpdateTaskRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.UpdateTask(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("complete", func(req *goja.Object) (any, error) {
		var r tasksv1.CompleteTaskRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.CompleteTask(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("assign", func(req *goja.Object) (any, error) {
		var r tasksv1.AssignTaskRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.AssignTask(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("delete", func(req *goja.Object) (any, error) {
		var r tasksv1.DeleteTaskRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.TaskClient.DeleteTask(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	return obj
}

func (e *Engine) getUsersClient() *goja.Object {
	obj := e.r.NewObject()

	obj.Set("get", func(req *goja.Object) (any, error) {
		var r idmv1.GetUserRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.UserClient.GetUser(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	obj.Set("list", func(req *goja.Object) (any, error) {
		var r idmv1.ListUsersRequest
		if err := parseMessage(req, &r); err != nil {
			return nil, err
		}

		res, err := e.providers.UserClient.ListUsers(context.Background(), connect.NewRequest(&r))
		if err != nil {
			return nil, err
		}

		return res.Msg, nil
	})

	return obj
}

func (e *Engine) getRolesClient() *goja.Object {
	obj := e.r.NewObject()
	return obj
}

func (e *Engine) getNotifyClient() *goja.Object {
	obj := e.r.NewObject()

	return obj
}

func sourceLoader(root fs.FS, filename string) ([]byte, error) {
	fp := filepath.FromSlash(filename)
	f, err := root.Open(fp)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = require.ModuleFileDoesNotExistError
		} else if runtime.GOOS == "windows" {
			if errors.Is(err, syscall.Errno(0x7b)) { // ERROR_INVALID_NAME, The filename, directory name, or volume label syntax is incorrect.
				err = require.ModuleFileDoesNotExistError
			}
		}
		return nil, err
	}

	defer f.Close()
	// On some systems (e.g. plan9 and FreeBSD) it is possible to use the standard read() call on directories
	// which means we cannot rely on read() returning an error, we have to do stat() instead.
	if fi, err := f.Stat(); err == nil {
		if fi.IsDir() {
			return nil, require.ModuleFileDoesNotExistError
		}
	} else {
		return nil, err
	}

	return io.ReadAll(f)
}
