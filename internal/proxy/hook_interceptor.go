package proxy

import (
	"context"
	"fmt"
	"plugin"

	"github.com/milvus-io/milvus-proto/go-api/hook"

	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type defaultHook struct {
}

func (d defaultHook) Init(params map[string]string) error {
	return nil
}

func (d defaultHook) Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	return false, nil, nil
}

func (d defaultHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	return ctx, nil
}

func (d defaultHook) After(ctx context.Context, result interface{}, err error, fullMethod string) error {
	return nil
}

func (d defaultHook) Release() {}

var hoo hook.Hook

func initHook() error {
	path := Params.ProxyCfg.SoPath
	if path == "" {
		hoo = defaultHook{}
		return nil
	}

	logger.Debug("start to load plugin", zap.String("path", path))
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("fail to open the plugin, error: %s", err.Error())
	}
	logger.Debug("plugin open")

	h, err := p.Lookup("MilvusHook")
	if err != nil {
		return fmt.Errorf("fail to the 'MilvusHook' object in the plugin, error: %s", err.Error())
	}

	var ok bool
	hoo, ok = h.(hook.Hook)
	if !ok {
		return fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = hoo.Init(Params.HookCfg.SoConfig); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	return nil
}

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	if hookError := initHook(); hookError != nil {
		logger.Error("hook error", zap.String("path", Params.ProxyCfg.SoPath), zap.Error(hookError))
		hoo = defaultHook{}
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var (
			fullMethod = info.FullMethod
			newCtx     context.Context
			isMock     bool
			mockResp   interface{}
			realResp   interface{}
			realErr    error
			err        error
		)

		if isMock, mockResp, err = hoo.Mock(ctx, req, fullMethod); isMock {
			return mockResp, err
		}

		if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
			return nil, err
		}
		realResp, realErr = handler(newCtx, req)
		if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
			return nil, err
		}
		return realResp, realErr
	}
}
