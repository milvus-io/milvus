package proxy

import (
	"context"
	"plugin"

	"github.com/milvus-io/milvus/api/hook"

	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type defaultHook struct {
}

func (d defaultHook) Init(params map[string]string) error {
	return nil
}

func (d defaultHook) Mock(req interface{}, fullMethod string) (bool, interface{}, error) {
	return false, nil, nil
}

func (d defaultHook) Before(req interface{}, fullMethod string) error {
	return nil
}

func (d defaultHook) After(result interface{}, err error, fullMethod string) error {
	return nil
}

func (d defaultHook) Release() {}

var hoo hook.Hook

func initHook() {
	path := Params.ProxyCfg.SoPath
	if path == "" {
		hoo = defaultHook{}
		return
	}

	logger.Debug("start to load plugin", zap.String("path", path))
	p, err := plugin.Open(path)
	if err != nil {
		exit("fail to open the plugin", err)
	}
	logger.Debug("plugin open")

	h, err := p.Lookup("MilvusHook")
	if err != nil {
		exit("fail to the 'MilvusHook' object in the plugin", err)
	}

	var ok bool
	hoo, ok = h.(hook.Hook)
	if !ok {
		exit("fail to convert the `Hook` interface", nil)
	}
	if err = hoo.Init(Params.HookCfg.SoConfig); err != nil {
		exit("fail to init configs for the hoo", err)
	}
}

func exit(errMsg string, err error) {
	logger.Panic("hoo error", zap.String("path", Params.ProxyCfg.SoPath), zap.String("msg", errMsg), zap.Error(err))
}

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	initHook()
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var (
			fullMethod = info.FullMethod
			isMock     bool
			mockResp   interface{}
			realResp   interface{}
			realErr    error
			err        error
		)

		if isMock, mockResp, err = hoo.Mock(req, fullMethod); isMock {
			return mockResp, err
		}

		if err = hoo.Before(req, fullMethod); err != nil {
			return nil, err
		}
		realResp, realErr = handler(ctx, req)
		if err = hoo.After(realResp, realErr, fullMethod); err != nil {
			return nil, err
		}
		return realResp, realErr
	}
}
