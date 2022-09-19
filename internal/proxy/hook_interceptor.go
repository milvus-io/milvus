package proxy

import (
	"context"
	"plugin"

	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type Hook interface {
	Init(params map[string]string) error
	Mock(req interface{}, fullMethod string) (bool, interface{}, error)
	Before(req interface{}, fullMethod string) error
	After(result interface{}, err error, fullMethod string) error
}

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

var hook Hook

func initHook() {
	if Params.HookCfg.SoPath == "" {
		hook = defaultHook{}
		return
	}
	logger.Debug("main function start")

	p, err := plugin.Open(Params.HookCfg.SoPath)
	if err != nil {
		exit("fail to open the plugin", err)
	}
	logger.Debug("plugin open")

	h, err := p.Lookup("MilvusHook")
	if err != nil {
		exit("fail to the 'MilvusHook' object in the plugin", err)
	}

	var ok bool
	hook, ok = h.(Hook)
	if !ok {
		exit("fail to convert the `Hook` interface", nil)
	}
}

func exit(errMsg string, err error) {
	logger.Panic("hook error, soPath: "+Params.HookCfg.SoPath, zap.String("msg", errMsg), zap.Error(err))
}

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	initHook()
	if err := hook.Init(Params.HookCfg.SoConfig); err != nil {
		exit("fail to init configs for the hook", err)
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var (
			fullMethod = info.FullMethod
			isMock     bool
			mockResp   interface{}
			realResp   interface{}
			realErr    error
			err        error
		)
		if globalMetaCache.SoFileFlag(false) {
			initHook()
		}

		if isMock, mockResp, err = hook.Mock(req, fullMethod); isMock {
			return mockResp, err
		}

		if err = hook.Before(req, fullMethod); err != nil {
			return nil, err
		}
		realResp, realErr = handler(ctx, req)
		if err = hook.After(realResp, realErr, fullMethod); err != nil {
			return nil, err
		}
		return realResp, realErr
	}
}
