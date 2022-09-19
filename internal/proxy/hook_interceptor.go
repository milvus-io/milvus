package proxy

import (
	"context"
	"fmt"
	"plugin"
	"strings"

	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type Hook interface {
	Init(params map[string]string) error
	Mock(req interface{}, fullMethod string) (bool, interface{}, error)
	Before(req interface{}, fullMethod string) error
	After(result interface{}, err error, fullMethod string) error
	Release()
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

func (d defaultHook) Release() {}

var hook Hook
var hookPrefix string
var hookVersion = 0

func initHook() {
	if Params.HookCfg.SoPath == "" {
		hook = defaultHook{}
		return
	}

	logger.Debug("start to load plugin", zap.String("path", Params.HookCfg.SoPath))
	path := Params.HookCfg.SoPath
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
	hook, ok = h.(Hook)
	if !ok {
		exit("fail to convert the `Hook` interface", nil)
	}
	if err = hook.Init(Params.HookCfg.SoConfig); err != nil {
		exit("fail to init configs for the hook", err)
	}
}

func exit(errMsg string, err error) {
	logger.Panic("hook error", zap.String("path", Params.HookCfg.SoPath), zap.String("msg", errMsg), zap.Error(err))
}

func reloadPlugin() {
	originHook := hook
	defer func() {
		if v := recover(); v != nil {
			logger.Warn("fail to reload plugin", zap.Int("version", hookVersion),
				zap.String("path", Params.HookCfg.SoPath), zap.Any("err", v))
			hook = originHook
		}
	}()
	hookVersion++
	Params.HookCfg.SoPath = fmt.Sprintf("%sv%d.so", hookPrefix, hookVersion)
	logger.Debug("start to reload plugin", zap.Int("version", hookVersion),
		zap.String("path", Params.HookCfg.SoPath))
	initHook()
	originHook.Release()
}

func extractHookPrefix() {
	path := Params.HookCfg.SoPath
	index := strings.Index(path, fmt.Sprintf("v%d.so", hookVersion))
	if index == -1 {
		exit("invalid so path", nil)
	}
	hookPrefix = path[0:index]
}

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	extractHookPrefix()
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
		if globalMetaCache.SoFileFlag(false) {
			reloadPlugin()
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
