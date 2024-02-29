package proxy

import (
	"context"
	"fmt"
	"plugin"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type defaultHook struct{}

func (d defaultHook) VerifyAPIKey(key string) (string, error) {
	return "", errors.New("default hook, can't verify api key")
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
	path := Params.ProxyCfg.SoPath.GetValue()
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
	if err = hoo.Init(paramtable.GetHookParams().SoConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	paramtable.GetHookParams().WatchHookWithPrefix("watch_hook", "", func(event *config.Event) {
		log.Info("receive the hook refresh event", zap.Any("event", event))
		go func() {
			soConfig := paramtable.GetHookParams().SoConfig.GetValue()
			log.Info("refresh hook configs", zap.Any("config", soConfig))
			if err = hoo.Init(soConfig); err != nil {
				log.Panic("fail to init configs for the hook when refreshing", zap.Error(err))
			}
		}()
	})
	return nil
}

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	if hookError := initHook(); hookError != nil {
		logger.Error("hook error", zap.String("path", Params.ProxyCfg.SoPath.GetValue()), zap.Error(hookError))
		hoo = defaultHook{}
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
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
			log.Info("hook mock", zap.String("user", getCurrentUser(ctx)),
				zap.String("full method", fullMethod), zap.Error(err))
			metrics.ProxyHookFunc.WithLabelValues(metrics.HookMock, fullMethod).Inc()
			updateProxyFunctionCallMetric(fullMethod)
			return mockResp, err
		}

		if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
			log.Warn("hook before error", zap.String("user", getCurrentUser(ctx)), zap.String("full method", fullMethod),
				zap.Any("request", req), zap.Error(err))
			metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, fullMethod).Inc()
			updateProxyFunctionCallMetric(fullMethod)
			return nil, err
		}
		realResp, realErr = handler(newCtx, req)
		if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
			log.Warn("hook after error", zap.String("user", getCurrentUser(ctx)), zap.String("full method", fullMethod),
				zap.Any("request", req), zap.Error(err))
			metrics.ProxyHookFunc.WithLabelValues(metrics.HookAfter, fullMethod).Inc()
			updateProxyFunctionCallMetric(fullMethod)
			return nil, err
		}
		return realResp, realErr
	}
}

func updateProxyFunctionCallMetric(fullMethod string) {
	strs := strings.Split(fullMethod, "/")
	method := strs[len(strs)-1]
	if method == "" {
		return
	}
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, "", "").Inc()
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.FailLabel, "", "").Inc()
}

func getCurrentUser(ctx context.Context) string {
	username, err := GetCurUserFromContext(ctx)
	if err != nil {
		log.Warn("fail to get current user", zap.Error(err))
	}
	return username
}

// MockAPIHook is a mock hook for api key verification, ONLY FOR TEST
type MockAPIHook struct {
	defaultHook
	mockErr error
	apiUser string
}

func (m MockAPIHook) VerifyAPIKey(apiKey string) (string, error) {
	return m.apiUser, m.mockErr
}

func SetMockAPIHook(apiUser string, mockErr error) {
	if apiUser == "" && mockErr == nil {
		hoo = defaultHook{}
		return
	}
	hoo = MockAPIHook{
		mockErr: mockErr,
		apiUser: apiUser,
	}
}
