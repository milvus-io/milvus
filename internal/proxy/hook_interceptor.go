package proxy

import (
	"context"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return HookInterceptor(ctx, req, GetCurUserFromContextOrDefault(ctx), info.FullMethod, handler)
	}
}

func HookInterceptor(ctx context.Context, req any, userName, fullMethod string, handler grpc.UnaryHandler) (interface{}, error) {
	hoo := hookutil.GetHook()
	var (
		newCtx   context.Context
		isMock   bool
		mockResp interface{}
		realResp interface{}
		realErr  error
		err      error
	)

	if isMock, mockResp, err = hoo.Mock(ctx, req, fullMethod); isMock {
		log.Info("hook mock", zap.String("user", userName),
			zap.String("full method", fullMethod), zap.Error(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookMock, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod)
		return mockResp, err
	}

	if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
		log.Warn("hook before error", zap.String("user", userName), zap.String("full method", fullMethod),
			zap.Any("request", req), zap.Error(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod)
		return nil, err
	}
	realResp, realErr = handler(newCtx, req)
	if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
		log.Warn("hook after error", zap.String("user", userName), zap.String("full method", fullMethod),
			zap.Any("request", req), zap.Error(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookAfter, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod)
		return nil, err
	}
	return realResp, realErr
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
