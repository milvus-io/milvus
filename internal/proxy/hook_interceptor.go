package proxy

import (
	"context"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
		if err != nil {
			// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
			err = status.Error(codes.InvalidArgument, "detail: "+err.Error())
		}
		return mockResp, err
	}

	if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
		log.Warn("hook before error", zap.String("user", userName), zap.String("full method", fullMethod),
			zap.Any("request", req), zap.Error(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod)
		// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
		return nil, status.Error(codes.InvalidArgument, "detail: "+err.Error())
	}
	realResp, realErr = handler(newCtx, req)
	if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
		log.Warn("hook after error", zap.String("user", userName), zap.String("full method", fullMethod),
			zap.Any("request", req), zap.Error(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookAfter, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod)
		// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
		return nil, status.Error(codes.InvalidArgument, "detail: "+err.Error())
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
