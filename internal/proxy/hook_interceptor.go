package proxy

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
		mlog.Info(ctx, "hook mock", mlog.String("user", userName),
			mlog.String("full method", fullMethod), mlog.Err(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookMock, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod, err)
		return mockResp, hookError(err)
	}

	if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
		mlog.Warn(ctx, "hook before error", mlog.String("user", userName), mlog.String("full method", fullMethod),
			GetRequestFieldWithoutSensitiveInfo(req), mlog.Err(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod, err)
		if responder, ok := hoo.(refusalResponder); ok {
			if resp, ok := responder.RefusalResponse(fullMethod, err); ok {
				return resp, nil
			}
		}
		return nil, hookError(err)
	}
	realResp, realErr = handler(newCtx, req)
	if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
		mlog.Warn(ctx, "hook after error", mlog.String("user", userName), mlog.String("full method", fullMethod),
			GetRequestFieldWithoutSensitiveInfo(req), mlog.Err(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookAfter, fullMethod).Inc()
		updateProxyFunctionCallMetric(fullMethod, err)
		return nil, hookError(err)
	}
	return realResp, realErr
}

// hookError is how a hook's refusal reaches the client.
//
// A refusal that must carry a classification the caller can act on does not
// come through here at all: the hook answers it from Mock, with the RPC's own
// response carrying merr.Status, which is how every milvus handler reports a
// refusal and what an SDK surfaces immediately. An error returned here can
// only become a gRPC status, and a bare error becomes codes.Unknown, which
// clients retry - the reason the original comment gives for not using merr.
// refusalResponder is optionally implemented by a hook whose Before refuses
// requests. When it answers a method with a response, the refusal travels in
// that response's Status like any other Milvus error - carrying its error code
// and reason to the client - instead of as the bare InvalidArgument below,
// which every SDK reads as a transport-level failure with no classification.
type refusalResponder interface {
	RefusalResponse(fullMethod string, err error) (resp any, ok bool)
}

func hookError(err error) error {
	if err == nil {
		return nil
	}
	// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
	return status.Error(codes.InvalidArgument, "detail: "+err.Error())
}

func updateProxyFunctionCallMetric(fullMethod string, err error) {
	strs := strings.Split(fullMethod, "/")
	method := strs[len(strs)-1]
	if method == "" {
		return
	}
	status, cause := failMetricLabel(err)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, metrics.TotalLabel, metrics.CauseNA, "", "").Inc()
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method, status, cause, "", "").Inc()
}
