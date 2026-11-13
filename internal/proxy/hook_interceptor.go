package proxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// UnaryServerHookInterceptor installs the request hook on every unary RPC.
//
// This is the extension seam the "Extension seam, see internal/util/hookutil"
// comments elsewhere in this package point at: a deployment form supplies a
// hook - compiled into the binary or loaded from proxy.soPath - and the hook
// may answer an RPC itself (Mock), inspect or rewrite it before it runs
// (Before), or see its result (After). With no hook installed the default one
// does nothing and every RPC behaves exactly as it did. How a hook is installed
// and configured is internal/util/hookutil, whose package comment names the
// design doc.
func UnaryServerHookInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return HookInterceptor(ctx, req, GetCurUserFromContextOrDefault(ctx), info.FullMethod, handler)
	}
}

// HookInterceptor does not count the request in ProxyFunctionCall: on gRPC
// UnaryRequestStatsInterceptor, and on REST v2 wrapperPost, wrap it and count
// every request once from the result the client gets, so counting here as well
// would double the total and, for a successful Mock answer, record a fail next
// to the success they record. CreateReplicateStream and DumpMessages, the
// streams that consult it by hand, are not counted in ProxyFunctionCall at all.
// ProxyHookFunc records the hook's verdict.
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
		return mockResp, hookError(err)
	}

	if newCtx, err = hoo.Before(ctx, req, fullMethod); err != nil {
		mlog.Warn(ctx, "hook before error", mlog.String("user", userName), mlog.String("full method", fullMethod),
			GetRequestFieldWithoutSensitiveInfo(req), mlog.Err(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookBefore, fullMethod).Inc()
		return nil, hookError(err)
	}
	realResp, realErr = handler(newCtx, req)
	if err = hoo.After(newCtx, realResp, realErr, fullMethod); err != nil {
		mlog.Warn(ctx, "hook after error", mlog.String("user", userName), mlog.String("full method", fullMethod),
			GetRequestFieldWithoutSensitiveInfo(req), mlog.Err(err))
		metrics.ProxyHookFunc.WithLabelValues(metrics.HookAfter, fullMethod).Inc()
		observeResourceGroupRequest(newCtx, fullMethod, req, nil, hookError(err))
		return nil, hookError(err)
	}
	observeResourceGroupRequest(newCtx, fullMethod, req, realResp, realErr)
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
func hookError(err error) error {
	if err == nil {
		return nil
	}
	// NOTE: don't use the merr, because it will cause the wrong retry behavior in the sdk
	return status.Error(codes.InvalidArgument, "detail: "+err.Error())
}
