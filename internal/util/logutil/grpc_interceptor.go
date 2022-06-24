package logutil

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/trace"
	"google.golang.org/grpc"
)

// UnaryTraceLoggerInterceptor adds a traced logger in unary rpc call ctx
func UnaryTraceLoggerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	traceID, _, _ := trace.InfoFromContext(ctx)
	if traceID != "" {
		newctx := log.WithTraceID(ctx, traceID)
		return handler(newctx, req)
	}
	return handler(ctx, req)
}

// StreamTraceLoggerInterceptor add a traced logger in stream rpc call ctx
func StreamTraceLoggerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	traceID, _, _ := trace.InfoFromContext(ctx)
	if traceID != "" {
		newctx := log.WithTraceID(ctx, traceID)
		wrappedStream := grpc_middleware.WrapServerStream(ss)
		wrappedStream.WrappedContext = newctx
		return handler(srv, wrappedStream)
	}
	return handler(srv, ss)
}
