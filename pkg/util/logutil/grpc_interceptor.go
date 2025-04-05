package logutil

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const (
	logLevelRPCMetaKey = "log_level"
	clientRequestIDKey = "client_request_id"
)

// UnaryTraceLoggerInterceptor adds a traced logger in unary rpc call ctx
func UnaryTraceLoggerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	newctx := withLevelAndTrace(ctx)
	return handler(newctx, req)
}

// StreamTraceLoggerInterceptor add a traced logger in stream rpc call ctx
func StreamTraceLoggerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	newctx := withLevelAndTrace(ctx)
	wrappedStream := grpc_middleware.WrapServerStream(ss)
	wrappedStream.WrappedContext = newctx
	return handler(srv, wrappedStream)
}

func withLevelAndTrace(ctx context.Context) context.Context {
	newctx := ctx
	var traceID trace.TraceID
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		levels := md.Get(logLevelRPCMetaKey)
		// get log level
		if len(levels) >= 1 {
			level := zapcore.DebugLevel
			if err := level.UnmarshalText([]byte(levels[0])); err != nil {
				newctx = ctx
			} else {
				switch level {
				case zapcore.DebugLevel:
					newctx = log.WithDebugLevel(ctx)
				case zapcore.InfoLevel:
					newctx = log.WithInfoLevel(ctx)
				case zapcore.WarnLevel:
					newctx = log.WithWarnLevel(ctx)
				case zapcore.ErrorLevel:
					newctx = log.WithErrorLevel(ctx)
				case zapcore.FatalLevel:
					newctx = log.WithFatalLevel(ctx)
				default:
					newctx = ctx
				}
			}
			// inject log level to outgoing meta
			newctx = metadata.AppendToOutgoingContext(newctx, logLevelRPCMetaKey, level.String())
		}
		// client request id
		requestID := md.Get(clientRequestIDKey)
		if len(requestID) >= 1 {
			// inject traceid in order to pass client request id
			newctx = metadata.AppendToOutgoingContext(newctx, clientRequestIDKey, requestID[0])
			var err error
			// if client_request_id is a valid traceID, use traceID path
			traceID, err = trace.TraceIDFromHex(requestID[0])
			if err != nil {
				// set request id to custom field
				newctx = log.WithFields(newctx, zap.String(clientRequestIDKey, requestID[0]))
			}
		}
	}
	// traceID not valid, generate a new one
	if !traceID.IsValid() {
		traceID = trace.SpanContextFromContext(newctx).TraceID()
	}
	if traceID.IsValid() {
		newctx = log.WithTraceID(newctx, traceID.String())
	}
	return newctx
}
