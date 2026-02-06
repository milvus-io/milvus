// Package grpc provides gRPC interceptors for mlog field propagation.
package grpc

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/mlog"
)

// MetadataPrefix is the prefix for mlog fields in gRPC metadata.
const MetadataPrefix = "mlog-"

// UnaryServerInterceptor extracts propagated fields from incoming metadata
// and adds module field to the context.
func UnaryServerInterceptor(module string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extractPropagated(ctx, mlog.String(mlog.KeyModule, module))
		return handler(ctx, req)
	}
}

// StreamServerInterceptor extracts propagated fields from incoming metadata
// and adds module field to the context.
func StreamServerInterceptor(module string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractPropagated(ss.Context(), mlog.String(mlog.KeyModule, module))
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

// UnaryClientInterceptor injects propagated fields into outgoing metadata.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = injectPropagated(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor injects propagated fields into outgoing metadata.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = injectPropagated(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// extractPropagated extracts mlog fields from incoming gRPC metadata and trace context.
// Extracted fields are marked as propagated so they will be forwarded in subsequent RPC calls.
// TraceID and SpanID are extracted from OpenTelemetry span context.
// Additional fields can be passed to be added in the same WithFields call.
func extractPropagated(ctx context.Context, extraFields ...mlog.Field) context.Context {
	var fields []mlog.Field

	// Extract TraceID and SpanID from OpenTelemetry span context
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		fields = append(fields, mlog.String(mlog.KeyTraceID, spanCtx.TraceID().String()))
	}
	if spanCtx.HasSpanID() {
		fields = append(fields, mlog.String(mlog.KeySpanID, spanCtx.SpanID().String()))
	}

	// Extract propagated fields from gRPC metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for key, vals := range md {
			if strings.HasPrefix(key, MetadataPrefix) && len(vals) > 0 {
				fieldKey := strings.TrimPrefix(key, MetadataPrefix)
				// Use PropagatedString to mark these fields for further propagation
				fields = append(fields, mlog.PropagatedString(fieldKey, vals[0]))
			}
		}
	}

	// Append extra fields
	fields = append(fields, extraFields...)

	if len(fields) > 0 {
		return mlog.WithFields(ctx, fields...)
	}
	return ctx
}

// injectPropagated injects propagated fields into outgoing gRPC metadata.
func injectPropagated(ctx context.Context) context.Context {
	props := mlog.GetPropagated(ctx)
	if len(props) == 0 {
		return ctx
	}

	pairs := make([]string, 0, len(props)*2)
	for k, v := range props {
		pairs = append(pairs, MetadataPrefix+k, v)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
