package mlog

import (
	"context"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MetadataPrefix is the prefix for mlog fields in gRPC metadata.
// Type-encoded prefixes: "mlog-s-" for string, "mlog-i-" for int64.
const MetadataPrefix = "mlog-"

const (
	metadataPrefixString = MetadataPrefix + "s-"
	metadataPrefixInt64  = MetadataPrefix + "i-"
)

// UnaryServerInterceptor extracts propagated fields from incoming metadata
// and adds module field to the context.
func UnaryServerInterceptor(module string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = extractPropagated(ctx, String(keyModule, module))
		return handler(ctx, req)
	}
}

// StreamServerInterceptor extracts propagated fields from incoming metadata
// and adds module field to the context.
func StreamServerInterceptor(module string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractPropagated(ss.Context(), String(keyModule, module))
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
func extractPropagated(ctx context.Context, extraFields ...Field) context.Context {
	var fields []Field

	// Extract TraceID and SpanID from OpenTelemetry span context
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		fields = append(fields, String(keyTraceID, spanCtx.TraceID().String()))
	}
	if spanCtx.HasSpanID() {
		fields = append(fields, String(keySpanID, spanCtx.SpanID().String()))
	}

	// Extract propagated fields from gRPC metadata.
	// Format: "mlog-{t}-{key}" where {t} is 's' (string) or 'i' (int64).
	// Legacy format "mlog-{key}" (no type tag) falls back to string.
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for key, vals := range md {
			if len(vals) == 0 || !strings.HasPrefix(key, MetadataPrefix) {
				continue
			}
			rest := key[len(MetadataPrefix):] // after "mlog-"
			if len(rest) >= 2 && rest[1] == '-' {
				fieldKey := rest[2:]
				switch rest[0] {
				case 'i':
					if v, err := strconv.ParseInt(vals[0], 10, 64); err == nil {
						fields = append(fields, propagatedInt64Field(fieldKey, v))
					}
					continue
				case 's':
					fields = append(fields, propagatedStringField(fieldKey, vals[0]))
					continue
				}
			}
			// Legacy format without type tag — treat as string
			fields = append(fields, propagatedStringField(rest, vals[0]))
		}
	}

	// Append extra fields
	fields = append(fields, extraFields...)

	if len(fields) > 0 {
		return WithFields(ctx, fields...)
	}
	return ctx
}

// injectPropagated injects propagated fields into outgoing gRPC metadata.
// Keys are type-encoded: "mlog-s-<key>" for string, "mlog-i-<key>" for int64.
func injectPropagated(ctx context.Context) context.Context {
	lc := getLogContext(ctx)
	if len(lc.fieldKeys) == 0 {
		return ctx
	}

	var pairs []string
	for _, f := range lc.fieldKeys {
		if !isPropagatedField(f) {
			continue
		}
		switch f.Type {
		case zapcore.Int64Type:
			pairs = append(pairs, metadataPrefixInt64+f.Key, strconv.FormatInt(f.Integer, 10))
		default:
			pairs = append(pairs, metadataPrefixString+f.Key, getPropagatedValue(f))
		}
	}
	if len(pairs) == 0 {
		return ctx
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
