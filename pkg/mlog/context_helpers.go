package mlog

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// WithTraceID returns a context with trace ID attached to logging fields.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return WithFields(ctx, FieldTraceID(traceID))
}

// WithReqID returns a context with request ID attached to logging fields.
func WithReqID(ctx context.Context, reqID int64) context.Context {
	return WithFields(ctx, Int64("req_id", reqID))
}

// WithModule returns a context with module name attached to logging fields.
func WithModule(ctx context.Context, module string) context.Context {
	return WithFields(ctx, FieldModule(module))
}

// NewIntentContext creates a root context with intent information and a tracing span.
func NewIntentContext(name string, intent string) (context.Context, trace.Span) {
	intentCtx, initSpan := otel.Tracer(name).Start(context.Background(), intent)
	intentCtx = WithFields(intentCtx,
		String("role", name),
		String("intent", intent),
		FieldTraceID(initSpan.SpanContext().TraceID().String()),
	)
	return intentCtx, initSpan
}
