package tracer

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// SetupSpan add span into ctx values.
func SetupSpan(ctx context.Context, span trace.Span) context.Context {
	return trace.ContextWithSpan(ctx, span)
}

// Propagate passes span context into a new ctx with different lifetime.
func Propagate(ctx, newRoot context.Context) context.Context {
	spanCtx := trace.SpanContextFromContext(ctx)

	return trace.ContextWithSpanContext(newRoot, spanCtx)
}
