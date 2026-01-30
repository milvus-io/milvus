package tracer

import (
	"context"
	"crypto/rand"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	clientRequestIDKeyLegacy = "client-request-id"
	clientRequestIDKey       = "client_request_id"
)

type TraceContext struct {
	propagation.TraceContext
}

func (tc TraceContext) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	tc.TraceContext.Inject(ctx, carrier)
}

func (tc TraceContext) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	ctx = tc.TraceContext.Extract(ctx, carrier)
	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx
	}
	// check if there is a legacy trace context
	for _, key := range []string{clientRequestIDKeyLegacy, clientRequestIDKey} {
		if val := carrier.Get(key); len(val) > 0 {
			traceID, err := trace.TraceIDFromHex(val)
			if err != nil {
				return ctx
			}
			var spanID trace.SpanID
			_, _ = rand.Read(spanID[:])
			sc := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    traceID,
				SpanID:     spanID,
				TraceFlags: trace.FlagsSampled,
				Remote:     true,
			})
			return trace.ContextWithSpanContext(ctx, sc)
		}
	}
	return ctx
}
