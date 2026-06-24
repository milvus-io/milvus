package message

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// InjectTraceContext writes the current span context into msg under the
// reserved key _tc as a base64-encoded marshaled TraceContextHeader.
// No-op when _tc already exists or no active / valid span is present on ctx.
func InjectTraceContext(ctx context.Context, msg TraceContextInjector) {
	if msg == nil {
		return
	}
	msg.WithTraceContext(ctx)
}

func injectTraceContext(ctx context.Context, p Properties) {
	if p.Exist(messageTraceContext) {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return
	}
	val, ok := encodeTraceContextHeader(sc)
	if !ok {
		return
	}
	p.Set(messageTraceContext, val)
}

// ExtractTraceContext reads _tc from msg and returns ctx with the
// extracted remote span context attached. Returns ctx unchanged when _tc is
// absent or malformed — trace propagation is never a correctness dependency.
func ExtractTraceContext(ctx context.Context, msg BasicMessage) context.Context {
	sc := extractSpanContext(msg)
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

func extractSpanContext(msg BasicMessage) trace.SpanContext {
	if msg == nil {
		return trace.SpanContext{}
	}
	return extractSpanContextFromProperties(msg.Properties())
}

func extractSpanContextFromProperties(p RProperties) trace.SpanContext {
	value, ok := p.Get(messageTraceContext)
	if !ok {
		return trace.SpanContext{}
	}
	hdr := &messagespb.TraceContextHeader{}
	if err := DecodeProto(value, hdr); err != nil {
		return trace.SpanContext{}
	}
	if len(hdr.GetTraceId()) != 16 || len(hdr.GetSpanId()) != 8 {
		return trace.SpanContext{}
	}
	var tid trace.TraceID
	var sid trace.SpanID
	copy(tid[:], hdr.GetTraceId())
	copy(sid[:], hdr.GetSpanId())
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.TraceFlags(hdr.GetFlags()),
		Remote:     true,
	})
}

// encodeTraceContextHeader returns the base64-encoded TraceContextHeader for
// the given span context. ok=false when the proto marshal fails.
func encodeTraceContextHeader(sc trace.SpanContext) (string, bool) {
	tid := sc.TraceID()
	sid := sc.SpanID()
	hdr := &messagespb.TraceContextHeader{
		TraceId: tid[:],
		SpanId:  sid[:],
		Flags:   uint32(sc.TraceFlags()),
	}
	val, err := EncodeProto(hdr)
	if err != nil {
		return "", false
	}
	return val, true
}
