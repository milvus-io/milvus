package message

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

// InjectTraceContext writes the current span context into msg Properties
// under the reserved key _tc as a base64-encoded marshaled TraceContextHeader.
// No-op when no active / valid span is present on ctx.
func InjectTraceContext(ctx context.Context, p Properties) {
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

// InjectTraceContextIntoMap is a variant operating on a raw map[string]string,
// used by CDC / replicate paths that work on proto-decoded Properties maps.
func InjectTraceContextIntoMap(ctx context.Context, m map[string]string) {
	if m == nil {
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
	m[messageTraceContext] = val
}

// ExtractTraceContext reads _tc from Properties and returns ctx with the
// extracted remote span context attached. Returns ctx unchanged when _tc is
// absent or malformed — trace propagation is never a correctness dependency.
func ExtractTraceContext(ctx context.Context, p RProperties) context.Context {
	sc := ExtractSpanContextFromProperties(p)
	if !sc.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// ExtractSpanContextFromProperties returns the raw SpanContext stored in
// Properties, or an invalid zero SpanContext when _tc is absent or malformed.
// Used by call sites that need a Link (rather than a parent) to the upstream
// span — e.g. CDC replication.
func ExtractSpanContextFromProperties(p RProperties) trace.SpanContext {
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
