package message

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

var noopSpan trace.Span = noop.Span{}

const (
	tracerName = "milvus.streaming.wal"

	spanAttrMessageType = "message.type"
	spanAttrVChannel    = "message.vchannel"
	spanAttrTimeTick    = "message.timetick"
	spanAttrReplicate   = "message.replicate"
	spanAttrTxnID       = "txn.id"

	spanAttrBroadcastID        = "broadcast.id"
	spanAttrBroadcastVChannels = "broadcast.vchannels"

	SpanNameWALAutocommit      = "wal.autocommit"
	SpanNameWALTxn             = "wal.txn"
	SpanNameWALBroadcast       = "wal.broadcast"
	SpanNameWALAppend          = "wal.append"
	SpanNameWALAppendImpl      = "wal.appendimpl"
	SpanNameWALDistAppend      = "wal.dist_append"
	SpanNameWALConsume         = "wal.consume"
	SpanNameWALDistConsume     = "wal.dist_consume"
	SpanNameReplicateSecondary = "replicate.secondary"
	SpanNameWALBCCallback      = "wal.bc_callback"
)

func StartSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, spanName)
}

func StartSpanForMessage(ctx context.Context, msg BasicMessage, spanName string) (context.Context, trace.Span) {
	if !shouldTraceMessage(msg) {
		return ctx, noopSpan
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName)
	if !span.IsRecording() {
		return ctx, span
	}
	span.SetAttributes(buildMessageSpanAttributes(msg)...)
	return ctx, span
}

func buildMessageSpanAttributes(msg BasicMessage) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(spanAttrMessageType, msg.MessageType().String()),
		attribute.String(spanAttrVChannel, getVChannel(msg)),
		attribute.Bool(spanAttrReplicate, isReplicateMessage(msg)),
	}
	if msg.Properties().Exist(messageTimeTick) {
		attrs = append(attrs, attribute.Int64(spanAttrTimeTick, int64(msg.TimeTick())))
	}
	if txnCtx := msg.TxnContext(); txnCtx != nil {
		attrs = append(attrs, attribute.Int64(spanAttrTxnID, int64(txnCtx.TxnID)))
	}
	if broadcastHeader := msg.BroadcastHeader(); broadcastHeader != nil {
		attrs = append(attrs,
			attribute.Int64(spanAttrBroadcastID, int64(broadcastHeader.BroadcastID)),
			attribute.StringSlice(spanAttrBroadcastVChannels, broadcastHeader.VChannels),
		)
	}
	return attrs
}

func shouldTraceMessage(msg BasicMessage) bool {
	return msg != nil && msg.MessageType() != MessageTypeTimeTick
}

func getVChannel(msg BasicMessage) string {
	if vchannel, ok := msg.Properties().Get(messageVChannel); ok {
		return vchannel
	}
	return ""
}

func isReplicateMessage(msg BasicMessage) bool {
	return msg.Properties().Exist(messageReplicateMesssageHeader)
}

type traceContextInjector interface {
	injectTraceContext(context.Context)
}

type traceContextOverwriter interface {
	overwriteTraceContext(context.Context)
}

// InjectTraceContext writes the current span context into msg under the
// reserved key _tc as a base64-encoded marshaled TraceContextHeader.
// No-op when _tc already exists or no active / valid span is present on ctx.
func InjectTraceContext(ctx context.Context, msg BasicMessage) {
	if !shouldTraceMessage(msg) {
		return
	}
	if writer, ok := msg.(traceContextInjector); ok {
		writer.injectTraceContext(ctx)
	}
}

// OverwriteTraceContext writes the current span context into msg under the
// reserved key _tc even when a trace context already exists.
func OverwriteTraceContext(ctx context.Context, msg BasicMessage) {
	if !shouldTraceMessage(msg) {
		return
	}
	if writer, ok := msg.(traceContextOverwriter); ok {
		writer.overwriteTraceContext(ctx)
	}
}

func injectTraceContext(ctx context.Context, p Properties) {
	if p.Exist(messageTraceContext) {
		return
	}
	overwriteTraceContext(ctx, p)
}

func overwriteTraceContext(ctx context.Context, p Properties) {
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
