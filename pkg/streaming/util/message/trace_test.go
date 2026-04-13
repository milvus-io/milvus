package message

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func makeSpanContext(t *testing.T, tid, sid string, flags byte) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(tid)
	assert.NoError(t, err)
	spanID, err := trace.SpanIDFromHex(sid)
	assert.NoError(t, err)
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(flags),
		Remote:     true,
	})
}

func TestInjectExtractTraceContext_RoundTrip(t *testing.T) {
	sc := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	ctxIn := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	p := propertiesImpl{}
	InjectTraceContext(ctxIn, p)

	_, ok := p.Get(messageTraceContext)
	assert.True(t, ok, "InjectTraceContext should set _tc")

	ctxOut := ExtractTraceContext(context.Background(), p)
	got := trace.SpanContextFromContext(ctxOut)
	assert.True(t, got.IsValid())
	assert.Equal(t, sc.TraceID(), got.TraceID())
	assert.Equal(t, sc.SpanID(), got.SpanID())
	assert.Equal(t, sc.TraceFlags(), got.TraceFlags())
}

func TestInjectTraceContext_NoActiveSpan_NoOp(t *testing.T) {
	p := propertiesImpl{}
	InjectTraceContext(context.Background(), p) // no span in ctx
	_, ok := p.Get(messageTraceContext)
	assert.False(t, ok, "InjectTraceContext should be a no-op without an active span")
}

func TestExtractTraceContext_MissingKey_ReturnsOriginalCtx(t *testing.T) {
	p := propertiesImpl{}
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, p)
	assert.Equal(t, baseCtx, ctx)
}

func TestExtractTraceContext_MalformedValue_ReturnsOriginalCtx(t *testing.T) {
	p := propertiesImpl{messageTraceContext: "!!!not-base64!!!"}
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, p)
	assert.Equal(t, baseCtx, ctx)
}

func TestExtractSpanContextFromProperties_EmptyWhenMissing(t *testing.T) {
	p := propertiesImpl{}
	sc := ExtractSpanContextFromProperties(p)
	assert.False(t, sc.IsValid())
}

func TestExtractSpanContextFromProperties_Valid(t *testing.T) {
	orig := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	p := propertiesImpl{}
	InjectTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), orig), p)
	sc := ExtractSpanContextFromProperties(p)
	assert.True(t, sc.IsValid())
	assert.Equal(t, orig.TraceID(), sc.TraceID())
	assert.Equal(t, orig.SpanID(), sc.SpanID())
	assert.Equal(t, orig.TraceFlags(), sc.TraceFlags())
}

func TestInjectTraceContextIntoMap_RoundTrip(t *testing.T) {
	sc := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	m := map[string]string{"other": "v"}
	InjectTraceContextIntoMap(ctx, m)

	_, ok := m[messageTraceContext]
	assert.True(t, ok)

	p := propertiesImpl(m)
	ctxOut := ExtractTraceContext(context.Background(), p)
	got := trace.SpanContextFromContext(ctxOut)
	assert.True(t, got.IsValid())
	assert.Equal(t, sc.TraceID(), got.TraceID())
	assert.Equal(t, sc.SpanID(), got.SpanID())
	assert.Equal(t, sc.TraceFlags(), got.TraceFlags())
}
