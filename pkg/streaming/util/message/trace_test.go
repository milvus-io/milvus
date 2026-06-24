package message

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
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

	msg := CreateTestEmptyInsertMesage(1, nil)
	InjectTraceContext(ctxIn, msg)

	_, ok := msg.Properties().Get(messageTraceContext)
	assert.True(t, ok, "InjectTraceContext should set _tc")

	ctxOut := ExtractTraceContext(context.Background(), msg)
	got := trace.SpanContextFromContext(ctxOut)
	assert.True(t, got.IsValid())
	assert.Equal(t, sc.TraceID(), got.TraceID())
	assert.Equal(t, sc.SpanID(), got.SpanID())
	assert.Equal(t, sc.TraceFlags(), got.TraceFlags())
}

func TestInjectTraceContext_NoActiveSpan_NoOp(t *testing.T) {
	msg := CreateTestEmptyInsertMesage(1, nil)
	InjectTraceContext(context.Background(), msg) // no span in ctx
	_, ok := msg.Properties().Get(messageTraceContext)
	assert.False(t, ok, "InjectTraceContext should be a no-op without an active span")
}

func TestInjectTraceContext_ExistingTraceContext_NoOp(t *testing.T) {
	existingSC := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	nextSC := makeSpanContext(t, "2122232425262728292a2b2c2d2e2f30", "3132333435363738", 0x01)

	msg := CreateTestEmptyInsertMesage(1, nil)
	InjectTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), existingSC), msg)
	InjectTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), nextSC), msg)

	got := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), msg))
	assert.True(t, got.IsValid())
	assert.Equal(t, existingSC.TraceID(), got.TraceID())
	assert.Equal(t, existingSC.SpanID(), got.SpanID())
	assert.Equal(t, existingSC.TraceFlags(), got.TraceFlags())
}

func TestExtractTraceContext_MissingKey_ReturnsOriginalCtx(t *testing.T) {
	msg := CreateTestEmptyInsertMesage(1, nil)
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, msg)
	assert.Equal(t, baseCtx, ctx)
}

func TestExtractTraceContext_MalformedValue_ReturnsOriginalCtx(t *testing.T) {
	msg := CreateTestEmptyInsertMesage(1, nil)
	msg.Properties().ToRawMap()[messageTraceContext] = "!!!not-base64!!!"
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, msg)
	assert.Equal(t, baseCtx, ctx)
}

func TestExtractSpanContext_EmptyWhenMissing(t *testing.T) {
	msg := CreateTestEmptyInsertMesage(1, nil)
	sc := extractSpanContext(msg)
	assert.False(t, sc.IsValid())
}

func TestExtractSpanContext_Valid(t *testing.T) {
	orig := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	msg := CreateTestEmptyInsertMesage(1, nil)
	InjectTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), orig), msg)
	sc := extractSpanContext(msg)
	assert.True(t, sc.IsValid())
	assert.Equal(t, orig.TraceID(), sc.TraceID())
	assert.Equal(t, orig.SpanID(), sc.SpanID())
	assert.Equal(t, orig.TraceFlags(), sc.TraceFlags())
}

func TestInjectTraceContext_BroadcastMutableMessage(t *testing.T) {
	sc := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	msg := NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast()
	InjectTraceContext(ctx, msg)

	_, ok := msg.Properties().Get(messageTraceContext)
	assert.True(t, ok)

	ctxOut := ExtractTraceContext(context.Background(), msg)
	got := trace.SpanContextFromContext(ctxOut)
	assert.True(t, got.IsValid())
	assert.Equal(t, sc.TraceID(), got.TraceID())
	assert.Equal(t, sc.SpanID(), got.SpanID())
	assert.Equal(t, sc.TraceFlags(), got.TraceFlags())
}
