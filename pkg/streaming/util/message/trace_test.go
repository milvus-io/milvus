package message

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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

func TestOverwriteTraceContext_ExistingTraceContext_Replaces(t *testing.T) {
	existingSC := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	nextSC := makeSpanContext(t, "2122232425262728292a2b2c2d2e2f30", "3132333435363738", 0x01)

	msg := CreateTestEmptyInsertMesage(1, nil)
	InjectTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), existingSC), msg)
	OverwriteTraceContext(trace.ContextWithRemoteSpanContext(context.Background(), nextSC), msg)

	got := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), msg))
	assert.True(t, got.IsValid())
	assert.Equal(t, nextSC.TraceID(), got.TraceID())
	assert.Equal(t, nextSC.SpanID(), got.SpanID())
	assert.Equal(t, nextSC.TraceFlags(), got.TraceFlags())
}

func TestTraceContext_TimeTickMessage_NoOp(t *testing.T) {
	sc := makeSpanContext(t, "0102030405060708090a0b0c0d0e0f10", "1112131415161718", 0x01)
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	msg, err := NewTimeTickMessageBuilderV1().
		WithHeader(&TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		BuildMutable()
	assert.NoError(t, err)
	msg.WithTimeTick(100).WithLastConfirmedUseMessageID()
	spanCtx, span := StartSpanForMessage(ctx, msg, SpanNameWALAppend)
	assert.Equal(t, ctx, spanCtx)
	assert.NotNil(t, span)
	assert.False(t, span.IsRecording())
	span.RecordError(assert.AnError)
	span.SetStatus(0, "")
	span.End()

	InjectTraceContext(ctx, msg)
	OverwriteTraceContext(ctx, msg)
	_, ok := msg.Properties().Get(messageTraceContext)
	assert.False(t, ok)
}

func TestStartSpanForMessage_AddsMessageAttributes(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	msg := CreateTestEmptyInsertMesage(1, nil)
	msg.WithTimeTick(100).WithTxnContext(TxnContext{TxnID: 42})
	_, span := StartSpanForMessage(context.Background(), msg, SpanNameWALAppend)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, SpanNameWALAppend, spans[0].Name)
	assertSpanAttribute(t, spans[0].Attributes, spanAttrMessageType, MessageTypeInsert.String())
	assertSpanAttribute(t, spans[0].Attributes, spanAttrVChannel, "v1")
	assertSpanInt64Attribute(t, spans[0].Attributes, spanAttrTimeTick, 100)
	assertSpanInt64Attribute(t, spans[0].Attributes, spanAttrTxnID, 42)
	assertSpanBoolAttribute(t, spans[0].Attributes, spanAttrReplicate, false)

	msgID := testMessageID("1")
	msg.WithReplicateHeader(&ReplicateHeader{
		ClusterID:              "cluster",
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               100,
		VChannel:               "v1",
	})
	_, span = StartSpanForMessage(context.Background(), msg, SpanNameWALAppend)
	span.End()

	spans = exporter.GetSpans()
	require.Len(t, spans, 2)
	assertSpanBoolAttribute(t, spans[1].Attributes, spanAttrReplicate, true)
}

func TestStartSpanForMessage_AddsBroadcastAttributes(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	msg := NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast().
		OverwriteBroadcastHeader(11)
	_, span := StartSpanForMessage(context.Background(), msg, SpanNameWALBroadcast)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, SpanNameWALBroadcast, spans[0].Name)
	assertSpanAttribute(t, spans[0].Attributes, spanAttrMessageType, MessageTypeDropCollection.String())
	assertSpanInt64Attribute(t, spans[0].Attributes, spanAttrBroadcastID, 11)
	assertSpanStringSliceAttribute(t, spans[0].Attributes, spanAttrBroadcastVChannels, []string{"v1", "v2"})
}

func TestImmutableTxnMessageBuildCopiesCommitTraceContext(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), SpanNameWALConsume)
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	sourceSpan.End()

	txnCtx := TxnContext{TxnID: 1}
	lastConfirmed := testMessageID("1")
	beginID := testMessageID("2")
	begin := NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&BeginTxnMessageHeader{}).
		WithBody(&BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(100).
		WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(beginID)
	builder := NewImmutableTxnMessageBuilder(MustAsImmutableBeginTxnMessageV2(begin))

	bodyID := testMessageID("3")
	body := CreateTestEmptyInsertMesage(1, nil).
		WithTxnContext(txnCtx).
		WithTimeTick(101).
		WithLastConfirmed(lastConfirmed).
		IntoImmutableMessage(bodyID)
	builder.Add(body)

	commitID := testMessageID("4")
	commit := NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&CommitTxnMessageHeader{}).
		WithBody(&CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(102).
		WithLastConfirmed(lastConfirmed)
	InjectTraceContext(sourceCtx, commit)

	txn, err := builder.Build(MustAsImmutableCommitTxnMessageV2(commit.IntoImmutableMessage(commitID)))
	require.NoError(t, err)

	txnSC := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), txn))
	assert.Equal(t, sourceSC.TraceID(), txnSC.TraceID())
	assert.Equal(t, sourceSC.SpanID(), txnSC.SpanID())
}

func TestExtractTraceContext_MissingKey_ReturnsOriginalCtx(t *testing.T) {
	msg := CreateTestEmptyInsertMesage(1, nil)
	baseCtx := context.Background()
	ctx := ExtractTraceContext(baseCtx, msg)
	assert.Equal(t, baseCtx, ctx)
}

func assertSpanAttribute(t *testing.T, attrs []attribute.KeyValue, key string, value string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			assert.Equal(t, value, attr.Value.AsString())
			return
		}
	}
	t.Fatalf("missing span attribute %q", key)
}

func assertSpanBoolAttribute(t *testing.T, attrs []attribute.KeyValue, key string, value bool) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			assert.Equal(t, value, attr.Value.AsBool())
			return
		}
	}
	t.Fatalf("missing span attribute %q", key)
}

func assertSpanInt64Attribute(t *testing.T, attrs []attribute.KeyValue, key string, value int64) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			assert.Equal(t, value, attr.Value.AsInt64())
			return
		}
	}
	t.Fatalf("missing span attribute %q", key)
}

func assertSpanStringSliceAttribute(t *testing.T, attrs []attribute.KeyValue, key string, value []string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			assert.ElementsMatch(t, value, attr.Value.AsStringSlice())
			return
		}
	}
	t.Fatalf("missing span attribute %q", key)
}

type testMessageID string

func (id testMessageID) WALName() WALName {
	return WALNameTest
}

func (id testMessageID) LT(MessageID) bool {
	return false
}

func (id testMessageID) LTE(MessageID) bool {
	return true
}

func (id testMessageID) EQ(other MessageID) bool {
	return id.String() == other.String()
}

func (id testMessageID) Marshal() string {
	return string(id)
}

func (id testMessageID) IntoProto() *commonpb.MessageID {
	return &commonpb.MessageID{
		WALName: commonpb.WALName(id.WALName()),
		Id:      id.Marshal(),
	}
}

func (id testMessageID) String() string {
	return string(id)
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
