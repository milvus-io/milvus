//go:build test && dynamic

package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestBroadcasterWithRK_InjectsTraceContextBeforeTaskPersist(t *testing.T) {
	defer mockey.UnPatchAll()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Stub the inner broadcast call to capture the msg Properties after injection.
	var capturedMsg message.BroadcastMutableMessage
	resourceKey := message.NewExclusiveCollectionNameResourceKey("db", "collection")
	mockey.Mock((*broadcastTaskManager).broadcast).To(
		func(_ *broadcastTaskManager, _ context.Context, msg message.BroadcastMutableMessage, _ uint64, _ *lockGuards) (*types.BroadcastAppendResult, error) {
			capturedMsg = msg
			return &types.BroadcastAppendResult{}, nil
		}).Build()

	msg := buildTestBroadcastMessageForTrace(t)

	// Caller ctx carries a traceable span.
	ctx, span := otel.Tracer("test").Start(context.Background(), "caller.ddl")
	expectedTraceID := trace.SpanContextFromContext(ctx).TraceID()
	defer span.End()

	b := &broadcasterWithRK{
		broadcaster: &broadcastTaskManager{},
		broadcastID: 11,
		guards:      buildTestLockGuards(resourceKey),
	}
	_, err := b.Broadcast(ctx, msg)
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var broadcastSpan tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALBroadcast {
			broadcastSpan = s
			break
		}
	}
	assert.Equal(t, message.SpanNameWALBroadcast, broadcastSpan.Name, "wal.broadcast span should be emitted")
	assert.Equal(t, expectedTraceID, broadcastSpan.SpanContext.TraceID())
	assertSpanAttribute(t, broadcastSpan.Attributes, "message.type", message.MessageTypeDropCollection.String())
	assertSpanInt64Attribute(t, broadcastSpan.Attributes, "broadcast.id", 11)
	assertSpanStringSliceAttribute(t, broadcastSpan.Attributes, "broadcast.vchannels", []string{"v1", "v2"})

	// Verify _tc was injected on the msg observed by the inner broadcast call.
	sc := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.True(t, sc.IsValid(), "_tc should be present after Broadcast")
	assert.Equal(t, broadcastSpan.SpanContext.TraceID(), sc.TraceID())
	assert.Equal(t, broadcastSpan.SpanContext.SpanID(), sc.SpanID())
	assert.Equal(t, uint64(11), capturedMsg.BroadcastHeader().BroadcastID)
	assert.True(t, capturedMsg.BroadcastHeader().ResourceKeys.Contain(resourceKey))
}

func TestBroadcasterWithRK_KeepsExistingTraceContext(t *testing.T) {
	defer mockey.UnPatchAll()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	var capturedMsg message.BroadcastMutableMessage
	mockey.Mock((*broadcastTaskManager).broadcast).To(
		func(_ *broadcastTaskManager, _ context.Context, msg message.BroadcastMutableMessage, _ uint64, _ *lockGuards) (*types.BroadcastAppendResult, error) {
			capturedMsg = msg
			return &types.BroadcastAppendResult{}, nil
		}).Build()

	msg := buildTestBroadcastMessageForTrace(t)
	originCtx, originSpan := otel.Tracer("test").Start(context.Background(), "origin.ddl")
	originSC := trace.SpanContextFromContext(originCtx)
	originSpan.End()
	message.InjectTraceContext(originCtx, msg)

	callerCtx, callerSpan := otel.Tracer("test").Start(context.Background(), "caller.ddl")
	defer callerSpan.End()

	b := &broadcasterWithRK{
		broadcaster: &broadcastTaskManager{},
		broadcastID: 11,
		guards:      buildTestLockGuards(message.NewExclusiveCollectionNameResourceKey("db", "collection")),
	}
	_, err := b.Broadcast(callerCtx, msg)
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var broadcastSpan tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALBroadcast {
			broadcastSpan = s
			break
		}
	}
	assert.Equal(t, message.SpanNameWALBroadcast, broadcastSpan.Name, "wal.broadcast span should be emitted")

	sc := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.True(t, sc.IsValid(), "_tc should still be present after Broadcast")
	assert.Equal(t, originSC.TraceID(), sc.TraceID())
	assert.Equal(t, originSC.SpanID(), sc.SpanID())
}

// buildTestBroadcastMessageForTrace builds a minimal BroadcastMutableMessage for tests.
func buildTestBroadcastMessageForTrace(t *testing.T) message.BroadcastMutableMessage {
	t.Helper()
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&messagespb.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"v1", "v2"}).
		BuildBroadcast()
	if err != nil {
		t.Fatalf("failed to build broadcast message: %v", err)
	}
	return msg.OverwriteBroadcastHeader(0)
}

func buildTestLockGuards(keys ...message.ResourceKey) *lockGuards {
	guards := &lockGuards{}
	for _, key := range keys {
		guards.append(&lockGuard{key: key})
	}
	return guards
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
