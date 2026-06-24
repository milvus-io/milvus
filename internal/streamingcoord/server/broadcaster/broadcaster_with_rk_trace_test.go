//go:build test && dynamic

package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
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
	}
	_, err := b.Broadcast(ctx, msg)
	assert.NoError(t, err)

	// Verify _tc was injected on the msg observed by the inner broadcast call.
	sc := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.True(t, sc.IsValid(), "_tc should be present after Broadcast")
	assert.Equal(t, expectedTraceID, sc.TraceID())
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
	}
	_, err := b.Broadcast(callerCtx, msg)
	assert.NoError(t, err)

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
