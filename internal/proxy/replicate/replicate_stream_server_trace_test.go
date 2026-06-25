//go:build test && dynamic

package replicate

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
)

// TestHandleReplicateMessage_OpensWalReplicateAppendSpan verifies that
// handleReplicateMessage extracts the replicated message trace context,
// opens a "replicate.secondary" child span, and keeps the source message
// trace context in the local mutable message before append.
func TestHandleReplicateMessage_OpensWalReplicateAppendSpan(t *testing.T) {
	defer mockey.UnPatchAll()

	// Set up an in-memory OTel exporter and make it the global provider.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Build a source-side traced context and capture the expected trace ID.
	sourceCtx, sourceSpan := otel.Tracer("test").Start(context.Background(), "source.wal.append")
	sourceSC := trace.SpanContextFromContext(sourceCtx)
	expectedTraceID := sourceSC.TraceID()
	sourceSpan.End()

	// Build a replicate message proto with _tc carried by the immutable message.
	reqMsg := buildTraceTestReplicateMsgProto(t, sourceCtx)

	req := &milvuspb.ReplicateRequest_ReplicateMessage{
		ReplicateMessage: &milvuspb.ReplicateMessage{
			SourceClusterId: "cluster-a",
			Message:         reqMsg,
		},
	}

	// Capture the ctx passed to Append.
	var capturedCtx context.Context
	var capturedMsg message.ReplicateMutableMessage

	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.ReplicateMutableMessage) (*types.AppendResult, error) {
			capturedCtx = ctx
			capturedMsg = msg
			return &types.AppendResult{TimeTick: 1}, nil
		})
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)

	// Build a minimal ReplicateStreamServer using the existing package helper.
	ctx := createContextWithClusterID("cluster-a")
	mockStream := newMockReplicateStreamServer(ctx)
	server, err := CreateReplicateServer(mockStream)
	assert.NoError(t, err)

	// Call handleReplicateMessage directly (synchronous).
	err = server.handleReplicateMessage(req)
	assert.NoError(t, err)

	// Flush the provider to ensure all spans are exported.
	_ = tp.ForceFlush(context.Background())

	// Assert that a "replicate.secondary" span was emitted with the right trace ID.
	spans := exporter.GetSpans()
	var walSpan tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameReplicateSecondary {
			walSpan = s
			assert.Equal(t, expectedTraceID, s.SpanContext.TraceID(),
				"replicate.secondary span must share the source trace ID")
		}
	}
	assert.Equal(t, message.SpanNameReplicateSecondary, walSpan.Name, "a 'replicate.secondary' span must be emitted")
	assert.Equal(t, sourceSC.SpanID(), walSpan.Parent.SpanID(),
		"replicate.secondary should be a child of the source message span")

	// Also verify that the ctx passed to Append carries the same trace ID.
	if capturedCtx != nil {
		capturedSpan := trace.SpanFromContext(capturedCtx)
		assert.True(t, capturedSpan.SpanContext().IsValid(),
			"ctx passed to Append should carry a valid span")
		assert.Equal(t, expectedTraceID, capturedSpan.SpanContext().TraceID(),
			"ctx passed to Append must share the source trace ID")
	}

	assert.NotNil(t, capturedMsg)
	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), capturedMsg))
	assert.True(t, msgSC.IsValid(), "replicate server should keep the source trace context in the mutable message")
	assert.Equal(t, sourceSC.TraceID(), msgSC.TraceID())
	assert.Equal(t, sourceSC.SpanID(), msgSC.SpanID())
}

// buildTraceTestReplicateMsgProto builds a *commonpb.ImmutableMessage that
// carries _tc through the normal message conversion path.
func buildTraceTestReplicateMsgProto(t *testing.T, tracedCtx context.Context) *commonpb.ImmutableMessage {
	t.Helper()
	messageID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
	tt := uint64(42)
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("test-vchannel").
		WithHeader(&messagespb.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(tt).
		WithLastConfirmed(messageID)

	msg.WithTraceContext(tracedCtx)
	milvusMsg := message.ImmutableMessageToMilvusMessage(commonpb.WALName_Pulsar.String(), msg.IntoImmutableMessage(messageID))
	return milvusMsg
}
