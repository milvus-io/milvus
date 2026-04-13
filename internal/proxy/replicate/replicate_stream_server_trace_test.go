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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
)

// TestHandleReplicateMessage_OpensWalReplicateAppendSpan verifies that
// handleReplicateMessage extracts the CDC-injected trace context from the
// message Properties and opens a "wal.replicate.append" child span whose
// trace ID matches the CDC parent.
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

	// Build a CDC-side traced context and capture the expected trace ID.
	cdcCtx, cdcSpan := otel.Tracer("test").Start(context.Background(), "cdc.replicate")
	expectedTraceID := trace.SpanContextFromContext(cdcCtx).TraceID()
	cdcSpan.End()

	// Build a replicate message proto with _tc injected (mimicking CDC sender).
	reqMsg := buildTraceTestReplicateMsgProto(t, cdcCtx)

	req := &milvuspb.ReplicateRequest_ReplicateMessage{
		ReplicateMessage: &milvuspb.ReplicateMessage{
			SourceClusterId: "cluster-a",
			Message:         reqMsg,
		},
	}

	// Capture the ctx passed to Append.
	var capturedCtx context.Context

	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.ReplicateMutableMessage) (*types.AppendResult, error) {
			capturedCtx = ctx
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

	// Assert that a "wal.replicate.append" span was emitted with the right trace ID.
	spans := exporter.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "wal.replicate.append" {
			assert.Equal(t, expectedTraceID, s.SpanContext.TraceID(),
				"wal.replicate.append span must share the CDC trace ID")
			found = true
		}
	}
	assert.True(t, found, "a 'wal.replicate.append' span must be emitted")

	// Also verify that the ctx passed to Append carries the same trace ID.
	if capturedCtx != nil {
		capturedSpan := trace.SpanFromContext(capturedCtx)
		assert.True(t, capturedSpan.SpanContext().IsValid(),
			"ctx passed to Append should carry a valid span")
		assert.Equal(t, expectedTraceID, capturedSpan.SpanContext().TraceID(),
			"ctx passed to Append must share the CDC trace ID")
	}
}

// buildTraceTestReplicateMsgProto builds a *commonpb.ImmutableMessage that
// NewReplicateMessage will accept, with _tc injected from tracedCtx.
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

	immutable := msg.IntoImmutableMessage(messageID)
	milvusMsg := message.ImmutableMessageToMilvusMessage(commonpb.WALName_Pulsar.String(), immutable)

	// Inject the CDC trace context into the proto Properties map.
	message.InjectTraceContextIntoMap(tracedCtx, milvusMsg.GetProperties())
	return milvusMsg
}
