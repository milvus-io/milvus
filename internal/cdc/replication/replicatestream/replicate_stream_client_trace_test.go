//go:build test && dynamic

package replicatestream

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	mock_message "github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// buildTestCDCImmutableMessage creates an ImmutableMessage with the trace context from primaryCtx injected.
func buildTestCDCImmutableMessage(t *testing.T, primaryCtx context.Context) message.ImmutableMessage {
	t.Helper()
	msgID := walimplstest.NewTestMessageID(1)
	mutableMsg := message.CreateTestEmptyInsertMesage(1, nil)
	mutableMsg.WithTimeTick(100)
	mutableMsg.WithLastConfirmed(msgID)
	mutableMsg.WithTraceContext(primaryCtx)
	return mutableMsg.IntoImmutableMessage(msgID)
}

// TestSendMessageWithCtx_OpensCdcSpanWithLink asserts that:
//   - A cdc.replicate span is exported with a Link back to the primary span
//     that was persisted in the incoming message's _tc property.
//   - The outgoing ReplicateRequest preserves the original immutable message
//     properties; secondary-side WAL span injection happens on the replicate server.
func TestSendMessageWithCtx_OpensCdcSpanWithLink(t *testing.T) {
	defer mockey.UnPatchAll()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Simulate a primary WAL message with a persisted _tc pointing at
	// the primary wal.append.server span.
	primaryCtx, primarySpan := otel.Tracer("test").Start(context.Background(), "primary.wal.append.server")
	primarySC := trace.SpanContextFromContext(primaryCtx)
	primarySpan.End()

	imsg := buildTestCDCImmutableMessage(t, primaryCtx)

	// Capture the ReplicateRequest that sendMessageWithCtx hands off to doSend.
	var capturedReq *milvuspb.ReplicateRequest
	mockey.Mock((*replicateStreamClient).doSend).To(
		func(_ *replicateStreamClient, req *milvuspb.ReplicateRequest) error {
			capturedReq = req
			return nil
		}).Build()

	c := &replicateStreamClient{
		clusterID: "test-cluster",
		metrics:   NewReplicateMetrics(nil),
	}
	err := c.sendMessageWithCtx(context.Background(), imsg)
	assert.NoError(t, err)
	assert.NotNil(t, capturedReq, "doSend must have been called")

	// Flush spans.
	_ = tp.ForceFlush(context.Background())

	// Outgoing _tc is still the primary span context. The replicate server owns
	// the next WAL span and will overwrite _tc after it starts that span.
	outProps := capturedReq.GetReplicateMessage().GetMessage().GetProperties()
	outMsg := message.MilvusMessageToImmutableMessage(capturedReq.GetReplicateMessage().GetMessage())
	outSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), outMsg))
	assert.True(t, outSC.IsValid(), "outgoing _tc must be valid")
	assert.Equal(t, primarySC.SpanID(), outSC.SpanID(),
		"outgoing _tc should preserve the immutable message trace context")
	assert.Equal(t, imsg.Properties().ToRawMap(), outProps,
		"sendMessageWithCtx should not mutate immutable message properties")

	// The cdc.replicate span must carry a Link with the primary's trace_id.
	spans := exporter.GetSpans()
	var cdc tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "cdc.replicate" {
			cdc = s
			break
		}
	}
	assert.Equal(t, "cdc.replicate", cdc.Name, "a cdc.replicate span must be exported")
	assert.GreaterOrEqual(t, len(cdc.Links), 1, "cdc.replicate span must have at least one Link")
	assert.Equal(t, primarySC.TraceID(), cdc.Links[0].SpanContext.TraceID(),
		"cdc.replicate link must point to primary trace ID")
}

// TestSendTxnMessage_OpensCdcReplicateTxnSpan verifies that sendTxnMessage:
// - Opens a cdc.replicate.txn span with a Link to the primary span from Begin's _tc.
// - Each inner send opens a child cdc.replicate span under cdc.replicate.txn.
func TestSendTxnMessage_OpensCdcReplicateTxnSpan(t *testing.T) {
	defer mockey.UnPatchAll()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Simulate a primary wal.txn trace context in the Begin message.
	primaryCtx, primarySpan := otel.Tracer("test").Start(context.Background(), "primary.wal.txn")
	primarySC := trace.SpanContextFromContext(primaryCtx)
	primarySpan.End()

	beginMsg := buildTestCDCImmutableMessage(t, primaryCtx)
	bodyMsg := buildTestCDCImmutableMessage(t, context.Background())
	commitMsg := buildTestCDCImmutableMessage(t, context.Background())

	// Build a mock ImmutableTxnMessage.
	txnMock := mock_message.NewMockImmutableTxnMessage(t)
	txnMock.EXPECT().Begin().Return(beginMsg)
	txnMock.EXPECT().RangeOver(mock.Anything).RunAndReturn(func(fn func(message.ImmutableMessage) error) error {
		return fn(bodyMsg)
	})
	txnMock.EXPECT().Commit().Return(commitMsg)

	var sendCount int
	mockey.Mock((*replicateStreamClient).doSend).To(
		func(_ *replicateStreamClient, req *milvuspb.ReplicateRequest) error {
			sendCount++
			return nil
		}).Build()

	c := &replicateStreamClient{
		clusterID: "test-cluster",
		metrics:   NewReplicateMetrics(nil),
	}
	err := c.sendTxnMessage(txnMock)
	assert.NoError(t, err)

	// begin + 1 body + commit = 3 sends.
	assert.Equal(t, 3, sendCount)

	_ = tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()

	var txnSpan tracetest.SpanStub
	var cdcReplicateSpans []tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "cdc.replicate.txn" {
			txnSpan = s
		}
		if s.Name == "cdc.replicate" {
			cdcReplicateSpans = append(cdcReplicateSpans, s)
		}
	}

	assert.Equal(t, "cdc.replicate.txn", txnSpan.Name, "a cdc.replicate.txn span must be exported")
	assert.GreaterOrEqual(t, len(txnSpan.Links), 1, "cdc.replicate.txn span must have at least one Link")
	assert.Equal(t, primarySC.TraceID(), txnSpan.Links[0].SpanContext.TraceID(),
		"cdc.replicate.txn link must point to primary trace ID")

	// Three inner cdc.replicate spans, all children of txnSpan.
	assert.Equal(t, 3, len(cdcReplicateSpans), "3 cdc.replicate spans must be exported for begin+body+commit")
	for _, s := range cdcReplicateSpans {
		assert.Equal(t, txnSpan.SpanContext.SpanID(), s.Parent.SpanID(),
			"cdc.replicate spans must be children of cdc.replicate.txn")
	}
}

// TestSendMessage_DelegatesToSendMessageWithCtx ensures the refactored sendMessage
// still calls through correctly (smoke test).
func TestSendMessage_DelegatesToSendMessageWithCtx(t *testing.T) {
	defer mockey.UnPatchAll()

	var sendCount int
	mockey.Mock((*replicateStreamClient).doSend).To(
		func(_ *replicateStreamClient, req *milvuspb.ReplicateRequest) error {
			sendCount++
			return nil
		}).Build()

	imsg := buildTestCDCImmutableMessage(t, context.Background())
	c := &replicateStreamClient{
		clusterID: "test-cluster",
		metrics:   NewReplicateMetrics(nil),
	}
	err := c.sendMessage(imsg)
	assert.NoError(t, err)
	assert.Equal(t, 1, sendCount)
}
