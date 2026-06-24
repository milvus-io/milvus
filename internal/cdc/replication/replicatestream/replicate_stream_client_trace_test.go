//go:build test && dynamic

package replicatestream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/cdc/meta"
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

func setupTraceExporter(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
	})
	return exporter
}

// TestSendMessage_OpensCdcSpanWithExtractedParent asserts that:
//   - A cdc.replicate span is exported under the source span carried by _tc.
//   - The outgoing ReplicateRequest preserves the original immutable message
//     properties; secondary-side WAL span injection happens on the replicate server.
func TestSendMessage_OpensCdcSpanWithExtractedParent(t *testing.T) {
	exporter := setupTraceExporter(t)

	// Simulate a primary WAL message with a persisted _tc pointing at
	// the primary wal.append.server span.
	primaryCtx, primarySpan := otel.Tracer("test").Start(context.Background(), "primary.wal.append.server")
	primarySC := trace.SpanContextFromContext(primaryCtx)
	primarySpan.End()

	imsg := buildTestCDCImmutableMessage(t, primaryCtx)
	client := newMockReplicateStreamClient(t)

	c := &replicateStreamClient{
		clusterID: "test-cluster",
		client:    client,
		channel:   &meta.ReplicateChannel{Key: "test-replicate-key"},
		metrics:   NewReplicateMetrics(nil),
	}
	err := c.sendMessage(imsg)
	assert.NoError(t, err)
	capturedReq := <-client.ch

	// Outgoing _tc is still the primary span context. The replicate server owns
	// the next WAL span and will overwrite _tc after it starts that span.
	outProps := capturedReq.GetReplicateMessage().GetMessage().GetProperties()
	outMsg := message.MilvusMessageToImmutableMessage(capturedReq.GetReplicateMessage().GetMessage())
	outSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), outMsg))
	assert.True(t, outSC.IsValid(), "outgoing _tc must be valid")
	assert.Equal(t, primarySC.SpanID(), outSC.SpanID(),
		"outgoing _tc should preserve the immutable message trace context")
	assert.Equal(t, imsg.Properties().ToRawMap(), outProps,
		"sendMessage should not mutate immutable message properties")

	// The cdc.replicate span should use the extracted _tc as its parent.
	spans := exporter.GetSpans()
	var cdc tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "cdc.replicate" {
			cdc = s
			break
		}
	}
	assert.Equal(t, "cdc.replicate", cdc.Name, "a cdc.replicate span must be exported")
	assert.Equal(t, primarySC.TraceID(), cdc.SpanContext.TraceID(),
		"cdc.replicate must share the source trace ID")
	assert.Equal(t, primarySC.SpanID(), cdc.Parent.SpanID(),
		"cdc.replicate must be a child of the source span")
}

// TestSendTxnMessage_SendsEachMessageWithItsOwnCdcSpan verifies that txn
// replication does not add an extra txn-level span.
func TestSendTxnMessage_SendsEachMessageWithItsOwnCdcSpan(t *testing.T) {
	exporter := setupTraceExporter(t)

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

	client := newMockReplicateStreamClient(t)
	c := &replicateStreamClient{
		clusterID: "test-cluster",
		client:    client,
		channel:   &meta.ReplicateChannel{Key: "test-replicate-key"},
		metrics:   NewReplicateMetrics(nil),
	}
	err := c.sendTxnMessage(txnMock)
	assert.NoError(t, err)

	// begin + 1 body + commit = 3 sends.
	assert.Len(t, client.ch, 3)

	spans := exporter.GetSpans()

	var cdcReplicateSpans []tracetest.SpanStub
	for _, s := range spans {
		assert.NotEqual(t, "cdc.replicate.txn", s.Name, "txn replication should not emit a txn-level span")
		if s.Name == "cdc.replicate" {
			cdcReplicateSpans = append(cdcReplicateSpans, s)
		}
	}

	assert.Equal(t, 3, len(cdcReplicateSpans), "3 cdc.replicate spans must be exported for begin+body+commit")
	var beginSpan tracetest.SpanStub
	for _, s := range cdcReplicateSpans {
		if s.Parent.SpanID() == primarySC.SpanID() {
			beginSpan = s
		}
	}
	assert.Equal(t, "cdc.replicate", beginSpan.Name, "begin message span should use the source span as parent")
}
