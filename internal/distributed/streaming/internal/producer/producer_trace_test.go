//go:build test && dynamic

package producer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

func TestProduceInternal_OpensClientSpanAndInjectsTraceContext(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	p := newTestResumableProducer(t)
	msg := buildTestInsertMessage(t)

	_, _ = p.produceInternal(context.Background(), msg)

	capturedProps := msg.Properties().ToRawMap()
	_, hasTc := capturedProps["_tc"]
	assert.True(t, hasTc, "produceInternal must inject _tc into msg.Properties()")

	spans := exporter.GetSpans()
	var found bool
	for _, s := range spans {
		if s.Name == "wal.append.client" {
			found = true
			break
		}
	}
	assert.True(t, found, "wal.append.client span must be exported")
}

// buildTestInsertMessage builds a minimal MutableMessage for testing.
func buildTestInsertMessage(t *testing.T) message.MutableMessage {
	t.Helper()
	return message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.InsertRequest{
			CollectionID: 1,
		}).
		WithVChannel("test-vchannel").
		MustBuildMutable()
}

func TestProduceWithTxnOnce_WrapsInWalTxnSpan(t *testing.T) {
	defer mockey.UnPatchAll()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Stub the three inner steps of produceWithTxnOnce so we only exercise
	// the span-wrapping logic of the function itself.
	mockey.Mock((*ProduceGuard).beginTxn).Return(&message.TxnContext{}, nil).Build()
	mockey.Mock((*ProduceGuard).appendTxnBody).Return(nil).Build()
	mockey.Mock((*ProduceGuard).commitTxn).Return(&types.AppendResult{}, nil).Build()

	g := &ProduceGuard{}
	msg := buildTestInsertMessage(t)

	_, err := g.produceWithTxnOnce(context.Background(), msg)
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var walTxn tracetest.SpanStub
	for _, s := range spans {
		if s.Name == "wal.txn" {
			walTxn = s
			break
		}
	}
	assert.Equal(t, "wal.txn", walTxn.Name, "wal.txn span should be emitted")
}

// newTestResumableProducer builds a ResumableProducer with a mocked inner handler
// so that produceInternal exits on the first iteration.
func newTestResumableProducer(t *testing.T) *ResumableProducer {
	t.Helper()

	mockMsgID := mock_message.NewMockMessageID(t)
	mockProd := mock_producer.NewMockProducer(t)
	mockProd.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: mockMsgID,
		TimeTick:  100,
	}, nil)
	mockProd.EXPECT().Available().Return(make(chan struct{}))
	mockProd.EXPECT().IsAvailable().Return(true)
	mockProd.EXPECT().Close().Return()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return mockProd, nil
	}, &ProducerOptions{
		PChannel: "test-trace",
	})
	t.Cleanup(rp.Close)
	return rp
}
