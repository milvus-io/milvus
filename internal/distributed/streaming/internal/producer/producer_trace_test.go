//go:build test && dynamic

package producer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestProduceAutocommit_OpensSpanAndInjectsTraceContext(t *testing.T) {
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

	g := &ProduceGuard{
		producer: p,
		msgs:     []message.MutableMessage{msg},
	}
	_, err := g.commit(context.Background())
	assert.NoError(t, err)

	msgSC := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), msg))
	spans := exporter.GetSpans()
	var autocommit tracetest.SpanStub
	for _, s := range spans {
		assert.NotEqual(t, "wal.append.client", s.Name, "client append span should be flattened")
		if s.Name == message.SpanNameWALAutocommit {
			autocommit = s
		}
	}
	assert.Equal(t, message.SpanNameWALAutocommit, autocommit.Name, "wal.autocommit span should be emitted")
	assert.True(t, msgSC.IsValid(), "autocommit message should carry _tc")
	assert.Equal(t, autocommit.SpanContext.TraceID(), msgSC.TraceID())
	assert.Equal(t, autocommit.SpanContext.SpanID(), msgSC.SpanID())
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

func TestProduceTxn_WrapsInWalTxnSpan(t *testing.T) {
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

	_, err := g.produceTxn(context.Background(), msg)
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var walTxn tracetest.SpanStub
	for _, s := range spans {
		assert.NotEqual(t, "wal.append.client", s.Name, "client append span should be flattened")
		if s.Name == message.SpanNameWALTxn {
			walTxn = s
			break
		}
	}
	assert.Equal(t, message.SpanNameWALTxn, walTxn.Name, "wal.txn span should be emitted")
}

func TestProduceTxn_UsesSameTraceContextForTxnMessages(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	mockMsgID := mock_message.NewMockMessageID(t)
	txnCtx := &message.TxnContext{
		TxnID:     1,
		Keepalive: time.Minute,
	}

	var mu sync.Mutex
	var msgSpanContexts []trace.SpanContext
	mockProd := mock_producer.NewMockProducer(t)
	mockProd.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			sc := trace.SpanContextFromContext(message.ExtractTraceContext(context.Background(), msg))
			mu.Lock()
			msgSpanContexts = append(msgSpanContexts, sc)
			mu.Unlock()
			return &types.AppendResult{
				MessageID: mockMsgID,
				TimeTick:  100,
				TxnCtx:    txnCtx,
			}, nil
		})
	mockProd.EXPECT().Available().Return(make(chan struct{}))
	mockProd.EXPECT().IsAvailable().Return(true)
	mockProd.EXPECT().Close().Return()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return mockProd, nil
	}, &ProducerOptions{
		PChannel: "test-trace",
	})
	t.Cleanup(rp.Close)

	g := &ProduceGuard{producer: rp}
	_, err := g.produceTxn(context.Background(), buildTestInsertMessage(t), buildTestInsertMessage(t))
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var walTxn tracetest.SpanStub
	for _, s := range spans {
		assert.NotEqual(t, "wal.append.client", s.Name, "client append span should be flattened")
		if s.Name == message.SpanNameWALTxn {
			walTxn = s
			break
		}
	}
	assert.Equal(t, message.SpanNameWALTxn, walTxn.Name, "wal.txn span should be emitted")
	assert.Len(t, msgSpanContexts, 4, "begin, two body messages and commit should be appended")
	for _, sc := range msgSpanContexts {
		assert.True(t, sc.IsValid(), "txn message should carry _tc")
		assert.Equal(t, walTxn.SpanContext.TraceID(), sc.TraceID())
		assert.Equal(t, walTxn.SpanContext.SpanID(), sc.SpanID())
	}
}

func TestProduceBroadcast_DoesNotOpenAutocommitOrTxnSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	p := newTestResumableProducer(t)
	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"test-vchannel"}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()[0]

	g := &ProduceGuard{
		producer: p,
		msgs:     []message.MutableMessage{msg},
	}
	_, err := g.commit(context.Background())
	assert.NoError(t, err)

	for _, s := range exporter.GetSpans() {
		assert.NotEqual(t, message.SpanNameWALAutocommit, s.Name, "broadcast append should not emit wal.autocommit")
		assert.NotEqual(t, message.SpanNameWALTxn, s.Name, "broadcast append should not emit wal.txn")
		assert.NotEqual(t, "wal.append.client", s.Name, "client append span should be flattened")
	}
}

func TestProduceBroadcast_PanicsWhenGuardHasMultipleMessages(t *testing.T) {
	msgs := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"test-vchannel-1", "test-vchannel-2"}).
		MustBuildBroadcast().
		WithBroadcastID(1).
		SplitIntoMutableMessage()

	g := &ProduceGuard{
		msgs: msgs,
	}
	assert.PanicsWithValue(t, "broadcast guard must hold exactly one message", func() {
		_, _ = g.commit(context.Background())
	})
}

func TestProduceInternalRemoteOpensDistAppendSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	mockMsgID := mock_message.NewMockMessageID(t)
	msg := buildTestInsertMessage(t)

	mockProd := mock_producer.NewMockProducer(t)
	mockProd.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			sc := trace.SpanContextFromContext(ctx)
			assert.True(t, sc.IsValid(), "remote append ctx should carry wal.dist_append span")
			return &types.AppendResult{
				MessageID: mockMsgID,
				TimeTick:  100,
			}, nil
		})
	mockProd.EXPECT().Available().Return(make(chan struct{}))
	mockProd.EXPECT().IsAvailable().Return(true)
	mockProd.EXPECT().Close().Return()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return mockProd, nil
	}, &ProducerOptions{
		PChannel: "test-trace",
	})
	t.Cleanup(rp.Close)

	_, err := rp.produceInternal(context.Background(), msg)
	assert.NoError(t, err)

	spans := exporter.GetSpans()
	var distAppend tracetest.SpanStub
	for _, s := range spans {
		if s.Name == message.SpanNameWALDistAppend {
			distAppend = s
			break
		}
	}
	assert.Equal(t, message.SpanNameWALDistAppend, distAppend.Name, "wal.dist_append span should be emitted for remote append")
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
