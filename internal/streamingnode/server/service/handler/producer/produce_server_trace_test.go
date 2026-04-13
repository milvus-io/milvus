//go:build test && dynamic

package producer

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

// TestHandleProduce_ExtractsAndOpensServerSpan verifies that handleProduce:
//  1. Extracts the client-injected trace context from message properties.
//  2. Starts a "wal.append.server" child span under the extracted trace.
//  3. Passes a ctx carrying that span to AppendAsync.
//  4. Ends the span inside the AppendAsync callback.
func TestHandleProduce_ExtractsAndOpensServerSpan(t *testing.T) {
	// Set up an in-memory OTel exporter and make it the global provider.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// Build a client-side traced context and record the expected trace ID.
	clientCtx, clientSpan := otel.Tracer("test").Start(context.Background(), "client.root")
	expectedTraceID := trace.SpanContextFromContext(clientCtx).TraceID()
	// End the client span now; the trace context is already injected below.
	clientSpan.End()

	// Build properties that carry the injected trace context (simulating what the
	// gRPC client does in Task 4).
	props := map[string]string{
		"_v": "1",
		"_t": strconv.FormatInt(int64(message.MessageTypeTimeTick), 10),
	}
	// Use InjectTraceContextIntoMap to inject the client span context into the raw map.
	message.InjectTraceContextIntoMap(clientCtx, props)

	req := &streamingpb.ProduceMessageRequest{
		RequestId: 42,
		Message: &messagespb.Message{
			Payload:    []byte("test-payload"),
			Properties: props,
		},
	}

	// Build a mock WAL; capture the ctx passed to AppendAsync.
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{Name: "test-ch", Term: 1}).Maybe()
	l.EXPECT().IsAvailable().Return(true)

	var (
		capturedCtx context.Context
		wg          sync.WaitGroup
	)
	wg.Add(1)

	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).
		Run(func(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
			capturedCtx = ctx
			msgID := walimplstest.NewTestMessageID(1)
			cb(&wal.AppendResult{
				MessageID:              msgID,
				LastConfirmedMessageID: msgID,
				TimeTick:               100,
			}, nil)
			wg.Done()
		}).Return()

	// Build a minimal ProduceServer (same pattern as existing tests).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	grpcProduceServer.EXPECT().Context().Return(ctx).Maybe()
	grpcProduceServer.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	ps := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
		metrics:          newProducerMetrics(types.PChannelInfo{Name: "test-ch", Term: 1}),
	}

	// Invoke handleProduce synchronously; the callback fires inside the mock.
	ps.handleProduce(req)
	wg.Wait()

	// Flush the provider to ensure all spans are exported.
	_ = tp.ForceFlush(context.Background())

	// 1. The ctx passed to AppendAsync must carry a valid span.
	assert.NotNil(t, capturedCtx)
	capturedSpan := trace.SpanFromContext(capturedCtx)
	assert.True(t, capturedSpan.SpanContext().IsValid(),
		"ctx passed to AppendAsync should carry a valid span")

	// 2. A "wal.append.server" span must have been exported with the same trace ID.
	spans := exporter.GetSpans()
	var serverFound bool
	for _, s := range spans {
		if s.Name == "wal.append.server" {
			assert.Equal(t, expectedTraceID, s.SpanContext.TraceID(),
				"wal.append.server span must share the client trace ID")
			serverFound = true
		}
	}
	assert.True(t, serverFound, "a 'wal.append.server' span must be exported")
}
