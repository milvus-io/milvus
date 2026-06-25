//go:build test && dynamic

package producer

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// TestHandleProduce_ExtractsTraceContext verifies that handleProduce restores
// the client-injected trace context from message properties before appending.
func TestHandleProduce_ExtractsTraceContext(t *testing.T) {
	expectedTraceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	clientCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    expectedTraceID,
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	}))

	// Build properties that carry the injected trace context (simulating what the
	// gRPC client does in Task 4).
	props := map[string]string{
		"_v": "1",
		"_t": strconv.FormatInt(int64(message.MessageTypeTimeTick), 10),
	}
	injectedMsg := message.NewMutableMessageBeforeAppend([]byte("test-payload"), props)
	message.InjectTraceContext(clientCtx, injectedMsg)

	req := &streamingpb.ProduceMessageRequest{
		RequestId: 42,
		Message: &messagespb.Message{
			Payload:    []byte("test-payload"),
			Properties: injectedMsg.Properties().ToRawMap(),
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
		logger:           mlog.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
		metrics:          newProducerMetrics(types.PChannelInfo{Name: "test-ch", Term: 1}),
	}

	// Invoke handleProduce synchronously; the callback fires inside the mock.
	ps.handleProduce(req)
	wg.Wait()

	// The ctx passed to AppendAsync must carry the extracted remote span context.
	assert.NotNil(t, capturedCtx)
	capturedSC := trace.SpanContextFromContext(capturedCtx)
	assert.True(t, capturedSC.IsValid(), "ctx passed to AppendAsync should carry a valid span context")
	assert.Equal(t, expectedTraceID, capturedSC.TraceID(), "AppendAsync ctx must share the client trace ID")
}
