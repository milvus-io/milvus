package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_consumer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	streamingnodehandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	vChannel1 = "by-dev-rootcoord-dml_1"
	vChannel2 = "by-dev-rootcoord-dml_2"
	vChannel3 = "by-dev-rootcoord-dml_3"
)

func createMockWAL(t *testing.T) (
	*walAccesserImpl,
	*mock_client.MockClient,
	*mock_client.MockBroadcastService,
	*mock_handler.MockHandlerClient,
) {
	coordClient := mock_client.NewMockClient(t)
	coordClient.EXPECT().Close().Return().Maybe()
	broadcastServce := mock_client.NewMockBroadcastService(t)
	broadcastServce.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, bmm message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			bmm = bmm.WithBroadcastID(1)
			result := make(map[string]*types.AppendResult)
			for idx, msg := range bmm.SplitIntoMutableMessage() {
				result[msg.VChannel()] = &types.AppendResult{
					MessageID: walimplstest.NewTestMessageID(int64(idx)),
					TimeTick:  uint64(time.Now().UnixMilli()),
				}
			}
			return &types.BroadcastAppendResult{
				AppendResults: result,
			}, nil
		}).Maybe()
	broadcastServce.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil).Maybe()
	coordClient.EXPECT().Broadcast().Return(broadcastServce).Maybe()
	handler := mock_handler.NewMockHandlerClient(t)
	c := mock_consumer.NewMockConsumer(t)
	handler.EXPECT().CreateConsumer(mock.Anything, mock.Anything).Return(c, nil).Maybe()
	handler.EXPECT().Close().Return().Maybe()

	w := &walAccesserImpl{
		lifetime:              typeutil.NewLifetime(),
		streamingCoordClient:  coordClient,
		handlerClient:         handler,
		producerMutex:         sync.Mutex{},
		producers:             make(map[string]*producer.ResumableProducer),
		appendExecutionPool:   conc.NewPool[struct{}](10),
		dispatchExecutionPool: conc.NewPool[struct{}](10),
	}
	return w, coordClient, broadcastServce, handler
}

func TestWAL(t *testing.T) {
	ctx := context.Background()
	w, _, _, handler := createMockWAL(t)

	available := make(chan struct{})
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().IsAvailable().RunAndReturn(func() bool {
		select {
		case <-available:
			return false
		default:
			return true
		}
	})
	p.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		TxnCtx: &message.TxnContext{
			TxnID:     1,
			Keepalive: 10 * time.Second,
		},
	}, nil)
	p.EXPECT().Available().Return(available)
	p.EXPECT().Close().Return()

	handler.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil)
	result, err := w.RawAppend(ctx, newInsertMessage(vChannel1))
	assert.NoError(t, err)
	assert.NotNil(t, result)

	resp := w.AppendMessages(ctx,
		newInsertMessage(vChannel1),
		newInsertMessage(vChannel2),
		newInsertMessage(vChannel2),
		newInsertMessage(vChannel3),
		newInsertMessage(vChannel3),
		newInsertMessage(vChannel3),
	)
	assert.NoError(t, resp.UnwrapFirstError())

	r, err := w.Broadcast().Append(ctx, newBroadcastMessage([]string{vChannel1, vChannel2, vChannel3}))
	assert.NoError(t, err)
	assert.Len(t, r.AppendResults, 3)

	err = w.Broadcast().Ack(ctx, message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vChannel1).
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.NoError(t, err)

	cnt := atomic.NewInt32(0)
	p.EXPECT().Append(mock.Anything, mock.Anything).Unset()
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
			if mm.MessageType() == message.MessageTypeInsert {
				cnt.Inc()
				if cnt.Load() == 1 {
					return nil, status.NewTransactionExpired("")
				}
			}
			return &types.AppendResult{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  10,
				TxnCtx: &message.TxnContext{
					TxnID:     1,
					Keepalive: 10 * time.Second,
				},
			}, nil
		})
	resp = w.AppendMessages(ctx,
		newInsertMessage(vChannel2),
		newInsertMessage(vChannel2),
		newInsertMessage(vChannel3),
		newInsertMessage(vChannel3),
		newInsertMessage(vChannel3),
	)
	assert.NoError(t, resp.UnwrapFirstError())

	w.Close()

	w.Local().GetLatestMVCCTimestampIfLocal(ctx, vChannel1)
	w.Local().GetMetricsIfLocal(ctx)

	resp = w.AppendMessages(ctx, newInsertMessage(vChannel1))
	assert.Error(t, resp.UnwrapFirstError())

	r, err = w.Broadcast().Append(ctx, newBroadcastMessage([]string{vChannel1, vChannel2, vChannel3}))
	assert.Error(t, err)
	assert.Nil(t, r)

	err = w.Broadcast().Ack(ctx, message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vChannel1).
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		MustBuildMutable().
		IntoImmutableMessage(rmq.NewRmqID(1)))
	assert.Error(t, err)
}

func newInsertMessage(vChannel string) message.MutableMessage {
	msg, err := message.NewInsertMessageBuilderV1().
		WithVChannel(vChannel).
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return msg
}

func newBroadcastMessage(vchannels []string) message.BroadcastMutableMessage {
	msg, err := message.NewDropCollectionMessageBuilderV1().
		WithBroadcast(vchannels).
		WithHeader(&message.DropCollectionMessageHeader{}).
		WithBody(&msgpb.DropCollectionRequest{}).
		BuildBroadcast()
	if err != nil {
		panic(err)
	}
	return msg
}

func TestAppendMessagesOrderConsistency(t *testing.T) {
	ctx := context.Background()
	w, _, _, handler := createMockWAL(t)
	defer w.Close()

	// Create mock producers for each vchannel that return different message IDs
	// to verify the order of responses matches the order of inputs.
	producers := make(map[string]*mock_producer.MockProducer)
	messageIDCounter := atomic.NewInt64(0)

	for _, vchannel := range []string{vChannel1, vChannel2, vChannel3} {
		p := mock_producer.NewMockProducer(t)
		available := make(chan struct{})
		p.EXPECT().IsAvailable().Return(true).Maybe()
		p.EXPECT().Available().Return(available).Maybe()
		p.EXPECT().Close().Return().Maybe()
		p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
				id := messageIDCounter.Inc()
				return &types.AppendResult{
					MessageID: walimplstest.NewTestMessageID(id),
					TimeTick:  uint64(id),
					TxnCtx: &message.TxnContext{
						TxnID:     message.TxnID(id),
						Keepalive: 10 * time.Second,
					},
				}, nil
			}).Maybe()
		producers[vchannel] = p
	}

	handler.EXPECT().CreateProducer(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts *streamingnodehandler.ProducerOptions) (streamingnodehandler.Producer, error) {
			return producers[opts.PChannel], nil
		})

	// Test case 1: Messages interleaved across multiple vchannels
	// The order should be preserved in the response
	msgs := []message.MutableMessage{
		newInsertMessage(vChannel1), // idx 0
		newInsertMessage(vChannel2), // idx 1
		newInsertMessage(vChannel1), // idx 2
		newInsertMessage(vChannel3), // idx 3
		newInsertMessage(vChannel2), // idx 4
		newInsertMessage(vChannel3), // idx 5
		newInsertMessage(vChannel1), // idx 6
	}

	resp := w.AppendMessages(ctx, msgs...)
	assert.NoError(t, resp.UnwrapFirstError())
	assert.Len(t, resp.Responses, len(msgs))

	// Verify that messages from the same vchannel have the same response
	// (because they are processed as a transaction per vchannel)
	// vChannel1: indices 0, 2, 6 should have the same result
	assert.Equal(t, resp.Responses[0].AppendResult.MessageID, resp.Responses[2].AppendResult.MessageID)
	assert.Equal(t, resp.Responses[0].AppendResult.MessageID, resp.Responses[6].AppendResult.MessageID)

	// vChannel2: indices 1, 4 should have the same result
	assert.Equal(t, resp.Responses[1].AppendResult.MessageID, resp.Responses[4].AppendResult.MessageID)

	// vChannel3: indices 3, 5 should have the same result
	assert.Equal(t, resp.Responses[3].AppendResult.MessageID, resp.Responses[5].AppendResult.MessageID)

	// Different vchannels should have different results
	assert.NotEqual(t, resp.Responses[0].AppendResult.MessageID, resp.Responses[1].AppendResult.MessageID)
	assert.NotEqual(t, resp.Responses[0].AppendResult.MessageID, resp.Responses[3].AppendResult.MessageID)
	assert.NotEqual(t, resp.Responses[1].AppendResult.MessageID, resp.Responses[3].AppendResult.MessageID)

	// Test case 2: All messages from the same vchannel
	messageIDCounter.Store(0)
	msgs2 := []message.MutableMessage{
		newInsertMessage(vChannel1),
		newInsertMessage(vChannel1),
		newInsertMessage(vChannel1),
	}

	resp2 := w.AppendMessages(ctx, msgs2...)
	assert.NoError(t, resp2.UnwrapFirstError())
	assert.Len(t, resp2.Responses, len(msgs2))

	// All responses should be the same (same vchannel -> same transaction result)
	assert.Equal(t, resp2.Responses[0].AppendResult.MessageID, resp2.Responses[1].AppendResult.MessageID)
	assert.Equal(t, resp2.Responses[0].AppendResult.MessageID, resp2.Responses[2].AppendResult.MessageID)

	// Test case 3: Single message
	resp3 := w.AppendMessages(ctx, newInsertMessage(vChannel2))
	assert.NoError(t, resp3.UnwrapFirstError())
	assert.Len(t, resp3.Responses, 1)
	assert.NotNil(t, resp3.Responses[0].AppendResult)
}
