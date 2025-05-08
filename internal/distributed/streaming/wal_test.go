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
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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

	// Test committed txn.
	txn, err := w.Txn(ctx, TxnOption{
		VChannel:  vChannel1,
		Keepalive: 10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	err = txn.Append(ctx, newInsertMessage(vChannel1))
	assert.NoError(t, err)
	err = txn.Append(ctx, newInsertMessage(vChannel1))
	assert.NoError(t, err)

	result, err = txn.Commit(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test rollback txn.
	txn, err = w.Txn(ctx, TxnOption{
		VChannel:  vChannel1,
		Keepalive: 10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	err = txn.Append(ctx, newInsertMessage(vChannel1))
	assert.NoError(t, err)
	err = txn.Append(ctx, newInsertMessage(vChannel1))
	assert.NoError(t, err)

	err = txn.Rollback(ctx)
	assert.NoError(t, err)

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

	err = w.Broadcast().Ack(ctx, types.BroadcastAckRequest{BroadcastID: 1, VChannel: vChannel1})
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

	w.GetLatestMVCCTimestampIfLocal(ctx, vChannel1)

	resp = w.AppendMessages(ctx, newInsertMessage(vChannel1))
	assert.Error(t, resp.UnwrapFirstError())

	r, err = w.Broadcast().Append(ctx, newBroadcastMessage([]string{vChannel1, vChannel2, vChannel3}))
	assert.Error(t, err)
	assert.Nil(t, r)

	err = w.Broadcast().Ack(ctx, types.BroadcastAckRequest{BroadcastID: 1, VChannel: vChannel1})
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
