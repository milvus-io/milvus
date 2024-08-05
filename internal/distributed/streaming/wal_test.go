package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	vChannel1 = "by-dev-rootcoord-dml_1"
	vChannel2 = "by-dev-rootcoord-dml_2"
	vChannel3 = "by-dev-rootcoord-dml_3"
)

func TestWAL(t *testing.T) {
	coordClient := mock_client.NewMockClient(t)
	coordClient.EXPECT().Close().Return()
	handler := mock_handler.NewMockHandlerClient(t)
	handler.EXPECT().Close().Return()

	w := &walAccesserImpl{
		lifetime:                       lifetime.NewLifetime(lifetime.Working),
		streamingCoordAssignmentClient: coordClient,
		handlerClient:                  handler,
		producerMutex:                  sync.Mutex{},
		producers:                      make(map[string]*producer.ResumableProducer),
		utility: &utility{
			appendExecutionPool:   conc.NewPool[struct{}](10),
			dispatchExecutionPool: conc.NewPool[struct{}](10),
		},
	}
	defer w.Close()

	ctx := context.Background()

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
	p.EXPECT().Produce(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  10,
		TxnCtx: &message.TxnContext{
			TxnID:    1,
			BeginTSO: 10,
			TTL:      10 * time.Second,
		},
	}, nil)
	p.EXPECT().Available().Return(available)
	p.EXPECT().Close().Return()

	handler.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil)
	result, err := w.Append(ctx, newFlushMessage(vChannel1))
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test commited txn.
	txn, err := w.Txn(ctx, TxnOption{
		VChannel: vChannel1,
		TTL:      10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	err = txn.Append(ctx, newFlushMessage(vChannel1))
	assert.NoError(t, err)
	err = txn.Append(ctx, newFlushMessage(vChannel1))
	assert.NoError(t, err)

	result, err = txn.Commit(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Test rollback txn.
	txn, err = w.Txn(ctx, TxnOption{
		VChannel: vChannel1,
		TTL:      10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, txn)

	err = txn.Append(ctx, newFlushMessage(vChannel1))
	assert.NoError(t, err)
	err = txn.Append(ctx, newFlushMessage(vChannel1))
	assert.NoError(t, err)

	err = txn.Rollback(ctx)
	assert.NoError(t, err)

	resp := w.Utility().AppendMessages(ctx,
		newFlushMessage(vChannel1),
		newFlushMessage(vChannel2),
		newFlushMessage(vChannel2),
		newFlushMessage(vChannel3),
		newFlushMessage(vChannel3),
		newFlushMessage(vChannel3),
	)
	assert.NoError(t, resp.UnwrapFirstError())

	// c := mock_consumer.NewMockConsumer(t)
	// handler.EXPECT().CreateConsumer(mock.Anything, mock.Anything).Return(c, nil)
}

func newFlushMessage(vChannel string) message.MutableMessage {
	msg, err := message.NewFlushMessageBuilderV2().
		WithVChannel(vChannel).
		WithHeader(&message.FlushMessageHeader{}).
		WithBody(&message.FlushMessageBody{}).
		BuildMutable()
	if err != nil {
		panic(err)
	}
	return msg
}
