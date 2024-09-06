package streaming

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
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
		appendExecutionPool:            conc.NewPool[struct{}](10),
		dispatchExecutionPool:          conc.NewPool[struct{}](10),
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
