package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestConsumer(t *testing.T) {
	c := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	cc := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeClient(t)
	recvCh := make(chan *streamingpb.ConsumeResponse, 10)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ConsumeResponse, error) {
		msg, ok := <-recvCh
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	})
	sendCh := make(chan *streamingpb.ConsumeRequest, 10)
	cc.EXPECT().Send(mock.Anything).RunAndReturn(func(cr *streamingpb.ConsumeRequest) error {
		sendCh <- cr
		return nil
	})
	c.EXPECT().Consume(mock.Anything, mock.Anything).Return(cc, nil)
	cc.EXPECT().CloseSend().RunAndReturn(func() error {
		recvCh <- &streamingpb.ConsumeResponse{Response: &streamingpb.ConsumeResponse_Close{}}
		close(recvCh)
		return nil
	})

	ctx := context.Background()
	resultCh := make(message.ChanMessageHandler, 1)
	opts := &ConsumerOptions{
		Assignment: &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: "test", Term: 1},
			Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
		},
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterVChannel("test-1"),
			options.DeliverFilterTimeTickGT(100),
		},
		MessageHandler: resultCh,
	}

	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Create{
			Create: &streamingpb.CreateConsumerResponse{
				WalName: walimplstest.WALName,
			},
		},
	}

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

	consumer, err := CreateConsumer(ctx, opts, c)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	msg := <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(1)))

	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	mmsg, _ = message.NewBeginTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(2), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewInsertMessageBuilderV1().
		WithVChannel("test-1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(3), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewCommitTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(4), mmsg.WithTxnContext(txnCtx))

	msg = <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(4)))
	assert.Equal(t, msg.TxnContext().TxnID, txnCtx.TxnID)
	assert.Equal(t, message.MessageTypeTxn, msg.MessageType())

	consumer.Close()
	<-consumer.Done()
	assert.NoError(t, consumer.Error())
}

func newConsumeResponse(id message.MessageID, msg message.MutableMessage) *streamingpb.ConsumeResponse {
	msg.WithTimeTick(tsoutil.GetCurrentTime())
	msg.WithLastConfirmed(walimplstest.NewTestMessageID(0))
	return &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Consume{
			Consume: &streamingpb.ConsumeMessageReponse{
				Message: &messagespb.ImmutableMessage{
					Id: &messagespb.MessageID{
						Id: id.Marshal(),
					},
					Payload:    msg.Payload(),
					Properties: msg.Properties().ToRawMap(),
				},
			},
		},
	}
}
