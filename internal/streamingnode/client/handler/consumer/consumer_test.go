package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func TestConsumer(t *testing.T) {
	resultCh := make(adaptor.ChanMessageHandler, 1)
	c := newMockedConsumerImpl(t, context.Background(), resultCh)

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

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
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(2), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewInsertMessageBuilderV1().
		WithVChannel("test-1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(3), mmsg.WithTxnContext(txnCtx))

	mmsg, _ = message.NewCommitTxnMessageBuilderV2().
		WithVChannel("test-1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(4), mmsg.WithTxnContext(txnCtx))

	msg = <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(4)))
	assert.Equal(t, msg.TxnContext().TxnID, txnCtx.TxnID)
	assert.Equal(t, message.MessageTypeTxn, msg.MessageType())

	c.consumer.Close()
	<-c.consumer.Done()
	assert.NoError(t, c.consumer.Error())
}

func TestConsumerWithCancellation(t *testing.T) {
	resultCh := make(adaptor.ChanMessageHandler, 1)
	ctx, cancel := context.WithCancel(context.Background())
	c := newMockedConsumerImpl(t, ctx, resultCh)

	mmsg, _ := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)
	// The recv goroutinue will be blocked until the context is canceled.
	mmsg, _ = message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("test-1").
		BuildMutable()
	c.recvCh <- newConsumeResponse(walimplstest.NewTestMessageID(1), mmsg)

	// The background recv loop should be started.
	time.Sleep(20 * time.Millisecond)

	go func() {
		c.consumer.Close()
	}()

	select {
	case <-c.consumer.Done():
		panic("should not reach here")
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	select {
	case <-c.consumer.Done():
	case <-time.After(20 * time.Millisecond):
		panic("should not reach here")
	}
	assert.ErrorIs(t, c.consumer.Error(), context.Canceled)
}

type mockedConsumer struct {
	consumer Consumer
	recvCh   chan *streamingpb.ConsumeResponse
}

func newMockedConsumerImpl(t *testing.T, ctx context.Context, h message.Handler) *mockedConsumer {
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

	opts := &ConsumerOptions{
		VChannel: "test-1",
		Assignment: &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: "test", Term: 1},
			Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
		},
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterTimeTickGT(100),
		},
		MessageHandler: h,
	}

	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Create{
			Create: &streamingpb.CreateConsumerResponse{
				WalName: walimplstest.WALName,
			},
		},
	}
	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_CreateVchannel{
			CreateVchannel: &streamingpb.CreateVChannelConsumerResponse{
				Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{
					ConsumerId: 1,
				},
			},
		},
	}
	consumer, err := CreateConsumer(ctx, opts, c)
	if err != nil {
		panic(err)
	}

	return &mockedConsumer{
		consumer: consumer,
		recvCh:   recvCh,
	}
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
