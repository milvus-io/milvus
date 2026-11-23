package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
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

func TestRemoteConsumerWithPendingMsgPack(t *testing.T) {
	for _, kind := range []string{"delete", "transaction"} {
		t.Run(kind, func(t *testing.T) {
			h := adaptor.NewMsgPackAdaptorHandler()
			// Cancel the previous consumer while its pack has no reader.
			oldCtx, cancel := context.WithCancel(t.Context())
			cancel()
			lastConfirmed := rmq.NewRmqID(1)
			pending := message.CreateTestInsertMessage(t, 1, 1, 60, lastConfirmed).IntoImmutableMessage(rmq.NewRmqID(60))
			result := h.Handle(message.HandleParam{Ctx: oldCtx, Message: pending})
			require.True(t, result.MessageHandled)
			require.ErrorIs(t, result.Error, context.Canceled)

			var frames []*streamingpb.ConsumeResponse
			addFrame := func(tt uint64, msg message.MutableMessage) {
				immutable := msg.WithTimeTick(tt).WithLastConfirmed(lastConfirmed).IntoImmutableMessage(rmq.NewRmqID(int64(tt)))
				frames = append(frames, consumeResponse(immutable))
			}
			del := message.NewDeleteMessageBuilderV1().WithVChannel("v1").
				WithHeader(&message.DeleteMessageHeader{}).WithBody(&msgpb.DeleteRequest{}).MustBuildMutable()
			if kind == "transaction" {
				txn := message.TxnContext{TxnID: 1, Keepalive: time.Second}
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
				commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
					WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable()
				addFrame(58, begin.WithTxnContext(txn))
				addFrame(59, del.WithTxnContext(txn))
				addFrame(61, commit.WithTxnContext(txn))
			} else {
				addFrame(61, del)
			}
			addFrame(68, message.CreateTestInsertMessage(t, 1, 1, 68, lastConfirmed))

			client := handshakeClient(t, func(stream streamingpb.StreamingNodeHandlerService_ConsumeServer) error {
				created := firstHandshakeResponse()
				created.GetCreate().WalName = lastConfirmed.WALName().String()
				if err := stream.Send(created); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if err := stream.Send(successfulHandshakeResponse()); err != nil {
					return err
				}
				for _, frame := range frames {
					if err := stream.Send(frame); err != nil {
						return err
					}
				}
				return nil
			})
			opts := handshakeOptions(h)
			opts.DeliverPolicy = options.DeliverPolicyStartFrom(lastConfirmed)
			opts.DeliverFilters = []options.DeliverFilter{options.DeliverFilterTimeTickGT(60)}
			ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			c, err := CreateConsumer(ctx, opts, client)
			require.NoError(t, err)
			var ticks []uint64
			for pack := range h.Chan() {
				for _, msg := range pack.Msgs {
					ticks = append(ticks, msg.GetTimestamp())
				}
			}
			require.NoError(t, c.Error())
			require.Equal(t, []uint64{60, 61, 68}, ticks)
		})
	}
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
			Create: &streamingpb.CreateConsumerResponse{},
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
	msg.WithTimeTick(tsoutil.ComposeTSByTime(time.Now()))
	msg.WithLastConfirmed(walimplstest.NewTestMessageID(0))
	return consumeResponse(msg.IntoImmutableMessage(id))
}

func consumeResponse(immutableMsg message.ImmutableMessage) *streamingpb.ConsumeResponse {
	return &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Consume{
			Consume: &streamingpb.ConsumeMessageReponse{
				Message: immutableMsg.IntoImmutableMessageProto(),
			},
		},
	}
}
