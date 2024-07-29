package consumer

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
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
				WalName: "test",
			},
		},
	}
	recvCh <- &streamingpb.ConsumeResponse{
		Response: &streamingpb.ConsumeResponse_Consume{
			Consume: &streamingpb.ConsumeMessageReponse{
				Id: &streamingpb.MessageID{
					Id: walimplstest.NewTestMessageID(1).Marshal(),
				},
				Message: &streamingpb.Message{
					Payload:    []byte{},
					Properties: make(map[string]string),
				},
			},
		},
	}
	consumer, err := CreateConsumer(ctx, opts, c)
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	consumer.Close()
	msg := <-resultCh
	assert.True(t, msg.MessageID().EQ(walimplstest.NewTestMessageID(1)))
	<-consumer.Done()
	assert.NoError(t, consumer.Error())
}
