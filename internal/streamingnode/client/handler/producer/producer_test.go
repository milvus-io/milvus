package producer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestProducer(t *testing.T) {
	c := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	cc := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceClient(t)
	recvCh := make(chan *streamingpb.ProduceResponse, 10)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ProduceResponse, error) {
		msg, ok := <-recvCh
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	})
	sendCh := make(chan struct{}, 5)
	cc.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceRequest) error {
		sendCh <- struct{}{}
		return nil
	})
	c.EXPECT().Produce(mock.Anything, mock.Anything).Return(cc, nil)
	cc.EXPECT().CloseSend().RunAndReturn(func() error {
		recvCh <- &streamingpb.ProduceResponse{Response: &streamingpb.ProduceResponse_Close{}}
		close(recvCh)
		return nil
	})

	ctx := context.Background()
	opts := &ProducerOptions{
		Assignment: &types.PChannelInfoAssigned{
			Channel: types.PChannelInfo{Name: "test", Term: 1},
			Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
		},
	}

	recvCh <- &streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Create{
			Create: &streamingpb.CreateProducerResponse{
				WalName: "test",
			},
		},
	}
	producer, err := CreateProducer(ctx, opts, c)
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	ch := make(chan struct{})
	go func() {
		msgID, err := producer.Produce(ctx, message.NewMutableMessageBuilder().
			WithMessageType(message.MessageTypeUnknown).
			WithPayload([]byte{}).
			BuildMutable())
		assert.Error(t, err)
		assert.Nil(t, msgID)
		msgID, err = producer.Produce(ctx, message.NewMutableMessageBuilder().
			WithMessageType(message.MessageTypeUnknown).
			WithPayload([]byte{}).
			BuildMutable())
		assert.NoError(t, err)
		assert.NotNil(t, msgID)
		close(ch)
	}()
	<-sendCh
	recvCh <- &streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Produce{
			Produce: &streamingpb.ProduceMessageResponse{
				RequestId: 1,
				Response: &streamingpb.ProduceMessageResponse_Error{
					Error: &streamingpb.StreamingError{Code: 1},
				},
			},
		},
	}
	<-sendCh
	recvCh <- &streamingpb.ProduceResponse{
		Response: &streamingpb.ProduceResponse_Produce{
			Produce: &streamingpb.ProduceMessageResponse{
				RequestId: 2,
				Response: &streamingpb.ProduceMessageResponse_Result{
					Result: &streamingpb.ProduceMessageResponseResult{
						Id: &streamingpb.MessageID{Id: walimplstest.NewTestMessageID(1).Marshal()},
					},
				},
			},
		},
	}
	<-ch

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = producer.Produce(ctx, message.NewMutableMessageBuilder().
		WithMessageType(message.MessageTypeUnknown).
		WithPayload([]byte{}).
		BuildMutable())
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, producer.IsAvailable())
	producer.Close()
	assert.False(t, producer.IsAvailable())
}
