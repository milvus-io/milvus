package producer

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/errs"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func TestResumableProducer(t *testing.T) {
	p := mock_producer.NewMockProducer(t)
	msgID := mock_message.NewMockMessageID(t)
	p.EXPECT().Produce(mock.Anything, mock.Anything).Return(&producer.ProduceResult{
		MessageID: msgID,
		TimeTick:  100,
	}, nil)
	p.EXPECT().Close().Return()
	ch := make(chan struct{})
	p.EXPECT().Available().Return(ch)
	p.EXPECT().IsAvailable().RunAndReturn(func() bool {
		select {
		case <-ch:
			return false
		default:
			return true
		}
	})

	i := 0
	ch2 := make(chan struct{})
	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		if i == 0 {
			i++
			return p, nil
		} else if i == 1 {
			i++
			return nil, errors.New("test")
		} else if i == 2 {
			p := mock_producer.NewMockProducer(t)
			msgID := mock_message.NewMockMessageID(t)
			p.EXPECT().Produce(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*producer.ProduceResult, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return &producer.ProduceResult{
					MessageID: msgID,
					TimeTick:  100,
				}, nil
			})
			p.EXPECT().Close().Return()
			p.EXPECT().Available().Return(ch2)
			p.EXPECT().IsAvailable().RunAndReturn(func() bool {
				select {
				case <-ch2:
					return false
				default:
					return true
				}
			})
			i++
			return p, nil
		}
		return nil, handler.ErrClientClosed
	}, &ProducerOptions{
		PChannel: "test",
	})

	msg := mock_message.NewMockMutableMessage(t)
	id, err := rp.Produce(context.Background(), msg)
	assert.NotNil(t, id)
	assert.NoError(t, err)
	close(ch)
	id, err = rp.Produce(context.Background(), msg)
	assert.NotNil(t, id)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	id, err = rp.Produce(ctx, msg)
	assert.Nil(t, id)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrCanceledOrDeadlineExceed))

	// Test the underlying handler close.
	close(ch2)
	id, err = rp.Produce(context.Background(), msg)
	assert.Nil(t, id)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, errs.ErrClosed))
	rp.Close()
}
