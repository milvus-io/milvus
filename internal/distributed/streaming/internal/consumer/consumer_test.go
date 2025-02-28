package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestResumableConsumer(t *testing.T) {
	i := 0
	c := mock_consumer.NewMockConsumer(t)
	ch := make(chan struct{})
	c.EXPECT().Done().Return(ch)
	c.EXPECT().Error().Return(errors.New("test"))
	c.EXPECT().Close().Return(nil)
	rc := NewResumableConsumer(func(ctx context.Context, opts *handler.ConsumerOptions) (consumer.Consumer, error) {
		if i == 0 {
			i++
			result := opts.MessageHandler.Handle(message.HandleParam{
				Ctx: context.Background(),
				Message: message.NewImmutableMesasge(
					walimplstest.NewTestMessageID(123),
					[]byte("payload"),
					map[string]string{
						"key": "value",
						"_t":  "1",
						"_tt": message.EncodeUint64(456),
						"_v":  "1",
						"_lc": walimplstest.NewTestMessageID(123).Marshal(),
					}),
			})
			assert.True(t, result.MessageHandled)
			assert.NoError(t, result.Error)
			return c, nil
		} else if i == 1 {
			i++
			return nil, errors.New("test")
		}
		newC := mock_consumer.NewMockConsumer(t)
		newC.EXPECT().Done().Return(make(<-chan struct{}))
		newC.EXPECT().Error().Return(errors.New("test"))
		newC.EXPECT().Close().Return(nil)
		return newC, nil
	}, &ConsumerOptions{
		PChannel:      "test",
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterTimeTickGT(1),
		},
		MessageHandler: adaptor.ChanMessageHandler(make(chan message.ImmutableMessage, 2)),
	})

	select {
	case <-rc.Done():
		t.Error("should not be done")
	case <-time.After(100 * time.Millisecond):
	}
	close(ch)
	select {
	case <-rc.Done():
		t.Error("should not be done")
	case <-time.After(100 * time.Millisecond):
	}

	rc.Close()
	<-rc.Done()
}

func TestHandler(t *testing.T) {
	ch := make(chan message.ImmutableMessage, 100)
	hNop := nopCloseHandler{
		Handler: adaptor.ChanMessageHandler(ch),
	}
	hNop.Handle(message.HandleParam{
		Ctx:     context.Background(),
		Message: message.NewImmutableMesasge(walimplstest.NewTestMessageID(123), []byte("payload"), nil),
	})
	assert.NotNil(t, <-ch)
	hNop.Close()
	select {
	case <-ch:
		panic("should not be closed")
	default:
	}
}
