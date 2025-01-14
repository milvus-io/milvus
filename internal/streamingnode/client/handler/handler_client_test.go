package handler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_assignment"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_consumer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_resolver"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_types"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestHandlerClient(t *testing.T) {
	assignment := &types.PChannelInfoAssigned{
		Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
		Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost"},
	}

	service := mock_lazygrpc.NewMockService[streamingpb.StreamingNodeHandlerServiceClient](t)
	handlerServiceClient := mock_streamingpb.NewMockStreamingNodeHandlerServiceClient(t)
	service.EXPECT().GetService(mock.Anything).Return(handlerServiceClient, nil)
	rb := mock_resolver.NewMockBuilder(t)
	rb.EXPECT().Close().Run(func() {})
	w := mock_assignment.NewMockWatcher(t)
	w.EXPECT().Close().Run(func() {})

	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Close().Run(func() {})
	c := mock_consumer.NewMockConsumer(t)
	c.EXPECT().Close().Run(func() {})

	rebalanceTrigger := mock_types.NewMockAssignmentRebalanceTrigger(t)
	rebalanceTrigger.EXPECT().ReportAssignmentError(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	pK := 0
	handler := &handlerClientImpl{
		lifetime:         typeutil.NewLifetime(),
		service:          service,
		rb:               rb,
		watcher:          w,
		rebalanceTrigger: rebalanceTrigger,
		newProducer: func(ctx context.Context, opts *producer.ProducerOptions, handler streamingpb.StreamingNodeHandlerServiceClient) (Producer, error) {
			if pK == 0 {
				pK++
				return nil, status.NewUnmatchedChannelTerm("pchannel", 1, 2)
			}
			return p, nil
		},
		newConsumer: func(ctx context.Context, opts *consumer.ConsumerOptions, handlerClient streamingpb.StreamingNodeHandlerServiceClient) (Consumer, error) {
			return c, nil
		},
	}
	ctx := context.Background()

	k := 0
	w.EXPECT().Get(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, s string) *types.PChannelInfoAssigned {
		if k == 0 {
			k++
			return nil
		}
		return assignment
	})
	w.EXPECT().Watch(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	producer, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	producer2, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer)
	producer3, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer3)
	producer.Close()
	producer2.Close()
	producer3.Close()

	producer4, err := handler.CreateProducer(ctx, &ProducerOptions{PChannel: "pchannel"})
	assert.NoError(t, err)
	assert.NotNil(t, producer4)
	producer4.Close()

	consumer, err := handler.CreateConsumer(ctx, &ConsumerOptions{
		PChannel:      "pchannel",
		VChannel:      "vchannel",
		DeliverPolicy: options.DeliverPolicyAll(),
		DeliverFilters: []options.DeliverFilter{
			options.DeliverFilterTimeTickGT(10),
			options.DeliverFilterTimeTickGTE(10),
		},
		MessageHandler: make(message.ChanMessageHandler),
	})
	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	consumer.Close()

	service.EXPECT().Close().Return()
	handler.Close()
	producer, err = handler.CreateProducer(ctx, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.Nil(t, producer)

	consumer, err = handler.CreateConsumer(ctx, nil)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrClientClosed)
	assert.Nil(t, consumer)
}

func TestDial(t *testing.T) {
	paramtable.Init()

	w := mock_types.NewMockAssignmentDiscoverWatcher(t)
	w.EXPECT().AssignmentDiscover(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, f func(*types.VersionedStreamingNodeAssignments) error) error {
			return context.Canceled
		},
	)
	handler := NewHandlerClient(w)
	assert.NotNil(t, handler)
	time.Sleep(100 * time.Millisecond)
	handler.Close()
}
