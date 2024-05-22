package handler

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/lognode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/lognode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/lazyconn"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
)

var _ HandlerClient = (*handlerClientImpl)(nil)

type (
	Producer = producer.Producer
	Consumer = consumer.Consumer
)

// HandlerClient is the interface that wraps logpb.LogNodeHandlerServiceClient.
type HandlerClient interface {
	// CreateProducer creates a producer.
	// Producer is a stream client, it will be available until context canceled or active close.
	CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error)

	// CreateConsumer creates a consumer.
	// Consumer is a stream client, it will be available until context canceled or active close.
	CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error)

	// GetLatestMessageID returns the latest message id of the channel.
	GetLatestMessageID(ctx context.Context, channelName string) (message.MessageID, error)

	// Close closes the handler client.
	Close()
}

type handlerClientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	conn     *lazyconn.LazyGRPCConn // TODO: use conn pool if there's huge amount of stream.
	// But there's low possibility that there's 100 stream on one lognode.
	rb      resolver.Builder
	watcher assignment.Watcher
}

// getHandlerService returns a handler service client.
func (c *handlerClientImpl) getHandlerService(ctx context.Context) (logpb.LogNodeHandlerServiceClient, error) {
	conn, err := c.conn.Get(ctx)
	if err != nil {
		return nil, err
	}
	return logpb.NewLogNodeHandlerServiceClient(conn), nil
}

// CreateProducer creates a producer.
func (hc *handlerClientImpl) CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error) {
	p, err := hc.createHandlerUntilLogNodeReady(ctx, opts.Channel, func(ctx context.Context, assign *assignment.Assignment) (any, error) {
		return hc.createProducer(ctx, opts, assign)
	})
	if err != nil {
		return nil, err
	}
	return p.(Producer), nil
}

// CreateConsumer creates a consumer.
func (hc *handlerClientImpl) CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error) {
	c, err := hc.createHandlerUntilLogNodeReady(ctx, opts.Channel, func(ctx context.Context, assign *assignment.Assignment) (any, error) {
		return hc.createConsumer(ctx, opts, assign)
	})
	if err != nil {
		return nil, err
	}
	return c.(Consumer), nil
}

// GetLatestMessageID returns the latest message id of the channel.
func (hc *handlerClientImpl) GetLatestMessageID(ctx context.Context, channelName string) (message.MessageID, error) {
	// Wait for handler service is ready.
	handlerService, err := hc.getHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := hc.createHandlerUntilLogNodeReady(ctx, channelName, func(ctx context.Context, assign *assignment.Assignment) (any, error) {
		// select server to consume.
		ctx = contextutil.WithPickServerID(ctx, assign.ServerID)
		return handlerService.GetLatestMessageID(ctx, &logpb.GetLatestMessageIDRequest{
			ChannelName: channelName,
			Term:        assign.Term,
		})
	})
	if err != nil {
		return nil, err
	}
	return message.NewMessageIDFromPBMessageID(resp.(*logpb.GetLatestMessageIDResponse).Id), nil
}

// createHandlerUntilLogNodeReady creates a handler until log node ready.
// If log node is not ready, it will block until new assignment term is coming or context timeout.
func (hc *handlerClientImpl) createHandlerUntilLogNodeReady(ctx context.Context, channel string, create func(ctx context.Context, assign *assignment.Assignment) (any, error)) (any, error) {
	for {
		assign := hc.watcher.Get(ctx, channel)
		if assign != nil {
			// Find assignment, try to create producer on this assignment.
			c, err := create(ctx, assign)
			if err == nil {
				return c, nil
			}

			// Check if wrong log node.
			logServiceErr := status.AsLogError(err)
			if !logServiceErr.IsWrongLogNode() {
				// stop retry if not wrong log node error.
				return nil, logServiceErr
			}
		}

		// Block until new assignment term is coming if wrong log node or no assignment.
		if err := hc.watcher.Watch(ctx, channel, assign); err != nil {
			// Context timeout
			log.Warn("wait for watch channel assignment timeout", zap.String("channel", channel), zap.Any("oldAssign", assign))
			return nil, err
		}
	}
}

func (hc *handlerClientImpl) createProducer(ctx context.Context, opts *options.ProducerOptions, assign *assignment.Assignment) (Producer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	// Wait for handler service is ready.
	handlerService, err := hc.getHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	return producer.CreateProducer(ctx, opts, handlerService, assign)
}

func (hc *handlerClientImpl) createConsumer(ctx context.Context, opts *options.ConsumerOptions, assign *assignment.Assignment) (Consumer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	// Wait for handler service is ready.
	handlerService, err := hc.getHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	return consumer.CreateConsumer(ctx, opts, handlerService, assign)
}

// Close closes the handler client.
func (hc *handlerClientImpl) Close() {
	hc.lifetime.SetState(lifetime.Stopped)
	hc.lifetime.Wait()
	hc.lifetime.Close()
	hc.watcher.Close()
	hc.conn.Close()
	hc.rb.Close()
}
