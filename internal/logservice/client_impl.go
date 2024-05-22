package logservice

import (
	"context"

	"github.com/milvus-io/milvus/internal/logcoord/client"
	"github.com/milvus-io/milvus/internal/lognode/client/handler"
	"github.com/milvus-io/milvus/internal/logservice/consumer"
	"github.com/milvus-io/milvus/internal/logservice/producer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
)

type clientImpl struct {
	// All services
	logCoordClient client.Client
	handlerClient  handler.HandlerClient
}

// ChannelManagement is a channel management tool to manage p/v channel.
func (c *clientImpl) ChannelManagement() client.ChannelService {
	return c.logCoordClient.Channel()
}

// CreateProducer creates a producer.
func (c *clientImpl) CreateProducer(opts *options.ProducerOptions) Producer {
	return producer.NewResumableProducer(c.handlerClient.CreateProducer, opts)
}

// CreateConsumer creates a consumer.
func (c *clientImpl) CreateConsumer(opts *options.ConsumerOptions) Consumer {
	return consumer.NewResumableConsumer(c.handlerClient.CreateConsumer, opts)
}

// GetLatestMessageID returns the latest message id of the channel.
func (c *clientImpl) GetLatestMessageID(ctx context.Context, channelName string) (message.MessageID, error) {
	return c.handlerClient.GetLatestMessageID(ctx, channelName)
}

// Close closes the handler client.
func (c *clientImpl) Close() {
	c.handlerClient.Close()
	c.logCoordClient.Close()
}
