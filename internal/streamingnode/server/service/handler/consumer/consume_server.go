package consumer

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// CreateConsumeServer create a new consumer.
// Expected message sequence:
// CreateConsumeServer:
// <- CreateVChannelConsumer 1
// -> CreateVChannelConsuemr 1
// -> ConsumeMessage 1.1
// <- CreateVChannelConsumer 2
// -> ConsumeMessage 1.2
// -> CreateVChannelConsumer 2
// -> ConsumeMessage 2.1
// -> ConsumeMessage 2.2
// -> ConsumeMessage 1.3
// <- CloseVChannelConsumer 1
// -> CloseVChannelConsumer 1
// -> ConsumeMessage 2.3
// <- CloseVChannelConsumer 2
// -> CloseVChannelConsumer 2
// CloseConsumer:
func CreateConsumeServer(walManager walmanager.Manager, streamServer streamingpb.StreamingNodeHandlerService_ConsumeServer) (*ConsumeServer, error) {
	createReq, err := contextutil.GetCreateConsumer(streamServer.Context())
	if err != nil {
		return nil, status.NewInvalidArgument("create consumer request is required")
	}

	l, err := walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(createReq.GetPchannel()))
	if err != nil {
		return nil, err
	}
	consumeServer := &consumeGrpcServerHelper{
		StreamingNodeHandlerService_ConsumeServer: streamServer,
	}
	if err := consumeServer.SendCreated(&streamingpb.CreateConsumerResponse{
		WalName: l.WALName().String(),
	}); err != nil {
		return nil, errors.Wrap(err, "at send created")
	}

	req, err := streamServer.Recv()
	if err != nil {
		return nil, status.NewInvalidArgument("receive create consumer request failed")
	}
	createVChannelReq := req.GetCreateVchannelConsumer()
	if createVChannelReq == nil {
		return nil, status.NewInvalidArgument("The first message must be  create vchannel consumer request")
	}
	scanner, err := l.Read(streamServer.Context(), wal.ReadOption{
		VChannel:               createVChannelReq.GetVchannel(),
		DeliverPolicy:          createVChannelReq.GetDeliverPolicy(),
		MessageFilter:          createVChannelReq.GetDeliverFilters(),
		IgnorePauseConsumption: createVChannelReq.GetIgnorePauseConsumption(),
	})
	if err != nil {
		return nil, err
	}

	// TODO: consumerID should be generated after we enabling multi-vchannel consuming on same grpc stream.
	consumerID := int64(1)
	if err := consumeServer.SendCreateVChannelConsumer(&streamingpb.CreateVChannelConsumerResponse{
		Response: &streamingpb.CreateVChannelConsumerResponse_ConsumerId{
			ConsumerId: consumerID,
		},
	}); err != nil {
		// release the scanner to avoid resource leak.
		if err := scanner.Close(); err != nil {
			resource.Resource().Logger().Warn(context.TODO(),

				"close scanner failed at create consume server", mlog.Err(err))
		}
		return nil, err
	}
	metrics := newConsumerMetrics(l.Channel().Name)
	return &ConsumeServer{
		consumerID:    1,
		scanner:       scanner,
		consumeServer: consumeServer,
		logger: resource.Resource().Logger().With(
			mlog.FieldComponent("consumer-server"),
			mlog.String("channel", l.Channel().Name),
			mlog.Int64("term", l.Channel().Term)), // Add trace info for all log.
		closeCh: make(chan struct{}),
		metrics: metrics,
	}, nil
}

// ConsumeServer is a ConsumeServer of log messages.
type ConsumeServer struct {
	consumerID    int64
	scanner       wal.Scanner
	consumeServer *consumeGrpcServerHelper
	logger        *mlog.Logger
	closeCh       chan struct{}
	metrics       *consumerMetrics
}

// Execute executes the consumer.
func (c *ConsumeServer) Execute() error {
	// recv loop will be blocked until the stream is closed.
	// 1. close by client.
	// 2. close by server context cancel by return of outside Execute.
	go c.recvLoop()

	// Start a send loop on current goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv close signal.
	// 3. scanner is quit with expected error.
	err := c.sendLoop()
	c.metrics.Close()
	return err
}

// sendLoop sends the message to client.
func (c *ConsumeServer) sendLoop() (err error) {
	defer func() {
		if err := c.scanner.Close(); err != nil {
			c.logger.Warn(context.TODO(), "close scanner failed", mlog.Err(err))
		}
		if err != nil {
			c.logger.Warn(context.TODO(), "send arm of stream closed by unexpected error", mlog.Err(err))
			return
		}
		c.logger.Info(context.TODO(), "send arm of stream closed")
	}()
	// Read ahead buffer is implemented by scanner.
	// Do not add buffer here.
	for {
		select {
		case msg, ok := <-c.scanner.Chan():
			if !ok {
				return status.NewInner("scanner error: %s", c.scanner.Error())
			}
			// If the message is a transaction message, we should send the sub messages one by one,
			// Otherwise we can send the full message directly.
			if txnMsg, ok := msg.(message.ImmutableTxnMessage); ok {
				if err := c.sendImmutableMessage(txnMsg.Begin()); err != nil {
					return err
				}
				if err := txnMsg.RangeOver(func(im message.ImmutableMessage) error {
					if err := c.sendImmutableMessage(im); err != nil {
						return err
					}
					return nil
				}); err != nil {
					return err
				}
				if err := c.sendImmutableMessage(txnMsg.Commit()); err != nil {
					return err
				}
			} else {
				if err := c.sendImmutableMessage(msg); err != nil {
					return err
				}
			}
		case <-c.closeCh:
			c.logger.Info(context.TODO(), "close channel notified")
			if err := c.consumeServer.SendClosed(); err != nil {
				c.logger.Warn(context.TODO(), "send close failed", mlog.Err(err))
				return status.NewInner("close send server failed: %s", err.Error())
			}
			return nil
		case <-c.consumeServer.Context().Done():
			return c.consumeServer.Context().Err()
		}
	}
}

func (c *ConsumeServer) sendImmutableMessage(msg message.ImmutableMessage) (err error) {
	metricsGuard := c.metrics.StartConsume(msg.EstimateSize())
	defer func() {
		metricsGuard.Finish(err)
	}()

	// Send Consumed message to client and do metrics.
	if err := c.consumeServer.SendConsumeMessage(&streamingpb.ConsumeMessageReponse{
		ConsumerId: c.consumerID,
		Message:    msg.IntoImmutableMessageProto(),
	}); err != nil {
		return status.NewInner("send consume message failed: %s", err.Error())
	}
	return nil
}

// recvLoop receives messages from client.
func (c *ConsumeServer) recvLoop() (err error) {
	defer func() {
		close(c.closeCh)
		if err != nil {
			c.logger.Warn(context.TODO(), "recv arm of stream closed by unexpected error", mlog.Err(err))
			return
		}
		c.logger.Info(context.TODO(), "recv arm of stream closed")
	}()

	for {
		req, err := c.consumeServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *streamingpb.ConsumeRequest_Close:
			c.logger.Info(context.TODO(), "close request received")
			// we will receive io.EOF soon, just do nothing here.
		default:
			// skip unknown message here, to keep the forward compatibility.
			c.logger.Warn(context.TODO(), "unknown request type", mlog.Any("request", req))
		}
	}
}
