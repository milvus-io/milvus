package consumer

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// The cosume target
	Assignment *types.PChannelInfoAssigned

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// DeliverFilters is the deliver filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler
}

// CreateConsumer creates a new consumer client.
func CreateConsumer(
	ctx context.Context,
	opts *ConsumerOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (Consumer, error) {
	ctx, err := createConsumeRequest(ctx, opts)
	if err != nil {
		return nil, err
	}

	// TODO: configurable or auto adjust grpc.MaxCallRecvMsgSize
	streamClient, err := handlerClient.Consume(ctx, grpc.MaxCallRecvMsgSize(8388608))
	if err != nil {
		return nil, err
	}

	// Recv the first response from server.
	// It must be a create response.
	resp, err := streamClient.Recv()
	if err != nil {
		return nil, err
	}
	createResp := resp.GetCreate()
	if createResp == nil {
		return nil, status.NewInvalidRequestSeq("first message arrive must be create response")
	}
	cli := &consumerImpl{
		walName:          createResp.GetWalName(),
		assignment:       *opts.Assignment,
		grpcStreamClient: streamClient,
		handlerClient:    handlerClient,
		logger: log.With(
			zap.String("walName", createResp.GetWalName()),
			zap.String("pchannel", opts.Assignment.Channel.Name),
			zap.Int64("term", opts.Assignment.Channel.Term),
			zap.Int64("streamingNodeID", opts.Assignment.Node.ServerID)),
		msgHandler: opts.MessageHandler,
		finishErr:  syncutil.NewFuture[error](),
	}
	go cli.execute()
	return cli, nil
}

// createConsumeRequest creates the consume request.
func createConsumeRequest(ctx context.Context, opts *ConsumerOptions) (context.Context, error) {
	// select server to consume.
	ctx = contextutil.WithPickServerID(ctx, opts.Assignment.Node.ServerID)
	// create the consumer request.
	return contextutil.WithCreateConsumer(ctx, &streamingpb.CreateConsumerRequest{
		Pchannel:       types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
		DeliverPolicy:  opts.DeliverPolicy,
		DeliverFilters: opts.DeliverFilters,
	}), nil
}

type consumerImpl struct {
	walName          string
	assignment       types.PChannelInfoAssigned
	grpcStreamClient streamingpb.StreamingNodeHandlerService_ConsumeClient
	handlerClient    streamingpb.StreamingNodeHandlerServiceClient
	logger           *log.MLogger
	msgHandler       message.Handler
	finishErr        *syncutil.Future[error]
	txnBuilder       *message.ImmutableTxnMessageBuilder
}

// Close close the consumer client.
func (c *consumerImpl) Close() {
	// Send the close request to server.
	if err := c.grpcStreamClient.Send(&streamingpb.ConsumeRequest{
		Request: &streamingpb.ConsumeRequest_Close{},
	}); err != nil {
		c.logger.Warn("send close request failed", zap.Error(err))
	}
	// close the grpc client stream.
	if err := c.grpcStreamClient.CloseSend(); err != nil {
		c.logger.Warn("close grpc stream failed", zap.Error(err))
	}
	<-c.finishErr.Done()
}

// Error returns the error of the consumer client.
func (c *consumerImpl) Error() error {
	return c.finishErr.Get()
}

// Done returns a channel that closes when the consumer client is closed.
func (c *consumerImpl) Done() <-chan struct{} {
	return c.finishErr.Done()
}

// execute starts the recv loop.
func (c *consumerImpl) execute() {
	c.recvLoop()
}

// recvLoop is the recv arm of the grpc stream.
// Throughput of the grpc framework should be ok to use single stream to receive message.
// Once throughput is not enough, look at https://grpc.io/docs/guides/performance/ to find the solution.
// recvLoop will always receive message from server by following sequence:
// - message at timetick 4.
// - message at timetick 5.
// - txn begin message at timetick 1.
// - txn body message at timetick 2.
// - txn body message at timetick 3.
// - txn commit message at timetick 6.
// - message at timetick 7.
// - Close.
// - EOF.
func (c *consumerImpl) recvLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("recv arm of stream closed with unexpected error", zap.Error(err))
		} else {
			c.logger.Info("recv arm of stream closed")
		}
		c.finishErr.Set(err)
		c.msgHandler.Close()
	}()

	for {
		resp, err := c.grpcStreamClient.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *streamingpb.ConsumeResponse_Consume:
			msgID, err := message.UnmarshalMessageID(c.walName, resp.Consume.GetMessage().GetId().GetId())
			if err != nil {
				return err
			}
			newImmutableMsg := message.NewImmutableMesasge(
				msgID,
				resp.Consume.GetMessage().GetPayload(),
				resp.Consume.GetMessage().GetProperties(),
			)
			if newImmutableMsg.TxnContext() != nil {
				c.handleTxnMessage(newImmutableMsg)
			} else {
				if c.txnBuilder != nil {
					panic("unreachable code: txn builder should be nil if we receive a non-txn message")
				}
				c.msgHandler.Handle(newImmutableMsg)
			}
		case *streamingpb.ConsumeResponse_Close:
			// Should receive io.EOF after that.
			// Do nothing at current implementation.
		default:
			c.logger.Warn("unknown response type", zap.Any("response", resp))
		}
	}
}

func (c *consumerImpl) handleTxnMessage(msg message.ImmutableMessage) {
	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		if c.txnBuilder != nil {
			panic("unreachable code: txn builder should be nil if we receive a begin txn message")
		}
		beginMsg, err := message.AsImmutableBeginTxnMessageV2(msg)
		if err != nil {
			c.logger.Warn("failed to convert message to begin txn message", zap.Any("messageID", beginMsg.MessageID()), zap.Error(err))
			return
		}
		c.txnBuilder = message.NewImmutableTxnMessageBuilder(beginMsg)
	case message.MessageTypeCommitTxn:
		if c.txnBuilder == nil {
			panic("unreachable code: txn builder should not be nil if we receive a commit txn message")
		}
		commitMsg, err := message.AsImmutableCommitTxnMessageV2(msg)
		if err != nil {
			c.logger.Warn("failed to convert message to commit txn message", zap.Any("messageID", commitMsg.MessageID()), zap.Error(err))
			c.txnBuilder = nil
			return
		}
		msg, err := c.txnBuilder.Build(commitMsg)
		c.txnBuilder = nil
		if err != nil {
			c.logger.Warn("failed to build txn message", zap.Any("messageID", commitMsg.MessageID()), zap.Error(err))
			return
		}
		c.msgHandler.Handle(msg)
	default:
		if c.txnBuilder == nil {
			panic("unreachable code: txn builder should not be nil if we receive a non-begin txn message")
		}
		c.txnBuilder.Add(msg)
	}
}
