package consumer

import (
	"context"
	"io"
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// ConsumerOptions is the options for creating a consumer.
type ConsumerOptions struct {
	// The cosume target
	Assignment *types.PChannelInfoAssigned

	// VChannel is the vchannel of the consumer.
	VChannel string

	// DeliverPolicy is the deliver policy of the consumer.
	DeliverPolicy options.DeliverPolicy

	// DeliverFilters is the deliver filters of the consumer.
	DeliverFilters []options.DeliverFilter

	// Handler is the message handler used to handle message after recv from consumer.
	MessageHandler message.Handler

	// IgnorePauseConsumption is the flag to ignore the consumption pause of the consumer.
	IgnorePauseConsumption bool
}

// CreateConsumer creates a new consumer client.
func CreateConsumer(
	ctx context.Context,
	opts *ConsumerOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (Consumer, error) {
	ctx = contextutil.WithCreateConsumer(ctx, &streamingpb.CreateConsumerRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})

	// TODO: configurable or auto adjust grpc.MaxCallRecvMsgSize
	// The messages are always managed by milvus cluster, so the size of message shouldn't be controlled here
	// to avoid infinitely blocks.
	streamClient, err := handlerClient.Consume(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
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
		ctx:              ctx,
		walName:          createResp.GetWalName(),
		opts:             opts,
		grpcStreamClient: streamClient,
		handlerClient:    handlerClient,
		logger: mlog.With(
			mlog.String("walName", createResp.GetWalName()),
			mlog.String("pchannel", opts.Assignment.Channel.Name),
			mlog.Int64("term", opts.Assignment.Channel.Term),
			mlog.Int64("streamingNodeID", opts.Assignment.Node.ServerID)),
		msgHandler: opts.MessageHandler,
		finishErr:  syncutil.NewFuture[error](),
	}
	go cli.execute()
	return cli, nil
}

type consumerImpl struct {
	ctx              context.Context // TODO: the cancel method of consumer should be managed by consumerImpl, fix it in future.
	walName          string
	opts             *ConsumerOptions
	grpcStreamClient streamingpb.StreamingNodeHandlerService_ConsumeClient
	handlerClient    streamingpb.StreamingNodeHandlerServiceClient
	logger           *mlog.Logger
	msgHandler       message.Handler
	finishErr        *syncutil.Future[error]
	txnBuilder       *message.ImmutableTxnMessageBuilder
}

// Close close the consumer client.
func (c *consumerImpl) Close() error {
	// Send the close request to server.
	if err := c.grpcStreamClient.Send(&streamingpb.ConsumeRequest{
		Request: &streamingpb.ConsumeRequest_Close{},
	}); err != nil {
		c.logger.Warn(c.ctx, "send close request failed", mlog.Err(err))
	}
	// close the grpc client stream.
	if err := c.grpcStreamClient.CloseSend(); err != nil {
		c.logger.Warn(c.ctx, "close grpc stream failed", mlog.Err(err))
	}
	return c.finishErr.Get()
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
			c.logger.Warn(c.ctx, "recv arm of stream closed with unexpected error", mlog.Err(err))
		} else {
			c.logger.Info(c.ctx, "recv arm of stream closed")
		}
		c.finishErr.Set(err)
		c.msgHandler.Close()
	}()
	if err := c.createVChannelConsumer(); err != nil {
		return err
	}
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
			msgID, err := message.UnmarshalMessageID(resp.Consume.Message.Id)
			if err != nil {
				return err
			}
			newImmutableMsg := message.NewImmutableMesasge(
				msgID,
				resp.Consume.GetMessage().GetPayload(),
				resp.Consume.GetMessage().GetProperties(),
			)
			msgCtx := message.ExtractTraceContext(c.ctx, newImmutableMsg)
			if newImmutableMsg.TxnContext() != nil {
				if err := c.handleTxnMessage(msgCtx, newImmutableMsg); err != nil {
					return err
				}
			} else {
				if c.txnBuilder != nil {
					panic("unreachable code: txn builder should be nil if we receive a non-txn message")
				}
				msgCtx = startDistConsumeSpanForMessage(msgCtx, newImmutableMsg)
				if result := c.msgHandler.Handle(message.HandleParam{
					Ctx:     msgCtx,
					Message: newImmutableMsg,
				}); result.Error != nil {
					c.logger.Warn(c.ctx, "message handle canceled", mlog.Err(err))
					return errors.Wrapf(result.Error, "At Handler")
				}
			}
		case *streamingpb.ConsumeResponse_Close:
			// Should receive io.EOF after that.
			// Do nothing at current implementation.
		default:
			c.logger.Warn(c.ctx, "unknown response type", mlog.Any("response", resp))
		}
	}
}

func (c *consumerImpl) createVChannelConsumer() error {
	// Create the vchannel client.
	if err := c.grpcStreamClient.Send(&streamingpb.ConsumeRequest{
		Request: &streamingpb.ConsumeRequest_CreateVchannelConsumer{
			CreateVchannelConsumer: &streamingpb.CreateVChannelConsumerRequest{
				Vchannel:               c.opts.VChannel,
				DeliverPolicy:          c.opts.DeliverPolicy,
				DeliverFilters:         c.opts.DeliverFilters,
				IgnorePauseConsumption: c.opts.IgnorePauseConsumption,
			},
		},
	}); err != nil {
		return err
	}
	resp, err := c.grpcStreamClient.Recv()
	if err != nil {
		return err
	}
	createVChannelResp := resp.GetCreateVchannel()
	if createVChannelResp == nil {
		return status.NewInvalidRequestSeq("expect create vchannel response")
	}
	return nil
}

func (c *consumerImpl) handleTxnMessage(ctx context.Context, msg message.ImmutableMessage) error {
	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		if c.txnBuilder != nil {
			panic("unreachable code: txn builder should be nil if we receive a begin txn message")
		}
		beginMsg, err := message.AsImmutableBeginTxnMessageV2(msg)
		if err != nil {
			c.logger.Warn(c.ctx, "failed to convert message to begin txn message", mlog.Any("messageID", beginMsg.MessageID()), mlog.Err(err))
			return nil
		}
		c.txnBuilder = message.NewImmutableTxnMessageBuilder(beginMsg)
	case message.MessageTypeCommitTxn:
		if c.txnBuilder == nil {
			panic("unreachable code: txn builder should not be nil if we receive a commit txn message")
		}
		commitMsg, err := message.AsImmutableCommitTxnMessageV2(msg)
		if err != nil {
			c.logger.Warn(c.ctx, "failed to convert message to commit txn message", mlog.Any("messageID", commitMsg.MessageID()), mlog.Err(err))
			c.txnBuilder = nil
			return nil
		}
		msg, err := c.txnBuilder.Build(commitMsg)
		c.txnBuilder = nil
		if err != nil {
			c.logger.Warn(c.ctx, "failed to build txn message", mlog.Any("messageID", commitMsg.MessageID()), mlog.Err(err))
			return nil
		}
		ctx = startDistConsumeSpanForMessage(ctx, msg)
		overwriteTxnMessagesTraceContext(ctx, message.AsImmutableTxnMessage(msg))
		if result := c.msgHandler.Handle(message.HandleParam{
			Ctx:     ctx,
			Message: msg,
		}); result.Error != nil {
			c.logger.Warn(c.ctx, "message handle canceled at txn", mlog.Err(result.Error))
			return errors.Wrapf(result.Error, "At Handler Of Txn")
		}
	default:
		if c.txnBuilder == nil {
			panic("unreachable code: txn builder should not be nil if we receive a non-begin txn message")
		}
		c.txnBuilder.Add(msg)
	}
	return nil
}

func startDistConsumeSpanForMessage(ctx context.Context, msg message.ImmutableMessage) context.Context {
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALDistConsume)
	message.OverwriteTraceContext(ctx, msg)
	span.End()
	return ctx
}

func overwriteTxnMessagesTraceContext(ctx context.Context, txnMsg message.ImmutableTxnMessage) {
	message.OverwriteTraceContext(ctx, txnMsg.Begin())
	_ = txnMsg.RangeOver(func(msg message.ImmutableMessage) error {
		message.OverwriteTraceContext(ctx, msg)
		return nil
	})
	message.OverwriteTraceContext(ctx, txnMsg.Commit())
}
