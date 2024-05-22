package consumer

import (
	"context"
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/lognode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// CreateConsumer creates a new consumer client.
func CreateConsumer(
	ctx context.Context,
	opts *options.ConsumerOptions,
	handlerClient logpb.LogNodeHandlerServiceClient,
	assignment *assignment.Assignment,
) (*ConsumerImpl, error) {
	// select server to consume.
	ctx = contextutil.WithPickServerID(ctx, assignment.ServerID)
	// select channel to consume.
	ctx = contextutil.WithCreateConsumer(ctx, &logpb.CreateConsumerRequest{
		ChannelName:   opts.Channel,
		Term:          assignment.Term,
		DeliverPolicy: opts.DeliverPolicy,
	})
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
	cli := &ConsumerImpl{
		channel:          opts.Channel,
		grpcStreamClient: streamClient,
		handlerClient:    handlerClient,
		logger:           log.With(zap.String("channel", opts.Channel)),
		msgHandler:       opts.MessageHandler,
		finishCh:         make(chan struct{}),
		err:              nil,
	}
	go cli.execute()
	return cli, nil
}

type ConsumerImpl struct {
	channel          string
	grpcStreamClient logpb.LogNodeHandlerService_ConsumeClient
	handlerClient    logpb.LogNodeHandlerServiceClient
	logger           *log.MLogger
	msgHandler       message.Handler
	finishCh         chan struct{}
	err              error
}

// Close close the consumer client.
func (c *ConsumerImpl) Close() {
	// Send the close request to server.
	if err := c.grpcStreamClient.Send(&logpb.ConsumeRequest{
		Request: &logpb.ConsumeRequest_Close{},
	}); err != nil {
		c.logger.Warn("send close request failed", zap.Error(err))
	}
	// close the grpc client stream.
	if err := c.grpcStreamClient.CloseSend(); err != nil {
		c.logger.Warn("close grpc stream failed", zap.Error(err))
	}
	<-c.finishCh
}

// Error returns the error of the consumer client.
func (c *ConsumerImpl) Error() error {
	<-c.finishCh
	return c.err
}

// Done returns a channel that closes when the consumer client is closed.
func (c *ConsumerImpl) Done() <-chan struct{} {
	return c.finishCh
}

// execute starts the recv loop.
func (c *ConsumerImpl) execute() {
	c.recvLoop()
	close(c.finishCh)
}

// recvLoop is the recv arm of the grpc stream.
// Throughput of the grpc framework should be ok to use single stream to receive message.
// Once throughput is not enough, look at https://grpc.io/docs/guides/performance/ to find the solution.
func (c *ConsumerImpl) recvLoop() (err error) {
	defer func() {
		if err != nil {
			c.logger.Warn("recv arm of stream closed with unexpected error", zap.Error(err))
		} else {
			c.logger.Info("recv arm of stream closed")
		}
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
		case *logpb.ConsumeResponse_Consume:
			c.msgHandler.Handle(message.NewImmutableMessageFromPBMessage(resp.Consume.Id, resp.Consume.Message))
		case *logpb.ConsumeResponse_Close:
			return nil
		default:
			panic("unreachable")
		}
	}
}
