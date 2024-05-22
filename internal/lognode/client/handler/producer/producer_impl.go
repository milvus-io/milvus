package producer

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"go.uber.org/zap"
)

// CreateProducer create a new producer client.
func CreateProducer(
	ctx context.Context,
	opts *options.ProducerOptions,
	handler logpb.LogNodeHandlerServiceClient,
	assignment *assignment.Assignment,
) (*ProducerImpl, error) {
	// select server to consume.
	ctx = contextutil.WithPickServerID(ctx, assignment.ServerID)
	// select channel to consume.
	ctx = contextutil.WithCreateProducer(ctx, &logpb.CreateProducerRequest{
		ChannelName: opts.Channel,
		Term:        assignment.Term,
	})
	streamClient, err := handler.Produce(ctx)
	if err != nil {
		return nil, err
	}

	// Initialize the producer client.
	produceClient := &produceGrpcClient{
		streamClient,
	}

	// Recv the first response from server.
	// It must be a create response.
	resp, err := produceClient.Recv()
	if err != nil {
		return nil, err
	}
	createResp := resp.GetCreate()
	if createResp == nil {
		return nil, status.NewInvalidRequestSeq("first message arrive must be create response")
	}

	// Initialize the producer client finished.
	cli := &ProducerImpl{
		channel:          opts.Channel,
		logger:           log.With(zap.String("channel", opts.Channel)),
		lifetime:         lifetime.NewLifetime[lifetime.State](lifetime.Working),
		idAllocator:      util.NewIDAllocator(),
		grpcStreamClient: produceClient,
		requestCh:        make(chan *produceRequest),
		sendExitCh:       make(chan struct{}),
		finishedCh:       make(chan struct{}),
	}

	// Start the producer client.
	go cli.execute()
	return cli, nil
}

// Expected message sequence:
// CreateProducer
// ProduceRequest 1 -> ProduceResponse Or Error 1
// ProduceRequest 2 -> ProduceResponse Or Error 2
// ProduceRequest 3 -> ProduceResponse Or Error 3
// CloseProducer
type ProducerImpl struct {
	channel          string
	logger           *log.MLogger
	lifetime         lifetime.Lifetime[lifetime.State]
	idAllocator      *util.IDAllocator
	grpcStreamClient *produceGrpcClient

	pendingRequests sync.Map
	requestCh       chan *produceRequest
	sendExitCh      chan struct{}
	finishedCh      chan struct{}
}

type produceRequest struct {
	ctx    context.Context
	msg    message.MutableMessage
	respCh chan produceResponse
}

type produceResponse struct {
	id  message.MessageID
	err error
}

// Produce sends the produce message to server.
func (p *ProducerImpl) Produce(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if p.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("producer client is shutting down")
	}
	defer p.lifetime.Done()

	respCh := make(chan produceResponse, 1)
	req := &produceRequest{
		ctx:    ctx,
		msg:    msg,
		respCh: respCh,
	}

	// Send the produce message to server.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case p.requestCh <- req:
	case <-p.sendExitCh:
		return nil, status.NewInner("producer stream client is closed")
	}

	// Wait for the response from server or context timeout.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp.id, resp.err
	}
}

// execute executes the producer client.
func (p *ProducerImpl) execute() {
	defer close(p.finishedCh)

	errSendCh := p.startSend()
	errRecvCh := p.startRecv()

	// Wait for send and recv arm to exit.
	err := <-errRecvCh
	<-errSendCh

	// Clear all pending request.
	p.pendingRequests.Range(func(key, value interface{}) bool {
		value.(*produceRequest).respCh <- produceResponse{
			err: status.NewUnknownError(fmt.Sprintf("request sent but no response returned, %s", err.Error())),
		}
		return true
	})
}

// IsAvailable returns whether the producer is available.
func (p *ProducerImpl) IsAvailable() bool {
	select {
	case <-p.sendExitCh:
		return false
	default:
		return true
	}
}

// Available returns a channel that will be closed when the producer is unavailable.
func (p *ProducerImpl) Available() <-chan struct{} {
	return p.sendExitCh
}

// Close close the producer client.
func (p *ProducerImpl) Close() {
	// Wait for all message has been sent.
	p.lifetime.SetState(lifetime.Stopped)
	p.lifetime.Wait()
	close(p.requestCh)

	// Wait for send and recv arm to exit.
	<-p.finishedCh
	return
}

// startSend starts the send loop.
func (p *ProducerImpl) startSend() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = p.sendLoop()
		close(ch)
	}()
	return ch
}

// startRecv starts the recv loop.
func (p *ProducerImpl) startRecv() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.recvLoop()
	}()
	return errCh
}

// sendLoop sends the produce message to server.
func (p *ProducerImpl) sendLoop() (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("send arm of stream closed")
		}
		close(p.sendExitCh)
		if err := p.grpcStreamClient.CloseSend(); err != nil {
			p.logger.Warn("failed to close send", zap.Error(err))
		}
	}()

	for {
		select {
		case req, ok := <-p.requestCh:
			if !ok {
				// all message has been sent, sent close response.
				return p.grpcStreamClient.SendClose()
			}
			requestID := int64(p.idAllocator.Allocate())
			// Store the request to pending request map.
			p.pendingRequests.Store(requestID, req)
			// Send the produce message to server.
			if err := p.grpcStreamClient.SendProduceMessage(int64(requestID), req.msg); err != nil {
				// If send failed, remove the request from pending request map and return error to client.
				p.notifyRequest(requestID, produceResponse{
					err: err,
				})
				return err
			}
			p.logger.Debug("send produce message to server", zap.Int64("requestID", requestID))
		}
	}
}

// recvLoop receives the produce response from server.
func (p *ProducerImpl) recvLoop() (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("recv arm of stream closed")
		}
	}()

	for {
		resp, err := p.grpcStreamClient.Recv()
		if errors.Is(err, io.EOF) {
			p.logger.Debug("stream closed successful")
			return nil
		}
		if err != nil {
			return err
		}
		switch resp := resp.Response.(type) {
		case *logpb.ProduceResponse_Produce:
			var result produceResponse
			switch produceResp := resp.Produce.Response.(type) {
			case *logpb.ProduceMessageResponse_Result:
				result = produceResponse{
					id: message.NewMessageIDFromPBMessageID(produceResp.Result.Id),
				}
			case *logpb.ProduceMessageResponse_Error:
				result = produceResponse{
					err: status.New(produceResp.Error.Code, produceResp.Error.Cause),
				}
			default:
				panic("unreachable")
			}
			p.notifyRequest(resp.Produce.RequestID, result)
		case *logpb.ProduceResponse_Close:
		default:
			// skip message here.
			p.logger.Error("unknown response type", zap.Any("response", resp))
		}
	}
}

// notifyRequest notify the request has been returned from server.
func (p *ProducerImpl) notifyRequest(requestID int64, resp produceResponse) {
	pendingRequest, loaded := p.pendingRequests.LoadAndDelete(requestID)
	if loaded {
		p.logger.Debug("recv send produce message from server", zap.Int64("requestID", requestID))
		pendingRequest.(*produceRequest).respCh <- resp
	}
}
