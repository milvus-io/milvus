package producer

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	// The produce target
	Assignment *types.PChannelInfoAssigned
}

// CreateProducer create a new producer client.
func CreateProducer(
	ctx context.Context,
	opts *ProducerOptions,
	handler streamingpb.StreamingNodeHandlerServiceClient,
) (Producer, error) {
	ctx = createProduceRequest(ctx, opts)
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
	cli := &producerImpl{
		assignment: *opts.Assignment,
		walName:    createResp.GetWalName(),
		logger: log.With(
			zap.String("walName", createResp.GetWalName()),
			zap.String("pchannel", opts.Assignment.Channel.Name),
			zap.Int64("term", opts.Assignment.Channel.Term),
			zap.Int64("streamingNodeID", opts.Assignment.Node.ServerID)),
		lifetime:         typeutil.NewLifetime(),
		idAllocator:      typeutil.NewIDAllocator(),
		grpcStreamClient: produceClient,
		pendingRequests:  sync.Map{},
		requestCh:        make(chan *produceRequest),
		sendExitCh:       make(chan struct{}),
		recvExitCh:       make(chan struct{}),
		finishedCh:       make(chan struct{}),
	}

	// Start the producer client.
	go cli.execute()
	return cli, nil
}

// createProduceRequest creates the produce request.
func createProduceRequest(ctx context.Context, opts *ProducerOptions) context.Context {
	// select server to consume.
	ctx = contextutil.WithPickServerID(ctx, opts.Assignment.Node.ServerID)
	// select channel to consume.
	return contextutil.WithCreateProducer(ctx, &streamingpb.CreateProducerRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})
}

// Expected message sequence:
// CreateProducer
// ProduceRequest 1 -> ProduceResponse Or Error 1
// ProduceRequest 2 -> ProduceResponse Or Error 2
// ProduceRequest 3 -> ProduceResponse Or Error 3
// CloseProducer
type producerImpl struct {
	assignment       types.PChannelInfoAssigned
	walName          string
	logger           *log.MLogger
	lifetime         *typeutil.Lifetime
	idAllocator      *typeutil.IDAllocator
	grpcStreamClient *produceGrpcClient

	pendingRequests sync.Map
	requestCh       chan *produceRequest
	sendExitCh      chan struct{}
	recvExitCh      chan struct{}
	finishedCh      chan struct{}
}

type produceRequest struct {
	ctx    context.Context
	msg    message.MutableMessage
	respCh chan produceResponse
}

type produceResponse struct {
	result *ProduceResult
	err    error
}

// Assignment returns the assignment of the producer.
func (p *producerImpl) Assignment() types.PChannelInfoAssigned {
	return p.assignment
}

// Produce sends the produce message to server.
func (p *producerImpl) Produce(ctx context.Context, msg message.MutableMessage) (*ProduceResult, error) {
	if !p.lifetime.Add(typeutil.LifetimeStateWorking) {
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
		return nil, status.NewInner("producer send arm is closed")
	case <-p.recvExitCh:
		return nil, status.NewInner("producer recv arm is closed")
	}

	// Wait for the response from server or context timeout.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp.result, resp.err
	}
}

// execute executes the producer client.
func (p *producerImpl) execute() {
	defer close(p.finishedCh)

	errSendCh := p.startSend()
	errRecvCh := p.startRecv()

	// Wait for send and recv arm to exit.
	err := <-errRecvCh
	<-errSendCh

	// Clear all pending request.
	p.pendingRequests.Range(func(key, value interface{}) bool {
		value.(*produceRequest).respCh <- produceResponse{
			err: status.NewUnknownError(fmt.Sprintf("request sent but no response returned, %v", err)),
		}
		return true
	})
}

// IsAvailable returns whether the producer is available.
func (p *producerImpl) IsAvailable() bool {
	select {
	case <-p.Available():
		return false
	default:
		return true
	}
}

// Available returns a channel that will be closed when the producer is unavailable.
func (p *producerImpl) Available() <-chan struct{} {
	return p.sendExitCh
}

// Close close the producer client.
func (p *producerImpl) Close() {
	// Wait for all message has been sent.
	p.lifetime.SetState(typeutil.LifetimeStateStopped)
	p.lifetime.Wait()
	close(p.requestCh)

	// Wait for send and recv arm to exit.
	<-p.finishedCh
}

// startSend starts the send loop.
func (p *producerImpl) startSend() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		_ = p.sendLoop()
		close(ch)
	}()
	return ch
}

// startRecv starts the recv loop.
func (p *producerImpl) startRecv() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.recvLoop()
	}()
	return errCh
}

// sendLoop sends the produce message to server.
func (p *producerImpl) sendLoop() (err error) {
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
		case <-p.recvExitCh:
			return errors.New("recv arm of stream closed")
		case req, ok := <-p.requestCh:
			if !ok {
				// all message has been sent, sent close response.
				return p.grpcStreamClient.SendClose()
			}
			requestID := p.idAllocator.Allocate()
			// Store the request to pending request map.
			p.pendingRequests.Store(requestID, req)
			// Send the produce message to server.
			if err := p.grpcStreamClient.SendProduceMessage(requestID, req.msg); err != nil {
				// If send failed, remove the request from pending request map and return error to client.
				p.notifyRequest(requestID, produceResponse{
					err: err,
				})
				return err
			}
		}
	}
}

// recvLoop receives the produce response from server.
func (p *producerImpl) recvLoop() (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		p.logger.Info("recv arm of stream closed")
		close(p.recvExitCh)
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
		case *streamingpb.ProduceResponse_Produce:
			var result produceResponse
			switch produceResp := resp.Produce.Response.(type) {
			case *streamingpb.ProduceMessageResponse_Result:
				msgID, err := message.UnmarshalMessageID(
					p.walName,
					produceResp.Result.GetId().GetId(),
				)
				if err != nil {
					return err
				}
				result = produceResponse{
					result: &ProduceResult{
						MessageID: msgID,
						TimeTick:  produceResp.Result.GetTimetick(),
						TxnCtx:    message.NewTxnContextFromProto(produceResp.Result.GetTxnContext()),
						Extra:     produceResp.Result.GetExtra(),
					},
				}
			case *streamingpb.ProduceMessageResponse_Error:
				result = produceResponse{
					err: status.New(produceResp.Error.Code, produceResp.Error.Cause),
				}
			default:
				panic("unreachable")
			}
			p.notifyRequest(resp.Produce.RequestId, result)
		case *streamingpb.ProduceResponse_Close:
			// recv io.EOF after this message.
		default:
			// skip message here.
			p.logger.Error("unknown response type", zap.Any("response", resp))
		}
	}
}

// notifyRequest notify the request has been returned from server.
func (p *producerImpl) notifyRequest(requestID int64, resp produceResponse) {
	pendingRequest, loaded := p.pendingRequests.LoadAndDelete(requestID)
	if loaded {
		p.logger.Debug("recv send produce message from server", zap.Int64("requestID", requestID))
		pendingRequest.(*produceRequest).respCh <- resp
	}
}
