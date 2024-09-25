package producer

import (
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// CreateProduceServer create a new producer.
// Expected message sequence:
// CreateProducer (Header)
// ProduceRequest 1 -> ProduceResponse Or Error 1
// ProduceRequest 2 -> ProduceResponse Or Error 2
// ProduceRequest 3 -> ProduceResponse Or Error 3
// CloseProducer
func CreateProduceServer(walManager walmanager.Manager, streamServer streamingpb.StreamingNodeHandlerService_ProduceServer) (*ProduceServer, error) {
	createReq, err := contextutil.GetCreateProducer(streamServer.Context())
	if err != nil {
		return nil, status.NewInvaildArgument("create producer request is required")
	}
	l, err := walManager.GetAvailableWAL(types.NewPChannelInfoFromProto(createReq.GetPchannel()))
	if err != nil {
		return nil, err
	}

	produceServer := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: streamServer,
	}
	if err := produceServer.SendCreated(&streamingpb.CreateProducerResponse{
		WalName: l.WALName(),
	}); err != nil {
		return nil, errors.Wrap(err, "at send created")
	}
	return &ProduceServer{
		wal:              l,
		produceServer:    produceServer,
		logger:           log.With(zap.String("channel", l.Channel().Name), zap.Int64("term", l.Channel().Term)),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse),
		appendWG:         sync.WaitGroup{},
	}, nil
}

// ProduceServer is a ProduceServer of log messages.
type ProduceServer struct {
	wal              wal.WAL
	produceServer    *produceGrpcServerHelper
	logger           *log.MLogger
	produceMessageCh chan *streamingpb.ProduceMessageResponse // All processing messages result should sent from theses channel.
	appendWG         sync.WaitGroup
}

// Execute starts the producer.
func (p *ProduceServer) Execute() error {
	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = p.recvLoop()
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv closed and all response is sent.
	return p.sendLoop()
}

// sendLoop sends the message to client.
func (p *ProduceServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		p.logger.Info("send arm of stream closed")
	}()
	available := p.wal.Available()
	var appendWGDoneChan <-chan struct{}

	for {
		select {
		case <-available:
			// If the wal is not available any more, we should stop sending message, and close the server.
			// appendWGDoneChan make a graceful shutdown for those case.
			available = nil
			appendWGDoneChan = p.getWaitAppendChan()
		case <-appendWGDoneChan:
			// All pending append request has been finished, we can close the streaming server now.
			// Recv arm will be closed by context cancel of stream server.
			// Send an unavailable response to ask client to release resource.
			p.produceServer.SendClosed()
			return errors.New("send loop is stopped for close of wal")
		case resp, ok := <-p.produceMessageCh:
			if !ok {
				// all message has been sent, sent close response.
				p.produceServer.SendClosed()
				return nil
			}
			if err := p.produceServer.SendProduceMessage(resp); err != nil {
				return err
			}
		case <-p.produceServer.Context().Done():
			return errors.Wrap(p.produceServer.Context().Err(), "cancel send loop by stream server")
		}
	}
}

// getWaitAppendChan returns the channel that can be used to wait for the append operation.
func (p *ProduceServer) getWaitAppendChan() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		p.appendWG.Wait()
		close(ch)
	}()
	return ch
}

// recvLoop receives the message from client.
func (p *ProduceServer) recvLoop() (err error) {
	defer func() {
		p.appendWG.Wait()
		close(p.produceMessageCh)
		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		p.logger.Info("recv arm of stream closed")
	}()

	for {
		req, err := p.produceServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *streamingpb.ProduceRequest_Produce:
			p.handleProduce(req.Produce)
		case *streamingpb.ProduceRequest_Close:
			p.logger.Info("recv arm of stream start to close, waiting for all append request finished...")
			// we will receive io.EOF after that.
		default:
			// skip message here, to keep the forward compatibility.
			p.logger.Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// handleProduce handles the produce message request.
func (p *ProduceServer) handleProduce(req *streamingpb.ProduceMessageRequest) {
	// Stop handling if the wal is not available any more.
	// The counter of  appendWG will never increased.
	if !p.wal.IsAvailable() {
		return
	}

	p.appendWG.Add(1)
	p.logger.Debug("recv produce message from client", zap.Int64("requestID", req.RequestId))
	msg := message.NewMutableMessage(req.GetMessage().GetPayload(), req.GetMessage().GetProperties())
	if err := p.validateMessage(msg); err != nil {
		p.logger.Warn("produce message validation failed", zap.Int64("requestID", req.RequestId), zap.Error(err))
		p.sendProduceResult(req.RequestId, nil, err)
		p.appendWG.Done()
		return
	}

	// Append message to wal.
	// Concurrent append request can be executed concurrently.
	messageSize := msg.EstimateSize()
	now := time.Now()
	p.wal.AppendAsync(p.produceServer.Context(), msg, func(appendResult *wal.AppendResult, err error) {
		defer func() {
			p.appendWG.Done()
			p.updateMetrics(messageSize, time.Since(now).Seconds(), err)
		}()
		p.sendProduceResult(req.RequestId, appendResult, err)
	})
}

// validateMessage validates the message.
func (p *ProduceServer) validateMessage(msg message.MutableMessage) error {
	// validate the msg.
	if !msg.Version().GT(message.VersionOld) {
		return status.NewInvaildArgument("unsupported message version")
	}
	if !msg.MessageType().Valid() {
		return status.NewInvaildArgument("unsupported message type")
	}
	return nil
}

// sendProduceResult sends the produce result to client.
func (p *ProduceServer) sendProduceResult(reqID int64, appendResult *wal.AppendResult, err error) {
	resp := &streamingpb.ProduceMessageResponse{
		RequestId: reqID,
	}
	if err != nil {
		p.logger.Warn("append message to wal failed", zap.Int64("requestID", reqID), zap.Error(err))
		resp.Response = &streamingpb.ProduceMessageResponse_Error{
			Error: status.AsStreamingError(err).AsPBError(),
		}
	} else {
		resp.Response = &streamingpb.ProduceMessageResponse_Result{
			Result: &streamingpb.ProduceMessageResponseResult{
				Id: &messagespb.MessageID{
					Id: appendResult.MessageID.Marshal(),
				},
				Timetick:   appendResult.TimeTick,
				TxnContext: appendResult.TxnCtx.IntoProto(),
				Extra:      appendResult.Extra,
			},
		}
	}

	// If server context is canceled, it means the stream has been closed.
	// all pending response message should be dropped, client side will handle it.
	select {
	case p.produceMessageCh <- resp:
		p.logger.Debug("send produce message response to client", zap.Int64("requestID", reqID), zap.Any("appendResult", appendResult), zap.Error(err))
	case <-p.produceServer.Context().Done():
		p.logger.Warn("stream closed before produce message response sent", zap.Int64("requestID", reqID), zap.Any("appendResult", appendResult), zap.Error(err))
		return
	}
}

// updateMetrics updates the metrics.
func (p *ProduceServer) updateMetrics(messageSize int, cost float64, err error) {
	name := p.wal.Channel().Name
	term := strconv.FormatInt(p.wal.Channel().Term, 10)
	metrics.StreamingNodeProduceBytes.WithLabelValues(paramtable.GetStringNodeID(), name, term, getStatusLabel(err)).Observe(float64(messageSize))
	metrics.StreamingNodeProduceDurationSeconds.WithLabelValues(paramtable.GetStringNodeID(), name, term, getStatusLabel(err)).Observe(cost)
}

// getStatusLabel returns the status label of error.
func getStatusLabel(err error) string {
	if status.IsCanceled(err) {
		return metrics.CancelLabel
	}
	if err != nil {
		return metrics.FailLabel
	}
	return metrics.SuccessLabel
}
