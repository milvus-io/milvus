package producer

import (
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/typeconverter"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
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
		return nil, errors.Wrap(err, "at get create producer request")
	}
	l, err := walManager.GetAvailableWAL(typeconverter.NewPChannelInfoFromProto(createReq.Pchannel))
	if err != nil {
		return nil, errors.Wrap(err, "at get available wal")
	}

	produceServer := &produceGrpcServerHelper{
		StreamingNodeHandlerService_ProduceServer: streamServer,
	}
	if err := produceServer.SendCreated(); err != nil {
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
	// sender: recv arm, receiver: send arm.
	recvDoneSignal := syncutil.NewFuture[error]()

	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = p.recvLoop(recvDoneSignal)
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv closed and all response is sent.
	return p.sendLoop(recvDoneSignal)
}

// sendLoop sends the message to client.
func (p *ProduceServer) sendLoop(recvDoneSignal *syncutil.Future[error]) (err error) {
	defer func() {
		recvErr := recvDoneSignal.Get()
		if err != nil || recvErr != nil {
			p.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err), zap.NamedError("recvErr", recvErr))
		} else {
			p.logger.Info("send arm of stream closed")
		}
	}()
	for {
		select {
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

// recvLoop receives the message from client.
func (p *ProduceServer) recvLoop(recvDoneSignal *syncutil.Future[error]) (err error) {
	defer func() {
		p.logger.Info("recv arm of stream start to close, waiting for all append request done")
		p.appendWG.Wait()
		close(p.produceMessageCh)
		recvDoneSignal.Set(err)

		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("recv arm of stream closed")
		}
	}()

	for {
		req, err := p.produceServer.Recv()
		if err == io.EOF {
			p.logger.Warn("stream closed by client unexpectedly")
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *streamingpb.ProduceRequest_Produce:
			p.handleProduce(req.Produce)
		case *streamingpb.ProduceRequest_Close:
			return nil
			// we will receive EOF after that.
		default:
			// skip message here, to keep the forward compatibility.
			p.logger.Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// handleProduce handles the produce message request.
func (p *ProduceServer) handleProduce(req *streamingpb.ProduceMessageRequest) {
	p.logger.Debug("recv produce message from client", zap.Int64("requestID", req.RequestId))
	msg := message.NewMutableMessageBuilder().
		WithPayload(req.GetMessage().GetPayload()).
		WithProperties(req.GetMessage().GetProperties()).
		BuildMutable()

	if err := p.validateMessage(msg); err != nil {
		p.logger.Warn("produce message validation failed", zap.Int64("requestID", req.RequestId), zap.Error(err))
		p.sendProduceResult(req.RequestId, nil, err)
		return
	}

	// Append message to wal.
	// Concurrent append request can be executed concurrently.
	messageSize := msg.EstimateSize()
	now := time.Now()
	p.appendWG.Add(1)
	p.wal.AppendAsync(p.produceServer.Context(), msg, func(id message.MessageID, err error) {
		defer func() {
			p.appendWG.Done()
			p.updateMetrics(messageSize, time.Since(now).Seconds(), err)
		}()
		p.sendProduceResult(req.RequestId, id, err)
	})
}

// validateMessage validates the message.
func (p *ProduceServer) validateMessage(msg message.MutableMessage) error {
	// validate the msg.
	if !msg.Version().GT(message.VersionOld) {
		return status.NewInner("unsupported message version")
	}
	if !msg.MessageType().Valid() {
		return status.NewInner("unsupported message type")
	}
	if msg.Payload() == nil {
		return status.NewInner("empty payload for message")
	}
	return nil
}

// sendProduceResult sends the produce result to client.
func (p *ProduceServer) sendProduceResult(reqID int64, id message.MessageID, err error) {
	resp := &streamingpb.ProduceMessageResponse{
		RequestId: reqID,
	}
	if err != nil {
		resp.Response = &streamingpb.ProduceMessageResponse_Error{
			Error: status.AsStreamingError(err).AsPBError(),
		}
	} else {
		resp.Response = &streamingpb.ProduceMessageResponse_Result{
			Result: &streamingpb.ProduceMessageResponseResult{
				Id: &streamingpb.MessageID{
					Id: id.Marshal(),
				},
			},
		}
	}

	// If server context is canceled, it means the stream has been closed.
	// all pending response message should be dropped, client side will handle it.
	select {
	case p.produceMessageCh <- resp:
		p.logger.Debug("send produce message response to client", zap.Int64("requestID", reqID), zap.Any("messageID", id), zap.Error(err))
	case <-p.produceServer.Context().Done():
		p.logger.Warn("stream closed before produce message response sent", zap.Int64("requestID", reqID), zap.Any("messageID", id))
		return
	}
}

// updateMetrics updates the metrics.
func (p *ProduceServer) updateMetrics(messageSize int, cost float64, err error) {
	name := p.wal.Channel().Name
	term := strconv.FormatInt(p.wal.Channel().Term, 10)
	if err == nil {
		metrics.StreamingNodeProduceBytes.WithLabelValues(paramtable.GetStringNodeID(), name, term).Observe(float64(messageSize))
	}
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
