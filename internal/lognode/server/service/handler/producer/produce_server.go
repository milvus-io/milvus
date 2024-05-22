package producer

import (
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

const (
	producerStateRunning = iota
	producerStateClosed
)

// CreateProduceServer create a new producer.
// Expected message sequence:
// CreateProducer (Header)
// ProduceRequest 1 -> ProduceResponse Or Error 1
// ProduceRequest 2 -> ProduceResponse Or Error 2
// ProduceRequest 3 -> ProduceResponse Or Error 3
// CloseProducer
func CreateProduceServer(walManager walmanager.Manager, streamServer logpb.LogNodeHandlerService_ProduceServer) (*ProduceServer, error) {
	createReq, err := contextutil.GetCreateProducer(streamServer.Context())
	if err != nil {
		return nil, errors.Wrap(err, "at get create producer request")
	}
	l, err := walManager.GetAvailableWAL(createReq.ChannelName, createReq.Term)
	if err != nil {
		return nil, errors.Wrap(err, "at get available wal")
	}

	produceServer := &produceGrpcServerHelper{
		LogNodeHandlerService_ProduceServer: streamServer,
	}
	if err := produceServer.SendCreated(); err != nil {
		return nil, errors.Wrap(err, "at send created")
	}
	return &ProduceServer{
		wal:              l,
		produceServer:    produceServer,
		logger:           log.With(zap.String("channel", l.Channel().Name), zap.Int64("term", l.Channel().Term)),
		produceMessageCh: make(chan *logpb.ProduceMessageResponse),
		appendWG:         sync.WaitGroup{},
	}, nil
}

// ProduceServer is a ProduceServer of log messages.
type ProduceServer struct {
	wal              wal.WAL
	produceServer    *produceGrpcServerHelper
	logger           *log.MLogger
	produceMessageCh chan *logpb.ProduceMessageResponse // All processing messages result should sent from theses channel.
	appendWG         sync.WaitGroup
}

// Execute starts the producer.
func (p *ProduceServer) Execute() error {
	// sender: recv arm, receiver: send arm.
	recvFailureSignal := typeutil.NewChanSignal[error]()

	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = p.recvLoop(recvFailureSignal)
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv closed and all response is sent.
	return p.sendLoop(recvFailureSignal)
}

// sendLoop sends the message to client.
func (p *ProduceServer) sendLoop(recvFailureSignal typeutil.ChanSignalListener[error]) (err error) {
	defer func() {
		if err != nil {
			p.logger.Warn("send arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("send arm of stream closed")
		}
	}()
	ch := recvFailureSignal.Chan()
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
		case err = <-ch:
			// get recv arm error, unregister the channel signal.
			ch = nil
		case <-p.produceServer.Context().Done():
			return errors.Wrap(p.produceServer.Context().Err(), "cancel send loop by stream server")
		}
	}
}

// recvLoop receives the message from client.
func (p *ProduceServer) recvLoop(recvFailureSignal typeutil.ChanSignalNotifier[error]) (err error) {
	recvState := producerStateRunning
	defer func() {
		p.logger.Info("recv arm of stream start to close, waiting for all append request done")
		p.appendWG.Wait()
		close(p.produceMessageCh)
		recvFailureSignal.Release()

		if err != nil {
			p.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
		} else {
			p.logger.Info("recv arm of stream closed")
		}
	}()

	for {
		req, err := p.produceServer.Recv()
		if err == io.EOF {
			p.logger.Debug("stream closed by client")
			return nil
		}
		if err != nil {
			recvFailureSignal.MustNotify(err)
			return err
		}
		switch req := req.Request.(type) {
		case *logpb.ProduceRequest_Produce:
			if recvState != producerStateRunning {
				err = status.NewInvalidRequestSeq("unexpected stream rpc arrive sequence, producer already closed or not ready")
				recvFailureSignal.MustNotify(err)
				return err
			}
			p.handleProduce(req.Produce)
		case *logpb.ProduceRequest_Close:
			if recvState != producerStateRunning {
				err = status.NewInvalidRequestSeq("unexpected stream rpc arrive sequence, close unready producer")
				recvFailureSignal.MustNotify(err)
				return err
			}
			recvFailureSignal.MustNotify(nil)
			recvState = producerStateClosed
		default:
			// skip message here, to keep the forward compatibility.
			p.logger.Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// handleProduce handles the produce message request.
func (p *ProduceServer) handleProduce(req *logpb.ProduceMessageRequest) {
	p.logger.Debug("recv produce message from client", zap.Int64("requestID", req.RequestID))
	msg := message.NewMessageFromPBMessage(req.Message)

	// Append message to wal.
	// Concurrent append request can be executed concurrently.
	messageSize := msg.EstimateSize()
	now := time.Now()
	p.appendWG.Add(1)
	p.wal.AppendAsync(p.produceServer.Context(), msg, func(id message.MessageID, err error) {
		defer func() {
			p.appendWG.Done()
			if err == nil {
				metrics.LogNodeProduceBytes.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(messageSize))
			}
			metrics.LogNodeProduceDurationSeconds.WithLabelValues(paramtable.GetStringNodeID(), getStatusLabel(err)).Observe(float64(time.Since(now).Seconds()))
		}()
		p.sendProduceResult(req.RequestID, id, err)
	})
}

// sendProduceResult sends the produce result to client.
func (p *ProduceServer) sendProduceResult(reqID int64, id message.MessageID, err error) {
	resp := &logpb.ProduceMessageResponse{
		RequestID: reqID,
	}
	if err != nil {
		resp.Response = &logpb.ProduceMessageResponse_Error{
			Error: status.AsLogError(err).AsPBError(),
		}
	} else {
		resp.Response = &logpb.ProduceMessageResponse_Result{
			Result: &logpb.ProduceMessageResponseResult{
				Id: message.NewPBMessageIDFromMessageID(id),
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
