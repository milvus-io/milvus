package replicate

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const replicateRespChanLength = 128

func CreateReplicateServer(streamServer milvuspb.MilvusService_CreateReplicateStreamServer) (*ReplicateStreamServer, error) {
	clusterID, err := contextutil.GetClusterID(streamServer.Context())
	if err != nil {
		return nil, err
	}
	return &ReplicateStreamServer{
		clusterID:       clusterID,
		streamServer:    streamServer,
		replicateRespCh: make(chan *milvuspb.ReplicateResponse, replicateRespChanLength),
		wg:              sync.WaitGroup{},
	}, nil
}

// ReplicateStreamServer is a ReplicateStreamServer of replicate messages.
type ReplicateStreamServer struct {
	clusterID       string
	streamServer    milvuspb.MilvusService_CreateReplicateStreamServer
	replicateRespCh chan *milvuspb.ReplicateResponse
	wg              sync.WaitGroup
}

// Execute starts the replicate server.
func (p *ReplicateStreamServer) Execute() error {
	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		_ = p.recvLoop()
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until the stream is closed.
	err := p.sendLoop()
	return err
}

// sendLoop sends the message to client.
func (p *ReplicateStreamServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			log.Warn("send arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		log.Info("send arm of stream closed")
	}()

	for {
		select {
		case resp, ok := <-p.replicateRespCh:
			if !ok {
				return nil
			}
			if err := p.streamServer.Send(resp); err != nil {
				return err
			}
		case <-p.streamServer.Context().Done():
			return errors.Wrap(p.streamServer.Context().Err(), "cancel send loop by stream server")
		}
	}
}

// recvLoop receives the message from client.
func (p *ReplicateStreamServer) recvLoop() (err error) {
	defer func() {
		p.wg.Wait()
		close(p.replicateRespCh)
		if err != nil {
			log.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		log.Info("recv arm of stream closed")
	}()

	for {
		req, err := p.streamServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Request.(type) {
		case *milvuspb.ReplicateRequest_ReplicateMessage:
			err := p.handleReplicateMessage(req)
			if err != nil {
				return err
			}
		default:
			log.Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// handleReplicateMessage handles the replicate message request.
func (p *ReplicateStreamServer) handleReplicateMessage(req *milvuspb.ReplicateRequest_ReplicateMessage) error {
	p.wg.Add(1)
	defer p.wg.Done()
	reqMsg := req.ReplicateMessage.GetMessage()
	msg, err := message.NewReplicateMessage(req.ReplicateMessage.SourceClusterId, reqMsg)
	if err != nil {
		return err
	}
	sourceTs := msg.ReplicateHeader().TimeTick
	log.Debug("recv replicate message from client",
		zap.String("messageID", reqMsg.GetId().GetId()),
		zap.Uint64("sourceTimeTick", sourceTs),
		log.FieldMessage(msg),
	)

	// Append message to wal.
	_, err = streaming.WAL().Replicate().Append(p.streamServer.Context(), msg)
	if err == nil {
		p.sendReplicateResult(sourceTs, msg)
		return nil
	}
	if status.AsStreamingError(err).IsIgnoredOperation() {
		log.Info("append replicate message to wal ignored", log.FieldMessage(msg), zap.Error(err))
		p.sendReplicateResult(sourceTs, msg)
		return nil
	}
	// unexpected error, will close the stream and wait for client to reconnect.
	log.Warn("append replicate message to wal failed", log.FieldMessage(msg), zap.Error(err))
	return err
}

// sendReplicateResult sends the replicate result to client.
func (p *ReplicateStreamServer) sendReplicateResult(sourceTimeTick uint64, msg message.ReplicateMutableMessage) {
	if msg.TxnContext() != nil && msg.MessageType() != message.MessageTypeCommitTxn {
		// Only confirm the commit message of a transaction.
		return
	}
	resp := &milvuspb.ReplicateResponse{
		Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{
				ConfirmedTimeTick: sourceTimeTick,
			},
		},
	}
	// If server context is canceled, it means the stream has been closed.
	// all pending response message should be dropped, client side will handle it.
	select {
	case p.replicateRespCh <- resp:
		log.Debug("send replicate message response to client", zap.Uint64("confirmedTimeTick", sourceTimeTick))
	case <-p.streamServer.Context().Done():
		log.Warn("stream closed before replicate message response sent", zap.Uint64("confirmedTimeTick", sourceTimeTick))
		return
	}
}
