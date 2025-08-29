package replicate

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
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
	replicateRespCh chan *milvuspb.ReplicateResponse // All processing messages result should sent from theses channel.
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
			p.handleReplicateMessage(req)
		default:
			log.Warn("unknown request type", zap.Any("request", req))
		}
	}
}

// handleReplicateMessage handles the replicate message request.
func (p *ReplicateStreamServer) handleReplicateMessage(req *milvuspb.ReplicateRequest_ReplicateMessage) {
	// TODO: sheep, update metrics.
	p.wg.Add(1)
	defer p.wg.Done()
	reqMsg := req.ReplicateMessage.GetMessage()
	log.Debug("recv replicate message from client", zap.String("messageID", reqMsg.GetId().GetId()))
	msg := message.NewReplicateMessage(p.clusterID, reqMsg)

	// Append message to wal.
	// Keep retrying until the message is appended successfully or the stream is closed.
	for {
		select {
		case <-p.streamServer.Context().Done():
			return
		default:
			// TODO: sheep, append async.
			appendResult, err := streaming.WAL().RawAppend(p.streamServer.Context(), msg)
			if err != nil {
				log.Warn("append replicate message to wal failed", zap.Error(err))
				continue
			}
			p.sendReplicateResult(appendResult)
			return
		}
	}
}

// sendReplicateResult sends the replicate result to client.
func (p *ReplicateStreamServer) sendReplicateResult(appendResult *wal.AppendResult) {
	resp := &milvuspb.ReplicateResponse{
		Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{
				ConfirmedTimeTick: appendResult.TimeTick,
			},
		},
	}
	// If server context is canceled, it means the stream has been closed.
	// all pending response message should be dropped, client side will handle it.
	select {
	case p.replicateRespCh <- resp:
		log.Debug("send replicate message response to client", zap.Uint64("confirmedTimeTick", appendResult.TimeTick))
	case <-p.streamServer.Context().Done():
		log.Warn("stream closed before replicate message response sent", zap.Uint64("confirmedTimeTick", appendResult.TimeTick))
		return
	}
}
