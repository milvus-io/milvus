package replicate

import (
	"io"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	ctx := p.streamServer.Context()
	defer func() {
		if err != nil {
			mlog.Warn(ctx, "send arm of stream closed by unexpected error", mlog.Err(err))
			return
		}
		mlog.Info(ctx, "send arm of stream closed")
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
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "cancel send loop by stream server")
		}
	}
}

// recvLoop receives the message from client.
func (p *ReplicateStreamServer) recvLoop() (err error) {
	ctx := p.streamServer.Context()
	defer func() {
		p.wg.Wait()
		close(p.replicateRespCh)
		if err != nil {
			mlog.Warn(ctx, "recv arm of stream closed by unexpected error", mlog.Err(err))
			return
		}
		mlog.Info(ctx, "recv arm of stream closed")
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
			mlog.Warn(ctx, "unknown request type", mlog.Any("request", req))
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
	ctx := p.streamServer.Context()
	mlog.Debug(ctx, "recv replicate message from client",
		mlog.String("messageID", reqMsg.GetId().GetId()),
		mlog.Uint64("sourceTimeTick", sourceTs),
		mlog.FieldMessage(msg),
	)

	// Append message to wal.
	_, err = streaming.WAL().Replicate().Append(ctx, msg)
	if err == nil {
		p.sendReplicateResult(sourceTs, msg)
		return nil
	}
	if status.AsStreamingError(err).IsIgnoredOperation() {
		mlog.Info(ctx, "append replicate message to wal ignored", mlog.FieldMessage(msg), mlog.Err(err))
		p.sendReplicateResult(sourceTs, msg)
		return nil
	}
	// unexpected error, will close the stream and wait for client to reconnect.
	mlog.Warn(ctx, "append replicate message to wal failed", mlog.FieldMessage(msg), mlog.Err(err))
	return err
}

// sendReplicateResult sends the replicate result to client.
func (p *ReplicateStreamServer) sendReplicateResult(sourceTimeTick uint64, msg message.ReplicateMutableMessage) {
	ctx := p.streamServer.Context()
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
		mlog.Debug(ctx, "send replicate message response to client", mlog.Uint64("confirmedTimeTick", sourceTimeTick))
	case <-ctx.Done():
		mlog.Warn(ctx, "stream closed before replicate message response sent", mlog.Uint64("confirmedTimeTick", sourceTimeTick))
		return
	}
}
