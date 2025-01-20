package broadcast

import (
	"context"
	"errors"
	"io"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

var errClosedByUser = errors.New("closed by user")

func NewBroadcastWatchServer(
	w broadcaster.Watcher,
	streamServer streamingpb.StreamingCoordBroadcastService_WatchServer,
) *BroadcastWatchServer {
	ctx, cancel := context.WithCancelCause(streamServer.Context())
	s := &BroadcastWatchServer{
		ctx:    ctx,
		cancel: cancel,
		w:      w,
		streamServer: broadcastWatchGrpcServerHelper{
			streamServer,
		},
	}
	s.SetLogger(resource.Resource().Logger().With(log.FieldComponent("broadcast-watch-server")))
	return s
}

type BroadcastWatchServer struct {
	log.Binder
	ctx          context.Context
	cancel       context.CancelCauseFunc
	w            broadcaster.Watcher
	streamServer broadcastWatchGrpcServerHelper
}

func (s *BroadcastWatchServer) Execute() error {
	// Start a recv arm to handle the control message from client.
	go func() {
		// recv loop will be blocked until the stream is closed.
		// 1. close by client.
		// 2. close by server context cancel by return of outside Execute.
		_ = s.recvLoop()
	}()

	// Start a send loop on current main goroutine.
	// the loop will be blocked until:
	// 1. the stream is broken.
	// 2. recv arm recv closed and all response is sent.
	return s.sendLoop()
}

// recvLoop receives the message from client.
func (s *BroadcastWatchServer) recvLoop() (err error) {
	defer func() {
		if err != nil {
			s.cancel(err)
			s.Logger().Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.cancel(errClosedByUser)
		s.Logger().Info("recv arm of stream closed")
	}()

	for {
		req, err := s.streamServer.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req := req.Command.(type) {
		case *streamingpb.BroadcastWatchRequest_CreateEventWatch:
			// Add new incoming resource key int watcher.
			s.w.ObserveResourceKeyEvent(s.streamServer.Context(), req.CreateEventWatch.Event)
		case *streamingpb.BroadcastWatchRequest_Close:
			// Ignore the command, the stream will be closed by client with io.EOF
		default:
			s.Logger().Warn("unknown command type ignored", zap.Any("command", req))
		}
	}
}

// sendLoop sends the message to client.
func (s *BroadcastWatchServer) sendLoop() (err error) {
	defer func() {
		if err != nil {
			s.Logger().Warn("send arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.Logger().Info("send arm of stream closed")
	}()
	for {
		select {
		case ev, ok := <-s.w.EventChan():
			if !ok {
				return errors.New("watcher is closed")
			}
			if err := s.streamServer.SendResourceKeyEvent(ev); err != nil {
				return err
			}
		case <-s.ctx.Done():
			err := context.Cause(s.ctx)
			if errors.Is(err, errClosedByUser) {
				return s.streamServer.SendCloseResponse()
			}
			return err
		}
	}
}
