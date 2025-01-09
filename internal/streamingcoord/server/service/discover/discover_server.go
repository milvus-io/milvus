package discover

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var errClosedByUser = errors.New("closed by user")

func NewAssignmentDiscoverServer(
	balancer balancer.Balancer,
	streamServer streamingpb.StreamingCoordAssignmentService_AssignmentDiscoverServer,
) *AssignmentDiscoverServer {
	ctx, cancel := context.WithCancelCause(streamServer.Context())
	return &AssignmentDiscoverServer{
		ctx:      ctx,
		cancel:   cancel,
		balancer: balancer,
		streamServer: discoverGrpcServerHelper{
			streamServer,
		},
		logger: resource.Resource().Logger().With(log.FieldComponent("assignment-discover-server")),
	}
}

type AssignmentDiscoverServer struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	balancer     balancer.Balancer
	streamServer discoverGrpcServerHelper
	logger       *log.MLogger
}

func (s *AssignmentDiscoverServer) Execute() error {
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
func (s *AssignmentDiscoverServer) recvLoop() (err error) {
	defer func() {
		if err != nil {
			s.cancel(err)
			s.logger.Warn("recv arm of stream closed by unexpected error", zap.Error(err))
			return
		}
		s.cancel(errClosedByUser)
		s.logger.Info("recv arm of stream closed")
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
		case *streamingpb.AssignmentDiscoverRequest_ReportError:
			channel := types.NewPChannelInfoFromProto(req.ReportError.GetPchannel())
			// mark the channel as unavailable and trigger a recover right away.
			s.balancer.MarkAsUnavailable(s.ctx, []types.PChannelInfo{channel})
		case *streamingpb.AssignmentDiscoverRequest_Close:
		default:
			s.logger.Warn("unknown command type", zap.Any("command", req))
		}
	}
}

// sendLoop sends the message to client.
func (s *AssignmentDiscoverServer) sendLoop() error {
	err := s.balancer.WatchChannelAssignments(s.ctx, s.streamServer.SendFullAssignment)
	if errors.Is(err, errClosedByUser) {
		return s.streamServer.SendCloseResponse()
	}
	return err
}
