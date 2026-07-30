package discover

import (
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

var errClosedByUser = errors.New("closed by user")

const (
	assignmentErrorBatchInterval = 10 * time.Millisecond
	assignmentErrorQueueCapacity = 128
)

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
		logger: resource.Resource().Logger().With(mlog.FieldComponent("assignment-discover-server")),
	}
}

type AssignmentDiscoverServer struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	balancer     balancer.Balancer
	streamServer discoverGrpcServerHelper
	logger       *mlog.Logger
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
	unavailableCh := make(chan types.PChannelInfo, assignmentErrorQueueCapacity)
	var unavailableWG sync.WaitGroup
	unavailableWG.Add(1)
	go func() {
		defer unavailableWG.Done()
		s.markUnavailableLoop(unavailableCh)
	}()

	defer func() {
		close(unavailableCh)
		unavailableWG.Wait()
		if err != nil {
			s.cancel(err)
			s.logger.Warn(s.ctx, "recv arm of stream closed by unexpected error", mlog.Err(err))
			return
		}
		s.cancel(errClosedByUser)
		s.logger.Info(s.ctx, "recv arm of stream closed")
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
			select {
			case unavailableCh <- channel:
			case <-s.ctx.Done():
				return context.Cause(s.ctx)
			}
		case *streamingpb.AssignmentDiscoverRequest_Close:
		default:
			s.logger.Warn(s.ctx, "unknown command type", mlog.Any("command", req))
		}
	}
}

func (s *AssignmentDiscoverServer) markUnavailableLoop(unavailableCh <-chan types.PChannelInfo) {
	for {
		first, ok := <-unavailableCh
		if !ok {
			return
		}

		batch := make(map[string]types.PChannelInfo)
		addUnavailableChannel(batch, first)
		batchTimer := time.NewTimer(assignmentErrorBatchInterval)

	collect:
		for {
			select {
			case channel, ok := <-unavailableCh:
				if !ok {
					if !batchTimer.Stop() {
						select {
						case <-batchTimer.C:
						default:
						}
					}
					s.markUnavailableBatch(batch)
					return
				}
				addUnavailableChannel(batch, channel)
			case <-batchTimer.C:
				break collect
			case <-s.ctx.Done():
				if !batchTimer.Stop() {
					select {
					case <-batchTimer.C:
					default:
					}
				}
				return
			}
		}

		s.markUnavailableBatch(batch)
	}
}

func addUnavailableChannel(batch map[string]types.PChannelInfo, channel types.PChannelInfo) {
	previous, ok := batch[channel.Name]
	if !ok || channel.Term > previous.Term {
		batch[channel.Name] = channel
	}
}

func (s *AssignmentDiscoverServer) markUnavailableBatch(batch map[string]types.PChannelInfo) {
	channels := make([]types.PChannelInfo, 0, len(batch))
	for _, channel := range batch {
		channels = append(channels, channel)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})

	if err := s.balancer.MarkAsUnavailable(s.ctx, channels); err != nil {
		s.logger.Warn(s.ctx, "failed to mark pchannels as unavailable",
			mlog.Int("pchannelCount", len(channels)),
			mlog.Err(err))
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
