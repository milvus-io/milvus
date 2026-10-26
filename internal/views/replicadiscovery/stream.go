package replicadiscovery

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Stream is one physical attempt. Close unblocks concurrent Send/Recv.
// Exactly one sender and one receiver use it; callbacks belong to Manager.
type Stream interface {
	Send(*viewpb.WatchVChannelReplicasRequest) error
	Recv() (*viewpb.WatchVChannelReplicasResponse, error)
	Close()
}

// Provider is the optional WAL capability shared by local and RPC adapters.
type Provider interface{ WatchReplicas() (*Watch, error) }

type remoteStream struct {
	viewpb.ViewQueryService_WatchVChannelReplicasClient
	cancel context.CancelFunc
}

func NewRemoteStream(stream viewpb.ViewQueryService_WatchVChannelReplicasClient, cancel context.CancelFunc) Stream {
	return &remoteStream{ViewQueryService_WatchVChannelReplicasClient: stream, cancel: cancel}
}

func (s *remoteStream) Close() { s.cancel() }

type localStream struct {
	ctx              context.Context
	cancel           context.CancelFunc
	watch            *Watch
	unavailable      <-chan struct{}
	mu               sync.Mutex
	acknowledgements []string
	wake             chan struct{}
}

// NewLocalStream bridges the exact same WAL-owned directory without gRPC.
// The supplied watch is owned by the stream and released on Close.
func NewLocalStream(ctx context.Context, watch *Watch, unavailable <-chan struct{}) Stream {
	ctx, cancel := context.WithCancel(ctx)
	return &localStream{ctx: ctx, cancel: cancel, watch: watch, unavailable: unavailable, wake: make(chan struct{}, 1)}
}

func (s *localStream) available() error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-s.unavailable:
		return status.NewOnShutdownError("local replica WAL unavailable")
	case <-s.watch.Done():
		return status.NewOnShutdownError("local replica directory closed")
	default:
		return nil
	}
}

func (s *localStream) Send(req *viewpb.WatchVChannelReplicasRequest) error {
	if err := s.available(); err != nil {
		return err
	}
	if req.GetUnsubscribe() {
		s.watch.Unsubscribe(req.GetVchannel())
		s.mu.Lock()
		s.acknowledgements = append(s.acknowledgements, req.GetVchannel())
		s.mu.Unlock()
		select {
		case s.wake <- struct{}{}:
		default:
		}
	} else {
		s.watch.Subscribe(req.GetVchannel())
	}
	return nil
}

func (s *localStream) Recv() (*viewpb.WatchVChannelReplicasResponse, error) {
	for {
		if err := s.available(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		var vc string
		if len(s.acknowledgements) != 0 {
			vc = s.acknowledgements[0]
			s.acknowledgements[0] = ""
			s.acknowledgements = s.acknowledgements[1:]
		}
		s.mu.Unlock()
		if vc != "" {
			return &viewpb.WatchVChannelReplicasResponse{Response: &viewpb.WatchVChannelReplicasResponse_UnsubscribedVchannel{UnsubscribedVchannel: vc}}, nil
		}
		if snapshot := s.watch.Pop(); snapshot != nil {
			return &viewpb.WatchVChannelReplicasResponse{Response: &viewpb.WatchVChannelReplicasResponse_Snapshot{Snapshot: snapshot}}, nil
		}
		select {
		case <-s.ctx.Done():
		case <-s.unavailable:
		case <-s.watch.Done():
		case <-s.wake:
		case <-s.watch.Changed():
		}
	}
}

func (s *localStream) Close() { s.cancel(); s.watch.Close() }
