package replicadiscovery

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/replicadiscovery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type EventStreamOptions struct {
	Assignment *types.PChannelInfoAssigned
}

// Bound both a flow-controlled closing send and the server's close response.
const eventStreamCloseTimeout = 5 * time.Second

func CreateEventStream(
	ctx context.Context,
	opts *EventStreamOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (*EventStream, error) {
	pchannel := opts.Assignment.Channel.Name
	ctx = contextutil.WithCreateReplicaWatchStream(ctx, &streamingpb.CreateReplicaWatchStreamRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})
	ctx, cancel := context.WithCancel(ctx)
	streamClient, err := handlerClient.SubscribeReplicas(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		cancel()
		return nil, err
	}
	// Wait for server-side owner validation and stream acquisition before returning.
	resp, err := streamClient.Recv()
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.GetCreateStream() == nil {
		cancel()
		return nil, status.NewInvalidRequestSeq("first replica watch response must acknowledge stream creation")
	}
	mlog.Debug(ctx, "handler replica watch stream created",
		mlog.FieldPChannel(pchannel),
		mlog.Int64("serverID", opts.Assignment.Node.ServerID),
		mlog.Int64("term", opts.Assignment.Channel.Term),
	)
	stream := &EventStream{
		ctx:           ctx,
		cancel:        cancel,
		pchannel:      pchannel,
		stream:        streamClient,
		subscriptions: make(map[int64]*eventSubscription),
		done:          make(chan struct{}),
		received:      make(chan struct{}),
	}
	go stream.recvLoop()
	return stream, nil
}

type EventStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	pchannel string
	stream   streamingpb.StreamingNodeHandlerService_SubscribeReplicasClient

	sendMu     sync.Mutex
	mu         sync.Mutex
	nextID     int64
	closing    bool
	err        error
	done       chan struct{}
	received   chan struct{}
	closeOnce  sync.Once
	finishOnce sync.Once

	subscriptions map[int64]*eventSubscription
}

func (s *EventStream) Done() <-chan struct{} {
	return s.done
}

func (s *EventStream) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *EventStream) Subscribe(ctx context.Context, vchannel string, handler replicadiscovery.Handler) (replicadiscovery.Subscription, error) {
	if handler == nil || vchannel == "" {
		return nil, status.NewInvalidArgument("replica subscription requires a VChannel and handler")
	}
	sub := s.newSubscription(vchannel, handler)
	if sub == nil {
		if err := s.Error(); err != nil {
			return nil, err
		}
		return nil, status.NewOnShutdownError("replica watch stream is closing")
	}
	if err := s.send(&streamingpb.ReplicaWatchRequest{
		Request: &streamingpb.ReplicaWatchRequest_Create{
			Create: &streamingpb.CreateReplicaWatchSubscriptionRequest{
				SubscriptionId: sub.subscriptionID,
				Vchannel:       vchannel,
			},
		},
	}); err != nil {
		// gRPC Send can report EOF before Recv obtains the peer's final status
		// so preserve the receive-side error for diagnostics.
		if errors.Is(err, io.EOF) {
			select {
			case <-s.done:
				if recvErr := s.Error(); recvErr != nil {
					return nil, recvErr
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			s.finish(err)
		}
		return nil, err
	}
	mlog.Debug(ctx, "handler replica watch stream sent subscription request",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(vchannel),
		mlog.Int64("subscriptionID", sub.subscriptionID),
	)
	select {
	case <-sub.ready:
		if sub.readyErr != nil {
			return nil, sub.readyErr
		}
		if err := sub.Error(); err != nil {
			return nil, err
		}
		return sub, nil
	case <-ctx.Done():
		_ = s.sendCloseSubscription(sub.subscriptionID)
		s.removeSubscription(sub.subscriptionID)
		sub.finish(ctx.Err())
		return nil, ctx.Err()
	}
}

func (s *EventStream) Close() error {
	s.markClosing()
	s.closeOnce.Do(func() {
		timer := time.AfterFunc(eventStreamCloseTimeout, func() {
			s.finish(context.DeadlineExceeded)
		})
		defer timer.Stop()
		s.closeSend()
		<-s.done
	})
	<-s.done
	<-s.received
	return s.Error()
}

// closeSend is called only by closeOnce. It shares sendMu with every Send;
// finish cancels the RPC instead of concurrently calling CloseSend.
func (s *EventStream) closeSend() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	select {
	case <-s.done:
		return
	default:
	}
	_ = s.stream.Send(&streamingpb.ReplicaWatchRequest{
		Request: &streamingpb.ReplicaWatchRequest_CloseStream{
			CloseStream: &streamingpb.CloseReplicaWatchStreamRequest{},
		},
	})
	_ = s.stream.CloseSend()
}

func (s *EventStream) newSubscription(vchannel string, handler replicadiscovery.Handler) *eventSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	default:
	}
	if s.closing {
		return nil
	}
	if handler == nil {
		return nil
	}
	s.nextID++
	sub := newEventSubscription(s, s.nextID, vchannel, handler)
	s.subscriptions[sub.subscriptionID] = sub
	return sub
}

func (s *EventStream) markClosing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
}

func (s *EventStream) getSubscription(subscriptionID int64) *eventSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptions[subscriptionID]
}

func (s *EventStream) removeSubscription(subscriptionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subscriptionID)
}

func (s *EventStream) sendCloseSubscription(subscriptionID int64) error {
	return s.send(&streamingpb.ReplicaWatchRequest{
		Request: &streamingpb.ReplicaWatchRequest_CloseSubscription{
			CloseSubscription: &streamingpb.CloseReplicaWatchSubscriptionRequest{SubscriptionId: subscriptionID},
		},
	})
}

func (s *EventStream) send(req *streamingpb.ReplicaWatchRequest) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	select {
	case <-s.done:
		if err := s.Error(); err != nil {
			return err
		}
		return status.NewOnShutdownError("replica watch stream is closing")
	default:
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return status.NewOnShutdownError("replica watch stream is closing")
	}
	return s.stream.Send(req)
}

func (s *EventStream) recvLoop() {
	defer close(s.received)
	var err error
	defer func() {
		if errors.Is(err, io.EOF) {
			err = nil
		}
		s.finish(err)
	}()
	for {
		resp, recvErr := s.stream.Recv()
		if recvErr != nil {
			err = recvErr
			return
		}
		switch resp := resp.GetResponse().(type) {
		case *streamingpb.ReplicaWatchResponse_Create:
			s.handleCreate(resp.Create)
		case *streamingpb.ReplicaWatchResponse_Snapshot:
			s.handleSnapshot(resp.Snapshot)
		case *streamingpb.ReplicaWatchResponse_SubscriptionError:
			if err = s.handleSubscriptionError(resp.SubscriptionError); err != nil {
				return
			}
		case *streamingpb.ReplicaWatchResponse_CloseSubscription:
			s.handleCloseSubscription(resp.CloseSubscription)
		case *streamingpb.ReplicaWatchResponse_CloseStream:
			err = nil
			return
		}
	}
}

func (s *EventStream) handleCreate(resp *streamingpb.CreateReplicaWatchSubscriptionResponse) {
	if resp == nil {
		return
	}
	if sub := s.getSubscription(resp.GetSubscriptionId()); sub != nil {
		sub.markReady(nil)
		mlog.Debug(s.ctx, "handler replica watch stream subscription created",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(resp.GetVchannel()),
			mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
		)
	}
}

func (s *EventStream) handleSnapshot(resp *streamingpb.ReplicaWatchSnapshot) {
	if sub := s.getSubscription(resp.GetSubscriptionId()); sub != nil {
		sub.handle(replicadiscovery.Snapshot{Valid: true, Replicas: replicadiscovery.ReplicasFromProto(resp.GetSnapshot().GetReplicas())})
	}
}

// A failed subscription ends this physical attempt. Resuming recreates all
// subscriptions with fresh full snapshots; handlers receive no errors.
func (s *EventStream) handleSubscriptionError(resp *streamingpb.ReplicaWatchSubscriptionError) error {
	if resp == nil || s.getSubscription(resp.GetSubscriptionId()) == nil {
		return nil
	}
	if resp.GetError() != nil {
		return status.AsStreamingError((*status.StreamingError)(resp.GetError()))
	}
	return status.NewUnknownError("replica watch subscription error")
}

func (s *EventStream) handleCloseSubscription(resp *streamingpb.CloseReplicaWatchSubscriptionResponse) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	s.removeSubscription(resp.GetSubscriptionId())
	sub.finish(nil)
	mlog.Debug(s.ctx, "handler replica watch stream subscription closed",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(resp.GetVchannel()),
		mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
	)
}

func (s *EventStream) finish(err error) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.closing = true
		subscriptions := s.subscriptions
		s.subscriptions = make(map[int64]*eventSubscription)
		close(s.done)
		s.mu.Unlock()
		// Do not wait for sendMu: Send may be blocked on gRPC flow control.
		// Cancel only this RPC, leaving the resumable stream's context intact.
		s.cancel()
		mlog.Debug(s.ctx, "handler replica watch stream finished",
			mlog.FieldPChannel(s.pchannel),
			mlog.Err(err),
		)
		for _, sub := range subscriptions {
			sub.finish(err)
		}
	})
}
