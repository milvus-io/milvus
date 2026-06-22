package transformlog

import (
	"context"
	"io"
	"math"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type StreamKey struct {
	PChannel string
	Term     int64
	ServerID int64
}

func NewStreamKey(assignment *types.PChannelInfoAssigned) StreamKey {
	return StreamKey{
		PChannel: assignment.Channel.Name,
		Term:     assignment.Channel.Term,
		ServerID: assignment.Node.ServerID,
	}
}

type StreamOptions struct {
	Assignment *types.PChannelInfoAssigned
	OnClose    func(StreamKey, *Stream)
}

func CreateStream(
	ctx context.Context,
	opts *StreamOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (*Stream, error) {
	ctx = contextutil.WithCreateTransformStream(context.WithoutCancel(ctx), &streamingpb.CreateTransformStreamRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})
	streamClient, err := handlerClient.SubscribeTransform(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return nil, err
	}
	stream := &Stream{
		key:           NewStreamKey(opts.Assignment),
		stream:        streamClient,
		onClose:       opts.OnClose,
		subscriptions: make(map[int64]*remoteSubscription),
		done:          make(chan struct{}),
	}
	go stream.recvLoop()
	return stream, nil
}

type Stream struct {
	key    StreamKey
	stream streamingpb.StreamingNodeHandlerService_SubscribeTransformClient

	sendMu     sync.Mutex
	mu         sync.Mutex
	nextID     int64
	active     int
	closing    bool
	err        error
	done       chan struct{}
	closeOnce  sync.Once
	finishOnce sync.Once
	onClose    func(StreamKey, *Stream)

	subscriptions map[int64]*remoteSubscription
}

func (s *Stream) Key() StreamKey {
	return s.key
}

func (s *Stream) Done() <-chan struct{} {
	return s.done
}

func (s *Stream) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *Stream) IsClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *Stream) Subscribe(ctx context.Context, opt wal.TransformLogReadOption) (wal.TransformLogScanner, error) {
	sub := s.newSubscription(opt)
	if sub == nil {
		if err := s.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	if err := s.send(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_Create{
			Create: &streamingpb.CreateTransformSubscriptionRequest{
				SubscriptionId:     sub.subscriptionID,
				Vchannel:           opt.VChannel,
				StartAfterTimeTick: opt.StartAfterTimeTick,
				EndTimeTick:        opt.EndTimeTick,
			},
		},
	}); err != nil {
		s.removeSubscription(sub.subscriptionID)
		sub.finish(err)
		return nil, err
	}
	select {
	case <-sub.created:
		if err := sub.Error(); err != nil {
			return nil, err
		}
		return sub, nil
	case <-sub.Done():
		return nil, sub.Error()
	case <-ctx.Done():
		_ = s.sendCloseSubscription(sub.subscriptionID)
		s.removeSubscription(sub.subscriptionID)
		sub.finish(ctx.Err())
		return nil, ctx.Err()
	}
}

func (s *Stream) Close() error {
	s.markClosing()
	s.closeOnce.Do(func() {
		_ = s.send(&streamingpb.TransformRequest{
			Request: &streamingpb.TransformRequest_CloseStream{
				CloseStream: &streamingpb.CloseTransformStreamRequest{},
			},
		})
		_ = s.stream.CloseSend()
	})
	<-s.done
	return s.Error()
}

func (s *Stream) newSubscription(opt wal.TransformLogReadOption) *remoteSubscription {
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
	s.nextID++
	sub := newRemoteSubscription(s, s.nextID, opt)
	s.subscriptions[sub.subscriptionID] = sub
	s.active++
	return sub
}

func (s *Stream) markClosing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
}

func (s *Stream) getSubscription(subscriptionID int64) *remoteSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptions[subscriptionID]
}

func (s *Stream) removeSubscription(subscriptionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subscriptionID)
}

func (s *Stream) onSubscriptionFinished(subscriptionID int64) {
	shouldClose := false
	s.mu.Lock()
	delete(s.subscriptions, subscriptionID)
	if s.active > 0 {
		s.active--
	}
	if s.active == 0 {
		select {
		case <-s.done:
		default:
			s.closing = true
			shouldClose = true
		}
	}
	s.mu.Unlock()
	if shouldClose {
		go func() {
			_ = s.Close()
		}()
	}
}

func (s *Stream) sendCloseSubscription(subscriptionID int64) error {
	return s.send(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: subscriptionID},
		},
	})
}

func (s *Stream) send(req *streamingpb.TransformRequest) error {
	select {
	case <-s.done:
		if err := s.Error(); err != nil {
			return err
		}
		return io.EOF
	default:
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.stream.Send(req)
}

func (s *Stream) recvLoop() {
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
		case *streamingpb.TransformResponse_Create:
			s.handleCreate(resp.Create)
		case *streamingpb.TransformResponse_MessageBatch:
			s.handleMessageBatch(resp.MessageBatch)
		case *streamingpb.TransformResponse_CaughtUp:
			s.handleCaughtUp(resp.CaughtUp)
		case *streamingpb.TransformResponse_SubscriptionError:
			s.handleSubscriptionError(resp.SubscriptionError)
		case *streamingpb.TransformResponse_CloseSubscription:
			s.handleCloseSubscription(resp.CloseSubscription)
		case *streamingpb.TransformResponse_CloseStream:
			err = nil
			return
		}
	}
}

func (s *Stream) handleCreate(resp *streamingpb.CreateTransformSubscriptionResponse) {
	if resp == nil {
		return
	}
	if sub := s.getSubscription(resp.GetSubscriptionId()); sub != nil {
		sub.markCreated(nil)
	}
}

func (s *Stream) handleMessageBatch(resp *streamingpb.TransformMessageBatch) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	for _, entry := range resp.GetEntries() {
		sub.sendEvent(wal.TransformLogEvent{Entry: entry})
	}
}

func (s *Stream) handleCaughtUp(resp *streamingpb.TransformSubscriptionCaughtUp) {
	if resp == nil {
		return
	}
	if sub := s.getSubscription(resp.GetSubscriptionId()); sub != nil {
		sub.sendEvent(wal.TransformLogEvent{
			CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: resp.GetStartAfterTimeTick()},
		})
	}
}

func (s *Stream) handleSubscriptionError(resp *streamingpb.TransformSubscriptionError) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	err := status.NewUnknownError("transform subscription error")
	if resp.GetError() != nil {
		err = status.AsStreamingError((*status.StreamingError)(resp.GetError()))
	}
	s.removeSubscription(resp.GetSubscriptionId())
	sub.finish(err)
}

func (s *Stream) handleCloseSubscription(resp *streamingpb.CloseTransformSubscriptionResponse) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	s.removeSubscription(resp.GetSubscriptionId())
	sub.markCloseAck(nil)
	sub.finish(nil)
}

func (s *Stream) finish(err error) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.closing = true
		s.active = 0
		subscriptions := s.subscriptions
		s.subscriptions = make(map[int64]*remoteSubscription)
		close(s.done)
		s.mu.Unlock()
		for _, sub := range subscriptions {
			sub.finish(err)
		}
		_ = s.stream.CloseSend()
		if s.onClose != nil {
			s.onClose(s.key, s)
		}
	})
}

type remoteSubscription struct {
	stream         *Stream
	name           string
	subscriptionID int64
	vchannel       string
	ch             chan wal.TransformLogEvent
	done           chan struct{}
	created        chan struct{}
	closeAck       chan struct{}

	errMu        sync.Mutex
	err          error
	createdOnce  sync.Once
	closeAckOnce sync.Once
	closeOnce    sync.Once
	finishOnce   sync.Once
}

func newRemoteSubscription(stream *Stream, subscriptionID int64, opt wal.TransformLogReadOption) *remoteSubscription {
	return &remoteSubscription{
		stream:         stream,
		name:           opt.Name,
		subscriptionID: subscriptionID,
		vchannel:       opt.VChannel,
		ch:             make(chan wal.TransformLogEvent, 16),
		done:           make(chan struct{}),
		created:        make(chan struct{}),
		closeAck:       make(chan struct{}),
	}
}

func (s *remoteSubscription) Name() string {
	return s.name
}

func (s *remoteSubscription) Chan() <-chan wal.TransformLogEvent {
	return s.ch
}

func (s *remoteSubscription) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *remoteSubscription) Done() <-chan struct{} {
	return s.done
}

func (s *remoteSubscription) Close() error {
	s.closeOnce.Do(func() {
		select {
		case <-s.done:
			return
		default:
		}
		if err := s.stream.sendCloseSubscription(s.subscriptionID); err != nil {
			s.stream.removeSubscription(s.subscriptionID)
			s.finish(err)
		}
	})
	<-s.done
	return s.Error()
}

func (s *remoteSubscription) sendEvent(event wal.TransformLogEvent) {
	select {
	case s.ch <- event:
	case <-s.done:
	}
}

func (s *remoteSubscription) markCreated(err error) {
	if err != nil {
		s.setError(err)
	}
	s.createdOnce.Do(func() {
		close(s.created)
	})
}

func (s *remoteSubscription) markCloseAck(err error) {
	if err != nil {
		s.setError(err)
	}
	s.closeAckOnce.Do(func() {
		close(s.closeAck)
	})
}

func (s *remoteSubscription) finish(err error) {
	s.finishOnce.Do(func() {
		if err != nil {
			s.setError(err)
		}
		s.markCreated(err)
		s.markCloseAck(err)
		close(s.done)
		close(s.ch)
		s.stream.onSubscriptionFinished(s.subscriptionID)
	})
}

func (s *remoteSubscription) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
