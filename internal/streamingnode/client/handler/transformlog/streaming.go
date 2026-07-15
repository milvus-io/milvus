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
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type EventStreamOptions struct {
	Assignment *types.PChannelInfoAssigned
}

func CreateEventStream(
	ctx context.Context,
	opts *EventStreamOptions,
	handlerClient streamingpb.StreamingNodeHandlerServiceClient,
) (*EventStream, error) {
	pchannel := opts.Assignment.Channel.Name
	ctx = contextutil.WithCreateTransformStream(ctx, &streamingpb.CreateTransformStreamRequest{
		Pchannel: types.NewProtoFromPChannelInfo(opts.Assignment.Channel),
	})
	streamClient, err := handlerClient.SubscribeTransform(ctx, grpc.MaxCallRecvMsgSize(math.MaxInt32))
	if err != nil {
		return nil, err
	}
	mlog.Debug(ctx, "handler transform log event stream created",
		mlog.FieldPChannel(pchannel),
		mlog.Int64("serverID", opts.Assignment.Node.ServerID),
		mlog.Int64("term", opts.Assignment.Channel.Term),
	)
	stream := &EventStream{
		ctx:           ctx,
		pchannel:      pchannel,
		stream:        streamClient,
		subscriptions: make(map[int64]*eventSubscription),
		done:          make(chan struct{}),
	}
	go stream.recvLoop()
	return stream, nil
}

type EventStream struct {
	ctx      context.Context
	pchannel string
	stream   streamingpb.StreamingNodeHandlerService_SubscribeTransformClient

	sendMu     sync.Mutex
	mu         sync.Mutex
	nextID     int64
	closing    bool
	err        error
	done       chan struct{}
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

func (s *EventStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
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
		s.finish(err)
		return nil, err
	}
	mlog.Debug(ctx, "handler transform log event stream sent subscription request",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(opt.VChannel),
		mlog.Uint64("startAfterTimeTick", opt.StartAfterTimeTick),
		mlog.Uint64("endTimeTick", opt.EndTimeTick),
		mlog.Int64("subscriptionID", sub.subscriptionID),
	)
	select {
	case <-sub.ready:
		if err := sub.Error(); err != nil {
			return nil, err
		}
		return sub, nil
	case <-sub.done:
		return nil, sub.Error()
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

func (s *EventStream) newSubscription(opt wal.TransformLogSubscriptionOption) *eventSubscription {
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
	if opt.Handler == nil {
		return nil
	}
	s.nextID++
	sub := newEventSubscription(s, s.nextID, opt)
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

func (s *EventStream) onSubscriptionFinished(subscriptionID int64) {
	shouldClose := false
	s.mu.Lock()
	delete(s.subscriptions, subscriptionID)
	if len(s.subscriptions) == 0 {
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

func (s *EventStream) sendCloseSubscription(subscriptionID int64) error {
	return s.send(&streamingpb.TransformRequest{
		Request: &streamingpb.TransformRequest_CloseSubscription{
			CloseSubscription: &streamingpb.CloseTransformSubscriptionRequest{SubscriptionId: subscriptionID},
		},
	})
}

func (s *EventStream) send(req *streamingpb.TransformRequest) error {
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

func (s *EventStream) recvLoop() {
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

func (s *EventStream) handleCreate(resp *streamingpb.CreateTransformSubscriptionResponse) {
	if resp == nil {
		return
	}
	if sub := s.getSubscription(resp.GetSubscriptionId()); sub != nil {
		sub.markReady(nil)
		mlog.Debug(s.ctx, "handler transform log event stream subscription created",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(resp.GetVchannel()),
			mlog.Uint64("startAfterTimeTick", resp.GetStartAfterTimeTick()),
			mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
		)
	}
}

func (s *EventStream) handleMessageBatch(resp *streamingpb.TransformMessageBatch) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	for _, entry := range resp.GetEntries() {
		mlog.Debug(s.ctx, "handler transform log event stream received entry",
			mlog.FieldPChannel(s.pchannel),
			mlog.FieldVChannel(resp.GetVchannel()),
			mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
			mlog.Uint64("timeTick", entry.GetTimeTick()),
		)
		if err := sub.handle(wal.TransformLogStreamEvent{
			SubscriptionID: resp.GetSubscriptionId(),
			VChannel:       resp.GetVchannel(),
			Entry:          entry,
		}); err != nil {
			s.removeSubscription(resp.GetSubscriptionId())
			sub.finish(err)
			return
		}
	}
}

func (s *EventStream) handleCaughtUp(resp *streamingpb.TransformSubscriptionCaughtUp) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	if err := sub.handle(wal.TransformLogStreamEvent{
		SubscriptionID: resp.GetSubscriptionId(),
		VChannel:       resp.GetVchannel(),
		CaughtUp:       &wal.TransformLogCaughtUp{StartAfterTimeTick: resp.GetStartAfterTimeTick()},
	}); err != nil {
		s.removeSubscription(resp.GetSubscriptionId())
		sub.finish(err)
	}
	mlog.Debug(s.ctx, "handler transform log event stream received caught-up",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(resp.GetVchannel()),
		mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
		mlog.Uint64("startAfterTimeTick", resp.GetStartAfterTimeTick()),
	)
}

func (s *EventStream) handleSubscriptionError(resp *streamingpb.TransformSubscriptionError) {
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
	_ = sub.handle(wal.TransformLogStreamEvent{
		SubscriptionID: resp.GetSubscriptionId(),
		VChannel:       resp.GetVchannel(),
		Err:            err,
	})
	sub.finish(err)
	mlog.Debug(s.ctx, "handler transform log event stream received subscription error",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(resp.GetVchannel()),
		mlog.Int64("subscriptionID", resp.GetSubscriptionId()),
		mlog.Err(err),
	)
}

func (s *EventStream) handleCloseSubscription(resp *streamingpb.CloseTransformSubscriptionResponse) {
	if resp == nil {
		return
	}
	sub := s.getSubscription(resp.GetSubscriptionId())
	if sub == nil {
		return
	}
	s.removeSubscription(resp.GetSubscriptionId())
	sub.finish(nil)
	mlog.Debug(s.ctx, "handler transform log event stream subscription closed",
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
		mlog.Debug(s.ctx, "handler transform log event stream finished",
			mlog.FieldPChannel(s.pchannel),
			mlog.Err(err),
		)
		for _, sub := range subscriptions {
			sub.finish(err)
		}
		_ = s.stream.CloseSend()
	})
}
