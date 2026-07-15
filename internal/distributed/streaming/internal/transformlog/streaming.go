package transformlog

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type StreamFactory = func(ctx context.Context, pchannel string) (wal.TransformLogStream, error)

func NewResumableStream(_ context.Context, pchannel string, factory StreamFactory) wal.TransformLogStream {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &resumableStream{
		ctx:           ctx,
		cancel:        cancel,
		pchannel:      pchannel,
		factory:       factory,
		done:          make(chan struct{}),
		wake:          make(chan struct{}, 1),
		subscriptions: make(map[int64]*resumableSubscription),
	}
	go stream.resumeLoop()
	return stream
}

type resumableStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	pchannel string
	factory  StreamFactory

	mu            sync.Mutex
	nextID        int64
	closing       bool
	err           error
	underlying    wal.TransformLogStream
	subscriptions map[int64]*resumableSubscription

	done       chan struct{}
	wake       chan struct{}
	closeOnce  sync.Once
	finishOnce sync.Once
}

func (s *resumableStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	sub := s.newSubscription(opt)
	if sub == nil {
		if err := s.Error(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	s.wakeResume()
	select {
	case <-sub.ready:
		if err := sub.Error(); err != nil {
			return nil, err
		}
		return sub, nil
	case <-sub.done:
		return nil, sub.Error()
	case <-ctx.Done():
		s.removeSubscription(sub, ctx.Err())
		return nil, ctx.Err()
	}
}

func (s *resumableStream) Done() <-chan struct{} {
	return s.done
}

func (s *resumableStream) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *resumableStream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		underlying := s.underlying
		s.mu.Unlock()
		s.cancel()
		if underlying != nil {
			_ = underlying.Close()
		}
	})
	<-s.done
	return s.Error()
}

func (s *resumableStream) newSubscription(opt wal.TransformLogSubscriptionOption) *resumableSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	default:
	}
	if s.closing || opt.Handler == nil {
		return nil
	}
	s.nextID++
	sub := newResumableSubscription(s, s.nextID, opt)
	s.subscriptions[sub.id] = sub
	return sub
}

func (s *resumableStream) resumeLoop() {
	var finalErr error
	defer func() {
		if s.isClosing() && errors.Is(finalErr, context.Canceled) {
			finalErr = nil
		}
		s.finish(finalErr)
	}()

	retryBackoff := backoff.NewExponentialBackOff()
	retryBackoff.InitialInterval = 100 * time.Millisecond
	retryBackoff.MaxInterval = 10 * time.Second
	retryBackoff.MaxElapsedTime = 0
	retryBackoff.Reset()

	for {
		if err := s.ctx.Err(); err != nil {
			finalErr = err
			return
		}
		underlying, err := s.factory(s.ctx, s.pchannel)
		if err != nil {
			mlog.Debug(s.ctx, "resumable transform log stream create failed, retrying",
				mlog.FieldPChannel(s.pchannel),
				mlog.Err(err),
			)
			if waitErr := s.waitNextRetry(retryBackoff.NextBackOff()); waitErr != nil {
				finalErr = waitErr
				return
			}
			continue
		}
		mlog.Debug(s.ctx, "resumable transform log stream acquired underlying stream",
			mlog.FieldPChannel(s.pchannel),
		)
		s.setUnderlying(underlying)
		err = s.subscribePending(underlying)
		if err == nil {
			err = s.waitUntilUnavailable(underlying)
		}
		_ = underlying.Close()
		s.clearUnderlying(underlying)
		mlog.Debug(s.ctx, "resumable transform log stream underlying stream unavailable, retrying",
			mlog.FieldPChannel(s.pchannel),
			mlog.Err(err),
		)
		if waitErr := s.waitNextRetry(retryBackoff.NextBackOff()); waitErr != nil {
			finalErr = waitErr
			return
		}
	}
}

func (s *resumableStream) setUnderlying(underlying wal.TransformLogStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.underlying = underlying
}

func (s *resumableStream) clearUnderlying(underlying wal.TransformLogStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.underlying == underlying {
		s.underlying = nil
	}
	for _, sub := range s.subscriptions {
		sub.remote = nil
	}
}

func (s *resumableStream) subscribePending(underlying wal.TransformLogStream) error {
	for _, sub := range s.subscriptionSnapshot() {
		if sub.hasRemote() {
			continue
		}
		if err := s.subscribeRemote(underlying, sub); err != nil {
			if underlying.Error() != nil {
				return err
			}
			_ = sub.handle(wal.TransformLogStreamEvent{
				SubscriptionID: sub.ID(),
				VChannel:       sub.VChannel(),
				Err:            err,
			})
			sub.markReady(nil)
		}
	}
	return nil
}

func (s *resumableStream) subscriptionSnapshot() []*resumableSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := make([]*resumableSubscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subs = append(subs, sub)
	}
	return subs
}

func (s *resumableStream) subscribeRemote(underlying wal.TransformLogStream, sub *resumableSubscription) error {
	opt := sub.option()
	opt.Handler = nopCloseHandler{TransformLogEventHandler: sub.handler}
	mlog.Debug(s.ctx, "resumable transform log stream subscribing vchannel",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(opt.VChannel),
		mlog.Uint64("startAfterTimeTick", opt.StartAfterTimeTick),
		mlog.Uint64("endTimeTick", opt.EndTimeTick),
		mlog.Int64("subscriptionID", sub.ID()),
	)
	remote, err := underlying.Subscribe(s.ctx, opt)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.subscriptions[sub.id] != sub {
		s.mu.Unlock()
		return remote.Close()
	}
	sub.remote = remote
	s.mu.Unlock()
	sub.markReady(nil)
	mlog.Debug(s.ctx, "resumable transform log stream subscribed vchannel",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(opt.VChannel),
		mlog.Uint64("startAfterTimeTick", opt.StartAfterTimeTick),
		mlog.Int64("subscriptionID", sub.ID()),
		mlog.Int64("remoteSubscriptionID", remote.ID()),
	)
	return nil
}

func (s *resumableStream) waitUntilUnavailable(underlying wal.TransformLogStream) error {
	for {
		select {
		case <-underlying.Done():
			return underlying.Error()
		case <-s.wake:
			if err := s.subscribePending(underlying); err != nil {
				return err
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *resumableStream) removeSubscription(sub *resumableSubscription, err error) {
	var remote wal.TransformLogSubscription
	shouldClose := false
	s.mu.Lock()
	if s.subscriptions[sub.id] == sub {
		delete(s.subscriptions, sub.id)
		remote = sub.remote
		sub.remote = nil
		shouldClose = len(s.subscriptions) == 0 && !s.closing
	}
	s.mu.Unlock()
	if remote != nil {
		_ = remote.Close()
	}
	sub.finish(err)
	if shouldClose {
		go func() {
			_ = s.Close()
		}()
	}
}

func (s *resumableStream) wakeResume() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *resumableStream) waitNextRetry(duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-s.wake:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *resumableStream) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *resumableStream) finish(err error) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.closing = true
		subscriptions := s.subscriptions
		s.subscriptions = make(map[int64]*resumableSubscription)
		close(s.done)
		s.mu.Unlock()
		for _, sub := range subscriptions {
			if err != nil {
				_ = sub.handle(wal.TransformLogStreamEvent{
					SubscriptionID: sub.ID(),
					VChannel:       sub.VChannel(),
					Err:            err,
				})
			}
			sub.finish(err)
		}
	})
}
