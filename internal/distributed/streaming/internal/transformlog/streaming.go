package transformlog

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

type StreamFactory = func(ctx context.Context, pchannel string) (wal.TransformLogStream, error)

func NewResumableStream(parent context.Context, pchannel string, factory StreamFactory) wal.TransformLogStream {
	ctx, cancel := context.WithCancel(parent) //nolint:gosec // stored in stream; canceled by finish and Close
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
	attemptCancel context.CancelFunc
	subscriptions map[int64]*resumableSubscription

	done       chan struct{}
	wake       chan struct{}
	closeOnce  sync.Once
	finishOnce sync.Once
}

func (s *resumableStream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if opt.Handler == nil || opt.VChannel == "" || (opt.EndTimeTick != 0 && opt.EndTimeTick < opt.StartAfterTimeTick) {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
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
		if err := sub.Error(); err != nil {
			return nil, err
		}
		return sub, nil
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
		ctx, cancel, err := s.waitForDemand()
		if err != nil {
			finalErr = err
			return
		}
		underlying, err := s.factory(ctx, s.pchannel)
		if err != nil {
			underlying = nil
		} else {
			mlog.Debug(s.ctx, "resumable transform log stream acquired underlying stream",
				mlog.FieldPChannel(s.pchannel),
			)
			s.setUnderlying(underlying)
			err = s.subscribePending(ctx, underlying)
			if err == nil {
				// Successful restoration ends this run of consecutive failures.
				retryBackoff.Reset()
				err = s.waitUntilUnavailable(ctx, underlying)
			}
		}
		// Removing the last subscription cancels this attempt, not the logical
		// stream. A new subscription may already be waiting for the next one.
		interrupted := ctx.Err() != nil
		cancel()
		if underlying != nil {
			_ = underlying.Close()
			s.clearUnderlying(underlying)
		}
		if interrupted {
			retryBackoff.Reset()
			continue
		}
		if err != nil && terminalSubscriptionError(err) {
			finalErr = err
			return
		}
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

// waitForDemand leaves an owner-held logical stream idle without opening an
// RPC. The demand check and attempt cancellation share the subscription lock,
// so an unsubscribe can also interrupt an in-flight connection or Subscribe.
func (s *resumableStream) waitForDemand() (context.Context, context.CancelFunc, error) {
	for {
		if err := s.ctx.Err(); err != nil {
			return nil, nil, err
		}
		s.mu.Lock()
		if len(s.subscriptions) > 0 {
			ctx, cancel := context.WithCancel(s.ctx) //nolint:gosec // resumeLoop closes every attempt; last unsubscribe can cancel it early
			s.attemptCancel = cancel
			s.mu.Unlock()
			return ctx, cancel, nil
		}
		s.mu.Unlock()
		select {
		case <-s.wake:
		case <-s.ctx.Done():
			return nil, nil, s.ctx.Err()
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

func (s *resumableStream) subscribePending(ctx context.Context, underlying wal.TransformLogStream) error {
	for _, sub := range s.subscriptionSnapshot() {
		if sub.hasRemote() {
			continue
		}
		if err := s.subscribeRemote(ctx, underlying, sub); err != nil {
			// Delivery can reject the logical subscription before Subscribe
			// returns. Its recorded failure must not reconnect other consumers.
			if subErr := sub.Error(); subErr != nil {
				s.removeSubscription(sub, subErr)
				continue
			}
			if ctx.Err() != nil || !terminalSubscriptionError(err) {
				return err
			}
			_ = sub.handle(wal.TransformLogStreamEvent{
				SubscriptionID: sub.ID(),
				VChannel:       sub.VChannel(),
				Err:            err,
			})
			s.removeSubscription(sub, err)
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

func (s *resumableStream) subscribeRemote(ctx context.Context, underlying wal.TransformLogStream, sub *resumableSubscription) error {
	opt := sub.option()
	if sub.isComplete() {
		s.removeSubscription(sub, nil)
		return nil
	}
	opt.Handler = resumeHandler{sub: sub}
	mlog.Debug(s.ctx, "resumable transform log stream subscribing vchannel",
		mlog.FieldPChannel(s.pchannel),
		mlog.FieldVChannel(opt.VChannel),
		mlog.Uint64("startAfterTimeTick", opt.StartAfterTimeTick),
		mlog.Uint64("endTimeTick", opt.EndTimeTick),
		mlog.Int64("subscriptionID", sub.ID()),
	)
	remote, err := underlying.Subscribe(ctx, opt)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.subscriptions[sub.id] != sub {
		s.mu.Unlock()
		_ = remote.Close()
		return nil
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

func (s *resumableStream) waitUntilUnavailable(ctx context.Context, underlying wal.TransformLogStream) error {
	for {
		select {
		case <-underlying.Done():
			return underlying.Error()
		case <-s.wake:
			if err := s.subscribePending(ctx, underlying); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *resumableStream) removeSubscription(sub *resumableSubscription, err error) {
	var remote wal.TransformLogSubscription
	s.mu.Lock()
	if s.subscriptions[sub.id] == sub {
		delete(s.subscriptions, sub.id)
		remote = sub.remote
		sub.remote = nil
		if len(s.subscriptions) == 0 && s.attemptCancel != nil {
			s.attemptCancel()
		}
	}
	s.mu.Unlock()
	s.wakeResume()
	if remote != nil {
		_ = remote.Close()
	}
	sub.finish(err)
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
		s.cancel()
		s.mu.Lock()
		s.err = err
		s.closing = true
		subscriptions := s.subscriptions
		s.subscriptions = make(map[int64]*resumableSubscription)
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
		close(s.done)
	})
}

func terminalSubscriptionError(err error) bool {
	return errors.Is(err, wal.ErrTransformLogInvalidReadOption) || errors.Is(err, wal.ErrTransformLogStartPointTruncated) || errors.Is(err, wal.ErrTransformLogVChannelUnavailable) || status.AsStreamingError(err).IsUnrecoverable()
}
