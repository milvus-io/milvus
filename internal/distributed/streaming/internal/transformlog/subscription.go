package transformlog

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type resumableSubscription struct {
	stream         *resumableStream
	id             int64
	vchannel       string
	endTimeTick    uint64
	nextStartAfter uint64
	remote         wal.TransformLogSubscription
	handler        *checkpointEventHandler

	done  chan struct{}
	ready chan struct{}
	// readyErr is the immutable creation result, published by closing ready.
	// It is separate from err: a normal Close can abort pending creation.
	readyErr   error
	errMu      sync.Mutex
	err        error
	closeOnce  sync.Once
	readyOnce  sync.Once
	finishOnce sync.Once
}

func newResumableSubscription(stream *resumableStream, id int64, opt wal.TransformLogSubscriptionOption) *resumableSubscription {
	sub := &resumableSubscription{
		stream:         stream,
		id:             id,
		vchannel:       opt.VChannel,
		endTimeTick:    opt.EndTimeTick,
		nextStartAfter: opt.StartAfterTimeTick,
		done:           make(chan struct{}),
		ready:          make(chan struct{}),
	}
	sub.handler = &checkpointEventHandler{sub: sub, inner: opt.Handler}
	return sub
}

func (s *resumableSubscription) ID() int64 {
	return s.id
}

func (s *resumableSubscription) VChannel() string {
	return s.vchannel
}

func (s *resumableSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.stream.removeSubscription(s, nil)
	})
	<-s.done
	return s.Error()
}

func (s *resumableSubscription) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *resumableSubscription) option() wal.TransformLogSubscriptionOption {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	return wal.TransformLogSubscriptionOption{
		VChannel:           s.vchannel,
		StartAfterTimeTick: s.nextStartAfter,
		EndTimeTick:        s.endTimeTick,
	}
}

func (s *resumableSubscription) hasRemote() bool {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	return s.remote != nil
}

func (s *resumableSubscription) handle(event wal.TransformLogStreamEvent) error {
	return s.handler.Handle(event)
}

func (s *resumableSubscription) advance(timetick uint64) {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	if timetick > s.nextStartAfter {
		s.nextStartAfter = timetick
	}
}

func (s *resumableSubscription) markReady(err error) {
	s.readyOnce.Do(func() {
		s.readyErr = err
		close(s.ready)
	})
}

func (s *resumableSubscription) finish(err error) {
	s.finishOnce.Do(func() {
		if err != nil {
			s.setError(err)
		}
		readyErr := err
		if readyErr == nil {
			readyErr = status.NewOnShutdownError("transform stream closed before subscription was ready")
		}
		s.markReady(readyErr)
		s.handler.Close()
		close(s.done)
	})
}

func (s *resumableSubscription) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

type checkpointEventHandler struct {
	mu     sync.Mutex
	closed bool
	sub    *resumableSubscription
	inner  wal.TransformLogEventHandler
}

func (h *checkpointEventHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.sub.Error(); err != nil {
		return err
	}
	if h.closed {
		return status.NewOnShutdownError("transform subscription is closed")
	}
	event.SubscriptionID = h.sub.ID()
	event.VChannel = h.sub.VChannel()
	err := h.inner.Handle(event)
	if err != nil {
		// A rejected delivery must remain replayable after owner recovery.
		return err
	}
	if event.Entry != nil {
		h.sub.advance(event.Entry.GetTimeTick())
	}
	if event.SyncUp != nil {
		h.sub.advance(event.SyncUp.TimeTick)
	}
	if event.Entry != nil || event.SyncUp != nil {
		h.sub.stream.accepted.Store(true)
	}
	return nil
}

func (h *checkpointEventHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	h.inner.Close()
}

type resumableEventHandler struct {
	wal.TransformLogEventHandler
	sub    *resumableSubscription
	cancel context.CancelFunc
}

func (h resumableEventHandler) Handle(event wal.TransformLogStreamEvent) error {
	h.sub.stream.mu.Lock()
	active := h.sub.stream.subscriptions[h.sub.id] == h.sub
	h.sub.stream.mu.Unlock()
	if !active {
		return nil
	}
	if err := h.sub.Error(); err != nil {
		return err
	}
	if isRetryableSubscriptionError(event.Err) {
		// Do not close the underlying stream inside its delivery callback:
		// Close may wait for that callback. The resume loop owns cleanup.
		h.cancel()
		return nil
	}
	return h.TransformLogEventHandler.Handle(event)
}

func (h resumableEventHandler) Close() {}

func isRetryableSubscriptionError(err error) bool {
	if status.IsCanceled(err) {
		return true
	}
	var se *status.StreamingError
	if !errors.As(err, &se) {
		return false
	}
	if se.IsOnShutdown() || se.IsWrongStreamingNode() || se.IsFenced() {
		return true
	}
	// Existing SNs encode context cancellation as UNKNOWN, losing its type.
	// Match only these exact legacy causes; other UNKNOWN errors remain fatal.
	return se.Code == streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN &&
		(se.Cause == context.Canceled.Error() || se.Cause == context.DeadlineExceeded.Error())
}
