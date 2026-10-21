package transformlog

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
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

func (h *checkpointEventHandler) Handle(event wal.TransformLogStreamEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	event.SubscriptionID = h.sub.ID()
	event.VChannel = h.sub.VChannel()
	h.inner.Handle(event)
	if event.Entry != nil {
		h.sub.advance(event.Entry.GetTimeTick())
	}
	if event.SyncUp != nil {
		h.sub.advance(event.SyncUp.TimeTick)
	}
	if event.Entry != nil || event.SyncUp != nil {
		h.sub.stream.accepted.Store(true)
	}
}

func (h *checkpointEventHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	h.inner.Close()
}

// Keep the business handler alive across underlying stream attempts.
type nopCloseHandler struct {
	wal.TransformLogEventHandler
}

func (h *nopCloseHandler) Close() {}
