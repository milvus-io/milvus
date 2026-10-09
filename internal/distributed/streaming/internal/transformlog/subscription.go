package transformlog

import (
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type resumableSubscription struct {
	stream         *resumableStream
	id             int64
	vchannel       string
	endTimeTick    uint64
	nextStartAfter uint64
	complete       bool
	remote         wal.TransformLogSubscription
	handler        *checkpointEventHandler

	done       chan struct{}
	ready      chan struct{}
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

func (s *resumableSubscription) advance(timetick uint64, syncUp bool) {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	if syncUp && s.endTimeTick != 0 && timetick >= s.endTimeTick {
		s.complete = true
	}
	if timetick > s.nextStartAfter {
		s.nextStartAfter = timetick
	}
}

func (s *resumableSubscription) markReady(err error) {
	if err != nil {
		s.setError(err)
	}
	s.readyOnce.Do(func() {
		close(s.ready)
	})
}

func (s *resumableSubscription) finish(err error) {
	s.finishOnce.Do(func() {
		if err != nil {
			s.setError(err)
		}
		s.markReady(err)
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
	if h.closed {
		return io.EOF
	}
	event.SubscriptionID = h.sub.ID()
	event.VChannel = h.sub.VChannel()
	err := h.inner.Handle(event)
	if err != nil {
		return err
	}
	if event.Entry != nil {
		h.sub.advance(event.Entry.GetTimeTick(), false)
	}
	if event.SyncUp != nil {
		h.sub.advance(event.SyncUp.TimeTick, true)
	}
	return nil
}

func (h *checkpointEventHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	h.inner.Close()
}

// Underlying streams close their handlers on disconnection as well as normal
// completion. Only a proven bounded end or a terminal subscription error ends
// the logical subscription; transport failures are resumed from accepted data.
type resumeHandler struct{ sub *resumableSubscription }

func (h resumeHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Err != nil {
		h.sub.setError(event.Err)
	}
	err := h.sub.handle(event)
	if err != nil {
		h.sub.setError(err)
	}
	if event.Err != nil {
		go h.sub.stream.removeSubscription(h.sub, event.Err)
	} else if err != nil {
		go h.sub.stream.removeSubscription(h.sub, err)
	}
	return err
}

func (h resumeHandler) Close() {
	if h.sub.isComplete() {
		go h.sub.stream.removeSubscription(h.sub, nil)
	}
}

func (s *resumableSubscription) isComplete() bool {
	s.stream.mu.Lock()
	defer s.stream.mu.Unlock()
	return s.complete
}
