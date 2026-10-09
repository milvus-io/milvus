package transformlog

import (
	"io"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type eventSubscription struct {
	stream         *EventStream
	subscriptionID int64
	vchannel       string
	endTimeTick    uint64
	handler        wal.TransformLogEventHandler
	done           chan struct{}
	ready          chan struct{}

	handlerMu  sync.Mutex
	finished   bool
	errMu      sync.Mutex
	err        error
	readyOnce  sync.Once
	closeOnce  sync.Once
	finishOnce sync.Once
}

func newEventSubscription(stream *EventStream, subscriptionID int64, opt wal.TransformLogSubscriptionOption) *eventSubscription {
	return &eventSubscription{
		stream:         stream,
		subscriptionID: subscriptionID,
		vchannel:       opt.VChannel,
		endTimeTick:    opt.EndTimeTick,
		handler:        opt.Handler,
		done:           make(chan struct{}),
		ready:          make(chan struct{}),
	}
}

func (s *eventSubscription) ID() int64 {
	return s.subscriptionID
}

func (s *eventSubscription) VChannel() string {
	return s.vchannel
}

func (s *eventSubscription) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *eventSubscription) Close() error {
	select {
	case <-s.done:
		return s.Error()
	default:
	}
	s.requestClose()
	<-s.done
	return s.Error()
}

// requestClose releases the server reader, including after a consumer failure
// has already finished this subscription locally.
func (s *eventSubscription) requestClose() {
	s.closeOnce.Do(func() {
		if err := s.stream.sendCloseSubscription(s.subscriptionID); err != nil {
			s.stream.removeSubscription(s.subscriptionID)
			s.finish(err)
		}
	})
}

func (s *eventSubscription) reject(err error) {
	s.setError(err)
	s.stream.removeSubscription(s.subscriptionID)
	// The receive callback must not wait for its own close acknowledgement.
	go s.requestClose()
	s.finish(err)
}

func (s *eventSubscription) handle(event wal.TransformLogStreamEvent) error {
	s.handlerMu.Lock()
	defer s.handlerMu.Unlock()
	if s.finished {
		return io.EOF
	}
	return s.handler.Handle(event)
}

func (s *eventSubscription) markReady(err error) {
	if err != nil {
		s.setError(err)
	}
	s.readyOnce.Do(func() {
		close(s.ready)
	})
}

func (s *eventSubscription) finish(err error) {
	s.finishOnce.Do(func() {
		if err != nil {
			s.setError(err)
		}
		s.markReady(err)
		s.handlerMu.Lock()
		s.finished = true
		s.handler.Close()
		s.handlerMu.Unlock()
		close(s.done)
		s.stream.onSubscriptionFinished(s.subscriptionID)
	})
}

func (s *eventSubscription) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
