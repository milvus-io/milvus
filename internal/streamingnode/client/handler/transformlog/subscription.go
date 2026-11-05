package transformlog

import (
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
)

type eventSubscription struct {
	stream         *EventStream
	subscriptionID int64
	vchannel       string
	handler        wal.TransformLogEventHandler
	done           chan struct{}
	ready          chan struct{}
	// readyErr is the immutable creation result, published by closing ready.
	// It is separate from err: a normal Close can abort pending creation.
	readyErr error

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

func (s *eventSubscription) handle(event wal.TransformLogStreamEvent) {
	s.handler.Handle(event)
}

func (s *eventSubscription) markReady(err error) {
	s.readyOnce.Do(func() {
		s.readyErr = err
		close(s.ready)
	})
}

func (s *eventSubscription) finish(err error) {
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
		// The stream owner controls the RPC lifetime, including idle periods.
		s.stream.removeSubscription(s.subscriptionID)
	})
}

func (s *eventSubscription) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
