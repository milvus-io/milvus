package qnview

import (
	"context"
	"sync"
	"time"
)

const defaultSegmentLoadInfoReconnectBackoff = 100 * time.Millisecond

// ReconnectingSegmentLoadInfoStream maintains independent, revision-resuming
// subscriptions for physical segments. A snapshot revision is acknowledged
// only after the handler accepts it.
type ReconnectingSegmentLoadInfoStream struct {
	ctx     context.Context
	cancel  context.CancelFunc
	source  SegmentLoadInfoEventSource
	backoff time.Duration

	mu            sync.Mutex
	nextID        uint64
	subscriptions map[uint64]*segmentLoadInfoSubscription
	closed        bool
}

func NewReconnectingSegmentLoadInfoStream(
	ctx context.Context,
	source SegmentLoadInfoEventSource,
	backoff time.Duration,
) *ReconnectingSegmentLoadInfoStream {
	if ctx == nil {
		ctx = context.Background()
	}
	if backoff <= 0 {
		backoff = defaultSegmentLoadInfoReconnectBackoff
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return &ReconnectingSegmentLoadInfoStream{
		ctx:           streamCtx,
		cancel:        cancel,
		source:        source,
		backoff:       backoff,
		subscriptions: make(map[uint64]*segmentLoadInfoSubscription),
	}
}

func (s *ReconnectingSegmentLoadInfoStream) Subscribe(option SegmentLoadInfoSubscriptionOption) SegmentLoadInfoSubscription {
	ctx, cancel := context.WithCancel(s.ctx)
	subscription := &segmentLoadInfoSubscription{
		collectionID: option.CollectionID,
		segmentID:    option.SegmentID,
		handler:      option.Handler,
		revision:     option.Revision,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		if subscription.handler != nil {
			subscription.handler.Close()
		}
		close(subscription.done)
		return subscription
	}
	s.nextID++
	subscription.id = s.nextID
	subscription.owner = s
	s.subscriptions[subscription.id] = subscription
	s.mu.Unlock()

	go subscription.run(s.source, s.backoff)
	return subscription
}

func (s *ReconnectingSegmentLoadInfoStream) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	subscriptions := make([]*segmentLoadInfoSubscription, 0, len(s.subscriptions))
	for _, subscription := range s.subscriptions {
		subscriptions = append(subscriptions, subscription)
	}
	s.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.Close()
	}
}

func (s *ReconnectingSegmentLoadInfoStream) remove(id uint64) {
	s.mu.Lock()
	delete(s.subscriptions, id)
	s.mu.Unlock()
}

type segmentLoadInfoSubscription struct {
	id           uint64
	owner        *ReconnectingSegmentLoadInfoStream
	collectionID int64
	segmentID    int64
	handler      SegmentLoadInfoEventHandler
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan struct{}
	closeOnce    sync.Once

	mu       sync.Mutex
	revision SegmentLoadInfoRevision
	err      error
}

func (s *segmentLoadInfoSubscription) CollectionID() int64 { return s.collectionID }
func (s *segmentLoadInfoSubscription) SegmentID() int64    { return s.segmentID }

func (s *segmentLoadInfoSubscription) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *segmentLoadInfoSubscription) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
	})
}

func (s *segmentLoadInfoSubscription) run(source SegmentLoadInfoEventSource, backoff time.Duration) {
	defer close(s.done)
	defer func() {
		if s.handler != nil {
			s.handler.Close()
		}
		if s.owner != nil {
			s.owner.remove(s.id)
		}
	}()
	if source == nil || s.handler == nil {
		return
	}
	for s.ctx.Err() == nil {
		reader, err := source.Open(s.ctx, s.collectionID, s.segmentID, s.currentRevision())
		if err != nil {
			if !s.retry(source, err, backoff) {
				return
			}
			continue
		}
		err = s.consume(reader)
		reader.Close()
		if err == nil || s.ctx.Err() != nil {
			return
		}
		if !s.retry(source, err, backoff) {
			return
		}
	}
}

func (s *segmentLoadInfoSubscription) consume(reader SegmentLoadInfoEventReader) error {
	for s.ctx.Err() == nil {
		snapshot, err := reader.Recv()
		if err != nil {
			return err
		}
		if snapshot.CollectionID != s.collectionID || snapshot.SegmentID != s.segmentID || snapshot.Revision.Empty() {
			continue
		}
		current := s.currentRevision()
		if snapshot.Revision.Revision <= current.Revision {
			continue
		}
		if err := s.handler.Handle(cloneSegmentLoadInfoSnapshot(snapshot)); err != nil {
			return err
		}
		s.mu.Lock()
		if snapshot.Revision.Revision > s.revision.Revision {
			s.revision = snapshot.Revision
		}
		s.mu.Unlock()
	}
	return s.ctx.Err()
}

func (s *segmentLoadInfoSubscription) retry(source SegmentLoadInfoEventSource, err error, backoff time.Duration) bool {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	if !source.Retryable(err) {
		return false
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func (s *segmentLoadInfoSubscription) currentRevision() SegmentLoadInfoRevision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}
