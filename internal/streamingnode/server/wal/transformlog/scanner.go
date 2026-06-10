package transformlog

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	transformlogapi "github.com/milvus-io/milvus/internal/streamingnode/transformlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type scanner struct {
	name       string
	startAfter uint64
	liveAfter  uint64
	ch         chan transformlogapi.Event
	done       chan struct{}
	close      chan struct{}
	errMu      sync.Mutex
	err        error
	closed     sync.Once
	liveMu     sync.Mutex
	caughtUp   bool
	pending    []transformlogapi.Event
}

func newScanner(name string, startAfter uint64, liveAfter uint64) *scanner {
	return &scanner{
		name:       name,
		startAfter: startAfter,
		liveAfter:  liveAfter,
		ch:         make(chan transformlogapi.Event, 16),
		done:       make(chan struct{}),
		close:      make(chan struct{}),
	}
}

func (s *scanner) Name() string {
	return s.name
}

func (s *scanner) Chan() <-chan transformlogapi.Event {
	return s.ch
}

func (s *scanner) Error() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

func (s *scanner) Done() <-chan struct{} {
	return s.done
}

func (s *scanner) Close() error {
	s.closed.Do(func() {
		close(s.close)
	})
	<-s.done
	return s.Error()
}

func (s *scanner) send(ctx context.Context, transformLog *transformLog, chunks []*streamingpb.TransformLogChunk) {
	defer close(s.done)
	defer transformLog.unregisterScanner(s)
	for _, chunk := range chunks {
		for _, entry := range chunk.GetEntries() {
			if entry.GetTimeTick() <= s.startAfter {
				continue
			}
			if !s.sendEvent(ctx, transformlogapi.Event{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)}) {
				return
			}
		}
	}
	if !s.sendEvent(ctx, transformlogapi.Event{CaughtUp: &transformlogapi.CaughtUp{StartAfterTimeTick: s.startAfter}}) {
		return
	}
	if !s.drainPending(ctx) {
		return
	}
	select {
	case <-s.close:
	case <-ctx.Done():
		s.setError(ctx.Err())
	}
}

func (s *scanner) sendEvent(ctx context.Context, event transformlogapi.Event) bool {
	select {
	case s.ch <- event:
		return true
	case <-s.close:
		return false
	case <-ctx.Done():
		s.setError(ctx.Err())
		return false
	}
}

func (s *scanner) publishEntry(entry *streamingpb.TransformLogEntry) {
	if entry.GetTimeTick() <= s.liveAfter {
		return
	}
	event := transformlogapi.Event{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if !s.caughtUp {
		s.pending = append(s.pending, event)
		return
	}
	_ = s.sendEvent(context.Background(), event)
}

func (s *scanner) drainPending(ctx context.Context) bool {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	s.caughtUp = true
	for _, event := range s.pending {
		if !s.sendEvent(ctx, event) {
			return false
		}
	}
	s.pending = nil
	return true
}

func (s *scanner) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}
