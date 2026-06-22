package transformlog

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type scanner struct {
	name       string
	startAfter uint64
	end        uint64
	liveAfter  uint64
	ch         chan wal.TransformLogEvent
	done       chan struct{}
	close      chan struct{}
	errMu      sync.Mutex
	err        error
	closed     sync.Once
	liveMu     sync.Mutex
	caughtUp   bool
	pending    []wal.TransformLogEvent
}

func newScanner(name string, startAfter uint64, end uint64, liveAfter uint64) *scanner {
	return &scanner{
		name:       name,
		startAfter: startAfter,
		end:        end,
		liveAfter:  liveAfter,
		ch:         make(chan wal.TransformLogEvent, 16),
		done:       make(chan struct{}),
		close:      make(chan struct{}),
	}
}

func (s *scanner) Name() string {
	return s.name
}

func (s *scanner) Chan() <-chan wal.TransformLogEvent {
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
			if s.exceedsEnd(entry.GetTimeTick()) {
				return
			}
			if !s.sendEvent(ctx, wal.TransformLogEvent{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)}) {
				return
			}
		}
	}
	if !s.sendEvent(ctx, wal.TransformLogEvent{CaughtUp: &wal.TransformLogCaughtUp{StartAfterTimeTick: s.startAfter}}) {
		return
	}
	if !s.drainPending(ctx) {
		return
	}
	if s.end > 0 {
		return
	}
	select {
	case <-s.close:
	case <-ctx.Done():
		s.setError(ctx.Err())
	}
}

func (s *scanner) sendEvent(ctx context.Context, event wal.TransformLogEvent) bool {
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
	if s.exceedsEnd(entry.GetTimeTick()) {
		return
	}
	event := wal.TransformLogEvent{Entry: proto.Clone(entry).(*streamingpb.TransformLogEntry)}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if !s.caughtUp {
		s.pending = append(s.pending, event)
		return
	}
	_ = s.sendEvent(context.Background(), event)
}

func (s *scanner) exceedsEnd(timeTick uint64) bool {
	return s.end > 0 && timeTick > s.end
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
