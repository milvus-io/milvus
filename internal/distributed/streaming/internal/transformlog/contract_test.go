package transformlog_test

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// contractSource scripts the local workspace's interface, rather than importing
// its Summary reader, recovery, replay implementation or retention policy.
type contractSource struct {
	mu          sync.Mutex
	acquireErr  error
	opened      chan *contractStream
	floor       uint64
	readable    uint64
	history     []wal.TransformLogStreamEvent
	subscribers map[*contractSubscription]struct{}
}

func (*contractSource) AcquireStream(context.Context, string) (wal.TransformLogStream, error) {
	panic("mockey")
}

type contractStream struct {
	source *contractSource
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup
}

func (*contractStream) Subscribe(context.Context, wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	panic("mockey")
}
func (*contractStream) Done() <-chan struct{} { panic("mockey") }
func (*contractStream) Error() error          { panic("mockey") }
func (*contractStream) Close() error          { panic("mockey") }

type contractSubscription struct {
	opt    wal.TransformLogSubscriptionOption
	ctx    context.Context
	cancel context.CancelFunc
	events chan wal.TransformLogStreamEvent
	done   chan struct{}
}

func (*contractSubscription) ID() int64        { panic("mockey") }
func (*contractSubscription) VChannel() string { panic("mockey") }
func (*contractSubscription) Close() error     { panic("mockey") }

func newContractSource(t *testing.T) *contractSource {
	t.Helper()
	source := &contractSource{opened: make(chan *contractStream, 100), subscribers: make(map[*contractSubscription]struct{})}
	patch(t, mockey.Mock((*contractSource).AcquireStream).To(func(s *contractSource, ctx context.Context, _ string) (wal.TransformLogStream, error) {
		if s.acquireErr != nil {
			return nil, s.acquireErr
		}
		ctx, cancel := context.WithCancel(ctx) //nolint:gosec // stored in contractStream; Close cancels it
		stream := &contractStream{source: s, ctx: ctx, cancel: cancel}
		s.opened <- stream
		return stream, nil
	}).Build())
	patch(t, mockey.Mock((*contractStream).Done).To(func(s *contractStream) <-chan struct{} { return s.ctx.Done() }).Build())
	patch(t, mockey.Mock((*contractStream).Error).To(func(s *contractStream) error { return s.ctx.Err() }).Build())
	patch(t, mockey.Mock((*contractStream).Close).To(func(s *contractStream) error { s.mu.Lock(); s.cancel(); s.mu.Unlock(); s.wg.Wait(); return nil }).Build())
	patch(t, mockey.Mock((*contractSubscription).ID).To(func(s *contractSubscription) int64 { return s.opt.SubscriptionID }).Build())
	patch(t, mockey.Mock((*contractSubscription).VChannel).To(func(s *contractSubscription) string { return s.opt.VChannel }).Build())
	patch(t, mockey.Mock((*contractSubscription).Close).To(func(s *contractSubscription) error { s.cancel(); <-s.done; return nil }).Build())
	patch(t, mockey.Mock((*contractStream).Subscribe).To(func(s *contractStream, ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
		if opt.VChannel == "" || opt.Handler == nil || (opt.EndTimeTick != 0 && opt.EndTimeTick < opt.StartAfterTimeTick) {
			return nil, wal.ErrTransformLogInvalidReadOption
		}
		if opt.VChannel == "missing" {
			return nil, wal.ErrTransformLogVChannelUnavailable
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.ctx.Err() != nil {
			return nil, s.ctx.Err()
		}
		ctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(s.ctx, cancel)
		sub := &contractSubscription{opt: opt, ctx: ctx, cancel: cancel, events: make(chan wal.TransformLogStreamEvent, 100), done: make(chan struct{})}
		source := s.source
		source.mu.Lock()
		source.subscribers[sub] = struct{}{}
		if opt.StartAfterTimeTick < source.floor {
			sub.events <- wal.TransformLogStreamEvent{Err: wal.ErrTransformLogStartPointTruncated}
		} else {
			for _, event := range source.history {
				if event.VChannel == opt.VChannel && event.Entry.GetTimeTick() > opt.StartAfterTimeTick && (opt.EndTimeTick == 0 || event.Entry.GetTimeTick() <= opt.EndTimeTick) {
					sub.events <- event
				}
			}
			if source.readable >= opt.StartAfterTimeTick {
				tt := source.readable
				if opt.EndTimeTick != 0 {
					tt = min(tt, opt.EndTimeTick)
				}
				sub.events <- wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: tt}}
			}
		}
		source.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer close(sub.done)
			defer opt.Handler.Close()
			defer stop()
			defer cancel()
			defer func() { source.mu.Lock(); delete(source.subscribers, sub); source.mu.Unlock() }()
			for {
				select {
				case <-ctx.Done():
					_ = opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, Err: ctx.Err()})
					return
				case event := <-sub.events:
					event.SubscriptionID = opt.SubscriptionID
					event.VChannel = opt.VChannel
					if opt.Handler.Handle(event) != nil || event.Err != nil {
						return
					}
					if opt.EndTimeTick != 0 && event.SyncUp != nil && event.SyncUp.TimeTick >= opt.EndTimeTick {
						return
					}
				}
			}
		}()
		return sub, nil
	}).Build())
	return source
}

func (s *contractSource) publish(vc string, entry *streamingpb.TransformLogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readable = entry.GetTimeTick()
	event := wal.TransformLogStreamEvent{VChannel: vc, Entry: entry}
	s.history = append(s.history, event)
	for sub := range s.subscribers {
		if vc == sub.opt.VChannel && entry.GetTimeTick() > sub.opt.StartAfterTimeTick && (sub.opt.EndTimeTick == 0 || entry.GetTimeTick() <= sub.opt.EndTimeTick) {
			sub.events <- event
		}
		tt := s.readable
		if sub.opt.EndTimeTick != 0 {
			tt = min(tt, sub.opt.EndTimeTick)
		}
		sub.events <- wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: tt}}
	}
}
