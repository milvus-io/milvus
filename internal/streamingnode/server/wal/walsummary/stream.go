package walsummary

import (
	"context"
	"math"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

// Stream adapts the shared summary reader to bounded query bootstrap reads.
// It owns no transform payload or acknowledgement state.
type Stream struct {
	reader TransformReader
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func NewStream(reader TransformReader) *Stream {
	ctx, cancel := context.WithCancel(context.Background())
	return &Stream{reader: reader, ctx: ctx, cancel: cancel}
}

func (s *Stream) Subscribe(ctx context.Context, opt wal.TransformLogSubscriptionOption) (wal.TransformLogSubscription, error) {
	if opt.VChannel == "" || opt.Handler == nil || (opt.EndTimeTick != 0 && opt.EndTimeTick < opt.StartAfterTimeTick) {
		return nil, wal.ErrTransformLogInvalidReadOption
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, context.Canceled
	}
	subctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	sub := &subscription{opt: opt, cancel: cancel, done: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(sub.done)
		defer opt.Handler.Close()
		defer stop()
		defer cancel()
		if err := s.read(subctx, opt); err != nil {
			_ = opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, Err: err})
		}
	}()
	return sub, nil
}

func (s *Stream) read(ctx context.Context, opt wal.TransformLogSubscriptionOption) error {
	through := opt.EndTimeTick
	if through == 0 {
		through = math.MaxUint64
	}
	after := opt.StartAfterTimeTick
	for {
		batch, err := s.reader.ReadTransform(ctx, opt.VChannel, after, through, ReadLimits{MaxRows: 4096, MaxBytes: 4 << 20})
		if err != nil {
			return err
		}
		if batch.FastForwardTimeTick > after {
			return wal.ErrTransformLogStartPointTruncated
		}
		for _, entry := range batch.Entries {
			if err := opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, Entry: entry}); err != nil {
				return err
			}
		}
		after = batch.CoveredThrough
		if after >= through || after >= batch.ReadableThrough {
			if opt.EndTimeTick == 0 || after >= through {
				if err := opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, SyncUp: &wal.TransformLogSyncUp{TimeTick: after}}); err != nil {
					return err
				}
			}
			if after >= through {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-batch.Changed:
			}
		}
	}
}
func (s *Stream) Done() <-chan struct{} { return s.ctx.Done() }
func (s *Stream) Error() error          { return s.ctx.Err() }
func (s *Stream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

type subscription struct {
	opt    wal.TransformLogSubscriptionOption
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *subscription) ID() int64        { return s.opt.SubscriptionID }
func (s *subscription) VChannel() string { return s.opt.VChannel }
func (s *subscription) Close() error     { s.cancel(); <-s.done; return nil }
