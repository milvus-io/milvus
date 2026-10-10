package walsummary

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	scopedNotifications := false
	if opt.EndTimeTick == 0 {
		if watcher, ok := s.reader.(TransformChangeWatcher); ok {
			release := watcher.WatchTransform(opt.VChannel)
			defer release()
			scopedNotifications = true
		}
	}
	through := opt.EndTimeTick
	if through == 0 {
		through = math.MaxUint64
	}
	after := opt.StartAfterTimeTick
	for {
		batch, err := s.readBatch(ctx, opt.VChannel, after, through)
		if err != nil {
			return err
		}
		for _, entry := range batch.Entries {
			if err := opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, Entry: entry}); err != nil {
				return err
			}
		}
		// Accept Summary's retained lower bound, including an entirely retired
		// bounded interval. Never advance beyond the requested end.
		after = max(batch.CoveredThrough, min(batch.FastForwardTimeTick, through))
		if after >= through || after >= batch.ReadableThrough {
			if opt.EndTimeTick == 0 || after >= through {
				if err := opt.Handler.Handle(wal.TransformLogStreamEvent{SubscriptionID: opt.SubscriptionID, VChannel: opt.VChannel, SyncUp: &wal.TransformLogSyncUp{TimeTick: after}}); err != nil {
					return err
				}
			}
			if after >= through {
				return nil
			}
			changed := batch.Changed
			if scopedNotifications {
				changed = batch.TransformChanged
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			}
		}
	}
}

// readBatch retries storage failures without advancing the delivery cursor.
// Retry outside ReadTransform so its snapshot lock and GC pin are released
// during backoff. Handler failures are deliberately outside this retry loop.
func (s *Stream) readBatch(ctx context.Context, vchannel string, after, through uint64) (TransformBatch, error) {
	var retryBackoff *backoff.ExponentialBackOff
	for {
		if err := ctx.Err(); err != nil {
			return TransformBatch{}, err
		}
		batch, err := s.reader.ReadTransform(ctx, vchannel, after, through, ReadLimits{MaxRows: 4096, MaxBytes: 4 << 20})
		if err == nil {
			return batch, nil
		}
		if ctx.Err() != nil {
			return TransformBatch{}, ctx.Err()
		}
		// A retained chunk is pinned against GC during the read. Its absence or
		// corruption cannot be repaired by retrying this subscription. Unknown
		// read failures, including operation-local timeouts, remain retryable.
		if errors.Is(err, ErrStoreCorrupted) || errors.Is(err, merr.ErrIoKeyNotFound) || errors.Is(err, merr.ErrDataIntegrity) {
			return TransformBatch{}, err
		}
		if retryBackoff == nil {
			retryBackoff = backoff.NewExponentialBackOff()
			retryBackoff.InitialInterval = 100 * time.Millisecond
			retryBackoff.MaxInterval = 10 * time.Second
			retryBackoff.MaxElapsedTime = 0
			retryBackoff.Reset()
		}
		delay := retryBackoff.NextBackOff()
		mlog.RatedWarn(ctx, 1, "retrying transform log read",
			mlog.FieldVChannel(vchannel), mlog.Uint64("startAfterTimeTick", after),
			mlog.Duration("backoff", delay), mlog.Err(err))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return TransformBatch{}, ctx.Err()
		case <-timer.C:
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
