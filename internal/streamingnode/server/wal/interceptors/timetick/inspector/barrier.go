package inspector

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// All state is guarded by inspector.barrierMu. A state belongs to one WAL
// registration; waiters do not survive a change of owner. WAB retains published
// volatile progress until consumed, so Inspector need not carry old scopes.
type barrierState struct {
	ctx      context.Context
	cancel   context.CancelFunc
	waiters  map[*barrierWaiter]struct{}
	revision uint64
}

type barrierWaiter struct {
	vchannel string
	timetick uint64
	done     chan struct{}
}

func (s *timeTickSyncInspectorImpl) EnsureBarrier(ctx context.Context, channel types.PChannelInfo, vchannel string, timetick uint64) error {
	if timetick == 0 || vchannel == "" {
		return merr.WrapErrServiceInternalMsg("query barrier requires a vchannel and timestamp")
	}
	s.barrierMu.Lock()
	state := s.barriers[channel]
	if state == nil || state.ctx.Err() != nil {
		s.barrierMu.Unlock()
		return status.NewOnShutdownError("query barrier WAL is unavailable: %s", channel.Name)
	}
	waiter := &barrierWaiter{vchannel: vchannel, timetick: timetick, done: make(chan struct{})}
	state.waiters[waiter] = struct{}{}
	state.revision++
	s.barrierMu.Unlock()
	defer func() {
		s.barrierMu.Lock()
		delete(state.waiters, waiter)
		s.barrierMu.Unlock()
	}()
	s.TriggerSync(channel, false)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-state.ctx.Done():
		return status.NewOnShutdownError("query barrier WAL is unavailable: %s", channel.Name)
	case <-waiter.done:
		return nil
	}
}

func (s *barrierState) targets() (map[string]uint64, uint64) {
	if s == nil {
		return nil, 0
	}
	// Only pending requests participate in the next publication. Idle syncs
	// must not copy or serialize every scope ever queried on this owner.
	targets := make(map[string]uint64, len(s.waiters))
	for waiter := range s.waiters {
		targets[waiter.vchannel] = max(targets[waiter.vchannel], waiter.timetick)
	}
	return targets, s.revision
}

func (s *timeTickSyncInspectorImpl) completeBarriers(state *barrierState, published map[string]uint64, err error) {
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	if state.ctx.Err() != nil {
		return
	}
	if err != nil {
		if se := status.AsStreamingError(err); se.IsFenced() || se.IsOnShutdown() {
			state.cancel()
		}
		// Other failures, or an ACK prefix still below the target, are retried
		// by the inspector's existing heartbeat. Callers own no retry timers.
		return
	}
	for waiter := range state.waiters {
		if published[waiter.vchannel] >= waiter.timetick {
			close(waiter.done)
			delete(state.waiters, waiter)
		}
	}
}
