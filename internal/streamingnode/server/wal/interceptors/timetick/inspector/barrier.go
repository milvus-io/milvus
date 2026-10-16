package inspector

import (
	"context"
	"maps"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// All state is guarded by inspector.barrierMu. A state belongs to one WAL
// registration; neither sent watermarks nor waiters survive a change of owner.
type barrierState struct {
	ctx      context.Context
	cancel   context.CancelFunc
	sent     map[string]uint64
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
	if state.sent[vchannel] >= timetick {
		s.barrierMu.Unlock()
		return nil
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
	// Carry already published frontiers on later sync messages as well. A
	// coalesced idle confirmation or durable catchup must preserve them.
	targets := maps.Clone(s.sent)
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
	for vchannel, tick := range published {
		state.sent[vchannel] = max(state.sent[vchannel], tick)
	}
	for waiter := range state.waiters {
		if state.sent[waiter.vchannel] >= waiter.timetick {
			close(waiter.done)
			delete(state.waiters, waiter)
		}
	}
}
