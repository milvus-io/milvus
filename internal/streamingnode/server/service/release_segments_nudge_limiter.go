package service

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// nudgeIntervalFlushRetryMultiplier scales the flush retry cadence into the
// nudge cadence.
//
// A nudge is a collection-scoped ManualFlush; it is worth appending ONCE per
// debt episode, because the first one seals the segments and from then on the
// flush path re-drives itself every dataNode.flushRetryInterval without needing
// another seal message. Repeating it faster than the flush can plausibly finish
// (seal -> sync task -> upload, i.e. several retry rounds) buys nothing and
// costs a collection-scoped FlushAndFenceSegmentAllocUntil plus a WAL record
// each time — exactly when storage is already unhealthy, which is the only
// situation where the debt does not settle immediately.
//
// The repeat still has to exist: a segment created after the previous
// ManualFlush is not sealed by it, so a debt episode can outlive its nudge.
// Ten retry rounds (30s at the default 3s) keeps the WAL churn negligible
// against a queryCoord.checkSegmentInterval of 3s — which would otherwise
// produce one ManualFlush every 3s for as long as the flush is stuck — while
// still re-nudging well within a human-visible timescale.
const nudgeIntervalFlushRetryMultiplier = 10

type nudgeKey struct {
	collectionID int64
	vchannel     string
}

// nudgeLimiter rate-limits release nudges per (collection, vchannel).
//
// It is a plain mutex-guarded map of last-nudge times, checked and set in one
// step BEFORE the append runs (outside the lock — holding it across a WAL
// append would serialize every concurrent release behind one network round
// trip). The nudge is therefore best effort: if the append then fails, that
// interval's nudge is simply wasted, with no compensation. The failure is
// harmless — the debt is still reported on every check (pending=true), the
// release is still refused, and the next interval re-nudges anyway. That
// harmlessness is what makes a reservation/release protocol unnecessary here.
//
// Entries older than the interval can no longer suppress anything, so they are
// pruned on every call. The map is therefore bounded by the channels nudged in
// the last interval, not by every channel ever released.
type nudgeLimiter struct {
	mu sync.Mutex
	// interval overrides the paramtable-derived cadence. Test-only; a negative
	// value disables rate limiting entirely.
	interval  time.Duration
	lastNudge map[nudgeKey]time.Time
}

func (l *nudgeLimiter) currentInterval() time.Duration {
	if l.interval != 0 {
		return l.interval
	}
	return nudgeIntervalFlushRetryMultiplier *
		paramtable.Get().DataNodeCfg.FlushRetryInterval.GetAsDuration(time.Millisecond)
}

// allow reports whether a nudge may be appended now, recording the nudge time
// when it may. The grant is consumed immediately — see the type comment for
// why a failed append afterwards needs no compensation.
func (l *nudgeLimiter) allow(key nudgeKey, now time.Time) bool {
	interval := l.currentInterval()
	if interval < 0 {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	for staleKey, last := range l.lastNudge {
		if now.Sub(last) >= interval {
			delete(l.lastNudge, staleKey)
		}
	}
	if _, ok := l.lastNudge[key]; ok {
		return false
	}
	if l.lastNudge == nil {
		l.lastNudge = make(map[nudgeKey]time.Time)
	}
	l.lastNudge[key] = now
	return true
}
