package message

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// The cache owns decoded bodies only, never messages, payloads or Ack handles.
// Its byte budget is an estimate of retained data, not a process heap limit.
var globalBodyCache = sync.OnceValue(func() *bodyCacheManager {
	return newBodyCacheManager(128<<20, 30*time.Second, time.Second)
})

const (
	bodyCacheShardCount    = 16
	bodyCacheEvictionBatch = 128
	bodyCacheEntryOverhead = 256
)

var bodyCacheNextShard atomic.Uint32

func newBodyCacheSlot() *bodyCacheSlot {
	return &bodyCacheSlot{shard: bodyCacheNextShard.Add(1) % bodyCacheShardCount}
}

// The shard is fixed at construction; all other fields are protected by that
// shard's mutex. Transaction clones share the slot, even if their TimeTick changes.
type bodyCacheSlot struct {
	shard      uint32
	body       proto.Message
	loading    *bodyCacheAttempt
	element    *list.Element
	lastAccess time.Time
	bytes      int64
}

type bodyCacheAttempt struct {
	done     chan struct{}
	body     proto.Message
	err      error
	canceled bool
}

type bodyCacheShard struct {
	mu      sync.Mutex
	entries list.List // least recently used first, within this shard
}

type bodyCacheManager struct {
	shards    [bodyCacheShardCount]bodyCacheShard
	bytes     atomic.Int64 // includes short-lived reservations before publication
	budget    int64
	idle      time.Duration
	closed    atomic.Bool
	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
}

func newBodyCacheManager(budget int64, idle, interval time.Duration) *bodyCacheManager {
	m := &bodyCacheManager{budget: budget, idle: idle, stop: make(chan struct{}), done: make(chan struct{})}
	go m.run(interval)
	return m
}

func (m *bodyCacheManager) get(ctx context.Context, slot *bodyCacheSlot, decode func() (proto.Message, int, error)) (proto.Message, error) {
	shard := &m.shards[slot.shard]
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		shard.mu.Lock()
		if slot.body != nil {
			body := slot.body
			slot.lastAccess = time.Now()
			shard.entries.MoveToBack(slot.element)
			shard.mu.Unlock()
			return body, nil
		}
		attempt := slot.loading
		if attempt == nil {
			// Keep the lock to install this caller's construction attempt.
			break
		}
		shard.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-attempt.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if attempt.canceled {
				// The loader's cancellation must not cancel independent callers.
				continue
			}
			return attempt.body, attempt.err
		}
	}
	attempt := &bodyCacheAttempt{done: make(chan struct{})}
	slot.loading = attempt
	shard.mu.Unlock()

	body, payloadBytes, err := decode()
	// Account for the plaintext bytes already consumed by the decoder, without
	// traversing the decoded protobuf again. This is still a heap estimate.
	size := int64(bodyCacheEntryOverhead) + 2*int64(payloadBytes)
	reserved := false
	if err == nil {
		reserved = m.reserve(ctx, size, int(slot.shard))
	} else {
		body = nil
	}
	shard.mu.Lock()
	if reserved {
		if m.closed.Load() {
			m.bytes.Add(-size)
		} else {
			slot.body, slot.bytes, slot.lastAccess = body, size, time.Now()
			slot.element = shard.entries.PushBack(slot)
		}
	}
	attempt.body, attempt.err = body, err
	attempt.canceled = err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err())
	slot.loading = nil
	close(attempt.done)
	shard.mu.Unlock()
	return body, err
}

func (m *bodyCacheManager) run(interval time.Duration) {
	defer close(m.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case now := <-ticker.C:
			m.recycle(now)
		}
	}
}

// tryReserve uses a shared byte budget without a process-wide LRU lock. Hits
// never touch this counter; even a body larger than budget/shardCount can fit.
func (m *bodyCacheManager) tryReserve(size int64) bool {
	for used := m.bytes.Load(); size <= m.budget-used; used = m.bytes.Load() {
		if m.bytes.CompareAndSwap(used, used+size) {
			return true
		}
	}
	return false
}

func (m *bodyCacheManager) reserve(ctx context.Context, size int64, first int) bool {
	if size > m.budget || m.closed.Load() {
		return false
	}
	if m.tryReserve(size) {
		return true
	}
	// Bound pressure work even if other publishers continuously refill shards.
	// A reservation not yet published cannot be evicted; bypass residency if
	// competing publishers leave no room after this bounded pass.
	remaining := m.bytes.Load() / bodyCacheEntryOverhead
	for remaining > 0 {
		removed := int64(0)
		for n := range len(m.shards) {
			if ctx.Err() != nil || m.closed.Load() {
				return false
			}
			shard := &m.shards[(first+n)%len(m.shards)]
			shard.mu.Lock()
			count := int64(0)
			for count < min(int64(bodyCacheEvictionBatch), remaining) && m.bytes.Load() > m.budget-size {
				e := shard.entries.Front()
				if e == nil {
					break
				}
				m.remove(shard, e.Value.(*bodyCacheSlot))
				count++
			}
			shard.mu.Unlock()
			remaining -= count
			removed += count
			if m.tryReserve(size) {
				return true
			}
		}
		if removed == 0 {
			return false
		}
	}
	return false
}

func (m *bodyCacheManager) recycle(now time.Time) {
	for i := range m.shards {
		for !m.closed.Load() && m.recycleBatch(&m.shards[i], now) {
		}
	}
}

// recycleBatch bounds each lock hold independently of the expired population.
func (m *bodyCacheManager) recycleBatch(shard *bodyCacheShard, now time.Time) bool {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	for range bodyCacheEvictionBatch {
		e := shard.entries.Front()
		if e == nil {
			return false
		}
		slot := e.Value.(*bodyCacheSlot)
		if !now.IsZero() && now.Sub(slot.lastAccess) < m.idle {
			return false
		}
		m.remove(shard, slot)
	}
	return true
}

// remove only drops the cache's reference; readers may still hold the body.
// The caller holds this shard's lock, and never another shard's lock.
func (m *bodyCacheManager) remove(shard *bodyCacheShard, slot *bodyCacheSlot) {
	shard.entries.Remove(slot.element)
	m.bytes.Add(-slot.bytes)
	slot.body, slot.element, slot.bytes = nil, nil, 0
}

func (m *bodyCacheManager) close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		close(m.stop)
		for i := range m.shards {
			// Closed managers cannot admit new bodies. Expire even freshly hit entries.
			for m.recycleBatch(&m.shards[i], time.Time{}) {
			}
		}
	})
	<-m.done
}
