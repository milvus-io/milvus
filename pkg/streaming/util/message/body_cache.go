package message

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// The cache owns decoded bodies only, never messages, payloads or Ack handles.
// Its byte budget is an estimate of retained data, not a process heap limit.
var globalBodyCache = sync.OnceValue(func() *bodyCacheManager {
	return newBodyCacheManager(256<<20, 30*time.Second, time.Second)
})

// All slot fields are protected by the manager's mutex. Transaction clones
// share the slot, while mutable messages never carry one.
type bodyCacheSlot struct {
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

type bodyCacheManager struct {
	mu      sync.Mutex
	entries list.List // least recently used first
	bytes   int64
	budget  int64
	idle    time.Duration
	closed  bool
	stop    chan struct{}
	done    chan struct{}
}

func newBodyCacheManager(budget int64, idle, interval time.Duration) *bodyCacheManager {
	m := &bodyCacheManager{budget: budget, idle: idle, stop: make(chan struct{}), done: make(chan struct{})}
	go m.run(interval)
	return m
}

func (m *bodyCacheManager) get(ctx context.Context, slot *bodyCacheSlot, decode func() (proto.Message, error)) (proto.Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		if slot.body != nil {
			body := slot.body
			slot.lastAccess = time.Now()
			m.entries.MoveToBack(slot.element)
			m.mu.Unlock()
			return body, nil
		}
		attempt := slot.loading
		if attempt == nil {
			// Keep the lock to install this caller's construction attempt.
			break
		}
		m.mu.Unlock()
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
	m.mu.Unlock()

	body, err := decode()
	// Estimate outside the global lock. Include a fixed overhead so empty
	// protobufs and registry entries are also bounded. Encoded size alone
	// undercounts object/slice overhead; the multiplier is still approximate.
	var size int64
	if err == nil {
		size = 256 + 2*int64(proto.Size(body))
	} else {
		body = nil
	}
	m.mu.Lock()
	if err == nil && !m.closed && size <= m.budget {
		// Every admitted entry can be evicted: existing readers own their
		// references. Make room before publishing this completed decode.
		for size > m.budget-m.bytes {
			m.remove(m.entries.Front().Value.(*bodyCacheSlot))
		}
		slot.body, slot.bytes, slot.lastAccess = body, size, time.Now()
		slot.element = m.entries.PushBack(slot)
		m.bytes += size
	}
	attempt.body, attempt.err = body, err
	attempt.canceled = err != nil && ctx.Err() != nil && errors.Is(err, ctx.Err())
	slot.loading = nil
	close(attempt.done)
	m.mu.Unlock()
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

func (m *bodyCacheManager) recycle(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for e := m.entries.Front(); e != nil; e = m.entries.Front() {
		slot := e.Value.(*bodyCacheSlot)
		if now.Sub(slot.lastAccess) < m.idle {
			break
		}
		m.remove(slot)
	}
}

// remove only drops the cache's reference; readers may still hold the body.
func (m *bodyCacheManager) remove(slot *bodyCacheSlot) {
	m.entries.Remove(slot.element)
	m.bytes -= slot.bytes
	slot.body, slot.element, slot.bytes = nil, nil, 0
}

func (m *bodyCacheManager) close() {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		close(m.stop)
		for e := m.entries.Front(); e != nil; e = m.entries.Front() {
			m.remove(e.Value.(*bodyCacheSlot))
		}
	}
	m.mu.Unlock()
	<-m.done
}
