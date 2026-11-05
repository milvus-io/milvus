// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package idempotency

import (
	"container/heap"
	"container/list"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type cachedRecord struct {
	*idempotencyview.Record
	window *Window
	key    IdempotencyKey
	at     time.Time
	size   int
	seq    uint64
	global *list.Element
	local  *list.Element
}

// windowManager owns one PChannel's committed records. Pending owners remain
// VChannel-scoped and never participate in TTL or capacity eviction.
type windowManager struct {
	mu       sync.Mutex
	config   WindowConfig
	order    list.List
	eligible windowHeap
	bytes    int
	sequence uint64
	stop     chan struct{}
	done     chan struct{}
	close    sync.Once
}

func newWindowManager(config WindowConfig) *windowManager {
	if config.Now == nil {
		config.Now = time.Now
	}
	return &windowManager{config: config}
}

func (m *windowManager) newWindow(vchannel string) *Window {
	return &Window{
		mu: &m.mu, manager: m, vchannel: vchannel, heapIndex: -1,
		entries:  make(map[IdempotencyKey]*cachedRecord),
		inflight: make(map[IdempotencyKey]*PendingEntry),
	}
}

func (m *windowManager) expired(record *cachedRecord, now time.Time) bool {
	return m.config.TTL > 0 && !now.Before(record.at.Add(m.config.TTL))
}

func (m *windowManager) expireLocked(now time.Time, limit int) int {
	removed := 0
	for m.order.Len() > 0 && removed < limit {
		record := m.order.Front().Value.(*cachedRecord)
		if !m.expired(record, now) {
			break
		}
		m.removeLocked(record)
		removed++
	}
	return removed
}

func (m *windowManager) touchLocked(record *cachedRecord, now time.Time) {
	record.at = now
	m.sequence++
	record.seq = m.sequence
	m.order.MoveToBack(record.global)
	record.window.order.MoveToBack(record.local)
	m.updateEligibleLocked(record.window)
}

func (m *windowManager) addLocked(w *Window, record *idempotencyview.Record, at time.Time) {
	key := IdempotencyKey(record.IdempotencyKey)
	if old := w.entries[key]; old != nil {
		m.removeLocked(old)
	}
	m.sequence++
	item := &cachedRecord{Record: record, window: w, key: key, at: at, size: record.Size(), seq: m.sequence}
	item.global = m.order.PushBack(item)
	item.local = w.order.PushBack(item)
	w.entries[key] = item
	w.bytes += item.size
	m.bytes += item.size
	m.updateEligibleLocked(w)
}

func (m *windowManager) removeLocked(record *cachedRecord) {
	w := record.window
	delete(w.entries, record.key)
	w.order.Remove(record.local)
	m.order.Remove(record.global)
	w.bytes -= record.size
	m.bytes -= record.size
	m.updateEligibleLocked(w)
	observeWindowEviction(w.vchannel, 1)
	observeWindowEntries(w.vchannel, len(w.entries))
}

func (m *windowManager) evictLocked(now time.Time) int {
	removed := 0
	for m.config.MaxSize > 0 && m.bytes > m.config.MaxSize {
		oldest := m.order.Front().Value.(*cachedRecord)
		// TTL always overrides minCount. Otherwise prefer an over-quota
		// VChannel, falling back to global LRU when all soft floors bind.
		if !m.expired(oldest, now) && len(m.eligible) > 0 {
			oldest = m.eligible[0].order.Front().Value.(*cachedRecord)
		}
		m.removeLocked(oldest)
		removed++
	}
	return removed
}

func (m *windowManager) updateEligibleLocked(w *Window) {
	if m.config.MinCount <= 0 {
		return
	}
	if len(w.entries) > m.config.MinCount {
		if w.heapIndex < 0 {
			heap.Push(&m.eligible, w)
		} else {
			heap.Fix(&m.eligible, w.heapIndex)
		}
	} else if w.heapIndex >= 0 {
		heap.Remove(&m.eligible, w.heapIndex)
	}
}

// restore is the compatibility path for unordered caller-provided snapshots.
// It preserves original commit age, without extending it to WAL-open time.
// Production WAL startup uses restoreOrdered and does not build or sort this array.
func (m *windowManager) restore(snapshots map[string]*idempotencyview.Snapshot) map[string]*Window {
	type recovered struct {
		window *Window
		record *idempotencyview.Record
	}
	windows := make(map[string]*Window, len(snapshots))
	var records []recovered
	now := m.config.Now()
	for vc, snapshot := range snapshots {
		w := m.newWindow(vc)
		windows[vc] = w
		for _, record := range snapshot.Records {
			if record == nil || record.IdempotencyKey == "" {
				continue
			}
			if m.config.TTL > 0 && !now.Before(tsoutil.PhysicalTime(record.SourceTimeTick).Add(m.config.TTL)) {
				continue
			}
			records = append(records, recovered{window: w, record: record})
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].record.SourceTimeTick < records[j].record.SourceTimeTick })
	for _, item := range records {
		at := tsoutil.PhysicalTime(item.record.SourceTimeTick)
		if at.After(now) {
			at = now
		}
		m.addLocked(item.window, item.record, at)
		m.evictLocked(now)
	}
	return windows
}

// restoreOrdered consumes Summary's chronological chunks directly. The generic
// map-based restore above still accepts unordered caller-provided snapshots.
func (m *windowManager) restoreOrdered(snapshot *idempotencyview.RecoverySnapshot) map[string]*Window {
	windows := make(map[string]*Window, len(snapshot.VChannels))
	now := m.config.Now()
	snapshot.Range(func(vc string, record *idempotencyview.Record) bool {
		at := tsoutil.PhysicalTime(record.SourceTimeTick)
		if m.config.TTL > 0 && !now.Before(at.Add(m.config.TTL)) {
			return true
		}
		if at.After(now) {
			at = now
		}
		w := windows[vc]
		if w == nil {
			w = m.newWindow(vc)
			windows[vc] = w
		}
		m.addLocked(w, record, at)
		m.evictLocked(now)
		return true
	})
	return windows
}

func (m *windowManager) start() {
	m.stop, m.done = make(chan struct{}), make(chan struct{})
	if m.config.TTL <= 0 {
		close(m.done)
		return
	}
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(min(time.Second, m.config.TTL))
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ticker.C:
				for {
					m.mu.Lock()
					removed := m.expireLocked(m.config.Now(), 256)
					m.mu.Unlock()
					if removed < 256 {
						break
					}
					select {
					case <-m.stop:
						return
					default:
					}
				}
			}
		}
	}()
}

func (m *windowManager) shutdown() {
	m.close.Do(func() { close(m.stop) })
	<-m.done
}

// Only channels above their soft count enter this heap. Its root identifies
// the oldest evictable entry without scanning protected VChannels.
type windowHeap []*Window

func (h windowHeap) Len() int { return len(h) }

func (h windowHeap) Less(i, j int) bool {
	return h[i].order.Front().Value.(*cachedRecord).seq < h[j].order.Front().Value.(*cachedRecord).seq
}

func (h windowHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex, h[j].heapIndex = i, j
}

func (h *windowHeap) Push(value any) {
	w := value.(*Window)
	w.heapIndex = len(*h)
	*h = append(*h, w)
}

func (h *windowHeap) Pop() any {
	last := len(*h) - 1
	w := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	w.heapIndex = -1
	return w
}
