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

package datacoord

import (
	"container/heap"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type statsReconcileKey struct {
	segmentID int64
	subjob    indexpb.StatsSubJob
}

type statsPendingEntry struct {
	key          statsReconcileKey
	collectionID int64
	generation   uint64
	sequence     uint64
	firstDirty   time.Time
	notBefore    time.Time
	failures     uint
}

type statsPendingHeap []*statsPendingEntry

func (h statsPendingHeap) Len() int { return len(h) }
func (h statsPendingHeap) Less(i, j int) bool {
	if h[i].notBefore.Equal(h[j].notBefore) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].notBefore.Before(h[j].notBefore)
}

func (h statsPendingHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *statsPendingHeap) Push(value any) {
	e := value.(*statsPendingEntry)
	*h = append(*h, e)
}

func (h *statsPendingHeap) Pop() any {
	last := len(*h) - 1
	e := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return e
}

type statsScanScope struct {
	generation uint64
	firstDirty time.Time
}

// statsReconcileQueue stores dirty state, not events. Every container is bounded,
// including delayed/in-flight keys and active collection scans. Notification
// producers never wait for task persistence, worker capacity or a metadata scan.
type statsReconcileQueue struct {
	mu                         sync.Mutex
	closed                     bool
	maxPending, maxCollections int
	sequence                   uint64
	pending                    map[statsReconcileKey]*statsPendingEntry
	perCollection              map[int64]int
	ready                      [2]statsPendingHeap
	nextJob                    int
	scopes                     map[int64]*statsScanScope // 0 is the coalesced full reconciliation.
	scanReady                  []int64
	wake                       chan struct{}
	overflows                  uint64
	reportedOverflows          uint64
}

func statsJobSlot(job indexpb.StatsSubJob) int {
	if job == indexpb.StatsSubJob_TextIndexJob {
		return 0
	}
	if job == indexpb.StatsSubJob_JsonKeyIndexJob {
		return 1
	}
	return -1
}

func newStatsReconcileQueue(maxPending, maxCollections int) *statsReconcileQueue {
	return &statsReconcileQueue{
		maxPending: maxPending, maxCollections: maxCollections,
		pending:       make(map[statsReconcileKey]*statsPendingEntry),
		perCollection: make(map[int64]int),
		scopes:        make(map[int64]*statsScanScope),
		wake:          make(chan struct{}, 1),
	}
}

func (q *statsReconcileQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// enqueue with overflow=false provides scan backpressure: the scanner retains
// its current key and retries, rather than recursively requesting another scan.
func (q *statsReconcileQueue) enqueue(collectionID int64, key statsReconcileKey, now time.Time, overflow bool) bool {
	slot := statsJobSlot(key.subjob)
	if slot < 0 {
		return true
	} // Sort/BM25 are not discovered by this inspector.
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if e := q.pending[key]; e != nil {
		// Scans are only a reconciliation request, not a new metadata version.
		// Do not keep a repeatedly deferred key hot merely by scanning it again.
		if overflow {
			e.generation++
		}
		return true
	}
	// Leave room for other collections during a large collection's backlog.
	if len(q.pending) >= q.maxPending || q.perCollection[collectionID] >= max(1, q.maxPending/8) {
		if overflow {
			q.overflows++
			q.requestScanLocked(collectionID, true)
		}
		return false
	}
	q.sequence++
	e := &statsPendingEntry{
		key: key, collectionID: collectionID, generation: 1, sequence: q.sequence,
		firstDirty: now, notBefore: now,
	}
	q.pending[key] = e
	q.perCollection[collectionID]++
	heap.Push(&q.ready[slot], e)
	q.signal()
	return true
}

func (q *statsReconcileQueue) notifySegment(collectionID, segmentID int64) {
	now := time.Now()
	q.enqueue(collectionID, statsReconcileKey{segmentID, indexpb.StatsSubJob_TextIndexJob}, now, true)
	q.enqueue(collectionID, statsReconcileKey{segmentID, indexpb.StatsSubJob_JsonKeyIndexJob}, now, true)
}

func (q *statsReconcileQueue) requestScan(collectionID int64, changed bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.requestScanLocked(collectionID, changed)
	}
}

func (q *statsReconcileQueue) requestScanLocked(collectionID int64, changed bool) {
	if s := q.scopes[collectionID]; s != nil {
		if changed {
			s.generation++
		}
		return
	}
	collections := len(q.scopes)
	if q.scopes[0] != nil {
		collections--
	}
	if collectionID != 0 && collections >= q.maxCollections {
		q.overflows++
		q.requestScanLocked(0, changed)
		return
	}
	q.scopes[collectionID] = &statsScanScope{generation: 1, firstDirty: time.Now()}
	q.scanReady = append(q.scanReady, collectionID)
}

func (q *statsReconcileQueue) beginScan() (collectionID int64, generation uint64, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.scanReady) == 0 {
		return 0, 0, false
	}
	collectionID = q.scanReady[0]
	copy(q.scanReady, q.scanReady[1:])
	q.scanReady = q.scanReady[:len(q.scanReady)-1]
	s := q.scopes[collectionID]
	return collectionID, s.generation, true
}

func (q *statsReconcileQueue) finishScan(collectionID int64, generation uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := q.scopes[collectionID]
	if q.closed || s == nil {
		return
	}
	if s.generation != generation {
		q.scanReady = append(q.scanReady, collectionID)
	} else {
		delete(q.scopes, collectionID)
	}
}

func (q *statsReconcileQueue) pop(now time.Time) (statsPendingEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return statsPendingEntry{}, false
	}
	for n := 0; n < len(q.ready); n++ {
		slot := (q.nextJob + n) % len(q.ready)
		if len(q.ready[slot]) == 0 || q.ready[slot][0].notBefore.After(now) {
			continue
		}
		e := heap.Pop(&q.ready[slot]).(*statsPendingEntry)
		q.nextJob = (slot + 1) % len(q.ready)
		return *e, true // copy generation; later notifications update the stored entry.
	}
	return statsPendingEntry{}, false
}

func (q *statsReconcileQueue) complete(work statsPendingEntry, retry bool, now time.Time, retryBase, retryMax time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e := q.pending[work.key]
	if q.closed || e == nil {
		return
	}
	if !retry && e.generation == work.generation {
		delete(q.pending, work.key)
		q.perCollection[e.collectionID]--
		if q.perCollection[e.collectionID] == 0 {
			delete(q.perCollection, e.collectionID)
		}
		return
	}
	e.notBefore = now
	if retry {
		delay := min(retryBase, retryMax)
		for i := uint(0); i < e.failures && delay < retryMax; i++ {
			delay = min(delay*2, retryMax)
		}
		e.failures = min(e.failures+1, 30)
		// Bounded deterministic jitter avoids an extra RNG lock on the hot path.
		jitter := time.Duration(uint64(e.key.segmentID)%101) * delay / 1000
		e.notBefore = now.Add(min(delay+jitter, retryMax))
	} else {
		e.failures = 0
	}
	q.sequence++
	e.sequence = q.sequence
	heap.Push(&q.ready[statsJobSlot(e.key.subjob)], e)
}

func (q *statsReconcileQueue) delay(now time.Time) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return time.Hour
	}
	delay := time.Hour
	for _, h := range q.ready {
		if len(h) > 0 {
			delay = min(delay, max(0, h[0].notBefore.Sub(now)))
		}
	}
	return delay
}

func (q *statsReconcileQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	clear(q.pending)
	clear(q.perCollection)
	clear(q.scopes)
	q.ready = [2]statsPendingHeap{}
	q.scanReady = nil
}

// Only bounded queue state is examined. Prometheus scrapes never enumerate meta.
func (q *statsReconcileQueue) updateMetrics() {
	q.mu.Lock()
	defer q.mu.Unlock()
	metrics.StatsDiscoveryPending.WithLabelValues("key").Set(float64(len(q.pending)))
	collections := len(q.scopes)
	global := 0
	if q.scopes[0] != nil {
		collections--
		global = 1
	}
	metrics.StatsDiscoveryPending.WithLabelValues("collection").Set(float64(collections))
	metrics.StatsDiscoveryPending.WithLabelValues("global").Set(float64(global))
	var oldest time.Time
	for _, e := range q.pending {
		if oldest.IsZero() || e.firstDirty.Before(oldest) {
			oldest = e.firstDirty
		}
	}
	for _, s := range q.scopes {
		if oldest.IsZero() || s.firstDirty.Before(oldest) {
			oldest = s.firstDirty
		}
	}
	age := float64(0)
	if !oldest.IsZero() {
		age = time.Since(oldest).Seconds()
	}
	metrics.StatsDiscoveryOldestAge.Set(age)
	metrics.StatsDiscoveryOverflow.Add(float64(q.overflows - q.reportedOverflows))
	q.reportedOverflows = q.overflows
}
