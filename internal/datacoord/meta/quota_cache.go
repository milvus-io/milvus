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

package meta

import (
	"maps"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

// collectionQuotaCache keeps absolute per-collection quota statistics for the
// segments of SegmentsInfo and recomputes only invalidated collections.
//
// Writers publish a segment first and invalidate its collection afterwards;
// a refresh reads the dirty generation of a collection before it copies the
// segment pointers. A refresh can therefore only attach a generation to a
// snapshot at least as new as the writes that generation covers, and a write
// it may have missed leaves a newer generation for the next refresh.
// Multi-segment mutations are installed with PublishSegments; the pointer copy
// uses MetaStore.GetSegments, so it never observes half of one.
type collectionQuotaCache struct {
	// tracking skips invalidation bookkeeping while no reader uses the cache.
	// It changes only under dirtyMu.
	tracking atomic.Bool
	// dirtyMu guards generation and dirty. It is a leaf lock.
	dirtyMu    sync.Mutex
	generation uint64
	dirty      map[int64]uint64

	// mu serializes refresh/publication and acknowledgement. Writers never take
	// it. Writers hold the MetaStore shard before dirtyMu; readers never hold
	// dirtyMu while acquiring a shard.
	mu                sync.Mutex
	revision          uint64
	baseline          uint64
	consumerNeedsFull bool
	values            map[int64]*metricsinfo.CollectionQuotaSnapshot
	pending           map[int64]*metricsinfo.CollectionQuotaSnapshot
	total             int64
}

// invalidateQuota runs after an accepted cache mutation of old (nil on insert)
// into current (nil on drop).
func (s *SegmentsInfo) invalidateQuota(old, current *datapb.SegmentInfo) {
	if old != nil {
		s.invalidateQuotaCollection(old.GetCollectionID())
	}
	if current != nil && (old == nil || old.GetCollectionID() != current.GetCollectionID()) {
		s.invalidateQuotaCollection(current.GetCollectionID())
	}
}

// invalidateQuotaCollection invalidates a collection whose quota statistics or
// display labels changed without a segment publication.
func (s *SegmentsInfo) invalidateQuotaCollection(id int64) {
	q := &s.quota
	if !q.tracking.Load() {
		return
	}
	q.dirtyMu.Lock()
	defer q.dirtyMu.Unlock()
	if !q.tracking.Load() {
		return
	}
	q.generation++
	q.dirty[id] = q.generation
}

func computeCollectionQuota(segments []*SegmentInfo, generation uint64) *metricsinfo.CollectionQuotaSnapshot {
	result := &metricsinfo.CollectionQuotaSnapshot{Generation: generation, Present: len(segments) != 0}
	var deltalog deltalogAggregate
	for _, segment := range segments {
		if !IsSegmentHealthy(segment) {
			continue
		}
		result.HasRows = true
		result.Rows += segment.GetNumOfRows()
		if segment.GetIsImporting() {
			continue
		}
		result.HasQuota = true
		deltalog.observe(segment)
		size := segment.GetSegmentSize()
		result.Bytes += size
		if result.PartitionBytes == nil {
			result.PartitionBytes = make(map[int64]int64)
		}
		result.PartitionBytes[segment.GetPartitionID()] += size
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			result.HasL0 = true
			result.L0Rows += segment.getDeltaCount()
		}
		index := -1
		for i := range result.Display {
			if result.Display[i].State == segment.GetState() {
				index = i
				break
			}
		}
		if index < 0 {
			index = len(result.Display)
			result.Display = append(result.Display, metricsinfo.QuotaStateStats{State: segment.GetState()})
		}
		result.Display[index].Bytes += size
		result.Display[index].Rows += segment.GetNumOfRows()
		result.Display[index].Files += int64(getBinlogFileCount(segment.SegmentInfo))
	}
	result.L0DeltalogFiles = deltalog.l0FileCount
	result.DeltalogQuantiles = deltalog.quantiles()
	return result
}

// refreshQuota visits only collections invalidated since their last refresh.
// Published SegmentInfo objects are immutable; copying the local pointer list
// through MetaStore gives a consistent collection snapshot. New writes can
// proceed while that snapshot is reduced. A hot collection may publish a
// historical snapshot, retaining its newer dirty generation for the next
// refresh. The caller holds quota.mu.
func (s *SegmentsInfo) refreshQuota() int {
	cache := &s.quota
	cache.dirtyMu.Lock()
	initialize := !cache.tracking.Load()
	if initialize {
		cache.generation++
		cache.dirty = make(map[int64]uint64)
		cache.tracking.Store(true)
	}
	cache.dirtyMu.Unlock()
	if initialize {
		// Publish tracking before enumeration, so concurrent writes either enter
		// this baseline or leave a newer dirty generation for the next refresh.
		segments := s.store.GetAllSegments()
		cache.dirtyMu.Lock()
		for _, segment := range segments {
			id := segment.GetCollectionID()
			if cache.dirty[id] == 0 {
				cache.dirty[id] = cache.generation
			}
		}
		cache.dirtyMu.Unlock()
	}
	cache.dirtyMu.Lock()
	// Transfer pending invalidations to this refresh. A cleared million-entry
	// map retains its table; reusing it would make sparse future rounds scan
	// that old capacity. Writers immediately get a small fresh work set.
	dirtyCollections := cache.dirty
	if len(dirtyCollections) > 0 {
		cache.dirty = make(map[int64]uint64)
	} else {
		dirtyCollections = nil
	}
	cache.dirtyMu.Unlock()
	if cache.values == nil {
		cache.baseline++
		cache.consumerNeedsFull = true
		cache.values = make(map[int64]*metricsinfo.CollectionQuotaSnapshot)
		cache.pending = make(map[int64]*metricsinfo.CollectionQuotaSnapshot)
	}
	scanned := 0
	for id, generation := range dirtyCollections {
		// Read the generation before copying the collection. GetSegments holds
		// the master's collection shard, so a compaction is seen whole.
		cache.dirtyMu.Lock()
		if newer := cache.dirty[id]; newer > generation {
			generation = newer
		}
		cache.dirtyMu.Unlock()
		protos := s.store.GetSegments(id)
		segments := make([]*SegmentInfo, 0, len(protos))
		for _, p := range protos {
			segments = append(segments, &SegmentInfo{SegmentInfo: p})
		}
		scanned += len(segments)
		next := computeCollectionQuota(segments, generation)
		if old := cache.values[id]; old != nil {
			if old.Generation >= generation {
				continue
			}
			cache.total -= old.Bytes
		}
		cache.total += next.Bytes
		if next.Present {
			cache.values[id] = next
		} else {
			delete(cache.values, id)
		}
		cache.pending[id] = next
		cache.revision++
		cache.dirtyMu.Lock()
		if cache.dirty[id] == generation {
			delete(cache.dirty, id)
		}
		cache.dirtyMu.Unlock()
	}
	metrics.DataCoordQuotaCacheWork.WithLabelValues("collection").Add(float64(len(dirtyCollections)))
	metrics.DataCoordQuotaCacheWork.WithLabelValues("segment").Add(float64(scanned))
	cache.dirtyMu.Lock()
	metadataPending := len(cache.dirty)
	cache.dirtyMu.Unlock()
	metrics.DataCoordQuotaCachePending.WithLabelValues("metadata").Set(float64(metadataPending))
	metrics.DataCoordQuotaCachePending.WithLabelValues("consumer").Set(float64(len(cache.pending)))
	return scanned
}

// ReadQuotaSnapshot is for the single in-process QuotaCenter consumer. A
// separate diagnostic reader requests full=true and never acknowledges changes.
func (s *SegmentsInfo) ReadQuotaSnapshot(full bool) *metricsinfo.QuotaSnapshot {
	s.quota.mu.Lock()
	defer s.quota.mu.Unlock()
	scanned := s.refreshQuota()
	full = full || s.quota.consumerNeedsFull
	values := s.quota.pending
	if full {
		values = s.quota.values
	}
	// Copy entries, not the source map's retained capacity after acknowledgements.
	changes := make(map[int64]*metricsinfo.CollectionQuotaSnapshot, len(values))
	maps.Copy(changes, values)
	return &metricsinfo.QuotaSnapshot{Full: full, BaselineGeneration: s.quota.baseline, Revision: s.quota.revision, TotalBytes: s.quota.total, Collections: changes, ScannedSegments: scanned}
}

// AcknowledgeQuotaSnapshot clears only values actually incorporated by the
// consumer. It cannot clear a newer publication or metadata awaiting refresh.
func (s *SegmentsInfo) AcknowledgeQuotaSnapshot(snapshot *metricsinfo.QuotaSnapshot) {
	s.quota.mu.Lock()
	defer s.quota.mu.Unlock()
	if snapshot.BaselineGeneration != s.quota.baseline {
		return
	}
	if snapshot.Full {
		s.quota.consumerNeedsFull = false
	}
	previousSize := len(s.quota.pending)
	for id, value := range snapshot.Collections {
		if pending := s.quota.pending[id]; pending != nil && pending.Generation == value.Generation {
			delete(s.quota.pending, id)
		}
	}
	if len(s.quota.pending)*2 < previousSize {
		pending := make(map[int64]*metricsinfo.CollectionQuotaSnapshot, len(s.quota.pending))
		maps.Copy(pending, s.quota.pending)
		s.quota.pending = pending
	}
	metrics.DataCoordQuotaCachePending.WithLabelValues("consumer").Set(float64(len(s.quota.pending)))
}

// resetQuotaCache is called only on the disabled reference path. It prevents
// inactive incremental tracking from retaining an unconsumed history of drops.
func (s *SegmentsInfo) resetQuotaCache() {
	if !s.quota.tracking.Load() {
		return
	}
	s.quota.mu.Lock()
	defer s.quota.mu.Unlock()
	s.quota.dirtyMu.Lock()
	defer s.quota.dirtyMu.Unlock()
	if !s.quota.tracking.Load() {
		return
	}
	s.quota.tracking.Store(false)
	s.quota.dirty = nil
	s.quota.values = nil
	s.quota.pending = nil
	s.quota.total = 0
	s.quota.baseline++
	s.quota.consumerNeedsFull = true
	metrics.DataCoordQuotaCachePending.WithLabelValues("metadata").Set(0)
	metrics.DataCoordQuotaCachePending.WithLabelValues("consumer").Set(0)
}

func (s *SegmentsInfo) discardQuotaPendingWithoutConsumer() {
	s.quota.mu.Lock()
	defer s.quota.mu.Unlock()
	if !Params.QuotaConfig.LocalQuotaCalculationEnabled.GetAsBool() {
		s.quota.pending = make(map[int64]*metricsinfo.CollectionQuotaSnapshot)
		s.quota.baseline++
		s.quota.consumerNeedsFull = true
		metrics.DataCoordQuotaCachePending.WithLabelValues("consumer").Set(0)
	}
}
