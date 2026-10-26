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

package metacache

import (
	"slices"
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// segmentTable holds the segments of the store. A point read takes no lock.
// The segments of one collection form a shard: every write holds the lock of
// each shard it changes, and a read of several segments copies them under the
// read lock of their shard, so it observes a write of the collection whole or
// not at all. Writes of different collections never wait for each other, and a
// read of all segments holds one shard at a time. Writes of one segment must
// not run concurrently; DataCoord serializes them with segment writer locks.
type segmentTable struct {
	segments *typeutil.ConcurrentMap[int64, *datapb.SegmentInfo]
	shards   *typeutil.ConcurrentMap[int64, *segmentShard]
	// channels maps an insert channel to the collections with segments on it.
	// channelsMu is taken under shard locks, never the other way around.
	channelsMu sync.RWMutex
	channels   map[string]map[int64]struct{}
}

type segmentShard struct {
	mu sync.RWMutex
	// retired is set when the shard lost its last segment and left the table;
	// a writer that finds it retired takes the collection's new shard.
	retired   bool
	ids       map[int64]struct{}
	byChannel map[string]map[int64]struct{}
}

func newSegmentTable() *segmentTable {
	return &segmentTable{
		segments: typeutil.NewConcurrentMap[int64, *datapb.SegmentInfo](),
		shards:   typeutil.NewConcurrentMap[int64, *segmentShard](),
		channels: make(map[string]map[int64]struct{}),
	}
}

func (t *segmentTable) get(segmentID int64) (*datapb.SegmentInfo, bool) {
	return t.segments.Get(segmentID)
}

// lockShard locks the live shard of collectionID for writing, creating it.
func (t *segmentTable) lockShard(collectionID int64) *segmentShard {
	for {
		shard, _ := t.shards.GetOrInsert(collectionID, &segmentShard{
			ids:       make(map[int64]struct{}),
			byChannel: make(map[string]map[int64]struct{}),
		})
		shard.mu.Lock()
		if !shard.retired {
			return shard
		}
		shard.mu.Unlock()
	}
}

// rlockShard read-locks the live shard of collectionID, or returns nil if the
// collection has no segment.
func (t *segmentTable) rlockShard(collectionID int64) *segmentShard {
	for {
		shard, ok := t.shards.Get(collectionID)
		if !ok {
			return nil
		}
		shard.mu.RLock()
		if !shard.retired {
			return shard
		}
		shard.mu.RUnlock()
	}
}

// apply installs puts and removes removes as one write: a reader of several
// segments sees all of it or none of it. Point readers see the segments it
// keeps or adds before the ones it retires (Dropped) or removes, so a
// compaction's outputs exist before its inputs leave. hook, if not nil, runs
// while the write holds the shards. withMetrics keeps DataCoordNumSegments in
// step with each change.
func (t *segmentTable) apply(puts []*datapb.SegmentInfo, removes []int64, hook func(), withMetrics bool) {
	shards := t.lockWritten(puts, removes)

	for _, retired := range []bool{false, true} {
		for _, segment := range puts {
			if (segment.GetState() == commonpb.SegmentState_Dropped) == retired {
				t.put(shards, segment, withMetrics)
			}
		}
	}
	for _, segmentID := range removes {
		if segment, ok := t.segments.Get(segmentID); ok {
			if shard := shards[segment.GetCollectionID()]; shard != nil {
				t.remove(shard, segment, withMetrics)
			}
		}
	}
	if hook != nil {
		hook()
	}

	for collectionID, shard := range shards {
		if len(shard.ids) == 0 {
			// No writer can add a shard for the collection while this one is in
			// the table, so the key still holds it.
			shard.retired = true
			t.shards.Remove(collectionID)
		}
		shard.mu.Unlock()
	}
}

// lockWritten locks, in collection order, the shards of the collections the
// segments of a write belong to now and will belong to after it.
func (t *segmentTable) lockWritten(puts []*datapb.SegmentInfo, removes []int64) map[int64]*segmentShard {
	collectionsOf := func() []int64 {
		collections := make([]int64, 0, 1)
		for _, segment := range puts {
			collections = append(collections, segment.GetCollectionID())
			if old, ok := t.segments.Get(segment.GetID()); ok {
				collections = append(collections, old.GetCollectionID())
			}
		}
		for _, segmentID := range removes {
			if old, ok := t.segments.Get(segmentID); ok {
				collections = append(collections, old.GetCollectionID())
			}
		}
		slices.Sort(collections)
		return slices.Compact(collections)
	}
	for {
		collections := collectionsOf()
		shards := make(map[int64]*segmentShard, len(collections))
		for _, collectionID := range collections {
			shards[collectionID] = t.lockShard(collectionID)
		}
		// A segment can only change collection under the locks of both.
		if slices.Equal(collections, collectionsOf()) {
			return shards
		}
		for _, shard := range shards {
			shard.mu.Unlock()
		}
	}
}

func (t *segmentTable) put(shards map[int64]*segmentShard, segment *datapb.SegmentInfo, withMetrics bool) {
	segmentID := segment.GetID()
	shard := shards[segment.GetCollectionID()]
	old, exists := t.segments.Get(segmentID)
	if exists {
		if withMetrics {
			segmentMetricTransition(old, segment)
		}
		if old.GetCollectionID() != segment.GetCollectionID() {
			if oldShard := shards[old.GetCollectionID()]; oldShard != nil {
				delete(oldShard.ids, segmentID)
				t.unindexChannel(oldShard, old.GetCollectionID(), old.GetInsertChannel(), segmentID)
			}
		} else if old.GetInsertChannel() != segment.GetInsertChannel() {
			t.unindexChannel(shard, segment.GetCollectionID(), old.GetInsertChannel(), segmentID)
		}
	} else if withMetrics {
		labels := segMetricLabels(segment)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Inc()
	}
	t.segments.Insert(segmentID, segment)
	shard.ids[segmentID] = struct{}{}
	channel := segment.GetInsertChannel()
	ids, ok := shard.byChannel[channel]
	if !ok {
		ids = make(map[int64]struct{})
		shard.byChannel[channel] = ids
		t.channelsMu.Lock()
		collections, ok := t.channels[channel]
		if !ok {
			collections = make(map[int64]struct{})
			t.channels[channel] = collections
		}
		collections[segment.GetCollectionID()] = struct{}{}
		t.channelsMu.Unlock()
	}
	ids[segmentID] = struct{}{}
}

func (t *segmentTable) remove(shard *segmentShard, segment *datapb.SegmentInfo, withMetrics bool) {
	segmentID := segment.GetID()
	t.segments.Remove(segmentID)
	delete(shard.ids, segmentID)
	t.unindexChannel(shard, segment.GetCollectionID(), segment.GetInsertChannel(), segmentID)
	if withMetrics {
		labels := segMetricLabels(segment)
		metrics.DataCoordNumSegments.WithLabelValues(labels[:]...).Dec()
	}
}

func (t *segmentTable) unindexChannel(shard *segmentShard, collectionID int64, channel string, segmentID int64) {
	ids, ok := shard.byChannel[channel]
	if !ok {
		return
	}
	delete(ids, segmentID)
	if len(ids) == 0 {
		delete(shard.byChannel, channel)
		t.channelsMu.Lock()
		delete(t.channels[channel], collectionID)
		if len(t.channels[channel]) == 0 {
			delete(t.channels, channel)
		}
		t.channelsMu.Unlock()
	}
}

// view runs read while no write of collectionID is in flight. read must only
// use point reads.
func (t *segmentTable) view(collectionID int64, read func()) {
	if shard := t.rlockShard(collectionID); shard != nil {
		defer shard.mu.RUnlock()
	}
	read()
}

func (t *segmentTable) copyLocked(ids map[int64]struct{}, into map[int64]*datapb.SegmentInfo) {
	for id := range ids {
		if segment, ok := t.segments.Get(id); ok {
			into[id] = segment
		}
	}
}

func (t *segmentTable) collection(collectionID int64) map[int64]*datapb.SegmentInfo {
	shard := t.rlockShard(collectionID)
	if shard == nil {
		return nil
	}
	defer shard.mu.RUnlock()
	result := make(map[int64]*datapb.SegmentInfo, len(shard.ids))
	t.copyLocked(shard.ids, result)
	return result
}

// channel reads the segments of a channel, those of each collection together.
func (t *segmentTable) channel(channel string) map[int64]*datapb.SegmentInfo {
	t.channelsMu.RLock()
	collections := make([]int64, 0, len(t.channels[channel]))
	for collectionID := range t.channels[channel] {
		collections = append(collections, collectionID)
	}
	t.channelsMu.RUnlock()
	if len(collections) == 0 {
		return nil
	}
	result := make(map[int64]*datapb.SegmentInfo)
	for _, collectionID := range collections {
		if shard := t.rlockShard(collectionID); shard != nil {
			t.copyLocked(shard.byChannel[channel], result)
			shard.mu.RUnlock()
		}
	}
	return result
}

// byIDs reads the segments of segmentIDs at one point in time: it read-locks,
// in collection order as writers lock them, the shards of every collection
// the segments belong to, and returns the segments found while it holds them
// all. A segment absent then is absent at that point.
func (t *segmentTable) byIDs(segmentIDs []int64) map[int64]*datapb.SegmentInfo {
	shards := make(map[int64]*segmentShard)
	defer func() {
		for _, shard := range shards {
			shard.mu.RUnlock()
		}
	}()
	for {
		result := make(map[int64]*datapb.SegmentInfo, len(segmentIDs))
		var unlocked []int64
		for _, id := range segmentIDs {
			segment, ok := t.segments.Get(id)
			if !ok {
				continue
			}
			if _, locked := shards[segment.GetCollectionID()]; !locked {
				// A collection without a shard has no write to wait for.
				if _, ok := t.shards.Get(segment.GetCollectionID()); ok {
					unlocked = append(unlocked, segment.GetCollectionID())
					continue
				}
			}
			result[id] = segment
		}
		if len(unlocked) == 0 {
			return result
		}
		collections := append(lo.Keys(shards), unlocked...)
		slices.Sort(collections)
		for collectionID, shard := range shards {
			shard.mu.RUnlock()
			delete(shards, collectionID)
		}
		for _, collectionID := range slices.Compact(collections) {
			if shard := t.rlockShard(collectionID); shard != nil {
				shards[collectionID] = shard
			}
		}
	}
}

// all reads every segment, the segments of each collection together: a write
// of one collection is seen whole or not at all.
func (t *segmentTable) all() map[int64]*datapb.SegmentInfo {
	result := make(map[int64]*datapb.SegmentInfo, t.segments.Len())
	for _, collectionID := range t.shards.Keys() {
		if shard := t.rlockShard(collectionID); shard != nil {
			t.copyLocked(shard.ids, result)
			shard.mu.RUnlock()
		}
	}
	return result
}

func (t *segmentTable) len() int {
	return t.segments.Len()
}

// rangeAll visits every segment without a lock, for metrics.
func (t *segmentTable) rangeAll(fn func(segment *datapb.SegmentInfo)) {
	t.segments.Range(func(_ int64, segment *datapb.SegmentInfo) bool {
		fn(segment)
		return true
	})
}
