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

package dataview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SplitAdoptionMove moves one segment of a split source to a target shard.
type SplitAdoptionMove struct {
	SegmentID int64
	// VChannel is the target shard the segment moves to.
	VChannel string
	// TransformStartAfterTimetick is the cursor of the segment on the stream
	// of its target. It must not be above the frontier of the target.
	TransformStartAfterTimetick uint64
}

// AdoptSplitDataViewEvent is the adoption of a shard split: every segment of
// the source moves to the target shard its label names, and the source shard
// is removed, in one DataVersion.
type AdoptSplitDataViewEvent struct {
	CollectionID int64
	// Source is the shard removed by the adoption.
	Source string
	// Moves must name every segment the source shard holds in the latest
	// DataView. A segment the source does not hold is accepted only if it is
	// already in the target the move names (a replay).
	Moves []SplitAdoptionMove
}

// PrepareAdoptSplit builds the DataView snapshot of the adoption of a split
// while holding the Collection lock. It is the manager side of the adoption
// txn, like PrepareFlush: the caller composes the returned snapshot into the
// same catalog.Update as the SegmentMeta changes that move the segments (see
// meta.UpdateSegmentsInfoAndDataView), then calls commit when the composite
// txn was persisted (loads the snapshot into memory and releases the lock),
// or abort when it failed (releases the lock without touching memory). All
// callbacks are idempotent.
//
// The adoption is the only event that moves a segment between shards. It
// keeps the Manifest version of each segment, sets its cursor to the one of
// its move, and advances compact_version once. The frontier of a target does
// not move: a target is pinned at the time tick the cursors of the moves are
// set to, and the minimum over its segments keeps it there until the L0
// compaction of the target advances them.
//
// A collection without the source shard has been adopted already: if no move
// names a segment outside its target, the current snapshot is returned with
// no-op callbacks and nothing is to be persisted (view is the current one,
// unchanged). A collection whose DataViews were dropped returns a nil view,
// like PrepareFlush.
func (m *dataViewManager) PrepareAdoptSplit(ctx context.Context, event AdoptSplitDataViewEvent) (*viewpb.DataViewOfCollection, func(), func(), error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		var once sync.Once
		noop := func() { once.Do(func() {}) }
		return nil, noop, noop, nil
	}
	fail := func(err error) (*viewpb.DataViewOfCollection, func(), func(), error) {
		unlock()
		return nil, nil, nil, err
	}

	base := latestView(state)
	if base == nil {
		return fail(merr.WrapErrServiceNotReadyMsg("collection %d DataView is not initialized", event.CollectionID))
	}
	if event.Source == "" {
		return fail(merr.WrapErrServiceInternalMsg("adoption of a split of collection %d names no source", event.CollectionID))
	}
	next := canonicalDataViewClone(base)
	shards := make(map[string]*viewpb.DataViewOfShard, len(next.GetShards()))
	for _, shard := range next.GetShards() {
		shards[shard.GetVchannel()] = shard
	}
	slots := dataViewSegmentSlots(next)
	source := shards[event.Source]

	moved := make(map[int64]struct{}, len(event.Moves))
	type pending struct {
		move            SplitAdoptionMove
		partitionID     int64
		manifestVersion int64
	}
	adds := make([]pending, 0, len(event.Moves))
	for _, move := range event.Moves {
		if move.SegmentID <= 0 || move.VChannel == "" || move.VChannel == event.Source || move.TransformStartAfterTimetick == 0 {
			return fail(merr.WrapErrServiceInternalMsg("invalid adoption move of collection %d: segment=%d target=%q cursor=%d",
				event.CollectionID, move.SegmentID, move.VChannel, move.TransformStartAfterTimetick))
		}
		if _, dup := moved[move.SegmentID]; dup {
			return fail(merr.WrapErrServiceInternalMsg("segment %d is moved twice by the adoption of %s", move.SegmentID, event.Source))
		}
		moved[move.SegmentID] = struct{}{}
		if shards[move.VChannel] == nil {
			return fail(merr.WrapErrServiceNotReadyMsg("target %s of the adoption of %s is not a shard of collection %d",
				move.VChannel, event.Source, event.CollectionID))
		}
		slot, known := slots[move.SegmentID]
		switch {
		case known && slot.location.vchannel == move.VChannel:
			// Already in its target: a replay of a move that was persisted.
			continue
		case known && slot.location.vchannel == event.Source:
			adds = append(adds, pending{
				move:            move,
				partitionID:     slot.location.partitionID,
				manifestVersion: slot.partition.GetSegmentManifestVersions()[slot.index],
			})
		case known:
			return fail(merr.WrapErrDataIntegrityMsg("segment %d to adopt from %s is in shard %s",
				move.SegmentID, event.Source, slot.location.vchannel))
		default:
			return fail(merr.WrapErrDataIntegrityMsg("segment %d to adopt from %s is not in the DataView of collection %d",
				move.SegmentID, event.Source, event.CollectionID))
		}
	}
	if source != nil {
		for _, partition := range source.GetPartitions() {
			for _, segmentID := range partition.GetSegmentIds() {
				if _, ok := moved[segmentID]; !ok {
					return fail(merr.WrapErrServiceNotReadyMsg("segment %d of %s is not moved by its adoption", segmentID, event.Source))
				}
			}
		}
	}
	if source == nil && len(adds) == 0 {
		var once sync.Once
		commit := func() { once.Do(unlock) }
		abort := func() { once.Do(unlock) }
		return base, commit, abort, nil
	}

	kept := next.Shards[:0]
	for _, shard := range next.GetShards() {
		if shard.GetVchannel() != event.Source {
			kept = append(kept, shard)
		}
	}
	next.Shards = kept
	for _, add := range adds {
		partition := findOrCreatePartition(shards[add.move.VChannel], add.partitionID)
		partition.SegmentIds = append(partition.SegmentIds, add.move.SegmentID)
		partition.SegmentManifestVersions = append(partition.SegmentManifestVersions, add.manifestVersion)
		partition.SegmentTransformStartAfterTimeticks = append(partition.SegmentTransformStartAfterTimeticks, add.move.TransformStartAfterTimetick)
	}
	if err := m.advanceTransformFrontiers(ctx, next, nil); err != nil {
		return fail(err)
	}
	// Membership changes exactly once with the atomic relocation. A replay
	// above returns the existing snapshot without advancing its generation.
	next.ShardSetEpoch = max(base.GetShardSetEpoch(), 1) + 1
	canonicalizeDataView(next)
	next.DataVersion = nextDataVersion(base, dataViewAdvanceCompact)

	// The segments are those of the base version, so are their statistics.
	stats := make(map[int64]SegmentStats, len(state.latest.stats))
	for id, value := range state.latest.stats {
		stats[id] = value
	}
	m.prepareLogicalSizes(stats)

	var once sync.Once
	commit := func() {
		once.Do(func() {
			defer unlock()
			if err := m.persistMemoryLockedWithStats(state, next, stats); err != nil {
				mlog.Warn(ctx, "failed to load the prepared adoption snapshot into DataView memory",
					mlog.FieldCollectionID(event.CollectionID), mlog.Err(err))
			}
		})
	}
	abort := func() { once.Do(unlock) }
	return next, commit, abort, nil
}
