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

// ChangeGroupDataViewEvent carries the batch-change publication request into
// PublishChange. It mirrors the design's view.proto ChangeGroupDataViewEvent
// (kept as a Go struct until the proto message lands): the ready group's new
// members (with their Manifest versions) enter the snapshot and its superseded
// parents leave it, all in one compact_version advance.
type ChangeGroupDataViewEvent struct {
	CollectionID int64
	// NewSegments are the ready group members published by this batch. Their
	// Manifest versions are resolved by the caller from the latest ManifestPath.
	NewSegments []LoadableSegment
	// SupersededSegmentIDs are the retired parents: they are removed from the
	// snapshot in the same version the members enter, so the snapshot never
	// lists a Dropped parent next to its replacement.
	SupersededSegmentIDs []int64
}

// PublishChange builds the post-batch-publish DataView snapshot while holding
// the Collection lock. It is the manager side of the batch atomic publish txn,
// symmetric with PrepareFlush: the caller composes the returned snapshot into
// the same catalog.Update as the SegmentMeta actions and the group record (see
// meta.UpdateSegmentsInfoAndChangeGroupsAndDataView), then calls commit when
// the composite txn was persisted (loads the snapshot into memory and releases
// the lock), or abort when the txn failed (releases the lock without touching
// memory). When nothing was persisted (the Collection's DataViews were
// dropped, PublishChange returns nil with no-op callbacks), the caller may skip
// both callbacks. All callbacks are idempotent.
//
// Unlike PrepareFlush (streaming_version +1, add-only), PublishChange advances
// only compact_version and applies membership both ways: NewSegments are added
// and SupersededSegmentIDs removed in the same snapshot. An idempotent replay
// of an already-published batch (e.g. the READY-replay probe after recovery)
// that leaves both the membership AND the Manifest versions unchanged returns
// the current snapshot without burning a version; a replay carrying a higher
// Manifest version is new information (addSegments advances the stored version
// monotonically) and therefore advances compact_version too. A publish that
// adds and removes nothing over an empty base produces an empty snapshot (no
// shards), which the caller must not compose into the catalog txn — same
// degenerate-publish contract as PrepareFlush.
func (m *dataViewManager) PublishChange(ctx context.Context, event ChangeGroupDataViewEvent) (*viewpb.DataViewOfCollection, func(), func(), error) {
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		// The Collection's DataViews were dropped while this publish was in
		// flight: publish nothing. The caller composes a nil snapshot, which
		// commits SegmentMeta and the group record alone.
		var once sync.Once
		noop := func() { once.Do(func() {}) }
		return nil, noop, noop, nil
	}

	base := latestView(state)
	next := canonicalDataViewClone(base)
	if next == nil {
		next = &viewpb.DataViewOfCollection{CollectionId: event.CollectionID}
	}
	if err := rejectMemberSupersededOverlap(event.NewSegments, event.SupersededSegmentIDs); err != nil {
		unlock()
		return nil, nil, nil, err
	}
	if err := addSegments(next, event.NewSegments); err != nil {
		unlock()
		return nil, nil, nil, err
	}
	if err := removeSegments(next, event.SupersededSegmentIDs); err != nil {
		unlock()
		return nil, nil, nil, err
	}
	canonicalizeDataView(next)
	if dataViewMembershipEqual(base, next) {
		// Idempotent replay of an already-published batch: no new snapshot to
		// persist, return the current one.
		view := base
		if view == nil {
			view = next
		}
		var once sync.Once
		commit := func() { once.Do(unlock) }
		abort := func() { once.Do(unlock) }
		return view, commit, abort, nil
	}
	next.DataVersion = nextDataVersion(base, dataViewAdvanceCompact)

	// The published snapshot is incremental: next carries the base version's
	// segments minus the superseded ones plus the new members, so its stats
	// index must merge the base entry's footprint, drop the retired parents'
	// stats, and add the new members' RowNum.
	stats := make(map[int64]SegmentStats)
	if baseEntry := state.latest; baseEntry != nil {
		for segmentID, rows := range baseEntry.stats {
			stats[segmentID] = rows
		}
	}
	for _, id := range event.SupersededSegmentIDs {
		delete(stats, id)
	}
	for segmentID, rows := range buildSegmentRowStats(event.NewSegments) {
		stats[segmentID] = rows
	}

	var once sync.Once
	commit := func() {
		once.Do(func() {
			defer unlock()
			if err := m.persistMemoryLockedWithStats(state, next, stats); err != nil {
				mlog.Warn(ctx, "failed to load prepared publish snapshot into DataView memory",
					mlog.Int64("collectionID", event.CollectionID),
					mlog.Err(err))
			}
		})
	}
	abort := func() {
		once.Do(unlock)
	}
	return next, commit, abort, nil
}

// rejectMemberSupersededOverlap rejects a publish event whose NewSegments and
// SupersededSegmentIDs intersect: a Segment in both lists would be added by
// addSegments and then silently deleted by removeSegments, making a published
// member vanish. The group model guarantees disjointness, so this is a
// defensive guard mirroring addSegments' conflicting-location check; a
// non-positive or duplicate ID is rejected by the per-list validations.
func rejectMemberSupersededOverlap(newSegments []LoadableSegment, supersededSegmentIDs []int64) error {
	if len(supersededSegmentIDs) == 0 {
		return nil
	}
	superseded := make(map[int64]struct{}, len(supersededSegmentIDs))
	for _, id := range supersededSegmentIDs {
		superseded[id] = struct{}{}
	}
	for _, segment := range newSegments {
		if _, ok := superseded[segment.SegmentID]; ok {
			return merr.WrapErrDataIntegrityMsg(
				"Segment %d is both a new member and a superseded parent in one publish", segment.SegmentID)
		}
	}
	return nil
}

// removeSegments removes the given Segment IDs from every shard/partition of
// the snapshot. A missing Segment ID is a no-op (the superseded parent may have
// been retired by an earlier replay); an invalid (non-positive) ID is rejected.
func removeSegments(view *viewpb.DataViewOfCollection, segmentIDs []int64) error {
	if len(segmentIDs) == 0 {
		return nil
	}
	remove := make(map[int64]struct{}, len(segmentIDs))
	for _, id := range segmentIDs {
		if id <= 0 {
			return merr.WrapErrServiceInternalMsg(
				"invalid Segment descriptor to remove from DataView: segment=%d", id)
		}
		remove[id] = struct{}{}
	}
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			segments := partition.GetSegmentIds()
			versions := partition.GetSegmentManifestVersions()
			keptSegments := segments[:0]
			keptVersions := versions[:0]
			for idx, segmentID := range segments {
				if _, ok := remove[segmentID]; ok {
					continue
				}
				keptSegments = append(keptSegments, segmentID)
				if idx < len(versions) {
					keptVersions = append(keptVersions, versions[idx])
				}
			}
			partition.SegmentIds = keptSegments
			partition.SegmentManifestVersions = keptVersions
		}
	}
	return nil
}
