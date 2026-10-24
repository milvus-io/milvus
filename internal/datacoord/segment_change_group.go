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
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file implements the group lifecycle layer of segment change staging:
// the READY -> COMMITTED atomic publication (publishGroup). It is dual-mode —
// the same code publishes on a dataview-enabled DataCoord (members visible +
// superseded retired + group COMMITTED + a new DataView snapshot in ONE catalog
// txn) and on a dataview-less DataCoord (the SegmentMeta-only composite write;
// the legacy view then reads the committed members directly). The state machine
// (STAGED->READY evaluation, FAILED/ABORTED transitions, recovery replay) lives
// in the inspector / reconciler; publishGroup never transitions the group on
// failure, it only reports it.

// errSegmentChangeGroupCollectionDropped reports a publish aborted because the
// collection's DataViews were dropped in the window (PublishChange returns nil):
// there is no consumer to publish into, so the caller should ABORT the group and
// reclaim its members.
var errSegmentChangeGroupCollectionDropped = errors.New("segment change group publish: collection data views dropped")

// groupUsesPublishCommitTS reports whether the group's source allocates a
// publish commit timestamp (design §5.4, C5): import / CDC / copy rows carry a
// data timestamp allocated at creation and become "officially present" at
// commit_ts. Compaction outputs (mix/sort/clustering/forcemerge) MUST keep
// CommitTimestamp=0 — their rows keep per-input normalized timestamps, and a
// non-zero commit_ts makes segcore treat every row as inserted at commit_ts,
// dropping any delete with a timestamp <= commit_ts (MVCC/TTL shift).
func groupUsesPublishCommitTS(source model.SegmentChangeSource) bool {
	switch source {
	case model.SegmentChangeSourceImportJob,
		model.SegmentChangeSourceCopySegment,
		model.SegmentChangeSourceCDCReplicated:
		return true
	default:
		return false
	}
}

// publishGroup atomically publishes a READY group (design §5.4): one catalog
// txn commits the members visible, retires the superseded parents (Dropped),
// advances the group to COMMITTED, and — on dataview-enabled DataCoords —
// publishes a new DataView snapshot (compact_version +1) with the members in
// and the parents out.
//
// Lock order: segmentManifestLocks -> DataView collection lock -> segMu. The
// segMu.RLock validation in phase 2 is brief and released before the DataView
// lock is acquired, so no path holds segMu while waiting on the DataView lock.
// Once the manifest locks are held, the DataView-lock holders (flush /
// Recompute) hold the collection lock only briefly and never request a manifest
// lock, so the publish wait is bounded (design §5.4.2).
//
// The write is idempotent for the READY-replay path after recovery: a replayed
// publish whose members are already visible and whose parents already retired
// converges (member flips and parent retirements are no-ops, and PublishChange
// returns the current snapshot without burning a version).
//
// The caller (groupInspector / reconciler) owns the state machine: on failure
// publishGroup leaves the group READY and returns an error so the inspector can
// re-evaluate and retry, or FAIL an unconvergeable group (member externally
// dropped), or ABORT on errSegmentChangeGroupCollectionDropped.
func (m *meta) publishGroup(ctx context.Context, g *model.SegmentChangeGroup, alloc allocator.Allocator, extraOperators ...UpdateOperator) error {
	if g == nil {
		return merr.WrapErrServiceInternalMsg("cannot publish a nil segment change group")
	}
	if g.State != model.SegmentChangeStateReady {
		return merr.WrapErrServiceInternalMsg(
			"segment change group %d cannot be published in state %s (requires READY)", g.GroupID, g.State)
	}

	// Phase -1: allocate commit_ts before holding any lock, so the rootcoord TSO
	// RTT never sits on a lock. TSO only increases; a failure may retry with a
	// fresh value.
	commitTs, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return err
	}

	// Phase 0: acquire the members' manifest locks as one atomic operation, so
	// the ManifestPath versions read below cannot be advanced by a concurrent
	// L0 materialization (design R6 / §5.4.2). Superseded parents are only
	// marked Dropped and need no lock.
	segmentIDs := sortedUniqueIDs(g.NewSegmentIDs)
	locks := m.getSegmentManifestLocks()
	if err := acquireSegmentManifestLocks(ctx, locks, segmentIDs); err != nil {
		return err
	}
	defer locks.UnlockMany(segmentIDs)

	// R5: re-validate readiness at publish time (design §5.4.1 / §5.5). The
	// inspector may have marked the group READY up to a tick ago; in the window
	// a member's index state can regress (rebuild retry / index DDL) or a
	// schema bump can land, and publishing would expose an indexed-out or
	// schema-behind member while its superseded parents are retired — data
	// temporarily vanishing from queries. A failed re-check returns an error
	// and the caller regresses the group to STAGED for re-evaluation.
	coll := m.GetCollection(g.CollectionID)
	readiness := m.prepareReadiness(coll)

	// Phase 2: read and validate the members under segMu.RLock and build the
	// loadable projection. The read lock is released before the DataView lock is
	// acquired, keeping the acquisition order acyclic. Ownership is enforced via
	// the stagedSegmentToGroup reverse index (F4: the change_group_id field is
	// not yet on SegmentInfo).
	m.segMu.RLock()
	loadable := make([]dataview.LoadableSegment, 0, len(g.NewSegmentIDs))
	for _, id := range g.NewSegmentIDs {
		seg := m.segments.GetSegment(id)
		if seg == nil {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d is missing from meta (externally dropped?)", g.GroupID, id)
		}
		if seg.GetState() == commonpb.SegmentState_Dropped {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d was externally dropped", g.GroupID, id)
		}
		if owner, ok := m.stagedSegmentToGroup[id]; !ok || owner != g.GroupID {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d is not staged by this group", g.GroupID, id)
		}
		if len(seg.GetBinlogs()) == 0 && seg.GetManifestPath() == "" {
			m.segMu.RUnlock()
			return merr.WrapErrDataIntegrityMsg(
				"segment change group %d publish: member %d has no data footprint", g.GroupID, id)
		}
		if readiness != nil {
			if !readiness.schemaReady(seg) {
				m.segMu.RUnlock()
				return merr.WrapErrDataIntegrityMsg(
					"segment change group %d publish: member %d schema version behind collection at publish (C23)", g.GroupID, id)
			}
			if !readiness.indexReady(seg) {
				m.segMu.RUnlock()
				return merr.WrapErrDataIntegrityMsg(
					"segment change group %d publish: member %d index state regressed after READY (R5)", g.GroupID, id)
			}
		}
		l, err := loadableSegmentOf(seg)
		if err != nil {
			m.segMu.RUnlock()
			return err
		}
		loadable = append(loadable, l)
	}
	m.segMu.RUnlock()

	// Phase 3a: build the post-publish DataView snapshot (acquires the DataView
	// collection lock; a nil view with no-op callbacks means the collection's
	// DataViews were dropped in the window).
	var (
		view       *viewpb.DataViewOfCollection
		commitView func()
		abortView  func()
	)
	if m.dataViewManager != nil {
		view, commitView, abortView, err = m.dataViewManager.PublishChange(ctx, dataview.ChangeGroupDataViewEvent{
			CollectionID:         g.CollectionID,
			NewSegments:          loadable,
			SupersededSegmentIDs: g.SupersededSegmentIDs,
		})
		if err != nil {
			// Monotonicity conflict: the manifest version was pushed higher by a
			// concurrent L0 and re-read lower. Back to the caller; the inspector
			// re-reads and retries.
			return err
		}
		if view == nil {
			// No consumer to publish into; the caller marks the group ABORTED
			// and reclaims the members.
			return errSegmentChangeGroupCollectionDropped
		}
	}

	// Phase 4: one catalog txn (SegmentMeta operators + group action + DataView
	// snapshot). The composite write owns ordering (members before superseded,
	// terminal group + DataView commit-marked last) and the C3 checks (every
	// member flipped visible, every superseded retired).
	operators := make([]UpdateOperator, 0, len(g.NewSegmentIDs)*2+len(g.SupersededSegmentIDs)+len(extraOperators))
	for _, id := range g.NewSegmentIDs {
		operators = append(operators, SetSegmentIsInvisible(id, false))
		if groupUsesPublishCommitTS(g.Source) {
			operators = append(operators, UpdateCommitTimestamp(id, commitTs))
		}
	}
	for _, id := range g.SupersededSegmentIDs {
		operators = append(operators, UpdateStatusOperator(id, commonpb.SegmentState_Dropped))
	}
	operators = append(operators, extraOperators...)

	committed := g.Clone()
	committed.State = model.SegmentChangeStateCommitted
	committed.CommitTS = commitTs
	committed.CommitTime = time.Now().Unix()
	groupActions := []metastore.UpdateAction{metastore.SaveSegmentChangeGroup(committed)}

	if err := m.UpdateSegmentsInfoAndChangeGroupsAndDataView(ctx, view, groupActions, operators...); err != nil {
		if abortView != nil {
			abortView()
		}
		return err
	}
	if commitView != nil {
		commitView()
	}
	mlog.Info(ctx, "segment change group published",
		mlog.Int64("groupID", g.GroupID),
		mlog.Int64("collectionID", g.CollectionID),
		mlog.String("source", g.Source.String()),
		mlog.Int64s("members", g.NewSegmentIDs),
		mlog.Uint64("commitTS", commitTs))
	return nil
}

// loadableSegmentOf converts a segment into the DataView loadable projection,
// parsing the latest Manifest version from its ManifestPath (0 for non-V3
// segments). Manifest path parsing is pure string I/O and runs outside segMu;
// the caller guarantees the manifest lock is held for the member so the version
// cannot be advanced concurrently.
func loadableSegmentOf(seg *SegmentInfo) (dataview.LoadableSegment, error) {
	manifestVersion := int64(0)
	if seg.GetManifestPath() != "" {
		_, mv, err := packed.UnmarshalManifestPath(seg.GetManifestPath())
		if err != nil {
			return dataview.LoadableSegment{}, merr.WrapErrStorage(err, "failed to parse Manifest path for Segment %d", seg.GetID())
		}
		manifestVersion = mv
	}
	return dataview.LoadableSegment{
		SegmentID:       seg.GetID(),
		VChannel:        seg.GetInsertChannel(),
		PartitionID:     seg.GetPartitionID(),
		ManifestVersion: manifestVersion,
		RowNum:          seg.GetNumOfRows(),
		Size:            seg.GetStats().GetInsertBinlogSize(),
	}, nil
}

// sortedUniqueIDs returns ids sorted ascending and deduplicated, the shape
// acquireSegmentManifestLocks requires.
func sortedUniqueIDs(ids []int64) []int64 {
	if len(ids) <= 1 {
		return ids
	}
	set := make(map[int64]struct{}, len(ids))
	unique := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, ok := set[id]; ok {
			continue
		}
		set[id] = struct{}{}
		unique = append(unique, id)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	return unique
}
