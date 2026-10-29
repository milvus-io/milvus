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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// reconcileSegmentChangeGroups runs with no RPCs, task workers, GC, or DataView
// manager active. It must finish before RecoverManager projects SegmentMeta:
// an interrupted chunked publish can leave visible members and retired parents
// without its final group/DataView commit marker. Projecting that intermediate
// state would make a partial batch a new committed snapshot.
func (m *meta) reconcileSegmentChangeGroups(ctx context.Context, alloc allocator.Allocator, live dataview.CollectionRecoveryValidator) error {
	if m.dataViewManager != nil {
		return merr.WrapErrServiceInternalMsg("segment change recovery must precede DataView recovery")
	}
	groups := m.GetSegmentChangeGroupsByCollection(ctx, 0)
	if len(groups) > 0 && live == nil {
		return merr.WrapErrServiceInternalMsg("segment change recovery requires a collection recovery validator")
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })
	for _, g := range groups {
		keep, err := live(ctx, g.CollectionID)
		if err != nil {
			return err
		}
		if !keep {
			if err := m.DropSegmentChangeGroupsOfCollection(ctx, g.CollectionID); err != nil {
				return err
			}
			continue
		}
		switch g.State {
		case model.SegmentChangeStateCommitted:
			continue
		case model.SegmentChangeStateFailed, model.SegmentChangeStateAborted:
			if err := m.reclaimSegmentChangeGroup(ctx, g); err != nil {
				return err
			}
		default:
			if err := m.reconcileAliveSegmentChangeGroup(ctx, g, alloc); err != nil {
				return err
			}
		}
	}
	return nil
}

// RecoverManager logs projection/persistence failures and can return a manager
// with the old snapshot. Collections with group records require a synchronous
// successful rebuild before RPCs or task workers may observe recovered meta.
func (m *meta) reconcileSegmentChangeDataViews(ctx context.Context, manager dataview.Manager) error {
	collections := make(map[int64]struct{})
	for _, g := range m.GetSegmentChangeGroupsByCollection(ctx, 0) {
		collections[g.CollectionID] = struct{}{}
	}
	for collectionID := range collections {
		ref, err := manager.Latest(ctx, collectionID)
		if err != nil {
			return err
		}
		if ref == nil {
			// Retry RecoverManager's declared-channel bootstrap rather than
			// creating a projection-only snapshot that omits empty channels.
			return merr.WrapErrServiceUnavailableMsg("segment change recovery requires an initialized DataView for collection %d", collectionID)
		}
		ref.Deref()
		version, err := manager.RecomputeNow(ctx, collectionID, m.loadableProjection)
		if err != nil {
			return merr.Wrap(err, "rebuild DataView after segment change recovery")
		}
		if version == nil {
			return merr.WrapErrServiceUnavailableMsg("DataView disappeared during segment change recovery for collection %d", collectionID)
		}
	}
	return nil
}

func (m *meta) reconcileAliveSegmentChangeGroup(ctx context.Context, g *model.SegmentChangeGroup, alloc allocator.Allocator) error {
	visible, incomplete, allEmpty := false, false, true
	commitTS := g.CommitTS
	for _, id := range g.NewSegmentIDs {
		seg := m.GetSegment(ctx, id)
		if seg == nil {
			incomplete = true
			allEmpty = false
			continue
		}
		if seg.GetCollectionID() != g.CollectionID || seg.GetLevel() == datapb.SegmentLevel_L0 {
			return merr.WrapErrDataIntegrityMsg("segment change group %d has invalid member %d during recovery", g.GroupID, id)
		}
		if isEmptySegmentChangeOutput(seg) {
			continue
		}
		allEmpty = false
		if seg.GetState() == commonpb.SegmentState_Dropped {
			incomplete = true
			continue
		}
		if seg.GetIsInvisible() {
			continue
		}
		visible = true
		if !isFlushState(seg.GetState()) || seg.GetIsImporting() || (len(seg.GetBinlogs()) == 0 && seg.GetManifestPath() == "") {
			return merr.WrapErrDataIntegrityMsg("segment change group %d has invalid visible member %d during recovery", g.GroupID, id)
		}
		if groupWritesMemberCommitTS(g) {
			// Never allocate a new temporal boundary for already-published rows.
			// Every visible chunk must agree on the original publish timestamp.
			if seg.GetCommitTimestamp() == 0 || (commitTS != 0 && commitTS != seg.GetCommitTimestamp()) {
				return merr.WrapErrDataIntegrityMsg("segment change group %d has inconsistent publish timestamps during recovery", g.GroupID)
			}
			commitTS = seg.GetCommitTimestamp()
		}
	}
	if visible && g.AwaitCommitFence && g.CommitTS == 0 {
		return merr.WrapErrDataIntegrityMsg("import group %d became visible before its WAL commit fence", g.GroupID)
	}
	emptyPublishAllowed := !g.AwaitCommitFence || g.CommitTS != 0
	if g.PreviousGroupID != 0 {
		previous := m.GetSegmentChangeGroup(ctx, g.CollectionID, g.PreviousGroupID)
		emptyPublishAllowed = emptyPublishAllowed && previous != nil && previous.State == model.SegmentChangeStateCommitted
	}
	if visible || (allEmpty && g.State == model.SegmentChangeStateReady && emptyPublishAllowed) {
		if incomplete || g.State != model.SegmentChangeStateReady {
			return merr.WrapErrDataIntegrityMsg("segment change group %d has visible members without a recoverable READY publish", g.GroupID)
		}
		for _, id := range g.NewSegmentIDs {
			seg := m.GetSegment(ctx, id)
			if isEmptySegmentChangeOutput(seg) {
				continue
			}
			if !isFlushState(seg.GetState()) || seg.GetIsImporting() || (len(seg.GetBinlogs()) == 0 && seg.GetManifestPath() == "") {
				return merr.WrapErrDataIntegrityMsg("segment change group %d has incomplete member %d in interrupted publish", g.GroupID, id)
			}
			if _, err := loadableSegmentOf(seg); err != nil {
				return err
			}
		}
		if commitTS == 0 && !allEmpty {
			if alloc == nil {
				return merr.WrapErrServiceInternalMsg("segment change group %d recovery requires a timestamp allocator", g.GroupID)
			}
			var err error
			commitTS, err = alloc.AllocTimestamp(ctx)
			if err != nil {
				return err
			}
		}
		// No readiness re-evaluation here: this is completion of a persisted
		// READY intent, not a new publication. Incomplete/corrupt batches fail
		// startup above; they must never be reclaimed as though unpublished.
		return m.commitSegmentChangeGroup(ctx, g, commitTS, nil)
	}
	if incomplete {
		if groupHasCommittedImportFence(g) {
			return merr.WrapErrDataIntegrityMsg("WAL-committed import group %d has missing outputs during recovery", g.GroupID)
		}
		failed := g.Clone()
		failed.State = model.SegmentChangeStateFailed
		failed.FailReason = "member missing or dropped during recovery"
		if err := m.UpdateSegmentChangeGroup(ctx, failed); err != nil {
			return err
		}
		return m.reclaimSegmentChangeGroup(ctx, failed)
	}
	// Fully unpublished STAGED/READY groups are resumed by the inspector after
	// all async workers start. Waiting for readiness during startup could wait
	// on index/stats tasks that cannot run until startup completes.
	return nil
}

// reclaimSegmentChangeGroup validates the entire batch before dropping any
// member. A terminal record alone is never permission to delete visible rows.
// The operation is retried by the inspector and by startup recovery; already
// Dropped or GC'd members are harmless no-ops. Parents are never touched.
func (m *meta) reclaimSegmentChangeGroup(ctx context.Context, g *model.SegmentChangeGroup) error {
	return m.UpdateSegmentsInfoAndChangeGroups(ctx, nil, func(pack *updateSegmentPack) bool {
		current := m.segmentChangeGroups[g.GroupID]
		if current == nil || (current.State != model.SegmentChangeStateFailed && current.State != model.SegmentChangeStateAborted) {
			return pack.fail(merr.WrapErrDataIntegrityMsg("segment change group %d is not reclaimable", g.GroupID))
		}
		if groupHasCommittedImportFence(current) {
			return pack.fail(merr.WrapErrDataIntegrityMsg("refusing to reclaim WAL-committed import group %d", current.GroupID))
		}
		for _, id := range current.UnpublishedSegmentIDs() {
			seg := m.segments.GetSegment(id)
			if seg == nil || seg.GetState() == commonpb.SegmentState_Dropped {
				continue
			}
			if !seg.GetIsInvisible() || seg.GetCollectionID() != current.CollectionID {
				return pack.fail(merr.WrapErrDataIntegrityMsg("refusing to reclaim visible or foreign member %d of segment change group %d", id, g.GroupID))
			}
			if owner := m.stagedSegmentToGroup[id]; owner != 0 {
				return pack.fail(merr.WrapErrDataIntegrityMsg("member %d of terminal segment change group %d is owned by group %d", id, g.GroupID, owner))
			}
		}
		for _, id := range current.UnpublishedSegmentIDs() {
			if seg := m.segments.GetSegment(id); seg != nil && seg.GetState() != commonpb.SegmentState_Dropped {
				UpdateStatusOperator(id, commonpb.SegmentState_Dropped)(pack)
			}
		}
		return true
	})
}

// validateSegmentChangeVisibilityLocked checks the resulting records, not just
// SetSegmentIsInvisible calls: custom operators and manifest writes must obey
// the same ownership boundary. Only the owning COMMITTED composite may expose
// an ALIVE member. Caller holds segMu.
func (m *meta) validateSegmentChangeVisibilityLocked(segments map[int64]*SegmentInfo, actions []metastore.UpdateAction) error {
	for id, segment := range segments {
		owner := m.stagedSegmentToGroup[id]
		if owner == 0 && !segment.GetIsInvisible() {
			// Terminal groups release their ALIVE claims before cleanup. Only
			// scan them on a visibility transition, not on normal stats/index
			// writes to unrelated already-visible segments.
			if current := m.segments.GetSegment(id); current == nil || current.GetIsInvisible() {
				for _, g := range m.segmentChangeGroups {
					if (g.State == model.SegmentChangeStateFailed || g.State == model.SegmentChangeStateAborted) && containsInt64(g.UnpublishedSegmentIDs(), id) {
						return merr.WrapErrDataIntegrityMsg("member %d belongs to unpublished terminal segment change group %d", id, g.GroupID)
					}
				}
			}
		}
		if owner == 0 || segment.GetIsInvisible() || segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		allowed := false
		for _, action := range actions {
			entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
			if ok && action.Type == metastore.ActionUpdate && entry.Group != nil &&
				entry.Group.GroupID == owner && entry.Group.State == model.SegmentChangeStateCommitted {
				allowed = true
				break
			}
		}
		if !allowed {
			return merr.WrapErrDataIntegrityMsg("member %d may only become visible through segment change group %d publication", id, owner)
		}
	}
	return nil
}

// Keep the recovery anchor and read-side exclusion until failed outputs have
// actually been retired. A metadata-only delete must not orphan invisible rows.
func (m *meta) validateSegmentChangeGroupDeletionLocked(g *model.SegmentChangeGroup, updates map[int64]*SegmentInfo) error {
	if g == nil {
		return nil
	}
	if successors := m.segmentChangeSuccessors[g.GroupID]; successors != 0 {
		return merr.WrapErrDataIntegrityMsg("segment change group %d cannot be deleted while %d successor groups still reference it", g.GroupID, successors)
	}
	if g.State == model.SegmentChangeStateCommitted {
		return nil
	}
	for _, id := range g.UnpublishedSegmentIDs() {
		seg := updates[id]
		if seg == nil {
			seg = m.segments.GetSegment(id)
		}
		if seg != nil && seg.GetState() != commonpb.SegmentState_Dropped {
			return merr.WrapErrDataIntegrityMsg("segment change group %d cannot be deleted before member %d is reclaimed", g.GroupID, id)
		}
	}
	return nil
}
