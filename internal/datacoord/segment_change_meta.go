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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file implements the meta-layer of SegmentChangeGroup, the atomic
// publication unit of batch segment changes. The in-memory table lives on
// `meta` and is guarded by segMu: the group record composes into the same
// catalog txn as segment writes (see UpdateSegmentsInfoAndChangeGroups), so it
// shares the segment lock. This is the layer that later batch pipelines
// (import, compaction, copy, ...) call to stage and publish their outputs, and
// that the DataView integration (PR #52537) extends by composing a DataView
// snapshot action into the same txn.

// WrapErrSegmentChangeGroupNotFound reports a missing segment change group.
func WrapErrSegmentChangeGroupNotFound(groupID int64) error {
	return merr.WrapErrServiceInternalMsg("segment change group %d not found", groupID)
}

// loadSegmentChangeGroups loads every persisted group and builds the staged-
// member and superseded-parent reverse indexes. Only ALIVE groups
// (STAGED/READY) contribute to the indexes; terminal groups do not (their
// members are visible or reclaimed, their superseded parents are retired or
// released). A persisted conflict — one segment referenced by two alive groups
// — is a data-integrity violation and is reported as an error so recovery can
// fail closed instead of silently picking a winner.
func (m *meta) loadSegmentChangeGroups(ctx context.Context) (map[int64]*model.SegmentChangeGroup, map[int64]int64, map[int64]int64, error) {
	groups, err := m.catalog.ListSegmentChangeGroups(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	byID := make(map[int64]*model.SegmentChangeGroup, len(groups))
	staged := make(map[int64]int64)
	superseded := make(map[int64]int64)
	for _, group := range groups {
		if group == nil || group.GroupID == 0 {
			// C14: a record that decodes to a zero-valued group (e.g. persisted
			// "null" or "{}") is a data-integrity violation, not a skip: silently
			// dropping it orphans its staged members (IsInvisible=true with no
			// owner, never published or reclaimed). This matches the kv-catalog
			// contract that a malformed record fails the walk.
			return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
				"persisted segment change group with missing or invalid group ID")
		}
		// C9: a duplicate groupID (across collections) is a data-integrity
		// violation, not a "keep the first" case — keep-first can resurrect a
		// stale ALIVE group whose collectionID no longer matches.
		if _, dup := byID[group.GroupID]; dup {
			return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
				"duplicate segment change group %d during recovery", group.GroupID)
		}
		byID[group.GroupID] = group
		if group.State != model.SegmentChangeStateStaged && group.State != model.SegmentChangeStateReady {
			continue
		}
		for _, id := range group.NewSegmentIDs {
			// C10: bidirectional, mirroring registration — a member must not be
			// another group's member OR another group's superseded parent.
			if owner, ok := staged[id]; ok {
				return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
					"segment %d is staged by multiple alive segment change groups during recovery (group %d)", id, owner)
			}
			if owner, ok := superseded[id]; ok {
				return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
					"segment %d is staged by one group and superseded by group %d during recovery", id, owner)
			}
			staged[id] = group.GroupID
		}
		for _, id := range group.SupersededSegmentIDs {
			// C11: the L0-exemption is taken from the PERSISTED decision, not
			// live SegmentMeta, so GC of an L0 parent cannot flip identical
			// bytes into a startup conflict.
			if containsInt64(group.SupersededL0SegmentIDs, id) {
				continue
			}
			if owner, ok := superseded[id]; ok {
				return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
					"segment %d is superseded by multiple alive segment change groups during recovery (group %d)", id, owner)
			}
			if owner, ok := staged[id]; ok {
				return nil, nil, nil, merr.WrapErrDataIntegrityMsg(
					"segment %d is staged by group %d and superseded by another group during recovery", id, owner)
			}
			superseded[id] = group.GroupID
		}
	}
	return byID, staged, superseded, nil
}

// AddSegmentChangeGroup creates a new group (STAGED by convention) and registers
// its members as staged. It rejects duplicate group IDs and members already
// staged by another group (the at-most-one-STAGED/READY-group-per-segment
// invariant). The record is written standalone; callers that create the staged
// members in the same txn should use UpdateSegmentsInfoAndChangeGroups instead.
func (m *meta) AddSegmentChangeGroup(ctx context.Context, group *model.SegmentChangeGroup) error {
	if group == nil {
		return merr.WrapErrServiceInternalMsg("cannot add a nil segment change group")
	}
	if err := group.Validate(); err != nil {
		return err
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	return m.addSegmentChangeGroupLocked(ctx, group)
}

func (m *meta) addSegmentChangeGroupLocked(ctx context.Context, group *model.SegmentChangeGroup) error {
	m.ensureSegmentChangeGroupStoreLocked()
	if existing := m.segmentChangeGroups[group.GroupID]; existing != nil {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d already exists in collection %d", group.GroupID, group.CollectionID)
	}
	if err := m.validateSegmentChangeGroupOwnershipLocked(group); err != nil {
		return err
	}
	// Persist the L0-exemption decision so recovery does not re-derive it from
	// live SegmentMeta (C11).
	m.stampSupersededL0ExemptionsLocked(group)
	if err := m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(group)); err != nil {
		return err
	}
	m.segmentChangeGroups[group.GroupID] = group.Clone()
	if group.State == model.SegmentChangeStateStaged || group.State == model.SegmentChangeStateReady {
		for _, id := range group.NewSegmentIDs {
			m.stagedSegmentToGroup[id] = group.GroupID
		}
		for _, id := range group.SupersededSegmentIDs {
			// L0-exempt superseded parents (persisted decision) are never
			// indexed.
			if containsInt64(group.SupersededL0SegmentIDs, id) {
				continue
			}
			m.supersededSegmentToGroup[id] = group.GroupID
		}
	}
	return nil
}

func containsInt64(list []int64, id int64) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

// validateSegmentChangeGroupOwnershipLocked enforces the anti-duplication
// invariant (§1.1 of the design) for a group being REGISTERED: an L1/L2
// segment may be referenced — as a new_segment or a superseded parent — by at
// most one ALIVE group per collection. Two alive groups replacing the same
// parent would, on double publish, expose two visible outputs covering the
// same logical rows. L0 is the explicit exception (monotonic delta
// application, absorbed by the DataView delta watermark), and groups never
// stage L0. The caller holds segMu; it must be invoked for every group
// creation entry point — both the standalone AddSegmentChangeGroup and the
// composite UpdateSegmentsInfoAndChangeGroups — so there is no defensive gap.
func (m *meta) validateSegmentChangeGroupOwnershipLocked(group *model.SegmentChangeGroup) error {
	return m.validateSegmentChangeGroupOwnershipAgainstLocked(group, m.stagedSegmentToGroup, m.supersededSegmentToGroup)
}

// validateSegmentChangeGroupOwnershipAgainstLocked is the parameterized form of
// validateSegmentChangeGroupOwnershipLocked: it validates against the supplied
// claim maps instead of the in-memory indexes, so a composite write can also
// see claims staged by sibling group actions within the SAME txn (review N1) —
// the in-memory indexes are only updated after the catalog write, so without
// this a single txn carrying two conflicting new groups would both pass and
// break the invariant at the persistence layer. The caller holds segMu.
func (m *meta) validateSegmentChangeGroupOwnershipAgainstLocked(group *model.SegmentChangeGroup, staged, superseded map[int64]int64) error {
	for _, id := range group.NewSegmentIDs {
		if owner, ok := staged[id]; ok {
			return merr.WrapErrDataIntegrityMsg(
				"segment %d is already staged by segment change group %d", id, owner)
		}
		if owner, ok := superseded[id]; ok {
			return merr.WrapErrDataIntegrityMsg(
				"segment %d is superseded by segment change group %d, cannot also be a new member", id, owner)
		}
	}
	for _, id := range group.SupersededSegmentIDs {
		// L0 exemption (C11): a superseded parent is exempt if it is L0 now or
		// another alive group already persisted it as L0-exempt. Segments whose
		// level is unknown (not in meta) are treated conservatively as
		// non-exempt unless previously recorded.
		if m.isSupersededExemptLocked(id) {
			continue
		}
		if owner, ok := superseded[id]; ok {
			return merr.WrapErrDataIntegrityMsg(
				"segment %d is already superseded by segment change group %d (duplicate data replacement)", id, owner)
		}
		if owner, ok := staged[id]; ok {
			return merr.WrapErrDataIntegrityMsg(
				"segment %d is a staged member of segment change group %d, cannot be superseded", id, owner)
		}
	}
	return nil
}

// validateSegmentChangeGroupTransitionLocked validates a state transition of an
// ALREADY-REGISTERED group: the transition must be legal AND the member set
// (NewSegmentIDs / SupersededSegmentIDs) must be unchanged — members are fixed
// at registration, so a transition that smuggles in new references would
// bypass the anti-duplication check. The caller holds segMu.
func (m *meta) validateSegmentChangeGroupTransitionLocked(group *model.SegmentChangeGroup) error {
	current := m.segmentChangeGroups[group.GroupID]
	if current == nil {
		return WrapErrSegmentChangeGroupNotFound(group.GroupID)
	}
	// C9: the caller-supplied collectionID must match the record, or a
	// transition would write a second key under another collection prefix and
	// strand the original.
	if group.CollectionID != current.CollectionID {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d collection mismatch during transition %s -> %s: current=%d supplied=%d",
			group.GroupID, current.State, group.State, current.CollectionID, group.CollectionID)
	}
	if !current.CanTransitionTo(group.State) {
		return merr.WrapErrDataIntegrityMsg(
			"illegal segment change group %d transition %s -> %s",
			group.GroupID, current.State, group.State)
	}
	// C17: only the caller-owned member sets are compared. SupersededL0SegmentIDs
	// is meta-computed at registration (stampSupersededL0ExemptionsLocked) and a
	// caller rebuilding the group from its own job/plan state cannot reproduce
	// it; comparing it would block every transition. The stored decision is
	// preserved onto the saved record instead.
	if !segmentIDSetEqual(current.NewSegmentIDs, group.NewSegmentIDs) ||
		!segmentIDSetEqual(current.SupersededSegmentIDs, group.SupersededSegmentIDs) {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d member set changed during transition %s -> %s; members are fixed at registration",
			group.GroupID, current.State, group.State)
	}
	return nil
}

// segmentIDSetEqual reports whether two segment ID lists contain the same set
// (order-insensitive, duplicates not expected after Validate).
func segmentIDSetEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[int64]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := set[id]; !ok {
			return false
		}
	}
	return true
}

// UpdateSegmentChangeGroup persists a group state change (STAGED→READY,
// STAGED/READY→FAILED/ABORTED, READY→COMMITTED, or an idempotent replay of the
// same state). The transition is validated against the current in-memory state.
//
// The READY→COMMITTED publish is normally performed atomically together with
// member flips, superseded retirement, and (later) a DataView snapshot via
// UpdateSegmentsInfoAndChangeGroups; this standalone form exists for terminal
// transitions that do not touch segments.
func (m *meta) UpdateSegmentChangeGroup(ctx context.Context, group *model.SegmentChangeGroup) error {
	if group == nil {
		return merr.WrapErrServiceInternalMsg("cannot update a nil segment change group")
	}
	if err := group.Validate(); err != nil {
		return err
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.ensureSegmentChangeGroupStoreLocked()
	if err := m.validateSegmentChangeGroupTransitionLocked(group); err != nil {
		return err
	}
	// C17: the L0-exemption decision is meta-owned (stamped at registration);
	// a caller-rebuilt group does not carry it, so preserve the stored value on
	// the saved record instead of trusting the caller.
	group.SupersededL0SegmentIDs = append([]int64(nil), m.segmentChangeGroups[group.GroupID].SupersededL0SegmentIDs...)
	action := metastore.SaveSegmentChangeGroup(group)
	if err := m.catalog.Update(ctx, action); err != nil {
		return err
	}
	m.applySegmentChangeGroupActionMemoryLocked(action)
	return nil
}

// GetSegmentChangeGroup returns a clone of the group, or nil when unknown.
func (m *meta) GetSegmentChangeGroup(ctx context.Context, collectionID, groupID int64) *model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	group := m.segmentChangeGroups[groupID]
	if group == nil || (collectionID > 0 && group.CollectionID != collectionID) {
		return nil
	}
	return group.Clone()
}

// GetSegmentChangeGroupsByCollection returns clones of all groups of a
// collection, in unspecified order. collectionID <= 0 returns every group.
func (m *meta) GetSegmentChangeGroupsByCollection(ctx context.Context, collectionID int64) []*model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	groups := make([]*model.SegmentChangeGroup, 0, len(m.segmentChangeGroups))
	for _, group := range m.segmentChangeGroups {
		if collectionID > 0 && group.CollectionID != collectionID {
			continue
		}
		groups = append(groups, group.Clone())
	}
	return groups
}

// GetSegmentChangeGroupBySegmentID returns the group that currently stages
// segmentID (STAGED/READY), or nil when the segment is not staged.
func (m *meta) GetSegmentChangeGroupBySegmentID(ctx context.Context, segmentID int64) *model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	groupID, ok := m.stagedSegmentToGroup[segmentID]
	if !ok {
		return nil
	}
	group := m.segmentChangeGroups[groupID]
	if group == nil {
		return nil
	}
	return group.Clone()
}

// HasStagedSegment reports whether segmentID is currently a STAGED/READY member
// of a group in collectionID.
func (m *meta) HasStagedSegment(ctx context.Context, collectionID, segmentID int64) bool {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	groupID, ok := m.stagedSegmentToGroup[segmentID]
	if !ok {
		return false
	}
	group := m.segmentChangeGroups[groupID]
	return group != nil && (collectionID <= 0 || group.CollectionID == collectionID)
}

// GetSegmentChangeGroupByReferencedSegment returns the ALIVE group that
// references segmentID — as a staged member OR as a superseded parent — or nil
// when the segment is claimed by no alive group. This is the "is this segment
// part of an in-flight change" query that planners (R8 exclusion of staged
// members) and DDL handlers (truncate/drop-partition triggering group
// convergence) use; unlike GetSegmentChangeGroupBySegmentID it also covers
// superseded-parent claims.
func (m *meta) GetSegmentChangeGroupByReferencedSegment(ctx context.Context, segmentID int64) *model.SegmentChangeGroup {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	groupID, ok := m.stagedSegmentToGroup[segmentID]
	if !ok {
		groupID, ok = m.supersededSegmentToGroup[segmentID]
	}
	if !ok {
		return nil
	}
	group := m.segmentChangeGroups[groupID]
	if group == nil {
		return nil
	}
	return group.Clone()
}

// HasReferencedSegment reports whether segmentID is claimed by an ALIVE group
// in collectionID, as a staged member or a superseded parent.
func (m *meta) HasReferencedSegment(ctx context.Context, collectionID, segmentID int64) bool {
	m.segMu.RLock()
	defer m.segMu.RUnlock()
	groupID, ok := m.stagedSegmentToGroup[segmentID]
	if !ok {
		groupID, ok = m.supersededSegmentToGroup[segmentID]
	}
	if !ok {
		return false
	}
	group := m.segmentChangeGroups[groupID]
	return group != nil && (collectionID <= 0 || group.CollectionID == collectionID)
}

// DeleteSegmentChangeGroup removes a group record. Only TERMINAL groups
// (COMMITTED/FAILED/ABORTED) may be deleted: deleting a STAGED/READY group
// would leave its members `IsInvisible=true` with no owner, so they could
// never be published or reclaimed. A missing group is an idempotent no-op.
// Callers must ensure the group's superseded segments are fully retired before
// deleting it.
func (m *meta) DeleteSegmentChangeGroup(ctx context.Context, collectionID, groupID int64) error {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.ensureSegmentChangeGroupStoreLocked()
	current := m.segmentChangeGroups[groupID]
	if current == nil {
		// Idempotent cleanup of a missing group.
		return nil
	}
	if !current.IsTerminal() {
		return merr.WrapErrDataIntegrityMsg(
			"cannot delete non-terminal segment change group %d (state %s); abort or fail it first",
			groupID, current.State)
	}
	// C9: the delete key is derived from the RECORD's collectionID, not the
	// caller's parameter, so a mismatched collectionID cannot strand the etcd
	// key while the in-memory record is dropped.
	recordCollectionID := current.CollectionID
	if collectionID > 0 && collectionID != recordCollectionID {
		return merr.WrapErrDataIntegrityMsg(
			"segment change group %d collection mismatch on delete: record=%d supplied=%d",
			groupID, recordCollectionID, collectionID)
	}
	action := metastore.DeleteSegmentChangeGroup(recordCollectionID, groupID)
	if err := m.catalog.Update(ctx, action); err != nil {
		return err
	}
	m.applySegmentChangeGroupActionMemoryLocked(action)
	return nil
}

// DropSegmentChangeGroupsOfCollection removes every group of one collection.
// Used by collection drop; per-collection prefix delete runs under the caller's
// collection lifecycle lock.
func (m *meta) DropSegmentChangeGroupsOfCollection(ctx context.Context, collectionID int64) error {
	m.segMu.Lock()
	defer m.segMu.Unlock()
	if err := m.catalog.DropSegmentChangeGroups(ctx, collectionID); err != nil {
		return err
	}
	for groupID, group := range m.segmentChangeGroups {
		if group.CollectionID != collectionID {
			continue
		}
		for _, id := range group.NewSegmentIDs {
			if m.stagedSegmentToGroup[id] == groupID {
				delete(m.stagedSegmentToGroup, id)
			}
		}
		for _, id := range group.SupersededSegmentIDs {
			if m.supersededSegmentToGroup[id] == groupID {
				delete(m.supersededSegmentToGroup, id)
			}
		}
		delete(m.segmentChangeGroups, groupID)
	}
	return nil
}

// UpdateSegmentsInfoAndChangeGroups applies segment operators and composes the
// given group actions into the SAME catalog txn, then applies the memory
// effects of both. This is the composite write of the batch publication
// protocol:
//
//   - STAGED creation: create the staged members (IsInvisible=true,
//     change_group_id) and SaveSegmentChangeGroup(STAGED) atomically;
//   - READY→COMMITTED publish: flip members visible, retire superseded,
//     SaveSegmentChangeGroup(COMMITTED), and (once PR #52537 lands) append a
//     DataView snapshot action — all in one txn.
//
// groupActions must only contain metastore.SaveSegmentChangeGroup /
// DeleteSegmentChangeGroup actions; any other entry type is rejected. A stale
// segment update (errIgnoredSegmentMetaOperation) skips the segment part but
// still persists the group actions, keeping an idempotent replay convergent.
func (m *meta) UpdateSegmentsInfoAndChangeGroups(ctx context.Context, groupActions []metastore.UpdateAction, operators ...UpdateOperator) error {
	for _, action := range groupActions {
		if _, ok := action.Entry.(metastore.SegmentChangeGroupEntry); !ok {
			return merr.WrapErrServiceInternalMsg(
				"UpdateSegmentsInfoAndChangeGroups only accepts segment change group actions, got %T", action.Entry)
		}
	}
	m.segMu.Lock()
	defer m.segMu.Unlock()
	m.ensureSegmentChangeGroupStoreLocked()

	// Semantic validation of every group action, under segMu, before any write.
	// A Save of an UNREGISTERED group is a creation and must pass the same
	// anti-duplication checks as AddSegmentChangeGroup (F1: no defensive gap on
	// the composite creation path). A Save of a REGISTERED group is a state
	// transition and must be legal with an unchanged member set (M2). A Delete
	// must target a terminal group (M3).
	//
	// The checks run against a claim set seeded from the in-memory indexes AND
	// folded with each sibling group's claims, so two conflicting new groups in
	// ONE txn are rejected too (review N1): the in-memory indexes are only
	// updated after the catalog write, so validating against them alone would
	// let both pass and break the invariant at the persistence layer.
	claimedStaged := make(map[int64]int64, len(m.stagedSegmentToGroup))
	for id, owner := range m.stagedSegmentToGroup {
		claimedStaged[id] = owner
	}
	claimedSuperseded := make(map[int64]int64, len(m.supersededSegmentToGroup))
	for id, owner := range m.supersededSegmentToGroup {
		claimedSuperseded[id] = owner
	}
	// C18: m.segmentChangeGroups is only updated after the catalog write, so a
	// SECOND action for the same groupID in one call would be validated as if
	// the first did not exist — with differing collectionIDs/members both would
	// persist under two keys and brick recovery on duplicate group IDs. Track
	// groupIDs seen within this call and reject duplicates, mirroring
	// addSegmentChangeGroupLocked and loadSegmentChangeGroups.
	seenGroupIDs := make(map[int64]struct{})
	for _, action := range groupActions {
		entry := action.Entry.(metastore.SegmentChangeGroupEntry)
		switch action.Type {
		case metastore.ActionUpdate:
			if entry.Group == nil {
				return merr.WrapErrServiceInternalMsg("segment change group action carries a nil group")
			}
			if err := entry.Group.Validate(); err != nil {
				return err
			}
			if _, dup := seenGroupIDs[entry.Group.GroupID]; dup {
				return merr.WrapErrDataIntegrityMsg(
					"duplicate segment change group %d in one composite write", entry.Group.GroupID)
			}
			seenGroupIDs[entry.Group.GroupID] = struct{}{}
			if m.segmentChangeGroups[entry.Group.GroupID] == nil {
				if err := m.validateSegmentChangeGroupOwnershipAgainstLocked(entry.Group, claimedStaged, claimedSuperseded); err != nil {
					return err
				}
				// Persist the L0-exemption decision (C11) and fold this group's
				// claims so a sibling action in the same txn sees them.
				m.stampSupersededL0ExemptionsLocked(entry.Group)
				for _, id := range entry.Group.NewSegmentIDs {
					claimedStaged[id] = entry.Group.GroupID
				}
				for _, id := range entry.Group.SupersededSegmentIDs {
					if containsInt64(entry.Group.SupersededL0SegmentIDs, id) {
						continue
					}
					claimedSuperseded[id] = entry.Group.GroupID
				}
			} else if err := m.validateSegmentChangeGroupTransitionLocked(entry.Group); err != nil {
				return err
			} else {
				// C17: preserve the meta-owned L0-exemption decision on a
				// caller-rebuilt transition record.
				entry.Group.SupersededL0SegmentIDs = append([]int64(nil), m.segmentChangeGroups[entry.Group.GroupID].SupersededL0SegmentIDs...)
			}
		case metastore.ActionDelete:
			if current := m.segmentChangeGroups[entry.GroupID]; current != nil && !current.IsTerminal() {
				return merr.WrapErrDataIntegrityMsg(
					"cannot delete non-terminal segment change group %d (state %s) in a composite write; abort or fail it first",
					entry.GroupID, current.State)
			}
		default:
			return merr.WrapErrServiceInternalMsg(
				"unsupported segment change group action type %v", action.Type)
		}
	}

	updatePack := &updateSegmentPack{
		meta:       m,
		segments:   make(map[int64]*SegmentInfo),
		increments: make(map[int64]metastore.BinlogsIncrement),
		metricMutation: &segMetricMutation{
			stateChange:             make(segmentMetricStateChange),
			deferSegmentLabelChange: true,
		},
	}
	for _, operator := range operators {
		operator(updatePack)
		if updatePack.err != nil {
			return updatePack.err
		}
	}
	if err := commitL0ManifestUpdates(updatePack.l0ManifestUpdates); err != nil {
		return err
	}
	for _, update := range updatePack.l0ManifestUpdates {
		if !update.apply(updatePack) {
			return updatePack.err
		}
	}

	if len(updatePack.segments) == 0 && len(groupActions) == 0 {
		return nil
	}

	// C15: `updateSegmentPack.Validate` panics when fromSaveBinlogPathSegmentID
	// is set but the target segment is absent from the pack (a save-binlog-paths
	// target concurrently dropped/GC'd). UpdateSegmentsInfo never reaches that
	// path because its empty-pack guard short-circuits first; here groupActions
	// can keep the write alive, so neutralize the stale marker before
	// validation — a dropped target is a benign stale update, not a panic.
	if updatePack.fromSaveBinlogPathSegmentID != 0 {
		if _, ok := updatePack.segments[updatePack.fromSaveBinlogPathSegmentID]; !ok {
			updatePack.fromSaveBinlogPathSegmentID = 0
		}
	}

	// C3: for a COMMITTED (publish) transition, every member must have been
	// touched by an operator (i.e. still present in meta and flipped). The
	// operator return value alone is ambiguous — UpdateStatusOperator returns
	// false both for an absent segment and for an already-reached target state
	// (the idempotent superseded-already-Dropped skip) — so verify member
	// presence explicitly against the pack instead. An externally dropped
	// member would otherwise let the publish write only
	// SaveSegmentChangeGroup(COMMITTED): the group reaches a terminal state
	// while its superseded parents stay unretired. The design's failure matrix
	// requires the caller to mark the group FAILED here.
	for _, action := range groupActions {
		entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
		if !ok || action.Type != metastore.ActionUpdate || entry.Group == nil ||
			entry.Group.State != model.SegmentChangeStateCommitted {
			continue
		}
		for _, id := range entry.Group.NewSegmentIDs {
			if _, ok := updatePack.segments[id]; !ok {
				return merr.WrapErrDataIntegrityMsg(
					"segment change group %d publish: member %d is absent from meta (externally dropped?)",
					entry.Group.GroupID, id)
			}
		}
	}

	if err := updatePack.Validate(); err != nil {
		// C16: errIgnoredSegmentMetaOperation must not fail the composite write
		// nor loop a retrying RPC. UpdateSegmentsInfo maps it to nil (a stale
		// save-binlog-paths update is a benign no-op); here the group actions
		// must still be persisted so the group record converges, so drop only
		// the stale segment from the pack and continue.
		if errors.Is(err, errIgnoredSegmentMetaOperation) {
			mlog.Info(ctx, "meta update: ignored stale segment meta operation, dropping it and persisting the rest",
				mlog.Err(err))
			delete(updatePack.segments, updatePack.fromSaveBinlogPathSegmentID)
			updatePack.fromSaveBinlogPathSegmentID = 0
			if len(updatePack.segments) == 0 && len(groupActions) == 0 {
				return nil
			}
		} else {
			return err
		}
	}
	updatePack.prepareSegmentMetricUpdates()

	// C2: deterministic action order. updatePack.segments is a Go map, so a
	// direct iteration interleaves member flips and superseded retirements
	// arbitrarily. Above MaxTxnOps, txn.Commit falls back to an ORDERED chunked
	// flush, and a crash mid-flush with parents retired before replacements are
	// visible would silently drop rows from queries. Pin members-first,
	// superseded-second (the same order the existing compaction path enforces:
	// compactTo published before compactFrom retired) so any fallback
	// intermediate state is transient duplication, never data loss.
	memberIDs := make(map[int64]struct{})
	supersededIDs := make(map[int64]struct{})
	for _, action := range groupActions {
		entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
		if !ok || action.Type != metastore.ActionUpdate || entry.Group == nil {
			continue
		}
		for _, id := range entry.Group.NewSegmentIDs {
			memberIDs[id] = struct{}{}
		}
		for _, id := range entry.Group.SupersededSegmentIDs {
			supersededIDs[id] = struct{}{}
		}
	}
	orderedSegments := make([]*SegmentInfo, 0, len(updatePack.segments))
	appendByClass := func(filter func(id int64) bool) {
		for _, segment := range updatePack.segments {
			if filter(segment.GetID()) {
				orderedSegments = append(orderedSegments, segment)
			}
		}
	}
	appendByClass(func(id int64) bool {
		_, ok := memberIDs[id]
		return ok
	})
	appendByClass(func(id int64) bool {
		_, ok := supersededIDs[id]
		return ok
	})
	appendByClass(func(id int64) bool {
		_, member := memberIDs[id]
		_, superseded := supersededIDs[id]
		return !member && !superseded
	})

	actions := make([]metastore.UpdateAction, 0, len(updatePack.segments)+len(groupActions))
	// C13: ALIVE group actions (STAGED creation / READY transition) come FIRST,
	// before the member actions. In the chunked-fallback flush the group record
	// must land before its staged members, so a crash can leave at worst an
	// orphan group (recoverable as FAILED), never invisible members with no
	// owner.
	for _, action := range groupActions {
		entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
		if !ok || action.Type != metastore.ActionUpdate || entry.Group == nil || entry.Group.IsTerminal() {
			continue
		}
		actions = append(actions, action)
	}
	for _, segment := range orderedSegments {
		var binlogs []metastore.BinlogsIncrement
		if inc, ok := updatePack.increments[segment.GetID()]; ok {
			binlogs = []metastore.BinlogsIncrement{inc}
		}
		actions = append(actions, metastore.UpdateAction{
			Type:  metastore.ActionUpdate,
			Entry: metastore.SegmentEntry{Segment: segment.SegmentInfo, Binlogs: binlogs, AlterEncoding: true},
		})
	}
	// TERMINAL group actions (COMMITTED publish / FAILED / ABORTED) come LAST:
	// the kv dispatch commit-marks them, so the fallback flush lands them after
	// every non-commit op as the visibility marker.
	for _, action := range groupActions {
		entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
		if !ok || action.Type != metastore.ActionUpdate || entry.Group == nil || !entry.Group.IsTerminal() {
			continue
		}
		actions = append(actions, action)
	}
	// Deletes are commit-marked removals; append them last for a deterministic
	// recorded order.
	for _, action := range groupActions {
		if _, ok := action.Entry.(metastore.SegmentChangeGroupEntry); !ok || action.Type != metastore.ActionDelete {
			continue
		}
		actions = append(actions, action)
	}

	if err := m.catalog.Update(ctx, actions...); err != nil {
		mlog.Error(ctx, "meta update: update segments info and segment change groups failed",
			mlog.Int("segments", len(updatePack.segments)),
			mlog.Int("groupActions", len(groupActions)),
			mlog.Err(err))
		return err
	}
	// Apply metric mutation and memory status after a successful meta update.
	updatePack.metricMutation.commit()
	for id, s := range updatePack.segments {
		m.segments.SetSegment(id, s)
	}
	for _, action := range groupActions {
		m.applySegmentChangeGroupActionMemoryLocked(action)
	}
	return nil
}

// applySegmentChangeGroupActionMemoryLocked folds one group action into the
// in-memory table and the staged-member reverse index. The caller holds segMu.
func (m *meta) applySegmentChangeGroupActionMemoryLocked(action metastore.UpdateAction) {
	entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry)
	if !ok {
		return
	}
	m.ensureSegmentChangeGroupStoreLocked()
	switch action.Type {
	case metastore.ActionUpdate:
		if entry.Group == nil {
			return
		}
		m.dropGroupReferencesLocked(entry.Group.GroupID)
		m.segmentChangeGroups[entry.Group.GroupID] = entry.Group.Clone()
		if entry.Group.State == model.SegmentChangeStateStaged || entry.Group.State == model.SegmentChangeStateReady {
			for _, id := range entry.Group.NewSegmentIDs {
				m.stagedSegmentToGroup[id] = entry.Group.GroupID
			}
			for _, id := range entry.Group.SupersededSegmentIDs {
				// C11: skip L0-exempt superseded parents using the PERSISTED
				// decision, not a live segment-level lookup (the segment may
				// already be GC'd).
				if containsInt64(entry.Group.SupersededL0SegmentIDs, id) {
					continue
				}
				m.supersededSegmentToGroup[id] = entry.Group.GroupID
			}
		}
	case metastore.ActionDelete:
		m.dropGroupReferencesLocked(entry.GroupID)
		delete(m.segmentChangeGroups, entry.GroupID)
	}
}

// dropGroupReferencesLocked removes every referenced segment (staged members
// and superseded parents) of a group from the reverse indexes. The caller
// holds segMu.
func (m *meta) dropGroupReferencesLocked(groupID int64) {
	group := m.segmentChangeGroups[groupID]
	if group == nil {
		return
	}
	for _, id := range group.NewSegmentIDs {
		if m.stagedSegmentToGroup[id] == groupID {
			delete(m.stagedSegmentToGroup, id)
		}
	}
	for _, id := range group.SupersededSegmentIDs {
		if m.supersededSegmentToGroup[id] == groupID {
			delete(m.supersededSegmentToGroup, id)
		}
	}
}

// isL0SegmentLocked reports whether segmentID exists in meta and is an L0 delta
// segment. The caller holds segMu (reads the shared SegmentsInfo map, no further
// locking needed).
func (m *meta) isL0SegmentLocked(segmentID int64) bool {
	segment := m.segments.GetSegment(segmentID)
	return segment != nil && segment.GetLevel() == datapb.SegmentLevel_L0
}

// isSupersededExemptLocked reports whether a superseded parent is exempt from
// the anti-duplication invariant: it is either an L0 delta segment in live meta
// NOW, or another ALIVE group already persisted it as L0-exempt (so the
// decision is monotonic once any owner recorded it, independent of later GC of
// the segment). The caller holds segMu.
func (m *meta) isSupersededExemptLocked(segmentID int64) bool {
	if m.isL0SegmentLocked(segmentID) {
		return true
	}
	for _, group := range m.segmentChangeGroups {
		if group.State != model.SegmentChangeStateStaged && group.State != model.SegmentChangeStateReady {
			continue
		}
		for _, exemptID := range group.SupersededL0SegmentIDs {
			if exemptID == segmentID {
				return true
			}
		}
	}
	return false
}

// stampSupersededL0ExemptionsLocked computes and records which superseded
// parents are exempt from the anti-duplication invariant, persisting the
// decision on the group (C11): recovery must not re-derive the exemption from
// live SegmentMeta, or a GC'd L0 parent would flip identical persisted bytes
// into a startup conflict. The caller holds segMu; group is mutated in place.
func (m *meta) stampSupersededL0ExemptionsLocked(group *model.SegmentChangeGroup) {
	group.SupersededL0SegmentIDs = group.SupersededL0SegmentIDs[:0]
	for _, id := range group.SupersededSegmentIDs {
		if m.isSupersededExemptLocked(id) {
			group.SupersededL0SegmentIDs = append(group.SupersededL0SegmentIDs, id)
		}
	}
}

// ensureSegmentChangeGroupStoreLocked lazily initializes the group table for a
// `meta` constructed directly (unit tests) rather than via newMeta. The caller
// holds segMu.
func (m *meta) ensureSegmentChangeGroupStoreLocked() {
	if m.segmentChangeGroups == nil {
		m.segmentChangeGroups = make(map[int64]*model.SegmentChangeGroup)
	}
	if m.stagedSegmentToGroup == nil {
		m.stagedSegmentToGroup = make(map[int64]int64)
	}
	if m.supersededSegmentToGroup == nil {
		m.supersededSegmentToGroup = make(map[int64]int64)
	}
}
