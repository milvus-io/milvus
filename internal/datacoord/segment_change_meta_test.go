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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func newTestGroup() *model.SegmentChangeGroup {
	return &model.SegmentChangeGroup{
		GroupID:              1,
		Source:               model.SegmentChangeSourceMixCompaction,
		CollectionID:         10,
		PartitionID:          100,
		SourceJobID:          999,
		State:                model.SegmentChangeStateStaged,
		NewSegmentIDs:        []int64{1001},
		SupersededSegmentIDs: []int64{2001},
		CreateTS:             123,
	}
}

// deleteGroupCleanup aborts (if non-terminal) then deletes a group. Used by
// tests as cleanup; production must go through ABORT/FAIL before Delete (M3).
func deleteGroupCleanup(t *testing.T, m *meta, collectionID, groupID int64) {
	t.Helper()
	ctx := context.Background()
	group := m.GetSegmentChangeGroup(ctx, collectionID, groupID)
	if group == nil {
		return
	}
	if !group.IsTerminal() {
		aborted := group.Clone()
		aborted.State = model.SegmentChangeStateAborted
		require.NoError(t, m.UpdateSegmentChangeGroup(ctx, aborted))
	}
	require.NoError(t, m.DeleteSegmentChangeGroup(ctx, collectionID, groupID))
}

func TestMeta_AddGetSegmentChangeGroup(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.NotNil(t, got)
	require.Equal(t, group, got)

	require.True(t, m.HasStagedSegment(ctx, 10, 1001))
	require.False(t, m.HasStagedSegment(ctx, 10, 9999))
	require.False(t, m.HasStagedSegment(ctx, 11, 1001), "member staged in another collection")

	bySegment := m.GetSegmentChangeGroupBySegmentID(ctx, 1001)
	require.NotNil(t, bySegment)
	require.Equal(t, int64(1), bySegment.GroupID)
	require.Nil(t, m.GetSegmentChangeGroupBySegmentID(ctx, 9999))

	groups := m.GetSegmentChangeGroupsByCollection(ctx, 10)
	require.Len(t, groups, 1)
	require.Empty(t, m.GetSegmentChangeGroupsByCollection(ctx, 11))
}

func TestMeta_AddSegmentChangeGroup_RejectsConflicts(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// Duplicate group ID.
	dup := group.Clone()
	require.Error(t, m.AddSegmentChangeGroup(ctx, dup))

	// Same member staged by another group.
	other := group.Clone()
	other.GroupID = 2
	other.Source = model.SegmentChangeSourceImportJob
	require.Error(t, m.AddSegmentChangeGroup(ctx, other))

	// Invalid group rejected before any write.
	invalid := group.Clone()
	invalid.NewSegmentIDs = nil
	require.Error(t, m.AddSegmentChangeGroup(ctx, invalid))
}

func TestMeta_UpdateSegmentChangeGroup_Transitions(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// Illegal direct STAGED -> COMMITTED must be rejected.
	illegal := group.Clone()
	illegal.State = model.SegmentChangeStateCommitted
	require.Error(t, m.UpdateSegmentChangeGroup(ctx, illegal))

	// STAGED -> READY.
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	ready.ReadyTS = 456
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))
	require.True(t, m.HasStagedSegment(ctx, 10, 1001), "READY members are still staged")

	// READY -> COMMITTED (standalone; the composite publish is tested below).
	committed := ready.Clone()
	committed.State = model.SegmentChangeStateCommitted
	committed.CommitTS = 500
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, committed))
	require.False(t, m.HasStagedSegment(ctx, 10, 1001), "COMMITTED members leave the staged index")

	// Updating an unknown group fails.
	ghost := committed.Clone()
	ghost.GroupID = 42
	require.Error(t, m.UpdateSegmentChangeGroup(ctx, ghost))
}

func TestMeta_DeleteSegmentChangeGroup(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	// Non-terminal groups may not be deleted directly (M3).
	require.Error(t, m.DeleteSegmentChangeGroup(ctx, 10, 1))
	deleteGroupCleanup(t, m, 10, 1)
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1))
	require.False(t, m.HasStagedSegment(ctx, 10, 1001))
}

func TestMeta_DropSegmentChangeGroupsOfCollection(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	g1 := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))
	g2 := newTestGroup()
	g2.GroupID = 2
	g2.NewSegmentIDs = []int64{1002}
	g2.SupersededSegmentIDs = []int64{2002} // disjoint from g1's superseded (anti-duplication)
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g2))

	require.NoError(t, m.DropSegmentChangeGroupsOfCollection(ctx, 10))
	require.Empty(t, m.GetSegmentChangeGroupsByCollection(ctx, 10))
	require.False(t, m.HasStagedSegment(ctx, 10, 1001))
	require.False(t, m.HasStagedSegment(ctx, 10, 1002))
}

// TestMeta_UpdateSegmentsInfoAndChangeGroups_Publish verifies the composite
// write: member visibility flip + superseded retirement + group COMMITTED land
// in ONE catalog txn, are reflected in memory, and survive a reload from the
// catalog.
func TestMeta_UpdateSegmentsInfoAndChangeGroups_Publish(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	// Seeded meta: staged member 1001 (invisible), superseded 2001 (visible).
	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
		}),
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// Mark READY (the publish state machine requires READY -> COMMITTED), then
	// publish: flip member visible + commit_ts, drop superseded, COMMITTED.
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))
	committed := ready.Clone()
	committed.State = model.SegmentChangeStateCommitted
	committed.CommitTS = 500
	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(committed)},
		SetSegmentIsInvisible(1001, false),
		UpdateCommitTimestamp(1001, 500),
		UpdateStatusOperator(2001, commonpb.SegmentState_Dropped),
	)
	require.NoError(t, err)

	member := m.GetSegment(ctx, 1001)
	require.NotNil(t, member)
	require.False(t, member.GetIsInvisible())
	require.Equal(t, uint64(500), member.GetCommitTimestamp())
	superseded := m.GetSegment(ctx, 2001)
	require.NotNil(t, superseded)
	require.Equal(t, commonpb.SegmentState_Dropped, superseded.GetState())
	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(ctx, 10, 1).State)
	require.False(t, m.HasStagedSegment(ctx, 10, 1001))

	// Reload from catalog: group state and reverse indexes must round-trip.
	byID, stagedIndex, supersededIndex, err := m.loadSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateCommitted, byID[1].State)
	require.Empty(t, stagedIndex)
	require.Empty(t, supersededIndex, "COMMITTED group releases its superseded references")
}

func TestMeta_UpdateSegmentsInfoAndChangeGroups_RejectsForeignActions(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.AddSegment(&datapb.SegmentInfo{ID: 1})},
	)
	require.Error(t, err)
}

// TestMeta_GetSegmentChangeGroupByReferencedSegment verifies M4: the
// any-reference query covers both staged members and superseded parents.
func TestMeta_GetSegmentChangeGroupByReferencedSegment(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup() // new=[1001], superseded=[2001]
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// Staged member.
	require.NotNil(t, m.GetSegmentChangeGroupByReferencedSegment(ctx, 1001))
	require.True(t, m.HasReferencedSegment(ctx, 10, 1001))
	// Superseded parent — covered by the any-reference API, not by the
	// member-only API.
	require.NotNil(t, m.GetSegmentChangeGroupByReferencedSegment(ctx, 2001))
	require.True(t, m.HasReferencedSegment(ctx, 10, 2001))
	require.Nil(t, m.GetSegmentChangeGroupBySegmentID(ctx, 2001), "member-only query must not see superseded parents")
	require.False(t, m.HasStagedSegment(ctx, 10, 2001))

	require.Nil(t, m.GetSegmentChangeGroupByReferencedSegment(ctx, 9999))
	require.False(t, m.HasReferencedSegment(ctx, 10, 9999))
	require.False(t, m.HasReferencedSegment(ctx, 11, 2001), "claim is collection-scoped")
}

// composite write enforces the anti-duplication invariant on UNREGISTERED
// (creation) groups and the fixed-member-set invariant on REGISTERED
// (transition) groups — no defensive gap on the composite creation path.
func TestMeta_UpdateSegmentsInfoAndChangeGroups_OwnershipChecks(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	create := func(g *model.SegmentChangeGroup) error {
		return m.UpdateSegmentsInfoAndChangeGroups(ctx,
			[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(g)})
	}

	t.Run("composite creation with a conflicting superseded parent is rejected", func(t *testing.T) {
		g1 := newTestGroup() // new=[1001], superseded=[2001]
		require.NoError(t, create(g1))

		g2 := g1.Clone()
		g2.GroupID = 2
		g2.NewSegmentIDs = []int64{1002}
		// Same superseded parent as g1 — must be rejected even via the composite.
		require.Error(t, create(g2), "composite creation must not bypass anti-duplication")
		require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 2), "conflicting group must not be registered")
		deleteGroupCleanup(t, m, 10, 1)
	})

	t.Run("composite transition with a changed member set is rejected", func(t *testing.T) {
		g1 := newTestGroup()
		require.NoError(t, create(g1))
		ready := g1.Clone()
		ready.State = model.SegmentChangeStateReady
		require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))

		// READY with a smuggled extra member — must be rejected.
		forged := ready.Clone()
		forged.NewSegmentIDs = []int64{1001, 1002}
		require.Error(t, create(forged), "composite transition must not change the member set")
		deleteGroupCleanup(t, m, 10, 1)
	})

	t.Run("composite transition skipping READY is rejected", func(t *testing.T) {
		g1 := newTestGroup()
		require.NoError(t, create(g1))
		committed := g1.Clone()
		committed.State = model.SegmentChangeStateCommitted
		require.Error(t, create(committed), "STAGED -> COMMITTED is not a legal transition")
		deleteGroupCleanup(t, m, 10, 1)
	})
}

// TestMeta_UpdateSegmentsInfoAndChangeGroups_MissingMemberFails verifies C3: an
// operator whose target segment is absent from meta must fail the whole
// composite write (the group must NOT reach a terminal state while its members
// are gone and superseded parents unretired).
func TestMeta_UpdateSegmentsInfoAndChangeGroups_MissingMemberFails(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	// Member 1001 is intentionally NOT added to meta.
	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))

	committed := ready.Clone()
	committed.State = model.SegmentChangeStateCommitted
	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(committed)},
		SetSegmentIsInvisible(1001, false),
	)
	require.Error(t, err, "publish with an absent member must fail the txn, not write group COMMITTED")
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 1).State,
		"group must stay READY, not reach a terminal state")
}

// TestMeta_UpdateSegmentsInfoAndChangeGroups_SupersededAlreadyDropped verifies
// that a publish whose superseded parent is ALREADY Dropped still succeeds:
// UpdateStatusOperator returns false for the already-reached state (idempotent
// skip), which must not fail the whole txn (the C3 member-presence check is
// scoped to members, not superseded).
func TestMeta_UpdateSegmentsInfoAndChangeGroups_SupersededAlreadyDropped(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsInvisible: true,
	})))
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
	})))

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))

	// Superseded parent already dropped by an external path before publish.
	require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(2001, commonpb.SegmentState_Dropped)))

	committed := ready.Clone()
	committed.State = model.SegmentChangeStateCommitted
	committed.CommitTS = 500
	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(committed)},
		SetSegmentIsInvisible(1001, false),
		UpdateCommitTimestamp(1001, 500),
		UpdateStatusOperator(2001, commonpb.SegmentState_Dropped), // already Dropped -> idempotent skip
	)
	require.NoError(t, err, "already-dropped superseded parent must not fail the publish")
	require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(ctx, 10, 1).State)
}

// TestMeta_LoadSegmentChangeGroups_L0ExemptionPersistedStable verifies C11: the
// L0-exemption is decided at registration and PERSISTED, so recovery is stable
// even after the L0 parent is GC'd from SegmentMeta — identical persisted bytes
// must not flip from conflict-free into a startup error.
func TestMeta_LoadSegmentChangeGroups_L0ExemptionPersistedStable(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 4001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
	})))

	g1 := newTestGroup()
	g1.GroupID = 1
	g1.SupersededSegmentIDs = []int64{4001}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))
	g2 := g1.Clone()
	g2.GroupID = 2
	g2.NewSegmentIDs = []int64{1002}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g2))
	require.Equal(t, []int64{4001}, g1.SupersededL0SegmentIDs, "exemption must be persisted at registration")

	// Simulate GC of the L0 parent after the groups were persisted: identical
	// bytes must reload without a conflict.
	m.segMu.Lock()
	delete(m.segments.segments, 4001)
	m.segMu.Unlock()

	byID, staged, superseded, err := m.loadSegmentChangeGroups(ctx)
	require.NoError(t, err, "GC'd L0 parent must not flip persisted groups into a recovery conflict")
	require.Len(t, byID, 2)
	require.Equal(t, map[int64]int64{1001: 1, 1002: 2}, staged, "members stay staged")
	require.Empty(t, superseded, "L0-exempt superseded parents are never indexed")
}

// TestMeta_LoadSegmentChangeGroups_ZeroGroupFailsClosed verifies C14: a
// persisted record that decodes to a zero-valued group (e.g. "{}" or "null")
// must fail recovery, not be silently dropped — dropping it would orphan its
// staged members with no owner.
func TestMeta_LoadSegmentChangeGroups_ZeroGroupFailsClosed(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	g1 := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))

	// Forge a zero-valued group record, simulating a persisted "{}" that
	// unmarshals without error but carries no identity.
	require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(&model.SegmentChangeGroup{})))

	_, _, _, err = m.loadSegmentChangeGroups(ctx)
	require.Error(t, err, "a zero-valued persisted group must fail recovery, not be skipped")
}

// TestMeta_UpdateSegmentsInfoAndChangeGroups_SaveBinlogStale verifies C15/C16:
// a stale save-binlog-paths update composed with group actions neither panics
// (target absent from the pack) nor fails the write (errIgnoredSegmentMetaOperation);
// the group actions still persist so the group record converges.
func TestMeta_UpdateSegmentsInfoAndChangeGroups_SaveBinlogStale(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	t.Run("absent target does not panic (C15)", func(t *testing.T) {
		group := newTestGroup()
		require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

		// A save-binlog-paths operator whose target does not exist in meta at
		// all. Before C15 this reached updateSegmentPack.Validate's panic.
		require.NotPanics(t, func() {
			err := m.UpdateSegmentsInfoAndChangeGroups(ctx,
				[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(group.Clone())},
				UpdateBinlogsFromSaveBinlogPathsOperator(777777, nil, nil, nil, nil),
			)
			require.NoError(t, err)
		})
		require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 1), "group action must still persist")
	})

	t.Run("already-flushed target maps to no-op (C16)", func(t *testing.T) {
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		})))
		group := newTestGroup()
		group.GroupID = 2 // distinct from the first subtest's group 1
		group.NewSegmentIDs = []int64{1003}
		group.SupersededSegmentIDs = []int64{2002}
		require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

		// A stale save-binlog-paths on an already-flushed segment returns
		// errIgnoredSegmentMetaOperation; the composite must drop the stale
		// segment and still persist the group action, returning nil.
		ready := group.Clone()
		ready.State = model.SegmentChangeStateReady
		err := m.UpdateSegmentsInfoAndChangeGroups(ctx,
			[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(ready)},
			UpdateBinlogsFromSaveBinlogPathsOperator(1001, nil, nil, nil, nil),
		)
		require.NoError(t, err)
		require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 2).State)
	})
}

// TestMeta_UpdateSegmentChangeGroup_PreservesL0Exemption verifies C17: a caller
// that rebuilds the group from its own job/plan state (without the meta-stamped
// SupersededL0SegmentIDs) can still transition the group, and the stored
// exemption decision is preserved on the saved record.
func TestMeta_UpdateSegmentChangeGroup_PreservesL0Exemption(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 4001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
	})))

	group := newTestGroup()
	group.SupersededSegmentIDs = []int64{4001}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	require.Equal(t, []int64{4001}, m.GetSegmentChangeGroup(ctx, 10, 1).SupersededL0SegmentIDs,
		"exemption stamped at registration")

	// Caller-rebuilt READY record without the meta-owned field.
	ready := &model.SegmentChangeGroup{
		GroupID:              1,
		Source:               model.SegmentChangeSourceMixCompaction,
		CollectionID:         10,
		State:                model.SegmentChangeStateReady,
		NewSegmentIDs:        []int64{1001},
		SupersededSegmentIDs: []int64{4001},
	}
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready), "transition must not fail on the meta-owned field")
	got := m.GetSegmentChangeGroup(ctx, 10, 1)
	require.Equal(t, model.SegmentChangeStateReady, got.State)
	require.Equal(t, []int64{4001}, got.SupersededL0SegmentIDs, "stored decision must be preserved")
}

// TestMeta_UpdateSegmentsInfoAndChangeGroups_OneTxnConflictingCreates verifies
// N1: two conflicting new groups in the SAME composite txn are rejected —
// sibling group actions must see each other's claims, not only the in-memory
// indexes.
func TestMeta_UpdateSegmentsInfoAndChangeGroups_OneTxnConflictingCreates(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	g1 := newTestGroup() // new=[1001], superseded=[2001]
	g2 := g1.Clone()
	g2.GroupID = 2
	g2.NewSegmentIDs = []int64{1002}
	// g2 claims the same superseded parent as g1, in the SAME txn.
	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{
			metastore.SaveSegmentChangeGroup(g1),
			metastore.SaveSegmentChangeGroup(g2),
		},
	)
	require.Error(t, err, "sibling group actions in one txn must see each other's claims")
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1), "whole txn must abort on conflict")
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 2))
}

// TestMeta_UpdateSegmentChangeGroup_MemberSetFixed verifies M2 on the standalone
// path: a legal state transition may not change the member set.
func TestMeta_UpdateSegmentChangeGroup_MemberSetFixed(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	ready.SupersededSegmentIDs = []int64{9999} // smuggled superseded
	require.Error(t, m.UpdateSegmentChangeGroup(ctx, ready), "member set must be fixed at registration")
}

// TestMeta_DeleteSegmentChangeGroup_TerminalGuard verifies M3: only terminal
// groups may be deleted; deleting a missing group is an idempotent no-op.
func TestMeta_DeleteSegmentChangeGroup_TerminalGuard(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// Deleting a STAGED group is rejected.
	require.Error(t, m.DeleteSegmentChangeGroup(ctx, 10, 1))
	require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 1))

	// ABORT then delete succeeds.
	aborted := group.Clone()
	aborted.State = model.SegmentChangeStateAborted
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, aborted))
	require.NoError(t, m.DeleteSegmentChangeGroup(ctx, 10, 1))
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1))

	// Deleting a missing group is a no-op success.
	require.NoError(t, m.DeleteSegmentChangeGroup(ctx, 10, 1))
}

func TestMeta_UpdateSegmentsInfoAndChangeGroups_GroupOnlyWrite(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	group := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, group))

	// A group-only composite write (no segment operators) must still persist
	// the group action: this is the STAGED->READY path used before publish.
	ready := group.Clone()
	ready.State = model.SegmentChangeStateReady
	ready.ReadyTS = 456
	err = m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(ready)},
	)
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 1).State)
	require.True(t, m.HasStagedSegment(ctx, 10, 1001), "READY members are still staged")
}

// TestMeta_AddSegmentChangeGroup_AntiDuplication enforces the invariant that an
// L1/L2 segment may be referenced (member or superseded) by at most one ALIVE
// group per collection; otherwise a double publish would expose the same rows
// under two visible outputs.
func TestMeta_AddSegmentChangeGroup_AntiDuplication(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 3001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
		// L0 superseded parent, used for the L0 exemption below.
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 4001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	base := newTestGroup() // new=[1001], superseded=[2001]

	t.Run("two alive groups superseding the same L1 parent are rejected", func(t *testing.T) {
		require.NoError(t, m.AddSegmentChangeGroup(ctx, base))
		overlap := base.Clone()
		overlap.GroupID = 2
		overlap.Source = model.SegmentChangeSourceForceMerge
		overlap.NewSegmentIDs = []int64{1002}
		overlap.SupersededSegmentIDs = []int64{2001} // same parent as group 1
		require.Error(t, m.AddSegmentChangeGroup(ctx, overlap), "duplicate data replacement must be rejected")
		deleteGroupCleanup(t, m, 10, 1)
	})

	t.Run("new member cannot be another group's superseded", func(t *testing.T) {
		parent := base.Clone()
		parent.GroupID = 2
		parent.SupersededSegmentIDs = []int64{3001}
		parent.NewSegmentIDs = []int64{1002}
		require.NoError(t, m.AddSegmentChangeGroup(ctx, parent))

		child := base.Clone()
		child.GroupID = 3
		child.NewSegmentIDs = []int64{3001} // 3001 is parent's superseded
		child.SupersededSegmentIDs = nil
		require.Error(t, m.AddSegmentChangeGroup(ctx, child))
		deleteGroupCleanup(t, m, 10, 2)
	})

	t.Run("superseded cannot be another group's staged member", func(t *testing.T) {
		member := base.Clone()
		member.GroupID = 2
		member.NewSegmentIDs = []int64{1001}
		member.SupersededSegmentIDs = nil
		require.NoError(t, m.AddSegmentChangeGroup(ctx, member))

		retire := base.Clone()
		retire.GroupID = 3
		retire.NewSegmentIDs = []int64{1002}
		retire.SupersededSegmentIDs = []int64{1001} // 1001 is member's new_segment
		require.Error(t, m.AddSegmentChangeGroup(ctx, retire))
		deleteGroupCleanup(t, m, 10, 2)
	})

	t.Run("L0 superseded is exempt from uniqueness", func(t *testing.T) {
		g1 := base.Clone()
		g1.GroupID = 2
		g1.SupersededSegmentIDs = []int64{4001} // L0 parent
		require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))
		g2 := base.Clone()
		g2.GroupID = 3
		g2.NewSegmentIDs = []int64{1002}        // distinct member
		g2.SupersededSegmentIDs = []int64{4001} // same L0 parent, allowed
		require.NoError(t, m.AddSegmentChangeGroup(ctx, g2))
		deleteGroupCleanup(t, m, 10, 2)
		deleteGroupCleanup(t, m, 10, 3)
	})

	t.Run("terminal group releases its superseded reference", func(t *testing.T) {
		require.NoError(t, m.AddSegmentChangeGroup(ctx, base))
		ready := base.Clone()
		ready.State = model.SegmentChangeStateReady
		require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready))
		committed := ready.Clone()
		committed.State = model.SegmentChangeStateCommitted
		require.NoError(t, m.UpdateSegmentChangeGroup(ctx, committed))

		reuse := base.Clone()
		reuse.GroupID = 2
		reuse.SupersededSegmentIDs = []int64{2001} // parent was released on commit
		require.NoError(t, m.AddSegmentChangeGroup(ctx, reuse))
		deleteGroupCleanup(t, m, 10, 1)
		deleteGroupCleanup(t, m, 10, 2)
	})
}

// TestMeta_LoadSegmentChangeGroups_ConflictFailClosed verifies recovery rejects
// a persisted superseded-overlap instead of silently picking a winner.
func TestMeta_LoadSegmentChangeGroups_ConflictFailClosed(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	for _, seg := range []*SegmentInfo{
		NewSegmentInfo(&datapb.SegmentInfo{
			ID: 2001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
		}),
	} {
		require.NoError(t, m.AddSegment(ctx, seg))
	}

	g1 := newTestGroup()
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))

	// g2 initially supersedes a DIFFERENT parent so registration succeeds.
	g2 := g1.Clone()
	g2.GroupID = 2
	g2.NewSegmentIDs = []int64{1002}
	g2.SupersededSegmentIDs = []int64{3001}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g2))

	ready1 := g1.Clone()
	ready1.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready1))
	ready2 := g2.Clone()
	ready2.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready2))

	// Directly overwrite group 2 to also supersede 2001, simulating a stale
	// persisted conflict from an older version.
	forged := ready2.Clone()
	forged.SupersededSegmentIDs = []int64{2001}
	require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(forged)))

	_, _, _, err = m.loadSegmentChangeGroups(ctx)
	require.Error(t, err, "persisted superseded overlap must fail recovery")
}

// TestMeta_LoadSegmentChangeGroups_MemberConflictFailClosed verifies N2: a
// persisted conflict where two ALIVE groups stage the SAME member must also
// fail recovery (the member-side overlap check, not only the superseded one).
func TestMeta_LoadSegmentChangeGroups_MemberConflictFailClosed(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()

	g1 := newTestGroup() // new=[1001]
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g1))

	// g2 stages a DIFFERENT member so registration succeeds.
	g2 := g1.Clone()
	g2.GroupID = 2
	g2.NewSegmentIDs = []int64{1002}
	g2.SupersededSegmentIDs = []int64{3001}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, g2))

	ready1 := g1.Clone()
	ready1.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready1))
	ready2 := g2.Clone()
	ready2.State = model.SegmentChangeStateReady
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, ready2))

	// Directly overwrite group 2 to also stage member 1001, simulating a stale
	// persisted conflict from an older version.
	forged := ready2.Clone()
	forged.NewSegmentIDs = []int64{1001}
	require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(forged)))

	_, _, _, err = m.loadSegmentChangeGroups(ctx)
	require.Error(t, err, "persisted member overlap must fail recovery")
}
