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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	kvcatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Exercise the real catalog encoder and ordered transaction fallback. Fail
// either during the member chunks or before/after the final commit marker.
type segmentChangeCrashKV struct {
	kv.MetaKv
	chunk        int
	failChunk    int
	failCommit   bool
	loseResponse bool
}

func (s *segmentChangeCrashKV) MaxTxnOps() int { return 2 }

func (s *segmentChangeCrashKV) MultiSave(ctx context.Context, values map[string]string) error {
	s.chunk++
	if s.chunk == s.failChunk {
		return merr.WrapErrServiceUnavailableMsg("injected chunk failure")
	}
	return s.MetaKv.MultiSave(ctx, values)
}

func (s *segmentChangeCrashKV) MultiSaveAndRemove(ctx context.Context, values map[string]string, removals []string, guards ...predicates.Predicate) error {
	if s.failCommit {
		return merr.WrapErrServiceUnavailableMsg("injected commit failure")
	}
	if err := s.MetaKv.MultiSaveAndRemove(ctx, values, removals, guards...); err != nil {
		return err
	}
	if s.loseResponse {
		return merr.WrapErrServiceUnavailableMsg("injected lost commit response")
	}
	return nil
}

func liveSegmentChangeCollection(context.Context, int64) (bool, error) { return true, nil }

func reloadSegmentChangeMeta(t *testing.T, old *meta) *meta {
	t.Helper()
	b := broker.NewMockBroker(t)
	b.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		DbCollections: []*rootcoordpb.DBCollections{{CollectionIDs: []int64{10, 11}}},
	}, nil)
	m, err := newMeta(t.Context(), old.catalog, nil, metacache.NewMetaStore(old.catalog), b)
	require.NoError(t, err)
	t.Cleanup(m.closeManifestCommitExecutor)
	m.AddCollection(&collectionInfo{ID: 10, Schema: readinessVectorSchema()})
	return m
}

func TestSegmentChangeRecovery_ChunkedPublish(t *testing.T) {
	for _, failure := range []string{"member_chunk", "commit_marker", "lost_response"} {
		t.Run(failure, func(t *testing.T) {
			ctx := t.Context()
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			for _, id := range []int64{1001, 1002, 1003, 2001} {
				require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
					ID: id, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
					State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
					IsInvisible: id != 2001, NumOfRows: 42,
					Binlogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: id}}}},
				})))
			}
			g := newTestGroup()
			g.Source = model.SegmentChangeSourceImportJob
			g.NewSegmentIDs = []int64{1001, 1002, 1003}
			require.NoError(t, m.AddSegmentChangeGroup(ctx, g))
			g.State = model.SegmentChangeStateReady
			require.NoError(t, m.UpdateSegmentChangeGroup(ctx, g))
			catalog := m.catalog.(*kvcatalog.Catalog)
			store := &segmentChangeCrashKV{MetaKv: catalog.MetaKv}
			switch failure {
			case "member_chunk":
				store.failChunk = 2
			case "commit_marker":
				store.failCommit = true
			case "lost_response":
				store.loseResponse = true
			}
			catalog.MetaKv = store
			// Record-only updates keep the original binlog keys. The first
			// chunk flips two members; the second flips the third and retires
			// the parent. Group + DataView are the final two commit ops.
			actions := make([]metastore.UpdateAction, 0)
			for _, id := range g.NewSegmentIDs {
				seg := m.GetSegment(ctx, id).Clone()
				seg.IsInvisible = false
				seg.CommitTimestamp = 500
				actions = append(actions, metastore.AlterSegment(seg.SegmentInfo))
			}
			parent := m.GetSegment(ctx, 2001).Clone()
			parent.State = commonpb.SegmentState_Dropped
			actions = append(actions, metastore.AlterSegment(parent.SegmentInfo))
			committed := g.Clone()
			committed.State = model.SegmentChangeStateCommitted
			committed.CommitTS = 500
			actions = append(actions, metastore.SaveSegmentChangeGroup(committed), metastore.SaveDataView(&viewpb.DataViewOfCollection{
				CollectionId: 10, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1},
				Shards: []*viewpb.DataViewOfShard{{Vchannel: "ch-1", Partitions: []*viewpb.DataViewOfPartition{{
					PartitionId: 100, SegmentIds: g.NewSegmentIDs, SegmentManifestVersions: []int64{0, 0, 0},
				}}}},
			}))
			require.Error(t, catalog.Update(ctx, actions...))
			catalog.MetaKv = store.MetaKv
			recovered := reloadSegmentChangeMeta(t, m)
			if failure == "member_chunk" {
				require.False(t, recovered.GetSegment(ctx, 1001).GetIsInvisible())
				require.True(t, recovered.GetSegment(ctx, 1003).GetIsInvisible())
			}
			if failure != "lost_response" {
				// Projection must not leak any ALIVE member before the barrier.
				segments, err := recovered.loadableProjection(ctx, 10)
				require.NoError(t, err)
				for _, seg := range segments {
					require.Equal(t, int64(2001), seg.SegmentID)
				}
			}
			// An already-started publication must complete even after a schema
			// bump. No fresh TSO is allowed for the imported rows.
			recovered.GetCollection(10).Schema.Version = 2
			require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
			require.Equal(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(ctx, 10, 1).State)
			for _, id := range g.NewSegmentIDs {
				require.False(t, recovered.GetSegment(ctx, id).GetIsInvisible())
				require.Equal(t, uint64(500), recovered.GetSegment(ctx, id).GetCommitTimestamp())
			}
			require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(ctx, 2001).GetState())
			manager, err := dataview.RecoverManager(ctx, catalog, liveSegmentChangeCollection, recovered.loadableProjection, []int64{10}, map[int64][]string{10: {"ch-1"}})
			require.NoError(t, err)
			require.NoError(t, recovered.reconcileSegmentChangeDataViews(ctx, manager))
			ref, err := manager.Latest(ctx, 10)
			require.NoError(t, err)
			require.NotNil(t, ref)
			defer ref.Deref()
			require.ElementsMatch(t, g.NewSegmentIDs, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
			require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection), "recovery is idempotent")
		})
	}
}

type segmentChangeRecoveryManager struct {
	dataview.Manager
	err error
}

func (m *segmentChangeRecoveryManager) RecomputeNow(context.Context, int64, dataview.Projector) (*viewpb.DataVersion, error) {
	return &viewpb.DataVersion{StreamingVersion: 1}, m.err
}

func TestSegmentChangeRecovery_RequiresSuccessfulDataViewRebuild(t *testing.T) {
	m, _ := seedStagedInspectorMeta(t)
	dv := dataview.NewManager(m.catalog.(dataview.Catalog), nil)
	manager := &segmentChangeRecoveryManager{Manager: dv, err: merr.WrapErrServiceUnavailableMsg("DataView catalog unavailable")}
	require.ErrorIs(t, m.reconcileSegmentChangeDataViews(t.Context(), manager), merr.ErrServiceUnavailable, "missing bootstrap must be retried before projection-only reconstruction")
	_, err := dv.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 10, VChannels: []string{"ch-1", "empty-channel"}})
	require.NoError(t, err)
	require.ErrorIs(t, m.reconcileSegmentChangeDataViews(t.Context(), manager), merr.ErrServiceUnavailable)
	manager.err = nil
	require.NoError(t, m.reconcileSegmentChangeDataViews(t.Context(), manager))
}

func TestSegmentChangeRecovery_MissingMemberAndUnpublishedReady(t *testing.T) {
	ctx := t.Context()
	m, g := seedStagedInspectorMeta(t)
	g.NewSegmentIDs = append(g.NewSegmentIDs, 1002) // registration chunk never landed
	// Inject the persisted group-first registration state.
	require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
	recovered := reloadSegmentChangeMeta(t, m)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
	require.Equal(t, model.SegmentChangeStateFailed, recovered.GetSegmentChangeGroup(ctx, 10, 1).State)
	require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(ctx, 1001).GetState())
	require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))

	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 1).State, "unpublished READY waits for task workers instead of blocking startup")
}

func TestSegmentChangeVisibilityAndCompactionOwnership(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	g := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	for _, write := range []func() error{
		func() error { return m.UpdateSegmentsInfo(ctx, SetSegmentIsInvisible(1001, false)) },
		func() error {
			_, err := m.UpdateSegmentsInfoAndDataView(ctx, nil, SetSegmentIsInvisible(1001, false))
			return err
		},
		func() error { return m.UpdateSegmentsInfoAndChangeGroups(ctx, nil, SetSegmentIsInvisible(1001, false)) },
		func() error {
			m.segMu.Lock()
			defer m.segMu.Unlock()
			_, err := m.applySegmentCatalogMutation(m.segments.GetSegment(1001), SegmentCatalogMutation{Operators: []UpdateOperator{SetSegmentIsInvisible(1001, false)}})
			return err
		},
	} {
		require.ErrorIs(t, write(), merr.ErrDataIntegrity)
		require.True(t, m.GetSegment(ctx, 1001).GetIsInvisible())
	}
	for _, id := range []int64{1001, 2001} {
		exists, allowed := m.CheckAndSetSegmentsCompacting(ctx, []int64{id})
		require.True(t, exists)
		require.False(t, allowed, "members and superseded parents remain reserved even when isCompacting is false")
	}
	// A group-less awaiting-sort segment retains ordinary compaction semantics.
	ordinary := m.GetSegment(ctx, 1001).Clone()
	ordinary.ID = 3001
	ordinary.IsSorted = false
	require.NoError(t, m.AddSegment(ctx, ordinary))
	exists, allowed := m.CheckAndSetSegmentsCompacting(ctx, []int64{3001})
	require.True(t, exists)
	require.True(t, allowed)
	require.NoError(t, m.publishGroup(ctx, g, newMockAllocator(t)))
	ref, err := m.catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, model.SegmentChangeStateCommitted, ref[0].State)
}

type segmentChangeReclaimCatalog struct {
	metastore.DataCoordCatalog
	cancel context.CancelFunc
	writes int
}

func (c *segmentChangeReclaimCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	c.writes++
	if c.cancel != nil {
		c.cancel()
		return ctx.Err()
	}
	return c.DataCoordCatalog.Update(ctx, actions...)
}

func TestSegmentChangeReclamation_RetryAndParentSafety(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	g := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	g.State = model.SegmentChangeStateAborted
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, g))
	require.True(t, m.unpublishedSegmentMembers(ctx, 10).Contain(1001))
	_, allowed := m.CheckAndSetSegmentsCompacting(ctx, []int64{1001})
	require.False(t, allowed, "failed members stay protected until reclamation completes")
	require.ErrorIs(t, m.UpdateSegmentsInfo(ctx, SetSegmentIsInvisible(1001, false)), merr.ErrDataIntegrity)
	require.ErrorIs(t, m.DeleteSegmentChangeGroup(ctx, 10, 1), merr.ErrDataIntegrity)
	require.ErrorIs(t, m.UpdateSegmentsInfoAndChangeGroups(ctx, []metastore.UpdateAction{metastore.DeleteSegmentChangeGroup(10, 1)}), merr.ErrDataIntegrity)

	failedCtx, cancel := context.WithCancel(ctx)
	catalog := &segmentChangeReclaimCatalog{DataCoordCatalog: m.catalog, cancel: cancel}
	m.catalog = catalog
	require.ErrorIs(t, m.reclaimSegmentChangeGroup(failedCtx, g), context.Canceled)
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 1001).GetState())
	catalog.cancel = nil
	i := newSegmentChangeInspector(ctx, m, nil)
	i.checkAllGroups(ctx)
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 1001).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 2001).GetState(), "abort never retires the original data")
	writes := catalog.writes
	i.checkAllGroups(ctx)
	require.Equal(t, writes, catalog.writes, "completed reclamation does not keep rewriting Dropped members")

	recovered := reloadSegmentChangeMeta(t, m)
	require.NoError(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection))
	require.Equal(t, commonpb.SegmentState_Dropped, recovered.GetSegment(ctx, 1001).GetState())
}

func TestSegmentChangeRecovery_RejectsUnsafePartialPublish(t *testing.T) {
	for _, failure := range []string{"staged", "missing_member", "timestamp_zero", "foreign_parent", "invalid_manifest", "allocator_failure"} {
		t.Run(failure, func(t *testing.T) {
			ctx := t.Context()
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			g := seedPublishMeta(t, m, model.SegmentChangeSourceImportJob)
			partial := m.GetSegment(ctx, 1001).Clone()
			partial.IsInvisible = false
			partial.CommitTimestamp = 500
			switch failure {
			case "staged":
				g.State = model.SegmentChangeStateStaged
			case "missing_member":
				g.NewSegmentIDs = append(g.NewSegmentIDs, 1002)
			case "timestamp_zero":
				partial.CommitTimestamp = 0
			case "foreign_parent":
				parent := m.GetSegment(ctx, 2001).Clone()
				require.NoError(t, m.DropSegment(ctx, 2001))
				parent.CollectionID = 11
				require.NoError(t, m.AddSegment(ctx, parent))
			case "invalid_manifest":
				partial.ManifestPath = "invalid"
			case "allocator_failure":
				g.Source = model.SegmentChangeSourceMixCompaction
				partial.CommitTimestamp = 0
			}
			require.NoError(t, m.catalog.AlterSegments(ctx, []*datapb.SegmentInfo{partial.SegmentInfo}))
			require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
			recovered := reloadSegmentChangeMeta(t, m)
			var alloc allocator.Allocator
			if failure == "allocator_failure" {
				mockAlloc := allocator.NewMockAllocator(t)
				mockAlloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(0), merr.WrapErrServiceUnavailableMsg("TSO unavailable"))
				alloc = mockAlloc
			}
			require.Error(t, recovered.reconcileSegmentChangeGroups(ctx, alloc, liveSegmentChangeCollection))
			require.NotEqual(t, model.SegmentChangeStateCommitted, recovered.GetSegmentChangeGroup(ctx, 10, 1).State)
			require.Equal(t, commonpb.SegmentState_Flushed, recovered.GetSegment(ctx, 1001).GetState(), "unsafe recovery must not reclaim visible rows")
		})
	}
}

func TestSegmentChangeRecovery_VisibleMemberNeverReclaimed(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	g := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	partial := m.GetSegment(ctx, 1001).Clone()
	partial.IsInvisible = false
	require.NoError(t, m.catalog.AlterSegments(ctx, []*datapb.SegmentInfo{partial.SegmentInfo}))
	m.segments.SetSegment(1001, partial)
	g.State = model.SegmentChangeStateAborted
	require.ErrorIs(t, m.UpdateSegmentChangeGroup(ctx, g), merr.ErrDataIntegrity)
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(ctx, 10, 1).State)

	// An old/corrupt terminal record must fail recovery without deleting rows.
	require.NoError(t, m.catalog.Update(ctx, metastore.SaveSegmentChangeGroup(g)))
	recovered := reloadSegmentChangeMeta(t, m)
	require.ErrorIs(t, recovered.reconcileSegmentChangeGroups(ctx, nil, liveSegmentChangeCollection), merr.ErrDataIntegrity)
	require.Equal(t, commonpb.SegmentState_Flushed, recovered.GetSegment(ctx, 1001).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, recovered.GetSegment(ctx, 2001).GetState())
}

func TestSegmentChangeL0Selection_PreservesAwaitingSort(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	ordinary := m.GetSegment(ctx, 1001).Clone()
	ordinary.ID = 3001
	ordinary.IsSorted = false
	require.NoError(t, m.AddSegment(ctx, ordinary))
	task := newL0CompactionTask(&datapb.CompactionTask{
		CollectionID: 10, PartitionID: 100, Channel: "ch-1", Pos: &msgpb.MsgPosition{Timestamp: 100},
	}, nil, m)
	segments, _, err := task.selectFlushedSegment()
	require.NoError(t, err)
	ids := make([]int64, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.GetID())
	}
	require.ElementsMatch(t, []int64{2001, 3001}, ids, "L0 skips group members but retains group-less awaiting-sort and visible parent targets")

	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(10)).Return(m.GetCollection(10), nil)
	policy := newSingleCompactionPolicy(m, nil, handler)
	invisible, visible, err := policy.triggerSortCompaction(ctx, 1, 10, 0)
	require.NoError(t, err)
	require.Len(t, invisible, 1, "the ordinary awaiting-sort segment still gets a sort task")
	require.Equal(t, int64(3001), invisible[0].GetSegmentsView()[0].ID)
	require.Empty(t, visible, "the superseded parent stays reserved")
}

func TestSegmentChangeRecovery_CollectionDropAndValidationFailure(t *testing.T) {
	ctx := t.Context()
	m, _ := seedStagedInspectorMeta(t)
	require.ErrorIs(t, m.reconcileSegmentChangeGroups(ctx, nil, func(context.Context, int64) (bool, error) {
		return false, merr.WrapErrServiceUnavailableMsg("rootcoord unavailable")
	}), merr.ErrServiceUnavailable)
	require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 1))
	require.NoError(t, m.reconcileSegmentChangeGroups(ctx, nil, func(context.Context, int64) (bool, error) { return false, nil }))
	require.Empty(t, m.GetSegmentChangeGroupsByCollection(ctx, 10))
	groups, err := m.catalog.ListSegmentChangeGroups(ctx)
	require.NoError(t, err)
	require.Empty(t, groups)
}
