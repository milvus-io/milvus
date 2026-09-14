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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotClusteringPublicationProtection(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	sm := createTestSnapshotMetaLoaded(t)
	catalog := catalogmocks.NewDataCoordCatalog(t)
	m := &meta{ctx: ctx, segments: NewSegmentsInfo(), snapshotMeta: sm, catalog: catalog}
	m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L1,
	}))
	m.segments.SetSegment(2, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 2, CollectionID: 100, State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L2, IsInvisible: true, CreatedByCompaction: true,
		CompactionFrom: []int64{1},
	}))
	task := &datapb.CompactionTask{PlanID: 10, CollectionID: 100,
		Type:          datapb.CompactionType_ClusteringCompaction,
		InputSegments: []int64{1}, ResultSegments: []int64{2},
		State: datapb.CompactionTaskState_indexing,
	}
	clustering := newClusteringCompactionTask(task, nil, m, nil, nil, nil)

	// A task admitted before the snapshot must not publish after capture starts.
	sm.SetSnapshotPending(100)
	exists, admitted := m.CheckAndSetSegmentsCompacting(ctx, []int64{1})
	require.True(t, exists)
	require.False(t, admitted)
	err := clustering.markResultSegmentsVisible()
	require.ErrorIs(t, err, merr.ErrCompactionBlocked)
	require.True(t, m.GetHealthySegment(ctx, 2).GetIsInvisible())
	clustering.retryOnError(err)
	require.Equal(t, datapb.CompactionTaskState_indexing, clustering.GetTaskProto().GetState())
	require.Zero(t, clustering.GetTaskProto().GetRetryTimes())
	require.True(t, sm.IsSegmentGCBlocked(100, 1))
	require.True(t, sm.IsBuildIDGCBlocked(100, 123))

	// Saving transfers pending protection to the referenced inputs' TTL.
	sm.registerSnapshotProtection(&datapb.SnapshotInfo{
		CollectionId: 100, CompactionExpireTime: uint64(time.Now().Unix()) + 600,
	}, []int64{1}, nil)
	sm.ClearSnapshotPending(100)
	exists, admitted = m.CheckAndSetSegmentsCompacting(ctx, []int64{1})
	require.True(t, exists)
	require.False(t, admitted)
	require.ErrorIs(t, clustering.markResultSegmentsVisible(), merr.ErrCompactionBlocked)
	require.ErrorIs(t, clustering.markInputSegmentsDropped(), merr.ErrCompactionBlocked)
	require.NotNil(t, m.GetHealthySegment(ctx, 1))

	// Expiry lets the same task finish through its normal metadata path.
	sm.segmentProtectionUntil[1] = 1
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Twice()
	require.NoError(t, clustering.markResultSegmentsVisible())
	require.False(t, m.GetHealthySegment(ctx, 2).GetIsInvisible())
	require.NoError(t, clustering.markInputSegmentsDropped())
	require.Nil(t, m.GetHealthySegment(ctx, 1))
}

func TestSnapshotRetriesPartiallyPublishedClustering(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        datapb.CompactionTaskState
		resultState  commonpb.SegmentState
		removeResult bool
	}{
		{name: "visible result", state: datapb.CompactionTaskState_indexing, resultState: commonpb.SegmentState_Flushed},
		{name: "result replaced", state: datapb.CompactionTaskState_completed, resultState: commonpb.SegmentState_Dropped},
		{name: "result garbage collected", state: datapb.CompactionTaskState_completed, removeResult: true},
		{name: "publication state not saved", state: datapb.CompactionTaskState_indexing, resultState: commonpb.SegmentState_Dropped},
		{name: "missing result before cleanup", state: datapb.CompactionTaskState_failed, removeResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := emptySnapshotTestMeta()
			// There is intentionally no intermediate sort segment in metadata.
			// The durable task supplies the original input/result relationship.
			m.compactionTaskMeta = &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{
				10: {11: {CollectionID: 100, Type: datapb.CompactionType_ClusteringCompaction,
					PlanID: 11, InputSegments: []int64{1}, ResultSegments: []int64{3},
					State: tc.state}},
			}}
			m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			}))
			if !tc.removeResult {
				m.segments.SetSegment(3, NewSegmentInfo(&datapb.SegmentInfo{
					ID: 3, CollectionID: 100, State: tc.resultState,
					CreatedByCompaction: true, CompactionFrom: []int64{2},
				}))
			}
			// A later compaction has replaced result 3. Neither 2 nor possibly 3
			// remains to connect this live descendant to original input 1.
			m.segments.SetSegment(4, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 4, CollectionID: 100, State: commonpb.SegmentState_Flushed,
				CreatedByCompaction: true, CompactionFrom: []int64{3},
			}))
			protection := createTestSnapshotMetaLoaded(t)
			m.snapshotMeta = protection
			manager := NewSnapshotManager(m, protection, nil, nil, nil, nil, nil, nil)
			// Fails before allocation/storage and releases pending for cleanup to run.
			_, err := manager.CreateSnapshot(ctx, 100, "overlap", "", 0, testCreateSnapshotBoundary(), false)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.False(t, protection.IsCollectionCompactionBlocked(100))
			m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 1, CollectionID: 100, State: commonpb.SegmentState_Dropped,
			}))
			require.NoError(t, manager.checkClusteringPublication(ctx, 100))
		})
	}
}

func TestSnapshotAllowsUnpublishedClustering(t *testing.T) {
	ctx := context.Background()
	m := emptySnapshotTestMeta()
	m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
	}))
	m.segments.SetSegment(3, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 100, State: commonpb.SegmentState_Flushed,
		IsInvisible: true, CreatedByCompaction: true, CompactionFrom: []int64{2},
	}))
	task := &datapb.CompactionTask{
		Type: datapb.CompactionType_ClusteringCompaction, State: datapb.CompactionTaskState_indexing,
		InputSegments: []int64{1}, ResultSegments: []int64{3},
	}
	require.False(t, clusteringPublicationOverlaps(ctx, m, task))
	// Failed, fully cleaned tasks may keep their inputs and lose the outputs.
	task.State = datapb.CompactionTaskState_cleaned
	m.segments.SetSegment(3, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 100, State: commonpb.SegmentState_Dropped,
		IsInvisible: false, CreatedByCompaction: true, CompactionFrom: []int64{2},
	}))
	require.False(t, clusteringPublicationOverlaps(ctx, m, task), "rollback keeps the old visibility bit on dropped results")
	task.ResultSegments = []int64{9}
	require.False(t, clusteringPublicationOverlaps(ctx, m, task))
}

func TestSnapshotRejectsUndrainableCompactionBeforeBroadcast(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name         string
		protected    bool
		enabled      bool
		taskType     datapb.CompactionType
		state        datapb.CompactionTaskState
		inputDropped bool
		wantError    bool
	}{
		{name: "protected queued task", protected: true, state: datapb.CompactionTaskState_pipelining, wantError: true},
		{name: "protected failed cleanup", protected: true, state: datapb.CompactionTaskState_failed, wantError: true},
		{name: "protected cleaned task", protected: true, state: datapb.CompactionTaskState_cleaned},
		{name: "ordinary queued task", state: datapb.CompactionTaskState_pipelining},
		{name: "ordinary published clustering", taskType: datapb.CompactionType_ClusteringCompaction, state: datapb.CompactionTaskState_completed, wantError: true},
		{name: "published inputs retired", taskType: datapb.CompactionType_ClusteringCompaction, state: datapb.CompactionTaskState_completed, inputDropped: true},
		{name: "L0 does not replace targets", protected: true, taskType: datapb.CompactionType_Level0DeleteCompaction, state: datapb.CompactionTaskState_pipelining},
		{name: "enabled can drain", protected: true, enabled: true, state: datapb.CompactionTaskState_pipelining},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := "false"
			if tc.enabled {
				value = "true"
			}
			require.NoError(t, paramtable.Get().Save(Params.DataCoordCfg.EnableCompaction.Key, value))
			t.Cleanup(func() { paramtable.Get().Reset(Params.DataCoordCfg.EnableCompaction.Key) })
			if tc.taskType == datapb.CompactionType_UndefinedCompaction {
				tc.taskType = datapb.CompactionType_MixCompaction
			}
			m := emptySnapshotTestMeta()
			state := commonpb.SegmentState_Flushed
			if tc.inputDropped {
				state = commonpb.SegmentState_Dropped
			}
			m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 100, State: state}))
			m.compactionTaskMeta = &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{
				10: {10: {PlanID: 10, CollectionID: 100, Type: tc.taskType, State: tc.state, InputSegments: []int64{1}}},
			}}
			err := checkSnapshotCompactionReachable(context.Background(), m, 100, tc.protected)
			if tc.wantError {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Contains(t, err.Error(), "re-enable dataCoord.enableCompaction")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSnapshotLegacyCallbackReplay(t *testing.T) {
	ctx := context.Background()
	protection := createTestSnapshotMetaLoaded(t)
	info := &datapb.SnapshotInfo{Id: 77, CollectionId: 100, Name: "legacy", State: datapb.SnapshotState_SnapshotStateCommitted}
	protection.snapshotID2Info.Insert(77, info)
	protection.addToSecondaryIndexes(info)
	callbacks := &DDLCallbacks{Server: &Server{snapshotManager: &snapshotManager{snapshotMeta: protection}}}
	msg := message.NewCreateSnapshotMessageBuilderV2().
		WithHeader(&message.CreateSnapshotMessageHeader{CollectionId: 100, Name: "legacy"}).
		WithBody(&message.CreateSnapshotMessageBody{}).
		WithBroadcast([]string{"by-dev-rootcoord-dml_0vcchan"}).MustBuildBroadcast()
	result := message.BroadcastResultCreateSnapshotMessageV2{
		Message: message.MustAsBroadcastCreateSnapshotMessageV2(msg),
		Results: map[string]*message.AppendResult{"by-dev-rootcoord-dml_0vcchan": {MessageID: rmq.NewRmqID(1), TimeTick: 100}},
	}
	// No allocator, handler or data watermark is needed for an already saved request.
	require.NoError(t, callbacks.createSnapshotV2AckCallback(ctx, result))
}

func TestSnapshotLegacyRequestUsesCheckpoints(t *testing.T) {
	ctx := context.Background()
	m := emptySnapshotTestMeta()
	protection := createTestSnapshotMetaLoaded(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(78), nil)
	handler := NewNMockHandler(t)
	handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
		ID: 100, VChannelNames: []string{testCreateSnapshotChannel},
	}, nil)
	handler.EXPECT().GenSnapshot(mock.Anything, int64(100), mock.MatchedBy(func(b *SnapshotBoundary) bool {
		return b != nil && len(b.SeekPositions) == 1 && b.SnapshotTs == testCreateSnapshotBoundaryTs+1
	})).Return(&snapshotstorage.SnapshotData{SnapshotInfo: &datapb.SnapshotInfo{CollectionId: 100}}, nil)
	saveErr := merr.WrapErrServiceUnavailableMsg("injected storage failure")
	patch := mockey.Mock((*snapshotMeta).SaveSnapshot).Return(saveErr).Build()
	defer patch.UnPatch()
	// Saving itself is covered by the snapshot storage suite. A catalog error
	// here proves legacy replay reached capture and releases the pending block.
	manager := NewSnapshotManager(m, protection, nil, alloc, handler, nil, nil, nil)
	_, err := manager.CreateSnapshot(ctx, 100, "legacy", "", 0, nil, false)
	require.ErrorIs(t, err, saveErr)
	require.False(t, protection.IsCollectionCompactionBlocked(100))
}

// A recovered task can still own a precomputed output before compacting flags
// have been restored. Protection must wait for it, even while outputs are invisible.
func TestSnapshotBackfillWaitsForExistingCompaction(t *testing.T) {
	ctx := context.Background()
	m := emptySnapshotTestMeta()
	input := NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
		InsertChannel: testCreateSnapshotChannel, Level: datapb.SegmentLevel_L1,
	})
	m.segments.SetSegment(1, input)
	sm := &snapshotManager{meta: m}
	require.NoError(t, sm.checkSnapshotBackfillTargets(ctx, 100, testCreateSnapshotBoundary()))
	input.isCompacting = true
	require.ErrorIs(t, sm.checkSnapshotBackfillTargets(ctx, 100, testCreateSnapshotBoundary()), merr.ErrServiceUnavailable)
	input.isCompacting = false
	task := &datapb.CompactionTask{PlanID: 10, CollectionID: 100,
		Type:          datapb.CompactionType_ClusteringCompaction,
		InputSegments: []int64{1}, ResultSegments: []int64{2},
		State: datapb.CompactionTaskState_indexing,
	}
	m.compactionTaskMeta = &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{10: {10: task}}}
	require.ErrorIs(t, sm.checkSnapshotBackfillTargets(ctx, 100, testCreateSnapshotBoundary()), merr.ErrServiceUnavailable)
	task.State = datapb.CompactionTaskState_cleaned
	require.NoError(t, sm.checkSnapshotBackfillTargets(ctx, 100, testCreateSnapshotBoundary()))
}

func TestSnapshotCapturePinsCurrentReplacement(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name       string
		protection int64
		failCommit bool
	}{
		{name: "ordinary snapshot"},
		{name: "protected snapshot", protection: 600},
		{name: "commit failure releases protection", protection: 600, failCommit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m := emptySnapshotTestMeta()
			protection := createTestSnapshotMetaLoaded(t)
			m.snapshotMeta = protection
			m.indexMeta = &indexMeta{segmentIndexes: typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *model.SegmentIndex]]()}
			catalog := catalogmocks.NewDataCoordCatalog(t)
			m.catalog = catalog
			protection.catalog = catalog
			input := &datapb.SegmentInfo{
				ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed,
				InsertChannel: testCreateSnapshotChannel, Level: datapb.SegmentLevel_L1, IsSorted: true,
				NumOfRows: 10, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 10}}}},
			}
			m.segments.SetSegment(1, NewSegmentInfo(input))
			// The clustering intermediate and result have both been GC'd. The
			// serving replacement carries only its immediate parent's identity.
			m.segments.SetSegment(4, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 4, CollectionID: 100, State: commonpb.SegmentState_Flushed,
				InsertChannel: testCreateSnapshotChannel, Level: datapb.SegmentLevel_L2, IsSorted: true,
				CreatedByCompaction: true, CompactionFrom: []int64{3}, NumOfRows: 10,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogID: 4, EntriesNum: 10}}}},
			}))
			m.compactionTaskMeta = &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{
				10: {10: {PlanID: 10, CollectionID: 100, Type: datapb.CompactionType_ClusteringCompaction,
					State: datapb.CompactionTaskState_completed, InputSegments: []int64{1}, ResultSegments: []int64{3}}},
			}}
			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocID(mock.Anything).Return(int64(78), nil).Once()
			coord := broker.NewMockBroker(t)
			coord.EXPECT().DescribeCollectionInternal(mock.Anything, int64(100)).Return(&milvuspb.DescribeCollectionResponse{
				Status: merr.Success(), Schema: newTestSchema(), CollectionID: 100,
				VirtualChannelNames: []string{testCreateSnapshotChannel},
			}, nil).Once()
			coord.EXPECT().ShowPartitions(mock.Anything, int64(100)).Return(&milvuspb.ShowPartitionsResponse{
				Status: merr.Success(), PartitionIDs: []int64{0}, PartitionNames: []string{"_default"},
			}, nil).Once()
			handler := &ServerHandler{s: &Server{meta: m, broker: coord}}
			manager := NewSnapshotManager(m, protection, nil, alloc, handler, nil, nil, nil)
			_, err := manager.CreateSnapshot(ctx, 100, "current", "", tc.protection, testCreateSnapshotBoundary(), false)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.False(t, protection.IsCollectionCompactionBlocked(100))

			// Cleanup retires the original input. Capture must now save only the
			// current replacement through the real GenSnapshot and SaveSnapshot.
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
			require.NoError(t, m.UpdateSegmentsInfo(ctx, UpdateStatusOperator(1, commonpb.SegmentState_Dropped)))
			commitErr := merr.WrapErrServiceUnavailableMsg("injected snapshot commit failure")
			catalog.EXPECT().SaveSnapshot(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, info *datapb.SnapshotInfo) error {
				require.True(t, protection.IsCollectionCompactionBlocked(100))
				require.True(t, protection.IsSegmentGCBlocked(100, 4))
				_, admitted := m.CheckAndSetSegmentsCompacting(ctx, []int64{4})
				require.False(t, admitted)
				if info.GetState() == datapb.SnapshotState_SnapshotStateCommitted && tc.failCommit {
					return commitErr
				}
				return nil
			}).Twice()
			id, err := manager.CreateSnapshot(ctx, 100, "current", "", tc.protection, testCreateSnapshotBoundary(), false)
			require.False(t, protection.IsCollectionCompactionBlocked(100))
			if tc.failCommit {
				require.ErrorIs(t, err, commitErr)
				_, err = protection.GetSnapshot(ctx, 100, "current")
				require.ErrorIs(t, err, merr.ErrSnapshotNotFound)
				require.False(t, protection.IsSegmentGCBlocked(100, 4))
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(78), id)
				info, err := protection.GetSnapshot(ctx, 100, "current")
				require.NoError(t, err)
				saved, err := protection.reader.ReadSnapshot(ctx, info.GetS3Location(), true)
				require.NoError(t, err)
				require.Equal(t, []int64{4}, saved.SegmentIDs)
				require.Len(t, saved.Segments, 1)
				require.Equal(t, int64(10), saved.Segments[0].GetNumOfRows())
				require.Equal(t, tc.protection > 0, info.GetWaitedForSortedSegments())
				require.True(t, protection.IsSegmentGCBlocked(100, 4))
			}
			_, admitted := m.CheckAndSetSegmentsCompacting(ctx, []int64{4})
			require.Equal(t, tc.protection == 0 || tc.failCommit, admitted)
		})
	}
}

func TestClusteringPublishedOutputWaitsForCleanup(t *testing.T) {
	paramtable.Init()
	for _, finalState := range []datapb.CompactionTaskState{datapb.CompactionTaskState_failed, datapb.CompactionTaskState_completed} {
		t.Run(finalState.String(), func(t *testing.T) {
			ctx := context.Background()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Once()
			catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
			taskMeta, err := newCompactionTaskMeta(ctx, catalog)
			require.NoError(t, err)
			m := &meta{ctx: ctx, segments: NewSegmentsInfo(), catalog: catalog, compactionTaskMeta: taskMeta}
			m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 1, CollectionID: 100, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			}))
			m.segments.SetSegment(3, NewSegmentInfo(&datapb.SegmentInfo{
				ID: 3, CollectionID: 100, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L2,
				IsInvisible: true, CreatedByCompaction: true, CompactionFrom: []int64{2},
			}))
			parent := newClusteringCompactionTask(&datapb.CompactionTask{
				PlanID: 10, TriggerID: 10, CollectionID: 100, Type: datapb.CompactionType_ClusteringCompaction,
				State: datapb.CompactionTaskState_indexing, InputSegments: []int64{1}, ResultSegments: []int64{3},
			}, nil, m, nil, nil, nil)
			require.NoError(t, parent.SaveTaskMeta())
			// The parent's own invisible sorting stage must still be admissible.
			_, admitted := m.CheckAndSetSegmentsCompacting(ctx, []int64{3})
			require.True(t, admitted)
			m.SetSegmentsCompacting(ctx, []int64{3}, false)
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
			require.NoError(t, parent.markResultSegmentsVisible())
			_, admitted = m.CheckAndSetSegmentsCompacting(ctx, []int64{3})
			require.False(t, admitted)
			child := &datapb.CompactionTask{PlanID: 20, CollectionID: 100, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{3}}
			require.ErrorIs(t, m.ValidateSegmentStateBeforeCompleteCompactionMutation(child), merr.ErrCompactionBlocked)
			_, _, err = m.CompleteCompactionMutation(ctx, child, nil)
			require.ErrorIs(t, err, merr.ErrCompactionBlocked, "publication must recheck admission after a concurrent parent publication")

			require.NoError(t, parent.updateAndSaveTaskMeta(setState(finalState)))
			// Once the parent leaves executing, the same durable guard must
			// survive a cleanup storage failure and its retry.
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(merr.WrapErrServiceUnavailableMsg("injected cleanup failure")).Once()
			require.Error(t, parent.doClean())
			_, admitted = m.CheckAndSetSegmentsCompacting(ctx, []int64{3})
			require.False(t, admitted)
			require.True(t, clusteringPublicationOverlaps(ctx, m, parent.GetTaskProto()))
			if finalState == datapb.CompactionTaskState_failed {
				// File cleanup is separate from the segment/task state transition
				// exercised here; avoid requiring a partition-stats file fixture.
				patch := mockey.Mock((*meta).CleanPartitionStatsInfo).Return(nil).Build()
				defer patch.UnPatch()
			}
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Once()
			require.NoError(t, parent.doClean())
			require.Equal(t, datapb.CompactionTaskState_cleaned, parent.GetTaskProto().GetState())
			require.False(t, clusteringPublicationOverlaps(ctx, m, parent.GetTaskProto()))
			servingID := int64(3)
			if finalState == datapb.CompactionTaskState_failed {
				servingID = 1
				require.Nil(t, m.GetHealthySegment(ctx, 3))
			} else {
				require.Nil(t, m.GetHealthySegment(ctx, 1))
			}
			_, admitted = m.CheckAndSetSegmentsCompacting(ctx, []int64{servingID})
			require.True(t, admitted)
		})
	}
}
