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
	"path"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

func clusterSortMetaFixture(t *testing.T) (*meta, *datapb.CompactionTask, *datapb.CompactionPlanResult) {
	t.Helper()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	parent := &datapb.CompactionTask{PlanID: 99, CollectionID: 10, PartitionID: 20, ClusteringKeyField: &schemapb.FieldSchema{FieldID: 103}, State: datapb.CompactionTaskState_statistic, TmpSegments: []int64{1, 2}}
	m := &meta{ctx: context.Background(), catalog: catalog, segments: NewSegmentsInfo(), compactionTaskMeta: &compactionTaskMeta{compactionTasks: map[int64]map[int64]*datapb.CompactionTask{10: {99: parent}}}}
	for _, id := range []int64{1, 2} {
		m.segments.SetSegment(id, NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 10, PartitionID: 20, State: commonpb.SegmentState_Flushed, IsInvisible: true, NumOfRows: 4, Level: datapb.SegmentLevel_L2,
			ClusterStats: &datapb.ClusterStats{Version: 1, ClusteringTaskId: 99, FieldId: 103, GroupId: 0, NumRows: 4, CentroidIds: []uint32{2, 0}, Files: []string{"staging.keys"}},
		}))
	}
	task := &datapb.CompactionTask{
		PlanID: 100, TriggerID: 99, Type: datapb.CompactionType_ClusterSortCompaction, State: datapb.CompactionTaskState_executing,
		CollectionID: 10, PartitionID: 20, InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Version: 1}, PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 200, End: 201},
	}
	result := &datapb.CompactionPlanResult{PlanID: 100, Type: task.Type, State: datapb.CompactionTaskState_completed, Segments: []*datapb.CompactionSegment{{
		SegmentID: 200, NumOfRows: 3,
		ClusterStats: &datapb.ClusterStats{Version: 1, ClusteringTaskId: 99, FieldId: 103, GroupId: 0, NumRows: 3, Sorted: true, Files: []string{"output.keys"}, RangesPath: "ranges", CentroidIds: []uint32{0, 2}},
	}}}
	return m, task, result
}

func TestClusterSortMetadataReplay(t *testing.T) {
	m, task, result := clusterSortMetaFixture(t)
	task.ClusterSortPendingResult = proto.Clone(result).(*datapb.CompactionPlanResult)
	outputs, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	require.True(t, outputs[0].GetIsInvisible())
	require.False(t, outputs[0].GetIsSorted())
	require.Equal(t, commonpb.SegmentState_Dropped, m.segments.GetSegment(1).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.segments.GetSegment(2).GetState())
	require.Equal(t, []int64{1}, outputs[0].GetCompactionFrom())
	// Crash after catalog mutation, before receipt persistence. Replay keeps
	// the same output and does not count it twice or require a live worker.
	again, metric, err := m.CompleteCompactionMutation(context.Background(), task, result)
	require.NoError(t, err)
	require.Same(t, outputs[0], again[0])
	require.Zero(t, metric.rowCountAccChange)
	m.compactionTaskMeta.compactionTasks[10][99].State = datapb.CompactionTaskState_failed
	_, _, err = m.CompleteCompactionMutation(context.Background(), task, result)
	require.ErrorContains(t, err, "parent")
}

func TestClusterSortEmptyMetadataReplay(t *testing.T) {
	m, task, result := clusterSortMetaFixture(t)
	m.catalog.(*mocks.DataCoordCatalog).EXPECT().Update(mock.Anything, mock.Anything).Return(nil)
	result.Segments = nil
	task.ClusterSortPendingResult = proto.Clone(result).(*datapb.CompactionPlanResult)
	for i := 0; i < 2; i++ {
		outputs, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
		require.NoError(t, err)
		require.Empty(t, outputs)
		require.Equal(t, commonpb.SegmentState_Dropped, m.segments.GetSegment(1).GetState())
		require.Equal(t, commonpb.SegmentState_Flushed, m.segments.GetSegment(2).GetState())
	}
}

func TestClusterSortDurableReceiptIncludesEmptyResult(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "rows", true: "all filtered"}[empty], func(t *testing.T) {
			m := NewMockCompactionMeta(t)
			result := &datapb.CompactionPlanResult{PlanID: 100, Type: datapb.CompactionType_ClusterSortCompaction, State: datapb.CompactionTaskState_completed}
			var outputs []*SegmentInfo
			if !empty {
				result.Segments = []*datapb.CompactionSegment{{SegmentID: 200, NumOfRows: 2}}
				outputs = []*SegmentInfo{NewSegmentInfo(&datapb.SegmentInfo{ID: 200})}
			}
			var persisted *datapb.CompactionTask
			calls := 0
			m.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, p *datapb.CompactionTask) error {
				calls++
				if calls == 2 {
					return errors.New("receipt save interrupted")
				}
				persisted = proto.Clone(p).(*datapb.CompactionTask)
				return nil
			})
			m.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, p *datapb.CompactionTask, r *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
				require.NotNil(t, persisted.GetClusterSortPendingResult())
				require.True(t, proto.Equal(result, r))
				return outputs, &segMetricMutation{}, nil
			})
			m.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).Return()
			task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 100, Type: result.Type, State: datapb.CompactionTaskState_executing}, nil, m, nil)
			require.ErrorContains(t, task.saveSegmentMeta(result), "interrupted")
			require.False(t, persisted.GetClusterSortCompleted())
			restored := newMixCompactionTask(persisted, nil, m, nil)
			require.True(t, restored.Process())
			require.True(t, restored.GetTaskProto().GetClusterSortCompleted())
			require.Nil(t, restored.GetTaskProto().GetClusterSortPendingResult())
			require.Len(t, restored.GetTaskProto().ResultSegments, len(outputs))
		})
	}
}

func TestClusterPartitionStatsRebuildAndReplay(t *testing.T) {
	values := []storage.VectorFieldValue{storage.NewFloatVectorFieldValue([]float32{2}), storage.NewFloatVectorFieldValue([]float32{0})}
	snapshot := &storage.PartitionStatsSnapshot{Version: 99, SegmentStats: map[int64]storage.SegmentStats{1: {NumRows: 5, FieldStats: []storage.FieldStats{{FieldID: 103, Type: schemapb.DataType_FloatVector, Centroids: values}}}}}
	segments := map[int64]*SegmentInfo{
		1: NewSegmentInfo(&datapb.SegmentInfo{ID: 1, NumOfRows: 5, ClusterStats: &datapb.ClusterStats{CentroidIds: []uint32{2, 0}}}),
		2: NewSegmentInfo(&datapb.SegmentInfo{ID: 2, NumOfRows: 3, ClusterStats: &datapb.ClusterStats{CentroidIds: []uint32{0, 2}}}),
	}
	get := func(id int64) *SegmentInfo { return segments[id] }
	results := map[int64][]int64{1: {2}}
	updated, err := rebuildClusterPartitionStats(snapshot, results, get)
	require.NoError(t, err)
	require.EqualValues(t, 99, updated.Version)
	require.Equal(t, []storage.VectorFieldValue{values[1], values[0]}, updated.SegmentStats[2].FieldStats[0].Centroids)
	require.Equal(t, 3, updated.SegmentStats[2].NumRows)
	again, err := rebuildClusterPartitionStats(updated, results, get)
	require.NoError(t, err)
	require.Equal(t, updated, again)
	empty, err := rebuildClusterPartitionStats(snapshot, map[int64][]int64{1: nil}, get)
	require.NoError(t, err)
	require.Empty(t, empty.SegmentStats)
}

func TestClusterSortLatestAttemptAndGCPath(t *testing.T) {
	old := &datapb.CompactionTask{PlanID: 10, Type: datapb.CompactionType_ClusterSortCompaction, ClusterSortGroupId: 1, InputSegments: []int64{1}}
	latest := proto.Clone(old).(*datapb.CompactionTask)
	latest.PlanID = 11
	foreign := proto.Clone(old).(*datapb.CompactionTask)
	foreign.PlanID = 12
	foreign.InputSegments = []int64{2}
	multi := proto.Clone(foreign).(*datapb.CompactionTask)
	multi.PlanID, multi.InputSegments = 13, []int64{1, 2}
	require.Same(t, latest, latestClusterSort([]*datapb.CompactionTask{old, foreign, latest, multi}, 1, 1))
	require.Same(t, foreign, latestClusterSort([]*datapb.CompactionTask{old, foreign, latest, multi}, 1, 2))
	id, err := parseClusterStatsSegmentID("root", "root/cluster_stats/10/20/30/attempt/0.keys")
	require.NoError(t, err)
	require.EqualValues(t, 30, id)
	_, err = parseClusterStatsSegmentID("root", "other/cluster_stats/10/20/30/attempt/0.keys")
	require.Error(t, err)
}

func TestClusterSortSubmitPerSegmentAndRetry(t *testing.T) {
	m, _, _ := clusterSortMetaFixture(t)
	alloc := allocator.NewMockAllocator(t)
	inspector := NewMockCompactionInspector(t)
	manager := &CompactionTriggerManager{meta: m, allocator: alloc, inspector: inspector}
	m.compactionTaskMeta.compactionTasks[99] = make(map[int64]*datapb.CompactionTask)
	next := int64(200)
	alloc.EXPECT().AllocN(int64(2)).RunAndReturn(func(int64) (int64, int64, error) {
		start := next
		next += 2
		return start, next, nil
	}).Times(3)
	var submitted []*datapb.CompactionTask
	inspector.EXPECT().enqueueCompaction(mock.Anything).RunAndReturn(func(task *datapb.CompactionTask) error {
		require.Len(t, task.InputSegments, 1)
		require.EqualValues(t, 1, task.PreAllocatedSegmentIDs.End-task.PreAllocatedSegmentIDs.Begin)
		require.EqualValues(t, 4, task.TotalRows)
		require.EqualValues(t, 0, task.ClusterSortGroupId)
		require.EqualValues(t, 99, task.TriggerID)
		submitted = append(submitted, task)
		m.compactionTaskMeta.compactionTasks[99][task.PlanID] = task
		return nil
	}).Times(3)
	for _, id := range []int64{1, 2} {
		require.NoError(t, manager.submitClusterSort(context.Background(), m.segments.GetSegment(id)))
	}
	require.Equal(t, []int64{1}, submitted[0].InputSegments)
	require.Equal(t, []int64{2}, submitted[1].InputSegments)
	// Neither the sibling's active task nor its completed receipt suppresses
	// retrying segment 1; the sibling itself must never be submitted twice.
	submitted[0].State = datapb.CompactionTaskState_cleaned
	submitted[1].ClusterSortCompleted = true
	require.NoError(t, manager.submitClusterSort(context.Background(), m.segments.GetSegment(1)))
	require.EqualValues(t, 2, submitted[2].RetryTimes)
	require.Equal(t, []int64{1}, submitted[2].InputSegments)
	require.NoError(t, manager.submitClusterSort(context.Background(), m.segments.GetSegment(1)))
	require.NoError(t, manager.submitClusterSort(context.Background(), m.segments.GetSegment(2)))
}

func TestClusterSortRejectsBoundaryChanges(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*datapb.CompactionTask, *datapb.CompactionPlanResult)
	}{
		{"multiple inputs", func(task *datapb.CompactionTask, _ *datapb.CompactionPlanResult) { task.InputSegments = []int64{1, 2} }},
		{"multiple outputs", func(_ *datapb.CompactionTask, result *datapb.CompactionPlanResult) {
			result.Segments = append(result.Segments, proto.Clone(result.Segments[0]).(*datapb.CompactionSegment))
		}},
		{"multiple allocated IDs", func(task *datapb.CompactionTask, _ *datapb.CompactionPlanResult) { task.PreAllocatedSegmentIDs.End++ }},
		{"changed group", func(_ *datapb.CompactionTask, result *datapb.CompactionPlanResult) {
			result.Segments[0].ClusterStats.GroupId++
		}},
		{"changed centroid", func(_ *datapb.CompactionTask, result *datapb.CompactionPlanResult) {
			result.Segments[0].ClusterStats.CentroidIds = []uint32{1}
		}},
		{"changed field", func(_ *datapb.CompactionTask, result *datapb.CompactionPlanResult) {
			result.Segments[0].ClusterStats.FieldId++
		}},
		{"wrong output ID", func(_ *datapb.CompactionTask, result *datapb.CompactionPlanResult) { result.Segments[0].SegmentID++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, task, result := clusterSortMetaFixture(t)
			test.mutate(task, result)
			_, _, err := m.CompleteCompactionMutation(context.Background(), task, result)
			require.Error(t, err)
			for _, id := range []int64{1, 2} {
				require.Equal(t, commonpb.SegmentState_Flushed, m.segments.GetSegment(id).GetState())
			}
			require.Nil(t, m.segments.GetSegment(200))
			m.catalog.(*mocks.DataCoordCatalog).AssertNotCalled(t, "Update", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

func TestClusterParentWaitsForEverySegmentInSameGroup(t *testing.T) {
	m := NewMockCompactionMeta(t)
	parent := &datapb.CompactionTask{PlanID: 99, State: datapb.CompactionTaskState_statistic, TmpSegments: []int64{1, 2}, ClusteringKeyField: &schemapb.FieldSchema{FieldID: 103}}
	for _, id := range []int64{1, 2} {
		s := NewSegmentInfo(&datapb.SegmentInfo{ID: id, ClusterStats: &datapb.ClusterStats{Version: 1, ClusteringTaskId: 99, FieldId: 103, GroupId: 0}})
		m.EXPECT().GetSegment(mock.Anything, id).Return(s)
	}
	children := []*datapb.CompactionTask{{PlanID: 100, Type: datapb.CompactionType_ClusterSortCompaction, InputSegments: []int64{1}, ClusterSortGroupId: 0, ClusterSortCompleted: true}}
	m.EXPECT().GetCompactionTasksByTriggerID(mock.Anything, int64(99)).RunAndReturn(func(context.Context, int64) []*datapb.CompactionTask { return children })
	task := newClusteringCompactionTask(parent, nil, m, nil, nil, nil)
	require.NoError(t, task.processClusterSort())
	require.Equal(t, datapb.CompactionTaskState_statistic, task.GetTaskProto().State)
	children = append(children, &datapb.CompactionTask{PlanID: 101, Type: datapb.CompactionType_ClusterSortCompaction, InputSegments: []int64{2}, ClusterSortGroupId: 0, State: datapb.CompactionTaskState_cleaned, RetryTimes: maxClusterSortAttempts, FailReason: "remote read failed"})
	require.ErrorContains(t, task.processClusterSort(), "exhausted retries")
}

func TestClusterParentFinishesEverySegmentAndReplaysSnapshot(t *testing.T) {
	ctx := context.Background()
	m := NewMockCompactionMeta(t)
	parent := &datapb.CompactionTask{
		PlanID: 99, CollectionID: 10, PartitionID: 20, Channel: t.Name(),
		State: datapb.CompactionTaskState_statistic, TmpSegments: []int64{1, 2, 3}, ClusteringKeyField: &schemapb.FieldSchema{FieldID: 103},
	}
	snapshot := &storage.PartitionStatsSnapshot{Version: 99, SegmentStats: make(map[int64]storage.SegmentStats)}
	values := []storage.VectorFieldValue{storage.NewFloatVectorFieldValue([]float32{2}), storage.NewFloatVectorFieldValue([]float32{0})}
	var children []*datapb.CompactionTask
	for _, id := range parent.TmpSegments {
		input := NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 10, PartitionID: 20, NumOfRows: 4,
			ClusterStats: &datapb.ClusterStats{Version: 1, ClusteringTaskId: 99, FieldId: 103, GroupId: 0, NumRows: 4, CentroidIds: []uint32{2, 0}},
		})
		m.EXPECT().GetSegment(mock.Anything, id).Return(input)
		snapshot.SegmentStats[id] = storage.SegmentStats{NumRows: 4, FieldStats: []storage.FieldStats{{FieldID: 103, Type: schemapb.DataType_FloatVector, Centroids: values}}}
		child := &datapb.CompactionTask{
			PlanID: id + 100, Type: datapb.CompactionType_ClusterSortCompaction,
			InputSegments: []int64{id}, ClusterSortGroupId: 0, ClusterSortCompleted: true,
		}
		if id != 3 { // The third segment was entirely filtered, but is complete.
			child.ResultSegments = []int64{id + 200}
			output := NewSegmentInfo(&datapb.SegmentInfo{
				ID: id + 200, NumOfRows: 3,
				ClusterStats: &datapb.ClusterStats{Version: 1, Sorted: true, ClusteringTaskId: 99, FieldId: 103, GroupId: 0, NumRows: 3, CentroidIds: []uint32{uint32((id - 1) * 2)}},
			})
			m.EXPECT().GetHealthySegment(mock.Anything, id+200).Return(output)
			m.EXPECT().GetSegment(mock.Anything, id+200).Return(output)
		}
		children = append(children, child)
	}
	m.EXPECT().GetCompactionTasksByTriggerID(mock.Anything, int64(99)).Return(children)
	cli, err := storage.NewChunkManagerFactoryWithParam(Params).NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)
	file := path.Join(cli.RootPath(), common.PartitionStatsPath, metautil.JoinIDPath(parent.CollectionID, parent.PartitionID), parent.Channel, strconv.FormatInt(parent.PlanID, 10))
	t.Cleanup(func() { require.NoError(t, cli.Remove(context.Background(), file)) })
	payload, err := storage.SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	require.NoError(t, cli.Write(ctx, file, payload))
	interrupted := false
	m.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, p *datapb.CompactionTask) error {
		if p.State == datapb.CompactionTaskState_indexing && !interrupted {
			interrupted = true
			return errors.New("state save interrupted after snapshot write")
		}
		return nil
	})
	task := newClusteringCompactionTask(parent, nil, m, nil, nil, nil)
	require.ErrorContains(t, task.processClusterSort(), "interrupted")
	require.Equal(t, datapb.CompactionTaskState_statistic, task.GetTaskProto().State)
	require.NoError(t, task.processClusterSort())
	require.Equal(t, datapb.CompactionTaskState_indexing, task.GetTaskProto().State)
	require.Equal(t, []int64{201, 202}, task.GetTaskProto().ResultSegments)
	payload, err = cli.Read(ctx, file)
	require.NoError(t, err)
	updated, err := storage.DeserializePartitionsStatsSnapshot(payload)
	require.NoError(t, err)
	require.Len(t, updated.SegmentStats, 2)
	require.Equal(t, []storage.VectorFieldValue{values[1]}, updated.SegmentStats[201].FieldStats[0].Centroids)
	require.Equal(t, []storage.VectorFieldValue{values[0]}, updated.SegmentStats[202].FieldStats[0].Centroids)
}
