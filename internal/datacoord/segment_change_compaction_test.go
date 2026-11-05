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
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newSegmentChangeTaskMeta(t *testing.T, m *meta.Meta) *ctask.CompactionTaskMeta {
	t.Helper()
	taskMeta, err := ctask.NewCompactionTaskMeta(t.Context(), m.Catalog)
	require.NoError(t, err)
	return taskMeta
}

func setSegmentChangeTestParam(t *testing.T, item *paramtable.ParamItem, value string) {
	t.Helper()
	original := item.GetValue()
	require.NoError(t, Params.Save(item.Key, value))
	t.Cleanup(func() { require.NoError(t, Params.Save(item.Key, original)) })
}

func seedMixProducer(t *testing.T) (*meta.Meta, *datapb.CompactionTask, *datapb.CompactionPlanResult) {
	t.Helper()
	m, err := meta.NewMemoryMeta(t)
	require.NoError(t, err)
	schema := readinessVectorSchema()
	m.AddCollection(&meta.CollectionInfo{ID: 10, Schema: schema, VChannelNames: []string{"ch-1"}})
	for _, id := range []int64{2001, 2002} {
		require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			NumOfRows: 42, IsSorted: true, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
		})))
	}
	m.SetSegmentsCompacting(t.Context(), []int64{2001, 2002}, true)
	manager := dataview.NewManager(m.Catalog.(dataview.Catalog), nil)
	m.DataViewManager = manager
	_, err = manager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 10, VChannels: []string{"ch-1"}})
	require.NoError(t, err)
	_, err = manager.RecomputeNow(t.Context(), 10, m.LoadableProjection)
	require.NoError(t, err)
	task := &datapb.CompactionTask{
		PlanID: 5000, CollectionID: 10, PartitionID: 100, Channel: "ch-1",
		InputSegments: []int64{2001, 2002}, Type: datapb.CompactionType_MixCompaction,
		State: datapb.CompactionTaskState_executing, Schema: schema,
	}
	result := &datapb.CompactionPlanResult{
		PlanID: 5000, State: datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{{
			SegmentID: 1001, NumOfRows: 84, IsSorted: true,
			InsertLogs: []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: 11, EntriesNum: 84}}}},
		}},
	}
	return m, task, result
}

func segmentChangeViewIDs(t *testing.T, m *meta.Meta) []int64 {
	t.Helper()
	ref, err := m.DataViewManager.Latest(t.Context(), 10)
	require.NoError(t, err)
	defer ref.Deref()
	var ids []int64
	for _, shard := range ref.DataView().GetShards() {
		for _, part := range shard.GetPartitions() {
			ids = append(ids, part.GetSegmentIds()...)
		}
	}
	return ids
}

func TestSegmentChangeRetainsL0WhilePublishing(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
	})))
	l0 := &datapb.CompactionTask{
		PlanID: 6000, CollectionID: 10, PartitionID: 100, Channel: "ch-1",
		InputSegments: []int64{3001}, Type: datapb.CompactionType_Level0DeleteCompaction, State: datapb.CompactionTaskState_pipelining,
	}
	ci := compaction.NewSegmentChangeTestInspector(t, m, meta.NewMockAllocator(t))
	queued, err := ci.CreateCompactTaskForTest(l0)
	require.NoError(t, err)
	require.NoError(t, ci.EnqueueForTest(queued))
	_, err = m.CompleteCompactionMutation(t.Context(), task, result)
	require.NoError(t, err)
	// Opt-out must still protect the already admitted group.
	require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
	_, err = ci.CreateCompactTaskForTest(l0)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, ci.ScheduleForTest())
	require.Zero(t, ci.QueueLenForTest(), "deferred L0 must not consume the inner sort planning budget")
	require.True(t, ci.CleaningForTest(6000))
	ci.CleanFailedForTest()
	require.False(t, ci.CleaningForTest(6000))
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 3001).GetState())
	require.False(t, m.GetSegment(t.Context(), 3001).IsCompacting)
	require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	inspector := newSegmentChangeInspector(t.Context(), m, meta.NewMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	_, err = ci.CreateCompactTaskForTest(l0)
	require.NoError(t, err, "L0 may be replanned once outputs are published")
}

func TestSegmentChangeRetainsL0DuringCopyAdmission(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, _ := seedMixProducer(t)
	copyMeta, err := copysegment.NewCopySegmentMeta(t.Context(), m.Catalog, m, nil, nil, nil)
	require.NoError(t, err)
	job := copysegment.NewTestCopyJob(8000, datapb.CopySegmentJobState_CopySegmentJobPending).(*copysegment.Job)
	job.CollectionId = 10
	require.NoError(t, copyMeta.AddJob(t.Context(), job))
	ci := compaction.NewSegmentChangeTestInspector(t, m, meta.NewMockAllocator(t))
	ci.SetCopySegmentMeta(copyMeta)
	require.True(t, ci.L0ExclusionsForTest().Contain("ch-1"))
	job.State = datapb.CopySegmentJobState_CopySegmentJobCompleted
	require.False(t, ci.L0ExclusionsForTest().Contain("ch-1"))
}

func TestSegmentChangeMixProducerOptIn(t *testing.T) {
	for _, scenario := range []string{"disabled", "enabled"} {
		t.Run(scenario, func(t *testing.T) {
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableCompaction, "true")
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.EnableSortCompaction, "true")
			m, task, result := seedMixProducer(t)
			if scenario == "disabled" {
				require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
			}
			if scenario == "unsorted_sort_chain" {
				result.Segments[0].IsSorted = false
			}
			_, err := m.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err)
			g, err := m.GetCompactionChangeGroup(task)
			require.NoError(t, err)
			if scenario != "enabled" {
				require.Nil(t, g)
				require.False(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
				require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(t.Context(), 2001).GetState())
				return
			}
			require.NotNil(t, g)
			require.True(t, m.GetSegment(t.Context(), 1001).GetIsInvisible())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
			require.ElementsMatch(t, []int64{2001, 2002}, segmentChangeViewIDs(t, m))
			_, allowed := m.CheckAndSetSegmentsCompacting(t.Context(), task.InputSegments)
			require.False(t, allowed)
			// Recompute while indexes/stats build must retain the original snapshot.
			_, err = m.DataViewManager.RecomputeNow(t.Context(), 10, m.LoadableProjection)
			require.NoError(t, err)
			require.ElementsMatch(t, []int64{2001, 2002}, segmentChangeViewIDs(t, m))
			inspector := newSegmentChangeInspector(t.Context(), m, meta.NewMockAllocator(t))
			inspector.checkAllGroups(t.Context())
			inspector.checkAllGroups(t.Context())
			require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
			require.Equal(t, []int64{1001}, segmentChangeViewIDs(t, m))
			require.True(t, m.GetSegment(t.Context(), 2001).GetCompacted())
			require.Zero(t, m.GetSegment(t.Context(), 1001).GetCommitTimestamp())
		})
	}
}

func TestSegmentChangeMixTaskRecoveryAndFlagOff(t *testing.T) {
	for _, shape := range []string{"nonempty", "empty", "no_outputs"} {
		t.Run(shape, func(t *testing.T) {
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
			m, task, result := seedMixProducer(t)
			switch shape {
			case "empty":
				result.Segments[0].NumOfRows = 0
				result.Segments[0].InsertLogs = nil
			case "no_outputs":
				result.Segments = nil
			}
			_, err := m.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err)
			// Simulate a crash before saving meta_saved: recovery has only the
			// executing task and the durable output/group transaction.
			taskMeta, err := ctask.NewCompactionTaskMeta(t.Context(), m.Catalog)
			require.NoError(t, err)
			require.NoError(t, taskMeta.SaveCompactionTask(t.Context(), task))
			require.NoError(t, Params.Save(Params.DataCoordCfg.SegmentChangeEnabled.Key, "false"))
			recovered := reloadSegmentChangeMeta(t, m)
			ci := compaction.NewSegmentChangeTestInspector(t, recovered, meta.NewMockAllocator(t))
			restored, err := ci.CreateCompactTaskForTest(task)
			require.NoError(t, err)
			require.Equal(t, datapb.CompactionTaskState_meta_saved, restored.GetTaskProto().GetState())
			require.Equal(t, taskcommon.Finished, restored.GetTaskState(), "readiness must not occupy a worker slot")
			require.False(t, restored.NeedReAssignNodeID())
			mt := restored.(*ctask.MixCompactionTask)
			// No worker calls are expected, even when its old result is gone.
			mt.QueryTaskOnWorker(session.NewMockCluster(t))
			require.False(t, mt.Process())
			_, err = recovered.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err)
			inspector := newSegmentChangeInspector(t.Context(), recovered, meta.NewMockAllocator(t))
			inspector.checkAllGroups(t.Context())
			inspector.checkAllGroups(t.Context())
			require.True(t, mt.Process())
			require.Equal(t, datapb.CompactionTaskState_completed, mt.GetTaskProto().GetState())
			if shape == "empty" {
				require.NoError(t, recovered.DropSegment(t.Context(), 1001))
			}
			_, err = recovered.CompleteCompactionMutation(t.Context(), task, result)
			require.NoError(t, err, "published output GC must not invalidate the durable task identity")
			require.True(t, mt.Clean())
			require.Equal(t, datapb.CompactionTaskState_completed, mt.GetTaskProto().GetTerminalState())
		})
	}
}

type segmentChangeProducerCatalog struct {
	metastore.DataCoordCatalog
	beforeRetry func()
	writes      int
}

func (c *segmentChangeProducerCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	c.writes++
	if c.writes == 1 {
		c.beforeRetry()
		return merr.WrapErrServiceUnavailableMsg("injected producer catalog failure")
	}
	return c.DataCoordCatalog.Update(ctx, actions...)
}

func TestSegmentChangeMixProducerFailureAndReadiness(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	catalog := &segmentChangeProducerCatalog{DataCoordCatalog: m.Catalog, beforeRetry: func() {
		// The composite retry holds segMu; inspect the in-memory store directly.
		require.Nil(t, m.Segments.GetSegment(1001))
		require.Nil(t, m.SegmentChangeGroupLockedForTest(5000))
		require.Equal(t, commonpb.SegmentState_Flushed, m.Segments.GetSegment(2001).GetState())
	}}
	m.Catalog = catalog
	m.GetCollection(10).Schema = readinessStatsSchema()
	task.Schema = m.GetCollection(10).Schema
	mt := ctask.NewMixCompactionTask(task, meta.NewMockAllocator(t), m, newSegmentChangeTaskMeta(t, m), index.NewTestVersionManager())
	require.NoError(t, mt.SaveSegmentMeta(result))
	require.Equal(t, 2, catalog.writes)
	inspector := newSegmentChangeInspector(t.Context(), m, meta.NewMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State,
		"missing text/JSON artifacts must retain the inputs")
	require.False(t, mt.Process())
	// Explicit cancellation reclaims only unpublished outputs and releases parents.
	require.True(t, mt.Clean())
	require.Equal(t, model.SegmentChangeStateAborted, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(t.Context(), 1001).GetState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	require.False(t, m.GetSegment(t.Context(), 2001).GetCompacted())
	_, allowed := m.CheckAndSetSegmentsCompacting(t.Context(), task.InputSegments)
	require.True(t, allowed)
}

func TestSegmentChangeMixPublishSnapshotProtection(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	_, err := m.CompleteCompactionMutation(t.Context(), task, result)
	require.NoError(t, err)
	inspector := newSegmentChangeInspector(t.Context(), m, meta.NewMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	protection := snapshot.CreateTestSnapshotMetaLoaded(t)
	m.CompactionProtection = protection
	protection.SetSnapshotPending(10)
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.ElementsMatch(t, []int64{2001, 2002}, segmentChangeViewIDs(t, m))
	protection.ClearSnapshotPending(10)
	inspector.checkAllGroups(t.Context())
	inspector.checkAllGroups(t.Context())
	require.Equal(t, []int64{1001}, segmentChangeViewIDs(t, m))
}

func TestSegmentChangeMixWaitsForIndexAndPropagatesFailure(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex, "1")
	m, task, result := seedMixProducer(t)
	m.IndexCatalog = newReadinessIndexMeta(10,
		map[int64]*model.Index{7: {CollectionID: 10, FieldID: 2, IndexID: 7}}, nil)
	mt := ctask.NewMixCompactionTask(task, meta.NewMockAllocator(t), m, newSegmentChangeTaskMeta(t, m), index.NewTestVersionManager())
	require.NoError(t, mt.SaveSegmentMeta(result))
	inspector := newSegmentChangeInspector(t.Context(), m, meta.NewMockAllocator(t))
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateStaged, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.False(t, mt.Process())
	require.Contains(t, index.UnindexedSegmentsForTest(t.Context(), m, m.IndexCatalog.(*index.IndexMeta)), int64(1001), "staged output must remain eligible for index construction")
	require.ElementsMatch(t, []int64{2001, 2002}, segmentChangeViewIDs(t, m))
	// A nonempty member dropped before admission cannot replace its parents.
	require.NoError(t, m.UpdateSegmentsInfo(t.Context(), meta.UpdateStatusOperator(1001, commonpb.SegmentState_Dropped)))
	inspector.checkAllGroups(t.Context())
	require.Equal(t, model.SegmentChangeStateFailed, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.True(t, mt.Process())
	require.Equal(t, datapb.CompactionTaskState_failed, mt.GetTaskProto().GetState())
	require.True(t, mt.Clean())
	require.Equal(t, datapb.CompactionTaskState_failed, mt.GetTaskProto().GetTerminalState())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	_, allowed := m.CheckAndSetSegmentsCompacting(t.Context(), task.InputSegments)
	require.True(t, allowed)
}

func TestSegmentChangeMixRecoveryPreservesFailedTask(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, task, result := seedMixProducer(t)
	_, err := m.CompleteCompactionMutation(t.Context(), task, result)
	require.NoError(t, err)
	task.State = datapb.CompactionTaskState_failed
	ci := compaction.NewSegmentChangeTestInspector(t, m, meta.NewMockAllocator(t))
	restored, err := ci.CreateCompactTaskForTest(task)
	require.NoError(t, err)
	require.Equal(t, datapb.CompactionTaskState_failed, restored.GetTaskProto().GetState())
	require.True(t, restored.Process())
	require.True(t, restored.Clean())
	require.Equal(t, model.SegmentChangeStateAborted, m.GetSegmentChangeGroup(t.Context(), 10, 5000).State)
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	require.Equal(t, datapb.CompactionTaskState_failed, restored.GetTaskProto().GetTerminalState())
}
