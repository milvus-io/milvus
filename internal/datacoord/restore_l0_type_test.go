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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreL0ReloadUsesJobAndOrdinaryWorkerPlan(t *testing.T) {
	mockey.PatchConvey("restored task keeps its mode and derives targets from the restored job", t, func() {
		paramtable.Init()
		ctx := t.Context()
		copies, m := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2).(*copySegmentTask))
		job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
		job.CollectionId = 200
		job.CreatesCollection = true
		job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}, {TargetSegmentId: 2}}
		require.NoError(t, copies.AddJob(ctx, job))
		for _, info := range []*datapb.SegmentInfo{
			{ID: 1, Level: datapb.SegmentLevel_L0},
			{ID: 2, Level: datapb.SegmentLevel_L1},
			{ID: 999, Level: datapb.SegmentLevel_L1}, // Same channel, outside this job.
		} {
			info.CollectionID, info.PartitionID, info.InsertChannel = 200, 10, "v1"
			info.State, info.IsImporting = commonpb.SegmentState_Flushed, true
			m.segments.SetSegment(info.ID, NewSegmentInfo(info))
		}
		var err error
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, m.catalog)
		require.NoError(t, err)
		plan := &datapb.CompactionTask{
			PlanID: 77, TriggerID: 100, CollectionID: 200, PartitionID: 10, Channel: "v1",
			Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: datapb.CompactionTaskState_executing,
			NodeID: 19, InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Name: "restored"},
		}
		require.NoError(t, m.SaveCompactionTask(ctx, plan))
		copies, err = NewCopySegmentMeta(ctx, m.catalog, m, nil, nil)
		require.NoError(t, err)
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, m.catalog)
		require.NoError(t, err)
		loaded := m.GetCompactionTasksByTriggerID(ctx, 100)[0]
		require.Equal(t, datapb.CompactionTaskState_executing, loaded.GetState(), "L0 reload does not require preallocated segment IDs")
		inspector := newCompactionInspector(m, nil, nil, dctask.NewGlobalTaskScheduler(ctx, nil), nil, nil)
		inspector.restoreJobs = copies
		inspector.loadMeta()
		compaction := inspector.getCompactionTask(77)
		require.NotNil(t, compaction)
		require.True(t, L0CompactionCompactionTaskFilter()(compaction))
		require.Equal(t, 1, LevelPrioritizer(compaction))
		require.Equal(t, 10, MixFirstPrioritizer(compaction))
		require.Equal(t, maxCompactionTaskExecutionDuration[datapb.CompactionType_Level0DeleteCompaction], maxCompactionTaskExecutionDuration[plan.GetType()])
		mockey.Mock(PreAllocateBinlogIDs).Return(&datapb.IDRange{Begin: 1000, End: 1100}, nil).Build()
		request, err := compaction.BuildCompactionRequest()
		require.NoError(t, err)
		require.Equal(t, datapb.CompactionType_Level0DeleteCompaction, request.GetType())
		require.Equal(t, datapb.CompactionType_RestoreLevel0DeleteCompaction, compaction.GetTaskProto().GetType())
		require.Len(t, request.GetSegmentBinlogs(), 2)
		require.EqualValues(t, 1, request.GetSegmentBinlogs()[0].GetSegmentID())
		require.EqualValues(t, 2, request.GetSegmentBinlogs()[1].GetSegmentID())
		require.Equal(t, commonpb.CompactionType_CompactionTypeLevel0Delete, getCompactionMergeInfo(plan).GetType())
	})
}

func TestRestoreL0SharesChannelExclusion(t *testing.T) {
	for _, restoreExecuting := range []bool{false, true} {
		t.Run(map[bool]string{false: "restore waits for mix", true: "mix waits for restore"}[restoreExecuting], func(t *testing.T) {
			paramtable.Init()
			restore := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Channel: "v1", Type: datapb.CompactionType_RestoreLevel0DeleteCompaction}, nil, nil)
			mix := newMixCompactionTask(&datapb.CompactionTask{PlanID: 2, Channel: "v1", Type: datapb.CompactionType_MixCompaction}, nil, nil, nil)
			var running, queued CompactionTask = mix, restore
			if restoreExecuting {
				running, queued = restore, mix
			}
			inspector := newCompactionInspector(nil, nil, nil, dctask.NewGlobalTaskScheduler(t.Context(), nil), nil, nil)
			inspector.executingTasks[running.GetTaskID()] = running
			require.NoError(t, inspector.queueTasks.Enqueue(queued))
			require.Empty(t, inspector.schedule())
			require.Equal(t, 1, inspector.queueTasks.Len())
			delete(inspector.executingTasks, running.GetTaskID())
			require.Equal(t, []CompactionTask{queued}, inspector.schedule())
		})
	}
}

func TestRestoreL0DerivesEmptyTargetSet(t *testing.T) {
	paramtable.Init()
	ctx := t.Context()
	copies, m := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 1).(*copySegmentTask))
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
	job.CollectionId = 200
	job.CreatesCollection = true
	job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
	require.NoError(t, copies.AddJob(ctx, job))
	m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 200, InsertChannel: "v1", PartitionID: 10,
		Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, IsImporting: true,
	}))
	var err error
	m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, m.catalog)
	require.NoError(t, err)
	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID: 77, TriggerID: 100, CollectionID: 200, PartitionID: 10, Channel: "v1",
		Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: datapb.CompactionTaskState_pipelining,
		InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Name: "restored"},
	}, nil, m)
	task.restoreJobs = copies
	task.CreateTaskOnWorker(19, nil) // Empty targets complete without a worker RPC.
	require.Equal(t, datapb.CompactionTaskState_meta_saved, task.GetTaskProto().GetState())
	require.True(t, task.Process())
	require.Equal(t, datapb.CompactionTaskState_completed, task.GetTaskProto().GetTerminalState())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 1).GetState())
	require.True(t, m.GetSegment(ctx, 1).GetCompacted())
}
