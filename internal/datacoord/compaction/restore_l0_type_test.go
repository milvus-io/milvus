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

package compaction

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	dctask "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreL0ReloadUsesJobAndOrdinaryWorkerPlan(t *testing.T) {
	mockey.PatchConvey("restored task keeps its mode and derives targets from the restored job", t, func() {
		paramtable.Init()
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		m := &meta.Meta{Ctx: ctx, Catalog: catalog, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
		job := &datapb.CopySegmentJob{JobId: 100, State: datapb.CopySegmentJobState_CopySegmentJobExecuting}
		job.CollectionId = 200
		job.CreatesCollection = true
		job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}, {TargetSegmentId: 2}}
		require.NoError(t, catalog.SaveCopySegmentJob(ctx, job))
		for _, info := range []*datapb.SegmentInfo{
			{ID: 1, Level: datapb.SegmentLevel_L0},
			{ID: 2, Level: datapb.SegmentLevel_L1},
			{ID: 999, Level: datapb.SegmentLevel_L1}, // Same channel, outside this job.
		} {
			info.CollectionID, info.PartitionID, info.InsertChannel = 200, 10, "v1"
			info.State, info.IsImporting = commonpb.SegmentState_Flushed, true
			m.Segments.SetSegment(info.ID, meta.NewSegmentInfo(info))
		}
		tasks, err := ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		plan := &datapb.CompactionTask{
			PlanID: 77, TriggerID: 100, CollectionID: 200, PartitionID: 10, Channel: "v1",
			Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: datapb.CompactionTaskState_executing,
			NodeID: 19, InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Name: "restored"},
		}
		require.NoError(t, tasks.SaveCompactionTask(ctx, plan))
		jobs, err := catalog.ListCopySegmentJobs(ctx)
		require.NoError(t, err)
		tasks, err = ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		loaded := tasks.GetCompactionTasksByTriggerID(100)[0]
		require.Equal(t, datapb.CompactionTaskState_executing, loaded.GetState(), "L0 reload does not require preallocated segment IDs")
		inspector := NewCompactionInspector(m, tasks, nil, nil, nil, nil, nil, nil, nil, dctask.NewGlobalTaskScheduler(ctx, nil), nil, nil)
		inspector.LookupRestoreJob = func(_ context.Context, id int64) *datapb.CopySegmentJob {
			require.EqualValues(t, 100, id)
			require.Len(t, jobs, 1)
			return jobs[0]
		}
		inspector.LoadMeta()
		compaction := inspector.getCompactionTask(77)
		require.NotNil(t, compaction)
		require.True(t, L0CompactionCompactionTaskFilter()(compaction))
		require.Equal(t, 1, LevelPrioritizer(compaction))
		require.Equal(t, 10, MixFirstPrioritizer(compaction))
		require.Equal(t, maxCompactionTaskExecutionDuration[datapb.CompactionType_Level0DeleteCompaction], maxCompactionTaskExecutionDuration[plan.GetType()])
		mockey.Mock(ctask.PreAllocateBinlogIDs).Return(&datapb.IDRange{Begin: 1000, End: 1100}, nil).Build()
		request, err := compaction.BuildCompactionRequest()
		require.NoError(t, err)
		require.Equal(t, datapb.CompactionType_Level0DeleteCompaction, request.GetType())
		require.Equal(t, datapb.CompactionType_RestoreLevel0DeleteCompaction, compaction.GetTaskProto().GetType())
		require.Len(t, request.GetSegmentBinlogs(), 2)
		require.EqualValues(t, 1, request.GetSegmentBinlogs()[0].GetSegmentID())
		require.EqualValues(t, 2, request.GetSegmentBinlogs()[1].GetSegmentID())
		require.Equal(t, commonpb.CompactionType_CompactionTypeLevel0Delete, policy.GetCompactionMergeInfo(plan).GetType())
	})
}

func TestRestoreL0SharesChannelExclusion(t *testing.T) {
	for _, restoreExecuting := range []bool{false, true} {
		t.Run(map[bool]string{false: "restore waits for mix", true: "mix waits for restore"}[restoreExecuting], func(t *testing.T) {
			paramtable.Init()
			restore := ctask.NewL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Channel: "v1", Type: datapb.CompactionType_RestoreLevel0DeleteCompaction}, nil, nil, nil)
			mix := ctask.NewMixCompactionTask(&datapb.CompactionTask{PlanID: 2, Channel: "v1", Type: datapb.CompactionType_MixCompaction}, nil, nil, nil, nil)
			var running, queued ctask.CompactionTask = mix, restore
			if restoreExecuting {
				running, queued = restore, mix
			}
			inspector := NewCompactionInspector(nil, nil, nil, nil, nil, nil, nil, nil, nil, dctask.NewGlobalTaskScheduler(t.Context(), nil), nil, nil)
			inspector.executingTasks[running.GetTaskID()] = running
			require.NoError(t, inspector.queueTasks.Enqueue(queued))
			require.Empty(t, inspector.schedule())
			require.Equal(t, 1, inspector.queueTasks.Len())
			delete(inspector.executingTasks, running.GetTaskID())
			require.Equal(t, []ctask.CompactionTask{queued}, inspector.schedule())
		})
	}
}

func TestRestoreL0DerivesEmptyTargetSet(t *testing.T) {
	paramtable.Init()
	ctx := t.Context()
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	store := metacache.NewMetaStore(nil)
	m := &meta.Meta{Ctx: ctx, Catalog: catalog, MetaStore: store, Segments: meta.NewSegmentsInfo(store)}
	job := &datapb.CopySegmentJob{JobId: 100, State: datapb.CopySegmentJobState_CopySegmentJobExecuting}
	job.CollectionId = 200
	job.CreatesCollection = true
	job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
	require.NoError(t, catalog.SaveCopySegmentJob(ctx, job))
	m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 200, InsertChannel: "v1", PartitionID: 10,
		Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, IsImporting: true,
	}))
	tasks, err := ctask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	task := ctask.NewL0CompactionTask(&datapb.CompactionTask{
		PlanID: 77, TriggerID: 100, CollectionID: 200, PartitionID: 10, Channel: "v1",
		Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, State: datapb.CompactionTaskState_pipelining,
		InputSegments: []int64{1}, Schema: &schemapb.CollectionSchema{Name: "restored"},
	}, nil, m, tasks)
	task.LookupRestoreJob = func(_ context.Context, id int64) *datapb.CopySegmentJob { require.EqualValues(t, 100, id); return job }
	task.CreateTaskOnWorker(19, nil) // Empty targets complete without a worker RPC.
	require.Equal(t, datapb.CompactionTaskState_meta_saved, task.GetTaskProto().GetState())
	require.True(t, task.Process())
	require.Equal(t, datapb.CompactionTaskState_completed, task.GetTaskProto().GetTerminalState())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 1).GetState())
	require.True(t, m.GetSegment(ctx, 1).GetCompacted())
}
