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
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// TestQuiesceImportJobKeepsTasksDuringRetention pins that unbound task records
// survive until cleanupTs passes: they back the terminal job's row accounting
// (getImportRowsInfo) during the retention window, like V2 GC.
func TestQuiesceImportJobKeepsTasksDuringRetention(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	log := mlog.With(mlog.FieldJobID(int64(1)))

	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Completed, NodeId: NullNodeID, SegmentId: 100, Rows: 42,
	}, importMeta, nil, nil)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Completed,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	require.False(t, checker.quiesceImportJob(job, log))

	job.CleanupTs = tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()
	require.True(t, checker.quiesceImportJob(job, log))
}

func TestCleanupPreparingV3ImportTasksIsIdempotent(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_None, NodeId: NullNodeID,
	}, importMeta, nil, nil)

	first := importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	second := importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mock.InOrder(first, second)
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()

	require.NoError(t, checker.cleanupPreparingV3ImportTasks(job))
	require.NoError(t, checker.cleanupPreparingV3ImportTasks(job))
}

func TestCalculateV3TaskSlots(t *testing.T) {
	const mib = int64(1024 * 1024)

	require.Equal(t, int64(3), calculateReshardTaskSlot(64*mib, 16*mib, 128*mib, 32*mib, 160*mib))
	// With the parquet read buffer charged and one prefetched batch running
	// ahead of the routing side, a 340 MiB per-slot limit still fits the
	// 416 MiB working set in two slots.
	require.Equal(t, int64(2), calculateReshardTaskSlot(64*mib, 16*mib, 128*mib, 32*mib, 340*mib))
	require.Equal(t, int64(4), calculateV3ImportTaskSlot(16*mib, 32*mib, 160*mib, 16))
	require.Equal(t, int64(1), calculateV3Slots(1, 160*mib))
	require.Equal(t, int64(2), calculateV3Slots(160*mib+1, 160*mib))
	// The slot helper must not divide by zero when the configured per-slot
	// memory limit is invalid; paramtable validation remains the primary guard.
	require.Equal(t, int64(1), calculateV3Slots(1, 0))
	require.Equal(t, int64(1), calculateV3Slots(1, -1))
}

func TestEffectiveImportV3FanIn(t *testing.T) {
	require.Equal(t, 16, effectiveImportV3FanIn(16, 0))
	require.Equal(t, 16, effectiveImportV3FanIn(16, 100))
	require.Equal(t, 5, effectiveImportV3FanIn(16, 5))
	require.Equal(t, 2, effectiveImportV3FanIn(16, 1))
	require.Equal(t, 2, effectiveImportV3FanIn(2, 1))
}

func TestCleanupPreparingV3ImportTasksKeepsReadyTask(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Pending, NodeId: NullNodeID,
	}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()

	require.NoError(t, checker.cleanupPreparingV3ImportTasks(job))
}

func TestCleanupPreparingV3ImportTaskDropsOwnedSegment(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	segment, err := addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Importing, segment.GetState())
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_None, NodeId: NullNodeID, SegmentId: 100,
	}, importMeta, meta, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()

	require.NoError(t, checker.cleanupPreparingV3ImportTasks(job))
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())
}

func TestCreateImportV3TaskPublishesPendingAfterSegments(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	meta.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(int64(1)).Return(int64(200), int64(201), nil).Once()
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta, alloc: alloc}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, DataTs: 1}}

	var task *importTaskV3
	add := importMeta.EXPECT().AddTask(mock.Anything, mock.Anything).Run(func(_ context.Context, added ImportTask) {
		task = added.(*importTaskV3)
		require.Equal(t, datapb.ImportTaskStateV2_None, task.GetState())
		require.Equal(t, int64(1), task.GetTaskSlot())
		require.Nil(t, meta.GetSegment(ctx, 100))
	}).Return(nil).Once()
	update := importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Run(func(_ context.Context, _ int64, actions ...UpdateAction) {
		require.NotNil(t, meta.GetSegment(ctx, 100))
		for _, action := range actions {
			action(task)
		}
	}).Return(nil).Once()
	mock.InOrder(add, update)

	err = checker.createImportV3Task(
		job,
		&datapb.WriterSpec{StorageVersion: 1, SchemaVersion: 3},
		10, 100,
		v3ImportTaskSpec{channel: "v0", partitionID: 4, fragments: []*datapb.FragmentRef{{Path: "f0", RowCount: 1}}, rows: 5},
	)
	require.NoError(t, err)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	p := task.task.Load()
	require.Equal(t, "v0", p.GetVchannel())
	require.Equal(t, int64(4), p.GetPartitionId())
	require.Len(t, p.GetFragments(), 1)
	require.Equal(t, "f0", p.GetFragments()[0].GetPath())
	segment := meta.GetSegment(ctx, 100)
	require.True(t, segment.GetIsImporting())
	require.False(t, segment.GetIsInvisible())
	require.Equal(t, int32(3), segment.GetSchemaVersion())
}

func TestCreateReshardTasksKeepsExistingAndAddsMissingSources(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	missingPath := path.Join(cm.RootPath(), "missing.json")
	require.NoError(t, cm.Write(ctx, missingPath, []byte("{}")))
	importMeta := NewMockImportMeta(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(int64(1)).Return(int64(20), int64(21), nil).Once()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	meta.chunkManager = cm
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta, alloc: alloc}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"existing.json"}}, {Id: 2, Paths: []string{missingPath}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	existing := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, SourceIds: []int64{1},
	}, importMeta, checker.meta, alloc)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{existing}).Once()
	importMeta.EXPECT().AddTask(mock.Anything, mock.Anything).Run(func(_ context.Context, added ImportTask) {
		p := added.(*reshardTask).task.Load()
		require.Equal(t, int64(3), p.GetSlot())
		require.Equal(t, []int64{2}, p.GetSourceIds())
	}).Return(nil).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()

	require.NoError(t, checker.createReshardTasks(job))
}

func TestCreateReshardTasksRetriesJobStateWhenSourcesCovered(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	meta.chunkManager = cm
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
		Files:  []*internalpb.ImportFile{{Id: 1, Paths: []string{"existing.json"}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed, SourceIds: []int64{1}}, importMeta, checker.meta, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()

	require.NoError(t, checker.createReshardTasks(job))
}

func TestBuildReshardTaskPlanDerivesExecutionInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}, {Id: 2, Paths: []string{"b.json"}}},
	}}
	task := &datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, SourceIds: []int64{2}}
	plan, err := buildReshardTaskPlan(job, task)
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.GetCollectionId())
	require.Len(t, plan.GetSources(), 1)
	require.Equal(t, int64(2), plan.GetSources()[0].GetFile().GetId())
	require.Equal(t, []string{"v0"}, plan.GetVchannels())
	require.Equal(t, []int64{3}, plan.GetPartitions())
	require.NotNil(t, plan.GetSchema())
	require.NotNil(t, plan.GetTempSchema())
	require.NotEmpty(t, plan.GetSort().GetFields())
	require.False(t, plan.GetBackup())
	require.Greater(t, plan.GetFragmentSize(), int64(0))

	task.SourceIds = []int64{9}
	_, err = buildReshardTaskPlan(job, task)
	require.Error(t, err)
}

// fragmentSizeInMB is refreshable but validated only at startup; a hot refresh
// to an invalid value must fail plan building loudly instead of reaching the
// DataNode as a zero fragment target (which would flush every non-empty bucket
// after every batch).
func TestBuildReshardTaskPlanRejectsInvalidFragmentSize(t *testing.T) {
	liveKey := paramtable.Get().DataCoordCfg.ImportFragmentSize.Key
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
	}}
	task := &datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, SourceIds: []int64{1}}
	for _, invalid := range []string{"0", "", "not-a-number", "-4"} {
		paramtable.Get().Save(liveKey, invalid)
		_, err := buildReshardTaskPlan(job, task)
		require.Error(t, err, "value=%q", invalid)
		require.Contains(t, err.Error(), "fragmentSizeInMB")
	}
	paramtable.Get().Reset(liveKey)
}

// createReshardTask must reject an invalid live fragment size with a terminal
// error so the checker fails the job instead of reserving a bogus slot.
func TestCreateReshardTaskRejectsInvalidFragmentSize(t *testing.T) {
	liveKey := paramtable.Get().DataCoordCfg.ImportFragmentSize.Key
	paramtable.Get().Save(liveKey, "0")
	defer paramtable.Get().Reset(liveKey)

	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: context.Background(), importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}

	err := checker.createReshardTask(job, 10, []reshardSource{{file: &internalpb.ImportFile{Id: 1}, size: 1}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fragmentSizeInMB")
	require.True(t, importcommon.IsTerminalImportV3Err(err))
}

func TestCreateImportV3TaskRejectsMissingDataTs(t *testing.T) {
	checker := &importCheckerV3{ctx: context.Background()}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}
	err := checker.createImportV3Task(
		job,
		&datapb.WriterSpec{},
		10, 100,
		v3ImportTaskSpec{channel: "v0", partitionID: 4},
	)
	require.Error(t, err)
}

func TestReshardTasksAllCompletedStates(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}

	newTask := func(state datapb.ImportTaskStateV2, taskID int64) *reshardTask {
		return newReshardTask(&datapb.ReshardTask{
			JobId: 1, TaskId: taskID, State: state,
		}, importMeta, nil, nil)
	}

	t.Run("failed task terminates the job", func(t *testing.T) {
		importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
			Return([]ImportTask{newTask(datapb.ImportTaskStateV2_Failed, 10)}).Once()
		completed, err := checker.reshardTasksAllCompleted(job)
		require.False(t, completed)
		require.Error(t, err)
	})

	t.Run("incomplete task waits without reading manifests", func(t *testing.T) {
		// The status-only check must not touch object storage: no chunkManager
		// is wired into the checker, so any manifest read would nil-panic.
		importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
			Return([]ImportTask{
				newTask(datapb.ImportTaskStateV2_Completed, 10),
				newTask(datapb.ImportTaskStateV2_InProgress, 11),
			}).Once()
		completed, err := checker.reshardTasksAllCompleted(job)
		require.False(t, completed)
		require.NoError(t, err)
	})

	t.Run("all completed", func(t *testing.T) {
		importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
			Return([]ImportTask{
				newTask(datapb.ImportTaskStateV2_Completed, 10),
				newTask(datapb.ImportTaskStateV2_Completed, 11),
			}).Once()
		completed, err := checker.reshardTasksAllCompleted(job)
		require.True(t, completed)
		require.NoError(t, err)
	})
}

func TestBuildV3SegmentPlans(t *testing.T) {
	fragments := []v3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 1, path: "f1", rows: 1, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 2, path: "f2", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 3, path: "f3", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 1, partitionIndex: 0, seq: 4, path: "f4", rows: 7, bytes: 1},
	}
	plans := buildV3SegmentPlans(fragments, []string{"v0", "v1"}, []int64{10}, 10)
	require.Len(t, plans, 3)
	require.Equal(t, "v0", plans[0].channel)
	require.Equal(t, []string{"f1", "f2"}, []string{plans[0].fragments[0].GetPath(), plans[0].fragments[1].GetPath()})
	require.Equal(t, int64(101), plans[0].rows)
	require.Equal(t, "v0", plans[1].channel)
	require.Equal(t, []string{"f3"}, []string{plans[1].fragments[0].GetPath()})
	require.Equal(t, int64(100), plans[1].rows)
	require.Equal(t, "v1", plans[2].channel)
	require.Equal(t, int64(7), plans[2].rows)
}

// TestBuildV3SegmentPlansPacksDefaultFragmentTarget pins the documented
// premise that one segment plan holds about 8 default-sized fragments: eight
// 128MiB fragments fit a 1GiB target in one plan, and the ninth fragment
// opens a second plan. A single oversized fragment still owns a plan alone.
func TestBuildV3SegmentPlansPacksDefaultFragmentTarget(t *testing.T) {
	const mib = int64(1024 * 1024)
	newFragments := func(n int) []v3PlanningFragment {
		fragments := make([]v3PlanningFragment, 0, n)
		for i := 0; i < n; i++ {
			fragments = append(fragments, v3PlanningFragment{
				sourceID: 1, channelIndex: 0, partitionIndex: 0,
				seq: int64(i), path: fmt.Sprintf("f%d", i), rows: 100, bytes: 128 * mib,
			})
		}
		return fragments
	}
	plans := buildV3SegmentPlans(newFragments(8), []string{"v0"}, []int64{10}, 1024*mib)
	require.Len(t, plans, 1)
	require.Len(t, plans[0].fragments, 8)

	plans = buildV3SegmentPlans(newFragments(9), []string{"v0"}, []int64{10}, 1024*mib)
	require.Len(t, plans, 2)
	require.Len(t, plans[0].fragments, 8)
	require.Len(t, plans[1].fragments, 1)

	oversized := []v3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 0, path: "big", rows: 1, bytes: 2 * 1024 * mib},
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 1, path: "small", rows: 1, bytes: 128 * mib},
	}
	plans = buildV3SegmentPlans(oversized, []string{"v0"}, []int64{10}, 1024*mib)
	require.Len(t, plans, 2)
	require.Equal(t, "big", plans[0].fragments[0].GetPath())
	require.Equal(t, "small", plans[1].fragments[0].GetPath())
}

func TestMissingV3ImportTaskSpecs(t *testing.T) {
	fragments := []v3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 1, path: "f1", rows: 1, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 2, path: "f2", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionIndex: 0, seq: 3, path: "f3", rows: 100, bytes: 4},
	}
	existing := []*datapb.FragmentRef{{Path: "f1", RowCount: 1}}
	missing, err := missingV3ImportTaskSpecs(fragments, existing, []string{"v0"}, []int64{10}, 10)
	require.NoError(t, err)
	require.Len(t, missing, 1)
	require.Equal(t, []string{"f2", "f3"}, []string{missing[0].fragments[0].GetPath(), missing[0].fragments[1].GetPath()})
	require.Equal(t, int64(200), missing[0].rows)
}

func TestLoadExistingImportV3Fragments(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Pending,
		SegmentId: 100, Vchannel: "v0", PartitionId: 4,
		Fragments: []*datapb.FragmentRef{{Path: "f0", RowCount: 5}},
	}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()

	fragments, err := checker.loadExistingImportV3Fragments(job)
	require.NoError(t, err)
	require.Len(t, fragments, 1)
	require.Equal(t, "f0", fragments[0].GetPath())
}

func TestLoadExistingImportV3FragmentsRejectsIncompleteTask(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Pending, SegmentId: 100,
	}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()

	_, err := checker.loadExistingImportV3Fragments(job)
	require.Error(t, err)
}

func TestValidateImportV3StorageVersion(t *testing.T) {
	textSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_Text},
	}}
	plainSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	paramtable.Get().Save("common.storage.useLoonFFI", "false")
	defer paramtable.Get().Reset("common.storage.useLoonFFI")
	require.Error(t, validateImportV3StorageVersion(textSchema))
	require.NoError(t, validateImportV3StorageVersion(plainSchema))
	paramtable.Get().Save("common.storage.useLoonFFI", "true")
	require.NoError(t, validateImportV3StorageVersion(textSchema))
}

func TestBuildImportV3TaskPlanDerivesExecutionInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, Schema: schema, DataTs: 7}}
	task := &datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, Vchannel: "v0", PartitionId: 4, Rows: 5,
		Fragments: []*datapb.FragmentRef{{Path: "f0", RowCount: 5}},
	}
	plan, err := buildImportV3TaskPlan(job, task)
	require.NoError(t, err)
	require.Equal(t, "v0", plan.GetVchannel())
	require.Equal(t, int64(4), plan.GetPartitionId())
	require.Equal(t, int64(5), plan.GetRows())
	require.Equal(t, uint64(7), plan.GetDataTs())
	require.Equal(t, int64(2), plan.GetCollectionId())
	require.Equal(t, int64(5), plan.GetWriter().GetPkCapacity())
	require.NotEmpty(t, plan.GetSort().GetFields())
	require.NotNil(t, plan.GetSchema())
	require.NotNil(t, plan.GetTempSchema())
	require.False(t, plan.GetBackup())

	task.Rows = 0
	_, err = buildImportV3TaskPlan(job, task)
	require.Error(t, err)
}

func TestImportCheckerV3CheckGCIgnoresNonTerminalJob(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Importing}}

	// No catalog interaction: checkGC returns before touching tasks or objects.
	checker.checkGC(job)
}

func TestImportCheckerV3QuiesceDropsBoundTask(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta, cluster: cluster}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Failed}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_InProgress, NodeId: 5,
	}, importMeta, nil, nil)

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	// The GC loop re-issues a version-aware best-effort Drop every tick so a
	// transient RPC failure or a lost node cannot pin the job forever.
	cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(nil).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()

	require.False(t, checker.quiesceImportJob(job, mlog.With(mlog.FieldJobID(job.GetJobID()))))
}

func TestImportCheckerV3QuiesceRemovesFailedTaskAndDropsSegment(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	segment, err := addImportSegment(ctx, meta, 100, 1, 10, 2, 3, "v0", datapb.SegmentLevel_L1, 1, 4)
	require.NoError(t, err)
	require.Equal(t, commonpb.SegmentState_Importing, segment.GetState())
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, State: internalpb.ImportJobState_Failed}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Completed, NodeId: NullNodeID, SegmentId: 100,
	}, importMeta, meta, nil)

	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()

	require.True(t, checker.quiesceImportJob(job, mlog.With(mlog.FieldJobID(job.GetJobID()))))
	require.Equal(t, commonpb.SegmentState_Dropped, meta.GetSegment(ctx, 100).GetState())
}

func TestImportCheckerV3CheckGCWaitsForCleanupTs(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}

	// Quiesce is done (no tasks), but the retention window has not elapsed, so
	// deleteImportJob must not run. RemoveJob is un-mocked on purpose: a call
	// would fail the test.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Once()
	checker.checkGC(job)
}

func TestImportCheckerV3CheckGCDeletesPastCleanup(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: true,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
	}}

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))
	// Quiesce sees no tasks; delete runs RemoveWithPrefix then RemoveJob.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Twice()
	importMeta.EXPECT().RemoveJob(mock.Anything, int64(1)).Return(nil).Once()

	checker.checkGC(job)
	exist, err := cm.Exist(ctx, planPath)
	require.NoError(t, err)
	require.False(t, exist) // prefix removed
}

func TestImportCheckerV3RollbackGateBlocksDelete(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: cm}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Failed, AutoCommit: false,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour)),
	}}
	rollbackCalled := false
	checker.hooks.rollbackImport = func(ctx context.Context, j ImportJob) error {
		rollbackCalled = true
		return merr.WrapErrServiceUnavailable("transient")
	}
	checker.hooks.isReplicatingCluster = func(ctx context.Context) (bool, error) { return true, nil }

	planPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1))
	require.NoError(t, cm.Write(ctx, planPath, []byte("plan")))

	// Rollback broadcast fails transiently: keep the job, no delete this tick.
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Once()
	checker.checkGC(job)
	require.True(t, rollbackCalled)

	// Next tick rollback succeeds; delete proceeds.
	checker.hooks.rollbackImport = func(ctx context.Context, j ImportJob) error { return nil }
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything).Return(nil).Twice()
	importMeta.EXPECT().RemoveJob(mock.Anything, int64(1)).Return(nil).Once()
	checker.checkGC(job)
}

// TestImportCheckerV3PlanningQuotaFailsJob pins V2 parity for disk-quota
// exhaustion during Planning, driving the production checkPlanningJob ->
// planV3Job -> CheckDiskQuotaV3 path end to end: the job fails immediately
// with the quota reason instead of retrying silently every 2s tick with a
// fixed 40% progress and an empty reason. A transient planning error (here, a
// missing reshard manifest surfacing a raw object-store read error) must
// instead leave the job in Planning for the next tick.
func TestImportCheckerV3PlanningQuotaFailsJob(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	meta.AddCollection(&collectionInfo{ID: 2, Schema: schema})
	meta.chunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	cm := meta.chunkManager

	// LogicalBytes feeds CheckDiskQuotaV3's requestSize (bytes * 1.5); the
	// DiskQuota param is MB-scaled by its formatter, so size the fragment
	// above the configured quota.
	manifest := &datapb.ReshardManifest{Fragments: []*datapb.FragmentDescriptor{{
		Path: "frag-0-0-0.parquet", Rows: 5, LogicalBytes: 200 << 20, ChannelIndex: 0, PartitionIndex: 0,
	}}}
	payload, err := proto.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 2))
	require.NoError(t, cm.Write(ctx, manifestPath, payload))

	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Planning,
		Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
	}}
	reshard := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, RunId: 2, State: datapb.ImportTaskStateV2_Completed,
	}, importMeta, meta, nil)
	// cleanupPreparingV3ImportTasks and loadExistingImportV3Fragments both
	// query the ImportTaskV3 set (empty here); planV3Job then queries the
	// reshard set. Probe the filter instead of relying on call order.
	v3Probe := newImportTaskV3(&datapb.ImportTaskV3{}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) == 1 && filters[0](v3Probe) {
				return nil
			}
			return []ImportTask{reshard}
		}).Times(3)
	importMeta.EXPECT().GetJobBy(mock.Anything).Return(nil).Once()

	paramtable.Get().Save(paramtable.Get().QuotaConfig.DiskProtectionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DiskQuota.Key, "100")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.DiskQuota.Key)
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.DiskProtectionEnabled.Key)

	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateJobAction) {
			applied := &importJob{ImportJob: &datapb.ImportJob{}}
			for _, action := range actions {
				action(applied)
			}
			require.Equal(t, internalpb.ImportJobState_Failed, applied.GetState())
			require.Contains(t, applied.GetReason(), "disk quota exceeded")
		}).Return(nil).Once()

	checker.checkPlanningJob(job)

	// Transient half: an unwarmed collection cache makes validateImportV3Schema
	// return ErrServiceNotReady (retriable by design -- datacoord repopulates
	// the cache after a restart). No UpdateJob expectation is registered on
	// this mock, so any fail-the-job call fails the test; the job stays in
	// Planning for the next tick. (A missing manifest object is deliberately
	// NOT used here: it surfaces key-not-found, a permanent error the shared
	// denylist fails fast on.)
	freshMeta, err := newMemoryMeta(t)
	require.NoError(t, err)
	freshMeta.chunkManager = cm
	transientMeta := NewMockImportMeta(t)
	transientChecker := &importCheckerV3{ctx: ctx, meta: freshMeta, importMeta: transientMeta}
	transientChecker.checkPlanningJob(job)
	require.Equal(t, internalpb.ImportJobState_Planning, job.GetState())
}
