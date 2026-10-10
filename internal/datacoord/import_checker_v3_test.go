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
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
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

	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskStateV2_Completed, NodeId: NullNodeID, SegmentId: 100, Rows: 42,
	}, importMeta, nil, nil)

	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, State: internalpb.ImportJobState_Completed,
		CleanupTs: tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
	}}
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	require.False(t, quiesceImportV3JobGC(newImportV3JobContext(checker, job)))

	job.CleanupTs = tsoutil.ComposeTSByTime(time.Now().Add(-time.Hour))
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1)).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()
	require.True(t, quiesceImportV3JobGC(newImportV3JobContext(checker, job)))
}

func TestCalculateImportV3TaskSlots(t *testing.T) {
	const mib = int64(1024 * 1024)

	// The model charges the whole per-task footprint scaled by the expansion
	// factor: fixed IO (the real parquet read stream 64MiB plus one packed
	// writer buffer 32MiB per detached write) + 6R pipeline (96MiB) + resident
	// buckets + detached fragment inputs + their structural overhead + one sort
	// copy per detached write. With R=16MiB, F=128MiB, N=2, an explicit 1.2
	// factor and a 15-field schema the single-bucket footprint is ~864MiB and
	// the charge is ceil(1.2*864/160)=7. The explicit factor keeps these
	// numbers independent of the configured default; the formula is what this
	// test pins.
	mem := reshardmem.Model{ReadBuffer: 16 * mib, FragmentTarget: 128 * mib, FlushConcurrency: 2, ExpansionFactor: 1.2}
	require.Equal(t, int64(7), calculateReshardTaskSlot(mem, 160*mib, 1, 16, 15))
	// A 340 MiB per-slot limit fits the ~1037 MiB working set in four slots.
	require.Equal(t, int64(4), calculateReshardTaskSlot(mem, 340*mib, 1, 16, 15))
	// Full in-flight coverage: each bucket up to the cap adds one scaled
	// fragmentTarget, so the DataNode's resident ceiling covers every bucket
	// (ceil(1498/160)=10 and ceil(3347.8/160)=21 slots).
	require.Equal(t, int64(10), calculateReshardTaskSlot(mem, 160*mib, 4, 16, 15))
	require.Equal(t, int64(21), calculateReshardTaskSlot(mem, 160*mib, 16, 16, 15))
	// Beyond the cap the resident demand flattens (the excess in-flight data
	// spills), but the structural term keeps growing with the bucket count:
	// 128 buckets charge 22 slots, while 2048 buckets shred the live set enough
	// to charge 27.
	require.Equal(t, int64(22), calculateReshardTaskSlot(mem, 160*mib, 128, 16, 15))
	require.Equal(t, int64(27), calculateReshardTaskSlot(mem, 160*mib, 2048, 16, 15))
	// Degenerate inputs clamp to the single-bucket base estimate.
	require.Equal(t, int64(7), calculateReshardTaskSlot(mem, 160*mib, 8, 0, 15))
	require.Equal(t, int64(7), calculateReshardTaskSlot(mem, 160*mib, 0, 16, 15))
	// What the detached concurrency costs a scheduler: one write instead of two
	// drops one writer buffer, one in-flight fragment input and one sort copy,
	// so the same shapes charge ceil(691.2/160)=5 and ceil(3002.2/160)=19.
	serial := reshardmem.Model{ReadBuffer: 16 * mib, FragmentTarget: 128 * mib, FlushConcurrency: 1, ExpansionFactor: 1.2}
	require.Equal(t, int64(5), calculateReshardTaskSlot(serial, 160*mib, 1, 16, 15))
	require.Equal(t, int64(19), calculateReshardTaskSlot(serial, 160*mib, 16, 16, 15))
	require.Equal(t, int64(4), calculateImportTaskV3Slot(16*mib, 32*mib, 160*mib, 16))
	require.Equal(t, int64(1), calculateImportV3Slots(1, 160*mib))
	require.Equal(t, int64(2), calculateImportV3Slots(160*mib+1, 160*mib))
	// The slot helper must not divide by zero when the configured per-slot
	// memory limit is invalid; paramtable validation remains the primary guard.
	require.Equal(t, int64(1), calculateImportV3Slots(1, 0))
	require.Equal(t, int64(1), calculateImportV3Slots(1, -1))
}

func TestEffectiveImportV3FanIn(t *testing.T) {
	require.Equal(t, 16, getImportV3FanIn(16, 0))
	require.Equal(t, 16, getImportV3FanIn(16, 100))
	require.Equal(t, 5, getImportV3FanIn(16, 5))
	require.Equal(t, 2, getImportV3FanIn(16, 1))
	require.Equal(t, 2, getImportV3FanIn(2, 1))
}

// TestCreateImportV3TaskWritesPendingWithoutSegment pins the new Planning
// contract: the task is persisted directly as Pending (not None), carrying the
// reserved segment id but no segment record. The segment is only created later,
// at dispatch, so a task never owns a segment it does not use.
func TestCreateImportV3TaskWritesPendingWithoutSegment(t *testing.T) {
	ctx := context.Background()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	importMeta := NewMockImportMeta(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(int64(1)).Return(int64(200), int64(201), nil).Once()
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta, alloc: alloc}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, DataTs: 1, Schema: &schemapb.CollectionSchema{Version: 3}}}

	var task *importTaskV3
	// A single AddTask write; the strict mock fails the test if Planning issues
	// any follow-up UpdateTask (the Pending state is written up front).
	importMeta.EXPECT().AddTask(mock.Anything, mock.Anything).Run(func(_ context.Context, added ImportTask) {
		task = added.(*importTaskV3)
	}).Return(nil).Once()

	err = createImportTaskV3(
		newImportV3JobContext(checker, job),
		&importv3pb.WriterSpec{StorageVersion: 1},
		10, 100,
		importTaskV3Spec{channel: "v0", partitionID: 4, fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 1}}, rows: 5},
	)
	require.NoError(t, err)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
	p := task.task.Load()
	require.Equal(t, int64(100), p.GetSegmentId())
	require.Equal(t, "v0", p.GetVchannel())
	require.Equal(t, int64(4), p.GetPartitionId())
	require.Len(t, p.GetFragments(), 1)
	require.Equal(t, "f0", p.GetFragments()[0].GetPath())
	require.Equal(t, int64(1), importSlot(len(p.GetFragments()), getImportV3Config()))
	// Planning creates no segment record.
	require.Nil(t, meta.GetSegment(ctx, 100))
}

func TestCreateReshardTasksKeepsExistingAndAddsMissingSources(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(int64(1)).Return(int64(20), int64(21), nil).Once()
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	// Sizing must come from the PreImportV3 stats, not the object store: the
	// chunk manager is a strict mock with no expectations, so any Size or
	// WalkWithPrefix call would fail the test.
	meta.chunkManager = mocks2.NewChunkManager(t)
	checker := &importCheckerV3{ctx: ctx, meta: meta, importMeta: importMeta, alloc: alloc}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		State: internalpb.ImportJobState_Pending,
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"existing.json"}}, {Id: 2, Paths: []string{"missing.json"}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	existing := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, FileIds: []int64{1},
	}, importMeta, checker.meta, alloc)
	// The preimport stats supply every file's packing size.
	sized := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 11, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{
			{FileId: 1, TotalMemorySize: 4096},
			{FileId: 2, FileSize: 4096},
		},
	}, importMeta)
	// CreateTasks queries the reshard set (for covered sources) and the
	// preimport set (for the packing-size input); probe the filter instead of
	// relying on call order.
	reshardProbe := newReshardTask(&datapb.ReshardTask{}, importMeta, nil, nil)
	preimportProbe := newPreImportTaskV3(&datapb.PreImportTaskV3{}, importMeta)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) != 1 {
				return nil
			}
			switch {
			case filters[0](reshardProbe):
				return []ImportTask{existing}
			case filters[0](preimportProbe):
				return []ImportTask{sized}
			}
			return nil
		}).Times(2)
	importMeta.EXPECT().AddTask(mock.Anything, mock.Anything).Run(func(_ context.Context, added ImportTask) {
		p := added.(*reshardTask).task.Load()
		// One bucket, one temp-schema field and the default two detached
		// writes: the whole per-task footprint (fixed IO 128 + pipeline 96 +
		// resident 128 + in-flight 256 + sort 256 MiB, structural negligible)
		// is expanded by the default 1.5 factor to ~1296MiB, which needs 3
		// slots at the 512MiB node slot unit (8GiB / workerSlotUnit 16). The
		// slot is no longer persisted; it is derived at dispatch.
		require.Equal(t, int64(3), reshardSlot(job, getImportV3Config()))
		require.Equal(t, []int64{2}, p.GetFileIds())
	}).Return(nil).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()

	require.NoError(t, createReshardTasks(newImportV3JobContext(checker, job)))
}

// TestGroupSourcesSizesFromPreImportV3Stats pins that the BFD packing unit comes
// from the job's PreImportV3 task stats — the decoded total_memory_size when the
// worker measured one, else the physical file_size — and never from an
// object-store stat. The chunk manager is a strict mock with no expectations, so
// any Size/WalkWithPrefix call would fail the test.
func TestGroupSourcesSizesFromPreImportV3Stats(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, meta: &meta{chunkManager: mocks2.NewChunkManager(t)}, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"a"}},
			{Id: 2, Paths: []string{"b"}},
			{Id: 3, Paths: []string{"c"}},
		},
	}, tr: timerecord.NewTimeRecorder("import job")}
	sized := newPreImportTaskV3(&datapb.PreImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportV3FileStats{
			{FileId: 1, FileSize: 7, TotalMemorySize: 200}, // decoded size wins
			{FileId: 2, FileSize: 50, TotalMemorySize: 0},  // physical fallback
			{FileId: 3, FileSize: 0, TotalMemorySize: 0},   // empty
		},
	}, importMeta)
	preimportProbe := newPreImportTaskV3(&datapb.PreImportTaskV3{}, importMeta)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) == 1 && filters[0](preimportProbe) {
				return []ImportTask{sized}
			}
			return nil
		}).Once()

	groups, err := groupSources(newImportV3JobContext(checker, job), job.GetFiles())
	require.NoError(t, err)
	// All three fit the default target, so one bin, sorted by the stats' sizes.
	require.Len(t, groups, 1)
	require.Len(t, groups[0], 3)
	require.Equal(t, []int64{200, 50, 0}, []int64{groups[0][0].size, groups[0][1].size, groups[0][2].size})
	require.Equal(t, []int64{1, 2, 3}, []int64{
		groups[0][0].file.GetId(), groups[0][1].file.GetId(), groups[0][2].file.GetId(),
	})
}

// TestPendingHandlerBackupCreatesPreImportV3Tasks pins the backup flow: a backup
// job no longer skips preimport and goes straight to Resharding; its Pending
// handler runs the count-only PreImportV3 stage like every other job.
func TestPendingHandlerBackupCreatesPreImportV3Tasks(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	alloc := allocator.NewMockAllocator(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta, alloc: alloc}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		State:   internalpb.ImportJobState_Pending,
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}},
		Files: []*internalpb.ImportFile{
			{Id: 1, Paths: []string{"insert", "delta"}},
			{Id: 2, Paths: []string{"insert2"}},
		},
	}, tr: timerecord.NewTimeRecorder("import job")}
	require.False(t, importV3NeedsIDRanges(job), "a backup job needs no ID ranges")

	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()
	alloc.EXPECT().AllocN(int64(1)).Return(int64(20), int64(21), nil).Once()
	var added ImportTask
	importMeta.EXPECT().AddTask(mock.Anything, mock.Anything).Run(func(_ context.Context, t ImportTask) {
		added = t
	}).Return(nil).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).RunAndReturn(
		func(_ context.Context, _ int64, actions ...UpdateJobAction) error {
			for _, a := range actions {
				a(job)
			}
			return nil
		}).Once()

	require.NoError(t, pendingHandler{}.Handle(newImportV3JobContext(checker, job)))
	require.NotNil(t, added, "the Pending handler must create a PreImportV3 task")
	preimport, ok := added.(*preImportV3Task)
	require.True(t, ok, "expected a preImportV3Task, got %T", added)
	require.Equal(t, PreImportTaskV3Type, preimport.GetType())
	stats := preimport.GetV3FileStats()
	require.Len(t, stats, 2)
	require.Equal(t, []int64{1, 2}, []int64{stats[0].GetFileId(), stats[1].GetFileId()})
	require.Equal(t, internalpb.ImportJobState_PreImporting, job.GetState())
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
		State:  internalpb.ImportJobState_Pending,
		Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
		Files:  []*internalpb.ImportFile{{Id: 1, Paths: []string{"existing.json"}}},
	}, tr: timerecord.NewTimeRecorder("import job")}
	task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Completed, FileIds: []int64{1}}, importMeta, checker.meta, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	importMeta.EXPECT().UpdateJob(mock.Anything, int64(1), mock.Anything).Return(nil).Once()

	require.NoError(t, createReshardTasks(newImportV3JobContext(checker, job)))
}

func TestBuildReshardTaskPlanDerivesExecutionInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}, {Id: 2, Paths: []string{"b.json"}}},
	}}
	task := &datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, FileIds: []int64{2}}
	plan, err := reshardPlan(job, task, getImportV3Config())
	require.NoError(t, err)
	require.Equal(t, int64(2), plan.GetCollectionId())
	require.Len(t, plan.GetFiles(), 1)
	require.Equal(t, int64(2), plan.GetFiles()[0].GetId())
	require.Equal(t, []string{"v0"}, plan.GetVchannels())
	require.Equal(t, []int64{3}, plan.GetPartitionIds())
	require.NotNil(t, plan.GetCollectionSchema())
	require.Greater(t, plan.GetFragmentSize(), int64(0))

	// The job's RLS write predicate must travel with the plan so the DataNode
	// can enforce it per source batch (Import V3 parity with Import V2).
	predicate := &planpb.Expr{}
	job.RlsCheckPredicate = predicate
	plan, err = reshardPlan(job, task, getImportV3Config())
	require.NoError(t, err)
	require.Same(t, predicate, plan.GetRlsCheckPredicate())

	task.FileIds = []int64{9}
	_, err = reshardPlan(job, task, getImportV3Config())
	require.Error(t, err)
}

// fragmentSizeInMB is refreshable but validated only at startup; a hot refresh
// to an invalid value must fail plan building loudly instead of reaching the
// DataNode as a zero fragment target (which would flush every non-empty bucket
// after every batch).
func TestBuildReshardTaskPlanRejectsInvalidFragmentSize(t *testing.T) {
	liveKey := paramtable.Get().DataCoordCfg.ImportFragmentSizeInMB.Key
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Schema: schema, Vchannels: []string{"v0"}, PartitionIDs: []int64{3},
		Files: []*internalpb.ImportFile{{Id: 1, Paths: []string{"a.json"}}},
	}}
	task := &datapb.ReshardTask{JobId: 1, TaskId: 10, CollectionId: 2, FileIds: []int64{1}}
	for _, invalid := range []string{"0", "", "not-a-number", "-4"} {
		paramtable.Get().Save(liveKey, invalid)
		_, err := reshardPlan(job, task, getImportV3Config())
		require.Error(t, err, "value=%q", invalid)
		require.Contains(t, err.Error(), "fragmentSizeInMB")
	}
	paramtable.Get().Reset(liveKey)
}

// createReshardTask must reject an invalid live fragment size with a terminal
// error so the checker fails the job instead of reserving a bogus slot.
func TestCreateReshardTaskRejectsInvalidFragmentSize(t *testing.T) {
	liveKey := paramtable.Get().DataCoordCfg.ImportFragmentSizeInMB.Key
	paramtable.Get().Save(liveKey, "0")
	defer paramtable.Get().Reset(liveKey)

	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: context.Background(), importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}

	err := createTask(newImportV3JobContext(checker, job), 10, []reshardSource{{file: &internalpb.ImportFile{Id: 1}, size: 1}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fragmentSizeInMB")
	require.True(t, importcommon.IsTerminalImportV3Err(err))
}

func TestCreateImportV3TaskRejectsMissingDataTs(t *testing.T) {
	checker := &importCheckerV3{ctx: context.Background()}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}
	err := createImportTaskV3(
		newImportV3JobContext(checker, job),
		&importv3pb.WriterSpec{},
		10, 100,
		importTaskV3Spec{channel: "v0", partitionID: 4},
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
		completed, err := reshardTasksAllCompleted(newImportV3JobContext(checker, job))
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
		completed, err := reshardTasksAllCompleted(newImportV3JobContext(checker, job))
		require.False(t, completed)
		require.NoError(t, err)
	})

	t.Run("all completed", func(t *testing.T) {
		importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
			Return([]ImportTask{
				newTask(datapb.ImportTaskStateV2_Completed, 10),
				newTask(datapb.ImportTaskStateV2_Completed, 11),
			}).Once()
		completed, err := reshardTasksAllCompleted(newImportV3JobContext(checker, job))
		require.True(t, completed)
		require.NoError(t, err)
	})
}

func TestBuildImportV3SegmentPlans(t *testing.T) {
	fragments := []importV3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 1, path: "f1", rows: 1, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 2, path: "f2", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 3, path: "f3", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 1, partitionID: 10, seq: 4, path: "f4", rows: 7, bytes: 1},
	}
	plans := buildImportV3SegmentPlans(fragments, []string{"v0", "v1"}, 10)
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

// TestBuildImportV3SegmentPlansPacksDefaultFragmentTarget pins the documented
// premise that one segment plan holds about 8 default-sized fragments: eight
// 128MiB fragments fit a 1GiB target in one plan, and the ninth fragment
// opens a second plan. A single oversized fragment still owns a plan alone.
func TestBuildImportV3SegmentPlansPacksDefaultFragmentTarget(t *testing.T) {
	const mib = int64(1024 * 1024)
	newFragments := func(n int) []importV3PlanningFragment {
		fragments := make([]importV3PlanningFragment, 0, n)
		for i := 0; i < n; i++ {
			fragments = append(fragments, importV3PlanningFragment{
				sourceID: 1, channelIndex: 0, partitionID: 10,
				seq: int64(i), path: fmt.Sprintf("f%d", i), rows: 100, bytes: 128 * mib,
			})
		}
		return fragments
	}
	plans := buildImportV3SegmentPlans(newFragments(8), []string{"v0"}, 1024*mib)
	require.Len(t, plans, 1)
	require.Len(t, plans[0].fragments, 8)

	plans = buildImportV3SegmentPlans(newFragments(9), []string{"v0"}, 1024*mib)
	require.Len(t, plans, 2)
	require.Len(t, plans[0].fragments, 8)
	require.Len(t, plans[1].fragments, 1)

	oversized := []importV3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 0, path: "big", rows: 1, bytes: 2 * 1024 * mib},
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 1, path: "small", rows: 1, bytes: 128 * mib},
	}
	plans = buildImportV3SegmentPlans(oversized, []string{"v0"}, 1024*mib)
	require.Len(t, plans, 2)
	require.Equal(t, "big", plans[0].fragments[0].GetPath())
	require.Equal(t, "small", plans[1].fragments[0].GetPath())
}

func TestMissingImportTaskV3Specs(t *testing.T) {
	fragments := []importV3PlanningFragment{
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 1, path: "f1", rows: 1, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 2, path: "f2", rows: 100, bytes: 4},
		{sourceID: 1, channelIndex: 0, partitionID: 10, seq: 3, path: "f3", rows: 100, bytes: 4},
	}
	existing := []*importv3pb.FragmentRef{{Path: "f1", Rows: 1}}
	missing, err := missingImportTaskV3Specs(fragments, existing, []string{"v0"}, 10)
	require.NoError(t, err)
	require.Len(t, missing, 1)
	require.Equal(t, []string{"f2", "f3"}, []string{missing[0].fragments[0].GetPath(), missing[0].fragments[1].GetPath()})
	require.Equal(t, int64(200), missing[0].rows)

	// A ready ref counts even if the row count differs: the match is by path,
	// so the mismatch is reported (as an integrity error naming both counts)
	// rather than silently covering the fragment.
	_, err = missingImportTaskV3Specs(fragments, []*importv3pb.FragmentRef{{Path: "f1", Rows: 999}}, []string{"v0"}, 10)
	require.ErrorContains(t, err, "row count 999 does not match the manifest row count 1")

	// A ref whose path is no longer in the planning input, and two refs for one
	// path, are integrity errors.
	_, err = missingImportTaskV3Specs(fragments, []*importv3pb.FragmentRef{{Path: "gone", Rows: 1}}, []string{"v0"}, 10)
	require.ErrorContains(t, err, "outside current planning input")
	_, err = missingImportTaskV3Specs(fragments, []*importv3pb.FragmentRef{{Path: "f1", Rows: 1}, {Path: "f1", Rows: 1}}, []string{"v0"}, 10)
	require.ErrorContains(t, err, "duplicate fragment")
}

func TestLoadExistingImportV3Fragments(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importCheckerV3{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_Pending,
		SegmentId: 100, Vchannel: "v0", PartitionId: 4,
		Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
	}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()

	fragments, err := loadExistingFragments(newImportV3JobContext(checker, job))
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

	_, err := loadExistingFragments(newImportV3JobContext(checker, job))
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
	require.Error(t, validateImportV3StorageVersion(textSchema, importStorageVersion(false)))
	require.NoError(t, validateImportV3StorageVersion(plainSchema, importStorageVersion(false)))
	paramtable.Get().Save("common.storage.useLoonFFI", "true")
	require.NoError(t, validateImportV3StorageVersion(textSchema, importStorageVersion(false)))
}

func TestBuildImportV3TaskPlanDerivesExecutionInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, Schema: schema, DataTs: 7}}
	task := &datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, CollectionId: 2, Vchannel: "v0", PartitionId: 4, Rows: 5,
		Fragments: []*importv3pb.FragmentRef{{Path: "f0", Rows: 5}},
	}
	plan, _, err := importPlan(job, task, getImportV3Config())
	require.NoError(t, err)
	require.Equal(t, int64(4), plan.GetPartitionId())
	require.Equal(t, int64(5), plan.GetRows())
	require.Equal(t, uint64(7), plan.GetDataTs())
	require.Equal(t, int64(2), plan.GetCollectionId())
	require.NotNil(t, plan.GetCollectionSchema())
	require.False(t, plan.GetBackup())

	task.Rows = 0
	_, _, err = importPlan(job, task, getImportV3Config())
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

	require.False(t, quiesceImportV3JobGC(newImportV3JobContext(checker, job)))
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

	require.True(t, quiesceImportV3JobGC(newImportV3JobContext(checker, job)))
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
	checker.hooks.getReplicationRole = func(ctx context.Context) (replicateutil.Role, bool, error) {
		return replicateutil.RolePrimary, true, nil
	}

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
// planImportV3Job -> CheckImportV3DiskQuota path end to end: the job fails immediately
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

	// LogicalBytes feeds CheckImportV3DiskQuota's requestSize; the DiskQuota
	// param is MB-scaled by its formatter, so size the fragment above the
	// configured quota.
	manifest := &importv3pb.ReshardManifest{Fragments: []*importv3pb.FragmentDescriptor{{
		Path: "frag-0-0-0.parquet", Rows: 5, LogicalBytes: 200 << 20, VchannelIndex: 0, PartitionId: 3,
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
	// loadExistingFragments queries the ImportTaskV3 set (empty here);
	// planImportV3Job then queries the reshard set. Probe the filter instead of
	// relying on call order.
	importTaskV3Probe := newImportTaskV3(&datapb.ImportTaskV3{}, importMeta, nil, nil)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) == 1 && filters[0](importTaskV3Probe) {
				return nil
			}
			return []ImportTask{reshard}
		}).Times(2)
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

	checker.checkJob(job)

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
	transientChecker.checkJob(job)
	require.Equal(t, internalpb.ImportJobState_Planning, job.GetState())
}
