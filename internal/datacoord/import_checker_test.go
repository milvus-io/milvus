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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestImportCheckerCompaction(t *testing.T) {
	paramtable.Init()
	Params.Save(Params.DataCoordCfg.ImportCheckIntervalHigh.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.ImportCheckIntervalHigh.Key)
	Params.Save(Params.DataCoordCfg.ImportCheckIntervalLow.Key, "10000")
	defer Params.Reset(Params.DataCoordCfg.ImportCheckIntervalLow.Key)

	// prepare objects
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil).Maybe()

	alloc := allocator.NewMockAllocator(t)

	broker := broker2.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)

	dm := &indexTestMetas{}
	meta, err := newMeta(context.TODO(), catalog, nil, metacache.NewMetaStore(catalog), broker, loadIndexTestMetas(catalog, dm))
	assert.NoError(t, err)

	importMeta, err := NewImportMeta(context.TODO(), catalog, alloc, meta)
	assert.NoError(t, err)

	cim := NewMockCompactionInspector(t)
	handler := NewNMockHandler(t)

	checker := NewImportChecker(context.TODO(), meta, dm.indexMeta, broker, alloc, importMeta, cim, handler, importCheckerHooks{
		getReplicationRole: func(context.Context) (replicateutil.Role, bool, error) {
			return replicateutil.RolePrimary, false, nil
		},
		assignImportIDRange: func(ctx context.Context, job ImportJob, fileRows []int64) error {
			// Mirror the production wiring: the primary allocates one exact range per file
			// and applies it to the job, which is what lets the range gate proceed.
			ranges := make([]*commonpb.IDRange, len(fileRows))
			begin := int64(100000)
			for i, rows := range fileRows {
				ranges[i] = &commonpb.IDRange{Begin: begin, End: begin + rows}
				begin += rows
			}
			return importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobIDRanges(ranges))
		},
	}).(*importChecker)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          1001,
			CollectionID:   1,
			PartitionIDs:   []int64{2},
			ReadyVchannels: []string{"ch0"},
			Vchannels:      []string{"ch0", "ch1"},
			State:          internalpb.ImportJobState_Pending,
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
				{
					Id:    2,
					Paths: []string{"b.json"},
				},
				{
					Id:    3,
					Paths: []string{"c.json"},
				},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	err = importMeta.AddJob(context.TODO(), job)
	assert.NoError(t, err)
	jobID := job.GetJobID()

	// start check
	go checker.Start()

	// sleep 1.5s and ready the job, go to pending stats
	time.Sleep(1500 * time.Millisecond)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	job2 := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:          1001,
			CollectionID:   1,
			PartitionIDs:   []int64{2},
			ReadyVchannels: []string{"ch1"},
			Vchannels:      []string{"ch0", "ch1"},
			State:          internalpb.ImportJobState_Pending,
			TimeoutTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			CleanupTs:      tsoutil.ComposeTSByTime(time.Now().Add(time.Hour)),
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
			Files: []*internalpb.ImportFile{
				{
					Id:    1,
					Paths: []string{"a.json"},
				},
				{
					Id:    2,
					Paths: []string{"b.json"},
				},
				{
					Id:    3,
					Paths: []string{"c.json"},
				},
			},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = importMeta.AddJob(context.TODO(), job2)
	assert.NoError(t, err)
	mlog.Info(context.TODO(), "job ready")

	// check pending
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	}).Maybe()
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil).Maybe()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		preimportTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
		taskLen := len(preimportTasks)
		mlog.Info(context.TODO(), "job pre-importing", mlog.Any("taskLen", taskLen), mlog.Any("jobState", job.GetState()))
		return taskLen == 2 && job.GetState() == internalpb.ImportJobState_PreImporting
	}, 5*time.Second, 500*time.Millisecond)
	mlog.Info(context.TODO(), "job pre-importing")

	// check pre-importing
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil).Twice()
	// The gate writes the job three times here: the AssigningIDRange transition, the ranges
	// the assign hook applies, and the Importing transition.
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	preimportTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(PreImportTaskType))
	// Complete every preimport task with one exact count per file, carrying the file id like
	// the datanode report does: the range gate needs a row count per job file.
	for _, pt := range preimportTasks {
		stats := make([]*datapb.ImportFileStats, 0, len(pt.GetFileStats()))
		for _, st := range pt.GetFileStats() {
			stats = append(stats, &datapb.ImportFileStats{
				ImportFile: &internalpb.ImportFile{Id: st.GetImportFile().GetId(), Paths: st.GetImportFile().GetPaths()},
				TotalRows:  100,
			})
		}
		err := importMeta.UpdateTask(context.TODO(), pt.GetTaskID(),
			UpdateState(datapb.ImportTaskStateV2_Completed), UpdateFileStats(stats))
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		importTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
		return len(importTasks) == 1 && job.GetState() == internalpb.ImportJobState_Importing
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job importing")

	// check importing
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	// AlterSegments was previously driven by unsetSegmentImporting (removed in 2PC);
	// the remaining segment writes in this flow may or may not hit it.
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// Maybe, not Once: the open-ended SaveImportJob expectation set for the pre-importing
	// gate window matches every later call first, so a later Once() would never be consumed.
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil).Once()
	importTasks := importMeta.GetTaskByJob(context.TODO(), job.GetJobID(), WithType(ImportTaskType))
	targetSegmentIDs := make([]int64, 0)
	for _, it := range importTasks {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            rand.Int63(),
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
			},
		}
		err := checker.meta.AddSegment(context.Background(), segment)
		assert.NoError(t, err)
		targetSegmentID := rand.Int63()
		err = importMeta.UpdateTask(context.TODO(), it.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Completed),
			UpdateSegmentIDs([]int64{segment.GetID()}), UpdateStatsSegmentIDs([]int64{targetSegmentID}))
		assert.NoError(t, err)
		targetSegmentIDs = append(targetSegmentIDs, targetSegmentID)
		err = checker.meta.UpdateChannelCheckpoint(context.TODO(), segment.GetInsertChannel(), &msgpb.MsgPosition{MsgID: []byte{0}})
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Sorting
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job stats")

	// check stats
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	for _, targetSegmentID := range targetSegmentIDs {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            targetSegmentID,
				State:         commonpb.SegmentState_Flushed,
				IsImporting:   true,
				InsertChannel: "ch0",
				IsSorted:      true,
			},
		}
		err := checker.meta.AddSegment(context.Background(), segment)
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_IndexBuilding
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job index building")

	// check index building → Uncommitted (2PC: no longer transitions directly to Completed;
	// the test does not wire up a CommitImport broadcaster, so the job stops at Uncommitted).
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Maybe()
	assert.Eventually(t, func() bool {
		job := importMeta.GetJob(context.TODO(), jobID)
		return job.GetState() == internalpb.ImportJobState_Uncommitted
	}, 5*time.Second, 100*time.Millisecond)
	mlog.Info(context.TODO(), "job uncommitted (awaiting CommitImport WAL fence)")
}

// ---------------------------------------------------------------------------
// Tests for checkUncommittedJob
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests for checkCommittingJob
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Tests for checkPreImportingJob — empty-import fast path
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Helpers (newCapturingImportMeta is shared with ddl_callbacks_import_test.go)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: predicate helpers
// ---------------------------------------------------------------------------

func TestJobIDRangesSet_NilCheckNotEmptyCheck(t *testing.T) {
	job := &importJob{ImportJob: &datapb.ImportJob{Files: []*internalpb.ImportFile{
		{Id: 1, IdRange: &commonpb.IDRange{Begin: 5, End: 5}}, // empty range counts as set
		{Id: 2},
	}}}
	assert.False(t, jobIDRangesSet(job))
	job.GetFiles()[1].IdRange = &commonpb.IDRange{Begin: 5, End: 9}
	assert.True(t, jobIDRangesSet(job))
}

// ---------------------------------------------------------------------------
// Gate: primary path (Test Plan item 1)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: secondary path (Test Plan item 2)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: indeterminate role (Test Plan item 3)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: assign hook errors are transient (Test Plan item 4)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: excluded jobs proceed without the hook (Test Plan item 5)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: totalRows == 0 (Test Plan item 6)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: post-gate divergence check (Test Plan item 7)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: missing preimport stat (Test Plan item 8)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: full primary transition PreImporting → AssigningIDRange → Importing
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: restart recovery from the persisted AssigningIDRange state
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Gate: progress bucket for AssigningIDRange
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// UpdateJobIDRanges (Test Plan item 15)
// ---------------------------------------------------------------------------

func TestUpdateJobIDRanges_SetsRangesAndPersists(t *testing.T) {
	ctx := context.Background()
	var saved []*datapb.ImportJob
	importMeta := newCapturingImportMeta(t, &saved)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_PreImporting,
			Files: []*internalpb.ImportFile{{Id: 10}, {Id: 11}},
		},
		tr: timerecord.NewTimeRecorder("test"),
	}
	require.NoError(t, importMeta.AddJob(ctx, job))

	ranges := []*commonpb.IDRange{
		{Begin: 100, End: 110},
		{Begin: 110, End: 110}, // zero-row file keeps its empty range
	}
	require.NoError(t, importMeta.UpdateJob(ctx, 1, UpdateJobIDRanges(ranges)))

	// Survives re-GetJob: ranges by position.
	got := importMeta.GetJob(ctx, 1)
	require.NotNil(t, got)
	assert.EqualValues(t, 100, got.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, got.GetFiles()[1].GetIdRange().GetEnd())

	// Persisted: the proto handed to the catalog carries the ranges.
	require.NotEmpty(t, saved)
	last := saved[len(saved)-1]
	assert.EqualValues(t, 100, last.GetFiles()[0].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[0].GetIdRange().GetEnd())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetBegin())
	assert.EqualValues(t, 110, last.GetFiles()[1].GetIdRange().GetEnd())
}
