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

package bulkimport

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	compactiontask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestImportCheckerSortDependencyAdmission(t *testing.T) {
	paramtable.Init()
	cfg := &Params.DataCoordCfg
	settings := map[string]string{
		cfg.EnableSortCompaction.Key:        "true",
		cfg.CompactionMaxTaskNum.Key:        "1",
		cfg.CompactionTaskQueueCapacity.Key: "2",
		cfg.CompactionRateLimitTokens.Key:   "2",
		cfg.CompactionRateLimitInterval.Key: "3600",
	}
	for key, value := range settings {
		require.NoError(t, Params.Save(key, value))
		t.Cleanup(func() { Params.Reset(key) })
	}

	ctx := t.Context()
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Once()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Twice()
	taskMeta, err := compactiontask.NewCompactionTaskMeta(ctx, catalog)
	require.NoError(t, err)
	mt := meta.NewTestMetaForCompactionTargets()
	mt.AddCollection(&meta.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{}})
	publish := func(id int64, importing bool) {
		mt.Segments.SetSegment(id, meta.NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 1, InsertChannel: "ch",
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			IsImporting: importing, NumOfRows: 10,
		}))
	}
	publish(1, false)
	publish(2, true)
	publish(3, true)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(100), nil).Twice()
	alloc.EXPECT().AllocN(int64(2)).Return(int64(200), int64(202), nil).Once()
	alloc.EXPECT().AllocN(int64(2)).Return(int64(202), int64(204), nil).Once()
	handler := meta.NewMockHandlerWithMeta(mt)
	idx := &index.IndexMeta{}
	inspector := compaction.NewCompactionInspector(mt, taskMeta, idx, nil, nil, nil, alloc, handler, nil, nil, nil)
	require.NoError(t, inspector.InitAdmission())
	require.NoError(t, inspector.EnqueueCompaction(ctx, &datapb.CompactionTask{
		PlanID: 11, TriggerID: 10, CollectionID: 1, Type: datapb.CompactionType_MixCompaction,
		InputSegments: []int64{1},
	}, compaction.AdmissionOptions{Producer: compaction.ProducerTBC}))

	importMeta := NewMockImportMeta(t)
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 99, CollectionID: 1, State: internalpb.ImportJobState_Sorting}}
	task := &Task{}
	task.Task.Store(&datapb.ImportTaskV2{
		JobID: job.GetJobID(), TaskID: 100, CollectionID: 1,
		SegmentIDs: []int64{2, 3}, SortedSegmentIDs: []int64{12, 13},
		State: datapb.ImportTaskStateV2_Completed,
	})
	importMeta.EXPECT().GetTaskByJob(mock.Anything, job.GetJobID(), mock.Anything).Return([]ImportTask{task}).Once()
	checker := NewImportChecker(ctx, mt, idx, nil, alloc, importMeta, inspector, handler, nil, nil, ImportCheckerHooks{}).(*Checker)
	checker.CheckSortingJob(job)

	plans := taskMeta.GetCompactionTasksByTriggerID(200)
	require.Len(t, plans, 1)
	require.Equal(t, datapb.CompactionType_SortCompaction, plans[0].GetType())
	require.Equal(t, []int64{2}, plans[0].GetInputSegments())
	require.Equal(t, int64(12), plans[0].GetPreAllocatedSegmentIDs().GetBegin())
	require.Empty(t, taskMeta.GetCompactionTasksByTriggerID(202))
	require.False(t, mt.IsSegmentCompacting(3))
}

func TestJobIDRangesSet_NilCheckNotEmptyCheck(t *testing.T) {
	job := &Job{ImportJob: &datapb.ImportJob{Files: []*internalpb.ImportFile{
		{Id: 1, IdRange: &commonpb.IDRange{Begin: 5, End: 5}}, // empty range counts as set
		{Id: 2},
	}}}
	assert.False(t, JobIDRangesSet(job))
	job.GetFiles()[1].IdRange = &commonpb.IDRange{Begin: 5, End: 9}
	assert.True(t, JobIDRangesSet(job))
}

func TestUpdateJobIDRanges_SetsRangesAndPersists(t *testing.T) {
	ctx := context.Background()
	var saved []*datapb.ImportJob
	importMeta := NewCapturingImportMeta(t, &saved)

	job := &Job{
		ImportJob: &datapb.ImportJob{
			JobID: 1,
			State: internalpb.ImportJobState_PreImporting,
			Files: []*internalpb.ImportFile{{Id: 10}, {Id: 11}},
		},
		Tr: timerecord.NewTimeRecorder("test"),
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
