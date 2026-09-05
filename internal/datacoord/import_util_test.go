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
	"math/rand"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

func TestImportUtil_NewPreImportTasks(t *testing.T) {
	fileGroups := [][]*internalpb.ImportFile{
		{
			{Id: 0, Paths: []string{"a.json"}},
			{Id: 1, Paths: []string{"b.json"}},
		},
		{
			{Id: 2, Paths: []string{"c.npy", "d.npy"}},
			{Id: 3, Paths: []string{"e.npy", "f.npy"}},
		},
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2},
	}
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	tasks, err := NewPreImportTasks(fileGroups, job, alloc, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
}

func TestImportUtil_NewImportTasks(t *testing.T) {
	oldCompaction := Params.DataCoordCfg.EnableCompaction.SwapTempValue("true")
	t.Cleanup(func() {
		Params.DataCoordCfg.EnableCompaction.SwapTempValue(oldCompaction)
	})

	dataSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	fileGroups := [][]*datapb.ImportFileStats{
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 0, Paths: []string{"a.json"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 1, Paths: []string{"b.json"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 2, Paths: []string{"c.npy", "d.npy"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 3, Paths: []string{"e.npy", "f.npy"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 2,
			Schema: &schemapb.CollectionSchema{
				Version: 7,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:      100,
						Name:         "pk",
						DataType:     schemapb.DataType_Int64,
						IsPrimaryKey: true,
					},
				},
			},
		},
	}
	alloc := allocator.NewMockAllocator(t)
	allocNCalls := 0
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		allocNCalls++
		id := rand.Int63()
		return id, id + n, nil
	})
	nextSegmentID := int64(1000)
	alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(context.Context) (int64, error) {
		nextSegmentID++
		return nextSegmentID, nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(rand.Uint64(), nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	sortPlanned, err := importSortPlannedForJob(context.TODO(), job, nil, meta)
	assert.NoError(t, err)
	assert.True(t, sortPlanned)
	tasks, segments, err := NewImportTasks(context.TODO(), fileGroups, job, alloc, meta, nil,
		1*1024*1024*1024, sortPlanned)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	segmentsByID := lo.SliceToMap(segments, func(segment *SegmentInfo) (int64, *SegmentInfo) {
		return segment.GetID(), segment
	})
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		assert.Equal(t, 3, len(segmentIDs))
		assert.Empty(t, task.(*importTask).GetSortedSegmentIDs(), "sorted outputs are no longer preallocated")
		for _, segmentID := range segmentIDs {
			assert.Nil(t, meta.GetSegment(context.Background(), segmentID),
				"planning must not publish a segment before its task")
			assert.True(t, segmentsByID[segmentID].GetIsInvisible(),
				"a sort-planned import origin must persist that it awaits its replacement")
			assert.EqualValues(t, job.GetSchema().GetVersion(), segmentsByID[segmentID].GetSchemaVersion(),
				"an imported segment already materializes the job schema and must not wait for schema-bump reconciliation")
		}
	}
	assert.Equal(t, 1, allocNCalls, "only task IDs are preallocated, never sorted targets")

	// Once the task and origins are published, IsInvisible preserves the plan
	// even if compaction is disabled before a restart.
	for _, segment := range segments {
		meta.segments.SetSegment(segment.GetID(), segment)
	}
	Params.DataCoordCfg.EnableCompaction.SwapTempValue("false")
	sortPlanned, err = importSortPlannedForJob(context.TODO(), job, tasks, meta)
	assert.NoError(t, err)
	assert.True(t, sortPlanned)
	restartedImportMeta := NewMockImportMeta(t)

	restartedTasks, restartedSegments, err := NewImportTasks(context.TODO(), fileGroups[:1], job, alloc, meta,
		restartedImportMeta, 1*1024*1024*1024, sortPlanned)
	assert.NoError(t, err)
	restartedSegmentsByID := lo.SliceToMap(restartedSegments, func(segment *SegmentInfo) (int64, *SegmentInfo) {
		return segment.GetID(), segment
	})
	if assert.Len(t, restartedTasks, 1) {
		restartedTask := restartedTasks[0].(*importTask)
		assert.Empty(t, restartedTask.GetSortedSegmentIDs())
		for _, segmentID := range restartedTask.GetSegmentIDs() {
			assert.Nil(t, meta.GetSegment(context.Background(), segmentID))
			assert.True(t, restartedSegmentsByID[segmentID].GetIsInvisible())
		}
	}
	assert.Equal(t, 2, allocNCalls)

	// A genuinely new job created while compaction is disabled keeps its
	// origins visible and skips the sort stage.
	sortPlanned, err = importSortPlannedForJob(context.TODO(), job, nil, meta)
	assert.NoError(t, err)
	assert.False(t, sortPlanned)
	unsortedTasks, unsortedSegments, err := NewImportTasks(context.TODO(), fileGroups[:1], job, alloc, meta, nil,
		1*1024*1024*1024, sortPlanned)
	assert.NoError(t, err)
	if assert.Len(t, unsortedTasks, 1) {
		for _, segment := range unsortedSegments {
			assert.False(t, segment.GetIsInvisible())
		}
	}
	assert.Equal(t, 3, allocNCalls)

	// L0 imports never sort: their origins are the final segments and stay
	// visible, regardless of any legacy sorted IDs older binaries wrote.
	legacyL0Job := &importJob{ImportJob: &datapb.ImportJob{
		JobID:        2,
		CollectionID: job.GetCollectionID(),
		Schema:       job.GetSchema(),
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.L0Import, Value: "true"},
		},
	}}
	legacyL0Meta := NewMockImportMeta(t)

	l0Tasks, l0Segments, err := NewImportTasks(context.TODO(), fileGroups[:1], legacyL0Job, alloc, meta, legacyL0Meta, 1*1024*1024*1024, true)
	assert.NoError(t, err)
	l0SegmentsByID := lo.SliceToMap(l0Segments, func(segment *SegmentInfo) (int64, *SegmentInfo) {
		return segment.GetID(), segment
	})
	if assert.Len(t, l0Tasks, 1) {
		l0Task := l0Tasks[0].(*importTask)
		assert.Empty(t, l0Task.GetSortedSegmentIDs(), "legacy sorted IDs must not turn an L0 job into a sort plan")
		for _, segmentID := range l0Task.GetSegmentIDs() {
			assert.Nil(t, meta.GetSegment(context.Background(), segmentID))
			segment := l0SegmentsByID[segmentID]
			if assert.NotNil(t, segment) {
				assert.Equal(t, datapb.SegmentLevel_L0, segment.GetLevel())
				assert.False(t, segment.GetIsInvisible(), "L0 origins are the final imported segments")
			}
		}
	}
	assert.Equal(t, 4, allocNCalls,
		"L0 recovery allocates task IDs only and must not allocate replacement segment IDs")
}

func TestImportUtil_NewImportTasksWithDataTt(t *testing.T) {
	dataSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	fileGroups := [][]*datapb.ImportFileStats{
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 0, Paths: []string{"a.json"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 1, Paths: []string{"b.json"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 2, Paths: []string{"c.npy", "d.npy"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 3, Paths: []string{"e.npy", "f.npy"}},
				HashedStats: map[string]*datapb.PartitionImportStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 2,
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
			DataTs: 100,
		},
	}
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	tasks, segments, err := NewImportTasks(context.TODO(), fileGroups, job, alloc, meta, nil, 1*1024*1024*1024, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	assert.Len(t, segments, 6)
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		assert.Equal(t, 3, len(segmentIDs))
	}
}

func TestImportUtil_AssembleRequest(t *testing.T) {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       3,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var pt ImportTask = &preImportTask{
		importMeta: importMeta,
	}
	pt.(*preImportTask).task.Store(preImportTaskProto)
	preimportReq := AssemblePreImportRequest(pt, job)
	assert.Equal(t, pt.GetJobID(), preimportReq.GetJobID())
	assert.Equal(t, pt.GetTaskID(), preimportReq.GetTaskID())
	assert.Equal(t, pt.GetCollectionID(), preimportReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), preimportReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), preimportReq.GetVchannels())

	importTaskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       4,
		CollectionID: 1,
		SegmentIDs:   []int64{5, 6},
	}
	var task ImportTask = &importTask{
		importMeta: importMeta,
	}
	task.(*importTask).task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(800, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true},
	}
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)
	segment.ID = 6
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	importReq, err := AssembleImportRequest(task, job, meta, alloc)
	assert.NoError(t, err)
	assert.Equal(t, task.GetJobID(), importReq.GetJobID())
	assert.Equal(t, task.GetTaskID(), importReq.GetTaskID())
	assert.Equal(t, task.GetCollectionID(), importReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), importReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), importReq.GetVchannels())
}

func TestImportUtil_AssembleRequestWithDataTt(t *testing.T) {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}, DataTs: 100},
	}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       3,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var pt ImportTask = &preImportTask{
		importMeta: importMeta,
	}
	pt.(*preImportTask).task.Store(preImportTaskProto)
	preimportReq := AssemblePreImportRequest(pt, job)
	assert.Equal(t, pt.GetJobID(), preimportReq.GetJobID())
	assert.Equal(t, pt.GetTaskID(), preimportReq.GetTaskID())
	assert.Equal(t, pt.GetCollectionID(), preimportReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), preimportReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), preimportReq.GetVchannels())

	importTaskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       4,
		CollectionID: 1,
		SegmentIDs:   []int64{5, 6},
	}
	var task ImportTask = &importTask{
		importMeta: importMeta,
	}
	task.(*importTask).task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true},
	}
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)
	segment.ID = 6
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	importReq, err := AssembleImportRequest(task, job, meta, alloc)
	assert.NoError(t, err)
	assert.Equal(t, task.GetJobID(), importReq.GetJobID())
	assert.Equal(t, task.GetTaskID(), importReq.GetTaskID())
	assert.Equal(t, task.GetCollectionID(), importReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), importReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), importReq.GetVchannels())
}

func TestImportUtil_L0ImportUsesStorageV2WhenLoonFFIEnabled(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 2,
			PartitionIDs: []int64{3},
			Vchannels:    []string{"c0"},
			Options: []*commonpb.KeyValuePair{
				{Key: importutilv2.L0Import, Value: "true"},
			},
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
		},
	}
	taskProto := &datapb.ImportTaskV2{
		JobID:        job.GetJobID(),
		TaskID:       4,
		CollectionID: job.GetCollectionID(),
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: &internalpb.ImportFile{Id: 0, Paths: []string{"l0-prefix"}},
				HashedStats: map[string]*datapb.PartitionImportStats{
					"c0": {PartitionDataSize: map[int64]int64{3: 1}},
				},
			},
		},
	}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)
	task := &importTask{
		importMeta: importMeta,
	}
	task.task.Store(taskProto)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(10), nil)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(100), nil)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 1000, 1000 + n, nil
	})

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	segments, err := AssignSegments(job, task, alloc, meta, 1024)
	assert.NoError(t, err)
	assert.Equal(t, []int64{10}, segments)
	segment := meta.GetSegment(context.Background(), 10)
	assert.NotNil(t, segment)
	assert.Equal(t, datapb.SegmentLevel_L0, segment.GetLevel())
	assert.EqualValues(t, storage.StorageV2, segment.GetStorageVersion())

	importReq, err := AssembleImportRequest(task, job, meta, alloc)
	assert.NoError(t, err)
	assert.EqualValues(t, storage.StorageV2, importReq.GetStorageVersion())
	assert.False(t, importReq.GetUseLoonFfi())
}

func TestImportUtil_RegroupImportFiles(t *testing.T) {
	fileNum := 4096
	dataSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	threshold := paramtable.Get().DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt64() * 1024 * 1024

	files := make([]*datapb.ImportFileStats, 0, fileNum)
	for i := 0; i < fileNum; i++ {
		files = append(files, &datapb.ImportFileStats{
			ImportFile: &internalpb.ImportFile{
				Id:    int64(i),
				Paths: []string{fmt.Sprintf("%d.json", i)},
			},
			TotalMemorySize: dataSize * (rand.Int63n(99) + 1) / 100,
		})
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        1,
			CollectionID: 2,
			PartitionIDs: []int64{3, 4, 5, 6, 7},
			Vchannels:    []string{"v0", "v1", "v2", "v3"},
		},
	}

	groups := RegroupImportFiles(job, files, 1*1024*1024*1024)
	total := 0
	for i, fs := range groups {
		sum := lo.SumBy(fs, func(f *datapb.ImportFileStats) int64 {
			return f.GetTotalMemorySize()
		})
		assert.True(t, sum <= threshold)
		if i != len(groups)-1 {
			assert.True(t, len(fs) >= int(threshold/dataSize))
			assert.True(t, sum >= threshold-dataSize)
		}
		total += len(fs)
	}
	assert.Equal(t, fileNum, total)
}

func TestImportUtil_CheckDiskQuota(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 100,
		},
	}
	err = importMeta.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:  job.GetJobID(),
		TaskID: 1,
		FileStats: []*datapb.ImportFileStats{
			{TotalMemorySize: 1000 * 1024 * 1024},
			{TotalMemorySize: 2000 * 1024 * 1024},
		},
	}
	pit := &preImportTask{}
	pit.task.Store(preImportTaskProto)
	err = importMeta.AddTask(context.TODO(), pit)
	assert.NoError(t, err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "false")
	defer Params.Reset(Params.QuotaConfig.DiskProtectionEnabled.Key)
	_, err = CheckDiskQuota(context.TODO(), job, meta, importMeta)
	assert.NoError(t, err)

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 5, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			Stats: &datapb.Statistics{InsertBinlogSize: 3000 * 1024 * 1024},
		},
	}
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "true")
	job.Options = []*commonpb.KeyValuePair{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.SkipDQC, Value: "true"},
	}
	_, err = CheckDiskQuota(context.TODO(), job, meta, importMeta)
	assert.NoError(t, err)

	job.Options = nil
	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "10000")
	defer Params.Reset(Params.QuotaConfig.DiskQuota.Key)
	defer Params.Reset(Params.QuotaConfig.DiskQuotaPerCollection.Key)
	requestSize, err := CheckDiskQuota(context.TODO(), job, meta, importMeta)
	assert.NoError(t, err)
	assert.Equal(t, int64(3000*1024*1024), requestSize)

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "5000")
	_, err = CheckDiskQuota(context.TODO(), job, meta, importMeta)
	assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "5000")
	_, err = CheckDiskQuota(context.TODO(), job, meta, importMeta)
	assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
}

func TestImportUtil_DropImportTask(t *testing.T) {
	cluster := session.NewMockCluster(t)
	cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	taskProto := &datapb.ImportTaskV2{
		JobID:  0,
		TaskID: 1,
	}
	task := &importTask{}
	task.task.Store(taskProto)
	err = importMeta.AddTask(context.TODO(), task)
	assert.NoError(t, err)

	err = DropImportTask(task, cluster, importMeta)
	assert.NoError(t, err)

	// An already-unassigned task needs no worker RPC or metadata write.
	unassigned := &importTask{}
	unassigned.task.Store(&datapb.ImportTaskV2{TaskID: 2, NodeID: NullNodeID})
	assert.NoError(t, DropImportTask(unassigned, session.NewMockCluster(t), NewMockImportMeta(t)))
}

func TestImportUtil_ListBinlogsAndGroupBySegment(t *testing.T) {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"
	)

	t.Run("normal case", func(t *testing.T) {
		segmentInsertPaths := []string{
			// segment 435978159261483008
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008",
			// segment 435978159261483009
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009",
		}

		segmentDeltaPaths := []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483008",
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009",
		}

		cm := mocks2.NewChunkManager(t)
		cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, p := range segmentInsertPaths {
					if !cowf(&storage.ChunkObjectInfo{FilePath: p}) {
						return nil
					}
				}
				return nil
			})
		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, p := range segmentDeltaPaths {
					if !cowf(&storage.ChunkObjectInfo{FilePath: p}) {
						return nil
					}
				}
				return nil
			})

		file := &internalpb.ImportFile{
			Id:    1,
			Paths: []string{insertPrefix, deltaPrefix},
		}

		files, err := ListBinlogsAndGroupBySegment(context.Background(), cm, file)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(files))
		for _, f := range files {
			assert.Equal(t, 2, len(f.GetPaths()))
			for _, p := range f.GetPaths() {
				segmentID := path.Base(p)
				assert.True(t, segmentID == "435978159261483008" || segmentID == "435978159261483009")
			}
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		file := &internalpb.ImportFile{
			Paths: []string{},
		}
		_, err := ListBinlogsAndGroupBySegment(context.Background(), nil, file)
		assert.Error(t, err)
		t.Logf("%s", err)

		file.Paths = []string{insertPrefix, deltaPrefix, "dummy_prefix"}
		_, err = ListBinlogsAndGroupBySegment(context.Background(), nil, file)
		assert.Error(t, err)
		t.Logf("%s", err)
	})
}

func TestImportUtil_GetImportProgress(t *testing.T) {
	ctx := context.Background()
	mockErr := "mock err"

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTargets(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	importMeta, err := NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	file1 := &internalpb.ImportFile{
		Id:    1,
		Paths: []string{"a.json"},
	}
	file2 := &internalpb.ImportFile{
		Id:    2,
		Paths: []string{"b.json"},
	}
	file3 := &internalpb.ImportFile{
		Id:    3,
		Paths: []string{"c.json"},
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 0,
			Files: []*internalpb.ImportFile{file1, file2, file3},
		},
	}
	err = importMeta.AddJob(context.TODO(), job)
	assert.NoError(t, err)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:  job.GetJobID(),
		TaskID: 1,
		State:  datapb.ImportTaskStateV2_Completed,
		Reason: mockErr,
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: file1,
			},
			{
				ImportFile: file2,
			},
		},
	}

	pit1 := &preImportTask{}
	pit1.task.Store(preImportTaskProto)
	err = importMeta.AddTask(context.TODO(), pit1)
	assert.NoError(t, err)

	preImportTaskProto2 := &datapb.PreImportTask{
		JobID:  job.GetJobID(),
		TaskID: 2,
		State:  datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: file3,
			},
		},
	}
	pit2 := &preImportTask{}
	pit2.task.Store(preImportTaskProto2)
	err = importMeta.AddTask(context.TODO(), pit2)
	assert.NoError(t, err)

	taskProto1 := &datapb.ImportTaskV2{
		JobID:      job.GetJobID(),
		TaskID:     3,
		SegmentIDs: []int64{10, 11, 12},
		State:      datapb.ImportTaskStateV2_Pending,
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: file1,
				TotalRows:  100,
			},
			{
				ImportFile: file2,
				TotalRows:  200,
			},
		},
		SortedSegmentIDs: []int64{100, 110, 120},
	}
	it1 := &importTask{}
	it1.task.Store(taskProto1)
	err = importMeta.AddTask(context.TODO(), it1)
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 10, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 11, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 12, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)

	taskProto2 := &datapb.ImportTaskV2{
		JobID:      job.GetJobID(),
		TaskID:     4,
		SegmentIDs: []int64{20, 21, 22},
		State:      datapb.ImportTaskStateV2_Pending,
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: file3,
				TotalRows:  300,
			},
		},
		SortedSegmentIDs: []int64{200, 210, 220},
	}
	it2 := &importTask{}
	it2.task.Store(taskProto2)
	err = importMeta.AddTask(context.TODO(), it2)
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 20, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 21, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 22, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)

	// job does not exist
	progress, state, _, _, reason := GetJobProgress(ctx, -1, importMeta, meta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportJobState_Failed, state)
	assert.NotEqual(t, "", reason)

	// pending state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Pending))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(10), progress)
	assert.Equal(t, internalpb.ImportJobState_Pending, state)
	assert.Equal(t, "", reason)

	// preImporting state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(10+30), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// importing state, segmentImportedRows/totalRows = 0.5
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(10+30+30*0.5), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// importing state, segmentImportedRows/totalRows = 1
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(10, 100))
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(20, 100))
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(11, 100))
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(12, 100))
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(21, 100))
	assert.NoError(t, err)
	err = meta.UpdateSegmentsInfo(context.TODO(), UpdateImportedRows(22, 100))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(float32(10+30+30)), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// stats state, len(statsSegmentIDs) / (len(originalSegmentIDs) = 0.5
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Sorting))
	assert.NoError(t, err)

	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             100,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{10},
		},
	})
	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             110,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{11},
		},
	})
	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             120,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{12},
		},
	})
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(10+30+30+10*0.5), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             200,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{20},
		},
	})
	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             210,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{21},
		},
	})
	_ = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             220,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{22},
		},
	})
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(10+30+30+10), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// auto-commit jobs should not expose transient commit states to progress callers.
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), func(job ImportJob) {
		job.(*importJob).AutoCommit = true
	}, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Committing))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), func(job ImportJob) {
		job.(*importJob).AutoCommit = false
	}, UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Uncommitted, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Committing))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Committing, state)
	assert.Equal(t, "", reason)

	// completed state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	progress, state, _, _, reason = GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(100), progress)
	assert.Equal(t, internalpb.ImportJobState_Completed, state)
	assert.Equal(t, "", reason)
}

func TestPreImportTask_MarshalJSON(t *testing.T) {
	taskProto := &datapb.PreImportTask{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		NodeID:       4,
		State:        datapb.ImportTaskStateV2_Pending,
		Reason:       "test reason",
		CreatedTime:  time.Now().Format(time.RFC3339),
		CompleteTime: time.Now().Add(time.Hour).Format(time.RFC3339),
	}
	task := &preImportTask{
		tr: timerecord.NewTimeRecorder("test"),
	}
	task.task.Store(taskProto)
	jsonData, err := task.MarshalJSON()
	assert.NoError(t, err)

	var importTask metricsinfo.ImportTask
	err = json.Unmarshal(jsonData, &importTask)
	assert.NoError(t, err)

	assert.Equal(t, task.GetJobID(), importTask.JobID)
	assert.Equal(t, task.GetTaskID(), importTask.TaskID)
	assert.Equal(t, task.GetCollectionID(), importTask.CollectionID)
	assert.Equal(t, task.GetNodeID(), importTask.NodeID)
	assert.Equal(t, task.GetState().String(), importTask.State)
	assert.Equal(t, task.GetReason(), importTask.Reason)
	assert.Equal(t, "PreImportTask", importTask.TaskType)
	assert.Equal(t, task.GetCreatedTime(), importTask.CreatedTime)
	assert.Equal(t, task.GetCompleteTime(), importTask.CompleteTime)
}

func TestImportTask_MarshalJSON(t *testing.T) {
	taskProto := &datapb.ImportTaskV2{
		JobID:        1,
		TaskID:       2,
		CollectionID: 3,
		NodeID:       4,
		State:        datapb.ImportTaskStateV2_Pending,
		Reason:       "test reason",
		CreatedTime:  time.Now().Format(time.RFC3339),
		CompleteTime: time.Now().Add(time.Hour).Format(time.RFC3339),
	}
	task := &importTask{
		tr: timerecord.NewTimeRecorder("test"),
	}
	task.task.Store(taskProto)
	jsonData, err := task.MarshalJSON()
	assert.NoError(t, err)

	var importTask metricsinfo.ImportTask
	err = json.Unmarshal(jsonData, &importTask)
	assert.NoError(t, err)

	assert.Equal(t, task.GetJobID(), importTask.JobID)
	assert.Equal(t, task.GetTaskID(), importTask.TaskID)
	assert.Equal(t, task.GetCollectionID(), importTask.CollectionID)
	assert.Equal(t, task.GetNodeID(), importTask.NodeID)
	assert.Equal(t, task.GetState().String(), importTask.State)
	assert.Equal(t, task.GetReason(), importTask.Reason)
	assert.Equal(t, "ImportTask", importTask.TaskType)
	assert.Equal(t, task.GetCreatedTime(), importTask.CreatedTime)
	assert.Equal(t, task.GetCompleteTime(), importTask.CompleteTime)
}

func TestLogResultSegmentsInfo(t *testing.T) {
	// Create mock catalog and broker
	mockCatalog := mocks.NewDataCoordCatalog(t)
	meta := &meta{
		segments: NewSegmentsInfo(),
		catalog:  mockCatalog,
	}

	// Create test segments
	segments := []*SegmentInfo{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "ch1",
				NumOfRows:     100,
				State:         commonpb.SegmentState_Flushed,
			},
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            2,
				CollectionID:  1,
				PartitionID:   1,
				InsertChannel: "ch1",
				NumOfRows:     200,
				State:         commonpb.SegmentState_Flushed,
			},
		},
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            3,
				CollectionID:  1,
				PartitionID:   2,
				InsertChannel: "ch2",
				NumOfRows:     300,
				State:         commonpb.SegmentState_Flushed,
			},
		},
	}

	// Add segments to meta
	for _, segment := range segments {
		meta.segments.SetSegment(segment.ID, segment)
	}

	jobID := int64(2)
	segmentIDs := []int64{1, 2, 3}

	// Call the function
	LogResultSegmentsInfo(jobID, meta, segmentIDs)
}

// TestImportUtil_ValidateBinlogImportRequest tests the validation of binlog import request
func TestImportUtil_ValidateBinlogImportRequest(t *testing.T) {
	ctx := context.Background()
	mockCM := mocks2.NewChunkManager(t)

	t.Run("empty files", func(t *testing.T) {
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		err := ValidateBinlogImportRequest(ctx, mockCM, nil, options)
		assert.Error(t, err)
	})

	t.Run("valid files - not backup", func(t *testing.T) {
		files := []*msgpb.ImportFile{
			{
				Id:    1,
				Paths: []string{"path1"},
			},
		}
		err := ValidateBinlogImportRequest(ctx, mockCM, files, nil)
		assert.NoError(t, err)
	})

	t.Run("invalid files - too many paths", func(t *testing.T) {
		files := []*msgpb.ImportFile{
			{
				Id:    1,
				Paths: []string{"path1", "path2", "path3"},
			},
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		err := ValidateBinlogImportRequest(ctx, mockCM, files, options)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many input paths")
	})
}

// TestImportUtil_ListBinlogImportRequestFiles tests listing binlog files from import request
func TestImportUtil_ListBinlogImportRequestFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("empty files", func(t *testing.T) {
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		files, err := ListBinlogImportRequestFiles(ctx, nil, nil, options)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no binlog to import")
		assert.Nil(t, files)
	})

	t.Run("not backup files", func(t *testing.T) {
		reqFiles := []*internalpb.ImportFile{
			{
				Paths: []string{"path1"},
			},
		}
		files, err := ListBinlogImportRequestFiles(ctx, nil, reqFiles, nil)
		assert.NoError(t, err)
		assert.Equal(t, reqFiles, files)
	})

	t.Run("backup files - list error", func(t *testing.T) {
		reqFiles := []*internalpb.ImportFile{
			{
				Paths: []string{"path1"},
			},
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		mockCM := mocks2.NewChunkManager(t)
		mockCM.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles, options)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "list binlogs failed")
		assert.Nil(t, files)
	})

	t.Run("backup files - success", func(t *testing.T) {
		reqFiles := []*internalpb.ImportFile{
			{
				Paths: []string{"path1"},
			},
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		mockCM := mocks2.NewChunkManager(t)
		mockCM.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
				walkFunc(&storage.ChunkObjectInfo{
					FilePath: "path1",
				})
				return nil
			})
		files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles, options)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files))
		assert.Equal(t, "path1", files[0].GetPaths()[0])
	})

	t.Run("backup files - empty result", func(t *testing.T) {
		reqFiles := []*internalpb.ImportFile{
			{
				Paths: []string{"path1"},
			},
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		mockCM := mocks2.NewChunkManager(t)
		mockCM.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
				return nil
			})
		files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles, options)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no binlog to import")
		assert.Nil(t, files)
	})

	t.Run("backup files - too many files", func(t *testing.T) {
		maxFiles := paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt()
		reqFiles := make([]*internalpb.ImportFile, maxFiles+1)
		for i := 0; i < maxFiles+1; i++ {
			reqFiles[i] = &internalpb.ImportFile{
				Paths: []string{fmt.Sprintf("path%d", i)},
			}
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		mockCM := mocks2.NewChunkManager(t)
		mockCM.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
				for i := 0; i < maxFiles+1; i++ {
					walkFunc(&storage.ChunkObjectInfo{
						FilePath: fmt.Sprintf("path%d", i),
					})
				}
				return nil
			})
		files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles, options)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("The max number of import files should not exceed %d", maxFiles))
		assert.Nil(t, files)
	})

	t.Run("backup files - multiple files with delta", func(t *testing.T) {
		reqFiles := []*internalpb.ImportFile{
			{
				Paths: []string{"insert/path1", "delta/path1"},
			},
		}
		options := []*commonpb.KeyValuePair{
			{
				Key:   importutilv2.BackupFlag,
				Value: "true",
			},
		}
		mockCM := mocks2.NewChunkManager(t)
		mockCM.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
				if strings.Contains(prefix, "insert") {
					walkFunc(&storage.ChunkObjectInfo{
						FilePath: "insert/path1",
					})
				} else if strings.Contains(prefix, "delta") {
					walkFunc(&storage.ChunkObjectInfo{
						FilePath: "delta/path1",
					})
				}
				return nil
			}).Times(2)
		files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles, options)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files))
		assert.Equal(t, 2, len(files[0].GetPaths()))
		assert.Equal(t, "insert/path1", files[0].GetPaths()[0])
		assert.Equal(t, "delta/path1", files[0].GetPaths()[1])
	})
}

// TestImportUtil_ValidateMaxImportJobExceed tests validation of maximum import jobs
func TestImportUtil_ValidateMaxImportJobExceed(t *testing.T) {
	ctx := context.Background()

	t.Run("job count within limit", func(t *testing.T) {
		mockImportMeta := NewMockImportMeta(t)
		mockImportMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).Return(1)
		err := ValidateMaxImportJobExceed(ctx, mockImportMeta)
		assert.NoError(t, err)
	})

	t.Run("job count exceeds limit", func(t *testing.T) {
		mockImportMeta := NewMockImportMeta(t)
		mockImportMeta.EXPECT().CountJobBy(mock.Anything, mock.Anything).
			Return(paramtable.Get().DataCoordCfg.MaxImportJobNum.GetAsInt() + 1)
		err := ValidateMaxImportJobExceed(ctx, mockImportMeta)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "The number of jobs has reached the limit")
	})
}

func TestImportUtil_AssembleRequestCarriesPKRange(t *testing.T) {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	// import task whose file carries a primary-allocated PK range
	importTaskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       4,
		CollectionID: 1,
		SegmentIDs:   []int64{5},
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: &internalpb.ImportFile{Id: 1, Paths: []string{"f1"}, PreAllocatedAutoIds: &commonpb.IDRange{Begin: 5000, End: 5100}},
				TotalRows:  50,
			},
		},
	}
	var task ImportTask = &importTask{importMeta: importMeta}
	task.(*importTask).task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(800, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)
	err = meta.AddSegment(context.Background(), &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true}})
	assert.NoError(t, err)

	importReq, err := AssembleImportRequest(task, job, meta, alloc)
	assert.NoError(t, err)
	// PK range is carried through to the datanode request, per file.
	assert.Equal(t, int64(5000), importReq.GetFiles()[0].GetPreAllocatedAutoIds().GetBegin())
	assert.Equal(t, int64(5100), importReq.GetFiles()[0].GetPreAllocatedAutoIds().GetEnd())
	// logID IDRange is still allocated locally and independently.
	assert.Greater(t, importReq.GetIDRange().GetEnd(), importReq.GetIDRange().GetBegin())
}

// The estimate path reserves a capped range rather than refusing an oversized
// bound, so this guard is what catches a file that really does hold more rows
// than its reservation. It must fire before any segment is written, and it must
// carry the sentinel so CreateTaskOnWorker fails the job instead of retrying a
// number that will never change.
func TestImportUtil_AssembleRefusesAnUnderSizedPKRange(t *testing.T) {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job).Maybe()

	// 100 ids reserved from the upper bound; pre-import then found 101 real rows.
	importTaskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       4,
		CollectionID: 1,
		// No segments, so the guard is reached without a meta to look them up in.
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: &internalpb.ImportFile{Id: 1, Paths: []string{"f1"}, PreAllocatedAutoIds: &commonpb.IDRange{Begin: 5000, End: 5100}},
				TotalRows:  101,
			},
		},
	}
	var task ImportTask = &importTask{importMeta: importMeta}
	task.(*importTask).task.Store(importTaskProto)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 1, 1 + n, nil
	}).Maybe()
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(800, nil).Maybe()

	// meta is only read after this guard, so the guard is reached without one.
	_, err := AssembleImportRequest(task, job, nil, alloc)
	require.Error(t, err)
	// cockroachdb/errors carries the marker, which stdlib errors.Is does not walk --
	// the same traversal CreateTaskOnWorker uses to reach its terminal branch.
	assert.True(t, errors.Is(err, ErrPKRangeTooSmall),
		"the sentinel must survive so the scheduler fails the job instead of retrying")
	assert.Contains(t, err.Error(), "101 rows, 100 ids reserved")
}

// The scheduler must be able to separate the one terminal assemble failure from
// the retriable ones. It cannot do that on merr classification: ErrImportSysFailed
// also carries transient cases, and merr.IsNonRetryableErr is a deny-list over
// ErrIo* sentinels AssembleImportRequest never returns -- so the branch that used
// it was unreachable for every failure it was written for.
func TestErrPKRangeTooSmall_IsDistinguishableAndKeepsItsCode(t *testing.T) {
	terminal := merr.Mark(merr.WrapErrImportSysFailedMsg(
		"reserved PK range too small for file %v: %d rows, %d ids reserved",
		[]string{"a.npy"}, 100, 10), ErrPKRangeTooSmall)

	assert.True(t, errors.Is(terminal, ErrPKRangeTooSmall))
	assert.Equal(t, merr.Code(merr.ErrImportSysFailed), merr.Code(terminal),
		"marking must not replace the merr code the wire projection carries")
	assert.Contains(t, terminal.Error(), "100 rows, 10 ids reserved")

	// merr.Mark carries a cockroachdb marker, which the standard library's
	// errors.Is does not resolve. CreateTaskOnWorker must use the cockroachdb
	// package -- which depguard already enforces repo-wide, so this cannot regress
	// by an accidental import swap.

	// The transient shape AssembleImportRequest and its callees also return: same
	// merr code, and it must NOT be treated as terminal.
	transient := merr.WrapErrImportSysFailedMsg("job %d not found, waiting for import job creation", 1)
	assert.False(t, errors.Is(transient, ErrPKRangeTooSmall))

	// The deny-list helper the old branch used returns false for both, which is
	// why the branch never fired.
	assert.False(t, merr.IsNonRetryableErr(terminal))
	assert.False(t, merr.IsNonRetryableErr(transient))
}

// A new import reads the compaction switch once. Persisted origin visibility
// keeps that decision stable across config changes and retries.
func TestImportSortPlanned(t *testing.T) {
	ctx := context.Background()
	segmentMeta := &meta{segments: NewSegmentsInfo()}
	mk := func(taskID, segmentID int64, invisible bool) ImportTask {
		task := &importTask{}
		task.task.Store(&datapb.ImportTaskV2{TaskID: taskID, SegmentIDs: []int64{segmentID}})
		segmentMeta.segments.SetSegment(segmentID, NewSegmentInfo(&datapb.SegmentInfo{
			ID:          segmentID,
			IsInvisible: invisible,
		}))
		return task
	}

	oldCompaction := Params.DataCoordCfg.EnableCompaction.SwapTempValue("true")
	t.Cleanup(func() {
		Params.DataCoordCfg.EnableCompaction.SwapTempValue(oldCompaction)
	})

	normalJob := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	planned, err := importSortPlannedForJob(ctx, normalJob, nil, segmentMeta)
	assert.NoError(t, err)
	assert.True(t, planned)

	Params.DataCoordCfg.EnableCompaction.SwapTempValue("false")
	planned, err = importSortPlannedForJob(ctx, normalJob, nil, segmentMeta)
	assert.NoError(t, err)
	assert.False(t, planned, "new imports must honor the compaction switch")

	planned, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{mk(1, 10, true)}, segmentMeta)
	assert.NoError(t, err)
	assert.True(t, planned, "an existing invisible origin still owes its sort")

	Params.DataCoordCfg.EnableCompaction.SwapTempValue("true")
	planned, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{mk(2, 20, false)}, segmentMeta)
	assert.NoError(t, err)
	assert.False(t, planned, "an existing visible origin stays the final output")

	legacy := mk(3, 30, false).(*importTask)
	legacy.task.Load().SortedSegmentIDs = []int64{31}
	planned, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{legacy}, segmentMeta)
	assert.NoError(t, err)
	assert.True(t, planned, "legacy preallocated sorted IDs preserve the old sort plan")
	_, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{legacy, mk(9, 90, false)}, segmentMeta)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)

	l0Job := &importJob{ImportJob: &datapb.ImportJob{
		JobID:   4,
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}
	planned, err = importSortPlannedForJob(ctx, l0Job, []ImportTask{mk(4, 40, true)}, segmentMeta)
	assert.NoError(t, err)
	assert.False(t, planned, "L0 imports never sort")

	_, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{
		mk(5, 50, true),
		mk(6, 60, false),
	}, segmentMeta)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)

	missing := &importTask{}
	missing.task.Store(&datapb.ImportTaskV2{TaskID: 7, SegmentIDs: []int64{70}})
	_, err = importSortPlannedForJob(ctx, normalJob, []ImportTask{missing}, segmentMeta)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)

	t.Run("corrupt task values still fail closed", func(t *testing.T) {
		var typedNil *importTask
		wrongType := &preImportTask{}
		wrongType.task.Store(&datapb.PreImportTask{TaskID: 8})
		for name, corruptTask := range map[string]ImportTask{
			"nil":        nil,
			"typed nil":  typedNil,
			"wrong type": wrongType,
		} {
			t.Run(name, func(t *testing.T) {
				assert.NotPanics(t, func() {
					_, err := importSortPlannedForJob(ctx, normalJob, []ImportTask{corruptTask}, segmentMeta)
					assert.ErrorIs(t, err, merr.ErrImportSysFailed)
				})
			})
		}
	})
}

func TestImportProgressIgnoresLegacyL0SortedIDs(t *testing.T) {
	const jobID = int64(100)
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: jobID,
		Options: []*commonpb.KeyValuePair{
			{Key: importutilv2.L0Import, Value: "true"},
		},
	}}
	task := &importTask{}
	task.task.Store(&datapb.ImportTaskV2{
		JobID:            jobID,
		TaskID:           101,
		SegmentIDs:       []int64{102},
		SortedSegmentIDs: []int64{103},
	})
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetTaskByJob(mock.Anything, jobID, mock.Anything).
		Return([]ImportTask{task}).Once()
	importMeta.EXPECT().GetJob(mock.Anything, jobID).Return(job).Twice()

	assert.Equal(t, float32(1), getStatsProgress(context.Background(), jobID, importMeta, nil))
	assert.Equal(t, float32(1), getIndexBuildingProgress(context.Background(), jobID, importMeta, nil))
}
