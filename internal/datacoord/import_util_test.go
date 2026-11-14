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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestImportUtil_NewImportTasks(t *testing.T) {
	dm := &domainMetas{}

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
	job := &bulkimport.Job{
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
		},
	}
	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(rand.Uint64(), nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
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
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)

	tasks, err := bulkimport.NewImportTasks(fileGroups, job, alloc, mt, nil, 1*1024*1024*1024)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	for _, task := range tasks {
		segmentIDs := task.(*bulkimport.Task).GetSegmentIDs()
		assert.Equal(t, 3, len(segmentIDs))
	}
}

func TestImportUtil_NewImportTasksWithDataTt(t *testing.T) {
	dm := &domainMetas{}

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
	job := &bulkimport.Job{
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)

	tasks, err := bulkimport.NewImportTasks(fileGroups, job, alloc, mt, nil, 1*1024*1024*1024)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	for _, task := range tasks {
		segmentIDs := task.(*bulkimport.Task).GetSegmentIDs()
		assert.Equal(t, 3, len(segmentIDs))
	}
}

func TestImportUtil_AssembleRequest(t *testing.T) {
	dm := &domainMetas{}

	var job bulkimport.ImportJob = &bulkimport.Job{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 1,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"v0"},
		},
	}
	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       3,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var pt bulkimport.ImportTask = &bulkimport.PreImportTask{
		ImportMeta: importMeta,
	}
	pt.(*bulkimport.PreImportTask).Task.Store(preImportTaskProto)
	preimportReq, err := bulkimport.AssemblePreImportRequest(pt, job)
	assert.NoError(t, err)
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
	var task bulkimport.ImportTask = &bulkimport.Task{
		ImportMeta: importMeta,
	}
	task.(*bulkimport.Task).Task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
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
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true},
	}
	err = mt.AddSegment(context.Background(), segment)
	assert.NoError(t, err)
	segment.ID = 6
	err = mt.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	importReq, err := bulkimport.AssembleImportRequest(task, job, mt, alloc)
	assert.NoError(t, err)
	assert.Equal(t, task.GetJobID(), importReq.GetJobID())
	assert.Equal(t, task.GetTaskID(), importReq.GetTaskID())
	assert.Equal(t, task.GetCollectionID(), importReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), importReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), importReq.GetVchannels())
}

func TestImportUtil_AssembleRequestWithDataTt(t *testing.T) {
	dm := &domainMetas{}

	var job bulkimport.ImportJob = &bulkimport.Job{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}, DataTs: 100},
	}
	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	preImportTaskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       3,
		CollectionID: 1,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var pt bulkimport.ImportTask = &bulkimport.PreImportTask{
		ImportMeta: importMeta,
	}
	pt.(*bulkimport.PreImportTask).Task.Store(preImportTaskProto)
	preimportReq, err := bulkimport.AssemblePreImportRequest(pt, job)
	assert.NoError(t, err)
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
	var task bulkimport.ImportTask = &bulkimport.Task{
		ImportMeta: importMeta,
	}
	task.(*bulkimport.Task).Task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
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

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{}, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)
	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true},
	}
	err = mt.AddSegment(context.Background(), segment)
	assert.NoError(t, err)
	segment.ID = 6
	err = mt.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	importReq, err := bulkimport.AssembleImportRequest(task, job, mt, alloc)
	assert.NoError(t, err)
	assert.Equal(t, task.GetJobID(), importReq.GetJobID())
	assert.Equal(t, task.GetTaskID(), importReq.GetTaskID())
	assert.Equal(t, task.GetCollectionID(), importReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), importReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), importReq.GetVchannels())
}

func TestImportUtil_L0ImportUsesStorageV2WhenLoonFFIEnabled(t *testing.T) {
	dm := &domainMetas{}

	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)

	job := &bulkimport.Job{
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
	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)
	task := &bulkimport.Task{
		ImportMeta: importMeta,
	}
	task.Task.Store(taskProto)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocID(mock.Anything).Return(int64(10), nil)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(uint64(100), nil)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		return 1000, 1000 + n, nil
	})

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil).Maybe()

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)

	segments, err := bulkimport.AssignSegments(job, task, alloc, mt, 1024)
	assert.NoError(t, err)
	assert.Equal(t, []int64{10}, segments)
	segment := mt.GetSegment(context.Background(), 10)
	assert.NotNil(t, segment)
	assert.Equal(t, datapb.SegmentLevel_L0, segment.GetLevel())
	assert.EqualValues(t, storage.StorageV2, segment.GetStorageVersion())

	importReq, err := bulkimport.AssembleImportRequest(task, job, mt, alloc)
	assert.NoError(t, err)
	assert.EqualValues(t, storage.StorageV2, importReq.GetStorageVersion())
	assert.False(t, importReq.GetUseLoonFfi())
}

func TestImportUtil_CheckDiskQuota(t *testing.T) {
	dm := &domainMetas{}

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	importMeta, err := bulkimport.NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)

	job := &bulkimport.Job{
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
	pit := &bulkimport.PreImportTask{}
	pit.Task.Store(preImportTaskProto)
	err = importMeta.AddTask(context.TODO(), pit)
	assert.NoError(t, err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "false")
	defer Params.Reset(Params.QuotaConfig.DiskProtectionEnabled.Key)
	_, err = bulkimport.CheckDiskQuota(context.TODO(), job, mt, importMeta)
	assert.NoError(t, err)

	segment := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 5, CollectionID: 100, State: commonpb.SegmentState_Flushed,
			Stats: &datapb.Statistics{InsertBinlogSize: 3000 * 1024 * 1024},
		},
	}
	err = mt.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "true")
	job.Options = []*commonpb.KeyValuePair{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.SkipDQC, Value: "true"},
	}
	_, err = bulkimport.CheckDiskQuota(context.TODO(), job, mt, importMeta)
	assert.NoError(t, err)

	job.Options = nil
	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "10000")
	defer Params.Reset(Params.QuotaConfig.DiskQuota.Key)
	defer Params.Reset(Params.QuotaConfig.DiskQuotaPerCollection.Key)
	requestSize, err := bulkimport.CheckDiskQuota(context.TODO(), job, mt, importMeta)
	assert.NoError(t, err)
	assert.Equal(t, int64(3000*1024*1024), requestSize)

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "5000")
	_, err = bulkimport.CheckDiskQuota(context.TODO(), job, mt, importMeta)
	assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "5000")
	_, err = bulkimport.CheckDiskQuota(context.TODO(), job, mt, importMeta)
	assert.True(t, errors.Is(err, merr.ErrServiceQuotaExceeded))
}

func TestImportUtil_GetImportProgress(t *testing.T) {
	dm := &domainMetas{}

	ctx := context.Background()
	mockErr := "mock err"

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
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	importMeta, err := bulkimport.NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
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
	job := &bulkimport.Job{
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

	pit1 := &bulkimport.PreImportTask{}
	pit1.Task.Store(preImportTaskProto)
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
	pit2 := &bulkimport.PreImportTask{}
	pit2.Task.Store(preImportTaskProto2)
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
	it1 := &bulkimport.Task{}
	it1.Task.Store(taskProto1)
	err = importMeta.AddTask(context.TODO(), it1)
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 10, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 11, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
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
	it2 := &bulkimport.Task{}
	it2.Task.Store(taskProto2)
	err = importMeta.AddTask(context.TODO(), it2)
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 20, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 21, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)
	err = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 22, IsImporting: true, State: commonpb.SegmentState_Flushed, NumOfRows: 50},
	})
	assert.NoError(t, err)

	// job does not exist
	progress, state, _, _, reason := bulkimport.GetJobProgress(ctx, -1, importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportJobState_Failed, state)
	assert.NotEqual(t, "", reason)

	// pending state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Pending))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(10), progress)
	assert.Equal(t, internalpb.ImportJobState_Pending, state)
	assert.Equal(t, "", reason)

	// preImporting state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_PreImporting))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(10+30), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// importing state, segmentImportedRows/totalRows = 0.5
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Importing))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(10+30+30*0.5), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// importing state, segmentImportedRows/totalRows = 1
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(10, 100))
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(20, 100))
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(11, 100))
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(12, 100))
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(21, 100))
	assert.NoError(t, err)
	err = mt.UpdateSegmentsInfo(context.TODO(), meta.UpdateImportedRows(22, 100))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(float32(10+30+30)), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// stats state, len(statsSegmentIDs) / (len(originalSegmentIDs) = 0.5
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Sorting))
	assert.NoError(t, err)

	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             100,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{10},
		},
	})
	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             110,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{11},
		},
	})
	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             120,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{12},
		},
	})
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(10+30+30+10*0.5), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             200,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{20},
		},
	})
	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             210,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{21},
		},
	})
	_ = mt.AddSegment(ctx, &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             220,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{22},
		},
	})
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(10+30+30+10), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	// auto-commit jobs should not expose transient commit states to progress callers.
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), func(job bulkimport.ImportJob) {
		job.(*bulkimport.Job).AutoCommit = true
	}, bulkimport.UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Committing))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Importing, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), func(job bulkimport.ImportJob) {
		job.(*bulkimport.Job).AutoCommit = false
	}, bulkimport.UpdateJobState(internalpb.ImportJobState_Uncommitted))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Uncommitted, state)
	assert.Equal(t, "", reason)

	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Committing))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(99), progress)
	assert.Equal(t, internalpb.ImportJobState_Committing, state)
	assert.Equal(t, "", reason)

	// completed state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Completed))
	assert.NoError(t, err)
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(100), progress)
	assert.Equal(t, internalpb.ImportJobState_Completed, state)
	assert.Equal(t, "", reason)
}

// TestImportUtil_GetV3ImportProgress pins the Import V3 progress bands:
// 10 Pending, 5 PreImporting/AssigningIDRange, 30 Resharding, 5 Planning,
// 10 Importing, 30 IndexBuilding, 10 Completed. An empty job drives every
// per-stage helper to its completed fraction, so each state lands on a band
// boundary.
func TestImportUtil_GetV3ImportProgress(t *testing.T) {
	dm := &domainMetas{}

	ctx := context.Background()

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
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil)

	importMeta, err := bulkimport.NewImportMeta(context.TODO(), catalog, nil, nil)
	assert.NoError(t, err)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)

	job := &bulkimport.Job{ImportJob: &datapb.ImportJob{
		Version: internalpb.ImportVersion_ImportVersionV3,
	}}
	assert.NoError(t, importMeta.AddJob(ctx, job))

	for _, tc := range []struct {
		state     internalpb.ImportJobState
		progress  int64
		userState internalpb.ImportJobState
	}{
		{internalpb.ImportJobState_Pending, 10, internalpb.ImportJobState_Pending},
		{internalpb.ImportJobState_PreImporting, 10 + 5, internalpb.ImportJobState_Importing},
		{internalpb.ImportJobState_AssigningIDRange, 10 + 5, internalpb.ImportJobState_Importing},
		{internalpb.ImportJobState_Resharding, 10 + 5 + 30, internalpb.ImportJobState_Importing},
		{internalpb.ImportJobState_Planning, 10 + 5 + 30, internalpb.ImportJobState_Importing},
		{internalpb.ImportJobState_Importing, 10 + 5 + 30 + 5 + 10, internalpb.ImportJobState_Importing},
		{internalpb.ImportJobState_IndexBuilding, 10 + 5 + 30 + 5 + 10 + 30, internalpb.ImportJobState_Importing},
	} {
		t.Run(tc.state.String(), func(t *testing.T) {
			assert.NoError(t, importMeta.UpdateJob(ctx, job.GetJobID(), bulkimport.UpdateJobState(tc.state)))
			progress, state, _, _, reason := bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
			assert.Equal(t, tc.progress, progress)
			assert.Equal(t, tc.userState, state)
			assert.Equal(t, "", reason)
		})
	}

	// Terminal states. UpdateJob refuses to leave Completed/Failed, so drive
	// each on its own job.
	assert.NoError(t, importMeta.UpdateJob(ctx, job.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Completed)))
	progress, state, _, _, reason := bulkimport.GetJobProgress(ctx, job.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(100), progress)
	assert.Equal(t, internalpb.ImportJobState_Completed, state)
	assert.Equal(t, "", reason)

	failed := &bulkimport.Job{ImportJob: &datapb.ImportJob{
		JobID:   1,
		Version: internalpb.ImportVersion_ImportVersionV3,
	}}
	assert.NoError(t, importMeta.AddJob(ctx, failed))
	assert.NoError(t, importMeta.UpdateJob(ctx, failed.GetJobID(), bulkimport.UpdateJobState(internalpb.ImportJobState_Failed), bulkimport.UpdateJobReason("boom")))
	progress, state, _, _, reason = bulkimport.GetJobProgress(ctx, failed.GetJobID(), importMeta, mt, dm.indexMeta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportJobState_Failed, state)
	assert.Equal(t, "boom", reason)
}

func TestImportUtil_AssembleRequestCarriesIDRange(t *testing.T) {
	dm := &domainMetas{}

	var job bulkimport.ImportJob = &bulkimport.Job{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}
	importMeta := bulkimport.NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job)

	// import task whose file carries a primary-allocated exact ID range
	importTaskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       4,
		CollectionID: 1,
		SegmentIDs:   []int64{5},
		FileStats: []*datapb.ImportFileStats{
			{
				ImportFile: &internalpb.ImportFile{Id: 1, Paths: []string{"f1"}, IdRange: &commonpb.IDRange{Begin: 5000, End: 5050}},
				TotalRows:  50,
			},
		},
	}
	var task bulkimport.ImportTask = &bulkimport.Task{ImportMeta: importMeta}
	task.(*bulkimport.Task).Task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListSnapshots(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshJobs(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListExternalCollectionRefreshTasks(mock.Anything).Return(nil, nil).Maybe()

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(800, nil)

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	mt, err := meta.NewMeta(context.TODO(), catalog, nil, broker, loadDomainMetas(catalog, nil, dm, false))
	assert.NoError(t, err)
	err = mt.AddSegment(context.Background(), &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true}})
	assert.NoError(t, err)

	importReq, err := bulkimport.AssembleImportRequest(task, job, mt, alloc)
	assert.NoError(t, err)
	// ID range is carried through to the datanode request, per file.
	assert.Equal(t, int64(5000), importReq.GetFiles()[0].GetIdRange().GetBegin())
	assert.Equal(t, int64(5050), importReq.GetFiles()[0].GetIdRange().GetEnd())
	// logID IDRange is still allocated locally and independently.
	assert.Greater(t, importReq.GetIDRange().GetEnd(), importReq.GetIDRange().GetBegin())
}
