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
	"os"
	"path"
	"path/filepath"
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
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

	broker := broker.NewMockBroker(t)
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(context.TODO(), catalog, nil, broker)
	assert.NoError(t, err)

	tasks, err := NewImportTasks(fileGroups, job, alloc, meta, nil, 1*1024*1024*1024)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		assert.Equal(t, 3, len(segmentIDs))
	}
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
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

	tasks, err := NewImportTasks(fileGroups, job, alloc, meta, nil, 1*1024*1024*1024)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportV2Tasks(mock.Anything).Return(nil, nil).Maybe()
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportV2Tasks(mock.Anything).Return(nil, nil).Maybe()
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
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportV2Tasks(mock.Anything).Return(nil, nil).Maybe()
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

// TestImportUtil_GetV3ImportProgress pins the Import V3 progress bands:
// 10 Pending, 5 PreImporting/AssigningIDRange, 30 Resharding, 5 Planning,
// 10 Importing, 30 IndexBuilding, 10 Completed. An empty job drives every
// per-stage helper to its completed fraction, so each state lands on a band
// boundary.
func TestImportUtil_GetV3ImportProgress(t *testing.T) {
	ctx := context.Background()

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListReshardTasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasksV3(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPreImportV2Tasks(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
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

	job := &importJob{ImportJob: &datapb.ImportJob{
		Version: datapb.ImportJobVersion_ImportJobVersionV3,
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
			assert.NoError(t, importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobState(tc.state)))
			progress, state, _, _, reason := GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
			assert.Equal(t, tc.progress, progress)
			assert.Equal(t, tc.userState, state)
			assert.Equal(t, "", reason)
		})
	}

	// Terminal states. UpdateJob refuses to leave Completed/Failed, so drive
	// each on its own job.
	assert.NoError(t, importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Completed)))
	progress, state, _, _, reason := GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(100), progress)
	assert.Equal(t, internalpb.ImportJobState_Completed, state)
	assert.Equal(t, "", reason)

	failed := &importJob{ImportJob: &datapb.ImportJob{
		JobID:   1,
		Version: datapb.ImportJobVersion_ImportJobVersionV3,
	}}
	assert.NoError(t, importMeta.AddJob(ctx, failed))
	assert.NoError(t, importMeta.UpdateJob(ctx, failed.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason("boom")))
	progress, state, _, _, reason = GetJobProgress(ctx, failed.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportJobState_Failed, state)
	assert.Equal(t, "boom", reason)
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
	mockCatalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
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

	t.Run("backup files - invalid paths take precedence over storage errors", func(t *testing.T) {
		for _, invalidPaths := range [][]string{nil, {"insert", "delta", "extra"}} {
			for _, invalidFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("paths_%d/invalid_first_%t", len(invalidPaths), invalidFirst), func(t *testing.T) {
					mockCM := mocks2.NewChunkManager(t)
					mockCM.EXPECT().WalkWithPrefix(mock.Anything, "valid", false, mock.Anything).
						Return(merr.WrapErrIoTooManyRequests("valid", errors.New("SlowDown"))).Maybe()
					reqFiles := []*internalpb.ImportFile{
						{Paths: []string{"valid"}},
						{Paths: invalidPaths},
					}
					if invalidFirst {
						reqFiles[0], reqFiles[1] = reqFiles[1], reqFiles[0]
					}

					files, err := ListBinlogImportRequestFiles(ctx, mockCM, reqFiles,
						[]*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}})
					require.Error(t, err)
					assert.Nil(t, files)
					assert.ErrorIs(t, err, merr.ErrImportFailed)
					status := merr.Status(err)
					assert.Equal(t, merr.Code(merr.ErrImportFailed), status.GetCode())
					assert.False(t, status.GetRetriable())
					assert.Equal(t, merr.InputError, merr.GetErrorType(merr.Error(status)))
					mockCM.AssertNotCalled(t, "WalkWithPrefix", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				})
			}
		}
	})

	t.Run("backup files - storage errors remain retryable", func(t *testing.T) {
		for _, test := range []struct {
			name string
			err  error
		}{
			{"untyped", errors.New("object storage unavailable")},
			{"throttled", merr.WrapErrIoTooManyRequests("insert", errors.New("SlowDown"))},
			{"missing object", merr.WrapErrIoKeyNotFound("insert")},
		} {
			t.Run(test.name, func(t *testing.T) {
				mockCM := mocks2.NewChunkManager(t)
				mockCM.EXPECT().WalkWithPrefix(mock.Anything, "insert", false, mock.Anything).
					Return(test.err).Once()
				files, err := ListBinlogImportRequestFiles(ctx, mockCM,
					[]*internalpb.ImportFile{{Paths: []string{"insert"}}},
					[]*commonpb.KeyValuePair{{Key: importutilv2.BackupFlag, Value: "true"}})
				require.Error(t, err)
				assert.Nil(t, files)
				assert.ErrorIs(t, err, test.err)
				status := merr.Status(err)
				assert.Equal(t, merr.Code(merr.ErrServiceUnavailable), status.GetCode())
				assert.True(t, status.GetRetriable())
				assert.Equal(t, merr.SystemError, merr.GetErrorType(merr.Error(status)))
				assert.True(t, merr.IsRetryableErr(merr.Error(status)))
			})
		}
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
		// Job-count backpressure is a server-side condition -> ErrImportSysFailed
		// (must not be bucketed as a user-caused failure).
		assert.ErrorIs(t, err, merr.ErrImportSysFailed)
		assert.Contains(t, err.Error(), "The number of jobs has reached the limit")
	})
}

func TestImportUtil_AssembleRequestCarriesIDRange(t *testing.T) {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}
	importMeta := NewMockImportMeta(t)
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
	var task ImportTask = &importTask{importMeta: importMeta}
	task.(*importTask).task.Store(importTaskProto)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegmentChangeGroups(mock.Anything).Return(nil, nil).Maybe()
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
	// ID range is carried through to the datanode request, per file.
	assert.Equal(t, int64(5000), importReq.GetFiles()[0].GetIdRange().GetBegin())
	assert.Equal(t, int64(5050), importReq.GetFiles()[0].GetIdRange().GetEnd())
	// logID IDRange is still allocated locally and independently.
	assert.Greater(t, importReq.GetIDRange().GetEnd(), importReq.GetIDRange().GetBegin())
}

// The per-file range guard: a range smaller than the file's row count is terminal (the
// cursor cannot cover the file), a range at least as large is allowed (the extra ids are
// never consumed), a nil range falls back to the task-level local allocator, and the
// zero-width range a zero-row file legitimately carries is accepted.
func TestImportUtil_AssembleRangeGuard(t *testing.T) {
	const rangeBegin = int64(5000)
	cases := []struct {
		name     string
		begin    int64
		end      int64
		nilRange bool
		rows     int64
		wantErr  bool
	}{
		{name: "exact range", begin: rangeBegin, end: rangeBegin + 100, rows: 100},
		{name: "over-reserved range is allowed", begin: rangeBegin, end: rangeBegin + 100, rows: 99},
		{name: "under-reserved range is terminal", begin: rangeBegin, end: rangeBegin + 100, rows: 101, wantErr: true},
		{name: "zero-width range on a zero-row file is allowed", begin: rangeBegin, end: rangeBegin, rows: 0},
		{name: "zero-width range on a non-empty file is terminal", begin: rangeBegin, end: rangeBegin, rows: 101, wantErr: true},
		{name: "nil range falls back to the local allocator", nilRange: true, rows: 101},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var job ImportJob = &importJob{
				ImportJob: &datapb.ImportJob{
					JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"},
				},
			}
			importMeta := NewMockImportMeta(t)
			importMeta.EXPECT().GetJob(mock.Anything, mock.Anything).Return(job).Maybe()

			importFile := &internalpb.ImportFile{Id: 1, Paths: []string{"f1"}}
			if !tc.nilRange {
				importFile.IdRange = &commonpb.IDRange{Begin: tc.begin, End: tc.end}
			}
			importTaskProto := &datapb.ImportTaskV2{
				JobID:        0,
				TaskID:       4,
				CollectionID: 1,
				FileStats: []*datapb.ImportFileStats{
					{
						ImportFile: importFile,
						TotalRows:  tc.rows,
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

			req, err := AssembleImportRequest(task, job, nil, alloc)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, req)
				assert.True(t, errors.Is(err, importid.ErrIDRangeTooSmall),
					"the sentinel must survive so the scheduler fails the job instead of retrying")
				assert.ErrorIs(t, err, merr.ErrImportSysFailed)
				assert.Contains(t, err.Error(),
					fmt.Sprintf("%d rows, %d ids reserved", tc.rows, tc.end-tc.begin))
				return
			}
			require.NoError(t, err)
			require.NotNil(t, req)
			if tc.nilRange {
				assert.Nil(t, req.GetFiles()[0].GetIdRange())
				return
			}
			assert.Equal(t, tc.begin, req.GetFiles()[0].GetIdRange().GetBegin())
			assert.Equal(t, tc.end, req.GetFiles()[0].GetIdRange().GetEnd())
		})
	}
}

func TestValidateImportFilePaths(t *testing.T) {
	backupOptions := []*commonpb.KeyValuePair{{Key: "backup", Value: "true"}}
	l0Options := []*commonpb.KeyValuePair{{Key: "l0_import", Value: "true"}}

	tests := []struct {
		name        string
		rootPath    string
		path        string
		options     []*commonpb.KeyValuePair
		wantReject  bool
		storageType string // "" means leave the configured default (remote)
	}{
		{"plain internal insert_log", "files", "files/insert_log/1/2/3/100/4", nil, true, ""},
		{"dot dot escape back into insert_log", "files", "files/../files/insert_log/1/2/3/100/4", nil, true, ""},
		{"leading slash", "files", "/files/insert_log/1/2/3/100/4", nil, true, ""},
		{"single dot segment", "files", "files/./insert_log/1/2/3/100/4", nil, true, ""},
		{"delta_log", "files", "files/delta_log/1/2/3/4", nil, true, ""},
		{"snapshots", "files", "files/snapshots/449/metadata/12.json", nil, true, ""},
		// Woodpecker's WAL lives under the same root; its segment reached the
		// registry only after the milvus#51894 review flagged the omission.
		{"woodpecker wal", "files", "files/wp/0/1/2.log", nil, true, ""},

		// The node-local cache subtree is at the storage root under
		// storageType=local, so it is denied there.
		{"local cache dir", "/var/lib/milvus/data", "/var/lib/milvus/data/cache/1/local_chunk/x", nil, true, "local"},

		// Everything segcore writes lives INSIDE that subtree, because its
		// ChunkManager is initialized with pathutil.GetPath(LocalChunkPath, nodeID)
		// = {localStorage.path}/cache/{nodeID}/local_chunk. The cache entry
		// therefore already covers raw_datas / ngram_log / tmp / rtree-index; they
		// must not be registered at the root as well.
		{"segcore raw_datas under the cache subtree", "/var/lib/milvus/data", "/var/lib/milvus/data/cache/1/local_chunk/raw_datas/449_100/0", nil, true, "local"},
		{"segcore temp index under the cache subtree", "/var/lib/milvus/data", "/var/lib/milvus/data/cache/1/local_chunk/tmp/HNSW/1", nil, true, "local"},

		// ...and the root-level names must stay ALLOWED, since Milvus never writes
		// them there. Denying <root>/tmp/ would break a caller staging imports in
		// a directory of that name while claiming it is Milvus-internal.
		{"local root, tmp is not internal", "/var/lib/milvus/data", "/var/lib/milvus/data/tmp/data.parquet", nil, false, "local"},
		{"local root, raw_datas is not internal", "/var/lib/milvus/data", "/var/lib/milvus/data/raw_datas/a.json", nil, false, "local"},
		{"remote root, tmp staging", "files", "files/tmp/data.parquet", nil, false, "remote"},

		// The cache entry itself is local-only: on a remote root Milvus does not
		// write it, so a caller directory of that name must pass.
		{"remote root, cache-named dir", "files", "files/cache/mine.json", nil, false, "remote"},

		// Explore planning manifests are written at the local root, and at the
		// bucket root outside minio.rootPath on remote storage -- so the segment
		// is denied under the root only when the storage type is local.
		{"local root, explore temp dir", "/var/lib/milvus/data", "/var/lib/milvus/data/__explore_temp__/coord_1/attempt_1/manifest.json", nil, true, "local"},
		{"remote root, explore-named dir", "files", "files/__explore_temp__/mine.json", nil, false, "remote"},
		// On remote storage the manifests themselves live at the bucket root,
		// outside minio.rootPath, so no root-anchored entry reaches them.
		{"remote root, explore temp at bucket root", "files", "__explore_temp__/coord_1/attempt_1/milvus-table-explore.json", nil, true, "remote"},

		// External refresh task results are root-anchored on both storage types.
		{"remote root, external refresh results", "files", "files/external_refresh_results/1/2/3/4/a.json", nil, true, "remote"},
		{"local root, external refresh results", "/var/lib/milvus/data", "/var/lib/milvus/data/external_refresh_results/1/2/3/4/a.json", nil, true, "local"},

		// Legacy StorageV3 segments keep living under <root>/<minio.rootPath>/insert_log
		// after an upgrade of a local deployment, so that directory is internal too.
		// On a remote root the same spelling is an ordinary caller directory.
		{"local root, legacy V3 insert_log", "/var/lib/milvus/data", "/var/lib/milvus/data/files/insert_log/1/2/3/_data/data.parquet", nil, true, "local"},
		{"local root, legacy prefix without insert_log", "/var/lib/milvus/data", "/var/lib/milvus/data/files/staging/a.json", nil, false, "local"},
		{"remote root, legacy prefix is not internal", "files", "files/files/insert_log/1/2/3/a.parquet", nil, false, "remote"},

		// A relative key is resolved by os.Open against the datanode working
		// directory, not against the storage root the deny entries are anchored
		// at, so it could never match one. With WORKDIR /milvus and
		// localStorage.path=/milvus/data this reads the snapshot dir while
		// comparing as "/data/snapshots/...".
		{"local root, relative path refused", "/milvus/data", "data/snapshots/449/metadata/12.json", nil, true, "local"},
		{"local root, relative staging path refused too", "/milvus/data", "staging/a.json", nil, true, "local"},

		// Remote keys are relative by nature and are used literally by S3, so the
		// rule must not leak outside local storage.
		{"remote root, relative path is normal", "files", "staging/a.json", nil, false, "remote"},

		// Woodpecker writes under BOTH roots, so it stays denied on remote.
		{"remote root, woodpecker still denied", "files", "files/wp/0/1/2.log", nil, true, "remote"},
		{"exact directory with no trailing content", "files", "files/insert_log", nil, true, ""},
		{"empty root path", "", "insert_log/1/2/3/100/4", nil, true, ""},

		// An absolute storage root is what storageType=local uses
		// (localStorage.path). The candidate path and the deny list must be
		// normalized into one namespace or none of these can ever match.
		{"absolute root", "/var/lib/milvus/data", "/var/lib/milvus/data/insert_log/1/2/3/100/4", nil, true, ""},
		{"absolute root, doubled slash", "/var/lib/milvus/data", "//var/lib/milvus/data/snapshots/449/metadata/1.json", nil, true, ""},

		// path.Clean collapses repeated slashes only once the key is rooted;
		// stripping a single leading slash by hand leaves "//x" as "/x".
		{"doubled leading slash", "files", "//files/insert_log/1/2/3/100/4", nil, true, ""},
		{"tripled leading slash", "files", "///files/insert_log/1/2/3/100/4", nil, true, ""},

		// LocalChunkManager passes the key straight to os.Open, where a leading
		// ".." resolves against the process working directory.
		{"leading dot dot", "files", "../files/insert_log/1/2/3/100/4", nil, true, ""},
		{"two leading dot dots", "files", "../../files/insert_log/1/2/3/100/4", nil, true, ""},

		// Must NOT be rejected: prefix collision on a non-boundary.
		{"user directory sharing a prefix", "files", "files/insert_logs_2026/a.json", nil, false, ""},
		{"ordinary staging path", "files", "staging/a.json", nil, false, ""},
		{"user file named centroids", "files", "staging/centroids", nil, false, ""},
		{"absolute root, unrelated path", "/var/lib/milvus/data", "/home/user/data.json", nil, false, ""},
		{"absolute root, prefix collision", "/var/lib/milvus/data", "/var/lib/milvus/data/insert_logs_2026/a.json", nil, false, ""},
		{"dot dot into an unrelated directory", "files", "../staging/a.json", nil, false, ""},
		{"caller top-level dir sharing the segment name", "files", "insert_log/mine.json", nil, false, ""},

		// Must NOT be rejected: backup and L0 import legitimately read binlogs.
		{"backup import into insert_log", "files", "files/insert_log/1/2/3", backupOptions, false, ""},
		{"l0 import into delta_log", "files", "files/delta_log/1/2/3", l0Options, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.storageType != "" {
				key := paramtable.Get().CommonCfg.StorageType.Key
				paramtable.Get().Save(key, tt.storageType)
				defer paramtable.Get().Reset(key)
			}
			// The legacy local namespace is <root>/<minio.rootPath>/insert_log,
			// so the cases above must not depend on the configured default.
			rootKey := paramtable.Get().MinioCfg.RootPath.Key
			paramtable.Get().Save(rootKey, "files")
			defer paramtable.Get().Reset(rootKey)

			rootPath, filePath := tt.rootPath, tt.path
			if tt.storageType == "local" {
				// Local keys are resolved on disk, so the root and the file must exist.
				rootPath, filePath = materializeLocalImportPath(t, tt.rootPath, tt.path)
			}

			cm := mocks2.NewChunkManager(t)
			cm.EXPECT().RootPath().Return(rootPath).Maybe()

			files := []*msgpb.ImportFile{{Paths: []string{filePath}}}
			err := ValidateImportFilePaths(cm, files, tt.options)

			if tt.wantReject {
				assert.Error(t, err)
				assert.ErrorIs(t, err, merr.ErrImportFailed)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// materializeLocalImportPath moves rootPath under a temp directory and creates
// filePath there when it is absolute. A relative filePath is returned as is.
func materializeLocalImportPath(t *testing.T, rootPath, filePath string) (string, string) {
	base := t.TempDir()
	localRoot := filepath.Join(base, rootPath)
	require.NoError(t, os.MkdirAll(localRoot, 0o755))
	if !filepath.IsAbs(filePath) {
		return localRoot, filePath
	}
	localFile := filepath.Join(base, filePath)
	require.NoError(t, os.MkdirAll(filepath.Dir(localFile), 0o755))
	require.NoError(t, os.WriteFile(localFile, []byte("{}"), 0o600))
	return localRoot, localFile
}

// Under storageType=local the datanode reads with os.Open, which follows
// symlinks and /proc magic links. An alias of an internal directory must be
// denied like the directory itself.
// An internal directory can itself be a symlink -- moving the cache subtree
// onto another disk is the ordinary reason. Both spellings must be denied: the
// one under the storage root, and the directory it resolves to.
func TestValidateImportFilePaths_SymlinkedInternalDir(t *testing.T) {
	key := paramtable.Get().CommonCfg.StorageType.Key
	paramtable.Get().Save(key, "local")
	defer paramtable.Get().Reset(key)

	base := t.TempDir()
	root := filepath.Join(base, "data")
	require.NoError(t, os.MkdirAll(root, 0o755))

	// <root>/cache -> <base>/nvme-cache, holding one segcore chunk file.
	elsewhere := filepath.Join(base, "nvme-cache")
	chunk := filepath.Join(elsewhere, "1", "local_chunk", "x.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(chunk), 0o755))
	require.NoError(t, os.WriteFile(chunk, []byte("x"), 0o600))
	require.NoError(t, os.Symlink(elsewhere, filepath.Join(root, common.LocalCacheRootPath)))

	staging := filepath.Join(base, "ordinary.json")
	require.NoError(t, os.WriteFile(staging, []byte("{}"), 0o600))

	// A registered segment that is an ordinary directory, with a symlink one
	// level below it: <root>/snapshots/449 -> <base>/nvme-snapshots.
	snapshotsDir := filepath.Join(root, common.SnapshotRootPath)
	require.NoError(t, os.MkdirAll(snapshotsDir, 0o755))
	sub := filepath.Join(base, "nvme-snapshots")
	require.NoError(t, os.MkdirAll(filepath.Join(sub, "metadata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "metadata", "12.json"), []byte("{}"), 0o600))
	require.NoError(t, os.Symlink(sub, filepath.Join(snapshotsDir, "449")))

	tests := []struct {
		name       string
		path       string
		wantReject bool
	}{
		{"through the root spelling", filepath.Join(root, common.LocalCacheRootPath, "1", "local_chunk", "x.parquet"), true},
		{"through the resolved directory", chunk, true},
		{"ordinary staging file", staging, false},
		// The link can also sit BELOW the registered segment, where the resolved
		// form leaves the root's namespace and no root-anchored entry matches it.
		{"symlink below the segment", filepath.Join(root, common.SnapshotRootPath, "449", "metadata", "12.json"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := mocks2.NewChunkManager(t)
			cm.EXPECT().RootPath().Return(root).Maybe()

			err := ValidateImportFilePaths(cm, []*msgpb.ImportFile{{Paths: []string{tt.path}}}, nil)
			if tt.wantReject {
				assert.ErrorIs(t, err, merr.ErrImportFailed)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// An unresolvable storage root is an operator-side fault, not a bad caller
// path: it must not be bucketed as an InputError, which also stops retry.
func TestValidateImportFilePaths_UnresolvableRootIsSystemError(t *testing.T) {
	key := paramtable.Get().CommonCfg.StorageType.Key
	paramtable.Get().Save(key, "local")
	defer paramtable.Get().Reset(key)

	missing := filepath.Join(t.TempDir(), "mount-dropped")
	cm := mocks2.NewChunkManager(t)
	cm.EXPECT().RootPath().Return(missing).Maybe()

	err := ValidateImportFilePaths(cm,
		[]*msgpb.ImportFile{{Paths: []string{filepath.Join(missing, "a.json")}}}, nil)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	assert.NotErrorIs(t, err, merr.ErrImportFailed)
}

func TestValidateImportFilePaths_LocalAliases(t *testing.T) {
	key := paramtable.Get().CommonCfg.StorageType.Key
	paramtable.Get().Save(key, "local")
	defer paramtable.Get().Reset(key)

	base := t.TempDir()
	root := filepath.Join(base, "data")
	snapshot := filepath.Join(root, "snapshots", "449", "metadata", "12.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(snapshot), 0o755))
	require.NoError(t, os.WriteFile(snapshot, []byte("{}"), 0o600))

	staging := filepath.Join(base, "staging")
	require.NoError(t, os.MkdirAll(staging, 0o755))
	link := filepath.Join(staging, "a.json")
	require.NoError(t, os.Symlink(snapshot, link))

	rootLink := filepath.Join(base, "data-link")
	require.NoError(t, os.Symlink(root, rootLink))

	tests := []struct {
		name       string
		rootPath   string
		path       string
		wantReject bool
	}{
		{"proc self root alias", root, "/proc/self/root" + snapshot, true},
		{"staging symlink into the root", root, link, true},
		{"symlinked root, resolved path", rootLink, snapshot, true},
		{"missing file", root, filepath.Join(staging, "missing.json"), true},
		{"ordinary staging file", root, filepath.Join(base, "ordinary.json"), false},
	}
	require.NoError(t, os.WriteFile(filepath.Join(base, "ordinary.json"), []byte("{}"), 0o600))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm := mocks2.NewChunkManager(t)
			cm.EXPECT().RootPath().Return(tt.rootPath).Maybe()

			err := ValidateImportFilePaths(cm, []*msgpb.ImportFile{{Paths: []string{tt.path}}}, nil)
			if tt.wantReject {
				assert.ErrorIs(t, err, merr.ErrImportFailed)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateImportFilePaths_ChecksEveryPathOfEveryFile(t *testing.T) {
	cm := mocks2.NewChunkManager(t)
	cm.EXPECT().RootPath().Return("files").Maybe()

	// The offending path is neither the first file nor the first path.
	files := []*msgpb.ImportFile{
		{Paths: []string{"staging/a.json"}},
		{Paths: []string{"staging/b.json", "files/stats_log/1/2/3/100/4"}},
	}

	err := ValidateImportFilePaths(cm, files, nil)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
}

// TestGetV3TaskProgressesFromReshardHashedRows pins the V3 per-file projection:
// one entry per source file of every reshard task, imported rows taken from the
// worker's per-source hashed rows, denominator from the count-only preimport
// stats.
func TestGetV3TaskProgressesFromReshardHashedRows(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}
	file1 := &internalpb.ImportFile{Id: 1, Paths: []string{"a.parquet"}}
	file2 := &internalpb.ImportFile{Id: 2, Paths: []string{"b.parquet"}}
	job := &importJob{ImportJob: &datapb.ImportJob{
		JobID: 1, CollectionID: 2, Version: datapb.ImportJobVersion_ImportJobVersionV3,
		Files: []*internalpb.ImportFile{file1, file2}, Schema: schema,
	}}

	importMeta := NewMockImportMeta(t)
	preimport := newPreImportV2Task(&datapb.PreImportV2Task{
		JobID: 1, TaskID: 10, State: datapb.ImportTaskStateV2_Completed,
		FileStats: []*datapb.ImportFileStats{
			{ImportFile: file1, TotalRows: 100, FileSize: 1000},
			{ImportFile: file2, TotalRows: 200, FileSize: 2000},
		},
	}, importMeta)
	reshard1 := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 11, State: datapb.ImportTaskStateV2_InProgress, SourceIds: []int64{1},
	}, importMeta, nil, nil)
	reshard1.setSourceProgress([]*datapb.ReshardSourceProgress{{FileId: 1, HashedRows: 50}})
	reshard2 := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 12, State: datapb.ImportTaskStateV2_Completed, SourceIds: []int64{2},
	}, importMeta, nil, nil)
	reshard2.setSourceProgress([]*datapb.ReshardSourceProgress{{FileId: 2, HashedRows: 200}})

	reshardProbe := newReshardTask(&datapb.ReshardTask{}, importMeta, nil, nil)
	preimportProbe := newPreImportV2Task(&datapb.PreImportV2Task{}, importMeta)
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	importMeta.EXPECT().GetTaskByJob(mock.Anything, int64(1), mock.Anything).RunAndReturn(
		func(_ context.Context, _ int64, filters ...ImportTaskFilter) []ImportTask {
			if len(filters) == 1 && filters[0](reshardProbe) {
				return []ImportTask{reshard1, reshard2}
			}
			if len(filters) == 1 && filters[0](preimportProbe) {
				return []ImportTask{preimport}
			}
			return nil
		}).Twice()

	progresses := GetTaskProgresses(ctx, 1, importMeta, nil)
	require.Len(t, progresses, 2)
	byName := make(map[string]*internalpb.ImportTaskProgress)
	for _, p := range progresses {
		byName[p.GetFileName()] = p
	}
	assert.Equal(t, int64(50), byName["[a.parquet]"].GetImportedRows())
	assert.Equal(t, int64(100), byName["[a.parquet]"].GetTotalRows())
	assert.Equal(t, int64(50), byName["[a.parquet]"].GetProgress())
	assert.Equal(t, int64(200), byName["[b.parquet]"].GetImportedRows())
	assert.Equal(t, int64(200), byName["[b.parquet]"].GetTotalRows())
	assert.Equal(t, int64(100), byName["[b.parquet]"].GetProgress())
}
