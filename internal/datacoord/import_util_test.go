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
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	broker2 "github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
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
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

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
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

	broker := broker2.NewMockBroker(t)
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
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

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
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

	alloc := allocator.NewMockAllocator(t)
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})

	broker := broker2.NewMockBroker(t)
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
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

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
		SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 100, State: commonpb.SegmentState_Flushed},
		size:        *atomic.NewInt64(3000 * 1024 * 1024),
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
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, 0, nil)

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

	// failed state
	err = importMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(mockErr))
	assert.NoError(t, err)

	progress, state, _, _, reason := GetJobProgress(ctx, job.GetJobID(), importMeta, meta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportJobState_Failed, state)
	assert.Equal(t, mockErr, reason)

	// job does not exist
	progress, state, _, _, reason = GetJobProgress(ctx, -1, importMeta, meta)
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

	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             100,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{10},
		},
	})
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             110,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{11},
		},
	})
	err = meta.AddSegment(ctx, &SegmentInfo{
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

	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             200,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{20},
		},
	})
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             210,
			IsImporting:    true,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      100,
			IsSorted:       true,
			CompactionFrom: []int64{21},
		},
	})
	err = meta.AddSegment(ctx, &SegmentInfo{
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
