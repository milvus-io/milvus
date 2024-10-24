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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestImportUtilSuite(t *testing.T) {
	suite.Run(t, new(ImportUtilSuite))
}

type ImportUtilSuite struct {
	suite.Suite

	mockAlloc       *allocator.MockAllocator
	handler         Handler
	mockPlanContext *MockCompactionPlanContext
	testLabel       *CompactionGroupLabel
	meta            *meta

	triggerManager *CompactionTriggerManager
}

func (s *ImportUtilSuite) TestImportUtil_NewPreImportTasks() {
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
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	tasks, err := NewPreImportTasks(fileGroups, job, alloc)
	s.NoError(err)
	s.Equal(2, len(tasks))
}

func (s *ImportUtilSuite) TestImportUtil_NewImportTasks() {
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
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2},
	}
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocID(mock.Anything).Return(rand.Int63(), nil)
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(rand.Uint64(), nil)

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil)

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

	tasks, err := NewImportTasks(fileGroups, job, alloc, meta)
	s.NoError(err)
	s.Equal(2, len(tasks))
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		s.Equal(3, len(segmentIDs))
	}
}

func (s *ImportUtilSuite) TestImportUtil_AssembleRequest() {
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{JobID: 0, CollectionID: 1, PartitionIDs: []int64{2}, Vchannels: []string{"v0"}},
	}

	var pt ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        0,
			TaskID:       3,
			CollectionID: 1,
			State:        datapb.ImportTaskStateV2_Pending,
		},
	}
	preimportReq := AssemblePreImportRequest(pt, job)
	s.Equal(pt.GetJobID(), preimportReq.GetJobID())
	s.Equal(pt.GetTaskID(), preimportReq.GetTaskID())
	s.Equal(pt.GetCollectionID(), preimportReq.GetCollectionID())
	s.Equal(job.GetPartitionIDs(), preimportReq.GetPartitionIDs())
	s.Equal(job.GetVchannels(), preimportReq.GetVchannels())

	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        0,
			TaskID:       4,
			CollectionID: 1,
			SegmentIDs:   []int64{5, 6},
		},
	}

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil)

	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().AllocTimestamp(mock.Anything).Return(800, nil)

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, IsImporting: true},
	}
	err = meta.AddSegment(context.Background(), segment)
	s.NoError(err)
	segment.ID = 6
	err = meta.AddSegment(context.Background(), segment)
	s.NoError(err)

	importReq, err := AssembleImportRequest(task, job, meta, alloc)
	s.NoError(err)
	s.Equal(task.GetJobID(), importReq.GetJobID())
	s.Equal(task.GetTaskID(), importReq.GetTaskID())
	s.Equal(task.GetCollectionID(), importReq.GetCollectionID())
	s.Equal(job.GetPartitionIDs(), importReq.GetPartitionIDs())
	s.Equal(job.GetVchannels(), importReq.GetVchannels())
}

func (s *ImportUtilSuite) TestImportUtil_RegroupImportFiles() {
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

	groups := RegroupImportFiles(job, files, false)
	total := 0
	for i, fs := range groups {
		sum := lo.SumBy(fs, func(f *datapb.ImportFileStats) int64 {
			return f.GetTotalMemorySize()
		})

		s.True(sum <= threshold)
		if i != len(groups)-1 {
			assert.True(s.T(), len(fs) >= int(threshold/dataSize))
			assert.True(s.T(), sum >= threshold-dataSize)
		}
		total += len(fs)
	}
	s.Equal(fileNum, total)
}

func (s *ImportUtilSuite) TestImportUtil_CheckDiskQuota() {
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil)

	imeta, err := NewImportMeta(catalog)
	s.NoError(err)

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: 100,
		},
	}
	err = imeta.AddJob(job)
	s.NoError(err)

	pit := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  job.GetJobID(),
			TaskID: 1,
			FileStats: []*datapb.ImportFileStats{
				{TotalMemorySize: 1000 * 1024 * 1024},
				{TotalMemorySize: 2000 * 1024 * 1024},
			},
		},
	}
	err = imeta.AddTask(pit)
	s.NoError(err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "false")
	defer Params.Reset(Params.QuotaConfig.DiskProtectionEnabled.Key)
	_, err = CheckDiskQuota(job, meta, imeta)
	s.NoError(err)

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 5, CollectionID: 100, State: commonpb.SegmentState_Flushed},
		size:        *atomic.NewInt64(3000 * 1024 * 1024),
	}
	err = meta.AddSegment(context.Background(), segment)
	s.NoError(err)

	Params.Save(Params.QuotaConfig.DiskProtectionEnabled.Key, "true")
	job.Options = []*commonpb.KeyValuePair{
		{Key: importutilv2.BackupFlag, Value: "true"},
		{Key: importutilv2.SkipDQC, Value: "true"},
	}
	_, err = CheckDiskQuota(job, meta, imeta)
	s.NoError(err)

	job.Options = nil
	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "10000")
	defer Params.Reset(Params.QuotaConfig.DiskQuota.Key)
	defer Params.Reset(Params.QuotaConfig.DiskQuotaPerCollection.Key)
	requestSize, err := CheckDiskQuota(job, meta, imeta)
	s.NoError(err)
	s.Equal(int64(3000*1024*1024), requestSize)

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "5000")
	_, err = CheckDiskQuota(job, meta, imeta)
	s.True(errors.Is(err, merr.ErrServiceQuotaExceeded))

	Params.Save(Params.QuotaConfig.DiskQuota.Key, "10000")
	Params.Save(Params.QuotaConfig.DiskQuotaPerCollection.Key, "5000")
	_, err = CheckDiskQuota(job, meta, imeta)
	s.True(errors.Is(err, merr.ErrServiceQuotaExceeded))
}

func (s *ImportUtilSuite) TestImportUtil_DropImportTask() {
	cluster := NewMockCluster(s.T())
	cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)

	imeta, err := NewImportMeta(catalog)
	s.NoError(err)

	task := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:  0,
			TaskID: 1,
		},
	}
	err = imeta.AddTask(task)
	s.NoError(err)

	err = DropImportTask(task, cluster, imeta)
	s.NoError(err)
}

func (s *ImportUtilSuite) TestImportUtil_ListBinlogsAndGroupBySegment() {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"
	)

	s.Run("normal case", func() {
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

		cm := mocks2.NewChunkManager(s.T())
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
		s.NoError(err)
		s.Equal(2, len(files))
		for _, f := range files {
			s.Equal(2, len(f.GetPaths()))
			for _, p := range f.GetPaths() {
				segmentID := path.Base(p)
				s.True(segmentID == "435978159261483008" || segmentID == "435978159261483009")
			}
		}
	})

	s.Run("invalid input", func() {
		file := &internalpb.ImportFile{
			Paths: []string{},
		}
		_, err := ListBinlogsAndGroupBySegment(context.Background(), nil, file)
		s.Error(err)
		s.T().Logf("%s", err)

		file.Paths = []string{insertPrefix, deltaPrefix, "dummy_prefix"}
		_, err = ListBinlogsAndGroupBySegment(context.Background(), nil, file)
		s.Error(err)
		s.T().Logf("%s", err)
	})
}

func (s *ImportUtilSuite) TestImportUtil_GetImportProgress() {
	ctx := context.Background()
	mockErr := "mock err"

	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardInfos(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListVShardTasks(mock.Anything).Return(nil, nil)

	imeta, err := NewImportMeta(catalog)
	s.NoError(err)

	meta, err := newMeta(context.TODO(), catalog, nil)
	s.NoError(err)

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
	err = imeta.AddJob(job)
	s.NoError(err)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
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
		},
	}
	err = imeta.AddTask(pit1)
	s.NoError(err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  job.GetJobID(),
			TaskID: 2,
			State:  datapb.ImportTaskStateV2_Completed,
			FileStats: []*datapb.ImportFileStats{
				{
					ImportFile: file3,
				},
			},
		},
	}
	err = imeta.AddTask(pit2)
	s.NoError(err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
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
		},
	}
	err = imeta.AddTask(it1)
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 10, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 11, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 12, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)

	it2 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
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
		},
	}
	err = imeta.AddTask(it2)
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 20, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 21, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 22, IsImporting: true, State: commonpb.SegmentState_Flushed}, currRows: 50,
	})
	s.NoError(err)

	// failed state
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Failed), UpdateJobReason(mockErr))
	s.NoError(err)

	progress, state, _, _, reason := GetJobProgress(job.GetJobID(), imeta, meta, nil)
	s.Equal(int64(0), progress)
	s.Equal(internalpb.ImportJobState_Failed, state)
	s.Equal(mockErr, reason)

	// job does not exist
	progress, state, _, _, reason = GetJobProgress(-1, imeta, meta, nil)
	s.Equal(int64(0), progress)
	s.Equal(internalpb.ImportJobState_Failed, state)
	s.NotEqual("", reason)

	// pending state
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Pending))
	s.NoError(err)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, nil)
	s.Equal(int64(10), progress)
	s.Equal(internalpb.ImportJobState_Pending, state)
	s.Equal("", reason)

	// preImporting state
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	s.NoError(err)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, nil)
	s.Equal(int64(10+30), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Equal("", reason)

	// importing state, segmentImportedRows/totalRows = 0.5
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
	s.NoError(err)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, nil)
	s.Equal(int64(10+30+30*0.5), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Equal("", reason)

	// importing state, segmentImportedRows/totalRows = 1
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(10, 100))
	s.NoError(err)
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(20, 100))
	s.NoError(err)
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(11, 100))
	s.NoError(err)
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(12, 100))
	s.NoError(err)
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(21, 100))
	s.NoError(err)
	err = meta.UpdateSegmentsInfo(UpdateImportedRows(22, 100))
	s.NoError(err)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, nil)
	s.Equal(int64(float32(10+30+30)), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Equal("", reason)

	// stats state, len(statsSegmentIDs) / (len(originalSegmentIDs) = 0.5
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Stats))
	s.NoError(err)
	sjm := NewMockStatsJobManager(s.T())
	sjm.EXPECT().GetStatsTaskState(mock.Anything, mock.Anything).RunAndReturn(func(segmentID int64, _ indexpb.StatsSubJob) indexpb.JobState {
		if lo.Contains([]int64{10, 11, 12}, segmentID) {
			return indexpb.JobState_JobStateFinished
		}
		return indexpb.JobState_JobStateInProgress
	})
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, sjm)
	s.Equal(int64(10+30+30+10*0.5), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Equal("", reason)

	// stats state, len(statsSegmentIDs) / (len(originalSegmentIDs) = 1
	sjm = NewMockStatsJobManager(s.T())
	sjm.EXPECT().GetStatsTaskState(mock.Anything, mock.Anything).Return(indexpb.JobState_JobStateFinished)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, sjm)
	s.Equal(int64(10+30+30+10), progress)
	s.Equal(internalpb.ImportJobState_Importing, state)
	s.Equal("", reason)

	// completed state
	err = imeta.UpdateJob(job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Completed))
	s.NoError(err)
	progress, state, _, _, reason = GetJobProgress(job.GetJobID(), imeta, meta, sjm)
	s.Equal(int64(100), progress)
	s.Equal(internalpb.ImportJobState_Completed, state)
	s.Equal("", reason)
}
