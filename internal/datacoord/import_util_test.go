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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	alloc := NewNMockAllocator(t)
	alloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	tasks, err := NewPreImportTasks(fileGroups, job, alloc)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
}

func TestImportUtil_NewImportTasks(t *testing.T) {
	dataSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	fileGroups := [][]*datapb.ImportFileStats{
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 0, Paths: []string{"a.json"}},
				HashedStats: map[string]*datapb.PartitionStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 1, Paths: []string{"b.json"}},
				HashedStats: map[string]*datapb.PartitionStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
		{
			{
				ImportFile:  &internalpb.ImportFile{Id: 2, Paths: []string{"c.npy", "d.npy"}},
				HashedStats: map[string]*datapb.PartitionStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize}}},
			},
			{
				ImportFile:  &internalpb.ImportFile{Id: 3, Paths: []string{"e.npy", "f.npy"}},
				HashedStats: map[string]*datapb.PartitionStats{"c0": {PartitionDataSize: map[int64]int64{100: dataSize * 2}}},
			},
		},
	}
	job := &importJob{
		ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2},
	}
	alloc := NewNMockAllocator(t)
	alloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	manager := NewMockManager(t)
	manager.EXPECT().AllocImportSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, taskID int64, collectionID int64, partitionID int64, vchannel string, schema *schemapb.CollectionSchema) (*SegmentInfo, error) {
			return &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            rand.Int63(),
					CollectionID:  collectionID,
					PartitionID:   partitionID,
					InsertChannel: vchannel,
					IsImporting:   true,
				},
			}, nil
		})
	tasks, err := NewImportTasks(fileGroups, job, manager, alloc)
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

	var pt ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        0,
			TaskID:       3,
			CollectionID: 1,
			State:        internalpb.ImportState_Pending,
		},
	}
	preimportReq := AssemblePreImportRequest(pt, job)
	assert.Equal(t, pt.GetJobID(), preimportReq.GetJobID())
	assert.Equal(t, pt.GetTaskID(), preimportReq.GetTaskID())
	assert.Equal(t, pt.GetCollectionID(), preimportReq.GetCollectionID())
	assert.Equal(t, job.GetPartitionIDs(), preimportReq.GetPartitionIDs())
	assert.Equal(t, job.GetVchannels(), preimportReq.GetVchannels())

	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        0,
			TaskID:       4,
			CollectionID: 1,
			SegmentIDs:   []int64{5, 6},
		},
	}

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)

	alloc := NewNMockAllocator(t)
	alloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(n int64) (int64, int64, error) {
		id := rand.Int63()
		return id, id + n, nil
	})
	alloc.EXPECT().allocTimestamp(mock.Anything).Return(800, nil)

	meta, err := newMeta(context.TODO(), catalog, nil)
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
	groups, err := RegroupImportFiles(job, files)
	assert.NoError(t, err)
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

func TestImportUtil_AddImportSegment(t *testing.T) {
	cluster := NewMockCluster(t)
	cluster.EXPECT().AddImportSegment(mock.Anything, mock.Anything).Return(nil, nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)

	meta, err := newMeta(context.TODO(), catalog, nil)
	assert.NoError(t, err)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 1, IsImporting: true},
	}
	err = meta.AddSegment(context.Background(), segment)
	assert.NoError(t, err)

	err = AddImportSegment(cluster, meta, segment.GetID())
	assert.NoError(t, err)
}

func TestImportUtil_DropImportTask(t *testing.T) {
	cluster := NewMockCluster(t)
	cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportJobs().Return(nil, nil)
	catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)

	imeta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	task := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:  0,
			TaskID: 1,
		},
	}
	err = imeta.AddTask(task)
	assert.NoError(t, err)

	err = DropImportTask(task, cluster, imeta)
	assert.NoError(t, err)
}

func TestImportUtil_ListBinlogsAndGroupBySegment(t *testing.T) {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"
	)

	insertBinlogs := []string{
		// segment 435978159261483008
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735821",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735822",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735823",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735831",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735832",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735833",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735841",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735842",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735843",
		// segment 435978159261483009
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/100/435978159903735851",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/100/435978159903735852",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/100/435978159903735853",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/101/435978159903735861",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/101/435978159903735862",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/101/435978159903735863",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/102/435978159903735871",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/102/435978159903735872",
		"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483009/102/435978159903735873",
	}

	deltaLogs := []string{
		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483008/434574382554415105",
		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483008/434574382554415106",

		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415115",
		"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415116",
	}

	ctx := context.Background()
	cm := mocks2.NewChunkManager(t)
	cm.EXPECT().ListWithPrefix(mock.Anything, insertPrefix, mock.Anything).Return(insertBinlogs, nil, nil)
	cm.EXPECT().ListWithPrefix(mock.Anything, deltaPrefix, mock.Anything).Return(deltaLogs, nil, nil)

	file := &internalpb.ImportFile{
		Id:    1,
		Paths: []string{insertPrefix, deltaPrefix},
	}

	files, err := ListBinlogsAndGroupBySegment(ctx, cm, file)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(files))
	for _, f := range files {
		assert.Equal(t, 2, len(f.GetPaths()))
		for _, p := range f.GetPaths() {
			segmentID := path.Base(p)
			assert.True(t, segmentID == "435978159261483008" || segmentID == "435978159261483009")
		}
	}
}

func TestImportUtil_GetImportProgress(t *testing.T) {
	ctx := context.Background()
	mockErr := "mock err"

	catalog := mocks.NewDataCoordCatalog(t)
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

	imeta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	meta, err := newMeta(context.TODO(), catalog, nil)
	assert.NoError(t, err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 0,
		},
	}
	err = imeta.AddJob(job)
	assert.NoError(t, err)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  job.GetJobID(),
			TaskID: 1,
			State:  internalpb.ImportState_Failed,
			Reason: mockErr,
		},
	}
	err = imeta.AddTask(pit1)
	assert.NoError(t, err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  job.GetJobID(),
			TaskID: 2,
			State:  internalpb.ImportState_Pending,
		},
	}
	err = imeta.AddTask(pit2)
	assert.NoError(t, err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      job.GetJobID(),
			TaskID:     3,
			SegmentIDs: []int64{10, 11, 12},
			State:      internalpb.ImportState_Pending,
		},
	}
	err = imeta.AddTask(it1)
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 10, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 11, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 12, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)

	it2 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:      job.GetJobID(),
			TaskID:     4,
			SegmentIDs: []int64{20, 21, 22},
			State:      internalpb.ImportState_Pending,
		},
	}
	err = imeta.AddTask(it2)
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 20, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 21, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)
	err = meta.AddSegment(ctx, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{ID: 22, IsImporting: true, MaxRowNum: 100}, currRows: 50,
	})
	assert.NoError(t, err)

	var (
		preparingProgress int64
		preImportProgress int64
		importProgress    int64
		segStateProgress  int64
	)

	// with failed
	progress, state, reason := GetImportProgress(job.GetJobID(), imeta, meta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportState_Failed, state)
	assert.Equal(t, mockErr, reason)

	// all pending
	err = imeta.UpdateTask(pit1.GetTaskID(), UpdateState(internalpb.ImportState_Pending))
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	assert.Equal(t, int64(0), progress)
	assert.Equal(t, internalpb.ImportState_InProgress, state)
	assert.Equal(t, "", reason)

	// in progress
	err = imeta.UpdateTask(pit1.GetTaskID(), UpdateState(internalpb.ImportState_InProgress))
	assert.NoError(t, err)
	err = imeta.UpdateTask(pit2.GetTaskID(), UpdateState(internalpb.ImportState_InProgress))
	assert.NoError(t, err)
	err = imeta.UpdateTask(it1.GetTaskID(), UpdateState(internalpb.ImportState_InProgress))
	assert.NoError(t, err)
	err = imeta.UpdateTask(it2.GetTaskID(), UpdateState(internalpb.ImportState_InProgress))
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	preparingProgress = 10
	preImportProgress = 40 * 0
	importProgress = 40 * 0.5
	segStateProgress = 0
	assert.Equal(t, preparingProgress+preImportProgress+importProgress+segStateProgress, progress)
	assert.Equal(t, internalpb.ImportState_InProgress, state)
	assert.Equal(t, "", reason)

	// partial completed
	err = imeta.UpdateTask(pit1.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
	assert.NoError(t, err)
	err = imeta.UpdateTask(pit2.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	preparingProgress = 10
	preImportProgress = 40
	importProgress = 40 * 0.5
	segStateProgress = 0
	assert.Equal(t, preparingProgress+preImportProgress+importProgress+segStateProgress, progress)
	assert.Equal(t, internalpb.ImportState_InProgress, state)
	assert.Equal(t, "", reason)

	// all completed, all segments is in importing state
	err = imeta.UpdateTask(it1.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
	assert.NoError(t, err)
	err = imeta.UpdateTask(it2.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	preparingProgress = 10
	preImportProgress = 40
	importProgress = 40
	segStateProgress = 0
	assert.Equal(t, preparingProgress+preImportProgress+importProgress+segStateProgress, progress)
	assert.Equal(t, internalpb.ImportState_InProgress, state)
	assert.Equal(t, "", reason)

	// all completed, partial segments is in importing state
	err = meta.UnsetIsImporting(10)
	assert.NoError(t, err)
	err = meta.UnsetIsImporting(20)
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	preparingProgress = 10
	preImportProgress = 40
	importProgress = 40
	segStateProgress = 10 * 2 / 6
	assert.Equal(t, preparingProgress+preImportProgress+importProgress+segStateProgress, progress)
	assert.Equal(t, internalpb.ImportState_InProgress, state)
	assert.Equal(t, "", reason)

	// all completed, no segment is in importing state
	err = meta.UnsetIsImporting(11)
	assert.NoError(t, err)
	err = meta.UnsetIsImporting(12)
	assert.NoError(t, err)
	err = meta.UnsetIsImporting(21)
	assert.NoError(t, err)
	err = meta.UnsetIsImporting(22)
	assert.NoError(t, err)
	progress, state, reason = GetImportProgress(job.GetJobID(), imeta, meta)
	preparingProgress = 10
	preImportProgress = 40
	importProgress = 40
	segStateProgress = 10
	assert.Equal(t, preparingProgress+preImportProgress+importProgress+segStateProgress, progress)
	assert.Equal(t, internalpb.ImportState_Completed, state)
	assert.Equal(t, "", reason)
}
