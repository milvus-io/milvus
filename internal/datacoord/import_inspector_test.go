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
	"math"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	task2 "github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
)

type ImportInspectorSuite struct {
	suite.Suite

	collectionID int64

	catalog   *mocks.DataCoordCatalog
	alloc     *allocator.MockAllocator
	cluster   *MockCluster
	meta      *meta
	imeta     ImportMeta
	inspector *importInspector
}

func (s *ImportInspectorSuite) SetupTest() {
	var err error

	s.collectionID = 1

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

	s.cluster = NewMockCluster(s.T())
	s.alloc = allocator.NewMockAllocator(s.T())
	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	s.meta, err = newMeta(context.TODO(), s.catalog, nil, broker)
	s.NoError(err)
	s.meta.AddCollection(&collectionInfo{
		ID:     s.collectionID,
		Schema: newTestSchema(),
	})
	s.imeta, err = NewImportMeta(context.TODO(), s.catalog, s.alloc, s.meta)
	s.NoError(err)
	scheduler := task2.NewMockGlobalScheduler(s.T())
	s.inspector = NewImportInspector(s.meta, s.imeta, scheduler).(*importInspector)
}

func (s *ImportInspectorSuite) TestProcessPreImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SavePreImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.PreImportTask{
		JobID:        0,
		TaskID:       1,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_Pending,
	}

	var task ImportTask = &preImportTask{
		imeta: s.imeta,
		tr:    timerecord.NewTimeRecorder("preimport task"),
	}
	task.(*preImportTask).task.Store(taskProto)
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			TimeoutTs:    math.MaxUint64,
			Schema:       &schemapb.CollectionSchema{},
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.imeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// pending -> inProgress
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreatePreImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(task task2.Task) {
		task.CreateTaskOnWorker(1, cluster)
	})
	s.inspector.inspect()
	task = s.imeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())

	// inProgress -> completed
	cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	task.QueryTaskOnWorker(cluster)
	task = s.imeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())
}

func (s *ImportInspectorSuite) TestProcessImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:        0,
		TaskID:       1,
		CollectionID: s.collectionID,
		State:        datapb.ImportTaskStateV2_Pending,
		FileStats: []*datapb.ImportFileStats{
			{
				HashedStats: map[string]*datapb.PartitionImportStats{
					"channel1": {
						PartitionRows: map[int64]int64{
							int64(2): 100,
						},
						PartitionDataSize: map[int64]int64{
							int64(2): 100,
						},
					},
				},
			},
		},
	}

	var task ImportTask = &importTask{
		alloc: s.alloc,
		meta:  s.meta,
		imeta: s.imeta,
		tr:    timerecord.NewTimeRecorder("import task"),
	}
	task.(*importTask).task.Store(taskProto)
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"channel1"},
			Schema:       &schemapb.CollectionSchema{},
			TimeoutTs:    math.MaxUint64,
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.imeta.AddJob(context.TODO(), job)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	s.alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	s.alloc.EXPECT().AllocTimestamp(mock.Anything).Return(300, nil)
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateImport(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.inspector.scheduler.(*task2.MockGlobalScheduler).EXPECT().Enqueue(mock.Anything).Run(func(task task2.Task) {
		task.CreateTaskOnWorker(nodeID, cluster)
	})
	s.inspector.inspect()
	task = s.imeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())

	// inProgress -> completed
	cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	task.QueryTaskOnWorker(cluster)
	task = s.imeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())
}

func (s *ImportInspectorSuite) TestProcessFailed() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything, mock.Anything).Return(nil)

	taskProto := &datapb.ImportTaskV2{
		JobID:           0,
		TaskID:          1,
		CollectionID:    s.collectionID,
		NodeID:          6,
		SegmentIDs:      []int64{2, 3},
		StatsSegmentIDs: []int64{4, 5},
		State:           datapb.ImportTaskStateV2_Failed,
	}

	var task ImportTask = &importTask{
		tr: timerecord.NewTimeRecorder("import task"),
	}
	task.(*importTask).task.Store(taskProto)
	err := s.imeta.AddTask(context.TODO(), task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			PartitionIDs: []int64{2},
			Vchannels:    []string{"channel1"},
			Schema:       &schemapb.CollectionSchema{},
			TimeoutTs:    math.MaxUint64,
		},
		tr: timerecord.NewTimeRecorder("import job"),
	}
	err = s.imeta.AddJob(context.TODO(), job)
	s.NoError(err)

	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{ID: id, State: commonpb.SegmentState_Importing, IsImporting: true},
		}
		err = s.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
	}
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := s.meta.GetSegment(context.TODO(), id)
		s.NotNil(segment)
	}

	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	s.inspector.inspect()
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := s.meta.GetSegment(context.TODO(), id)
		s.Equal(commonpb.SegmentState_Dropped, segment.GetState())
	}
	task = s.imeta.GetTask(context.TODO(), task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, task.GetState())
	s.Equal(0, len(task.(*importTask).GetSegmentIDs()))
}

func TestImportInspector(t *testing.T) {
	suite.Run(t, new(ImportInspectorSuite))
}
