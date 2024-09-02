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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ImportSchedulerSuite struct {
	suite.Suite

	collectionID int64

	catalog   *mocks.DataCoordCatalog
	alloc     *allocator.MockAllocator
	cluster   *MockCluster
	meta      *meta
	imeta     ImportMeta
	scheduler *importScheduler
}

func (s *ImportSchedulerSuite) SetupTest() {
	var err error

	s.collectionID = 1

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListImportJobs().Return(nil, nil)
	s.catalog.EXPECT().ListPreImportTasks().Return(nil, nil)
	s.catalog.EXPECT().ListImportTasks().Return(nil, nil)
	s.catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListAnalyzeTasks(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListPartitionStatsInfos(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return(nil, nil)

	s.cluster = NewMockCluster(s.T())
	s.alloc = allocator.NewMockAllocator(s.T())
	s.meta, err = newMeta(context.TODO(), s.catalog, nil)
	s.NoError(err)
	s.meta.AddCollection(&collectionInfo{
		ID:     s.collectionID,
		Schema: newTestSchema(),
	})
	s.imeta, err = NewImportMeta(s.catalog)
	s.NoError(err)
	buildIndexCh := make(chan UniqueID, 1024)
	s.scheduler = NewImportScheduler(s.meta, s.cluster, s.alloc, s.imeta, buildIndexCh).(*importScheduler)
}

func (s *ImportSchedulerSuite) TestProcessPreImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	s.catalog.EXPECT().SavePreImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:        0,
			TaskID:       1,
			CollectionID: s.collectionID,
			State:        datapb.ImportTaskStateV2_Pending,
		},
	}
	err := s.imeta.AddTask(task)
	s.NoError(err)
	var job ImportJob = &importJob{
		ImportJob: &datapb.ImportJob{
			JobID:        0,
			CollectionID: s.collectionID,
			TimeoutTs:    math.MaxUint64,
			Schema:       &schemapb.CollectionSchema{},
		},
	}
	err = s.imeta.AddJob(job)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		Slots: 1,
	}, nil)
	s.cluster.EXPECT().PreImport(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().GetSessions().RunAndReturn(func() []*session.Session {
		sess := session.NewSession(&session.NodeInfo{
			NodeID: nodeID,
		}, nil)
		return []*session.Session{sess}
	})
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())
	s.Equal(int64(nodeID), task.GetNodeID())

	// inProgress -> completed
	s.cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())

	// drop import task
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func (s *ImportSchedulerSuite) TestProcessImport() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
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
		},
	}
	err := s.imeta.AddTask(task)
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
	}
	err = s.imeta.AddJob(job)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	s.alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	s.alloc.EXPECT().AllocTimestamp(mock.Anything).Return(300, nil)
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		Slots: 1,
	}, nil)
	s.cluster.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().GetSessions().RunAndReturn(func() []*session.Session {
		sess := session.NewSession(&session.NodeInfo{
			NodeID: nodeID,
		}, nil)
		return []*session.Session{sess}
	})
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_InProgress, task.GetState())
	s.Equal(int64(nodeID), task.GetNodeID())

	// inProgress -> completed
	s.cluster.ExpectedCalls = lo.Filter(s.cluster.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "QueryImport"
	})
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		State: datapb.ImportTaskStateV2_Completed,
	}, nil)
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Completed, task.GetState())

	// drop import task
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	s.scheduler.process()
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func (s *ImportSchedulerSuite) TestProcessFailed() {
	s.catalog.EXPECT().SaveImportJob(mock.Anything).Return(nil)
	s.catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        0,
			TaskID:       1,
			CollectionID: s.collectionID,
			NodeID:       6,
			SegmentIDs:   []int64{2, 3},
			State:        datapb.ImportTaskStateV2_Failed,
		},
	}
	err := s.imeta.AddTask(task)
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
	}
	err = s.imeta.AddJob(job)
	s.NoError(err)

	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		Slots: 1,
	}, nil)
	s.cluster.EXPECT().GetSessions().RunAndReturn(func() []*session.Session {
		sess := session.NewSession(&session.NodeInfo{
			NodeID: 6,
		}, nil)
		return []*session.Session{sess}
	})
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{ID: id, State: commonpb.SegmentState_Importing, IsImporting: true},
		}
		err = s.meta.AddSegment(context.Background(), segment)
		s.NoError(err)
	}
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := s.meta.GetSegment(id)
		s.NotNil(segment)
	}

	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil)
	s.scheduler.process()
	for _, id := range task.(*importTask).GetSegmentIDs() {
		segment := s.meta.GetSegment(id)
		s.Equal(commonpb.SegmentState_Dropped, segment.GetState())
	}
	task = s.imeta.GetTask(task.GetTaskID())
	s.Equal(datapb.ImportTaskStateV2_Failed, task.GetState())
	s.Equal(0, len(task.(*importTask).GetSegmentIDs()))
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func TestImportScheduler(t *testing.T) {
	suite.Run(t, new(ImportSchedulerSuite))
}
