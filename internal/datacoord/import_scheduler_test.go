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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type ImportSchedulerSuite struct {
	suite.Suite

	collectionID int64

	catalog   *mocks.DataCoordCatalog
	alloc     *NMockAllocator
	cluster   *MockCluster
	meta      *meta
	imeta     ImportMeta
	scheduler *importScheduler
}

func (s *ImportSchedulerSuite) SetupTest() {
	var err error

	s.collectionID = 1

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().ListImportTasks().Return(nil, nil)
	s.catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	s.catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)

	s.cluster = NewMockCluster(s.T())
	s.alloc = NewNMockAllocator(s.T())
	s.meta, err = newMeta(context.TODO(), s.catalog, nil)
	s.NoError(err)
	s.meta.AddCollection(&collectionInfo{
		ID:     s.collectionID,
		Schema: newTestSchema(),
	})
	s.imeta, err = NewImportMeta(s.catalog)
	s.NoError(err)
	s.scheduler = NewImportScheduler(s.meta, s.cluster, s.alloc, s.imeta).(*importScheduler)
}

func (s *ImportSchedulerSuite) TestProcessPreImport() {
	s.catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	var task ImportTask = &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID:    0,
			TaskID:       1,
			CollectionID: s.collectionID,
			State:        internalpb.ImportState_Pending,
		},
	}
	err := s.imeta.Add(task)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		Slots: 1,
	}, nil)
	s.cluster.EXPECT().PreImport(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().GetSessions().Return([]*Session{
		{
			info: &NodeInfo{
				NodeID: nodeID,
			},
		},
	})
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(internalpb.ImportState_InProgress, task.GetState())
	s.Equal(int64(nodeID), task.GetNodeID())

	// inProgress -> completed
	s.cluster.EXPECT().QueryPreImport(mock.Anything, mock.Anything).Return(&datapb.QueryPreImportResponse{
		State: internalpb.ImportState_Completed,
	}, nil)
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(internalpb.ImportState_Completed, task.GetState())

	// drop import task
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func (s *ImportSchedulerSuite) TestProcessImport() {
	s.catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	var task ImportTask = &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID:    0,
			TaskID:       1,
			CollectionID: s.collectionID,
			State:        internalpb.ImportState_Pending,
			FileStats: []*datapb.ImportFileStats{
				{
					HashedRows: map[string]*datapb.PartitionRows{
						"channel1": {
							PartitionRows: map[int64]int64{
								int64(2): 100,
							},
						},
					},
				},
			},
		},
	}
	err := s.imeta.Add(task)
	s.NoError(err)

	// pending -> inProgress
	const nodeID = 10
	const segmentID = 20
	s.alloc.EXPECT().allocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		return segmentID, nil
	})
	s.alloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)
	s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		Slots: 1,
	}, nil)
	s.cluster.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(nil)
	s.cluster.EXPECT().GetSessions().Return([]*Session{
		{
			info: &NodeInfo{
				NodeID: nodeID,
			},
		},
	})
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(internalpb.ImportState_InProgress, task.GetState())
	s.Equal(int64(nodeID), task.GetNodeID())
	s.Equal(1, len(task.(*importTask).GetSegmentIDs()))
	s.Equal(int64(segmentID), task.(*importTask).GetSegmentIDs()[0])

	// inProgress -> completed
	s.catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.cluster.ExpectedCalls = lo.Filter(s.cluster.ExpectedCalls, func(call *mock.Call, _ int) bool {
		return call.Method != "QueryImport"
	})
	s.cluster.EXPECT().QueryImport(mock.Anything, mock.Anything).Return(&datapb.QueryImportResponse{
		State: internalpb.ImportState_Completed,
		ImportSegmentsInfo: []*datapb.ImportSegmentInfo{
			{
				SegmentID:    segmentID,
				ImportedRows: 100,
			},
		},
	}, nil)
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(internalpb.ImportState_Completed, task.GetState())

	// drop import task
	s.cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	s.scheduler.process()
	task = s.imeta.Get(task.GetTaskID())
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func TestImportScheduler(t *testing.T) {
	suite.Run(t, new(ImportSchedulerSuite))
}
