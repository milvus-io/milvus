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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type indexTaskSuite struct {
	suite.Suite
	mt *meta

	collID   int64
	partID   int64
	indexID  int64
	segID    int64
	taskID   int64
	targetID int64
	fieldID  int64
}

func Test_indexTaskSuite(t *testing.T) {
	suite.Run(t, new(indexTaskSuite))
}

func (s *indexTaskSuite) SetupSuite() {
	s.collID = 1
	s.partID = 2
	s.indexID = 3
	s.fieldID = 4
	s.taskID = 1178
	s.segID = 1179
	s.targetID = 1180

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.mt = &meta{
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				s.segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            s.segID,
						CollectionID:  s.collID,
						PartitionID:   s.partID,
						InsertChannel: "ch1",
						NumOfRows:     65535,
						State:         commonpb.SegmentState_Flushed,
						MaxRowNum:     65535,
						Level:         datapb.SegmentLevel_L2,
					},
				},
			},
		},
		indexMeta: createIndexMetaWithSegment(catalog, s.collID, s.partID, s.segID, s.indexID, s.fieldID, s.taskID),
	}
}

func (s *indexTaskSuite) TestBasicTaskOperations() {
	t := &model.SegmentIndex{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		IndexID:      s.indexID,
		BuildID:      s.taskID,
		IndexState:   commonpb.IndexState_Unissued,
		NumRows:      65535,
	}
	it := newIndexBuildTask(t, 1, s.mt, nil, nil, nil)

	s.Run("task type and state", func() {
		s.Equal(taskcommon.Index, it.GetTaskType())
		s.Equal(taskcommon.State(it.IndexState), it.GetTaskState())
		s.Equal(int64(1), it.GetTaskSlot())
	})

	s.Run("time management", func() {
		now := time.Now()

		it.SetTaskTime(taskcommon.TimeQueue, now)
		s.Equal(now, it.GetTaskTime(taskcommon.TimeQueue))

		it.SetTaskTime(taskcommon.TimeStart, now)
		s.Equal(now, it.GetTaskTime(taskcommon.TimeStart))

		it.SetTaskTime(taskcommon.TimeEnd, now)
		s.Equal(now, it.GetTaskTime(taskcommon.TimeEnd))
	})

	s.Run("state management", func() {
		it.SetState(indexpb.JobState_JobStateInProgress, "test reason")
		s.Equal(indexpb.JobState_JobStateInProgress, indexpb.JobState(it.IndexState))
		s.Equal("test reason", it.FailReason)
	})
}

func (s *indexTaskSuite) TestCreateTaskOnWorker() {
	t := &model.SegmentIndex{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		IndexID:      s.indexID,
		BuildID:      s.taskID,
		IndexState:   commonpb.IndexState_Unissued,
		NumRows:      65535,
	}
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
		ID: s.collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					Name:     "field1",
					FieldID:  s.fieldID,
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "128"},
					},
				},
			},
		},
		Partitions: []int64{s.partID},
	}, nil)
	cm := mocks.NewChunkManager(s.T())
	cm.EXPECT().RootPath().Return("root")
	it := newIndexBuildTask(t, 1, s.mt, handler, cm, newIndexEngineVersionManager())

	s.Run("task not exist in meta", func() {
		s.mt.indexMeta.segmentBuildInfo.buildID2SegmentIndex.Remove(s.taskID)
		cluster := session.NewMockCluster(s.T())
		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateNone, indexpb.JobState(it.IndexState))
	})

	s.Run("segment not healthy", func() {
		s.mt.indexMeta.segmentBuildInfo.buildID2SegmentIndex.Insert(s.taskID, &model.SegmentIndex{
			CollectionID: s.collID,
			PartitionID:  s.partID,
			SegmentID:    s.segID,
			IndexID:      s.indexID,
			BuildID:      s.taskID,
			IndexState:   commonpb.IndexState_Unissued,
			NumRows:      65535,
		})
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Dropped
		cluster := session.NewMockCluster(s.T())
		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateNone, indexpb.JobState(it.IndexState))
	})

	s.Run("index not exist", func() {
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		s.mt.indexMeta.indexes[s.collID][s.indexID].IsDeleted = true
		defer func() {
			s.mt.indexMeta.indexes[s.collID][s.indexID].IsDeleted = false
		}()
		cluster := session.NewMockCluster(s.T())
		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateNone, indexpb.JobState(it.IndexState))
	})

	s.Run("update version failed", func() {
		it.SetState(indexpb.JobState_JobStateInit, "")
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		s.mt.indexMeta.catalog = catalogMock

		cluster := session.NewMockCluster(s.T())
		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("create job on worker failed", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)

		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("Update Inprogress failed", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Once()
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error")).Once()
		s.mt.indexMeta.catalog = catalogMock

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil)
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)

		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("successful creation", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateIndex(mock.Anything, mock.Anything).Return(nil)

		it.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, indexpb.JobState(it.IndexState))
	})
}

func (s *indexTaskSuite) TestQueryTaskOnWorker() {
	t := &model.SegmentIndex{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		IndexID:      s.indexID,
		BuildID:      s.taskID,
		IndexState:   commonpb.IndexState_InProgress,
	}
	it := newIndexBuildTask(t, 1, s.mt, nil, nil, nil)
	it.NodeID = 1
	s.Run("worker not found", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(nil, merr.ErrNodeNotFound)
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)
		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("query failed", func() {
		it.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error"))
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)
		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("task finished", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		it.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(&workerpb.IndexJobResults{
			Results: []*workerpb.IndexTaskInfo{{
				BuildID: s.taskID,
				State:   commonpb.IndexState_Finished,
			}},
		}, nil)

		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateFinished, indexpb.JobState(it.IndexState))
	})

	s.Run("return retry", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		it.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(&workerpb.IndexJobResults{
			Results: []*workerpb.IndexTaskInfo{{
				BuildID:    s.taskID,
				State:      commonpb.IndexState_Retry,
				FailReason: "mock error",
			}},
		}, nil)
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)

		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
		s.Equal("mock error", it.FailReason)
	})

	s.Run("return none", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		it.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(&workerpb.IndexJobResults{
			Results: []*workerpb.IndexTaskInfo{{
				BuildID: s.taskID,
				State:   commonpb.IndexState_IndexStateNone,
			}},
		}, nil)
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)

		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})

	s.Run("worker does not have task", func() {
		catalogMock := catalogmocks.NewDataCoordCatalog(s.T())
		catalogMock.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)
		s.mt.indexMeta.catalog = catalogMock

		it.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryIndex(mock.Anything, mock.Anything).Return(&workerpb.IndexJobResults{}, nil)

		it.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, indexpb.JobState(it.IndexState))
	})
}

func (s *indexTaskSuite) TestDropTaskOnWorker() {
	t := &model.SegmentIndex{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		IndexID:      s.indexID,
		BuildID:      s.taskID,
		IndexState:   commonpb.IndexState_Unissued,
	}
	it := newIndexBuildTask(t, 1, s.mt, nil, nil, nil)
	it.NodeID = 1

	s.Run("worker not found", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		it.DropTaskOnWorker(cluster)
	})

	s.Run("drop failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		it.DropTaskOnWorker(cluster)
	})

	s.Run("drop success", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropIndex(mock.Anything, mock.Anything).Return(nil)
		it.DropTaskOnWorker(cluster)
	})
}

func (s *indexTaskSuite) TestSetJobInfo() {
	t := &model.SegmentIndex{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		IndexID:      s.indexID,
		BuildID:      s.taskID,
		IndexState:   commonpb.IndexState_Unissued,
	}
	it := newIndexBuildTask(t, 1, s.mt, nil, nil, nil)
	result := &workerpb.IndexTaskInfo{
		BuildID: s.taskID,
		State:   commonpb.IndexState_Finished,
	}

	s.Run("save index result failed", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.indexMeta.catalog = catalog
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
		err := it.setJobInfo(result)
		s.Error(err)
	})

	s.Run("successful update", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.indexMeta.catalog = catalog
		catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil)

		err := it.setJobInfo(result)
		s.NoError(err)
		s.Equal(indexpb.JobState_JobStateFinished, indexpb.JobState(it.IndexState))
	})
}
