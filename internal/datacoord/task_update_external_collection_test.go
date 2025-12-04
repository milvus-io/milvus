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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	kvcatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type updateExternalCollectionTaskSuite struct {
	suite.Suite
	mt *meta

	collID int64
	taskID int64
	nodeID int64

	externalSource string
	externalSpec   string
}

func Test_updateExternalCollectionTaskSuite(t *testing.T) {
	suite.Run(t, new(updateExternalCollectionTaskSuite))
}

func (s *updateExternalCollectionTaskSuite) SetupSuite() {
	s.collID = 100
	s.taskID = 200
	s.nodeID = 1
	s.externalSource = "s3"
	s.externalSpec = "spec"
}

func (s *updateExternalCollectionTaskSuite) SetupTest() {
	catalog := kvcatalog.NewCatalog(NewMetaMemoryKV(), "", "")
	mockBroker := broker.NewMockBroker(s.T())
	mockBroker.EXPECT().ShowCollectionIDs(mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
		Status: merr.Success(),
		DbCollections: []*rootcoordpb.DBCollections{
			{
				DbName:        "default",
				CollectionIDs: []int64{s.collID},
			},
		},
	}, nil)

	mt, err := newMeta(context.Background(), catalog, nil, mockBroker)
	s.Require().NoError(err)
	s.mt = mt
	// ensure each test starts from a clean segment/task state
	s.mt.segments = NewSegmentsInfo()
	s.mt.externalCollectionTaskMeta = &externalCollectionTaskMeta{
		ctx:                context.Background(),
		catalog:            catalog,
		keyLock:            lock.NewKeyLock[UniqueID](),
		tasks:              typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
		collectionID2Tasks: typeutil.NewConcurrentMap[UniqueID, *indexpb.UpdateExternalCollectionTask](),
	}

	collection := &collectionInfo{
		ID:     s.collID,
		Schema: newTestSchema(),
	}
	collection.Schema.ExternalSource = s.externalSource
	collection.Schema.ExternalSpec = s.externalSpec
	s.mt.AddCollection(collection)
}

func (s *updateExternalCollectionTaskSuite) TestSetJobInfo_KeepAndAddSegments() {
	// Setup: Create initial segments
	seg1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: s.collID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    1000,
		},
	}
	seg2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: s.collID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    2000,
		},
	}
	s.mt.segments.SetSegment(1, seg1)
	s.mt.segments.SetSegment(2, seg2)

	// Create mock allocator
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(100), nil).Once()
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(101), nil).Once()

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
		State:          indexpb.JobState_JobStateInit,
	}, s.mt, mockAlloc)

	// Create response: keep segment 1, drop segment 2, add 2 new segments
	resp := &datapb.UpdateExternalCollectionResponse{
		KeptSegments: []int64{1},
		UpdatedSegments: []*datapb.SegmentInfo{
			{
				ID:           10, // placeholder
				CollectionID: s.collID,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    3000,
			},
			{
				ID:           20, // placeholder
				CollectionID: s.collID,
				State:        commonpb.SegmentState_Flushed,
				NumOfRows:    4000,
			},
		},
		State: indexpb.JobState_JobStateFinished,
	}

	// Execute
	err := task.SetJobInfo(context.Background(), resp)
	s.NoError(err)

	// Verify: segment 1 should still be there and flushed
	seg1After := s.mt.segments.GetSegment(1)
	s.NotNil(seg1After)
	s.Equal(commonpb.SegmentState_Flushed, seg1After.GetState())

	// Verify: segment 2 should be dropped
	seg2After := s.mt.segments.GetSegment(2)
	s.NotNil(seg2After)
	s.Equal(commonpb.SegmentState_Dropped, seg2After.GetState())

	// Verify: new segments should be added with allocated IDs
	newSeg1 := s.mt.segments.GetSegment(100)
	s.NotNil(newSeg1)
	s.Equal(int64(3000), newSeg1.GetNumOfRows())
	s.Equal(commonpb.SegmentState_Flushed, newSeg1.GetState())

	newSeg2 := s.mt.segments.GetSegment(101)
	s.NotNil(newSeg2)
	s.Equal(int64(4000), newSeg2.GetNumOfRows())
	s.Equal(commonpb.SegmentState_Flushed, newSeg2.GetState())
}

func (s *updateExternalCollectionTaskSuite) TestSetJobInfo_DropAllSegments() {
	// Setup: Create initial segments
	seg1 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           1,
			CollectionID: s.collID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    1000,
		},
	}
	seg2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:           2,
			CollectionID: s.collID,
			State:        commonpb.SegmentState_Flushed,
			NumOfRows:    2000,
		},
	}
	s.mt.segments.SetSegment(1, seg1)
	s.mt.segments.SetSegment(2, seg2)

	// Create mock allocator
	mockAlloc := allocator.NewMockAllocator(s.T())

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
		State:          indexpb.JobState_JobStateInit,
	}, s.mt, mockAlloc)

	// Create response: drop all segments, no new segments
	resp := &datapb.UpdateExternalCollectionResponse{
		KeptSegments:    []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{},
		State:           indexpb.JobState_JobStateFinished,
	}

	// Execute
	err := task.SetJobInfo(context.Background(), resp)
	s.NoError(err)

	// Verify: all segments should be dropped
	seg1After := s.mt.segments.GetSegment(1)
	s.NotNil(seg1After)
	s.Equal(commonpb.SegmentState_Dropped, seg1After.GetState())

	seg2After := s.mt.segments.GetSegment(2)
	s.NotNil(seg2After)
	s.Equal(commonpb.SegmentState_Dropped, seg2After.GetState())
}

func (s *updateExternalCollectionTaskSuite) TestSetJobInfo_AllocatorError() {
	// Create mock allocator that fails
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(0), errors.New("alloc failed"))

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
		State:          indexpb.JobState_JobStateInit,
	}, s.mt, mockAlloc)

	// Create response with new segments
	resp := &datapb.UpdateExternalCollectionResponse{
		KeptSegments: []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{
			{
				ID:           10,
				CollectionID: s.collID,
				NumOfRows:    1000,
			},
		},
		State: indexpb.JobState_JobStateFinished,
	}

	// Execute
	err := task.SetJobInfo(context.Background(), resp)
	s.Error(err)
	s.Contains(err.Error(), "alloc failed")
}

func (s *updateExternalCollectionTaskSuite) TestQueryTaskOnWorker_Success() {
	// Create mock allocator
	mockAlloc := allocator.NewMockAllocator(s.T())
	mockAlloc.EXPECT().AllocID(mock.Anything).Return(int64(100), nil).Maybe()

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		State:          indexpb.JobState_JobStateInProgress,
		NodeID:         s.nodeID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
	}, s.mt, mockAlloc)

	// Add task to meta
	s.mt.externalCollectionTaskMeta.tasks.Insert(s.taskID, task.UpdateExternalCollectionTask)

	// Create mock cluster
	mockCluster := session.NewMockCluster(s.T())
	mockCluster.EXPECT().QueryExternalCollectionTask(s.nodeID, s.taskID).Return(&datapb.UpdateExternalCollectionResponse{
		State:           indexpb.JobState_JobStateFinished,
		KeptSegments:    []int64{},
		UpdatedSegments: []*datapb.SegmentInfo{},
	}, nil)

	// Execute
	task.QueryTaskOnWorker(mockCluster)

	// Verify task state is finished
	s.Equal(indexpb.JobState_JobStateFinished, task.GetState())
}

func (s *updateExternalCollectionTaskSuite) TestQueryTaskOnWorker_Failed() {
	// Create mock allocator
	mockAlloc := allocator.NewMockAllocator(s.T())

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		State:          indexpb.JobState_JobStateInProgress,
		NodeID:         s.nodeID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
	}, s.mt, mockAlloc)

	// Add task to meta
	s.mt.externalCollectionTaskMeta.tasks.Insert(s.taskID, task.UpdateExternalCollectionTask)

	// Create mock cluster that returns failed state
	mockCluster := session.NewMockCluster(s.T())
	mockCluster.EXPECT().QueryExternalCollectionTask(s.nodeID, s.taskID).Return(&datapb.UpdateExternalCollectionResponse{
		State:      indexpb.JobState_JobStateFailed,
		FailReason: "task execution failed",
	}, nil)

	// Execute
	task.QueryTaskOnWorker(mockCluster)

	// Verify task state is failed
	s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	s.Equal("task execution failed", task.GetFailReason())
}

func (s *updateExternalCollectionTaskSuite) TestQueryTaskOnWorker_QueryError() {
	// Create mock allocator
	mockAlloc := allocator.NewMockAllocator(s.T())

	// Create task
	task := newUpdateExternalCollectionTask(&indexpb.UpdateExternalCollectionTask{
		TaskID:         s.taskID,
		CollectionID:   s.collID,
		State:          indexpb.JobState_JobStateInProgress,
		NodeID:         s.nodeID,
		ExternalSource: s.externalSource,
		ExternalSpec:   s.externalSpec,
	}, s.mt, mockAlloc)

	// Add task to meta
	s.mt.externalCollectionTaskMeta.tasks.Insert(s.taskID, task.UpdateExternalCollectionTask)

	// Create mock cluster that returns error
	mockCluster := session.NewMockCluster(s.T())
	mockCluster.EXPECT().QueryExternalCollectionTask(s.nodeID, s.taskID).Return(nil, errors.New("query failed"))

	// Execute
	task.QueryTaskOnWorker(mockCluster)

	// Verify task state is failed
	s.Equal(indexpb.JobState_JobStateFailed, task.GetState())
	s.Contains(task.GetFailReason(), "query task failed")
}
