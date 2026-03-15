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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	taskcommon "github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestBackfillCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(BackfillCompactionTaskSuite))
}

type BackfillCompactionTaskSuite struct {
	suite.Suite

	mockID    atomic.Int64
	mockAlloc *allocator.MockAllocator
	meta      *meta
	handler   *NMockHandler
	ievm      IndexEngineVersionManager
}

func (s *BackfillCompactionTaskSuite) SetupTest() {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(""))
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	broker := broker.NewMockBroker(s.T())
	broker.EXPECT().ShowCollectionIDs(mock.Anything).Return(nil, nil)
	meta, err := newMeta(ctx, catalog, cm, broker)
	s.NoError(err)
	s.meta = meta

	s.mockID.Store(time.Now().UnixMilli())
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	s.mockAlloc.EXPECT().AllocN(mock.Anything).RunAndReturn(func(x int64) (int64, int64, error) {
		start := s.mockID.Load()
		end := s.mockID.Add(int64(x))
		return start, end, nil
	}).Maybe()
	s.mockAlloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		end := s.mockID.Add(1)
		return end, nil
	}).Maybe()

	s.handler = NewNMockHandler(s.T())
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil).Maybe()
	s.ievm = newIndexEngineVersionManager()
}

func (s *BackfillCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *BackfillCompactionTaskSuite) generateBasicTask() *backfillCompactionTask {
	schema := &schemapb.CollectionSchema{
		Name:        "test_backfill_collection",
		Description: "test collection for backfill compaction",
		Version:     2,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
			},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  102,
				Name:     "sparse_vector",
				DataType: schemapb.DataType_SparseFloatVector,
			},
		},
	}

	// Create BM25 function schema
	// Note: FunctionSchema structure needs to be verified from actual proto definition
	// For now, we create an empty functions slice
	functions := []*schemapb.FunctionSchema{}

	compactionTask := &datapb.CompactionTask{
		PlanID:         1,
		TriggerID:      19530,
		CollectionID:   1,
		PartitionID:    10,
		Type:           datapb.CompactionType_BackfillCompaction,
		NodeID:         1,
		State:          datapb.CompactionTaskState_pipelining,
		Schema:         schema,
		InputSegments:  []int64{101},
		ResultSegments: []int64{1000},
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: 1000,
			End:   2000,
		},
		Channel: "ch-1",
	}

	task := newBackfillCompactionTask(compactionTask, s.mockAlloc, s.meta, s.handler, s.ievm, functions)
	return task
}

func (s *BackfillCompactionTaskSuite) TestBackfillCompactionTaskBasic() {
	task := s.generateBasicTask()

	// Test basic getters
	s.Equal(int64(1), task.GetTaskID())
	s.Equal(taskcommon.Compaction, task.GetTaskType())
	s.Equal(int64(1), task.GetSlotUsage())
	s.Equal("10-ch-1", task.GetLabel())
	s.Equal(int64(0), task.GetTaskVersion())

	// Test task proto
	taskProto := task.GetTaskProto()
	s.NotNil(taskProto)
	s.Equal(int64(1), taskProto.GetPlanID())
	s.Equal(int64(19530), taskProto.GetTriggerID())
	s.Equal(int64(1), taskProto.GetCollectionID())
	s.Equal(int64(10), taskProto.GetPartitionID())
	s.Equal(datapb.CompactionType_BackfillCompaction, taskProto.GetType())
	s.Equal(datapb.CompactionTaskState_pipelining, taskProto.GetState())
	s.Equal([]int64{101}, taskProto.GetInputSegments())
	s.Equal([]int64{1000}, taskProto.GetResultSegments())
}

func (s *BackfillCompactionTaskSuite) TestBuildCompactionRequest() {
	// Add a segment to meta
	segmentID := int64(101)
	err := s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			Binlogs: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{LogID: 1000, EntriesNum: 1000},
					},
				},
			},
		},
	})
	s.NoError(err)

	task := s.generateBasicTask()

	// Build compaction request
	plan, err := task.BuildCompactionRequest()
	s.NoError(err)
	s.NotNil(plan)

	// Verify plan
	s.Equal(int64(1), plan.GetPlanID())
	s.Equal(datapb.CompactionType_BackfillCompaction, plan.GetType())
	s.Equal("ch-1", plan.GetChannel())
	s.Equal(1, len(plan.GetSegmentBinlogs()))
	s.Equal(segmentID, plan.GetSegmentBinlogs()[0].GetSegmentID())
	s.Equal(int64(1), plan.GetSegmentBinlogs()[0].GetCollectionID())
	s.Equal(int64(10), plan.GetSegmentBinlogs()[0].GetPartitionID())
	s.NotNil(plan.GetFunctions())
}

func (s *BackfillCompactionTaskSuite) TestBuildCompactionRequestSegmentNotFound() {
	task := s.generateBasicTask()

	// Try to build compaction request without adding segment to meta
	plan, err := task.BuildCompactionRequest()
	s.Error(err)
	s.Nil(plan)
	s.Contains(err.Error(), "segment not found")
}

func (s *BackfillCompactionTaskSuite) TestCreateTaskOnWorker() {
	s.Run("CreateTaskOnWorker fail, segment not found", func() {
		task := s.generateBasicTask()
		cluster := session.NewMockCluster(s.T())
		task.CreateTaskOnWorker(1, cluster)
		s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	})

	s.Run("CreateTaskOnWorker fail, CreateCompaction error", func() {
		// Add segment to meta
		segmentID := int64(101)
		err := s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  1,
				PartitionID:   10,
				InsertChannel: "ch-1",
				Level:         datapb.SegmentLevel_L1,
				State:         commonpb.SegmentState_Flushed,
				NumOfRows:     1000,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 101,
						Binlogs: []*datapb.Binlog{
							{LogID: 1000, EntriesNum: 1000},
						},
					},
				},
			},
		})
		s.NoError(err)

		task := s.generateBasicTask()
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything).Return(merr.WrapErrNodeNotFound(1))
		task.CreateTaskOnWorker(1, cluster)
		// Should remain in pipelining state when CreateCompaction fails
		s.Equal(datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
		// NodeID should be set to NullNodeID (-1) when CreateCompaction fails
		s.Equal(int64(-1), task.GetTaskProto().GetNodeID())
	})

	s.Run("CreateTaskOnWorker succeed", func() {
		// Add segment to meta
		segmentID := int64(101)
		err := s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  1,
				PartitionID:   10,
				InsertChannel: "ch-1",
				Level:         datapb.SegmentLevel_L1,
				State:         commonpb.SegmentState_Flushed,
				NumOfRows:     1000,
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID: 101,
						Binlogs: []*datapb.Binlog{
							{LogID: 1000, EntriesNum: 1000},
						},
					},
				},
			},
		})
		s.NoError(err)

		task := s.generateBasicTask()
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateCompaction(mock.Anything, mock.Anything).Return(nil)
		task.CreateTaskOnWorker(1, cluster)
		s.Equal(datapb.CompactionTaskState_executing, task.GetTaskProto().GetState())
		s.Equal(int64(1), task.GetTaskProto().GetNodeID())
	})
}

func (s *BackfillCompactionTaskSuite) TestQueryTaskOnWorker() {
	s.Run("QueryTaskOnWorker, node not found", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(1)).Once()
		task.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_pipelining, task.GetTaskProto().GetState())
		s.Equal(int64(-1), task.GetTaskProto().GetNodeID())
	})

	s.Run("QueryTaskOnWorker, result is nil", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(nil, nil).Once()
		task.QueryTaskOnWorker(cluster)
		// State should remain unchanged when result is nil
		s.Equal(datapb.CompactionTaskState_executing, task.GetTaskProto().GetState())
	})

	s.Run("QueryTaskOnWorker, completed with empty segments", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State:    datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{},
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	})

	s.Run("QueryTaskOnWorker, pipelining state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_pipelining,
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)
		// State should remain unchanged
		s.Equal(datapb.CompactionTaskState_executing, task.GetTaskProto().GetState())
	})

	s.Run("QueryTaskOnWorker, executing state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_executing,
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)
		// State should remain unchanged
		s.Equal(datapb.CompactionTaskState_executing, task.GetTaskProto().GetState())
	})

	s.Run("QueryTaskOnWorker, timeout state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_timeout,
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_timeout, task.GetTaskProto().GetState())
	})

	s.Run("QueryTaskOnWorker, failed state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryCompaction(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_failed,
		}, nil).Once()
		task.QueryTaskOnWorker(cluster)
		s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	})
}

func (s *BackfillCompactionTaskSuite) TestProcess() {
	s.Run("Process meta_saved state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_meta_saved)))
		result := task.Process()
		// processMetaSaved should transition to completed and return true
		s.True(result)
		s.Equal(datapb.CompactionTaskState_completed, task.GetTaskProto().GetState())
	})

	s.Run("Process completed state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_completed)))
		result := task.Process()
		s.True(result)
		s.Equal(datapb.CompactionTaskState_completed, task.GetTaskProto().GetState())
	})

	s.Run("Process failed state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_failed)))
		result := task.Process()
		s.True(result)
		s.Equal(datapb.CompactionTaskState_failed, task.GetTaskProto().GetState())
	})

	s.Run("Process timeout state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_timeout)))
		result := task.Process()
		s.True(result)
		s.Equal(datapb.CompactionTaskState_timeout, task.GetTaskProto().GetState())
	})

	s.Run("Process other states return false", func() {
		testStates := []datapb.CompactionTaskState{
			datapb.CompactionTaskState_pipelining,
			datapb.CompactionTaskState_executing,
			datapb.CompactionTaskState_unknown,
		}
		for _, state := range testStates {
			task := s.generateBasicTask()
			task.SetTask(task.ShadowClone(setState(state)))
			result := task.Process()
			s.False(result, "state %s should return false", state.String())
		}
	})
}

func (s *BackfillCompactionTaskSuite) TestClean() {
	task := s.generateBasicTask()
	// Mark segment as compacting
	s.meta.SetSegmentsCompacting(context.TODO(), []int64{101}, true)

	// Add segment to meta
	segmentID := int64(101)
	err := s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  1,
			PartitionID:   10,
			InsertChannel: "ch-1",
			Level:         datapb.SegmentLevel_L1,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
		},
	})
	s.NoError(err)

	result := task.Clean()
	s.True(result)
	s.Equal(datapb.CompactionTaskState_cleaned, task.GetTaskProto().GetState())

	// Verify segment compacting flag is reset
	seg := s.meta.GetSegment(context.TODO(), segmentID)
	s.NotNil(seg)
	s.False(seg.isCompacting)
}

func (s *BackfillCompactionTaskSuite) TestNeedReAssignNodeID() {
	s.Run("NeedReAssignNodeID, pipelining with nodeID 0", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_pipelining), setNodeID(0)))
		s.True(task.NeedReAssignNodeID())
	})

	s.Run("NeedReAssignNodeID, pipelining with NullNodeID", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_pipelining), setNodeID(-1)))
		s.True(task.NeedReAssignNodeID())
	})

	s.Run("NeedReAssignNodeID, pipelining with valid nodeID", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_pipelining), setNodeID(1)))
		s.False(task.NeedReAssignNodeID())
	})

	s.Run("NeedReAssignNodeID, non-pipelining state", func() {
		task := s.generateBasicTask()
		task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(0)))
		s.False(task.NeedReAssignNodeID())
	})
}

func (s *BackfillCompactionTaskSuite) TestDropTaskOnWorker() {
	task := s.generateBasicTask()
	task.SetTask(task.ShadowClone(setState(datapb.CompactionTaskState_executing), setNodeID(1)))
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().DropCompaction(mock.Anything, mock.Anything).Return(nil).Once()
	task.DropTaskOnWorker(cluster)
}

func (s *BackfillCompactionTaskSuite) TestSetNodeID() {
	task := s.generateBasicTask()
	err := task.SetNodeID(100)
	s.NoError(err)
	s.Equal(int64(100), task.GetTaskProto().GetNodeID())
}
