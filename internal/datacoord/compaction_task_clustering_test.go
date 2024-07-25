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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestClusteringCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskSuite))
}

type ClusteringCompactionTaskSuite struct {
	suite.Suite

	mockID      atomic.Int64
	mockAlloc   *NMockAllocator
	meta        *meta
	mockSessMgr *MockSessionManager
	handler     *NMockHandler
	session     *MockSessionManager
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	cm := storage.NewLocalChunkManager(storage.RootPath(""))
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	meta, err := newMeta(context.TODO(), catalog, cm)
	s.NoError(err)
	s.meta = meta

	s.mockSessMgr = NewMockSessionManager(s.T())

	s.mockID.Store(time.Now().UnixMilli())
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockAlloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(x int64) (int64, int64, error) {
		start := s.mockID.Load()
		end := s.mockID.Add(int64(x))
		return start, end, nil
	}).Maybe()
	s.mockAlloc.EXPECT().allocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		end := s.mockID.Add(1)
		return end, nil
	}).Maybe()

	s.handler = NewNMockHandler(s.T())
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil).Maybe()

	s.session = NewMockSessionManager(s.T())
}

func (s *ClusteringCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ClusteringCompactionTaskSuite) TestClusteringCompactionSegmentMetaChange() {
	channel := "Ch-1"

	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    101,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    102,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			PartitionStatsVersion: 10000,
		},
	})
	session := NewSessionManagerImpl()

	schema := ConstructScalarClusteringSchema("TestClusteringCompactionTask", 32, true)
	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          true,
		IsClusteringKey: true,
	}

	task := &clusteringCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:             1,
			TriggerID:          19530,
			CollectionID:       1,
			PartitionID:        10,
			Channel:            channel,
			Type:               datapb.CompactionType_ClusteringCompaction,
			NodeID:             1,
			State:              datapb.CompactionTaskState_pipelining,
			Schema:             schema,
			ClusteringKeyField: pk,
			InputSegments:      []int64{101, 102},
			ResultSegments:     []int64{1000, 1100},
		},
		meta:     s.meta,
		sessions: session,
	}

	task.processPipelining()

	seg11 := s.meta.GetSegment(101)
	s.Equal(datapb.SegmentLevel_L2, seg11.Level)
	seg21 := s.meta.GetSegment(102)
	s.Equal(datapb.SegmentLevel_L2, seg21.Level)
	s.Equal(int64(10000), seg21.PartitionStatsVersion)

	task.ResultSegments = []int64{103, 104}
	// fake some compaction result segment
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    103,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    104,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})

	task.processFailedOrTimeout()

	seg12 := s.meta.GetSegment(101)
	s.Equal(datapb.SegmentLevel_L1, seg12.Level)
	seg22 := s.meta.GetSegment(102)
	s.Equal(datapb.SegmentLevel_L2, seg22.Level)
	s.Equal(int64(10000), seg22.PartitionStatsVersion)

	seg32 := s.meta.GetSegment(103)
	s.Equal(datapb.SegmentLevel_L1, seg32.Level)
	s.Equal(int64(0), seg32.PartitionStatsVersion)
	seg42 := s.meta.GetSegment(104)
	s.Equal(datapb.SegmentLevel_L1, seg42.Level)
	s.Equal(int64(0), seg42.PartitionStatsVersion)
}

func (s *ClusteringCompactionTaskSuite) generateBasicTask() *clusteringCompactionTask {
	schema := ConstructScalarClusteringSchema("TestClusteringCompactionTask", 32, true)
	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            Int64Field,
		IsPrimaryKey:    true,
		DataType:        schemapb.DataType_Int64,
		AutoID:          true,
		IsClusteringKey: true,
	}

	task := &clusteringCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:             1,
			TriggerID:          19530,
			CollectionID:       1,
			PartitionID:        10,
			Type:               datapb.CompactionType_ClusteringCompaction,
			NodeID:             1,
			State:              datapb.CompactionTaskState_pipelining,
			Schema:             schema,
			ClusteringKeyField: pk,
			InputSegments:      []int64{101, 102},
			ResultSegments:     []int64{1000, 1100},
		},
		meta:     s.meta,
		handler:  s.handler,
		sessions: s.session,
	}
	return task
}

func (s *ClusteringCompactionTaskSuite) TestProcessRetryLogic() {
	task := s.generateBasicTask()
	task.maxRetryTimes = 3
	// process pipelining fail
	s.Equal(false, task.Process())
	s.Equal(int32(1), task.RetryTimes)
	s.Equal(false, task.Process())
	s.Equal(int32(2), task.RetryTimes)
	s.Equal(false, task.Process())
	s.Equal(int32(3), task.RetryTimes)
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetState())
	s.Equal(false, task.Process())
	s.Equal(int32(0), task.RetryTimes)
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())
}

func (s *ClusteringCompactionTaskSuite) TestProcessStateChange() {
	task := s.generateBasicTask()

	// process pipelining fail
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	// process pipelining succeed
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    101,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    102,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			PartitionStatsVersion: 10000,
		},
	})

	s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	task.State = datapb.CompactionTaskState_pipelining
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_executing, task.GetState())

	// process executing
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(1)).Once()
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetState())

	// repipelining
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetState())
	task.NodeID = 1
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_executing, task.GetState())

	// process executing
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(nil, nil).Once()
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_executing, task.GetState())
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_executing,
	}, nil).Once()
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_executing, task.GetState())
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID: 1000,
			},
			{
				SegmentID: 1001,
			},
		},
	}, nil).Once()
	s.Equal(false, task.Process())
	s.Equal(datapb.CompactionTaskState_indexing, task.GetState())
}

func (s *ClusteringCompactionTaskSuite) TestProcessExecuting() {
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    101,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    102,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			PartitionStatsVersion: 10000,
		},
	})
	task := s.generateBasicTask()
	s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(merr.WrapErrDataNodeSlotExhausted())
	task.State = datapb.CompactionTaskState_pipelining
	s.NoError(task.doCompact())
	s.Equal(int64(NullNodeID), task.GetNodeID())
}

func (s *ClusteringCompactionTaskSuite) TestProcessExecutingState() {
	task := s.generateBasicTask()
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_failed,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_indexing,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_pipelining,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_completed,
	}, nil).Once()
	s.Error(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID: 1000,
			},
			{
				SegmentID: 1001,
			},
		},
	}, nil).Once()
	s.Error(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())
}

const (
	Int64Field    = "int64Field"
	FloatVecField = "floatVecField"
)

func ConstructScalarClusteringSchema(collection string, dim int, autoID bool, fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	// if fields are specified, construct it
	if len(fields) > 0 {
		return &schemapb.CollectionSchema{
			Name:   collection,
			AutoID: autoID,
			Fields: fields,
		}
	}

	// if no field is specified, use default
	pk := &schemapb.FieldSchema{
		FieldID:         100,
		Name:            Int64Field,
		IsPrimaryKey:    true,
		Description:     "",
		DataType:        schemapb.DataType_Int64,
		TypeParams:      nil,
		IndexParams:     nil,
		AutoID:          autoID,
		IsClusteringKey: true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}
}
