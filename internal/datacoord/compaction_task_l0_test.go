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
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestL0CompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(L0CompactionTaskSuite))
}

type L0CompactionTaskSuite struct {
	suite.Suite

	mockAlloc   *allocator.MockAllocator
	mockMeta    *MockCompactionMeta
	mockSessMgr *session.MockDataNodeManager
}

func (s *L0CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockSessMgr = session.NewMockDataNodeManager(s.T())
	s.mockAlloc = allocator.NewMockAllocator(s.T())
	// s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
}

func (s *L0CompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_NormalL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
		[]*SegmentInfo{
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            200,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            201,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
			{SegmentInfo: &datapb.SegmentInfo{
				ID:            202,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			}},
		},
	)

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta, nil)
	alloc := allocator.NewMockAllocator(s.T())
	alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc
	plan, err := task.BuildCompactionRequest()
	s.Require().NoError(err)

	s.Equal(5, len(plan.GetSegmentBinlogs()))
	segIDs := lo.Map(plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
		return b.GetSegmentID()
	})

	s.ElementsMatch([]int64{200, 201, 202, 100, 101}, segIDs)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SegmentNotFoundL0() {
	channel := "Ch-1"
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return nil
	}).Once()
	task := newL0CompactionTask(&datapb.CompactionTask{
		InputSegments: []int64{102},
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Channel:       channel,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
	}, nil, s.mockMeta, nil)

	_, err := task.BuildCompactionRequest()
	s.Error(err)
	s.ErrorIs(err, merr.ErrSegmentNotFound)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SelectZeroSegmentsL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	task := newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        1,
		State:         datapb.CompactionTaskState_executing,
		InputSegments: []int64{100, 101},
	}, nil, s.mockMeta, nil)
	_, err := task.BuildCompactionRequest()
	s.Error(err)
}

func (s *L0CompactionTaskSuite) TestBuildCompactionRequestFailed_AllocFailed() {
	var task CompactionTask

	s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, errors.New("mock alloc err"))

	meta, err := newMemoryMeta(s.T())
	s.NoError(err)
	task = &l0CompactionTask{
		allocator: s.mockAlloc,
		meta:      meta,
	}
	task.SetTask(&datapb.CompactionTask{})
	_, err = task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)

	task = &mixCompactionTask{
		allocator: s.mockAlloc,
		meta:      meta,
	}
	task.SetTask(&datapb.CompactionTask{})
	_, err = task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)

	task = &clusteringCompactionTask{
		allocator: s.mockAlloc,
		meta:      meta,
	}
	task.SetTask(&datapb.CompactionTask{})
	_, err = task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)
}

func (s *L0CompactionTaskSuite) generateTestL0Task(state datapb.CompactionTaskState) *l0CompactionTask {
	return newL0CompactionTask(&datapb.CompactionTask{
		PlanID:        1,
		TriggerID:     19530,
		CollectionID:  1,
		PartitionID:   10,
		Type:          datapb.CompactionType_Level0DeleteCompaction,
		NodeID:        NullNodeID,
		State:         state,
		Channel:       "ch-1",
		InputSegments: []int64{100, 101},
	}, s.mockAlloc, s.mockMeta, s.mockSessMgr)
}

func (s *L0CompactionTaskSuite) TestPorcessStateTrans() {
	s.Run("test pipelining needReassignNodeID", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t.updateAndSaveTaskMeta(setNodeID(NullNodeID))
		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().State)
		s.EqualValues(NullNodeID, t.GetTaskProto().NodeID)
	})

	s.Run("test pipelining Compaction failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.updateAndSaveTaskMeta(setNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)

		s.mockSessMgr.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
			s.Require().EqualValues(t.GetTaskProto().NodeID, nodeID)
			return errors.New("mock error")
		})

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().State)
		s.EqualValues(NullNodeID, t.GetTaskProto().NodeID)
	})

	s.Run("test pipelining success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		s.mockAlloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.updateAndSaveTaskMeta(setNodeID(100))
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()

		s.mockSessMgr.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
			s.Require().EqualValues(t.GetTaskProto().NodeID, nodeID)
			return nil
		})

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail NodeNotFound", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(t.GetTaskProto().NodeID)).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetTaskProto().GetState())
		s.EqualValues(NullNodeID, t.GetTaskProto().GetNodeID())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail mock error", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).Return(nil, errors.New("mock error")).Times(12)
		for i := 0; i < 12; i++ {
			got := t.Process()
			s.False(got)
			s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
			s.EqualValues(100, t.GetTaskProto().GetNodeID())
		}
	})

	s.Run("test executing with result executing", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_executing,
			}, nil).Once()

		got := t.Process()
		s.False(got)
	})

	s.Run("test executing with result completed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetTaskProto().GetNodeID(), mock.Anything).Return(nil)

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Times(2)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).Return().Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})
	s.Run("test executing with result completed save segment meta failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})
	s.Run("test executing with result completed save compaction meta failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	s.Run("test executing with result failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
	})
	s.Run("test executing with result failed save compaction meta failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.GetTaskProto().NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetTaskProto().GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetTaskProto().GetState())
	})

	s.Run("test metaSaved success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}

		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetTaskProto().GetNodeID(), mock.Anything).Return(nil).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test metaSaved failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_meta_saved, t.GetTaskProto().GetState())
	})

	s.Run("test complete drop failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetTaskProto().GetNodeID(), mock.Anything).Return(errors.New("mock error")).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test complete success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetTaskProto().GetNodeID(), mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, mock.Anything, false).RunAndReturn(func(ctx context.Context, segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetTaskProto().GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetTaskProto().GetState())
	})

	s.Run("test process failed success", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
	})

	s.Run("test process failed failed", func() {
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.updateAndSaveTaskMeta(setNodeID(100))
		s.Require().True(t.GetTaskProto().GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetTaskProto().GetState())
	})

	s.Run("test unknown task", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_unknown)

		got := t.Process()
		s.True(got)
	})
}

func (s *L0CompactionTaskSuite) TestSetterGetter() {
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
	t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)

	span := t.GetSpan()
	s.Nil(span)
	s.NotPanics(t.EndSpan)

	t.SetSpan(trace.SpanFromContext(context.TODO()))
	s.NotPanics(t.EndSpan)

	rst := t.GetResult()
	s.Nil(rst)
	t.SetResult(&datapb.CompactionPlanResult{PlanID: 19530})
	s.NotNil(t.GetResult())

	label := t.GetLabel()
	s.Equal("10-ch-1", label)

	t.updateAndSaveTaskMeta(setStartTime(100))
	s.EqualValues(100, t.GetTaskProto().GetStartTime())

	t.SetTask(nil)
	t.SetPlan(&datapb.CompactionPlan{PlanID: 19530})
	s.NotNil(t.GetPlan())

	s.Run("set NodeID", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
		t.SetNodeID(1000)
		s.EqualValues(1000, t.GetTaskProto().GetNodeID())
	})
}

// TestSelectSealedSegment_ForceSelectAllFlag exercises the flag end-to-end:
// build an L0 view, call Trigger() with the flag off / on, feed the resulting
// earliestGrowingSegmentPos into an l0CompactionTask (the same wiring
// compaction_trigger_v2.go uses), and verify selectSealedSegment's output
// actually changes based on the flag. A high-StartPosition segment (the
// shape silently dropped by the import-position bug) is excluded with the
// flag off and included with the flag on.
func (s *L0CompactionTaskSuite) TestSelectSealedSegment_ForceSelectAllFlag() {
	paramtable.Init()
	const flagKey = "dataCoord.compaction.levelzero.forceSelectAllSegments"

	channel := "ch-1"
	collectionID := int64(1)
	partitionID := int64(10)
	label := &CompactionGroupLabel{
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Channel:      channel,
	}

	// Flushed L1 segments visible to selectSealedSegment. The one with
	// StartPosition > realL0DmlTs is the case we care about — it would
	// normally be filtered out by `startPos < taskPos`.
	const realL0DmlTs = uint64(5000)
	lowPosSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            200,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 3000},
	}}
	highPosSeg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:            201,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channel,
		Level:         datapb.SegmentLevel_L1,
		State:         commonpb.SegmentState_Flushed,
		StartPosition: &msgpb.MsgPosition{ChannelName: channel, Timestamp: 20000},
	}}

	// Mockery's SelectSegments returns whatever we tell it without applying
	// filters. We need the real filter logic to run so the `startPos <
	// taskPos` predicate is actually exercised — install RunAndReturn that
	// evaluates each SegmentFilter.Match against our fixed segment set.
	installFilteringMock := func() {
		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, filters ...SegmentFilter) []*SegmentInfo {
				all := []*SegmentInfo{lowPosSeg, highPosSeg}
				result := make([]*SegmentInfo, 0, len(all))
				for _, seg := range all {
					matched := true
					for _, f := range filters {
						if !f.Match(seg) {
							matched = false
							break
						}
					}
					if matched {
						result = append(result, seg)
					}
				}
				return result
			})
	}

	// Build an L0 view with a couple of L0 segments whose dmlPos is
	// `realL0DmlTs`. Trigger() runs resolveLatestDeletePos under the current
	// flag value, so the returned view's earliestGrowingSegmentPos reflects
	// the flag.
	buildView := func() *LevelZeroSegmentsView {
		l0Segs := []*SegmentView{
			{
				ID:            100,
				label:         label,
				dmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
			{
				ID:            101,
				label:         label,
				dmlPos:        &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
				Level:         datapb.SegmentLevel_L0,
				State:         commonpb.SegmentState_Flushed,
				DeltalogCount: 100,
				DeltaSize:     1,
				DeltaRowCount: 1,
			},
		}
		return &LevelZeroSegmentsView{
			label:                     label,
			segments:                  l0Segs,
			earliestGrowingSegmentPos: &msgpb.MsgPosition{ChannelName: channel, Timestamp: realL0DmlTs},
		}
	}

	// Mirrors compaction_trigger_v2.go — feed the triggered view's
	// earliestGrowingSegmentPos into task.Pos and run selectSealedSegment.
	runSelectWithTriggeredPos := func() []int64 {
		installFilteringMock()
		srcView := buildView()
		triggered, reason := srcView.Trigger()
		s.Require().NotNil(triggered, "Trigger returned nil: %s", reason)
		triggeredView := triggered.(*LevelZeroSegmentsView)

		task := newL0CompactionTask(&datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			Channel:       channel,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
			InputSegments: []int64{100, 101},
			Pos:           triggeredView.earliestGrowingSegmentPos,
		}, s.mockAlloc, s.mockMeta, s.mockSessMgr)

		sealed, _ := task.selectSealedSegment()
		return lo.Map(sealed, func(seg *SegmentInfo, _ int) int64 { return seg.GetID() })
	}

	s.Run("flag_off_excludes_high_start_position_segment", func() {
		paramtable.Get().Save(flagKey, "false")
		defer paramtable.Get().Reset(flagKey)

		gotIDs := runSelectWithTriggeredPos()
		s.ElementsMatch([]int64{200}, gotIDs,
			"with flag off, segment 201 (StartPosition=20000 > taskPos=%d) must be excluded", realL0DmlTs)
	})

	s.Run("flag_on_includes_high_start_position_segment", func() {
		paramtable.Get().Save(flagKey, "true")
		defer paramtable.Get().Reset(flagKey)

		gotIDs := runSelectWithTriggeredPos()
		s.ElementsMatch([]int64{200, 201}, gotIDs,
			"with flag on, resolveLatestDeletePos must lift taskPos so segment 201 is included")
	})
}
