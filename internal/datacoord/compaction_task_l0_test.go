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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
