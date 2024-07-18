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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestL0CompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(L0CompactionTaskSuite))
}

type L0CompactionTaskSuite struct {
	suite.Suite

	mockAlloc   *NMockAllocator
	mockMeta    *MockCompactionMeta
	mockSessMgr *MockSessionManager
}

func (s *L0CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockSessMgr = NewMockSessionManager(s.T())
	s.mockAlloc = NewNMockAllocator(s.T())
}

func (s *L0CompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_NormalL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(
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

	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	task := &l0CompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  1,
			PartitionID:   10,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
			InputSegments: []int64{100, 101},
		},
		meta: s.mockMeta,
	}
	alloc := NewNMockAllocator(s.T())
	alloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)
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
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
		return nil
	}).Once()
	task := &l0CompactionTask{
		CompactionTask: &datapb.CompactionTask{
			InputSegments: []int64{102},
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  1,
			PartitionID:   10,
			Channel:       channel,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
		},
		meta: s.mockMeta,
	}
	alloc := NewNMockAllocator(s.T())
	alloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc

	_, err := task.BuildCompactionRequest()
	s.Error(err)
	s.ErrorIs(err, merr.ErrSegmentNotFound)
}

func (s *L0CompactionTaskSuite) TestProcessRefreshPlan_SelectZeroSegmentsL0() {
	channel := "Ch-1"
	deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID:            segID,
			Level:         datapb.SegmentLevel_L0,
			InsertChannel: channel,
			State:         commonpb.SegmentState_Flushed,
			Deltalogs:     deltaLogs,
		}}
	}).Times(2)
	s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(nil).Once()

	task := &l0CompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  1,
			PartitionID:   10,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        1,
			State:         datapb.CompactionTaskState_executing,
			InputSegments: []int64{100, 101},
		},
		meta: s.mockMeta,
	}
	alloc := NewNMockAllocator(s.T())
	alloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)
	task.allocator = alloc
	_, err := task.BuildCompactionRequest()
	s.Error(err)
}

func (s *L0CompactionTaskSuite) TestBuildCompactionRequestFailed_AllocFailed() {
	var task CompactionTask

	alloc := NewNMockAllocator(s.T())
	alloc.EXPECT().allocN(mock.Anything).Return(100, 200, errors.New("mock alloc err"))

	task = &l0CompactionTask{
		allocator: alloc,
	}
	_, err := task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)

	task = &mixCompactionTask{
		allocator: alloc,
	}
	_, err = task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)

	task = &clusteringCompactionTask{
		allocator: alloc,
	}
	_, err = task.BuildCompactionRequest()
	s.T().Logf("err=%v", err)
	s.Error(err)
}

func (s *L0CompactionTaskSuite) generateTestL0Task(state datapb.CompactionTaskState) *l0CompactionTask {
	return &l0CompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:        1,
			TriggerID:     19530,
			CollectionID:  1,
			PartitionID:   10,
			Type:          datapb.CompactionType_Level0DeleteCompaction,
			NodeID:        NullNodeID,
			State:         state,
			InputSegments: []int64{100, 101},
		},
		meta:      s.mockMeta,
		sessions:  s.mockSessMgr,
		allocator: s.mockAlloc,
	}
}

func (s *L0CompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *L0CompactionTaskSuite) TestPorcessStateTrans() {
	s.mockAlloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)
	s.Run("test pipelining needReassignNodeID", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = NullNodeID
		t.allocator = alloc
		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.State)
		s.EqualValues(NullNodeID, t.NodeID)
	})

	s.Run("test pipelining BuildCompactionRequest failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}, isCompacting: true},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).Return()

		s.mockSessMgr.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.State)
	})

	s.Run("test pipelining Compaction failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)

		s.mockSessMgr.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
			s.Require().EqualValues(t.NodeID, nodeID)
			return errors.New("mock error")
		})

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.State)
		s.EqualValues(NullNodeID, t.NodeID)
	})

	s.Run("test pipelining success", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{
				{SegmentInfo: &datapb.SegmentInfo{
					ID:            200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				}},
			},
		)

		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltaLogs,
			}}
		}).Twice()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Once()

		s.mockSessMgr.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
			s.Require().EqualValues(t.NodeID, nodeID)
			return nil
		})

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetState())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail NodeNotFound", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(t.NodeID)).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.GetState())
		s.EqualValues(NullNodeID, t.GetNodeID())
	})

	// stay in executing state when GetCompactionPlanResults error except ErrNodeNotFound
	s.Run("test executing GetCompactionPlanResult fail mock error", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).Return(nil, errors.New("mock error")).Times(12)
		for i := 0; i < 12; i++ {
			got := t.Process()
			s.False(got)
			s.Equal(datapb.CompactionTaskState_executing, t.GetState())
			s.EqualValues(100, t.GetNodeID())
		}
	})

	s.Run("test executing with result executing", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_executing,
			}, nil).Twice()

		got := t.Process()
		s.False(got)

		// test timeout
		t.StartTime = time.Now().Add(-time.Hour).Unix()
		t.TimeoutInSeconds = 10

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).
			RunAndReturn(func(inputs []int64, compacting bool) {
				s.ElementsMatch(inputs, t.GetInputSegments())
				s.False(compacting)
			}).Once()

		got = t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_timeout, t.GetState())
	})

	s.Run("test executing with result executing timeout and updataAndSaveTaskMeta failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_executing,
			}, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(errors.New("mock error")).Once()

		t.StartTime = time.Now().Add(-time.Hour).Unix()
		t.TimeoutInSeconds = 10

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetState())
	})

	s.Run("test executing with result completed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetNodeID(), mock.Anything).Return(nil)

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Times(2)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).Return().Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetState())
	})
	s.Run("test executing with result completed save segment meta failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetState())
	})
	s.Run("test executing with result completed save compaction meta failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_completed,
			}, nil).Once()

		s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetState())
	})

	s.Run("test executing with result failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetNodeID(), mock.Anything).Return(nil)

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Times(1)
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).Return().Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetState())
	})
	s.Run("test executing with result failed save compaction meta failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_executing)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		s.mockSessMgr.EXPECT().GetCompactionPlanResult(t.NodeID, mock.Anything).
			Return(&datapb.CompactionPlanResult{
				PlanID: t.GetPlanID(),
				State:  datapb.CompactionTaskState_failed,
			}, nil).Once()
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.GetState())
	})
}
