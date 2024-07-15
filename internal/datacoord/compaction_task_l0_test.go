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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func (s *CompactionTaskSuite) TestProcessRefreshPlan_NormalL0() {
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

func (s *CompactionTaskSuite) TestProcessRefreshPlan_SegmentNotFoundL0() {
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

func (s *CompactionTaskSuite) TestProcessRefreshPlan_SelectZeroSegmentsL0() {
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

func (s *CompactionTaskSuite) TestBuildCompactionRequestFailed_AllocFailed() {
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

func generateTestL0Task(state datapb.CompactionTaskState) *l0CompactionTask {
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
	}
}

func (s *CompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *CompactionTaskSuite) TestProcessStateTrans() {
	alloc := NewNMockAllocator(s.T())
	alloc.EXPECT().allocN(mock.Anything).Return(100, 200, nil)

	s.Run("test pipelining needReassignNodeID", func() {
		t := generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = NullNodeID
		t.allocator = alloc
		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.State)
		s.EqualValues(NullNodeID, t.NodeID)
	})

	s.Run("test pipelining BuildCompactionRequest failed", func() {
		t := generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		t.meta = s.mockMeta
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

		t.sessions = s.mockSessMgr
		s.mockSessMgr.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.State)
	})

	s.Run("test pipelining Compaction failed", func() {
		t := generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		t.meta = s.mockMeta
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

		t.sessions = s.mockSessMgr
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
		t := generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
		t.allocator = alloc
		channel := "ch-1"
		deltaLogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

		t.meta = s.mockMeta
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

		t.sessions = s.mockSessMgr
		s.mockSessMgr.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, nodeID int64, plan *datapb.CompactionPlan) error {
			s.Require().EqualValues(t.NodeID, nodeID)
			return nil
		})

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_executing, t.State)
	})
}
