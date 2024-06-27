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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestCompactionPlanHandlerSuite(t *testing.T) {
	suite.Run(t, new(CompactionPlanHandlerSuite))
}

type CompactionPlanHandlerSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockAlloc   *NMockAllocator
	mockSch     *MockScheduler
	mockCm      *MockChannelManager
	mockSessMgr *MockSessionManager
}

func (s *CompactionPlanHandlerSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockSch = NewMockScheduler(s.T())
	s.mockCm = NewMockChannelManager(s.T())
	s.mockSessMgr = NewMockSessionManager(s.T())
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannel() {
	s.mockSch.EXPECT().Finish(mock.Anything, mock.Anything).Return().Once()
	handler := newCompactionPlanHandler(nil, nil, nil, nil, nil)
	handler.scheduler = s.mockSch

	var ch string = "ch1"
	handler.mu.Lock()
	handler.plans[1] = &compactionTask{
		plan:        &datapb.CompactionPlan{PlanID: 19530},
		dataNodeID:  1,
		triggerInfo: &compactionSignal{channel: ch},
	}
	handler.mu.Unlock()

	handler.removeTasksByChannel(ch)

	handler.mu.Lock()
	s.Equal(0, len(handler.plans))
	handler.mu.Unlock()
}

func (s *CompactionPlanHandlerSuite) TestCheckResult() {
	s.mockSessMgr.EXPECT().GetCompactionPlansResults().Return(map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult]{
		1: {A: 100, B: &datapb.CompactionPlanResult{PlanID: 1, State: commonpb.CompactionState_Executing}},
		2: {A: 100, B: &datapb.CompactionPlanResult{PlanID: 2, State: commonpb.CompactionState_Completed, Segments: []*datapb.CompactionSegment{{PlanID: 2}}}},
		3: {A: 100, B: &datapb.CompactionPlanResult{PlanID: 3, State: commonpb.CompactionState_Executing}},
		4: {A: 100, B: &datapb.CompactionPlanResult{PlanID: 4, State: commonpb.CompactionState_Executing}},
	}, nil)

	{
		s.mockAlloc.EXPECT().allocTimestamp(mock.Anything).Return(0, errors.New("mock")).Once()
		handler := newCompactionPlanHandler(nil, s.mockSessMgr, nil, nil, s.mockAlloc)
		handler.checkResult()
	}

	{
		s.mockAlloc.EXPECT().allocTimestamp(mock.Anything).Return(19530, nil).Once()
		handler := newCompactionPlanHandler(nil, s.mockSessMgr, nil, nil, s.mockAlloc)
		handler.checkResult()
	}
}

func (s *CompactionPlanHandlerSuite) TestClean() {
	startTime := tsoutil.ComposeTSByTime(time.Now(), 0)
	cleanTime := tsoutil.ComposeTSByTime(time.Now().Add(-2*time.Hour), 0)
	c := &compactionPlanHandler{
		allocator: s.mockAlloc,
		plans: map[int64]*compactionTask{
			1: {
				state: executing,
			},
			2: {
				state: pipelining,
			},
			3: {
				state: completed,
				plan: &datapb.CompactionPlan{
					StartTime: startTime,
				},
			},
			4: {
				state: completed,
				plan: &datapb.CompactionPlan{
					StartTime: cleanTime,
				},
			},
		},
	}

	c.Clean()
	s.Len(c.plans, 3)
}

func (s *CompactionPlanHandlerSuite) TestHandleL0CompactionResults() {
	channel := "Ch-1"
	s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(operators ...UpdateOperator) {
			s.Equal(7, len(operators))
		}).Return(nil).Once()

	deltalogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
	// 2 l0 segments, 3 sealed segments
	plan := &datapb.CompactionPlan{
		PlanID: 1,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:     100,
				Deltalogs:     deltalogs,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
			},
			{
				SegmentID:     101,
				Deltalogs:     deltalogs,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
			},
			{
				SegmentID:     200,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			},
			{
				SegmentID:     201,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			},
			{
				SegmentID:     202,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
			},
		},
		Type: datapb.CompactionType_Level0DeleteCompaction,
	}

	result := &datapb.CompactionPlanResult{
		PlanID:  plan.GetPlanID(),
		State:   commonpb.CompactionState_Completed,
		Channel: channel,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID: 200,
				Deltalogs: deltalogs,
				Channel:   channel,
			},
			{
				SegmentID: 201,
				Deltalogs: deltalogs,
				Channel:   channel,
			},
			{
				SegmentID: 202,
				Deltalogs: deltalogs,
				Channel:   channel,
			},
		},
	}

	handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
	err := handler.handleL0CompactionResult(plan, result)
	s.NoError(err)
}

func (s *CompactionPlanHandlerSuite) TestRefreshL0Plan() {
	channel := "Ch-1"
	deltalogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}
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

	s.Run("normal_refresh", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltalogs,
			}}
		}).Times(2)
		// 2 l0 segments
		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:     100,
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: channel,
				},
				{
					SegmentID:     101,
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: channel,
				},
			},
			Type: datapb.CompactionType_Level0DeleteCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 19530, collectionID: 1, partitionID: 10},
			state:       executing,
			plan:        plan,
			dataNodeID:  1,
		}

		handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
		err := handler.RefreshPlan(task)
		s.Require().NoError(err)

		s.Equal(5, len(task.plan.GetSegmentBinlogs()))
		segIDs := lo.Map(task.plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
			return b.GetSegmentID()
		})

		s.ElementsMatch([]int64{200, 201, 202, 100, 101}, segIDs)
	})

	s.Run("segment_not_found", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return nil
		}).Once()
		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:     102,
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: channel,
				},
			},
			Type: datapb.CompactionType_Level0DeleteCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 19531, collectionID: 1, partitionID: 10},
			state:       executing,
			plan:        plan,
			dataNodeID:  1,
		}

		handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
		err := handler.RefreshPlan(task)
		s.Error(err)
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})

	s.Run("select zero segments", func() {
		s.SetupTest()
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L0,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Deltalogs:     deltalogs,
			}}
		}).Times(2)
		s.mockMeta.EXPECT().SelectSegments(mock.Anything, mock.Anything).Return(nil).Once()

		// 2 l0 segments
		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:     100,
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: channel,
				},
				{
					SegmentID:     101,
					Level:         datapb.SegmentLevel_L0,
					InsertChannel: channel,
				},
			},
			Type: datapb.CompactionType_Level0DeleteCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 19530, collectionID: 1, partitionID: 10},
			state:       executing,
			plan:        plan,
			dataNodeID:  1,
		}

		handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
		err := handler.RefreshPlan(task)
		s.Error(err)
	})
}

func (s *CompactionPlanHandlerSuite) TestRefreshPlanMixCompaction() {
	channel := "Ch-1"
	binlogs := []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)}

	s.Run("normal_refresh", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            segID,
				Level:         datapb.SegmentLevel_L1,
				InsertChannel: channel,
				State:         commonpb.SegmentState_Flushed,
				Binlogs:       binlogs,
			}}
		}).Times(2)
		// 2 l0 segments
		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:     200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				},
				{
					SegmentID:     201,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				},
			},
			Type: datapb.CompactionType_MixCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 19530, collectionID: 1, partitionID: 10},
			state:       executing,
			plan:        plan,
			dataNodeID:  1,
		}

		handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
		err := handler.RefreshPlan(task)
		s.Require().NoError(err)

		s.Equal(2, len(task.plan.GetSegmentBinlogs()))
		segIDs := lo.Map(task.plan.GetSegmentBinlogs(), func(b *datapb.CompactionSegmentBinlogs, _ int) int64 {
			return b.GetSegmentID()
		})

		s.ElementsMatch([]int64{200, 201}, segIDs)
	})

	s.Run("segment_not_found", func() {
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(func(segID int64) *SegmentInfo {
			return nil
		}).Once()
		// 2 l0 segments
		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:     200,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				},
				{
					SegmentID:     201,
					Level:         datapb.SegmentLevel_L1,
					InsertChannel: channel,
				},
			},
			Type: datapb.CompactionType_MixCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 19530, collectionID: 1, partitionID: 10},
			state:       executing,
			plan:        plan,
			dataNodeID:  1,
		}

		handler := newCompactionPlanHandler(nil, nil, nil, s.mockMeta, s.mockAlloc)
		err := handler.RefreshPlan(task)
		s.Error(err)
		s.ErrorIs(err, merr.ErrSegmentNotFound)
	})
}

func (s *CompactionPlanHandlerSuite) TestExecCompactionPlan() {
	s.mockSch.EXPECT().Submit(mock.Anything).Return().Once()

	handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
	handler.scheduler = s.mockSch

	sig := &compactionSignal{id: int64(1)}
	plan := &datapb.CompactionPlan{
		PlanID: int64(1),
	}
	plan.Channel = "ch-1"

	handler.execCompactionPlan(sig, plan)
	handler.mu.RLock()
	defer handler.mu.RUnlock()
	_, ok := handler.plans[int64(1)]
	s.True(ok)
}

func (s *CompactionPlanHandlerSuite) TestHandleMergeCompactionResult() {
	plan := &datapb.CompactionPlan{
		PlanID: 1,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{SegmentID: 1},
			{SegmentID: 2},
		},
		Type: datapb.CompactionType_MixCompaction,
	}

	s.Run("illegal nil result", func() {
		s.SetupTest()
		handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
		err := handler.handleMergeCompactionResult(nil, nil)
		s.Error(err)
	})

	s.Run("not empty compacted to segment info", func() {
		s.SetupTest()
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).RunAndReturn(
			func(segID int64) *SegmentInfo {
				if segID == 3 {
					return NewSegmentInfo(&datapb.SegmentInfo{ID: 3})
				}
				return nil
			}).Once()

		handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
		handler.plans[plan.PlanID] = &compactionTask{dataNodeID: 111, plan: plan}

		compactionResult := &datapb.CompactionPlanResult{
			PlanID: plan.PlanID,
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 3, NumOfRows: 15},
			},
		}

		err := handler.handleMergeCompactionResult(plan, compactionResult)
		s.NoError(err)
	})

	s.Run("complete compaction mutation error", func() {
		s.SetupTest()
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything).Return(
			nil, nil, errors.New("mock error")).Once()

		handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
		handler.plans[plan.PlanID] = &compactionTask{dataNodeID: 111, plan: plan}
		compactionResult := &datapb.CompactionPlanResult{
			PlanID: plan.PlanID,
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 4, NumOfRows: 15},
			},
		}

		err := handler.handleMergeCompactionResult(plan, compactionResult)
		s.Error(err)
	})

	s.Run("sync segment error", func() {
		s.SetupTest()
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{segment},
			&segMetricMutation{}, nil).Once()

		handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
		handler.plans[plan.PlanID] = &compactionTask{dataNodeID: 111, plan: plan}
		compactionResult := &datapb.CompactionPlanResult{
			PlanID: plan.PlanID,
			Segments: []*datapb.CompactionSegment{
				{SegmentID: 4, NumOfRows: 15},
			},
		}

		err := handler.handleMergeCompactionResult(plan, compactionResult)
		s.NoError(err)
	})
}

func (s *CompactionPlanHandlerSuite) TestCompleteCompaction() {
	s.Run("test not exists compaction task", func() {
		handler := newCompactionPlanHandler(nil, nil, nil, nil, nil)
		err := handler.completeCompaction(&datapb.CompactionPlanResult{PlanID: 2})
		s.Error(err)
	})

	s.Run("test completed compaction task", func() {
		c := &compactionPlanHandler{
			plans: map[int64]*compactionTask{1: {state: completed}},
		}
		err := c.completeCompaction(&datapb.CompactionPlanResult{PlanID: 1})
		s.Error(err)
	})

	s.Run("test complete merge compaction task", func() {
		// mock for handleMergeCompactionResult
		s.mockMeta.EXPECT().GetHealthySegment(mock.Anything).Return(nil).Once()
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
		s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything).Return(
			[]*SegmentInfo{segment},
			&segMetricMutation{}, nil).Once()
		s.mockSch.EXPECT().Finish(mock.Anything, mock.Anything).Return()

		dataNodeID := UniqueID(111)

		seg1 := &datapb.SegmentInfo{
			ID:        1,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(101, 1)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 2)},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 3)},
		}

		seg2 := &datapb.SegmentInfo{
			ID:        2,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogIDs(101, 4)},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 5)},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 6)},
		}

		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           seg1.ID,
					FieldBinlogs:        seg1.GetBinlogs(),
					Field2StatslogPaths: seg1.GetStatslogs(),
					Deltalogs:           seg1.GetDeltalogs(),
				},
				{
					SegmentID:           seg2.ID,
					FieldBinlogs:        seg2.GetBinlogs(),
					Field2StatslogPaths: seg2.GetStatslogs(),
					Deltalogs:           seg2.GetDeltalogs(),
				},
			},
			Type: datapb.CompactionType_MixCompaction,
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 1},
			state:       executing,
			plan:        plan,
			dataNodeID:  dataNodeID,
		}

		plans := map[int64]*compactionTask{1: task}

		compactionResult := datapb.CompactionPlanResult{
			PlanID: 1,
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:           3,
					NumOfRows:           15,
					InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(101, 301)},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 302)},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogIDs(101, 303)},
				},
			},
		}

		c := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
		c.scheduler = s.mockSch
		c.plans = plans

		err := c.completeCompaction(&compactionResult)
		s.NoError(err)
		s.Nil(compactionResult.GetSegments()[0].GetInsertLogs())
		s.Nil(compactionResult.GetSegments()[0].GetField2StatslogPaths())
		s.Nil(compactionResult.GetSegments()[0].GetDeltalogs())
	})
}

func (s *CompactionPlanHandlerSuite) TestGetCompactionTask() {
	inPlans := map[int64]*compactionTask{
		1: {
			triggerInfo: &compactionSignal{id: 1},
			plan:        &datapb.CompactionPlan{PlanID: 1},
			state:       executing,
		},
		2: {
			triggerInfo: &compactionSignal{id: 1},
			plan:        &datapb.CompactionPlan{PlanID: 2},
			state:       completed,
		},
		3: {
			triggerInfo: &compactionSignal{id: 1},
			plan:        &datapb.CompactionPlan{PlanID: 3},
			state:       failed,
		},
	}

	expected := lo.Values(inPlans)

	handler := &compactionPlanHandler{plans: inPlans}
	got := handler.getCompactionTasksBySignalID(1)
	s.ElementsMatch(expected, got)

	task := handler.getCompaction(1)
	s.NotNil(task)
	s.EqualValues(1, task.plan.PlanID)

	task = handler.getCompaction(19530)
	s.Nil(task)
}

func (s *CompactionPlanHandlerSuite) TestUpdateCompaction() {
	s.mockSessMgr.EXPECT().GetCompactionPlansResults().Return(map[int64]*typeutil.Pair[int64, *datapb.CompactionPlanResult]{
		1: {A: 111, B: &datapb.CompactionPlanResult{PlanID: 1, State: commonpb.CompactionState_Executing}},
		2: {A: 111, B: &datapb.CompactionPlanResult{PlanID: 2, State: commonpb.CompactionState_Completed, Segments: []*datapb.CompactionSegment{{PlanID: 2}}}},
		3: {A: 111, B: &datapb.CompactionPlanResult{PlanID: 3, State: commonpb.CompactionState_Executing}},
		5: {A: 222, B: &datapb.CompactionPlanResult{PlanID: 5, State: commonpb.CompactionState_Completed, Segments: []*datapb.CompactionSegment{{PlanID: 5}}}},
		6: {A: 111, B: &datapb.CompactionPlanResult{Channel: "ch-2", PlanID: 5, State: commonpb.CompactionState_Completed, Segments: []*datapb.CompactionSegment{{PlanID: 6}}}},
	}, nil)

	inPlans := map[int64]*compactionTask{
		1: {
			triggerInfo: &compactionSignal{},
			plan:        &datapb.CompactionPlan{PlanID: 1, Channel: "ch-1"},
			state:       executing,
			dataNodeID:  111,
		},
		2: {
			triggerInfo: &compactionSignal{},
			plan:        &datapb.CompactionPlan{PlanID: 2, Channel: "ch-1"},
			state:       executing,
			dataNodeID:  111,
		},
		3: {
			triggerInfo: &compactionSignal{},
			plan:        &datapb.CompactionPlan{PlanID: 3, Channel: "ch-1"},
			state:       timeout,
			dataNodeID:  111,
		},
		4: {
			triggerInfo: &compactionSignal{},
			plan:        &datapb.CompactionPlan{PlanID: 4, Channel: "ch-1"},
			state:       timeout,
			dataNodeID:  111,
		},
		6: {
			triggerInfo: &compactionSignal{},
			plan:        &datapb.CompactionPlan{PlanID: 6, Channel: "ch-2"},
			state:       executing,
			dataNodeID:  111,
		},
	}

	s.mockCm.EXPECT().Match(int64(111), "ch-1").Return(true)
	s.mockCm.EXPECT().Match(int64(111), "ch-2").Return(false).Once()

	handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc)
	handler.plans = inPlans

	_, ok := handler.plans[5]
	s.Require().False(ok)

	err := handler.updateCompaction(0)
	s.NoError(err)

	task := handler.plans[1]
	s.Equal(executing, task.state)

	task = handler.plans[2]
	s.Equal(executing, task.state)

	task = handler.plans[3]
	s.Equal(timeout, task.state)

	task = handler.plans[4]
	s.Equal(failed, task.state)

	task = handler.plans[6]
	s.Equal(failed, task.state)
}

func getFieldBinlogIDs(fieldID int64, logIDs ...int64) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(logIDs)),
	}
	for _, id := range logIDs {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogID: id})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getFieldBinlogPaths(fieldID int64, paths ...string) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(paths)),
	}
	for _, path := range paths {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogPath: path})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getFieldBinlogIDsWithEntry(fieldID int64, entry int64, logIDs ...int64) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: fieldID,
		Binlogs: make([]*datapb.Binlog, 0, len(logIDs)),
	}
	for _, id := range logIDs {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogID: id, EntriesNum: entry})
	}
	err := binlog.CompressFieldBinlogs([]*datapb.FieldBinlog{l})
	if err != nil {
		panic(err)
	}
	return l
}

func getInsertLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildInsertLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getStatsLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildStatsLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getDeltaLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildDeltaLogPath(rootPath, 10, 100, segmentID, 10000)
}
