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
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestL0CompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(L0CompactionTaskSuite))
}

type L0CompactionTaskSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockSessMgr *MockSessionManager
}

func (s *L0CompactionTaskSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockSessMgr = NewMockSessionManager(s.T())
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
	_, err := task.BuildCompactionRequest()
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
			Channel:       "ch-1",
			InputSegments: []int64{100, 101},
		},
		meta:     s.mockMeta,
		sessions: s.mockSessMgr,
	}
}

func (s *L0CompactionTaskSuite) TestStateTrans() {
	s.Run("test pipelining needReassignNodeID", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = NullNodeID
		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_pipelining, t.State)
		s.EqualValues(NullNodeID, t.NodeID)
	})

	s.Run("test pipelining Compaction failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.NodeID = 100
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
			}, nil).Once()

		got := t.Process()
		s.False(got)
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
		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Once()

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

	s.Run("test metaSaved success", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).RunAndReturn(func(segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetInputSegments())
		}).Once()
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetNodeID(), mock.Anything).Return(nil).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetState())
	})

	s.Run("test metaSaved failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_meta_saved)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(errors.New("mock error")).Once()

		got := t.Process()
		s.False(got)
		s.Equal(datapb.CompactionTaskState_meta_saved, t.GetState())
	})

	s.Run("test complete drop failed", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetNodeID(), mock.Anything).Return(errors.New("mock error")).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).RunAndReturn(func(segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetState())
	})

	s.Run("test complete success", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_completed)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)
		t.result = &datapb.CompactionPlanResult{}
		s.mockSessMgr.EXPECT().DropCompactionPlan(t.GetNodeID(), mock.Anything).Return(nil).Once()
		s.mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, false).RunAndReturn(func(segIDs []int64, isCompacting bool) {
			s.ElementsMatch(segIDs, t.GetInputSegments())
		}).Once()

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_completed, t.GetState())
	})

	s.Run("test process failed success", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_failed)
		t.NodeID = 100
		s.Require().True(t.GetNodeID() > 0)

		got := t.Process()
		s.True(got)
		s.Equal(datapb.CompactionTaskState_failed, t.GetState())
	})

	s.Run("test unkonwn task", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_unknown)

		got := t.Process()
		s.True(got)
	})
}

func (s *L0CompactionTaskSuite) TestSetterGetter() {
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

	t.SetStartTime(100)
	s.EqualValues(100, t.GetStartTime())

	t.SetTask(nil)
	t.SetPlan(&datapb.CompactionPlan{PlanID: 19530})
	s.NotNil(t.GetPlan())

	s.Run("set NodeID", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)

		s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
		t.SetNodeID(1000)
		s.EqualValues(1000, t.GetNodeID())
	})
}

func (s *L0CompactionTaskSuite) TestCleanLogPath() {
	s.Run("plan nil", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.CleanLogPath()
	})

	s.Run("clear path", func() {
		t := s.generateTestL0Task(datapb.CompactionTaskState_pipelining)
		t.SetPlan(&datapb.CompactionPlan{
			Channel: "ch-1",
			Type:    datapb.CompactionType_MixCompaction,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           100,
					FieldBinlogs:        []*datapb.FieldBinlog{getFieldBinlogIDs(101, 4)},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 5)},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogIDs(101, 6)},
				},
			},
			PlanID: 19530,
		})

		t.SetResult(&datapb.CompactionPlanResult{
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID:           100,
					InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(101, 4)},
					Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(101, 5)},
					Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogIDs(101, 6)},
				},
			},
			PlanID: 19530,
		})

		t.CleanLogPath()

		s.Empty(t.GetPlan().GetSegmentBinlogs()[0].GetFieldBinlogs())
		s.Empty(t.GetPlan().GetSegmentBinlogs()[0].GetField2StatslogPaths())
		s.Empty(t.GetPlan().GetSegmentBinlogs()[0].GetDeltalogs())

		s.Empty(t.GetResult().GetSegments()[0].GetInsertLogs())
		s.Empty(t.GetResult().GetSegments()[0].GetField2StatslogPaths())
		s.Empty(t.GetResult().GetSegments()[0].GetDeltalogs())
	})
}
