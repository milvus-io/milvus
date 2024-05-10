package datacoord

import (
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/testutils"
)

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

type SchedulerSuite struct {
	testutils.PromMetricsSuite
	scheduler *CompactionScheduler
}

func (s *SchedulerSuite) SetupTest() {
	sessionMgr := NewMockSessionManager(s.T())
	s.scheduler = NewCompactionScheduler(sessionMgr)
	s.scheduler.parallelTasks = map[int64][]*compactionTask{
		100: {
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 1, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction}},
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 2, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction}},
		},
		101: {
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 3, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction}},
		},
		102: {
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 4, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction}},
		},
	}
	s.scheduler.taskNumber.Add(4)
}

func (s *SchedulerSuite) TestScheduleEmpty() {
	sessionMgr := NewMockSessionManager(s.T())
	emptySch := NewCompactionScheduler(sessionMgr)

	tasks := emptySch.Schedule()
	s.Empty(tasks)

	s.Equal(0, emptySch.GetTaskCount())
	s.Empty(emptySch.queuingTasks)
	s.Empty(emptySch.parallelTasks)
}

func (s *SchedulerSuite) TestScheduleParallelTaskFull() {
	// dataNode 100's paralleTasks is full
	tests := []struct {
		description string
		tasks       []*compactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks", []*compactionTask{
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{}},
		{"without L0 tasks", []*compactionTask{
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_MinorCompaction}},
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{}},
		{"empty tasks", []*compactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				sessionMgr := NewMockSessionManager(s.T())
				sessionMgr.EXPECT().GetSessions().Return([]*Session{{info: &NodeInfo{NodeID: 100}}})
				sessionMgr.EXPECT().QuerySlot(mock.Anything).Return(&datapb.QuerySlotResponse{NumSlots: 0}, nil)
				s.scheduler.sessions = sessionMgr
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			}))
		})
	}
}

func (s *SchedulerSuite) TestScheduleNodeWith1ParallelTask() {
	// dataNode 101's paralleTasks has 1 task running, not L0 task
	tests := []struct {
		description string
		tasks       []*compactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []*compactionTask{
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{10}},
		{"with L0 tasks same channel", []*compactionTask{
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-2", Type: datapb.CompactionType_Level0DeleteCompaction}},
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{11}},
		{"without L0 tasks", []*compactionTask{
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 14, Channel: "ch-2", Type: datapb.CompactionType_MinorCompaction}},
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{14}},
		{"empty tasks", []*compactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				sessionMgr := NewMockSessionManager(s.T())
				sessionMgr.EXPECT().GetSessions().Return([]*Session{{info: &NodeInfo{NodeID: 101}}})
				sessionMgr.EXPECT().QuerySlot(mock.Anything).Return(&datapb.QuerySlotResponse{NumSlots: 2}, nil)
				s.scheduler.sessions = sessionMgr
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			}))

			// the second schedule returns empty for no slot
			if len(test.tasks) > 0 {
				sessionMgr := NewMockSessionManager(s.T())
				sessionMgr.EXPECT().GetSessions().Return([]*Session{{info: &NodeInfo{NodeID: 101}}})
				sessionMgr.EXPECT().QuerySlot(mock.Anything).Return(&datapb.QuerySlotResponse{NumSlots: 0}, nil)
				s.scheduler.sessions = sessionMgr
			}
			gotTasks = s.scheduler.Schedule()
			s.Empty(gotTasks)

			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())
		})
	}
}

func (s *SchedulerSuite) TestScheduleNodeWithL0Executing() {
	// dataNode 102's paralleTasks has running L0 tasks
	// nothing of the same channel will be able to schedule
	tests := []struct {
		description string
		tasks       []*compactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []*compactionTask{
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{10}},
		{"with L0 tasks same channel", []*compactionTask{
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction}},
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-3", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{11}},
		{"without L0 tasks", []*compactionTask{
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 14, Channel: "ch-3", Type: datapb.CompactionType_MinorCompaction}},
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{13}},
		{"empty tasks", []*compactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				sessionMgr := NewMockSessionManager(s.T())
				sessionMgr.EXPECT().GetSessions().Return([]*Session{{info: &NodeInfo{NodeID: 102}}})
				sessionMgr.EXPECT().QuerySlot(mock.Anything).Return(&datapb.QuerySlotResponse{NumSlots: 2}, nil)
				s.scheduler.sessions = sessionMgr
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			}))

			// the second schedule returns empty for no slot
			if len(test.tasks) > 0 {
				sessionMgr := NewMockSessionManager(s.T())
				sessionMgr.EXPECT().GetSessions().Return([]*Session{{info: &NodeInfo{NodeID: 101}}})
				sessionMgr.EXPECT().QuerySlot(mock.Anything).Return(&datapb.QuerySlotResponse{NumSlots: 0}, nil)
				s.scheduler.sessions = sessionMgr
			}
			if len(gotTasks) > 0 {
				gotTasks = s.scheduler.Schedule()
				s.Empty(gotTasks)
			}

			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())
		})
	}
}

func (s *SchedulerSuite) TestFinish() {
	s.Run("finish from parallelTasks", func() {
		s.SetupTest()
		metrics.DataCoordCompactionTaskNum.Reset()

		s.scheduler.Finish(100, &datapb.CompactionPlan{PlanID: 1, Type: datapb.CompactionType_MixCompaction})
		taskNum, err := metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues("100", datapb.CompactionType_MixCompaction.String(), metrics.Executing)
		s.NoError(err)
		s.MetricsEqual(taskNum, -1)

		taskNum, err = metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues("100", datapb.CompactionType_MixCompaction.String(), metrics.Done)
		s.NoError(err)
		s.MetricsEqual(taskNum, 1)
	})

	s.Run("finish from queuingTasks", func() {
		s.SetupTest()
		metrics.DataCoordCompactionTaskNum.Reset()
		var datanodeID int64 = 10000

		plan := &datapb.CompactionPlan{PlanID: 19530, Type: datapb.CompactionType_Level0DeleteCompaction}
		s.scheduler.Submit(&compactionTask{plan: plan, dataNodeID: datanodeID})

		taskNum, err := metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues(fmt.Sprint(datanodeID), datapb.CompactionType_Level0DeleteCompaction.String(), metrics.Pending)
		s.NoError(err)
		s.MetricsEqual(taskNum, 1)

		s.scheduler.Finish(datanodeID, plan)
		taskNum, err = metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues(fmt.Sprint(datanodeID), datapb.CompactionType_Level0DeleteCompaction.String(), metrics.Pending)
		s.NoError(err)
		s.MetricsEqual(taskNum, 0)

		taskNum, err = metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues(fmt.Sprint(datanodeID), datapb.CompactionType_Level0DeleteCompaction.String(), metrics.Done)
		s.NoError(err)
		s.MetricsEqual(taskNum, 1)
	})
}

func (s *SchedulerSuite) TestPickNode() {
	s.Run("test pickAnyNode", func() {
		nodeSlots := map[int64]int64{
			100: 2,
			101: 6,
		}
		node := s.scheduler.pickAnyNode(nodeSlots)
		s.Equal(int64(101), node)

		node = s.scheduler.pickAnyNode(map[int64]int64{})
		s.Equal(int64(NullNodeID), node)
	})
}
