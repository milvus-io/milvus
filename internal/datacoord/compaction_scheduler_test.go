package datacoord

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SchedulerSuite))
}

type SchedulerSuite struct {
	suite.Suite
	scheduler *CompactionScheduler
}

func (s *SchedulerSuite) SetupTest() {
	s.scheduler = NewCompactionScheduler()
	s.scheduler.parallelTasks = map[int64][]*compactionTask{
		100: {
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 1, Channel: "ch-1", Type: datapb.CompactionType_MinorCompaction}},
			{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 2, Channel: "ch-1", Type: datapb.CompactionType_MinorCompaction}},
		},
		101: {
			{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 3, Channel: "ch-2", Type: datapb.CompactionType_MinorCompaction}},
		},
		102: {
			{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 4, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction}},
		},
	}
	s.scheduler.taskNumber.Add(4)
}

func (s *SchedulerSuite) TestScheduleEmpty() {
	emptySch := NewCompactionScheduler()

	tasks := emptySch.schedule()
	s.Empty(tasks)

	s.Equal(0, emptySch.getExecutingTaskNum())
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
			s.Require().Equal(4, s.scheduler.getExecutingTaskNum())

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.getExecutingTaskNum())

			gotTasks := s.scheduler.schedule()
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
			s.Require().Equal(4, s.scheduler.getExecutingTaskNum())

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.getExecutingTaskNum())

			gotTasks := s.scheduler.schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			}))

			// the second schedule returns empty for full paralleTasks
			gotTasks = s.scheduler.schedule()
			s.Empty(gotTasks)

			s.Equal(4+len(test.tasks), s.scheduler.getExecutingTaskNum())
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
			s.Require().Equal(4, s.scheduler.getExecutingTaskNum())

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.getExecutingTaskNum())

			gotTasks := s.scheduler.schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t *compactionTask, _ int) int64 {
				return t.plan.PlanID
			}))

			// the second schedule returns empty for full paralleTasks
			if len(gotTasks) > 0 {
				gotTasks = s.scheduler.schedule()
				s.Empty(gotTasks)
			}

			s.Equal(4+len(test.tasks), s.scheduler.getExecutingTaskNum())
		})
	}
}
