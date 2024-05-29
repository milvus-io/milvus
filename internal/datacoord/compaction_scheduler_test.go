package datacoord

import (
	"fmt"
	"testing"

	"github.com/samber/lo"
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
	cluster := NewMockCluster(s.T())
	s.scheduler = NewCompactionScheduler(cluster)
	s.scheduler.parallelTasks = map[int64][]CompactionTask{
		100: {
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 1, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction}},
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 2, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction}},
		},
		101: {
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 3, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction}},
		},
		102: {
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 4, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction}},
		},
	}
	s.scheduler.taskNumber.Add(4)
}

func (s *SchedulerSuite) TestScheduleEmpty() {
	cluster := NewMockCluster(s.T())
	emptySch := NewCompactionScheduler(cluster)

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
		tasks       []CompactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks", []CompactionTask{
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction}},
		}, []UniqueID{}},
		{"without L0 tasks", []CompactionTask{
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_MinorCompaction}},
			&mixCompactionTask{dataNodeID: 100, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{}},
		{"empty tasks", []CompactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				cluster := NewMockCluster(s.T())
				cluster.EXPECT().QuerySlots().Return(map[int64]int64{100: 0})
				s.scheduler.cluster = cluster
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetPlanID()
			}))
		})
	}
}

func (s *SchedulerSuite) TestScheduleNodeWith1ParallelTask() {
	// dataNode 101's paralleTasks has 1 task running, not L0 task
	tests := []struct {
		description string
		tasks       []CompactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []CompactionTask{
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{10}},
		{"with L0 tasks same channel", []CompactionTask{
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-2", Type: datapb.CompactionType_Level0DeleteCompaction}},
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction}},
		}, []UniqueID{11}},
		{"without L0 tasks", []CompactionTask{
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 14, Channel: "ch-2", Type: datapb.CompactionType_MinorCompaction}},
			&mixCompactionTask{dataNodeID: 101, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{14}},
		{"empty tasks", []CompactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				cluster := NewMockCluster(s.T())
				cluster.EXPECT().QuerySlots().Return(map[int64]int64{101: 2})
				s.scheduler.cluster = cluster
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetPlanID()
			}))

			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())
		})
	}
}

func (s *SchedulerSuite) TestScheduleNodeWithL0Executing() {
	// dataNode 102's paralleTasks has running L0 tasks
	// nothing of the same channel will be able to schedule
	tests := []struct {
		description string
		tasks       []CompactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []CompactionTask{
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction}},
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{10}},
		{"with L0 tasks same channel", []CompactionTask{
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 10, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction}},
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction}},
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction}},
		}, []UniqueID{11}},
		{"without L0 tasks", []CompactionTask{
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 14, Channel: "ch-3", Type: datapb.CompactionType_MinorCompaction}},
			&mixCompactionTask{dataNodeID: 102, plan: &datapb.CompactionPlan{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MinorCompaction}},
		}, []UniqueID{13}},
		{"empty tasks", []CompactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(4, s.scheduler.GetTaskCount())

			if len(test.tasks) > 0 {
				cluster := NewMockCluster(s.T())
				cluster.EXPECT().QuerySlots().Return(map[int64]int64{102: 2})
				s.scheduler.cluster = cluster
			}

			// submit the testing tasks
			s.scheduler.Submit(test.tasks...)
			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())

			gotTasks := s.scheduler.Schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetPlanID()
			}))

			s.Equal(4+len(test.tasks), s.scheduler.GetTaskCount())
		})
	}
}

func (s *SchedulerSuite) TestFinish() {
	s.Run("finish from parallelTasks", func() {
		s.SetupTest()
		metrics.DataCoordCompactionTaskNum.Reset()

		s.scheduler.Finish(100, &mixCompactionTask{plan: &datapb.CompactionPlan{PlanID: 1, Type: datapb.CompactionType_MixCompaction}})
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
		task := &mixCompactionTask{plan: plan, dataNodeID: datanodeID}
		s.scheduler.Submit(task)

		taskNum, err := metrics.DataCoordCompactionTaskNum.GetMetricWithLabelValues(fmt.Sprint(datanodeID), datapb.CompactionType_Level0DeleteCompaction.String(), metrics.Pending)
		s.NoError(err)
		s.MetricsEqual(taskNum, 1)

		s.scheduler.Finish(datanodeID, task)
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
