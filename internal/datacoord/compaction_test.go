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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestCompactionPlanHandlerSuite(t *testing.T) {
	suite.Run(t, new(CompactionPlanHandlerSuite))
}

type CompactionPlanHandlerSuite struct {
	suite.Suite

	mockMeta    *MockCompactionMeta
	mockAlloc   *NMockAllocator
	mockCm      *MockChannelManager
	mockSessMgr *MockSessionManager
	handler     *compactionPlanHandler
	cluster     Cluster
}

func (s *CompactionPlanHandlerSuite) SetupTest() {
	s.mockMeta = NewMockCompactionMeta(s.T())
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockCm = NewMockChannelManager(s.T())
	s.mockSessMgr = NewMockSessionManager(s.T())
	s.cluster = NewMockCluster(s.T())
	s.handler = newCompactionPlanHandler(s.cluster, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc, nil, nil)
}

func (s *CompactionPlanHandlerSuite) TestScheduleEmpty() {
	s.SetupTest()
	s.handler.schedule()
	s.Empty(s.handler.executingTasks)
}

func (s *CompactionPlanHandlerSuite) generateInitTasksForSchedule() {
	ret := []CompactionTask{
		&mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  1,
				Type:    datapb.CompactionType_MixCompaction,
				State:   datapb.CompactionTaskState_pipelining,
				Channel: "ch-1",
				NodeID:  100,
			},
			plan:     &datapb.CompactionPlan{PlanID: 1, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		&mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  2,
				Type:    datapb.CompactionType_MixCompaction,
				State:   datapb.CompactionTaskState_pipelining,
				Channel: "ch-1",
				NodeID:  100,
			},
			plan:     &datapb.CompactionPlan{PlanID: 2, Channel: "ch-1", Type: datapb.CompactionType_MixCompaction},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		&mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  3,
				Type:    datapb.CompactionType_MixCompaction,
				State:   datapb.CompactionTaskState_pipelining,
				Channel: "ch-2",
				NodeID:  101,
			},
			plan:     &datapb.CompactionPlan{PlanID: 3, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		&mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  4,
				Type:    datapb.CompactionType_Level0DeleteCompaction,
				State:   datapb.CompactionTaskState_pipelining,
				Channel: "ch-3",
				NodeID:  102,
			},
			plan:     &datapb.CompactionPlan{PlanID: 4, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
	}
	for _, t := range ret {
		s.handler.restoreTask(t)
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWith1ParallelTask() {
	// dataNode 101's paralleTasks has 1 task running, not L0 task
	tests := []struct {
		description string
		tasks       []CompactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []CompactionTask{
			&l0CompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{10, 11}},
		{"with L0 tasks same channel", []CompactionTask{
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&l0CompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 10, Channel: "ch-11", Type: datapb.CompactionType_Level0DeleteCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{10}},
		{"without L0 tasks", []CompactionTask{
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-2",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 14, Channel: "ch-2", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  101,
				},
				plan:     &datapb.CompactionPlan{PlanID: 13, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{13, 14}},
		{"empty tasks", []CompactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.generateInitTasksForSchedule()
			s.Require().Equal(4, s.handler.getTaskCount())
			// submit the testing tasks
			for _, t := range test.tasks {
				s.handler.submitTask(t)
			}
			s.Equal(4+len(test.tasks), s.handler.getTaskCount())

			gotTasks := s.handler.schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetPlanID()
			}))

			s.Equal(4+len(test.tasks), s.handler.getTaskCount())
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestScheduleNodeWithL0Executing() {
	// dataNode 102's paralleTasks has running L0 tasks
	// nothing of the same channel will be able to schedule
	tests := []struct {
		description string
		tasks       []CompactionTask
		expectedOut []UniqueID // planID
	}{
		{"with L0 tasks diff channel", []CompactionTask{
			&l0CompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-10",
					NodeID:  102,
				},
				// plan:     &datapb.CompactionPlan{PlanID: 10, Channel: "ch-10", Type: datapb.CompactionType_Level0DeleteCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				},
				// plan:     &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{10, 11}},
		{"with L0 tasks same channel", []CompactionTask{
			&l0CompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  10,
					Type:    datapb.CompactionType_Level0DeleteCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				},
				plan:     &datapb.CompactionPlan{PlanID: 10, Channel: "ch-3", Type: datapb.CompactionType_Level0DeleteCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  11,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-11",
					NodeID:  102,
				},
				plan:     &datapb.CompactionPlan{PlanID: 11, Channel: "ch-11", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					State:   datapb.CompactionTaskState_pipelining,
					Channel: "ch-3",
					NodeID:  102,
				},
				plan:     &datapb.CompactionPlan{PlanID: 13, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{10, 13}},
		{"without L0 tasks", []CompactionTask{
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  14,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-3",
					NodeID:  102,
				},
				plan:     &datapb.CompactionPlan{PlanID: 14, Channel: "ch-3", Type: datapb.CompactionType_MixCompaction},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
			&mixCompactionTask{
				CompactionTask: &datapb.CompactionTask{
					PlanID:  13,
					Type:    datapb.CompactionType_MixCompaction,
					Channel: "ch-11",
					NodeID:  102,
				},
				sessions: s.mockSessMgr,
				meta:     s.mockMeta,
			},
		}, []UniqueID{13, 14}},
		{"empty tasks", []CompactionTask{}, []UniqueID{}},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			s.SetupTest()
			s.Require().Equal(0, s.handler.getTaskCount())

			// submit the testing tasks
			for _, t := range test.tasks {
				s.handler.submitTask(t)
			}
			s.Equal(len(test.tasks), s.handler.getTaskCount())

			gotTasks := s.handler.schedule()
			s.Equal(test.expectedOut, lo.Map(gotTasks, func(t CompactionTask, _ int) int64 {
				return t.GetPlanID()
			}))
		})
	}
}

func (s *CompactionPlanHandlerSuite) TestPickAnyNode() {
	s.SetupTest()
	nodeSlots := map[int64]int64{
		100: 2,
		101: 3,
	}
	node := s.handler.pickAnyNode(nodeSlots)
	s.Equal(int64(101), node)

	node = s.handler.pickAnyNode(map[int64]int64{})
	s.Equal(int64(NullNodeID), node)
}

func (s *CompactionPlanHandlerSuite) TestPickShardNode() {
	s.SetupTest()
	nodeSlots := map[int64]int64{
		100: 2,
		101: 6,
	}

	t1 := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:  19530,
			Type:    datapb.CompactionType_MixCompaction,
			Channel: "ch-01",
			NodeID:  1,
		},
		plan: &datapb.CompactionPlan{
			PlanID:  19530,
			Channel: "ch-01",
			Type:    datapb.CompactionType_MixCompaction,
		},
		sessions: s.mockSessMgr,
		meta:     s.mockMeta,
	}
	t2 := &l0CompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:  19531,
			Type:    datapb.CompactionType_MixCompaction,
			Channel: "ch-02",
			NodeID:  1,
		},
		plan: &datapb.CompactionPlan{
			PlanID:  19531,
			Channel: "ch-02",
			Type:    datapb.CompactionType_Level0DeleteCompaction,
		},
		sessions: s.mockSessMgr,
		meta:     s.mockMeta,
	}

	s.mockCm.EXPECT().FindWatcher(mock.Anything).RunAndReturn(func(channel string) (int64, error) {
		if channel == "ch-01" {
			return 100, nil
		}
		if channel == "ch-02" {
			return 101, nil
		}
		return 1, nil
	}).Twice()

	node := s.handler.pickShardNode(nodeSlots, t1)
	s.Equal(int64(100), node)

	node = s.handler.pickShardNode(nodeSlots, t2)
	s.Equal(int64(101), node)
}

func (s *CompactionPlanHandlerSuite) TestRemoveTasksByChannel() {
	s.SetupTest()
	ch := "ch1"
	t1 := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:  19530,
			Type:    datapb.CompactionType_MixCompaction,
			Channel: ch,
			NodeID:  1,
		},
		plan: &datapb.CompactionPlan{
			PlanID:  19530,
			Channel: ch,
			Type:    datapb.CompactionType_MixCompaction,
		},
		sessions: s.mockSessMgr,
		meta:     s.mockMeta,
	}
	t2 := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:  19531,
			Type:    datapb.CompactionType_MixCompaction,
			Channel: ch,
			NodeID:  1,
		},
		plan: &datapb.CompactionPlan{
			PlanID:  19531,
			Channel: ch,
			Type:    datapb.CompactionType_MixCompaction,
		},
		sessions: s.mockSessMgr,
		meta:     s.mockMeta,
	}
	s.handler.submitTask(t1)
	s.handler.restoreTask(t2)
	s.handler.removeTasksByChannel(ch)
	s.Equal(0, s.handler.getTaskCount())
}

func (s *CompactionPlanHandlerSuite) TestGetCompactionTask() {
	s.SetupTest()
	inTasks := map[int64]CompactionTask{
		1: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				TriggerID: 1,
				PlanID:    1,
				Type:      datapb.CompactionType_MixCompaction,
				Channel:   "ch-01",
				State:     datapb.CompactionTaskState_executing,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  1,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-01",
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		2: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				TriggerID: 1,
				PlanID:    2,
				Type:      datapb.CompactionType_MixCompaction,
				Channel:   "ch-01",
				State:     datapb.CompactionTaskState_completed,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  2,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-01",
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		3: &l0CompactionTask{
			CompactionTask: &datapb.CompactionTask{
				TriggerID: 1,
				PlanID:    3,
				Type:      datapb.CompactionType_Level0DeleteCompaction,
				Channel:   "ch-02",
				State:     datapb.CompactionTaskState_failed,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  3,
				Type:    datapb.CompactionType_Level0DeleteCompaction,
				Channel: "ch-02",
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
	}
	s.mockMeta.EXPECT().GetCompactionTasksByTriggerID(mock.Anything).RunAndReturn(func(i int64) []*datapb.CompactionTask {
		var ret []*datapb.CompactionTask
		for _, t := range inTasks {
			if t.GetTriggerID() != i {
				continue
			}
			ret = append(ret, t.ShadowClone())
		}
		return ret
	})

	for _, t := range inTasks {
		s.handler.submitTask(t)
	}

	s.Equal(3, s.handler.getTaskCount())
	s.handler.doSchedule()
	s.Equal(3, s.handler.getTaskCount())

	info := s.handler.getCompactionInfo(1)
	s.Equal(1, info.completedCnt)
	s.Equal(1, info.executingCnt)
	s.Equal(1, info.failedCnt)
}

func (s *CompactionPlanHandlerSuite) TestExecCompactionPlan() {
	s.SetupTest()
	s.mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything).Return(true, true).Once()
	handler := newCompactionPlanHandler(nil, s.mockSessMgr, s.mockCm, s.mockMeta, s.mockAlloc, nil, nil)

	task := &datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Channel:   "ch-1",
		Type:      datapb.CompactionType_MixCompaction,
	}
	err := handler.enqueueCompaction(task)
	s.NoError(err)
	t := handler.getCompactionTask(1)
	s.NotNil(t)
	s.handler.taskNumber.Add(1000)
	task.PlanID = 2
	err = s.handler.enqueueCompaction(task)
	s.Error(err)
}

func (s *CompactionPlanHandlerSuite) TestCheckCompaction() {
	s.SetupTest()

	s.mockSessMgr.EXPECT().GetCompactionPlanResult(UniqueID(111), int64(1)).Return(
		&datapb.CompactionPlanResult{PlanID: 1, State: datapb.CompactionTaskState_executing}, nil).Once()

	s.mockSessMgr.EXPECT().GetCompactionPlanResult(UniqueID(111), int64(2)).Return(
		&datapb.CompactionPlanResult{
			PlanID:   2,
			State:    datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{{PlanID: 2}},
		}, nil).Once()

	s.mockSessMgr.EXPECT().GetCompactionPlanResult(UniqueID(111), int64(6)).Return(
		&datapb.CompactionPlanResult{
			PlanID:   6,
			Channel:  "ch-2",
			State:    datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{{PlanID: 6}},
		}, nil).Once()

	s.mockSessMgr.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil)

	inTasks := map[int64]CompactionTask{
		1: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:           1,
				Type:             datapb.CompactionType_MixCompaction,
				TimeoutInSeconds: 1,
				Channel:          "ch-1",
				State:            datapb.CompactionTaskState_executing,
				NodeID:           111,
			},
			plan: &datapb.CompactionPlan{
				PlanID: 1, Channel: "ch-1",
				TimeoutInSeconds: 1,
				Type:             datapb.CompactionType_MixCompaction,
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		2: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  2,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-1",
				State:   datapb.CompactionTaskState_executing,
				NodeID:  111,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  2,
				Channel: "ch-1",
				Type:    datapb.CompactionType_MixCompaction,
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		3: &l0CompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  3,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-1",
				State:   datapb.CompactionTaskState_timeout,
				NodeID:  111,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  3,
				Channel: "ch-1",
				Type:    datapb.CompactionType_MixCompaction,
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		4: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  4,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-1",
				State:   datapb.CompactionTaskState_timeout,
				NodeID:  111,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  4,
				Channel: "ch-1",
				Type:    datapb.CompactionType_MixCompaction,
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
		6: &mixCompactionTask{
			CompactionTask: &datapb.CompactionTask{
				PlanID:  6,
				Type:    datapb.CompactionType_MixCompaction,
				Channel: "ch-2",
				State:   datapb.CompactionTaskState_executing,
				NodeID:  111,
			},
			plan: &datapb.CompactionPlan{
				PlanID:  6,
				Channel: "ch-2",
				Type:    datapb.CompactionType_MixCompaction,
			},
			sessions: s.mockSessMgr,
			meta:     s.mockMeta,
		},
	}

	// s.mockSessMgr.EXPECT().SyncSegments(int64(111), mock.Anything).Return(nil)
	// s.mockMeta.EXPECT().UpdateSegmentsInfo(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything).RunAndReturn(
		func(plan *datapb.CompactionPlan, result *datapb.CompactionPlanResult) ([]*SegmentInfo, *segMetricMutation, error) {
			if plan.GetPlanID() == 2 {
				segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
				return []*SegmentInfo{segment}, &segMetricMutation{}, nil
			} else if plan.GetPlanID() == 6 {
				return nil, nil, errors.Errorf("intended error")
			}
			return nil, nil, errors.Errorf("unexpected error")
		}).Twice()

	for _, t := range inTasks {
		s.handler.submitTask(t)
	}

	picked := s.handler.schedule()
	s.NotEmpty(picked)

	s.handler.doSchedule()
	// time.Sleep(2 * time.Second)
	s.handler.checkCompaction()

	t := s.handler.getCompactionTask(1)
	// timeout
	s.Nil(t)

	t = s.handler.getCompactionTask(2)
	// completed
	s.Nil(t)

	t = s.handler.getCompactionTask(3)
	s.Nil(t)

	t = s.handler.getCompactionTask(4)
	s.Nil(t)

	t = s.handler.getCompactionTask(5)
	// not exist
	s.Nil(t)

	t = s.handler.getCompactionTask(6)
	s.Equal(datapb.CompactionTaskState_executing, t.GetState())
}

func (s *CompactionPlanHandlerSuite) TestProcessCompleteCompaction() {
	s.SetupTest()

	// s.mockSessMgr.EXPECT().SyncSegments(mock.Anything, mock.Anything).Return(nil).Once()
	s.mockMeta.EXPECT().SaveCompactionTask(mock.Anything).Return(nil)
	s.mockMeta.EXPECT().SetSegmentCompacting(mock.Anything, mock.Anything).Return().Twice()
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 100})
	s.mockMeta.EXPECT().CompleteCompactionMutation(mock.Anything, mock.Anything).Return(
		[]*SegmentInfo{segment},
		&segMetricMutation{}, nil).Once()

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

	task := &mixCompactionTask{
		CompactionTask: &datapb.CompactionTask{
			PlanID:    plan.GetPlanID(),
			TriggerID: 1,
			Type:      plan.GetType(),
			State:     datapb.CompactionTaskState_executing,
			NodeID:    dataNodeID,
		},
		plan:     plan,
		sessions: s.mockSessMgr,
		meta:     s.mockMeta,
	}

	compactionResult := datapb.CompactionPlanResult{
		PlanID: 1,
		State:  datapb.CompactionTaskState_completed,
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

	s.mockSessMgr.EXPECT().GetCompactionPlanResult(UniqueID(111), int64(1)).Return(&compactionResult, nil).Once()
	s.mockSessMgr.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil)

	s.handler.submitTask(task)
	s.handler.doSchedule()
	s.Equal(1, s.handler.getTaskCount())
	err := s.handler.checkCompaction()
	s.NoError(err)
	s.Equal(0, len(s.handler.getTasksByState(datapb.CompactionTaskState_completed)))
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
