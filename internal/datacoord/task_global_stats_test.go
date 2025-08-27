package datacoord

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
)

type globalStatsTaskSuite struct {
	suite.Suite
	mt *meta

	collID    int64
	partID    int64
	taskID    int64
	vChannel  string
	allocator allocator.Allocator
	handler   Handler
}

func Test_globalStatsTaskSuite(t *testing.T) {
	suite.Run(t, new(globalStatsTaskSuite))
}

func (s *globalStatsTaskSuite) SetupSuite() {
	s.collID = 1
	s.partID = 2
	s.taskID = 1000
	s.vChannel = "test_vchannel"

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	globalStatsMt := &globalStatsMeta{
		ctx:     context.Background(),
		catalog: catalog,
		tasks:   make(map[int64]*datapb.GlobalStatsTask),
	}

	globalStatsTask := &datapb.GlobalStatsTask{
		CollectionID: s.collID,
		PartitionID:  s.partID,
		TaskID:       s.taskID,
		Version:      1,
		VChannel:     s.vChannel,
		NodeID:       0,
		State:        indexpb.JobState_JobStateInit,
		FailReason:   "",
		SegmentInfos: []*datapb.SegmentInfo{
			{ID: 101},
			{ID: 102},
		},
	}
	globalStatsMt.tasks[s.taskID] = globalStatsTask

	s.mt = &meta{
		globalStatsMeta: globalStatsMt,
	}

	s.allocator = &allocator.MockAllocator{}
}

func (s *globalStatsTaskSuite) TestBasicTaskOperations() {
	gt := newGlobalStatsTask(&datapb.GlobalStatsTask{
		TaskID: s.taskID,
		State:  indexpb.JobState_JobStateInit,
	}, 1, s.mt, s.handler, s.allocator)

	s.Run("task type and state", func() {
		s.Equal(taskcommon.GlobalStats, gt.GetTaskType())
		s.Equal(gt.GetState(), gt.GetTaskState())
		s.Equal(int64(1), gt.GetTaskSlot())
	})

	s.Run("time management", func() {
		now := time.Now()

		gt.SetTaskTime(taskcommon.TimeQueue, now)
		s.Equal(now, gt.GetTaskTime(taskcommon.TimeQueue))

		gt.SetTaskTime(taskcommon.TimeStart, now)
		s.Equal(now, gt.GetTaskTime(taskcommon.TimeStart))

		gt.SetTaskTime(taskcommon.TimeEnd, now)
		s.Equal(now, gt.GetTaskTime(taskcommon.TimeEnd))
	})

	s.Run("state management", func() {
		gt.SetState(indexpb.JobState_JobStateInProgress, "test reason")
		s.Equal(indexpb.JobState_JobStateInProgress, gt.GetState())
		s.Equal("test reason", gt.GetFailReason())
	})
}

func (s *globalStatsTaskSuite) TestCreateTaskOnWorker() {
	gt := newGlobalStatsTask(&datapb.GlobalStatsTask{
		TaskID: s.taskID,
		State:  indexpb.JobState_JobStateInit,
	}, 1, s.mt, s.handler, s.allocator)

	s.Run("successful creation", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateGlobalStats(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.On("SaveGlobalStatsTask", mock.Anything, mock.Anything).Return(nil)
		s.mt.globalStatsMeta.catalog = catalog

		gt.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, gt.GetState())
	})
}

func (s *globalStatsTaskSuite) TestQueryTaskOnWorker() {
	gt := newGlobalStatsTask(&datapb.GlobalStatsTask{
		TaskID: s.taskID,
		NodeID: 1,
		State:  indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, s.handler, s.allocator)

	s.Run("query failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryGlobalStats(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error"))
		cluster.EXPECT().DropGlobalStats(mock.Anything, mock.Anything).Return(nil)

		gt.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, gt.GetState())
	})

	s.Run("task finished", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryGlobalStats(mock.Anything, mock.Anything).Return(&workerpb.GlobalStatsResults{
			Results: []*workerpb.GlobalStatsResult{{
				TaskID:     s.taskID,
				State:      indexpb.JobState_JobStateFinished,
				FailReason: "",
				Files:      []string{"file1", "file2"},
			}},
		}, nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveGlobalStatsTask(mock.Anything, mock.Anything).Return(nil)
		s.mt.globalStatsMeta.catalog = catalog

		gt.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateFinished, gt.GetState())
	})

	s.Run("task failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryGlobalStats(mock.Anything, mock.Anything).Return(&workerpb.GlobalStatsResults{
			Results: []*workerpb.GlobalStatsResult{{
				TaskID:     s.taskID,
				State:      indexpb.JobState_JobStateFailed,
				FailReason: "task failed",
			}},
		}, nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveGlobalStatsTask(mock.Anything, mock.Anything).Return(nil)
		s.mt.globalStatsMeta.catalog = catalog

		gt.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateFailed, gt.GetState())
		s.Equal("task failed", gt.GetFailReason())
	})
}

func (s *globalStatsTaskSuite) TestDropTaskOnWorker() {
	gt := newGlobalStatsTask(&datapb.GlobalStatsTask{
		TaskID: s.taskID,
		NodeID: 1,
		State:  indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, s.handler, s.allocator)

	s.Run("drop failed", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropGlobalStats(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))

		gt.DropTaskOnWorker(cluster)
	})

	s.Run("drop success", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropGlobalStats(mock.Anything, mock.Anything).Return(nil)

		gt.DropTaskOnWorker(cluster)
	})
}
