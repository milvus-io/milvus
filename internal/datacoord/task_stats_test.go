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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type statsTaskSuite struct {
	suite.Suite
	mt *meta

	collID   int64
	partID   int64
	segID    int64
	taskID   int64
	targetID int64
}

func Test_statsTaskSuite(t *testing.T) {
	suite.Run(t, new(statsTaskSuite))
}

func (s *statsTaskSuite) SetupSuite() {
	s.collID = 1
	s.partID = 2
	s.taskID = 1178
	s.segID = 1179
	s.targetID = 1180

	tasks := typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
	statsTask := &indexpb.StatsTask{
		CollectionID:  1,
		PartitionID:   2,
		SegmentID:     s.segID,
		InsertChannel: "ch1",
		TaskID:        s.taskID,
		SubJobType:    indexpb.StatsSubJob_Sort,
		Version:       0,
		NodeID:        0,
		State:         indexpb.JobState_JobStateInit,
		FailReason:    "",
	}
	tasks.Insert(s.taskID, statsTask)
	secondaryIndex := typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
	secondaryKey := createSecondaryIndexKey(statsTask.GetSegmentID(), statsTask.GetSubJobType().String())
	secondaryIndex.Insert(secondaryKey, statsTask)

	s.mt = &meta{
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				s.segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            s.segID,
						CollectionID:  s.collID,
						PartitionID:   s.partID,
						InsertChannel: "ch1",
						NumOfRows:     65535,
						State:         commonpb.SegmentState_Flushed,
						MaxRowNum:     65535,
						Level:         datapb.SegmentLevel_L2,
					},
					size: *atomic.NewInt64(512 * 1024 * 1024),
				},
			},
			secondaryIndexes: segmentInfoIndexes{
				coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
					s.collID: {
						s.segID: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:            s.segID,
								CollectionID:  s.collID,
								PartitionID:   s.partID,
								InsertChannel: "ch1",
								NumOfRows:     65535,
								State:         commonpb.SegmentState_Flushed,
								MaxRowNum:     65535,
								Level:         datapb.SegmentLevel_L2,
							},
						},
					},
				},
				channel2Segments: map[string]map[UniqueID]*SegmentInfo{
					"ch1": {
						s.segID: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:            s.segID,
								CollectionID:  s.collID,
								PartitionID:   s.partID,
								InsertChannel: "ch1",
								NumOfRows:     65535,
								State:         commonpb.SegmentState_Flushed,
								MaxRowNum:     65535,
								Level:         datapb.SegmentLevel_L2,
							},
						},
					},
				},
			},
			compactionTo: map[UniqueID][]UniqueID{},
		},

		statsTaskMeta: &statsTaskMeta{
			keyLock:         lock.NewKeyLock[UniqueID](),
			ctx:             context.Background(),
			catalog:         nil,
			tasks:           tasks,
			segmentID2Tasks: secondaryIndex,
		},
	}
}

func (s *statsTaskSuite) TestBasicTaskOperations() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:          s.taskID,
		SegmentID:       s.segID,
		TargetSegmentID: s.targetID,
		SubJobType:      indexpb.StatsSubJob_Sort,
		State:           indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	s.Run("task type and state", func() {
		s.Equal(taskcommon.Stats, st.GetTaskType())
		s.Equal(st.GetState(), st.GetTaskState())
		s.Equal(int64(1), st.GetTaskSlot())
	})

	s.Run("time management", func() {
		now := time.Now()

		st.SetTaskTime(taskcommon.TimeQueue, now)
		s.Equal(now, st.GetTaskTime(taskcommon.TimeQueue))

		st.SetTaskTime(taskcommon.TimeStart, now)
		s.Equal(now, st.GetTaskTime(taskcommon.TimeStart))

		st.SetTaskTime(taskcommon.TimeEnd, now)
		s.Equal(now, st.GetTaskTime(taskcommon.TimeEnd))
	})

	s.Run("state management", func() {
		st.SetState(indexpb.JobState_JobStateInProgress, "test reason")
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
		s.Equal("test reason", st.GetFailReason())
	})
}

func (s *statsTaskSuite) TestUpdateStateAndVersion() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInit,
		Version:    1,
		NodeID:     0,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	s.Run("update state success", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		err := st.UpdateStateWithMeta(indexpb.JobState_JobStateInProgress, "running")
		s.NoError(err)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
		s.Equal("running", st.GetFailReason())
	})

	s.Run("update state failure", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		err := st.UpdateStateWithMeta(indexpb.JobState_JobStateFailed, "error")
		s.Error(err)
	})

	s.Run("update version success", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		err := st.UpdateTaskVersion(100)
		s.NoError(err)
		s.Equal(int64(2), st.GetVersion())
		s.Equal(int64(100), st.GetNodeID())
	})

	s.Run("update version failure", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		err := st.UpdateTaskVersion(200)
		s.Error(err)
	})
}

func (s *statsTaskSuite) TestResetTask() {
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	s.mt.statsTaskMeta.catalog = catalog

	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	s.Run("reset success", func() {
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.resetTask(context.Background(), "reset task")
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
		s.Equal("reset task", st.GetFailReason())
		s.False(s.mt.segments.segments[s.segID].isCompacting)
	})

	s.Run("reset with update failure", func() {
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		st.resetTask(context.Background(), "reset task")
		// State remains unchanged on error
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})
}

func (s *statsTaskSuite) TestHandleEmptySegment() {
	handler := NewNMockHandler(s.T())

	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, handler, nil, newIndexEngineVersionManager())

	s.Run("handle empty segment success", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		err := st.handleEmptySegment(context.Background())
		s.NoError(err)
		s.Equal(indexpb.JobState_JobStateFinished, st.GetState())
	})

	s.Run("handle empty segment with update failure", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		err := st.handleEmptySegment(context.Background())
		s.Error(err)
	})
}

func (s *statsTaskSuite) TestCreateTaskOnWorker() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:          s.taskID,
		SegmentID:       s.segID,
		TargetSegmentID: s.targetID,
		SubJobType:      indexpb.StatsSubJob_Sort,
		State:           indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	s.Run("segment is compacting", func() {
		s.mt.segments.segments[s.segID].isCompacting = true
		s.Run("drop task failed", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
			st.meta.statsTaskMeta.catalog = catalog
			st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
			s.Equal(indexpb.JobState_JobStateInit, st.GetState())
		})

		s.Run("drop task success", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)
			st.meta.statsTaskMeta.catalog = catalog
			st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
			s.Equal(indexpb.JobState_JobStateNone, st.GetState())
		})
		s.mt.segments.segments[s.segID].isCompacting = false
	})

	s.Run("segment in L0 compaction", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		compactionInspector := NewMockCompactionInspector(s.T())
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(false)
		st.compactionInspector = compactionInspector
		st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
		// No state change as we just log and exit
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	compactionInspector := NewMockCompactionInspector(s.T())
	compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
	st.compactionInspector = compactionInspector

	s.Run("segment not healthy", func() {
		// Set up a temporary nil segment return
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Dropped

		s.Run("drop task failed", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
			st.meta.statsTaskMeta.catalog = catalog
			s.NoError(s.mt.statsTaskMeta.AddStatsTask(st.StatsTask))
			st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
			s.Equal(indexpb.JobState_JobStateInit, st.GetState())
		})

		s.Run("drop task success", func() {
			s.mt.segments.segments[s.segID].isCompacting = false
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil)
			st.meta.statsTaskMeta.catalog = catalog
			st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
			s.Equal(indexpb.JobState_JobStateNone, st.GetState())
		})
	})

	s.Run("empty segment", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog
		st.meta.catalog = catalog
		s.NoError(s.mt.statsTaskMeta.AddStatsTask(st.StatsTask))
		s.mt.segments.segments[s.segID].NumOfRows = 0
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed

		st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
		s.Equal(indexpb.JobState_JobStateFinished, st.GetState())
	})

	s.Run("update version failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		s.mt.segments.segments[s.segID].NumOfRows = 1000
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		st.meta.statsTaskMeta.catalog = catalog

		st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	s.Run("prepare job request failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		st.handler = handler

		cluster := session.NewMockCluster(s.T())
		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	s.Run("send job to worker failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID: s.collID,
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "3600",
			},
			Schema: newTestSchema(),
		}, nil)
		st.handler = handler
		ac := allocator.NewMockAllocator(s.T())
		ac.EXPECT().AllocN(mock.Anything).Return(1, 1000000, nil)
		st.allocator = ac

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	s.Run("update InProgress failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error")).Once()
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(nil)
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	s.Run("success case", func() {
		s.mt.segments.segments[s.segID].isCompacting = false
		compactionInspector.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(nil)

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})
}

func (s *statsTaskSuite) TestQueryTaskOnWorker() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
		NodeID:     100,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.mt.catalog = catalog

	s.Run("query task success", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(&workerpb.StatsResults{
			Results: []*workerpb.StatsResult{{
				TaskID: s.taskID,
				State:  indexpb.JobState_JobStateFinished,
			}},
		}, nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog

		st.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateFinished, st.GetState())
	})

	s.Run("node not found", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(nil, merr.ErrNodeNotFound)
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog

		// Should skip the query
		st.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, st.GetState()) // No change
	})

	s.Run("query with error", func() {
		st.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		st.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInit, st.GetState()) // No change
	})
}

func (s *statsTaskSuite) TestDropTaskOnWorker() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
		NodeID:     100,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	s.Run("drop task success", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		st.DropTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})

	s.Run("drop with error from worker", func() {
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(errors.New("mock error"))

		st.DropTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})
}

func (s *statsTaskSuite) TestSetJobInfo() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	result := &workerpb.StatsResult{
		TaskID:       s.taskID,
		State:        indexpb.JobState_JobStateFinished,
		FailReason:   "",
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		Channel:      "test-channel",
		NumRows:      1000,
	}

	// Temporarily replace the segment with one we control
	origSegments := s.mt.segments

	// Create test segment for testing
	testSegment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            s.segID,
			CollectionID:  s.collID,
			PartitionID:   s.partID,
			InsertChannel: "ch1",
		},
	}

	s.mt.segments.segments[s.segID] = testSegment

	s.Run("set job info success for different sub job types", func() {
		// Create mock catalog
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		s.mt.statsTaskMeta.catalog = catalog
		s.mt.catalog = catalog

		// Test Sort job type
		st.SubJobType = indexpb.StatsSubJob_Sort
		err := st.SetJobInfo(context.Background(), result)
		s.NoError(err)

		// Test TextIndexJob job type
		st.SubJobType = indexpb.StatsSubJob_TextIndexJob
		err = st.SetJobInfo(context.Background(), result)
		s.NoError(err)

		// Test BM25Job job type
		st.SubJobType = indexpb.StatsSubJob_BM25Job
		err = st.SetJobInfo(context.Background(), result)
		s.NoError(err)
	})

	// Restore original segments
	s.mt.segments = origSegments
}

// TestPrepareJobRequest tests edge cases of prepareJobRequest
func (s *statsTaskSuite) TestPrepareJobRequest() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, nil, newIndexEngineVersionManager())

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            s.segID,
			CollectionID:  s.collID,
			PartitionID:   s.partID,
			InsertChannel: "test-channel",
			NumOfRows:     1000,
		},
	}

	s.Run("get collection failed", func() {
		// Create a handler that returns nil collection
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(nil, errors.New("collection not found"))
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment)
		s.Error(err)
		s.Contains(err.Error(), "failed to get collection info")
	})

	s.Run("invalid collection TTL", func() {
		// Create handler with collection that has invalid TTL
		collection := &collectionInfo{
			Schema: newTestSchema(),
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "invalid-ttl", // This will cause strconv.Atoi to fail
			},
		}

		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(collection, nil)
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment)
		s.Error(err)
		s.Contains(err.Error(), "failed to get collection TTL")
	})

	s.Run("allocation failure", func() {
		// Create a handler with valid collection
		collection := &collectionInfo{
			Schema: newTestSchema(),
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "3600",
			},
		}

		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(collection, nil)

		// Create allocator that fails
		ac := allocator.NewMockAllocator(s.T())
		ac.EXPECT().AllocN(mock.Anything).Return(int64(0), int64(0), errors.New("allocation failed"))

		st.handler = handler
		st.allocator = ac

		_, err := st.prepareJobRequest(context.Background(), segment)
		s.Error(err)
		s.Contains(err.Error(), "failed to allocate log IDs")
	})

	s.Run("success case", func() {
		// Create a handler with valid collection
		collection := &collectionInfo{
			Schema: newTestSchema(),
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "3600",
			},
		}

		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(collection, nil)

		// Create successful allocator
		ac := allocator.NewMockAllocator(s.T())
		startID, endID := int64(1000), int64(2000)
		ac.EXPECT().AllocN(mock.Anything).Return(startID, endID, nil)

		st.handler = handler
		st.allocator = ac

		// Add binlogs and deltalogs to the segment
		segment.Binlogs = []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "binlog1"}}},
		}
		segment.Deltalogs = []*datapb.FieldBinlog{
			{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "deltalog1"}}},
		}

		req, err := st.prepareJobRequest(context.Background(), segment)
		s.NoError(err)
		s.NotNil(req)

		// Verify request fields
		s.Equal(s.taskID, req.TaskID)
		s.Equal(s.collID, req.CollectionID)
		s.Equal(s.partID, req.PartitionID)
		s.Equal(s.segID, req.SegmentID)
		s.Equal(startID, req.StartLogID)
		s.Equal(endID, req.EndLogID)
		s.Equal(int64(1000), req.NumRows)
		s.Equal(int64(3600000000000), req.CollectionTtl) // 1 hour in nanoseconds
	})
}
