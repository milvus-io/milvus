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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

type mockeyDataCoordCatalog struct {
	metastore.DataCoordCatalog
}

type mockeyStatsCluster struct {
	session.Cluster
}

type mockeyChunkManager struct {
	storage.ChunkManager
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
		SubJobType:    indexpb.StatsSubJob_JsonKeyIndexJob,
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
						Stats:         &datapb.Statistics{InsertBinlogSize: 512 * 1024 * 1024},
					},
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
		SubJobType:      indexpb.StatsSubJob_JsonKeyIndexJob,
		State:           indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInit,
		Version:    1,
		NodeID:     0,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
		err := st.UpdateTaskVersion(100, "manifest-at-dispatch")
		s.NoError(err)
		s.Equal(int64(2), st.GetVersion())
		s.Equal(int64(100), st.GetNodeID())
		s.Equal("manifest-at-dispatch", st.GetDispatchedManifest())
	})

	s.Run("update version failure", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		err := st.UpdateTaskVersion(200, "manifest-at-dispatch")
		s.Error(err)
	})
}

func (s *statsTaskSuite) TestResetTask() {
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	s.mt.statsTaskMeta.catalog = catalog

	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInit,
	}, 1, s.mt, handler, nil, newIndexEngineVersionManager())

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
		SubJobType:      indexpb.StatsSubJob_JsonKeyIndexJob,
		State:           indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

	s.Run("segment not healthy", func() {
		// Set up a temporary nil segment return
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Dropped

		s.Run("drop task failed", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
			st.meta.statsTaskMeta.catalog = catalog
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

	s.Run("send job failed and cancel failed stays in progress", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(errors.New("mock create error"))
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(errors.New("mock cancel error"))

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})

	s.Run("success case", func() {
		s.mt.segments.segments[s.segID].isCompacting = false
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(nil)

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})
}

// The CAS expected value (StatsTask.dispatched_manifest) and the manifest the
// worker is told to build on (CreateStatsRequest.manifest_path) must be the same
// string. A stamp staler than the request rejects every result; a stamp fresher
// than it silently adopts a stale one - the exact lost update the fence exists to
// prevent. Both now come from one local in CreateTaskOnWorker; this pins that.
func (s *statsTaskSuite) TestCreateTaskOnWorkerStampMatchesDispatchedRequest() {
	const dispatchedManifest = `{"base_path":"files/insert_log/1/2/1179","ver":4}`

	restore := s.installJSONStatsSegment(dispatchedManifest)
	defer restore()
	restoreCollection := s.installStatsTaskCollection(false)
	defer restoreCollection()

	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
		ID: s.collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		},
	}, nil)

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
	s.mt.statsTaskMeta.catalog = catalog

	st := newStatsTask(&indexpb.StatsTask{
		CollectionID:    s.collID,
		PartitionID:     s.partID,
		SegmentID:       s.segID,
		TargetSegmentID: s.segID,
		InsertChannel:   "ch1",
		TaskID:          s.taskID,
		SubJobType:      indexpb.StatsSubJob_TextIndexJob,
		State:           indexpb.JobState_JobStateInit,
	}, 1, s.mt, handler, nil, newIndexEngineVersionManager())

	ac := allocator.NewMockAllocator(s.T())
	ac.EXPECT().AllocN(mock.Anything).Return(1, 1000000, nil)
	st.allocator = ac

	var sentManifest string
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).
		RunAndReturn(func(_ int64, req *workerpb.CreateStatsRequest) error {
			sentManifest = req.GetManifestPath()
			return nil
		})

	st.CreateTaskOnWorker(1, cluster)

	s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	s.Equal(dispatchedManifest, sentManifest, "the worker must build on the manifest we dispatched")
	s.Equal(dispatchedManifest, st.GetDispatchedManifest(), "the task record must stamp the same manifest")
	s.Equal(st.GetDispatchedManifest(), sentManifest, "stamp and request must never diverge")
}

func (s *statsTaskSuite) TestCreateTaskOnWorkerDropsExternalJSONWithoutV3Manifest() {
	restoreCollection := s.installStatsTaskCollection(true)
	defer restoreCollection()

	segmentID := int64(3179)
	taskID := int64(4179)
	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   s.collID,
			PartitionID:    s.partID,
			InsertChannel:  "ch1",
			NumOfRows:      1024,
			State:          commonpb.SegmentState_Flushed,
			MaxRowNum:      2048,
			Level:          datapb.SegmentLevel_L1,
			StorageVersion: storage.StorageV2,
		},
	}
	s.mt.segments.segments[segmentID] = segment
	defer delete(s.mt.segments.segments, segmentID)

	statsTask := &indexpb.StatsTask{
		CollectionID:    s.collID,
		PartitionID:     s.partID,
		SegmentID:       segmentID,
		TargetSegmentID: segmentID,
		InsertChannel:   "ch1",
		TaskID:          taskID,
		SubJobType:      indexpb.StatsSubJob_JsonKeyIndexJob,
		State:           indexpb.JobState_JobStateInit,
	}
	s.mt.statsTaskMeta.tasks.Insert(taskID, statsTask)
	s.mt.statsTaskMeta.segmentID2Tasks.Insert(
		createSecondaryIndexKey(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob.String()),
		statsTask,
	)

	dropped := make([]int64, 0)
	catalog := &mockeyDataCoordCatalog{}
	mockDropStatsTask := mockey.Mock((*mockeyDataCoordCatalog).DropStatsTask).To(
		func(*mockeyDataCoordCatalog, context.Context, int64) error {
			dropped = append(dropped, taskID)
			return nil
		}).Build()
	defer mockDropStatsTask.UnPatch()
	s.mt.statsTaskMeta.catalog = catalog

	created := 0
	cluster := &mockeyStatsCluster{}
	mockCreateStats := mockey.Mock((*mockeyStatsCluster).CreateStats).To(
		func(*mockeyStatsCluster, int64, *workerpb.CreateStatsRequest) error {
			created++
			return nil
		}).Build()
	defer mockCreateStats.UnPatch()

	st := newStatsTask(statsTask, 1, s.mt, nil, nil, newIndexEngineVersionManager())

	st.CreateTaskOnWorker(1, cluster)

	s.Equal(indexpb.JobState_JobStateNone, st.GetState())
	s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob))
	s.Equal([]int64{taskID}, dropped)
	s.Zero(created)
}

func (s *statsTaskSuite) TestQueryTaskOnWorker() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInProgress,
		NodeID:     100,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInProgress,
		NodeID:     100,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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
			State:         commonpb.SegmentState_Flushed,
		},
	}

	s.mt.segments.segments[s.segID] = testSegment

	s.Run("set job info success for different sub job types", func() {
		catalog := &mockeyDataCoordCatalog{}
		s.mt.statsTaskMeta.catalog = catalog
		s.mt.catalog = catalog

		st.SubJobType = indexpb.StatsSubJob_JsonKeyIndexJob
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

func (s *statsTaskSuite) TestSetJobInfoJSONStatsResultManifestHandling() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	statsLogs := map[int64]*datapb.JsonKeyStats{
		500: {
			FieldID:                500,
			Version:                1,
			BuildID:                s.taskID,
			JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
		},
	}

	testCases := []struct {
		name           string
		current        string
		base           string
		result         string
		logs           map[int64]*datapb.JsonKeyStats
		expectManifest string
		expectStats    bool
		expectCatalog  bool
		expectErr      error
	}{
		{
			name:           "stale_result",
			current:        currentManifest,
			base:           oldManifest,
			result:         resultManifest,
			logs:           statsLogs,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			name:           "fresh_result",
			current:        currentManifest,
			base:           currentManifest,
			result:         resultManifest,
			logs:           statsLogs,
			expectManifest: resultManifest,
			expectStats:    true,
			expectCatalog:  true,
		},
		{
			name:           "empty_stats_noop",
			current:        currentManifest,
			base:           currentManifest,
			result:         currentManifest,
			logs:           map[int64]*datapb.JsonKeyStats{},
			expectManifest: currentManifest,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(testCase.current)
			defer restore()

			alterSegmentsCount := 0
			mockAlterSegments := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).To(
				func(
					_ *mockeyDataCoordCatalog,
					_ context.Context,
					_ []*datapb.SegmentInfo,
					_ ...metastore.BinlogsIncrement,
				) error {
					alterSegmentsCount++
					return nil
				}).Build()
			defer mockAlterSegments.UnPatch()
			s.mt.catalog = &mockeyDataCoordCatalog{}

			err := s.newJSONStatsTask(testCase.base).SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:       s.taskID,
				CollectionID: s.collID,
				PartitionID:  s.partID,
				SegmentID:    s.segID,
				Channel:      "ch1",
				// Still reported by the worker, and still used to derive the paths of
				// a rejected result's files (collectRejectedStatsResultFiles) - just no
				// longer what the adoption CAS fences on.
				BaseManifest:     testCase.base,
				Manifest:         testCase.result,
				JsonKeyStatsLogs: testCase.logs,
			})
			if testCase.expectErr != nil {
				s.ErrorIs(err, testCase.expectErr)
			} else {
				s.NoError(err)
			}

			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
			if testCase.expectStats {
				s.Require().Contains(segment.GetJsonKeyStats(), int64(500))
				s.Equal(s.taskID, segment.GetJsonKeyStats()[500].GetBuildID())
			} else {
				s.Empty(segment.GetJsonKeyStats())
			}
			if testCase.expectCatalog {
				s.Equal(1, alterSegmentsCount)
			} else {
				s.Equal(0, alterSegmentsCount)
			}
		})
	}
}

// TestSetJobInfoSortResultManifestHandling covers the shared, non-text/json manifest
// update branch (sort sub-job). A result built on a non-empty stale base is rejected
// instead of blindly overwriting a concurrently-committed manifest. A baseless result
// is adopted (fail-open): sort results carry no base by design (birth commit), and
// older DataNodes cannot report one, so the CAS is skipped for an empty base.
func (s *statsTaskSuite) TestSetJobInfoSortResultManifestHandling() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	testCases := []struct {
		name           string
		current        string
		base           string
		result         string
		expectManifest string
		expectCatalog  bool
		expectErr      error
	}{
		{
			name:           "stale_result_rejected",
			current:        currentManifest,
			base:           oldManifest,
			result:         resultManifest,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			name:           "fresh_result_adopted",
			current:        currentManifest,
			base:           currentManifest,
			result:         resultManifest,
			expectManifest: resultManifest,
			expectCatalog:  true,
		},
		{
			// A baseless result is adopted (fail-open): birth commit / older worker.
			name:           "baseless_adopted",
			current:        currentManifest,
			base:           "",
			result:         resultManifest,
			expectManifest: resultManifest,
			expectCatalog:  true,
		},
		{
			// True birth: the freshly allocated sort target has no manifest yet.
			name:           "true_birth_adopted",
			current:        "",
			base:           "",
			result:         resultManifest,
			expectManifest: resultManifest,
			expectCatalog:  true,
		},
		{
			// Idempotent replay of an already adopted result: no-op, no error.
			name:           "baseless_replay_noop",
			current:        resultManifest,
			base:           "",
			result:         resultManifest,
			expectManifest: resultManifest,
		},
		{
			// Correct base, but the result points at a DIFFERENT segment's manifest.
			// base==current alone would have adopted it (corrupting the pointer); the
			// successor check rejects a cross-segment manifest.
			name:           "cross_segment_result_rejected",
			current:        currentManifest,
			base:           currentManifest,
			result:         `{"base_path":"files/insert_log/9/9/9999","ver":3}`,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			// Correct base, but the result rolls the version backwards on the same
			// base path — reject rather than regress the segment.
			name:           "rollback_result_rejected",
			current:        currentManifest,
			base:           currentManifest,
			result:         oldManifest,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			// Baseless (fail-open on the missing base) but the result still moves the
			// pointer of a materialized segment to another segment's manifest: refuse
			// even without a base, since we can still see it is not a forward successor.
			name:           "baseless_cross_segment_rejected",
			current:        currentManifest,
			base:           "",
			result:         `{"base_path":"files/insert_log/9/9/9999","ver":3}`,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			// Baseless rollback on the same base path: reject.
			name:           "baseless_rollback_rejected",
			current:        currentManifest,
			base:           "",
			result:         oldManifest,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(testCase.current)
			defer restore()

			alterSegmentsCount := 0
			mockAlterSegments := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).To(
				func(
					_ *mockeyDataCoordCatalog,
					_ context.Context,
					_ []*datapb.SegmentInfo,
					_ ...metastore.BinlogsIncrement,
				) error {
					alterSegmentsCount++
					return nil
				}).Build()
			defer mockAlterSegments.UnPatch()
			s.mt.catalog = &mockeyDataCoordCatalog{}

			err := s.newSortStatsTask(testCase.base).SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:       s.taskID,
				CollectionID: s.collID,
				PartitionID:  s.partID,
				SegmentID:    s.segID,
				Channel:      "ch1",
				Manifest:     testCase.result,
			})
			if testCase.expectErr != nil {
				s.ErrorIs(err, testCase.expectErr)
			} else {
				s.NoError(err)
			}

			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
			if testCase.expectCatalog {
				s.Equal(1, alterSegmentsCount)
			} else {
				s.Equal(0, alterSegmentsCount)
			}
		})
	}
}

// A real Sort writes to a freshly allocated TARGET segment while its base
// describes the ORIGIN it read. The two manifests belong to different entities,
// so the base fence must not compare them - doing so would reject every sort
// result. The successor fence still applies, which is what keeps a retry from
// rolling an already-materialized target backwards.
//
// Note the other sort tests use newSortStatsTask, whose TargetSegmentID equals
// SegmentID; those exercise the same-segment path where the fence does apply.
func (s *statsTaskSuite) TestSetJobInfoSortCrossSegmentSkipsBaseFence() {
	const targetSegID = int64(987654)
	originManifest := `{"base_path":"files/insert_log/1/2/1179","ver":7}`
	targetV1 := `{"base_path":"files/insert_log/1/2/987654","ver":1}`
	targetV2 := `{"base_path":"files/insert_log/1/2/987654","ver":2}`

	testCases := []struct {
		name           string
		targetCurrent  string
		base           string
		result         string
		expectManifest string
		expectErr      error
	}{
		{
			// The case that motivated this: base is the origin's manifest and can
			// never equal the target's. Adopted because the adoption is a birth.
			name:           "origin_base_on_fresh_target_adopted",
			targetCurrent:  "",
			base:           originManifest,
			result:         targetV1,
			expectManifest: targetV1,
		},
		{
			// Retry after the target was already materialized: still cross-segment,
			// so the base is still ignored, but the successor fence must advance.
			name:           "retry_forward_on_materialized_target_adopted",
			targetCurrent:  targetV1,
			base:           originManifest,
			result:         targetV2,
			expectManifest: targetV2,
		},
		{
			// Skipping the base fence must not disable the successor fence: a
			// rollback onto a materialized target is still refused.
			name:           "retry_rollback_on_materialized_target_rejected",
			targetCurrent:  targetV2,
			base:           originManifest,
			result:         targetV1,
			expectManifest: targetV2,
			expectErr:      errStatsResultStale,
		},
		{
			// Nor may a cross-segment adoption point the target at some third
			// segment's manifest.
			name:           "foreign_manifest_on_materialized_target_rejected",
			targetCurrent:  targetV1,
			base:           originManifest,
			result:         `{"base_path":"files/insert_log/9/9/9999","ver":3}`,
			expectManifest: targetV1,
			expectErr:      errStatsResultStale,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restoreOrigin := s.installJSONStatsSegment(originManifest)
			defer restoreOrigin()

			origTarget, hadTarget := s.mt.segments.segments[targetSegID]
			s.mt.segments.segments[targetSegID] = &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:             targetSegID,
					CollectionID:   s.collID,
					PartitionID:    s.partID,
					InsertChannel:  "ch1",
					NumOfRows:      1024,
					State:          commonpb.SegmentState_Flushed,
					Level:          datapb.SegmentLevel_L1,
					ManifestPath:   testCase.targetCurrent,
					StorageVersion: 3,
				},
			}
			defer func() {
				if hadTarget {
					s.mt.segments.segments[targetSegID] = origTarget
					return
				}
				delete(s.mt.segments.segments, targetSegID)
			}()

			mockAlterSegments := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).Return(nil).Build()
			defer mockAlterSegments.UnPatch()
			s.mt.catalog = &mockeyDataCoordCatalog{}

			st := newStatsTask(&indexpb.StatsTask{
				CollectionID:    s.collID,
				PartitionID:     s.partID,
				SegmentID:       s.segID,
				TargetSegmentID: targetSegID,
				InsertChannel:   "ch1",
				TaskID:          s.taskID,
				SubJobType:      indexpb.StatsSubJob_Sort,
				State:           indexpb.JobState_JobStateInProgress,
				// Dispatched on the ORIGIN's manifest: the value that must not be
				// compared against the target's.
				DispatchedManifest: testCase.base,
			}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

			err := st.SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:       s.taskID,
				CollectionID: s.collID,
				PartitionID:  s.partID,
				SegmentID:    s.segID,
				Channel:      "ch1",
				Manifest:     testCase.result,
			})
			if testCase.expectErr != nil {
				s.ErrorIs(err, testCase.expectErr)
			} else {
				s.NoError(err)
			}

			target := s.mt.GetHealthySegment(context.Background(), targetSegID)
			s.Require().NotNil(target)
			s.Equal(testCase.expectManifest, target.GetManifestPath())
			// The origin is never touched by a cross-segment adoption.
			origin := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(origin)
			s.Equal(originManifest, origin.GetManifestPath())
		})
	}
}

func (s *statsTaskSuite) TestSetJobInfoTextStatsResultManifestHandling() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	statsLogs := map[int64]*datapb.TextIndexStats{
		500: {
			FieldID: 500,
			Version: 1,
			BuildID: s.taskID,
			Files:   []string{"files/insert_log/1/2/1179/_stats/text_index.500/tokenizer.json"},
		},
	}

	testCases := []struct {
		name           string
		current        string
		base           string
		result         string
		logs           map[int64]*datapb.TextIndexStats
		expectManifest string
		expectStats    bool
		expectCatalog  bool
		expectErr      error
	}{
		{
			name:           "stale_result",
			current:        currentManifest,
			base:           oldManifest,
			result:         resultManifest,
			logs:           statsLogs,
			expectManifest: currentManifest,
			expectErr:      errStatsResultStale,
		},
		{
			name:           "fresh_result",
			current:        currentManifest,
			base:           currentManifest,
			result:         resultManifest,
			logs:           statsLogs,
			expectManifest: resultManifest,
			expectStats:    true,
			expectCatalog:  true,
		},
		{
			name:           "empty_stats_noop",
			current:        currentManifest,
			base:           currentManifest,
			result:         currentManifest,
			logs:           map[int64]*datapb.TextIndexStats{},
			expectManifest: currentManifest,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(testCase.current)
			defer restore()

			alterSegmentsCount := 0
			mockAlterSegments := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).To(
				func(
					_ *mockeyDataCoordCatalog,
					_ context.Context,
					_ []*datapb.SegmentInfo,
					_ ...metastore.BinlogsIncrement,
				) error {
					alterSegmentsCount++
					return nil
				}).Build()
			defer mockAlterSegments.UnPatch()
			s.mt.catalog = &mockeyDataCoordCatalog{}

			err := s.newTextStatsTask(testCase.base).SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:        s.taskID,
				CollectionID:  s.collID,
				PartitionID:   s.partID,
				SegmentID:     s.segID,
				Channel:       "ch1",
				Manifest:      testCase.result,
				TextStatsLogs: testCase.logs,
			})
			if testCase.expectErr != nil {
				s.ErrorIs(err, testCase.expectErr)
			} else {
				s.NoError(err)
			}

			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
			if testCase.expectStats {
				s.Require().Contains(segment.GetTextStatsLogs(), int64(500))
				s.Equal(s.taskID, segment.GetTextStatsLogs()[500].GetBuildID())
			} else {
				s.Empty(segment.GetTextStatsLogs())
			}
			if testCase.expectCatalog {
				s.Equal(1, alterSegmentsCount)
			} else {
				s.Equal(0, alterSegmentsCount)
			}
		})
	}
}

func (s *statsTaskSuite) TestCollectRejectedStatsResultFiles() {
	baseManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	s.Run("collect text and json stats files", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json"},
				},
			},
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		}, baseManifest)

		s.NoError(err)
		s.ElementsMatch([]string{
			"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json",
			"files/insert_log/1/2/1179/_stats/json_stats.102/shared_key_index/.managed.json_0",
		}, files)
	})

	s.Run("deduplicate text stats files without json stats", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{
						"",
						"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json",
						"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json",
					},
				},
			},
		}, baseManifest)

		s.NoError(err)
		s.Equal([]string{"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json"}, files)
	})

	s.Run("json stats without trusted manifest returns typed error", func() {
		// No manifest DataCoord vouches for => refuse to resolve any delete paths.
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		}, "")

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "no trusted manifest for rejected stats result")
	})

	s.Run("text stats without trusted manifest returns typed error", func() {
		// Text keys arrive absolute, so with no trusted manifest there is nothing to
		// check them against. Refuse rather than feed unverified keys to MultiRemove.
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json"},
				},
			},
		}, "")

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "no trusted manifest for rejected stats result")
	})

	s.Run("text stats file outside the written segment prefix is refused", func() {
		// A rejected result's own file list must not be able to aim MultiRemove at
		// another segment. The whole call fails; nothing is returned for deletion,
		// including the one legitimately-prefixed key alongside it.
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{
						"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json",
						"files/insert_log/9/9/9999/_stats/text_index.101/tokenizer.json",
					},
				},
			},
		}, baseManifest)

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "outside the written segment prefix")
	})

	s.Run("text stats prefix check is not fooled by a sibling path sharing a string prefix", func() {
		// "…/1179extra/…" shares the literal prefix "…/1179" but is a different
		// segment directory, so the check must compare on a path boundary.
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{"files/insert_log/1/2/1179extra/_stats/text_index.101/tokenizer.json"},
				},
			},
		}, baseManifest)

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "outside the written segment prefix")
	})

	s.Run("text stats prefix check rejects dot-dot traversal", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					Files: []string{"files/insert_log/1/2/1179/_stats/../1180/foreign"},
				},
			},
		}, baseManifest)

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "outside the written segment prefix")
	})

	s.Run("json stats path join cannot escape with dot-dot", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"../../../1180/foreign"},
				},
			},
		}, baseManifest)

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "unsafe file")
	})

	s.Run("worker supplied manifests cannot steer deletion", func() {
		// The result echoes a foreign base AND a foreign result manifest - pointing
		// at another segment is one of the reasons a result gets rejected. Deletion
		// must follow the trusted manifest only.
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			BaseManifest: `{"base_path":"files/insert_log/9/9/9999","ver":1}`,
			Manifest:     `{"base_path":"files/insert_log/8/8/8888","ver":1}`,
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		}, baseManifest)

		s.NoError(err)
		s.Equal([]string{
			"files/insert_log/1/2/1179/_stats/json_stats.102/shared_key_index/.managed.json_0",
		}, files)
	})

	s.Run("json stats with invalid manifest returns error", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		}, "invalid")

		s.Empty(files)
		s.Error(err)
	})
}

// The CAS base comes from the task record, so the fence no longer depends on the
// DataNode's version: a worker too old to echo base_manifest is fenced exactly
// like a current one. Before the base moved onto the task this same result was
// adopted unfenced, because an empty result base had to fail open.
func (s *statsTaskSuite) TestSetJobInfoFencesOldDataNodeViaDispatchedManifest() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	testCases := []struct {
		name               string
		dispatchedManifest string
		expectManifest     string
		expectErr          error
	}{
		{
			// Old DataNode: reports no base. The attempt was dispatched on a manifest
			// a concurrent commit has since superseded, and we know that from our own
			// task record - so reject and re-run rather than clobber that commit.
			name:               "old_datanode_stale_attempt_rejected",
			dispatchedManifest: oldManifest,
			expectManifest:     currentManifest,
			expectErr:          errStatsResultStale,
		},
		{
			// Same old DataNode, but the attempt really was dispatched on what the
			// segment still holds: adopt.
			name:               "old_datanode_fresh_attempt_adopted",
			dispatchedManifest: currentManifest,
			expectManifest:     resultManifest,
		},
		{
			// Attempt dispatched by a DataCoord predating the field and still in
			// flight across the upgrade: nothing to fence with, so fail open. This
			// window drains within one upgrade cycle.
			name:               "attempt_from_pre_field_datacoord_adopted",
			dispatchedManifest: "",
			expectManifest:     resultManifest,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(currentManifest)
			defer restore()

			mockAlterSegments := mockey.Mock((*mockeyDataCoordCatalog).AlterSegments).Return(nil).Build()
			defer mockAlterSegments.UnPatch()
			s.mt.catalog = &mockeyDataCoordCatalog{}

			err := s.newTextStatsTask(testCase.dispatchedManifest).SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:       s.taskID,
				CollectionID: s.collID,
				PartitionID:  s.partID,
				SegmentID:    s.segID,
				Channel:      "ch1",
				// An old worker sends no base at all.
				BaseManifest:  "",
				Manifest:      resultManifest,
				TextStatsLogs: map[int64]*datapb.TextIndexStats{500: {Files: []string{"f"}}},
			})
			if testCase.expectErr != nil {
				s.ErrorIs(err, testCase.expectErr)
			} else {
				s.NoError(err)
			}
			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
		})
	}
}

func (s *statsTaskSuite) TestQueryTaskOnWorkerDiscardsStaleStatsResult() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	resultFiles := []string{"files/insert_log/1/2/1179/_stats/text_index.500/tokenizer.json"}
	testCases := []struct {
		name          string
		external      bool
		expectCleanup bool
	}{
		{
			name:     "internal_collection_skips_file_cleanup",
			external: false,
		},
		{
			name:          "external_collection_cleans_files",
			external:      true,
			expectCleanup: true,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restoreSegment := s.installJSONStatsSegment(currentManifest)
			defer restoreSegment()
			restoreCollection := s.installStatsTaskCollection(testCase.external)
			defer restoreCollection()

			task := &indexpb.StatsTask{
				CollectionID:    s.collID,
				PartitionID:     s.partID,
				SegmentID:       s.segID,
				TargetSegmentID: s.segID,
				InsertChannel:   "ch1",
				TaskID:          s.taskID + 1000,
				SubJobType:      indexpb.StatsSubJob_TextIndexJob,
				State:           indexpb.JobState_JobStateInProgress,
				NodeID:          11,
				// Dispatched on a manifest a concurrent commit has since superseded.
				DispatchedManifest: oldManifest,
			}
			origStatsTaskMeta := s.mt.statsTaskMeta
			droppedStatsTasks := make([]int64, 0)
			statsCatalog := &mockeyDataCoordCatalog{}
			mockSaveStatsTask := mockey.Mock((*mockeyDataCoordCatalog).SaveStatsTask).Return(nil).Build()
			defer mockSaveStatsTask.UnPatch()
			mockDropStatsTask := mockey.Mock((*mockeyDataCoordCatalog).DropStatsTask).To(
				func(_ *mockeyDataCoordCatalog, _ context.Context, taskID int64) error {
					droppedStatsTasks = append(droppedStatsTasks, taskID)
					return nil
				}).Build()
			defer mockDropStatsTask.UnPatch()
			s.mt.statsTaskMeta = &statsTaskMeta{
				keyLock:         lock.NewKeyLock[UniqueID](),
				ctx:             context.Background(),
				catalog:         statsCatalog,
				tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
				segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
			}
			s.NoError(s.mt.statsTaskMeta.AddStatsTask(task))
			defer func() {
				s.mt.statsTaskMeta = origStatsTaskMeta
			}()

			removedFiles := make([]string, 0)
			chunkManager := &mockeyChunkManager{}
			mockMultiRemove := mockey.Mock((*mockeyChunkManager).MultiRemove).To(
				func(_ *mockeyChunkManager, _ context.Context, filePaths []string) error {
					removedFiles = append(removedFiles, filePaths...)
					return nil
				}).Build()
			defer mockMultiRemove.UnPatch()
			origChunkManager := s.mt.chunkManager
			s.mt.chunkManager = chunkManager
			defer func() {
				s.mt.chunkManager = origChunkManager
			}()

			result := &workerpb.StatsResult{
				TaskID:        task.GetTaskID(),
				State:         indexpb.JobState_JobStateFinished,
				CollectionID:  s.collID,
				PartitionID:   s.partID,
				SegmentID:     s.segID,
				Channel:       "ch1",
				BaseManifest:  oldManifest,
				Manifest:      resultManifest,
				TextStatsLogs: map[int64]*datapb.TextIndexStats{500: {Files: resultFiles}},
			}
			cluster := &mockeyStatsCluster{}
			mockQueryStats := mockey.Mock((*mockeyStatsCluster).QueryStats).Return(
				&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Build()
			defer mockQueryStats.UnPatch()
			droppedWorkerTasks := make([]int64, 0)
			mockDropStats := mockey.Mock((*mockeyStatsCluster).DropStats).To(
				func(_ *mockeyStatsCluster, _ int64, taskID int64) error {
					droppedWorkerTasks = append(droppedWorkerTasks, taskID)
					return nil
				}).Build()
			defer mockDropStats.UnPatch()
			st := newStatsTask(task, 1, s.mt, nil, nil, newIndexEngineVersionManager())

			st.QueryTaskOnWorker(cluster)

			s.Equal(indexpb.JobState_JobStateNone, st.GetState())
			s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(s.segID, indexpb.StatsSubJob_TextIndexJob))
			s.Equal([]int64{task.GetTaskID()}, droppedStatsTasks)
			s.Equal([]int64{task.GetTaskID()}, droppedWorkerTasks)
			if testCase.expectCleanup {
				s.ElementsMatch(resultFiles, removedFiles)
			} else {
				s.Empty(removedFiles)
			}
		})
	}
}

func (s *statsTaskSuite) installStatsTaskCollection(external bool) func() {
	origCollections := s.mt.collections
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				Name:     "pk",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	if external {
		schema.Fields[0].ExternalField = "pk"
	}
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(s.collID, &collectionInfo{
		ID:     s.collID,
		Schema: schema,
	})
	s.mt.collections = collections
	return func() {
		s.mt.collections = origCollections
	}
}

func (s *statsTaskSuite) installJSONStatsSegment(manifest string) func() {
	origSegment := s.mt.segments.segments[s.segID]
	origCatalog := s.mt.catalog
	s.mt.segments.segments[s.segID] = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             s.segID,
			CollectionID:   s.collID,
			PartitionID:    s.partID,
			InsertChannel:  "ch1",
			NumOfRows:      1024,
			State:          commonpb.SegmentState_Flushed,
			Level:          datapb.SegmentLevel_L1,
			ManifestPath:   manifest,
			StorageVersion: 3,
		},
	}
	return func() {
		s.mt.segments.segments[s.segID] = origSegment
		s.mt.catalog = origCatalog
	}
}

// dispatchedManifest is the manifest DataCoord stamped on the attempt - the
// expected value of the adoption CAS. It no longer comes off the result.
func (s *statsTaskSuite) newJSONStatsTask(dispatchedManifest string) *statsTask {
	return newStatsTask(&indexpb.StatsTask{
		CollectionID:       s.collID,
		PartitionID:        s.partID,
		SegmentID:          s.segID,
		TargetSegmentID:    s.segID,
		InsertChannel:      "ch1",
		TaskID:             s.taskID,
		SubJobType:         indexpb.StatsSubJob_JsonKeyIndexJob,
		State:              indexpb.JobState_JobStateInProgress,
		DispatchedManifest: dispatchedManifest,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())
}

// dispatchedManifest is the manifest DataCoord stamped on the attempt - the
// expected value of the adoption CAS. It no longer comes off the result.
func (s *statsTaskSuite) newTextStatsTask(dispatchedManifest string) *statsTask {
	return newStatsTask(&indexpb.StatsTask{
		CollectionID:       s.collID,
		PartitionID:        s.partID,
		SegmentID:          s.segID,
		TargetSegmentID:    s.segID,
		InsertChannel:      "ch1",
		TaskID:             s.taskID,
		SubJobType:         indexpb.StatsSubJob_TextIndexJob,
		State:              indexpb.JobState_JobStateInProgress,
		DispatchedManifest: dispatchedManifest,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())
}

// dispatchedManifest is the manifest DataCoord stamped on the attempt - the
// expected value of the adoption CAS. It no longer comes off the result.
func (s *statsTaskSuite) newSortStatsTask(dispatchedManifest string) *statsTask {
	return newStatsTask(&indexpb.StatsTask{
		CollectionID:       s.collID,
		PartitionID:        s.partID,
		SegmentID:          s.segID,
		TargetSegmentID:    s.segID,
		InsertChannel:      "ch1",
		TaskID:             s.taskID,
		SubJobType:         indexpb.StatsSubJob_Sort,
		State:              indexpb.JobState_JobStateInProgress,
		DispatchedManifest: dispatchedManifest,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())
}

// TestPrepareJobRequest tests edge cases of prepareJobRequest
func (s *statsTaskSuite) TestPrepareJobRequest() {
	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInit,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

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

		_, err := st.prepareJobRequest(context.Background(), segment, "manifest-at-dispatch")
		s.Error(err)
		s.Contains(err.Error(), "failed to get collection info")
	})

	s.Run("nil schema", func() {
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
			Schema: nil,
		}, nil)
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment, "manifest-at-dispatch")
		s.Error(err)
		s.Contains(err.Error(), "collection schema is nil or has no fields")
	})

	s.Run("empty schema fields", func() {
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{}},
		}, nil)
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment, "manifest-at-dispatch")
		s.Error(err)
		s.Contains(err.Error(), "collection schema is nil or has no fields")
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

		_, err := st.prepareJobRequest(context.Background(), segment, "manifest-at-dispatch")
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

		req, err := st.prepareJobRequest(context.Background(), segment, "manifest-at-dispatch")
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
	})
}
