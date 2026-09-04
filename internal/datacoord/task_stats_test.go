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
	"strconv"
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
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
		st.SetState(indexpb.JobState_JobStateRetry, "retry")
		s.Equal(taskcommon.Retry, st.GetTaskState(), "Stats inspector owns Retry")
	})
}

func (s *statsTaskSuite) TestUpdateStateAndAssignment() {
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

	s.Run("assignment success", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		err := st.assignTask(100)
		s.NoError(err)
		s.Equal(int64(100), st.GetNodeID())
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
		s.Zero(st.GetTaskVersion())
		s.Equal(int64(1), st.GetVersion(), "legacy field is retained but no longer advanced")
	})

	s.Run("assignment failure", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		err := st.assignTask(200)
		s.Error(err)
		s.Equal(int64(100), st.GetNodeID())
	})
}

func (s *statsTaskSuite) TestRetryTask() {
	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	s.mt.statsTaskMeta.catalog = catalog

	st := newStatsTask(&indexpb.StatsTask{
		TaskID:     s.taskID,
		SegmentID:  s.segID,
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		State:      indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())

	s.Run("retry success", func() {
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.retryTask("retry task")
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
		s.Equal("retry task", st.GetFailReason())
		s.False(s.mt.segments.segments[s.segID].isCompacting)
	})

	s.Run("retry with update failure still releases local wrapper", func() {
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).
			Return(errors.New("mock error"))
		st.retryTask("retry task")
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
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

	s.Run("persist assignment failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		s.mt.segments.segments[s.segID].NumOfRows = 1000
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		st.meta.statsTaskMeta.catalog = catalog
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{
			ID:     s.collID,
			Schema: newTestSchema(),
		}, nil)
		st.handler = handler
		ac := allocator.NewMockAllocator(s.T())
		ac.EXPECT().AllocN(mock.Anything).Return(1, 1000000, nil)
		st.allocator = ac

		st.CreateTaskOnWorker(1, session.NewMockCluster(s.T()))
		s.Equal(indexpb.JobState_JobStateInit, st.GetState())
	})

	s.Run("prepare job request failed", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
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
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
	})

	s.Run("worker drop failure still retries", func() {
		st.SetState(indexpb.JobState_JobStateInit, "")
		s.mt.segments.segments[s.segID].isCompacting = false
		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Twice()
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(errors.New("drop failed"))

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
	})

	s.Run("success case", func() {
		s.mt.segments.segments[s.segID].isCompacting = false
		// A wrapper recovered from legacy metadata may still carry Version, but
		// new worker requests identify the attempt only by the fresh task ID.
		st.Version = 9
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)
		st.meta.statsTaskMeta.catalog = catalog

		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().CreateStats(mock.Anything, mock.MatchedBy(func(req *workerpb.CreateStatsRequest) bool {
			return req.GetTaskID() == st.GetTaskID() && req.GetTaskVersion() == 0
		})).Return(nil)

		st.CreateTaskOnWorker(1, cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	})
}

func (s *statsTaskSuite) TestCreateTaskOnWorkerDropsExternalJSONWithoutV3Manifest() {
	handler := s.newStatsTaskCollectionHandler(true)

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

	st := newStatsTask(statsTask, 1, s.mt, handler, nil, newIndexEngineVersionManager())

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

	s.Run("finished result meta failure keeps assigned attempt", func() {
		st.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(&workerpb.StatsResults{
			Results: []*workerpb.StatsResult{{
				TaskID: s.taskID,
				State:  indexpb.JobState_JobStateFinished,
			}},
		}, nil)

		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		st.meta.statsTaskMeta.catalog = catalog

		st.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
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
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
	})

	s.Run("query with error", func() {
		st.SetState(indexpb.JobState_JobStateInProgress, "")
		cluster := session.NewMockCluster(s.T())
		cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		cluster.EXPECT().DropStats(mock.Anything, mock.Anything).Return(nil)

		st.QueryTaskOnWorker(cluster)
		s.Equal(indexpb.JobState_JobStateRetry, st.GetState())
	})
}

func (s *statsTaskSuite) TestQueryTaskOnWorkerFailStopsOnStatsPublicationError() {
	baseManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`
	restoreSegment := s.installJSONStatsSegment(baseManifest)
	defer restoreSegment()

	origCtx := s.mt.ctx
	s.mt.ctx = context.Background()
	defer func() {
		s.mt.ctx = origCtx
	}()

	writeErr := errors.New("ambiguous catalog response")
	mockManifestCommit := mockey.Mock(packed.CommitManifestUpdates).Return(resultManifest, nil).Build()
	defer mockManifestCommit.UnPatch()
	mockCatalogUpdate := mockey.Mock((*mockeyDataCoordCatalog).Update).Return(writeErr).Build()
	defer mockCatalogUpdate.UnPatch()
	origCatalog := s.mt.catalog
	s.mt.catalog = &mockeyDataCoordCatalog{}
	defer func() {
		s.mt.catalog = origCatalog
	}()

	fatalCalled := false
	mockFatal := mockey.Mock(mlog.Fatal).
		To(func(context.Context, string, ...mlog.Field) {
			fatalCalled = true
		}).
		Build()
	defer mockFatal.UnPatch()

	result := &workerpb.StatsResult{
		TaskID:       s.taskID,
		State:        indexpb.JobState_JobStateFinished,
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		Channel:      "ch1",
		BaseManifest: baseManifest,
		Manifest:     resultManifest,
		JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
			500: {
				FieldID: 500,
				Version: 1,
				BuildID: s.taskID,
			},
		},
	}
	cluster := session.NewMockCluster(s.T())
	cluster.EXPECT().QueryStats(mock.Anything, mock.Anything).Return(
		&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil)
	st := s.newJSONStatsTask()

	st.QueryTaskOnWorker(cluster)

	s.True(fatalCalled)
	s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
	segment := s.mt.GetHealthySegment(context.Background(), s.segID)
	s.Require().NotNil(segment)
	s.Equal(baseManifest, segment.GetManifestPath())
	s.Empty(segment.GetJsonKeyStats())
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

	s.Run("skip task without worker", func() {
		cluster := session.NewMockCluster(s.T())
		st.NodeID = 0

		st.DropTaskOnWorker(cluster)
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

// TestSetJobInfoJSONStatsResultManifestHandling exercises the structured-delta
// publish path for a standalone JsonKeyIndexJob: DataCoord rebuilds the JSON key
// StatEntries from the worker's raw result and runs the manifest transaction
// itself, rebasing on the segment's CURRENT manifest.
func (s *statsTaskSuite) TestSetJobInfoJSONStatsResultManifestHandling() {
	basePath := "files/insert_log/1/2/1179"
	currentManifest := packed.MarshalManifestPath(basePath, 2)
	committedManifest := packed.MarshalManifestPath(basePath, 3)

	// Older workers may omit the taskID/version attempt directory. Current V3 workers
	// include it but keep the path relative to the field directory. DataCoord also
	// accepts complete object keys without adding the stats prefix twice.
	relativeFiles := []string{"shared_key_index/.managed.json_0"}
	absoluteFiles := []string{basePath + "/_stats/json_stats.500/shared_key_index/.managed.json_0"}
	workerRelativeFiles := []string{strconv.FormatInt(s.taskID, 10) + "/0/shared_key_index/.managed.json_0"}
	workerFullFiles := []string{basePath + "/_stats/json_stats.500/" + workerRelativeFiles[0]}
	legacyStats := map[int64]*datapb.JsonKeyStats{
		500: {
			FieldID:                500,
			Version:                1,
			BuildID:                s.taskID,
			Files:                  relativeFiles,
			JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
		},
	}
	workerStats := map[int64]*datapb.JsonKeyStats{
		500: {
			FieldID:                500,
			Version:                1,
			BuildID:                s.taskID,
			Files:                  workerRelativeFiles,
			JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
		},
	}
	fullPathStats := map[int64]*datapb.JsonKeyStats{
		500: {
			FieldID:                500,
			Version:                1,
			BuildID:                s.taskID,
			Files:                  workerFullFiles,
			JsonKeyStatsDataFormat: common.JSONStatsDataFormatVersion,
		},
	}

	testCases := []struct {
		name           string
		preStats       map[int64]*datapb.JsonKeyStats
		logs           map[int64]*datapb.JsonKeyStats
		expectCommit   bool
		expectManifest string
		expectStats    bool
		expectFiles    []string
	}{
		{
			name:           "legacy_relative_path_commit",
			logs:           legacyStats,
			expectCommit:   true,
			expectManifest: committedManifest,
			expectStats:    true,
			expectFiles:    absoluteFiles,
		},
		{
			name:           "v3_worker_task_relative_path_commit",
			logs:           workerStats,
			expectCommit:   true,
			expectManifest: committedManifest,
			expectStats:    true,
			expectFiles:    workerFullFiles,
		},
		{
			name:           "full_path_compatibility_commit",
			logs:           fullPathStats,
			expectCommit:   true,
			expectManifest: committedManifest,
			expectStats:    true,
			expectFiles:    workerFullFiles,
		},
		{
			// Result already persisted (same BuildID): the idempotent-replay guard
			// short-circuits before any manifest transaction.
			name:           "already_applied_skip",
			preStats:       map[int64]*datapb.JsonKeyStats{500: {FieldID: 500, BuildID: s.taskID}},
			logs:           legacyStats,
			expectCommit:   false,
			expectManifest: currentManifest,
			expectStats:    true,
		},
		{
			name:           "empty_stats_noop",
			logs:           map[int64]*datapb.JsonKeyStats{},
			expectCommit:   false,
			expectManifest: currentManifest,
			expectStats:    false,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(currentManifest)
			defer restore()
			if testCase.preStats != nil {
				s.mt.segments.segments[s.segID].JsonKeyStats = testCase.preStats
			}

			commitCalled := false
			mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
				func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
					commitCalled = true
					// Rebased on the segment's current manifest (version 2), not the
					// worker's plan-time base.
					s.Equal(basePath, base)
					s.EqualValues(2, version)
					s.Require().Len(updates.Stats, 1)
					s.Equal("json_stats.500", updates.Stats[0].Key)
					// Relative paths are expanded once; complete worker paths are preserved.
					s.Equal(testCase.expectFiles, updates.Stats[0].Files)
					return committedManifest, nil
				}).Build()
			defer mockCommit.UnPatch()

			catalogWrites := 0
			s.mt.catalog = &mockeyDataCoordCatalog{}
			mockUpdate := mockey.Mock((*mockeyDataCoordCatalog).Update).To(
				func(_ *mockeyDataCoordCatalog, _ context.Context, _ ...metastore.UpdateAction) error {
					catalogWrites++
					return nil
				}).Build()
			defer mockUpdate.UnPatch()

			err := s.newJSONStatsTask().SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:           s.taskID,
				CollectionID:     s.collID,
				PartitionID:      s.partID,
				SegmentID:        s.segID,
				Channel:          "ch1",
				JsonKeyStatsLogs: testCase.logs,
			})
			s.NoError(err)
			s.Equal(testCase.expectCommit, commitCalled)

			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
			if testCase.expectStats {
				s.Require().Contains(segment.GetJsonKeyStats(), int64(500))
				s.Equal(s.taskID, segment.GetJsonKeyStats()[500].GetBuildID())
			} else {
				s.Empty(segment.GetJsonKeyStats())
			}
			if testCase.expectCommit {
				s.Equal(1, catalogWrites)
				// Manifest normalization works on a clone; the SegmentInfo dual-write
				// keeps the worker representation unchanged.
				s.Equal(testCase.logs[500].GetFiles(), segment.GetJsonKeyStats()[500].GetFiles())
			} else {
				s.Equal(0, catalogWrites)
			}
		})
	}
}

// TestSetJobInfoTextStatsResultManifestHandling exercises the structured-delta
// publish path for a standalone TextIndexJob: DataCoord rebuilds the text-index
// StatEntries from the worker's raw result (pinning the scalar index version the
// index was built with) and runs the manifest transaction itself, rebasing on the
// segment's CURRENT manifest.
func (s *statsTaskSuite) TestSetJobInfoTextStatsResultManifestHandling() {
	basePath := "files/insert_log/1/2/1179"
	currentManifest := packed.MarshalManifestPath(basePath, 2)
	committedManifest := packed.MarshalManifestPath(basePath, 3)

	files := []string{basePath + "/_stats/text_index.500/tokenizer.json"}
	freshStats := map[int64]*datapb.TextIndexStats{
		500: {
			FieldID:                   500,
			Version:                   1,
			BuildID:                   s.taskID,
			Files:                     files,
			CurrentScalarIndexVersion: 7,
		},
	}

	testCases := []struct {
		name           string
		preStats       map[int64]*datapb.TextIndexStats
		logs           map[int64]*datapb.TextIndexStats
		expectCommit   bool
		expectManifest string
		expectStats    bool
	}{
		{
			name:           "fresh_commit",
			logs:           freshStats,
			expectCommit:   true,
			expectManifest: committedManifest,
			expectStats:    true,
		},
		{
			// Result already persisted (same BuildID): the idempotent-replay guard
			// short-circuits before any manifest transaction.
			name:           "already_applied_skip",
			preStats:       map[int64]*datapb.TextIndexStats{500: {FieldID: 500, BuildID: s.taskID}},
			logs:           freshStats,
			expectCommit:   false,
			expectManifest: currentManifest,
			expectStats:    true,
		},
		{
			name:           "empty_stats_noop",
			logs:           map[int64]*datapb.TextIndexStats{},
			expectCommit:   false,
			expectManifest: currentManifest,
			expectStats:    false,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restore := s.installJSONStatsSegment(currentManifest)
			defer restore()
			if testCase.preStats != nil {
				s.mt.segments.segments[s.segID].TextStatsLogs = testCase.preStats
			}

			commitCalled := false
			mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
				func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
					commitCalled = true
					// Rebased on the segment's current manifest (version 2), not the
					// worker's plan-time base.
					s.Equal(basePath, base)
					s.EqualValues(2, version)
					s.Require().Len(updates.Stats, 1)
					s.Equal("text_index.500", updates.Stats[0].Key)
					s.Equal(files, updates.Stats[0].Files)
					// Scalar index version pinned to the value the worker built with.
					s.Equal("7", updates.Stats[0].Metadata["current_scalar_index_version"])
					return committedManifest, nil
				}).Build()
			defer mockCommit.UnPatch()

			catalogWrites := 0
			s.mt.catalog = &mockeyDataCoordCatalog{}
			mockUpdate := mockey.Mock((*mockeyDataCoordCatalog).Update).To(
				func(_ *mockeyDataCoordCatalog, _ context.Context, _ ...metastore.UpdateAction) error {
					catalogWrites++
					return nil
				}).Build()
			defer mockUpdate.UnPatch()

			err := s.newTextStatsTask().SetJobInfo(context.Background(), &workerpb.StatsResult{
				TaskID:        s.taskID,
				CollectionID:  s.collID,
				PartitionID:   s.partID,
				SegmentID:     s.segID,
				Channel:       "ch1",
				TextStatsLogs: testCase.logs,
			})
			s.NoError(err)
			s.Equal(testCase.expectCommit, commitCalled)

			segment := s.mt.GetHealthySegment(context.Background(), s.segID)
			s.Require().NotNil(segment)
			s.Equal(testCase.expectManifest, segment.GetManifestPath())
			if testCase.expectStats {
				s.Require().Contains(segment.GetTextStatsLogs(), int64(500))
				s.Equal(s.taskID, segment.GetTextStatsLogs()[500].GetBuildID())
			} else {
				s.Empty(segment.GetTextStatsLogs())
			}
			if testCase.expectCommit {
				s.Equal(1, catalogWrites)
			} else {
				s.Equal(0, catalogWrites)
			}
		})
	}
}

func (s *statsTaskSuite) TestClassifyStatsManifestCommitError() {
	s.Nil(classifyStatsManifestCommitError(nil))

	transient := merr.WrapErrServiceUnavailableMsg("object storage temporarily unavailable")
	classified := classifyStatsManifestCommitError(transient)
	s.ErrorIs(classified, merr.ErrServiceUnavailable)
	s.NotErrorIs(classified, errStatsResultStale)

	conflict := staleSegmentManifestError(s.segID, "manifest-1", "manifest-2")
	classified = classifyStatsManifestCommitError(conflict)
	s.ErrorIs(classified, merr.ErrServiceUnavailable)
	s.ErrorIs(classified, errSegmentManifestStale)
	s.ErrorIs(classified, errStatsResultStale)
}

func (s *statsTaskSuite) TestSetJobInfoStatsResultReplayIsIdempotent() {
	baseManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`
	restore := s.installJSONStatsSegment(baseManifest)
	defer restore()

	manifestCommitCount := 0
	mockCommit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			manifestCommitCount++
			s.Equal("files/insert_log/1/2/1179", base)
			s.EqualValues(2, version)
			s.Require().Len(updates.Stats, 1)
			s.Equal("text_index.500", updates.Stats[0].Key)
			return resultManifest, nil
		}).Build()
	defer mockCommit.UnPatch()

	catalogWrites := 0
	s.mt.catalog = &mockeyDataCoordCatalog{}
	mockUpdate := mockey.Mock((*mockeyDataCoordCatalog).Update).To(
		func(_ *mockeyDataCoordCatalog, _ context.Context, _ ...metastore.UpdateAction) error {
			catalogWrites++
			return nil
		}).Build()
	defer mockUpdate.UnPatch()

	result := &workerpb.StatsResult{
		TaskID:       s.taskID,
		CollectionID: s.collID,
		PartitionID:  s.partID,
		SegmentID:    s.segID,
		Channel:      "ch1",
		BaseManifest: baseManifest,
		Manifest:     resultManifest,
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			500: {
				FieldID: 500,
				Version: 1,
				BuildID: s.taskID,
				Files:   []string{"files/insert_log/1/2/1179/_stats/text_index.500/tokenizer.json"},
			},
		},
	}
	task := s.newTextStatsTask()

	s.NoError(task.SetJobInfo(context.Background(), result))
	s.NoError(task.SetJobInfo(context.Background(), result))

	segment := s.mt.GetHealthySegment(context.Background(), s.segID)
	s.Require().NotNil(segment)
	s.Equal(resultManifest, segment.GetManifestPath())
	s.Require().Contains(segment.GetTextStatsLogs(), int64(500))
	s.Equal(s.taskID, segment.GetTextStatsLogs()[500].GetBuildID())
	s.Equal(1, manifestCommitCount, "replaying an already-published result must not rewrite the manifest")
	s.Equal(1, catalogWrites, "replaying an already-published result must not write the segment again")
}

func (s *statsTaskSuite) TestCollectRejectedStatsResultFiles() {
	baseManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	s.Run("collect text and json stats files", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			BaseManifest: baseManifest,
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
		})

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
		})

		s.NoError(err)
		s.Equal([]string{"files/insert_log/1/2/1179/_stats/text_index.101/tokenizer.json"}, files)
	})

	s.Run("json stats without manifest returns typed error", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		})

		s.Empty(files)
		s.ErrorIs(err, merr.ErrServiceInternal)
		s.Contains(err.Error(), "manifest is empty for rejected json stats result")
	})

	s.Run("json stats with invalid manifest returns error", func() {
		files, err := collectRejectedStatsResultFiles(&workerpb.StatsResult{
			BaseManifest: "invalid",
			JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
				102: {
					Files: []string{"shared_key_index/.managed.json_0"},
				},
			},
		})

		s.Empty(files)
		s.Error(err)
	})
}

func (s *statsTaskSuite) TestQueryTaskOnWorkerDiscardsStaleStatsResult() {
	oldManifest := `{"base_path":"files/insert_log/1/2/1179","ver":1}`
	currentManifest := `{"base_path":"files/insert_log/1/2/1179","ver":2}`
	resultManifest := `{"base_path":"files/insert_log/1/2/1179","ver":3}`

	resultFiles := []string{"files/insert_log/1/2/1179/_stats/text_index.500/tokenizer.json"}
	testCases := []struct {
		name                string
		external            bool
		collectionLookupErr error
		invalidJSONManifest bool
		removeErr           error
		workerDropErr       error
		metaDropErr         error
		expectCleanup       bool
		expectRetained      bool
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
		{
			name:                "collection_lookup_error_retains_cleanup_anchor",
			collectionLookupErr: merr.WrapErrServiceUnavailableMsg("rootcoord temporarily unavailable"),
			expectRetained:      true,
		},
		{
			name:                "dropped_collection_releases_cleanup_anchor",
			collectionLookupErr: merr.WrapErrCollectionNotFound(s.collID),
		},
		{
			name:                "invalid_result_path_does_not_replay_result",
			external:            true,
			invalidJSONManifest: true,
		},
		{
			name:          "object_storage_error_does_not_replay_result",
			external:      true,
			removeErr:     merr.WrapErrServiceUnavailableMsg("object storage temporarily unavailable"),
			expectCleanup: true,
		},
		{
			name:           "worker_drop_failure_retains_task_meta",
			external:       true,
			workerDropErr:  merr.WrapErrServiceUnavailableMsg("worker temporarily unavailable"),
			expectRetained: true,
		},
		{
			name:           "meta_drop_failure_retains_task_meta",
			external:       true,
			metaDropErr:    merr.WrapErrServiceUnavailableMsg("catalog temporarily unavailable"),
			expectCleanup:  true,
			expectRetained: true,
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			restoreSegment := s.installJSONStatsSegment(currentManifest)
			defer restoreSegment()
			var handler Handler
			if testCase.collectionLookupErr != nil {
				mockHandler := NewNMockHandler(s.T())
				mockHandler.EXPECT().GetCollection(mock.Anything, s.collID).Return(nil, testCase.collectionLookupErr)
				handler = mockHandler
			} else {
				handler = s.newStatsTaskCollectionHandler(testCase.external)
			}

			// The delta commit rebases on the current manifest under the per-segment
			// lock, so it does not itself reject on a concurrent advance; this guards
			// the remaining defensive wiring: if CommitSegmentManifest ever surfaces a
			// classified stale conflict, QueryTaskOnWorker must discard (not retry) the
			// obsolete worker result. Mock the commit to return that classified error.
			mockCommit := mockey.Mock((*meta).CommitSegmentManifest).Return(
				staleSegmentManifestError(s.segID, oldManifest, currentManifest)).Build()
			defer mockCommit.UnPatch()

			subJobType := indexpb.StatsSubJob_TextIndexJob
			if testCase.invalidJSONManifest {
				subJobType = indexpb.StatsSubJob_JsonKeyIndexJob
			}
			task := &indexpb.StatsTask{
				CollectionID:    s.collID,
				PartitionID:     s.partID,
				SegmentID:       s.segID,
				TargetSegmentID: s.segID,
				InsertChannel:   "ch1",
				TaskID:          s.taskID + 1000,
				SubJobType:      subJobType,
				State:           indexpb.JobState_JobStateInProgress,
				NodeID:          11,
			}
			origStatsTaskMeta := s.mt.statsTaskMeta
			droppedStatsTasks := make([]int64, 0)
			statsCatalog := &mockeyDataCoordCatalog{}
			mockSaveStatsTask := mockey.Mock((*mockeyDataCoordCatalog).SaveStatsTask).Return(nil).Build()
			defer mockSaveStatsTask.UnPatch()
			mockDropStatsTask := mockey.Mock((*mockeyDataCoordCatalog).DropStatsTask).To(
				func(_ *mockeyDataCoordCatalog, _ context.Context, taskID int64) error {
					droppedStatsTasks = append(droppedStatsTasks, taskID)
					return testCase.metaDropErr
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
			task.State = indexpb.JobState_JobStateInProgress
			defer func() {
				s.mt.statsTaskMeta = origStatsTaskMeta
			}()

			removedFiles := make([]string, 0)
			chunkManager := &mockeyChunkManager{}
			mockMultiRemove := mockey.Mock((*mockeyChunkManager).MultiRemove).To(
				func(_ *mockeyChunkManager, _ context.Context, filePaths []string) error {
					removedFiles = append(removedFiles, filePaths...)
					return testCase.removeErr
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
			if testCase.invalidJSONManifest {
				result.BaseManifest = "invalid"
				result.TextStatsLogs = nil
				result.JsonKeyStatsLogs = map[int64]*datapb.JsonKeyStats{
					500: {Files: []string{"task/path/.managed.json_0"}},
				}
			}
			cluster := &mockeyStatsCluster{}
			mockQueryStats := mockey.Mock((*mockeyStatsCluster).QueryStats).Return(
				&workerpb.StatsResults{Results: []*workerpb.StatsResult{result}}, nil).Build()
			defer mockQueryStats.UnPatch()
			droppedWorkerTasks := make([]int64, 0)
			mockDropStats := mockey.Mock((*mockeyStatsCluster).DropStats).To(
				func(_ *mockeyStatsCluster, _ int64, taskID int64) error {
					droppedWorkerTasks = append(droppedWorkerTasks, taskID)
					return testCase.workerDropErr
				}).Build()
			defer mockDropStats.UnPatch()
			st := newStatsTask(task, 1, s.mt, handler, nil, newIndexEngineVersionManager())

			st.QueryTaskOnWorker(cluster)

			ownershipUnknown := testCase.collectionLookupErr != nil &&
				!errors.Is(testCase.collectionLookupErr, merr.ErrCollectionNotFound)
			if testCase.expectRetained {
				s.Equal(indexpb.JobState_JobStateInProgress, st.GetState())
				s.NotNil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(s.segID, subJobType))
			} else {
				s.Equal(indexpb.JobState_JobStateNone, st.GetState())
				s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(s.segID, subJobType))
			}
			if ownershipUnknown {
				s.Empty(droppedStatsTasks)
				s.Empty(droppedWorkerTasks)
			} else {
				s.Equal([]int64{task.GetTaskID()}, droppedWorkerTasks)
				if testCase.workerDropErr != nil {
					s.Empty(droppedStatsTasks)
				} else {
					s.Equal([]int64{task.GetTaskID()}, droppedStatsTasks)
				}
			}
			if testCase.expectCleanup {
				s.ElementsMatch(resultFiles, removedFiles)
			} else {
				s.Empty(removedFiles)
			}
		})
	}
}

func (s *statsTaskSuite) newStatsTaskCollectionHandler(external bool) Handler {
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
	handler := NewNMockHandler(s.T())
	handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
		ID:     s.collID,
		Schema: schema,
	}, nil).Maybe()
	return handler
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

func (s *statsTaskSuite) newJSONStatsTask() *statsTask {
	return newStatsTask(&indexpb.StatsTask{
		CollectionID:    s.collID,
		PartitionID:     s.partID,
		SegmentID:       s.segID,
		TargetSegmentID: s.segID,
		InsertChannel:   "ch1",
		TaskID:          s.taskID,
		SubJobType:      indexpb.StatsSubJob_JsonKeyIndexJob,
		State:           indexpb.JobState_JobStateInProgress,
	}, 1, s.mt, nil, nil, newIndexEngineVersionManager())
}

func (s *statsTaskSuite) newTextStatsTask() *statsTask {
	return newStatsTask(&indexpb.StatsTask{
		CollectionID:    s.collID,
		PartitionID:     s.partID,
		SegmentID:       s.segID,
		TargetSegmentID: s.segID,
		InsertChannel:   "ch1",
		TaskID:          s.taskID,
		SubJobType:      indexpb.StatsSubJob_TextIndexJob,
		State:           indexpb.JobState_JobStateInProgress,
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

		_, err := st.prepareJobRequest(context.Background(), segment)
		s.Error(err)
		s.Contains(err.Error(), "failed to get collection info")
	})

	s.Run("nil schema", func() {
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
			Schema: nil,
		}, nil)
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment)
		s.Error(err)
		s.Contains(err.Error(), "collection schema is nil or has no fields")
	})

	s.Run("empty schema fields", func() {
		handler := NewNMockHandler(s.T())
		handler.EXPECT().GetCollection(mock.Anything, s.collID).Return(&collectionInfo{
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{}},
		}, nil)
		st.handler = handler

		_, err := st.prepareJobRequest(context.Background(), segment)
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
		s.False(req.GetUseV3StatsAttemptPath(), "rolling upgrades must keep the legacy V3 stats layout")
	})
}
