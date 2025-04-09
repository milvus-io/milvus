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
	"fmt"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type statsTaskSuite struct {
	suite.Suite
	mt *meta

	segID    int64
	taskID   int64
	targetID int64
}

func Test_statsTaskSuite(t *testing.T) {
	suite.Run(t, new(statsTaskSuite))
}

func (s *statsTaskSuite) SetupSuite() {
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
						CollectionID:  collID,
						PartitionID:   partID,
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
					collID: {
						s.segID: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:            s.segID,
								CollectionID:  collID,
								PartitionID:   partID,
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
								CollectionID:  collID,
								PartitionID:   partID,
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

func (s *statsTaskSuite) TestTaskStats_PreCheck() {
	st := newStatsTask(s.taskID, s.segID, s.targetID, indexpb.StatsSubJob_Sort, 1)

	s.Equal(s.taskID, st.GetTaskID())

	s.Run("queue time", func() {
		t := time.Now()
		st.SetQueueTime(t)
		s.Equal(t, st.GetQueueTime())
	})

	s.Run("start time", func() {
		t := time.Now()
		st.SetStartTime(t)
		s.Equal(t, st.GetStartTime())
	})

	s.Run("end time", func() {
		t := time.Now()
		st.SetEndTime(t)
		s.Equal(t, st.GetEndTime())
	})

	s.Run("CheckTaskHealthy", func() {
		s.True(st.CheckTaskHealthy(s.mt))

		s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Dropped
		s.False(st.CheckTaskHealthy(s.mt))
	})

	s.Run("UpdateVersion", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog

		s.Run("segment is compacting", func() {
			s.mt.segments.segments[s.segID].isCompacting = true

			s.Error(st.UpdateVersion(context.Background(), 1, s.mt, nil))
		})

		s.Run("segment is in l0 compaction", func() {
			s.mt.segments.segments[s.segID].isCompacting = false
			compactionHandler := NewMockCompactionPlanContext(s.T())
			compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(false)
			s.Error(st.UpdateVersion(context.Background(), 1, s.mt, compactionHandler))
			s.False(s.mt.segments.segments[s.segID].isCompacting)
		})

		s.Run("normal case", func() {
			s.mt.segments.segments[s.segID].isCompacting = false
			compactionHandler := NewMockCompactionPlanContext(s.T())
			compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)

			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()
			s.NoError(st.UpdateVersion(context.Background(), 1, s.mt, compactionHandler))
		})

		s.Run("failed case", func() {
			s.mt.segments.segments[s.segID].isCompacting = false
			compactionHandler := NewMockCompactionPlanContext(s.T())
			compactionHandler.EXPECT().checkAndSetSegmentStating(mock.Anything, mock.Anything).Return(true)

			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(fmt.Errorf("error")).Once()
			s.Error(st.UpdateVersion(context.Background(), 1, s.mt, compactionHandler))
		})
	})

	s.Run("UpdateMetaBuildingState", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog

		s.Run("normal case", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Once()
			s.NoError(st.UpdateMetaBuildingState(s.mt))
		})

		s.Run("update error", func() {
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(fmt.Errorf("error")).Once()
			s.Error(st.UpdateMetaBuildingState(s.mt))
		})
	})

	s.Run("PreCheck", func() {
		catalog := catalogmocks.NewDataCoordCatalog(s.T())
		s.mt.statsTaskMeta.catalog = catalog

		s.Run("segment not healthy", func() {
			s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Dropped

			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta: s.mt,
			})

			s.False(checkPass)
		})

		s.Run("segment is sorted", func() {
			s.mt.segments.segments[s.segID].State = commonpb.SegmentState_Flushed
			s.mt.segments.segments[s.segID].IsSorted = true

			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta: s.mt,
			})

			s.False(checkPass)
		})

		s.Run("get collection failed", func() {
			s.mt.segments.segments[s.segID].IsSorted = false

			handler := NewNMockHandler(s.T())
			handler.EXPECT().GetCollection(context.Background(), collID).Return(nil, fmt.Errorf("mock error")).Once()
			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta:    s.mt,
				handler: handler,
			})

			s.False(checkPass)
		})

		s.Run("get collection ttl failed", func() {
			handler := NewNMockHandler(s.T())
			handler.EXPECT().GetCollection(context.Background(), collID).Return(&collectionInfo{
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Name: "test_1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      100,
							Name:         "pk",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_Int64,
							AutoID:       true,
						},
						{
							FieldID:      101,
							Name:         "embedding",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_FloatVector,
							AutoID:       true,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: "dim", Value: "8"},
							},
						},
					},
				},
				Properties: map[string]string{common.CollectionTTLConfigKey: "false"},
			}, nil).Once()

			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta:    s.mt,
				handler: handler,
			})

			s.False(checkPass)
		})

		s.Run("alloc failed", func() {
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().AllocN(mock.Anything).Return(0, 0, fmt.Errorf("mock error"))

			handler := NewNMockHandler(s.T())
			handler.EXPECT().GetCollection(context.Background(), collID).Return(&collectionInfo{
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Name: "test_1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      100,
							Name:         "pk",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_Int64,
							AutoID:       true,
						},
						{
							FieldID:      101,
							Name:         "embedding",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_FloatVector,
							AutoID:       true,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: "dim", Value: "8"},
							},
						},
					},
				},
				Properties: map[string]string{common.CollectionTTLConfigKey: "100"},
			}, nil)

			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta:      s.mt,
				handler:   handler,
				allocator: alloc,
			})

			s.False(checkPass)
		})

		s.Run("normal case", func() {
			alloc := allocator.NewMockAllocator(s.T())
			alloc.EXPECT().AllocN(mock.Anything).Return(1, 100, nil)

			handler := NewNMockHandler(s.T())
			handler.EXPECT().GetCollection(context.Background(), collID).Return(&collectionInfo{
				ID: collID,
				Schema: &schemapb.CollectionSchema{
					Name: "test_1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      100,
							Name:         "pk",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_Int64,
							AutoID:       true,
						},
						{
							FieldID:      101,
							Name:         "embedding",
							IsPrimaryKey: true,
							DataType:     schemapb.DataType_FloatVector,
							AutoID:       true,
							TypeParams: []*commonpb.KeyValuePair{
								{Key: "dim", Value: "8"},
							},
						},
					},
				},
				Properties: map[string]string{common.CollectionTTLConfigKey: "100"},
			}, nil)

			checkPass := st.PreCheck(context.Background(), &taskScheduler{
				meta:      s.mt,
				handler:   handler,
				allocator: alloc,
			})

			s.True(checkPass)
		})
	})

	s.Run("AssignTask", func() {
		s.Run("assign failed", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mock error",
			}, nil)

			mt := &meta{
				segments: &SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						st.segmentID: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    st.segmentID,
								State: commonpb.SegmentState_Flushed,
							},
						},
					},
				},
			}

			s.False(st.AssignTask(context.Background(), in, mt))
		})

		s.Run("assign success", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().CreateJobV2(mock.Anything, mock.Anything).Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			}, nil)

			mt := &meta{
				segments: &SegmentsInfo{
					segments: map[int64]*SegmentInfo{
						st.segmentID: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:    st.segmentID,
								State: commonpb.SegmentState_Flushed,
							},
						},
					},
				},
			}

			s.True(st.AssignTask(context.Background(), in, mt))
		})
	})

	s.Run("QueryResult", func() {
		s.Run("query failed", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "mock failed",
				},
			}, nil)

			st.QueryResult(context.Background(), in)
		})

		s.Run("state finished", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Result: &workerpb.QueryJobsV2Response_StatsJobResults{
					StatsJobResults: &workerpb.StatsResults{
						Results: []*workerpb.StatsResult{
							{
								TaskID:        s.taskID,
								State:         indexpb.JobState_JobStateFinished,
								FailReason:    "",
								CollectionID:  collID,
								PartitionID:   partID,
								SegmentID:     s.segID,
								Channel:       "ch1",
								InsertLogs:    nil,
								StatsLogs:     nil,
								TextStatsLogs: nil,
								NumRows:       65535,
							},
						},
					},
				},
			}, nil)

			st.QueryResult(context.Background(), in)
			s.Equal(indexpb.JobState_JobStateFinished, st.taskInfo.State)
		})

		s.Run("task none", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Result: &workerpb.QueryJobsV2Response_StatsJobResults{
					StatsJobResults: &workerpb.StatsResults{
						Results: []*workerpb.StatsResult{
							{
								TaskID:       s.taskID,
								State:        indexpb.JobState_JobStateNone,
								FailReason:   "",
								CollectionID: collID,
								PartitionID:  partID,
								SegmentID:    s.segID,
								NumRows:      65535,
							},
						},
					},
				},
			}, nil)

			st.QueryResult(context.Background(), in)
			s.Equal(indexpb.JobState_JobStateRetry, st.taskInfo.State)
		})

		s.Run("task not exist", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().QueryJobsV2(mock.Anything, mock.Anything).Return(&workerpb.QueryJobsV2Response{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_Success,
				},
				Result: &workerpb.QueryJobsV2Response_StatsJobResults{
					StatsJobResults: &workerpb.StatsResults{
						Results: []*workerpb.StatsResult{},
					},
				},
			}, nil)

			st.QueryResult(context.Background(), in)
			s.Equal(indexpb.JobState_JobStateRetry, st.taskInfo.State)
		})
	})

	s.Run("DropTaskOnWorker", func() {
		s.Run("drop failed", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "mock error",
			}, nil)

			s.False(st.DropTaskOnWorker(context.Background(), in))
		})

		s.Run("drop success", func() {
			in := mocks.NewMockDataNodeClient(s.T())
			in.EXPECT().DropJobsV2(mock.Anything, mock.Anything).Return(&commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "",
			}, nil)

			s.True(st.DropTaskOnWorker(context.Background(), in))
		})
	})

	s.Run("SetJobInfo", func() {
		st.taskInfo = &workerpb.StatsResult{
			TaskID:       s.taskID,
			State:        indexpb.JobState_JobStateFinished,
			FailReason:   "",
			CollectionID: collID,
			PartitionID:  partID,
			SegmentID:    s.segID + 1,
			Channel:      "ch1",
			InsertLogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{{LogID: 1000}, {LogID: 1002}},
				},
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{{LogID: 1001}, {LogID: 1003}},
				},
			},
			StatsLogs: []*datapb.FieldBinlog{
				{
					FieldID: 100,
					Binlogs: []*datapb.Binlog{{LogID: 1004}},
				},
			},
			TextStatsLogs: map[int64]*datapb.TextIndexStats{
				101: {
					FieldID:    101,
					Version:    1,
					Files:      []string{"file1", "file2"},
					LogSize:    100,
					MemorySize: 100,
				},
			},
			NumRows: 65500,
		}

		s.Run("set target segment failed", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			s.mt.catalog = catalog
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))
			s.Error(st.SetJobInfo(s.mt))
		})

		s.Run("update stats task failed", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			s.mt.catalog = catalog
			s.mt.statsTaskMeta.catalog = catalog
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(fmt.Errorf("mock error"))

			s.Error(st.SetJobInfo(s.mt))
		})

		s.Run("normal case", func() {
			catalog := catalogmocks.NewDataCoordCatalog(s.T())
			catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil)
			s.mt.catalog = catalog
			s.mt.statsTaskMeta.catalog = catalog
			updateStateOp := UpdateStatusOperator(s.segID, commonpb.SegmentState_Flushed)
			err := s.mt.UpdateSegmentsInfo(context.TODO(), updateStateOp)
			s.NoError(err)
			catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil)

			s.NoError(st.SetJobInfo(s.mt))
			s.NotNil(s.mt.GetHealthySegment(context.TODO(), s.targetID))
			t, ok := s.mt.statsTaskMeta.tasks.Get(s.taskID)
			s.True(ok)
			s.Equal(indexpb.JobState_JobStateFinished, t.GetState())
			s.Equal(datapb.SegmentLevel_L2, s.mt.GetHealthySegment(context.TODO(), s.targetID).GetLevel())
		})
	})
}
