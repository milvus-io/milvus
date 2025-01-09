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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func TestClusteringCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionTaskSuite))
}

type ClusteringCompactionTaskSuite struct {
	suite.Suite

	mockID           atomic.Int64
	mockAlloc        *NMockAllocator
	meta             *meta
	mockSessMgr      *MockSessionManager
	handler          *NMockHandler
	session          *MockSessionManager
	analyzeScheduler *taskScheduler
}

func (s *ClusteringCompactionTaskSuite) SetupTest() {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(storage.RootPath(""))
	catalog := datacoord.NewCatalog(NewMetaMemoryKV(), "", "")
	meta, err := newMeta(ctx, catalog, cm)
	s.NoError(err)
	s.meta = meta

	s.mockSessMgr = NewMockSessionManager(s.T())

	s.mockID.Store(time.Now().UnixMilli())
	s.mockAlloc = NewNMockAllocator(s.T())
	s.mockAlloc.EXPECT().allocN(mock.Anything).RunAndReturn(func(x int64) (int64, int64, error) {
		start := s.mockID.Load()
		end := s.mockID.Add(int64(x))
		return start, end, nil
	}).Maybe()
	s.mockAlloc.EXPECT().allocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		end := s.mockID.Add(1)
		return end, nil
	}).Maybe()

	s.handler = NewNMockHandler(s.T())
	s.handler.EXPECT().GetCollection(mock.Anything, mock.Anything).Return(&collectionInfo{}, nil).Maybe()

	s.session = NewMockSessionManager(s.T())

	scheduler := newTaskScheduler(ctx, s.meta, nil, cm, newIndexEngineVersionManager(), nil)
	s.analyzeScheduler = scheduler
}

func (s *ClusteringCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *ClusteringCompactionTaskSuite) TestClusteringCompactionSegmentMetaChange() {
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:    101,
			State: commonpb.SegmentState_Flushed,
			Level: datapb.SegmentLevel_L1,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    102,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			PartitionStatsVersion: 10000,
		},
	})
	s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.session.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil)

	task := s.generateBasicTask(false)

	err := task.processPipelining()
	s.NoError(err)

	seg11 := s.meta.GetSegment(101)
	s.Equal(datapb.SegmentLevel_L1, seg11.Level)
	seg21 := s.meta.GetSegment(102)
	s.Equal(datapb.SegmentLevel_L2, seg21.Level)
	s.Equal(int64(10000), seg21.PartitionStatsVersion)

	task.updateAndSaveTaskMeta(setResultSegments([]int64{103, 104}))
	// fake some compaction result segment
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    103,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			LastLevel:             datapb.SegmentLevel_L1,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})
	s.meta.AddSegment(context.TODO(), &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                    104,
			State:                 commonpb.SegmentState_Flushed,
			Level:                 datapb.SegmentLevel_L2,
			LastLevel:             datapb.SegmentLevel_L1,
			CreatedByCompaction:   true,
			PartitionStatsVersion: 10001,
		},
	})

	s.session.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil)

	err = task.doClean()
	s.NoError(err)

	s.Run("v2.4.x", func() {
		// fake some compaction result segment
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:        101,
				State:     commonpb.SegmentState_Dropped,
				LastLevel: datapb.SegmentLevel_L1,
				Level:     datapb.SegmentLevel_L2,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Dropped,
				LastLevel:             datapb.SegmentLevel_L2,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})

		// fake some compaction result segment
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    103,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				CreatedByCompaction:   true,
				PartitionStatsVersion: 10001,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    104,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				CreatedByCompaction:   true,
				PartitionStatsVersion: 10001,
			},
		})

		task := s.generateBasicTask(false)
		task.sessions = s.session
		s.session.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil)
		task.InputSegments = []int64{101, 102}
		task.ResultSegments = []int64{103, 104}

		task.Clean()

		seg12 := s.meta.GetSegment(101)
		s.Equal(datapb.SegmentLevel_L1, seg12.Level)
		seg22 := s.meta.GetSegment(102)
		s.Equal(datapb.SegmentLevel_L2, seg22.Level)
		s.Equal(int64(10000), seg22.PartitionStatsVersion)

		seg32 := s.meta.GetSegment(103)
		s.Equal(datapb.SegmentLevel_L1, seg32.Level)
		s.Equal(int64(0), seg32.PartitionStatsVersion)
		s.Equal(commonpb.SegmentState_Flushed, seg32.GetState())
		seg42 := s.meta.GetSegment(104)
		s.Equal(datapb.SegmentLevel_L1, seg42.Level)
		s.Equal(int64(0), seg42.PartitionStatsVersion)
		s.Equal(commonpb.SegmentState_Flushed, seg42.GetState())
	})
}

func (s *ClusteringCompactionTaskSuite) generateBasicTask(vectorClusteringKey bool) *clusteringCompactionTask {
	schema := ConstructClusteringSchema("TestClusteringCompactionTask", 32, true, vectorClusteringKey)
	var pk *schemapb.FieldSchema
	if vectorClusteringKey {
		pk = &schemapb.FieldSchema{
			FieldID:         101,
			Name:            FloatVecField,
			IsPrimaryKey:    false,
			DataType:        schemapb.DataType_FloatVector,
			IsClusteringKey: true,
		}
	} else {
		pk = &schemapb.FieldSchema{
			FieldID:         100,
			Name:            Int64Field,
			IsPrimaryKey:    true,
			DataType:        schemapb.DataType_Int64,
			AutoID:          true,
			IsClusteringKey: true,
		}
	}

	compactionTask := &datapb.CompactionTask{
		PlanID:             1,
		TriggerID:          19530,
		CollectionID:       1,
		PartitionID:        10,
		Type:               datapb.CompactionType_ClusteringCompaction,
		NodeID:             1,
		State:              datapb.CompactionTaskState_pipelining,
		Schema:             schema,
		ClusteringKeyField: pk,
		InputSegments:      []int64{101, 102},
		ResultSegments:     []int64{1000, 1100},
	}

	task := newClusteringCompactionTask(compactionTask, s.meta, s.session, s.handler, s.analyzeScheduler)
	task.maxRetryTimes = 0
	return task
}

func (s *ClusteringCompactionTaskSuite) TestProcessRetryLogic() {
	task := s.generateBasicTask(false)
	task.maxRetryTimes = 3
	// process pipelining fail
	s.Equal(false, task.Process())
	s.Equal(int32(1), task.RetryTimes)
	s.Equal(false, task.Process())
	s.Equal(int32(2), task.RetryTimes)
	s.Equal(false, task.Process())
	s.Equal(int32(3), task.RetryTimes)
	s.Equal(datapb.CompactionTaskState_pipelining, task.GetState())
	s.True(task.Process())
	s.Equal(int32(0), task.RetryTimes)
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())
}

func (s *ClusteringCompactionTaskSuite) TestProcessPipelining() {
	s.Run("process pipelining fail, segment not found", func() {
		task := s.generateBasicTask(false)
		task.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_pipelining))
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_failed, task.GetState())
	})

	s.Run("pipelining fail, no datanode slot", func() {
		task := s.generateBasicTask(false)
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(merr.WrapErrDataNodeSlotExhausted())
		task.State = datapb.CompactionTaskState_pipelining
		s.False(task.Process())
		s.Equal(int64(NullNodeID), task.GetNodeID())
	})

	s.Run("process succeed, scalar clustering key", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_pipelining
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		task.State = datapb.CompactionTaskState_pipelining
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_executing, task.GetState())
	})

	s.Run("process succeed, vector clustering key", func() {
		task := s.generateBasicTask(true)
		task.State = datapb.CompactionTaskState_pipelining
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		task.State = datapb.CompactionTaskState_pipelining
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_analyzing, task.GetState())
	})
}

func (s *ClusteringCompactionTaskSuite) TestProcessExecuting() {
	s.Run("process executing, get compaction result fail", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_executing
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(nil, merr.WrapErrNodeNotFound(1)).Once()
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_pipelining, task.GetState())
	})

	s.Run("process executing, compaction result not ready", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_executing
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(nil, nil).Once()
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_executing, task.GetState())
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_executing,
		}, nil).Once()
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_executing, task.GetState())
	})

	s.Run("process executing, scalar clustering key, compaction result ready", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_executing
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 1000,
				},
				{
					SegmentID: 1001,
				},
			},
		}, nil).Once()
		s.session.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(nil).Once()
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_indexing, task.GetState())
	})

	s.Run("process executing, compaction result ready", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_executing
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_completed,
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 1000,
				},
				{
					SegmentID: 1001,
				},
			},
		}, nil).Once()
		// DropCompactionPlan fail
		s.session.EXPECT().DropCompactionPlan(mock.Anything, mock.Anything).Return(merr.WrapErrNodeNotFound(1)).Once()
		s.Equal(false, task.Process())
		s.Equal(datapb.CompactionTaskState_indexing, task.GetState())
	})

	s.Run("process executing, compaction result timeout", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_executing
		task.StartTime = time.Now().Unix()
		task.TimeoutInSeconds = 1
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
			State: datapb.CompactionTaskState_executing,
			Segments: []*datapb.CompactionSegment{
				{
					SegmentID: 1000,
				},
				{
					SegmentID: 1001,
				},
			},
		}, nil).Once()

		time.Sleep(time.Second * 1)
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_timeout, task.GetState())
	})
}

func (s *ClusteringCompactionTaskSuite) TestProcessExecutingState() {
	task := s.generateBasicTask(false)
	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_failed,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_failed,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_pipelining,
	}, nil).Once()
	s.NoError(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_completed,
	}, nil).Once()
	s.Error(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())

	s.session.EXPECT().GetCompactionPlanResult(mock.Anything, mock.Anything).Return(&datapb.CompactionPlanResult{
		State: datapb.CompactionTaskState_completed,
		Segments: []*datapb.CompactionSegment{
			{
				SegmentID: 1000,
			},
			{
				SegmentID: 1001,
			},
		},
	}, nil).Once()
	s.Error(task.processExecuting())
	s.Equal(datapb.CompactionTaskState_failed, task.GetState())
}

func (s *ClusteringCompactionTaskSuite) TestProcessIndexingState() {
	s.Run("collection has no index", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_indexing
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_completed, task.GetState())
	})

	s.Run("collection has index, segment is not indexed", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_indexing
		index := &model.Index{
			CollectionID: 1,
			IndexID:      3,
		}
		err := s.meta.indexMeta.CreateIndex(index)
		s.NoError(err)

		s.False(task.Process())
		s.Equal(datapb.CompactionTaskState_indexing, task.GetState())
	})

	s.Run("collection has index, segment indexed", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_indexing
		index := &model.Index{
			CollectionID: 1,
			IndexID:      3,
		}
		err := s.meta.indexMeta.CreateIndex(index)
		s.NoError(err)

		s.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			IndexID:      3,
			SegmentID:    1000,
			CollectionID: 1,
			IndexState:   commonpb.IndexState_Finished,
		})
		s.meta.indexMeta.updateSegmentIndex(&model.SegmentIndex{
			IndexID:      3,
			SegmentID:    1100,
			CollectionID: 1,
			IndexState:   commonpb.IndexState_Finished,
		})

		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_completed, task.GetState())
	})
}

func (s *ClusteringCompactionTaskSuite) TestProcessAnalyzingState() {
	s.Run("analyze task not found", func() {
		task := s.generateBasicTask(false)
		task.updateAndSaveTaskMeta(setState(datapb.CompactionTaskState_analyzing))
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_failed, task.GetState())
	})

	s.Run("analyze task failed", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_analyzing
		task.AnalyzeTaskID = 7
		t := &indexpb.AnalyzeTask{
			CollectionID: task.CollectionID,
			PartitionID:  task.PartitionID,
			FieldID:      task.ClusteringKeyField.FieldID,
			SegmentIDs:   task.InputSegments,
			TaskID:       7,
			State:        indexpb.JobState_JobStateFailed,
		}
		s.meta.analyzeMeta.AddAnalyzeTask(t)
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_failed, task.GetState())
	})

	s.Run("analyze task fake finish, vector not support", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_analyzing
		task.AnalyzeTaskID = 7
		t := &indexpb.AnalyzeTask{
			CollectionID:  task.CollectionID,
			PartitionID:   task.PartitionID,
			FieldID:       task.ClusteringKeyField.FieldID,
			SegmentIDs:    task.InputSegments,
			TaskID:        7,
			State:         indexpb.JobState_JobStateFinished,
			CentroidsFile: "",
		}
		s.meta.analyzeMeta.AddAnalyzeTask(t)
		s.True(task.Process())
		s.Equal(datapb.CompactionTaskState_failed, task.GetState())
	})

	s.Run("analyze task finished", func() {
		task := s.generateBasicTask(false)
		task.State = datapb.CompactionTaskState_analyzing
		task.AnalyzeTaskID = 7
		t := &indexpb.AnalyzeTask{
			CollectionID:  task.CollectionID,
			PartitionID:   task.PartitionID,
			FieldID:       task.ClusteringKeyField.FieldID,
			SegmentIDs:    task.InputSegments,
			TaskID:        7,
			State:         indexpb.JobState_JobStateFinished,
			CentroidsFile: "somewhere",
		}
		s.meta.analyzeMeta.AddAnalyzeTask(t)
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:    101,
				State: commonpb.SegmentState_Flushed,
				Level: datapb.SegmentLevel_L1,
			},
		})
		s.meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:                    102,
				State:                 commonpb.SegmentState_Flushed,
				Level:                 datapb.SegmentLevel_L2,
				PartitionStatsVersion: 10000,
			},
		})
		s.session.EXPECT().Compaction(mock.Anything, mock.Anything, mock.Anything).Return(nil)

		s.False(task.Process())
		s.Equal(datapb.CompactionTaskState_executing, task.GetState())
	})
}

// fix: https://github.com/milvus-io/milvus/issues/35110
func (s *ClusteringCompactionTaskSuite) TestCompleteTask() {
	task := s.generateBasicTask(false)
	task.completeTask()
	partitionStats := s.meta.GetPartitionStatsMeta().GetPartitionStats(task.GetCollectionID(), task.GetPartitionID(), task.GetChannel(), task.GetPlanID())
	s.True(partitionStats.GetCommitTime() > time.Now().Add(-2*time.Second).Unix())
}

const (
	Int64Field    = "int64Field"
	FloatVecField = "floatVecField"
)

func ConstructClusteringSchema(collection string, dim int, autoID bool, vectorClusteringKey bool, fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	// if fields are specified, construct it
	if len(fields) > 0 {
		return &schemapb.CollectionSchema{
			Name:   collection,
			AutoID: autoID,
			Fields: fields,
		}
	}

	// if no field is specified, use default
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       autoID,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}

	if vectorClusteringKey {
		pk.IsClusteringKey = true
	} else {
		fVec.IsClusteringKey = true
	}

	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}
}
