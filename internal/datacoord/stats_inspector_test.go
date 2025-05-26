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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type statsInspectorSuite struct {
	suite.Suite

	ctx       context.Context
	cancel    context.CancelFunc
	mt        *meta
	alloc     *allocator.MockAllocator
	catalog   *mocks.DataCoordCatalog
	cluster   session.Cluster
	scheduler task.GlobalScheduler
	inspector *statsInspector
}

func Test_statsInspectorSuite(t *testing.T) {
	suite.Run(t, new(statsInspectorSuite))
}

func (s *statsInspectorSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	Params.Save(Params.DataCoordCfg.TaskCheckInterval.Key, "1")
	Params.Save(Params.DataCoordCfg.GCInterval.Key, "1")
	Params.Save(Params.DataCoordCfg.EnableStatsTask.Key, "true")

	s.alloc = allocator.NewMockAllocator(s.T())
	var idCounter int64 = 1000
	s.alloc.EXPECT().AllocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		idCounter++
		return idCounter, nil
	}).Maybe()

	s.catalog = mocks.NewDataCoordCatalog(s.T())
	s.catalog.EXPECT().SaveStatsTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().DropStatsTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.catalog.EXPECT().ListStatsTasks(mock.Anything).Return([]*indexpb.StatsTask{}, nil).Maybe()

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(1, &collectionInfo{
		ID: 1,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "pk",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  101,
					Name:     "var",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key: "enable_match", Value: "true",
						},
						{
							Key: "enable_analyzer", Value: "true",
						},
					},
				},
			},
		},
	})

	s.mt = &meta{
		collections: collections,
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           10,
						CollectionID: 1,
						PartitionID:  2,
						IsSorted:     false,
						State:        commonpb.SegmentState_Flushed,
						NumOfRows:    1000,
						MaxRowNum:    2000,
						Level:        2,
					},
				},
				20: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           20,
						CollectionID: 1,
						PartitionID:  2,
						IsSorted:     true,
						State:        commonpb.SegmentState_Flushed,
						NumOfRows:    1000,
						MaxRowNum:    2000,
						Level:        2,
					},
				},
			},
			secondaryIndexes: segmentInfoIndexes{
				coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
					1: {
						10: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           10,
								CollectionID: 1,
								PartitionID:  2,
								IsSorted:     false,
								State:        commonpb.SegmentState_Flushed,
								NumOfRows:    1000,
								MaxRowNum:    2000,
								Level:        2,
							},
						},
						20: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           20,
								CollectionID: 1,
								PartitionID:  2,
								IsSorted:     true,
								State:        commonpb.SegmentState_Flushed,
								NumOfRows:    1000,
								MaxRowNum:    2000,
								Level:        2,
							},
						},
					},
				},
			},
		},
		statsTaskMeta: &statsTaskMeta{
			ctx:             s.ctx,
			catalog:         s.catalog,
			keyLock:         lock.NewKeyLock[UniqueID](),
			tasks:           typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask](),
			segmentID2Tasks: typeutil.NewConcurrentMap[string, *indexpb.StatsTask](),
		},
	}
	s.cluster = session.NewMockCluster(s.T())

	gs := task.NewMockGlobalScheduler(s.T())
	gs.EXPECT().Enqueue(mock.Anything).Return().Maybe()
	gs.EXPECT().AbortAndRemoveTask(mock.Anything).Return().Maybe()
	s.scheduler = gs

	s.inspector = newStatsInspector(
		s.ctx,
		s.mt,
		s.scheduler,
		s.alloc,
		nil,
		nil,
		newIndexEngineVersionManager(),
	)
}

func (s *statsInspectorSuite) TearDownTest() {
	s.cancel()
}

func (s *statsInspectorSuite) TestStart() {
	s.inspector.Start()
	time.Sleep(10 * time.Millisecond) // Give goroutines some time to start

	s.inspector.Stop()
	s.False(s.inspector.ctx.Done() == nil, "Context should be cancelled")
}

func (s *statsInspectorSuite) TestSubmitStatsTask() {
	// Test submitting a valid stats task
	err := s.inspector.SubmitStatsTask(10, 10, indexpb.StatsSubJob_Sort, true)
	s.NoError(err)

	// Test submitting a task for non-existent segment
	err = s.inspector.SubmitStatsTask(999, 999, indexpb.StatsSubJob_Sort, true)
	s.Error(err)
	s.True(errors.Is(err, merr.ErrSegmentNotFound), "Error should be ErrSegmentNotFound")

	s.mt.statsTaskMeta.tasks.Insert(1001, &indexpb.StatsTask{
		TaskID:     1001,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
	})
	s.mt.statsTaskMeta.segmentID2Tasks.Insert("10-Sort", &indexpb.StatsTask{
		TaskID:     1001,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
	})

	// Simulate duplicate task error
	err = s.inspector.SubmitStatsTask(10, 10, indexpb.StatsSubJob_Sort, true)
	s.NoError(err) // Duplicate tasks are handled as success
}

func (s *statsInspectorSuite) TestGetStatsTask() {
	// Set up mock task
	s.mt.statsTaskMeta.tasks.Insert(1002, &indexpb.StatsTask{
		TaskID:     1002,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
	})
	s.mt.statsTaskMeta.segmentID2Tasks.Insert("10-Sort", &indexpb.StatsTask{
		TaskID:     1002,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
	})

	// Test retrieving task
	task := s.inspector.GetStatsTask(10, indexpb.StatsSubJob_Sort)
	s.NotNil(task)
	s.Equal(int64(1002), task.TaskID)
	s.Equal(indexpb.JobState_JobStateInProgress, task.State)
}

func (s *statsInspectorSuite) TestDropStatsTask() {
	// Set up mock task
	s.mt.statsTaskMeta.tasks.Insert(1003, &indexpb.StatsTask{
		TaskID:     1003,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
	})
	s.mt.statsTaskMeta.segmentID2Tasks.Insert("10-Sort", &indexpb.StatsTask{
		TaskID:     1003,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
	})

	// Test dropping task
	err := s.inspector.DropStatsTask(10, indexpb.StatsSubJob_Sort)
	s.NoError(err)

	// Test dropping non-existent task
	err = s.inspector.DropStatsTask(999, indexpb.StatsSubJob_Sort)
	s.NoError(err) // Non-existent tasks should return success
}

func (s *statsInspectorSuite) TestTriggerSortStatsTask() {
	// Test triggering sort stats task
	s.inspector.triggerSortStatsTask()

	// Verify AllocID was called
	s.alloc.AssertCalled(s.T(), "AllocID", mock.Anything)
}

func (s *statsInspectorSuite) TestCreateSortStatsTaskForSegment() {
	// Get a test segment
	segment := s.mt.segments.segments[10]

	// Test creating sort stats task for segment
	s.inspector.createSortStatsTaskForSegment(segment)

	// Verify AllocID was called
	s.alloc.AssertCalled(s.T(), "AllocID", mock.Anything)
}

func (s *statsInspectorSuite) TestTriggerTextStatsTask() {
	// Set up a sorted segment without text index
	segment := s.mt.segments.segments[20]
	segment.IsSorted = true
	segment.TextStatsLogs = nil

	// Test triggering text index stats task
	s.inspector.triggerTextStatsTask()

	// Verify task creation
	s.alloc.AssertCalled(s.T(), "AllocID", mock.Anything)
}

func (s *statsInspectorSuite) TestTriggerBM25StatsTask() {
	// BM25 functionality is disabled in current version
	s.inspector.triggerBM25StatsTask()

	// No tasks should be created
	// Because enableBM25() returns false
}

func (s *statsInspectorSuite) TestTriggerStatsTaskLoop() {
	// Simulate short-term triggerStatsTaskLoop execution
	s.inspector.loopWg.Add(1)
	go s.inspector.triggerStatsTaskLoop()

	// Wait for goroutine to execute
	time.Sleep(1500 * time.Millisecond)

	// Send a segment ID to the channel
	getStatsTaskChSingleton() <- 10

	// Wait for processing
	time.Sleep(500 * time.Millisecond)

	// Stop the loop
	s.cancel()
	s.inspector.loopWg.Wait()
}

func (s *statsInspectorSuite) TestCleanupStatsTasksLoop() {
	// Simulate short-term cleanupStatsTasksLoop execution
	s.inspector.loopWg.Add(1)
	go s.inspector.cleanupStatsTasksLoop()

	// Wait for GC to execute
	time.Sleep(1500 * time.Millisecond)

	// Stop the loop
	s.cancel()
	s.inspector.loopWg.Wait()
}

func (s *statsInspectorSuite) TestReloadFromMeta() {
	// Set up some existing tasks
	s.mt.statsTaskMeta.tasks.Insert(1005, &indexpb.StatsTask{
		TaskID:     1005,
		SegmentID:  10,
		SubJobType: indexpb.StatsSubJob_Sort,
		State:      indexpb.JobState_JobStateInProgress,
	})

	// Test reloading
	s.inspector.reloadFromMeta()
}

func (s *statsInspectorSuite) TestNeedDoTextIndex() {
	// Test case when text index is needed
	segment := s.mt.segments.segments[20]
	segment.IsSorted = true
	result := needDoTextIndex(segment, []int64{101})
	s.True(result, "Segment should need text index")

	// Test case when text index already exists
	segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{
		101: {},
	}
	result = needDoTextIndex(segment, []int64{101})
	s.False(result, "Segment should not need text index")

	// Test case with unsorted segment
	segment.IsSorted = false
	result = needDoTextIndex(segment, []int64{101})
	s.False(result, "Unsorted segment should not need text index")
}

func (s *statsInspectorSuite) TestEnableBM25() {
	// Test if BM25 is enabled
	result := s.inspector.enableBM25()
	s.False(result, "BM25 should be disabled by default")
}
