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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	taskcommon "github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

type mockeyStatsSchedulerTarget struct {
	task.GlobalScheduler
}

func Test_statsInspectorSuite(t *testing.T) {
	suite.Run(t, new(statsInspectorSuite))
}

func (s *statsInspectorSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	Params.Save(Params.DataCoordCfg.TaskCheckInterval.Key, "1")
	Params.Save(Params.DataCoordCfg.GCInterval.Key, "1")

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
	collections.Insert(2, &collectionInfo{
		ID: 2,
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:       200,
					Name:          "pk",
					DataType:      schemapb.DataType_Int64,
					Nullable:      true,
					ExternalField: "pk_col",
				},
				{
					FieldID:       201,
					Name:          "var",
					DataType:      schemapb.DataType_VarChar,
					Nullable:      true,
					ExternalField: "var_col",
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key: "enable_match", Value: "true",
						},
						{
							Key: "enable_analyzer", Value: "true",
						},
					},
				},
				{
					FieldID:       202,
					Name:          "json",
					DataType:      schemapb.DataType_JSON,
					Nullable:      true,
					ExternalField: "json_col",
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
				30: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           30,
						CollectionID: 1,
						PartitionID:  2,
						State:        commonpb.SegmentState_Flushing,
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
						30: {
							SegmentInfo: &datapb.SegmentInfo{
								ID:           30,
								CollectionID: 1,
								PartitionID:  2,
								State:        commonpb.SegmentState_Flushing,
								NumOfRows:    1000,
								MaxRowNum:    2000,
								Level:        2,
							},
						},
					},
				},
				channel2Segments: map[string]map[UniqueID]*SegmentInfo{},
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
	gs.EXPECT().GetPendingTaskCount(taskcommon.Stats).Return(0).Maybe()
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

func (s *statsInspectorSuite) putExternalSegment(segmentID UniqueID, sorted bool, storageVersion int64, manifestPath string) {
	s.mt.segments.SetSegment(segmentID, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   2,
			PartitionID:    3,
			InsertChannel:  "by-dev-rootcoord-dml-channel",
			IsSorted:       sorted,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      1000,
			MaxRowNum:      2000,
			Level:          datapb.SegmentLevel_L1,
			StorageVersion: storageVersion,
			ManifestPath:   manifestPath,
		},
	})
}

func (s *statsInspectorSuite) putExternalStatsTask(taskID, segmentID UniqueID, subJobType indexpb.StatsSubJob) {
	s.mt.statsTaskMeta.tasks.Insert(taskID, &indexpb.StatsTask{
		CollectionID: 2,
		TaskID:       taskID,
		SegmentID:    segmentID,
		SubJobType:   subJobType,
		State:        indexpb.JobState_JobStateInProgress,
	})
}

func (s *statsInspectorSuite) TestStart() {
	s.inspector.Start()
	time.Sleep(10 * time.Millisecond) // Give goroutines some time to start

	s.inspector.Stop()
	s.False(s.inspector.ctx.Done() == nil, "Context should be canceled")
}

func (s *statsInspectorSuite) TestTriggerTextStatsTaskFileResourcesByMode() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 7, Name: "dict", Path: "dict.jieba"},
	}
	collection := s.mt.GetCollection(1)
	collection.Schema.FileResourceIds = []int64{7}
	resourceBroker := broker.NewMockBroker(s.T())
	resourceBroker.EXPECT().GetFileResources(mock.Anything, int64(7)).Return(resources, nil).Once()
	s.mt.broker = resourceBroker

	for _, testCase := range []struct {
		name            string
		mode            string
		expectResources bool
	}{
		{name: "ref", mode: "ref", expectResources: true},
		{name: "sync", mode: "sync", expectResources: false},
	} {
		s.Run(testCase.name, func() {
			paramtable.Get().Save(Params.CommonCfg.DNFileResourceMode.Key, testCase.mode)
			s.T().Cleanup(func() {
				paramtable.Get().Reset(Params.CommonCfg.DNFileResourceMode.Key)
			})
			s.mt.statsTaskMeta.tasks = typeutil.NewConcurrentMap[int64, *indexpb.StatsTask]()
			s.mt.statsTaskMeta.segmentID2Tasks = typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()

			s.inspector.triggerTextStatsTask()

			task := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(20, indexpb.StatsSubJob_TextIndexJob)
			s.Require().NotNil(task)
			if testCase.expectResources {
				s.Equal(resources, task.GetFileResources())
			} else {
				s.Empty(task.GetFileResources())
			}
		})
	}
}

func (s *statsInspectorSuite) TestSubmitStatsTask() {
	// Test submitting a valid stats task
	err := s.inspector.SubmitStatsTask(10, 10, indexpb.StatsSubJob_Sort, true, nil)
	s.NoError(err)

	// Test submitting a task for non-existent segment
	err = s.inspector.SubmitStatsTask(999, 999, indexpb.StatsSubJob_Sort, true, nil)
	s.Error(err)
	s.True(errors.Is(err, merr.ErrSegmentNotFound), "Error should be ErrSegmentNotFound")

	// Duplicate tasks are skipped before checking the scheduler or allocating a task ID.
	s.inspector.scheduler = task.NewMockGlobalScheduler(s.T())
	s.inspector.allocator = allocator.NewMockAllocator(s.T())
	err = s.inspector.SubmitStatsTask(10, 10, indexpb.StatsSubJob_Sort, true, nil)
	s.NoError(err)
}

func (s *statsInspectorSuite) TestSubmitStatsTaskPendingLimit() {
	pendingTaskLimit := Params.DataCoordCfg.StatsTaskPendingLimit.GetAsInt()

	s.Run("allow at limit", func() {
		scheduler := task.NewMockGlobalScheduler(s.T())
		scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats).Return(pendingTaskLimit).Once()
		scheduler.EXPECT().Enqueue(mock.Anything).Return().Once()
		s.inspector.scheduler = scheduler

		err := s.inspector.SubmitStatsTask(20, 20, indexpb.StatsSubJob_TextIndexJob, true, nil)
		s.NoError(err)
		s.NotNil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(20, indexpb.StatsSubJob_TextIndexJob))
	})

	s.Run("skip over limit", func() {
		scheduler := task.NewMockGlobalScheduler(s.T())
		scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats).Return(pendingTaskLimit + 1).Once()
		s.inspector.scheduler = scheduler
		// A strict allocator asserts the admission check runs before task ID allocation.
		s.inspector.allocator = allocator.NewMockAllocator(s.T())

		err := s.inspector.SubmitStatsTask(10, 10, indexpb.StatsSubJob_TextIndexJob, true, nil)
		s.NoError(err)
		s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(10, indexpb.StatsSubJob_TextIndexJob))
	})
}

// A segment whose task is already in meta must be dropped by the segment
// selector, so a repeated tick never reaches SubmitStatsTask at all. Asserting
// on side effects (no ID allocated, nothing enqueued) would not prove this:
// SubmitStatsTask's own duplicate guard produces exactly the same side effects,
// so the assertion has to be on whether SubmitStatsTask is called.
func (s *statsInspectorSuite) TestTriggerTextStatsTaskSkipsSubmittedSegments() {
	submitted := make([]UniqueID, 0)
	mockSubmit := mockey.Mock((*statsInspector).SubmitStatsTask).To(
		func(_ *statsInspector, originSegmentID, _ int64, _ indexpb.StatsSubJob, _ bool, _ []*internalpb.FileResourceInfo) error {
			submitted = append(submitted, originSegmentID)
			return nil
		}).Build()
	defer mockSubmit.UnPatch()

	// Without a task in meta the segment is a candidate.
	s.inspector.triggerTextStatsTask()
	s.Equal([]UniqueID{20}, submitted)

	// Once the task is recorded the selector must drop the segment.
	s.NoError(s.mt.statsTaskMeta.AddStatsTask(&indexpb.StatsTask{
		CollectionID: 1,
		TaskID:       1001,
		SegmentID:    20,
		SubJobType:   indexpb.StatsSubJob_TextIndexJob,
	}))
	submitted = submitted[:0]
	s.inspector.triggerTextStatsTask()
	s.Empty(submitted)
}

// The sub-jobs share one admission budget, so the trigger that runs first wins
// it. Consecutive rounds must not give the same sub-job first claim.
func (s *statsInspectorSuite) TestTriggerStatsTasksAlternatesSubJobOrder() {
	s.putExternalSegment(220, false, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/2/3/220", 1))

	order := make([]indexpb.StatsSubJob, 0)
	mockSubmit := mockey.Mock((*statsInspector).SubmitStatsTask).To(
		func(_ *statsInspector, _, _ int64, subJobType indexpb.StatsSubJob, _ bool, _ []*internalpb.FileResourceInfo) error {
			order = append(order, subJobType)
			return nil
		}).Build()
	defer mockSubmit.UnPatch()

	// Both sub-jobs have candidates, so the first entry is the one that would
	// have claimed the budget had it been exhausted.
	s.inspector.triggerStatsTasks(0)
	s.Require().NotEmpty(order)
	s.Equal(indexpb.StatsSubJob_TextIndexJob, order[0])
	s.Contains(order, indexpb.StatsSubJob_JsonKeyIndexJob)

	order = order[:0]
	s.inspector.triggerStatsTasks(1)
	s.Require().NotEmpty(order)
	s.Equal(indexpb.StatsSubJob_JsonKeyIndexJob, order[0])
	s.Contains(order, indexpb.StatsSubJob_TextIndexJob)
}

// jsonShreddingTriggerCount is deprecated, but 0 used to disable JSON key index
// submission outright and must keep doing so instead of silently turning
// shredding back on after an upgrade.
func (s *statsInspectorSuite) TestTriggerJSONKeyIndexStatsTaskHonorsDeprecatedZero() {
	segmentID := UniqueID(230)
	s.putExternalSegment(segmentID, false, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/2/3/230", 1))

	// Sanity check: the segment is a candidate while the deprecated key is unset.
	s.inspector.triggerJSONKeyIndexStatsTask()
	s.NotNil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob))
	s.NoError(s.mt.statsTaskMeta.DropStatsTask(s.ctx,
		s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob).GetTaskID()))

	Params.Save(Params.DataCoordCfg.JSONStatsTriggerCount.Key, "0")
	defer Params.Reset(Params.DataCoordCfg.JSONStatsTriggerCount.Key)
	s.inspector.allocator = allocator.NewMockAllocator(s.T())

	s.inspector.triggerJSONKeyIndexStatsTask()
	s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob))
}

// Every trigger loop must give up as soon as the scheduler is backlogged instead
// of walking the remaining collections. The call count is what proves it: the
// text and JSON loops each ask once for the first collection they look at and
// then return, while the BM25 loop returns before asking because BM25 is not
// docked yet. Walking on (continue) would ask once per collection instead.
func (s *statsInspectorSuite) TestTriggerStatsTaskStopsWhenSchedulerBacklogged() {
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats).
		Return(Params.DataCoordCfg.StatsTaskPendingLimit.GetAsInt() + 1).Times(2)
	s.inspector.scheduler = scheduler
	s.inspector.allocator = allocator.NewMockAllocator(s.T())

	s.putExternalSegment(210, false, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/2/3/210", 1))

	s.inspector.triggerTextStatsTask()
	s.inspector.triggerJSONKeyIndexStatsTask()
	s.inspector.triggerBM25StatsTask()

	s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(20, indexpb.StatsSubJob_TextIndexJob))
	s.Nil(s.mt.statsTaskMeta.GetStatsTaskBySegmentID(210, indexpb.StatsSubJob_JsonKeyIndexJob))
}

// The loop keeps submitting while the pending count is at the limit and stops on
// the first segment that would exceed it, mid-collection.
func (s *statsInspectorSuite) TestTriggerJSONKeyIndexStatsTaskStopsAtPendingLimit() {
	Params.Save(Params.DataCoordCfg.StatsTaskPendingLimit.Key, "1")
	defer Params.Reset(Params.DataCoordCfg.StatsTaskPendingLimit.Key)

	segmentIDs := []UniqueID{301, 302, 303, 304}
	for _, segmentID := range segmentIDs {
		s.putExternalSegment(segmentID, false, storage.StorageV3,
			packed.MarshalManifestPath(fmt.Sprintf("files/insert_log/2/3/%d", segmentID), 1))
	}

	pending := 0
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().GetPendingTaskCount(taskcommon.Stats).RunAndReturn(func(taskcommon.Type) int { return pending })
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(_ task.Task) { pending++ }).Return()
	s.inspector.scheduler = scheduler

	s.inspector.triggerJSONKeyIndexStatsTask()

	// pending 0 -> allowed, 1 -> allowed (== limit), 2 -> skipped (> limit).
	submitted := 0
	for _, segmentID := range segmentIDs {
		if s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob) != nil {
			submitted++
		}
	}
	s.Equal(2, submitted)
	s.Equal(2, pending)
}

func (s *statsInspectorSuite) TestSubmitStatsTaskSkipExternalCollection() {
	segmentID := UniqueID(200)
	s.putExternalSegment(segmentID, true, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/2/3/200", 1))

	err := s.inspector.SubmitStatsTask(segmentID, segmentID, indexpb.StatsSubJob_Sort, true, nil)
	s.NoError(err)
	sortTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_Sort)
	s.Nil(sortTask)

	err = s.inspector.SubmitStatsTask(segmentID, segmentID, indexpb.StatsSubJob_TextIndexJob, true, nil)
	s.NoError(err)
	textTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_TextIndexJob)
	s.NotNil(textTask)

	err = s.inspector.SubmitStatsTask(segmentID, segmentID, indexpb.StatsSubJob_JsonKeyIndexJob, true, nil)
	s.NoError(err)
	jsonTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob)
	s.NotNil(jsonTask)
}

func (s *statsInspectorSuite) TestSubmitStatsTaskSkipExternalJSONWithoutV3Manifest() {
	segmentID := UniqueID(205)
	s.putExternalSegment(segmentID, true, storage.StorageV2, "")

	err := s.inspector.SubmitStatsTask(segmentID, segmentID, indexpb.StatsSubJob_JsonKeyIndexJob, true, nil)
	s.NoError(err)
	jsonTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob)
	s.Nil(jsonTask)
}

func (s *statsInspectorSuite) TestTriggerJSONKeyIndexStatsTaskExternalCollectionUnsortedV3() {
	segmentID := UniqueID(201)
	s.putExternalSegment(segmentID, false, storage.StorageV3, packed.MarshalManifestPath("files/insert_log/2/3/201", 1))

	s.inspector.triggerJSONKeyIndexStatsTask()

	jsonTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_JsonKeyIndexJob)
	s.NotNil(jsonTask)
}

func (s *statsInspectorSuite) TestTriggerJSONKeyIndexStatsTaskExternalCollectionSkipsInvalidManifestSegments() {
	testCases := []struct {
		name           string
		segmentID      UniqueID
		storageVersion int64
		manifestPath   string
	}{
		{
			name:           "v3_empty_manifest",
			segmentID:      206,
			storageVersion: storage.StorageV3,
		},
		{
			name:           "v2_with_manifest",
			segmentID:      207,
			storageVersion: storage.StorageV2,
			manifestPath:   packed.MarshalManifestPath("files/insert_log/2/3/207", 1),
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		s.Run(testCase.name, func() {
			s.putExternalSegment(testCase.segmentID, false, testCase.storageVersion, testCase.manifestPath)

			s.inspector.triggerJSONKeyIndexStatsTask()

			jsonTask := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(testCase.segmentID, indexpb.StatsSubJob_JsonKeyIndexJob)
			s.Nil(jsonTask)
		})
	}
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

func (s *statsInspectorSuite) TestTriggerTextStatsTaskExternalCollection() {
	segmentID := UniqueID(200)
	seg := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            segmentID,
			CollectionID:  2,
			PartitionID:   3,
			InsertChannel: "by-dev-rootcoord-dml-channel",
			IsSorted:      false,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     1000,
			MaxRowNum:     2000,
			Level:         2,
		},
	}
	s.mt.segments.segments[segmentID] = seg
	s.mt.segments.secondaryIndexes.coll2Segments[2] = map[UniqueID]*SegmentInfo{
		segmentID: seg,
	}

	s.inspector.triggerTextStatsTask()

	task := s.mt.statsTaskMeta.GetStatsTaskBySegmentID(segmentID, indexpb.StatsSubJob_TextIndexJob)
	s.NotNil(task)
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
	enqueueCount := 0
	scheduler := &mockeyStatsSchedulerTarget{}
	mockEnqueue := mockey.Mock((*mockeyStatsSchedulerTarget).Enqueue).To(
		func(*mockeyStatsSchedulerTarget, task.Task) {
			enqueueCount++
		}).Build()
	defer mockEnqueue.UnPatch()
	s.inspector.scheduler = scheduler

	// Set up some existing tasks
	s.mt.statsTaskMeta.tasks.Insert(1005, &indexpb.StatsTask{
		CollectionID: 1,
		TaskID:       1005,
		SegmentID:    10,
		SubJobType:   indexpb.StatsSubJob_Sort,
		State:        indexpb.JobState_JobStateInProgress,
	})

	// Test reloading
	s.inspector.reloadFromMeta()
	s.Equal(1, enqueueCount)
}

func (s *statsInspectorSuite) TestReloadFromMetaEstimatesPerSubJobType() {
	// A recovered manifest-backed V3 segment carries no FieldBinlog arrays
	// (the catalog does not persist them), so recovered stats tasks keep the
	// Stats-derived whole segment size — doubled for the json key index job —
	// and recovery must not resolve collections through the handler
	// (rootcoord).
	const (
		collectionID = UniqueID(5)
		segmentID    = UniqueID(50)
		pkFieldID    = int64(500)
		jsonFieldID  = int64(501)
		numRows      = int64(1000)
		segmentSize  = int64(2 * 1024 * 1024 * 1024)
	)

	s.mt.collections.Insert(collectionID, &collectionInfo{
		ID: collectionID,
		Schema: &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{FieldID: jsonFieldID, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
		},
	})
	// no GetCollection expectation: any handler resolution fails the test
	s.inspector.handler = NewNMockHandler(s.T())

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             segmentID,
			CollectionID:   collectionID,
			PartitionID:    3,
			State:          commonpb.SegmentState_Flushed,
			NumOfRows:      numRows,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath("files/insert_log/5/3/50", 1),
			Stats:          &datapb.Statistics{InsertBinlogSize: segmentSize},
		},
	}
	s.mt.segments.SetSegment(segmentID, segment)

	for taskID, subJobType := range map[UniqueID]indexpb.StatsSubJob{
		2001: indexpb.StatsSubJob_JsonKeyIndexJob,
		2002: indexpb.StatsSubJob_Sort,
	} {
		s.mt.statsTaskMeta.tasks.Insert(taskID, &indexpb.StatsTask{
			CollectionID: collectionID,
			TaskID:       taskID,
			SegmentID:    segmentID,
			SubJobType:   subJobType,
			State:        indexpb.JobState_JobStateInProgress,
		})
	}

	slots := make(map[indexpb.StatsSubJob]int64)
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		st := t.(*statsTask)
		slots[st.GetSubJobType()] = t.GetTaskSlot()
	}).Return()
	s.inspector.scheduler = scheduler

	s.inspector.reloadFromMeta()

	// without binlogs to attribute, the json key index task keeps the whole
	// segment size doubled, and the sort task the whole segment size
	s.Equal(calculateStatsTaskSlot(segmentSize*2), slots[indexpb.StatsSubJob_JsonKeyIndexJob])
	s.Equal(calculateStatsTaskSlot(segmentSize), slots[indexpb.StatsSubJob_Sort])
}

func (s *statsInspectorSuite) TestReloadFromMetaSkipsCollectionResolutionWithoutProjection() {
	// Recovery reads collections from the local meta cache only; it must never
	// resolve one through the handler (rootcoord), whose cache-miss retries
	// would block Start().
	// no GetCollection expectation: any resolution attempt fails the test
	handler := NewNMockHandler(s.T())
	s.inspector.handler = handler

	s.mt.statsTaskMeta.tasks.Insert(1005, &indexpb.StatsTask{
		CollectionID: 1,
		TaskID:       1005,
		SegmentID:    10,
		SubJobType:   indexpb.StatsSubJob_Sort,
		State:        indexpb.JobState_JobStateInProgress,
	})

	slots := make([]int64, 0, 1)
	scheduler := task.NewMockGlobalScheduler(s.T())
	scheduler.EXPECT().Enqueue(mock.Anything).Run(func(t task.Task) {
		slots = append(slots, t.GetTaskSlot())
	}).Return()
	s.inspector.scheduler = scheduler

	s.inspector.reloadFromMeta()

	s.Require().Len(slots, 1)
	segment := s.mt.GetHealthySegment(s.ctx, 10)
	s.Equal(calculateStatsTaskSlot(s.inspector.estimateStatsTaskSize(nil, segment, indexpb.StatsSubJob_Sort)), slots[0])
}

func (s *statsInspectorSuite) TestReloadFromMetaExternalStatsTask() {
	testCases := []struct {
		name           string
		taskID         UniqueID
		segmentID      UniqueID
		subJobType     indexpb.StatsSubJob
		storageVersion int64
		manifestPath   string
		expectEnqueue  bool
	}{
		{
			name:           "json_v3_manifest",
			taskID:         1006,
			segmentID:      202,
			subJobType:     indexpb.StatsSubJob_JsonKeyIndexJob,
			storageVersion: storage.StorageV3,
			manifestPath:   packed.MarshalManifestPath("files/insert_log/2/3/202", 1),
			expectEnqueue:  true,
		},
		{
			name:           "json_v2_no_manifest",
			taskID:         1009,
			segmentID:      208,
			subJobType:     indexpb.StatsSubJob_JsonKeyIndexJob,
			storageVersion: storage.StorageV2,
			expectEnqueue:  true,
		},
		{
			name:           "json_v3_empty_manifest",
			taskID:         1010,
			segmentID:      209,
			subJobType:     indexpb.StatsSubJob_JsonKeyIndexJob,
			storageVersion: storage.StorageV3,
			expectEnqueue:  true,
		},
		{
			name:          "text",
			taskID:        1008,
			segmentID:     204,
			subJobType:    indexpb.StatsSubJob_TextIndexJob,
			expectEnqueue: true,
		},
		{
			name:           "unsupported_sort",
			taskID:         1007,
			segmentID:      203,
			subJobType:     indexpb.StatsSubJob_Sort,
			storageVersion: storage.StorageV3,
			manifestPath:   packed.MarshalManifestPath("files/insert_log/2/3/203", 1),
			expectEnqueue:  true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		s.Run(testCase.name, func() {
			enqueueCount := 0
			scheduler := &mockeyStatsSchedulerTarget{}
			mockEnqueue := mockey.Mock((*mockeyStatsSchedulerTarget).Enqueue).To(
				func(*mockeyStatsSchedulerTarget, task.Task) {
					enqueueCount++
				}).Build()
			defer mockEnqueue.UnPatch()
			s.inspector.scheduler = scheduler
			s.mt.statsTaskMeta.tasks = typeutil.NewConcurrentMap[UniqueID, *indexpb.StatsTask]()
			s.mt.statsTaskMeta.segmentID2Tasks = typeutil.NewConcurrentMap[string, *indexpb.StatsTask]()
			s.putExternalSegment(testCase.segmentID, false, testCase.storageVersion, testCase.manifestPath)
			s.putExternalStatsTask(testCase.taskID, testCase.segmentID, testCase.subJobType)

			s.inspector.reloadFromMeta()

			statsTask, ok := s.mt.statsTaskMeta.tasks.Get(testCase.taskID)
			s.True(ok)
			s.False(statsTask.GetCanRecycle())
			if testCase.expectEnqueue {
				s.Equal(1, enqueueCount)
			} else {
				s.Zero(enqueueCount)
			}
		})
	}
}

func (s *statsInspectorSuite) TestNeedDoTextIndex() {
	// Test case when text index is needed
	segment := s.mt.segments.segments[20]
	segment.IsSorted = true
	result := needDoTextIndex(segment, []int64{101}, false)
	s.True(result, "Segment should need text index")

	// Test case when text index already exists
	segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{
		101: {},
	}
	result = needDoTextIndex(segment, []int64{101}, false)
	s.False(result, "Segment should not need text index")

	// Test case with unsorted segment
	segment.IsSorted = false
	result = needDoTextIndex(segment, []int64{101}, false)
	s.False(result, "Unsorted segment should not need text index")

	result = needDoTextIndex(segment, []int64{101}, true)
	s.False(result, "Segment with existing text index should not need text index")

	segment.TextStatsLogs = nil
	result = needDoTextIndex(segment, []int64{101}, true)
	s.True(result, "Unsorted external segment can build text index")

	segment.State = commonpb.SegmentState_Growing
	result = needDoTextIndex(segment, []int64{101}, true)
	s.False(result, "Growing segment should not need text index")

	segment.State = commonpb.SegmentState_Flushed
	segment.Level = datapb.SegmentLevel_L0
	result = needDoTextIndex(segment, []int64{101}, true)
	s.False(result, "L0 segment should not need text index")

	segment.Level = datapb.SegmentLevel_Legacy
	segment.TextStatsLogs = map[int64]*datapb.TextIndexStats{102: {}}
	result = needDoTextIndex(segment, []int64{101}, true)
	s.True(result, "Segment should build missing text index field")
}

func (s *statsInspectorSuite) TestEnableBM25() {
	// Test if BM25 is enabled
	result := s.inspector.enableBM25()
	s.False(result, "BM25 should be disabled by default")
}

func (s *statsInspectorSuite) TestStatsTaskFieldIDs() {
	// collection 2 declares one json field (202) and one match enabled varchar
	// (201) among a pk.
	coll := s.mt.GetCollection(2)
	s.Equal([]int64{202}, statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_JsonKeyIndexJob))
	s.Equal([]int64{201}, statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_TextIndexJob))
	// a sort task reads every column
	s.Nil(statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_Sort))

	// collection 1 has no json field at all.
	coll = s.mt.GetCollection(1)
	s.Empty(statsTaskFieldIDs(coll.Schema, indexpb.StatsSubJob_JsonKeyIndexJob))
}

func (s *statsInspectorSuite) TestGetCollectionWithoutMeta() {
	inspector := &statsInspector{}
	s.Nil(inspector.getCollection(1))
	s.False(inspector.isExternalCollection(1))
}

func (s *statsInspectorSuite) TestEstimateStatsTaskSize() {
	const (
		collectionID = UniqueID(4)
		numRows      = int64(1000)
		pkFieldID    = int64(300)
		textFieldID  = int64(301)
		jsonFieldID  = int64(302)
		wholeRowSize = int64(100 * 1024 * 1024)
	)

	// an internal storage v3 collection whose short columns share one column
	// group: the non-nullable pk is exact and deductible from the group budget
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64},
			{
				FieldID:  textFieldID,
				Name:     "var",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxLengthKey, Value: "1024"},
					{Key: "enable_match", Value: "true"},
					{Key: "enable_analyzer", Value: "true"},
				},
			},
			{FieldID: jsonFieldID, Name: "json", DataType: schemapb.DataType_JSON},
		},
	}
	s.mt.collections.Insert(collectionID, &collectionInfo{ID: collectionID, Schema: schema})

	perRecord := func(field *schemapb.FieldSchema) int64 {
		size, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{field},
		})
		s.NoError(err)
		return int64(size)
	}
	pkSize := perRecord(schema.Fields[0])
	// both variable width columns of the group, only used to assert the
	// estimation is not the raw schema guess
	schemaGuess := perRecord(schema.Fields[1]) + perRecord(schema.Fields[2])
	// bytes the two variable width columns really take in total
	residualBytes := wholeRowSize - pkSize*numRows
	s.Greater(residualBytes, schemaGuess*numRows)

	newSegment := func(collID UniqueID) *SegmentInfo {
		return &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             30,
				CollectionID:   collID,
				NumOfRows:      numRows,
				StorageVersion: storage.StorageV3,
				ManifestPath:   "manifest.json",
				// external segments report one synthetic column group holding
				// every column at once
				Binlogs: []*datapb.FieldBinlog{
					{
						FieldID:     0,
						ChildFields: []int64{pkFieldID, textFieldID, jsonFieldID},
						Binlogs: []*datapb.Binlog{
							{EntriesNum: numRows, LogSize: wholeRowSize, MemorySize: wholeRowSize},
						},
					},
				},
			},
		}
	}

	s.Run("sort job reads the whole segment", func() {
		segment := newSegment(collectionID)
		s.Equal(segment.getSegmentSize(), s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_Sort))
	})

	s.Run("json job drops the fixed width columns of the group", func() {
		// the json column is charged the whole measured residual of the group,
		// i.e. everything but the pk, doubled for the index it writes
		segment := newSegment(collectionID)
		expected := residualBytes * 2
		s.Equal(expected, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
		s.Less(expected, segment.getSegmentSize()*2)
	})

	s.Run("text job is not doubled", func() {
		segment := newSegment(collectionID)
		expected := residualBytes
		s.Equal(expected, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_TextIndexJob))
		s.Less(expected, segment.getSegmentSize())
	})

	s.Run("storage v2 falls back to the whole segment", func() {
		segment := newSegment(collectionID)
		segment.StorageVersion = storage.StorageV2
		segment.ManifestPath = ""
		s.Equal(segment.getSegmentSize()*2, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("collection without the indexed column falls back to segment size", func() {
		segment := newSegment(1)
		s.Equal(segment.getSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("unresolvable collection falls back to segment size", func() {
		segment := newSegment(999)
		s.Equal(segment.getSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("segment without the field binlog falls back to segment size", func() {
		segment := newSegment(collectionID)
		segment.Binlogs[0].ChildFields = []int64{pkFieldID}
		s.Equal(segment.getSegmentSize()*2, s.inspector.estimateStatsTaskSize(s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})

	s.Run("non milvus-table external collection keeps the whole segment", func() {
		// The only schema shape production can produce for a non milvus-table
		// external collection: source and spec both set, every user field
		// nullable (Pass 2 of NormalizeAndValidateExternalCollectionSchema).
		// With nothing exact to deduct, a variable length target column is
		// charged the whole synthetic group, i.e. the pre-attribution size.
		const externalCollID = UniqueID(7)
		externalSchema := &schemapb.CollectionSchema{
			ExternalSource: "s3://external",
			ExternalSpec:   `{"format":"parquet"}`,
			Fields: []*schemapb.FieldSchema{
				{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, Nullable: true, ExternalField: "pk_col"},
				{
					FieldID:       textFieldID,
					Name:          "var",
					DataType:      schemapb.DataType_VarChar,
					Nullable:      true,
					ExternalField: "var_col",
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.MaxLengthKey, Value: "1024"},
						{Key: "enable_match", Value: "true"},
						{Key: "enable_analyzer", Value: "true"},
					},
				},
				{FieldID: jsonFieldID, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true, ExternalField: "json_col"},
			},
		}
		s.mt.collections.Insert(externalCollID, &collectionInfo{ID: externalCollID, Schema: externalSchema})

		segment := newSegment(externalCollID)
		s.Equal(wholeRowSize, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_TextIndexJob))
		s.Equal(wholeRowSize*2, s.inspector.estimateStatsTaskSize(
			s.mt.GetCollection(segment.GetCollectionID()), segment, indexpb.StatsSubJob_JsonKeyIndexJob))
	})
}
