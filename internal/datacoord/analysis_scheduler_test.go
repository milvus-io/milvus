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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type AnalysisSchedulerSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	fieldID      int64
	segmentIDs   []int64
	nodeID       int64
	duration     time.Duration
}

func (s *AnalysisSchedulerSuite) initParams() {
	s.collectionID = 100
	s.partitionID = 101
	s.fieldID = 102
	s.nodeID = 103
	s.segmentIDs = []int64{1000, 1001, 1002}
	s.duration = time.Millisecond * 100
}

func (s *AnalysisSchedulerSuite) createAnalysisMeta(catalog metastore.DataCoordCatalog) *analysisMeta {
	return &analysisMeta{
		ctx:     context.Background(),
		lock:    sync.RWMutex{},
		catalog: catalog,
		tasks: map[int64]*model.AnalysisTask{
			1: {
				TenantID:     "",
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       1,
				State:        commonpb.IndexState_Unissued,
			},
			2: {
				TenantID:     "",
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       2,
				NodeID:       s.nodeID,
				State:        commonpb.IndexState_InProgress,
			},
			3: {
				TenantID:     "",
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       3,
				NodeID:       s.nodeID,
				State:        commonpb.IndexState_Finished,
			},
			4: {
				TenantID:     "",
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   s.segmentIDs,
				TaskID:       4,
				NodeID:       s.nodeID,
				State:        commonpb.IndexState_Failed,
			},
			5: {
				TenantID:     "",
				CollectionID: s.collectionID,
				PartitionID:  s.partitionID,
				FieldID:      s.fieldID,
				SegmentIDs:   []int64{1001, 1002},
				TaskID:       5,
				NodeID:       s.nodeID,
				State:        commonpb.IndexState_Retry,
			},
		},
	}
}

func (s *AnalysisSchedulerSuite) createMeta() *meta {
	return &meta{
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1000: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1000,
						CollectionID: s.collectionID,
						PartitionID:  s.partitionID,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1001: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1001,
						CollectionID: s.collectionID,
						PartitionID:  s.partitionID,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1002: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1002,
						CollectionID: s.collectionID,
						PartitionID:  s.partitionID,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: s.fieldID, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
			},
		},
	}
}

func (s *AnalysisSchedulerSuite) Test_analysisScheduler() {
	s.initParams()
	ctx := context.Background()

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil)

	in := mocks.NewMockIndexNodeClient(s.T())
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil)
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			results := make(map[int64]*indexpb.AnalysisResult)
			for _, taskID := range request.GetTaskIDs() {
				results[taskID] = &indexpb.AnalysisResult{
					TaskID:        taskID,
					State:         commonpb.IndexState_Finished,
					CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
					SegmentOffsetMappingFiles: map[int64]string{
						1000: "1000/offset_mapping",
						1001: "1001/offset_mapping",
						1002: "1002/offset_mapping",
					},
					FailReason: "",
				}
			}
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   results,
			}, nil
		})
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil)

	workerManager := NewMockWorkerManager(s.T())
	workerManager.EXPECT().SelectNodeAndAssignTask(mock.Anything).RunAndReturn(
		func(f func(int64, types.IndexNodeClient) error) error {
			return f(s.nodeID, in)
		})
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true)

	mt := s.createMeta()
	at := s.createAnalysisMeta(catalog)

	scheduler := newAnalysisTaskScheduler(ctx, mt, at, workerManager)
	s.Equal(3, len(scheduler.tasks))
	s.Equal(taskInit, scheduler.tasks[1])
	s.Equal(taskInProgress, scheduler.tasks[2])
	s.Equal(taskInit, scheduler.tasks[5])

	scheduler.scheduleDuration = time.Millisecond * 500
	scheduler.Start()

	s.Run("enqueue", func() {
		newTask := &model.AnalysisTask{
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			FieldID:      s.fieldID,
			SegmentIDs:   s.segmentIDs,
			TaskID:       6,
		}
		err := scheduler.analysisMeta.AddAnalysisTask(newTask)
		s.NoError(err)
		scheduler.enqueue(6)
	})

	for {
		scheduler.lock.RLock()
		taskNum := len(scheduler.tasks)
		scheduler.lock.RUnlock()

		if taskNum == 0 {
			break
		}
		time.Sleep(time.Second)
	}

	scheduler.Stop()
}

func (s *AnalysisSchedulerSuite) Test_failCase() {
	s.initParams()
	ctx := context.Background()

	catalog := catalogmocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().DropAnalysisTask(mock.Anything, mock.Anything).Return(nil)

	in := mocks.NewMockIndexNodeClient(s.T())

	workerManager := NewMockWorkerManager(s.T())
	workerManager.EXPECT().SelectNodeAndAssignTask(mock.Anything).RunAndReturn(
		func(f func(int64, types.IndexNodeClient) error) error {
			return f(s.nodeID, in)
		})

	mt := s.createMeta()
	at := s.createAnalysisMeta(catalog)

	scheduler := newAnalysisTaskScheduler(ctx, mt, at, workerManager)

	// remove task in meta
	err := scheduler.analysisMeta.DropAnalysisTask(2)
	s.NoError(err)

	mt.segments.DropSegment(1000)
	scheduler.scheduleDuration = s.duration
	scheduler.Start()

	// 1. update version failed --> state: init
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(errors.New("catalog update version error")).Once()

	// 2. update version success, but building fail --> state: init
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Once()
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(errors.New("catalog update building error")).Once()

	// 3. update version success, building success, but assign task fail --> state: retry
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), errors.New("assign task error")).Once()

	// 4. drop task success, --> state: init
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 5. update version success, building success, assign task success --> state: InProgress
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 6. get task state: InProgress --> state: InProgress
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			results := make(map[int64]*indexpb.AnalysisResult)
			for _, taskID := range request.GetTaskIDs() {
				results[taskID] = &indexpb.AnalysisResult{
					TaskID: taskID,
					State:  commonpb.IndexState_InProgress,
				}
			}
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   results,
			}, nil
		}).Once()

	// 7. get task state: Finished, but save meta fail --> state: InProgress
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			results := make(map[int64]*indexpb.AnalysisResult)
			for _, taskID := range request.GetTaskIDs() {
				results[taskID] = &indexpb.AnalysisResult{
					TaskID:        taskID,
					State:         commonpb.IndexState_Finished,
					CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
					SegmentOffsetMappingFiles: map[int64]string{
						1000: "1000/offset_mapping",
						1001: "1001/offset_mapping",
						1002: "1002/offset_mapping",
					},
					FailReason: "",
				}
			}
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   results,
			}, nil
		}).Once()
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(errors.New("catalog save finished error")).Once()

	// 8. get task state error --> state: retry
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).Return(nil, errors.New("get task result error")).Once()

	// 9. drop task success --> state: init
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 10. update version success, building success, assign task success --> state: InProgress
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 11. get task state: retry --> state: retry
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			results := make(map[int64]*indexpb.AnalysisResult)
			for _, taskID := range request.GetTaskIDs() {
				results[taskID] = &indexpb.AnalysisResult{
					TaskID:     taskID,
					State:      commonpb.IndexState_Retry,
					FailReason: "state is retry",
				}
			}
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   results,
			}, nil
		}).Once()

	// 12. drop task success --> state: init
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 13. update version success, building success, assign task success --> state: InProgress
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 14. get task state: task not exist in node --> state: retry
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   map[int64]*indexpb.AnalysisResult{},
			}, nil
		}).Once()

	// 15. drop task success --> state: init
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 16. update version success, building success, assign task success --> state: InProgress
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 17. get task state: node not exist --> retry
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

	// 18. drop task success --> state: init
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(merr.Success(), nil).Once()

	// 19. update version success, building success, assign task success --> state: InProgress
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Twice()
	in.EXPECT().Analysis(mock.Anything, mock.Anything).Return(merr.Success(), nil)

	// 20. get task state: Finished, save success --> state: done
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().QueryAnalysisResult(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *indexpb.QueryAnalysisResultRequest, option ...grpc.CallOption) (*indexpb.QueryAnalysisResultResponse, error) {
			results := make(map[int64]*indexpb.AnalysisResult)
			for _, taskID := range request.GetTaskIDs() {
				results[taskID] = &indexpb.AnalysisResult{
					TaskID:        taskID,
					State:         commonpb.IndexState_Finished,
					CentroidsFile: fmt.Sprintf("%d/stats_file", taskID),
					SegmentOffsetMappingFiles: map[int64]string{
						1000: "1000/offset_mapping",
						1001: "1001/offset_mapping",
						1002: "1002/offset_mapping",
					},
					FailReason: "",
				}
			}
			return &indexpb.QueryAnalysisResultResponse{
				Status:    merr.Success(),
				ClusterID: request.GetClusterID(),
				Results:   results,
			}, nil
		}).Once()
	catalog.EXPECT().SaveAnalysisTask(mock.Anything, mock.Anything).Return(nil).Once()

	// 21. drop task fail --> state: done
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(in, true).Once()
	in.EXPECT().DropAnalysisTasks(mock.Anything, mock.Anything).Return(nil, errors.New("drop task error")).Once()

	//21. drop task: node not exist --> task done, remove task
	workerManager.EXPECT().GetClientByID(mock.Anything).Return(nil, false).Once()

	for {
		scheduler.lock.RLock()
		taskNum := len(scheduler.tasks)
		scheduler.lock.RUnlock()

		if taskNum == 0 {
			break
		}
		time.Sleep(time.Second)
	}

	scheduler.Stop()
}

func Test_AnalysisScheduler(t *testing.T) {
	suite.Run(t, new(AnalysisSchedulerSuite))
}
