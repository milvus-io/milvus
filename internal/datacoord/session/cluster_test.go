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

package session

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestCluster_createTask(t *testing.T) {
	t.Run("GetClient failed", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, errors.New("mock err"))
		c := NewCluster(mockNodeManager)

		err := c.(*cluster).createTask(1, &workerpb.CreateTaskRequest{}, nil)
		assert.Error(t, err)
	})

	t.Run("CreateTask rpc failed", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		err := c.(*cluster).createTask(1, &workerpb.CreateTaskRequest{}, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		err := c.(*cluster).createTask(1, &workerpb.CreateTaskRequest{}, nil)
		assert.NoError(t, err)
	})
}

func TestCluster_queryTask(t *testing.T) {
	t.Run("GetClient failed", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(nil, errors.New("mock err"))
		c := NewCluster(mockNodeManager)
		result, err := c.(*cluster).queryTask(1, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("QueryTask rpc failed", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))

		result, err := c.(*cluster).queryTask(1, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("normal case", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		expectedResult := &workerpb.QueryTaskResponse{
			Status: merr.Success(),
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:  merr.Success(),
			Payload: payload,
		}, nil)

		result, err := c.(*cluster).queryTask(1, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestCluster_dropTask(t *testing.T) {
	t.Run("GetClient failed", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(nil, errors.New("mock err"))
		c := NewCluster(mockNodeManager)
		err := c.(*cluster).dropTask(1, nil)
		assert.Error(t, err)
	})

	t.Run("DropTask rpc failed", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(nil, errors.New("mock err"))
		err := c.(*cluster).dropTask(1, nil)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager := NewMockNodeManager(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		c := NewCluster(mockNodeManager)

		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)
		err := c.(*cluster).dropTask(1, nil)
		assert.NoError(t, err)
	})
}

func TestCluster_QuerySlot(t *testing.T) {
	t.Run("normal case", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClientIDs().Return([]int64{1, 2})
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		expectedSlots := &workerpb.GetJobStatsResponse{
			TotalSlots:     10,
			AvailableSlots: 5,
		}
		payload, _ := proto.Marshal(expectedSlots)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:  merr.Success(),
			Payload: payload,
		}, nil)

		// Test
		result := cluster.QuerySlot()
		assert.NotNil(t, result)
		assert.Len(t, result, 2)
		for _, slots := range result {
			assert.Equal(t, int64(10), slots.TotalSlots)
			assert.Equal(t, int64(5), slots.AvailableSlots)
		}
	})

	t.Run("client error", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client error
		mockNodeManager.EXPECT().GetClientIDs().Return([]int64{1})
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(nil, assert.AnError)

		// Test
		result := cluster.QuerySlot()
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

func TestCluster_Compaction(t *testing.T) {
	t.Run("create compaction", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreateCompaction(1, &datapb.CompactionPlan{})
		assert.NoError(t, err)
	})

	t.Run("query compaction", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		expectedResult := &datapb.CompactionStateResponse{
			Results: []*datapb.CompactionPlanResult{
				{
					PlanID: 1,
					Segments: []*datapb.CompactionSegment{
						{
							SegmentID: 1,
						},
					},
				},
			},
		}
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Finished)
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryCompaction(1, &datapb.CompactionStateRequest{PlanID: 1})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.PlanID)
	})

	t.Run("drop compaction", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.DropCompaction(1, &datapb.DropCompactionPlanRequest{})
		assert.NoError(t, err)
	})
}

func TestCluster_Import(t *testing.T) {
	t.Run("create pre-import", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreatePreImport(1, &datapb.PreImportRequest{}, 1)
		assert.NoError(t, err)
	})

	t.Run("create import", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreateImport(1, &datapb.ImportRequest{}, 1)
		assert.NoError(t, err)
	})

	t.Run("query pre-import", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Finished)
		expectedResult := &datapb.QueryPreImportResponse{}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryPreImport(1, &datapb.QueryPreImportRequest{})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("query import", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Finished)
		expectedResult := &datapb.QueryImportResponse{}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryImport(1, &datapb.QueryImportRequest{})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("drop import", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.DropImport(1, &datapb.DropImportRequest{})
		assert.NoError(t, err)
	})
}

func TestCluster_Index(t *testing.T) {
	t.Run("create index", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreateIndex(1, &workerpb.CreateJobRequest{})
		assert.NoError(t, err)
	})

	t.Run("query index", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Finished)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_IndexJobResults{
				IndexJobResults: &workerpb.IndexJobResults{},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryIndex(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("query index failed", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Failed)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_IndexJobResults{
				IndexJobResults: &workerpb.IndexJobResults{
					Results: []*workerpb.IndexTaskInfo{
						{
							BuildID:    1,
							State:      commonpb.IndexState_Failed,
							FailReason: "mock reason",
						},
					},
				},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryIndex(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, commonpb.IndexState_Failed, result.Results[0].State)
		assert.Equal(t, "mock reason", result.Results[0].FailReason)
	})

	t.Run("drop index", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.DropIndex(1, &workerpb.DropJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
	})
}

func TestCluster_Stats(t *testing.T) {
	t.Run("create stats", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreateStats(1, &workerpb.CreateStatsRequest{})
		assert.NoError(t, err)
	})

	t.Run("query stats", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Finished)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_StatsJobResults{
				StatsJobResults: &workerpb.StatsResults{},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryStats(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("query stats failed", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Failed)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_StatsJobResults{
				StatsJobResults: &workerpb.StatsResults{
					Results: []*workerpb.StatsResult{
						{
							TaskID:     1,
							State:      taskcommon.Failed,
							FailReason: "mock reason",
						},
					},
				},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryStats(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, taskcommon.Failed, result.Results[0].State)
		assert.Equal(t, "mock reason", result.Results[0].FailReason)
	})

	t.Run("drop stats", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.DropStats(1, &workerpb.DropJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
	})
}

func TestCluster_Analyze(t *testing.T) {
	t.Run("create analyze", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().CreateTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.CreateAnalyze(1, &workerpb.AnalyzeRequest{})
		assert.NoError(t, err)
	})

	t.Run("query analyze", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.InProgress)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
				AnalyzeJobResults: &workerpb.AnalyzeResults{},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryAnalyze(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("query analyze failed", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

		// Mock response
		properties := taskcommon.NewProperties(nil)
		properties.AppendTaskState(taskcommon.Failed)
		expectedResult := &workerpb.QueryJobsV2Response{
			Result: &workerpb.QueryJobsV2Response_AnalyzeJobResults{
				AnalyzeJobResults: &workerpb.AnalyzeResults{
					Results: []*workerpb.AnalyzeResult{
						{
							TaskID:     1,
							State:      taskcommon.Failed,
							FailReason: "mock reason",
						},
					},
				},
			},
		}
		payload, _ := proto.Marshal(expectedResult)
		mockClient.EXPECT().QueryTask(mock.Anything, mock.Anything).Return(&workerpb.QueryTaskResponse{
			Status:     merr.Success(),
			Payload:    payload,
			Properties: properties,
		}, nil)

		// Test
		result, err := cluster.QueryAnalyze(1, &workerpb.QueryJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, taskcommon.Failed, result.Results[0].State)
		assert.Equal(t, "mock reason", result.Results[0].FailReason)
	})

	t.Run("drop analyze", func(t *testing.T) {
		mockNodeManager := NewMockNodeManager(t)
		cluster := NewCluster(mockNodeManager)

		// Mock client
		mockClient := mocks.NewMockDataNodeClient(t)
		mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().DropTask(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		// Test
		err := cluster.DropAnalyze(1, &workerpb.DropJobsRequest{TaskIDs: []int64{1}})
		assert.NoError(t, err)
	})
}
