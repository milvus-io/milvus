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
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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
		err := cluster.DropCompaction(1, 1)
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
		err := cluster.DropImport(1, 1)
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
		err := cluster.DropIndex(1, 1)
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
		err := cluster.DropStats(1, 1)
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
		err := cluster.DropAnalyze(1, 1)
		assert.NoError(t, err)
	})
}

func TestCluster_CreateProperties(t *testing.T) {
	mockNodeManager := NewMockNodeManager(t)
	cluster := NewCluster(mockNodeManager)
	mockClient := mocks.NewMockDataNodeClient(t)
	mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

	// Set up common mock response
	successResponse := merr.Success()
	mockClient.EXPECT().CreateTask(mock.Anything, mock.MatchedBy(func(req *workerpb.CreateTaskRequest) bool {
		props := taskcommon.NewProperties(req.GetProperties())
		clusterID, err := props.GetClusterID()
		assert.NoError(t, err)
		taskID, err := props.GetTaskID()
		assert.NoError(t, err)
		taskType, err := props.GetTaskType()
		assert.NoError(t, err)
		_, err = props.GetTaskSlot()
		assert.NoError(t, err)

		// Verify basic properties
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), clusterID)
		assert.NotEqual(t, int64(-1), taskID)

		// Verify specific properties based on task type
		switch taskType {
		case taskcommon.Compaction:
			assert.Equal(t, taskcommon.Compaction, taskType)
		case taskcommon.PreImport:
			assert.Equal(t, taskcommon.PreImport, taskType)
		case taskcommon.Import:
			assert.Equal(t, taskcommon.Import, taskType)
		case taskcommon.Index:
			assert.Equal(t, taskcommon.Index, taskType)
			rows := props.GetNumRows()
			assert.Greater(t, rows, int64(0))
			version := props.GetTaskVersion()
			assert.Greater(t, version, int64(0))
		case taskcommon.Stats:
			assert.Equal(t, taskcommon.Stats, taskType)
			rows := props.GetNumRows()
			assert.Greater(t, rows, int64(0))
			version := props.GetTaskVersion()
			assert.Greater(t, version, int64(0))
			subType := props.GetSubTaskType()
			assert.NotEmpty(t, subType)
		case taskcommon.Analyze:
			assert.Equal(t, taskcommon.Analyze, taskType)
			version := props.GetTaskVersion()
			assert.Greater(t, version, int64(0))
		default:
			t.Errorf("unexpected task type: %v", taskType)
		}

		return true
	})).Return(successResponse, nil)

	t.Run("CreateCompaction", func(t *testing.T) {
		req := &datapb.CompactionPlan{
			PlanID:    1,
			SlotUsage: 1,
		}
		err := cluster.CreateCompaction(1, req)
		assert.NoError(t, err)
	})

	t.Run("CreatePreImport", func(t *testing.T) {
		req := &datapb.PreImportRequest{
			TaskID: 1,
		}
		err := cluster.CreatePreImport(1, req, 1)
		assert.NoError(t, err)
	})

	t.Run("CreateImport", func(t *testing.T) {
		req := &datapb.ImportRequest{
			TaskID: 1,
		}
		err := cluster.CreateImport(1, req, 1)
		assert.NoError(t, err)
	})

	t.Run("CreateIndex", func(t *testing.T) {
		req := &workerpb.CreateJobRequest{
			BuildID:      1,
			TaskSlot:     1,
			NumRows:      1000,
			IndexVersion: 1,
		}
		err := cluster.CreateIndex(1, req)
		assert.NoError(t, err)
	})

	t.Run("CreateStats", func(t *testing.T) {
		req := &workerpb.CreateStatsRequest{
			TaskID:      1,
			TaskSlot:    1,
			NumRows:     1000,
			TaskVersion: 1,
			SubJobType:  indexpb.StatsSubJob_Sort,
		}
		err := cluster.CreateStats(1, req)
		assert.NoError(t, err)
	})

	t.Run("CreateAnalyze", func(t *testing.T) {
		req := &workerpb.AnalyzeRequest{
			TaskID:   1,
			TaskSlot: 1,
			Version:  1,
		}
		err := cluster.CreateAnalyze(1, req)
		assert.NoError(t, err)
	})
}

func TestCluster_QueryProperties(t *testing.T) {
	mockNodeManager := NewMockNodeManager(t)
	cluster := NewCluster(mockNodeManager)
	mockClient := mocks.NewMockDataNodeClient(t)
	mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)
	mockNodeManager.EXPECT().GetClientIDs().Return([]int64{1})

	// Set up common mock response
	expectedResponse := &workerpb.QueryTaskResponse{
		Status: merr.Success(),
		Properties: map[string]string{
			taskcommon.StateKey: taskcommon.Init.String(),
		},
	}
	mockClient.EXPECT().QueryTask(mock.Anything, mock.MatchedBy(func(req *workerpb.QueryTaskRequest) bool {
		props := taskcommon.NewProperties(req.GetProperties())
		clusterID, err := props.GetClusterID()
		assert.NoError(t, err)
		taskID, err := props.GetTaskID()
		assert.NoError(t, err)
		taskType, err := props.GetTaskType()
		assert.NoError(t, err)

		// Verify basic properties
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), clusterID)
		assert.NotEqual(t, int64(0), taskID)

		// Verify specific properties based on task type
		switch taskType {
		case taskcommon.QuerySlot:
			assert.Equal(t, taskcommon.QuerySlot, taskType)
		case taskcommon.Compaction:
			assert.Equal(t, taskcommon.Compaction, taskType)
		case taskcommon.PreImport:
			assert.Equal(t, taskcommon.PreImport, taskType)
		case taskcommon.Import:
			assert.Equal(t, taskcommon.Import, taskType)
		case taskcommon.Index:
			assert.Equal(t, taskcommon.Index, taskType)
		case taskcommon.Stats:
			assert.Equal(t, taskcommon.Stats, taskType)
		case taskcommon.Analyze:
			assert.Equal(t, taskcommon.Analyze, taskType)
		default:
			t.Errorf("unexpected task type: %v", taskType)
		}

		return true
	})).Return(expectedResponse, nil)

	t.Run("QuerySlot", func(t *testing.T) {
		cluster.QuerySlot()
	})

	t.Run("QueryCompaction", func(t *testing.T) {
		req := &datapb.CompactionStateRequest{
			PlanID: 1,
		}
		_, err := cluster.QueryCompaction(1, req)
		assert.NoError(t, err)
	})

	t.Run("QueryPreImport", func(t *testing.T) {
		req := &datapb.QueryPreImportRequest{
			TaskID: 1,
		}
		_, err := cluster.QueryPreImport(1, req)
		assert.NoError(t, err)
	})

	t.Run("QueryImport", func(t *testing.T) {
		req := &datapb.QueryImportRequest{
			TaskID: 1,
		}
		_, err := cluster.QueryImport(1, req)
		assert.NoError(t, err)
	})

	t.Run("QueryIndex", func(t *testing.T) {
		req := &workerpb.QueryJobsRequest{
			TaskIDs: []int64{1},
		}
		_, err := cluster.QueryIndex(1, req)
		assert.NoError(t, err)
	})

	t.Run("QueryStats", func(t *testing.T) {
		req := &workerpb.QueryJobsRequest{
			TaskIDs: []int64{1},
		}
		_, err := cluster.QueryStats(1, req)
		assert.NoError(t, err)
	})

	t.Run("QueryAnalyze", func(t *testing.T) {
		req := &workerpb.QueryJobsRequest{
			TaskIDs: []int64{1},
		}
		_, err := cluster.QueryAnalyze(1, req)
		assert.NoError(t, err)
	})
}

func TestCluster_DropProperties(t *testing.T) {
	mockNodeManager := NewMockNodeManager(t)
	cluster := NewCluster(mockNodeManager)
	mockClient := mocks.NewMockDataNodeClient(t)
	mockNodeManager.EXPECT().GetClient(mock.Anything).Return(mockClient, nil)

	// Set up common mock response
	successResponse := merr.Success()
	mockClient.EXPECT().DropTask(mock.Anything, mock.MatchedBy(func(req *workerpb.DropTaskRequest) bool {
		props := taskcommon.NewProperties(req.GetProperties())
		clusterID, err := props.GetClusterID()
		assert.NoError(t, err)
		taskID, err := props.GetTaskID()
		assert.NoError(t, err)
		taskType, err := props.GetTaskType()
		assert.NoError(t, err)

		// Verify basic properties
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), clusterID)
		assert.NotEqual(t, int64(-1), taskID)

		// Verify specific properties based on task type
		switch taskType {
		case taskcommon.Compaction:
			assert.Equal(t, taskcommon.Compaction, taskType)
		case taskcommon.Import:
			assert.Equal(t, taskcommon.Import, taskType)
		case taskcommon.Index:
			assert.Equal(t, taskcommon.Index, taskType)
		case taskcommon.Stats:
			assert.Equal(t, taskcommon.Stats, taskType)
		case taskcommon.Analyze:
			assert.Equal(t, taskcommon.Analyze, taskType)
		default:
			t.Errorf("unexpected task type: %v", taskType)
		}

		return true
	})).Return(successResponse, nil)

	t.Run("DropCompaction", func(t *testing.T) {
		err := cluster.DropCompaction(1, 1)
		assert.NoError(t, err)
	})

	t.Run("DropImport", func(t *testing.T) {
		err := cluster.DropImport(1, 1)
		assert.NoError(t, err)
	})

	t.Run("DropIndex", func(t *testing.T) {
		err := cluster.DropIndex(1, 1)
		assert.NoError(t, err)
	})

	t.Run("DropStats", func(t *testing.T) {
		err := cluster.DropStats(1, 1)
		assert.NoError(t, err)
	})

	t.Run("DropAnalyze", func(t *testing.T) {
		err := cluster.DropAnalyze(1, 1)
		assert.NoError(t, err)
	})
}
