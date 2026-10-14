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

package index

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestAnalyzeParametersAndBudget(t *testing.T) {
	req := &workerpb.AnalyzeRequest{FieldID: 101, FieldType: schemapb.DataType_FloatVector, Dim: 128, NumClusters: 16, ClusterType: "KMEANS", ClusterParams: map[string]string{"metric_type": "COSINE", "num_iter": "7"}, MaxTrainSizeRatio: 0.8, StorageConfig: &indexpb.StorageConfig{}, SegmentStats: map[int64]*indexpb.SegmentStats{1: {ID: 1, NumRows: 1000, LogIDs: []int64{12, 3}}}}
	require.NoError(t, validateAnalyzeRequest(req, 1024*1024))
	info := buildAnalyzeInfo(req)
	require.Equal(t, req.ClusterType, info.ClusterType)
	require.Equal(t, req.ClusterParams, info.ClusterParams)
	info.ClusterParams["metric_type"] = "L2"
	require.Equal(t, "COSINE", req.ClusterParams["metric_type"])
	for name, change := range map[string]func(*workerpb.AnalyzeRequest){
		"nan_budget":      func(r *workerpb.AnalyzeRequest) { r.MaxTrainSizeRatio = math.NaN() },
		"negative_budget": func(r *workerpb.AnalyzeRequest) { r.MaxTrainSizeRatio = -1 },
		"excess_budget":   func(r *workerpb.AnalyzeRequest) { r.MaxTrainSizeRatio = 2 },
		"overflow":        func(r *workerpb.AnalyzeRequest) { r.SegmentStats[1].NumRows = math.MaxInt64 },
		"negative_rows":   func(r *workerpb.AnalyzeRequest) { r.SegmentStats[1].NumRows = -1 },
		"wrong_segment":   func(r *workerpb.AnalyzeRequest) { r.SegmentStats[1].ID = 2 },
		"mixed_storage":   func(r *workerpb.AnalyzeRequest) { r.SegmentStats[1].ManifestPath = "manifest" },
		"half_deferred":   func(r *workerpb.AnalyzeRequest) { r.FieldType = schemapb.DataType_Float16Vector },
	} {
		t.Run(name, func(t *testing.T) {
			copy := proto.Clone(req).(*workerpb.AnalyzeRequest)
			change(copy)
			require.Error(t, validateAnalyzeRequest(copy, 1024*1024))
		})
	}
	require.Error(t, validateAnalyzeRequest(req, math.MaxUint64))
}

func TestAnalyzeGenerationFencesLateCallbacks(t *testing.T) {
	manager := NewTaskManager(context.Background())
	manager.LoadOrStoreAnalyzeTask("cluster", 1, &AnalyzeTaskInfo{Version: 2, State: indexpb.JobState_JobStateInProgress})
	manager.StoreAnalyzeFilesAndStatistic("cluster", 1, 1, "old")
	manager.StoreAnalyzeTaskState("cluster", 1, 1, indexpb.JobState_JobStateFinished, "")
	current := manager.GetAnalyzeTaskInfo("cluster", 1)
	require.Equal(t, indexpb.JobState_JobStateInProgress, current.State)
	require.Empty(t, current.CentroidsFile)
	manager.StoreAnalyzeFilesAndStatistic("cluster", 1, 2, "new")
	manager.StoreAnalyzeTaskState("cluster", 1, 2, indexpb.JobState_JobStateFinished, "")
	current = manager.GetAnalyzeTaskInfo("cluster", 1)
	require.Equal(t, int64(2), current.Version)
	require.Equal(t, "new", current.CentroidsFile)
	require.Equal(t, indexpb.JobState_JobStateFinished, current.State)
}

// A native upload/read failure must never publish the partial generation.
type incompleteAnalyze struct{ fakeAnalyze }

func (a *incompleteAnalyze) GetResult(int) (string, int64, []string, []int64, error) {
	return "", 0, nil, nil, merr.WrapErrIoKeyNotFound("assignment")
}

func TestAnalyzeIncompleteOrCanceledResultIsNotPublished(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		manager := NewTaskManager(context.Background())
		manager.LoadOrStoreAnalyzeTask("cluster", 1, &AnalyzeTaskInfo{Version: 2, State: indexpb.JobState_JobStateInProgress})
		native := &incompleteAnalyze{}
		task := &analyzeTask{req: &workerpb.AnalyzeRequest{ClusterID: "cluster", TaskID: 1, Version: 2}, analyze: native, manager: manager}
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		}
		require.Error(t, task.PostExecute(ctx))
		cancel()
		info := manager.GetAnalyzeTaskInfo("cluster", 1)
		require.Equal(t, indexpb.JobState_JobStateInProgress, info.State)
		require.Empty(t, info.CentroidsFile)
		require.Equal(t, 1, native.deleted)
	}
}
