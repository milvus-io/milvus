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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newJSONFlatWorkerTask(t *testing.T, fieldType schemapb.DataType, cast string, legacy bool) (*indexBuildTask, *TaskManager) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req := &workerpb.CreateJobRequest{
		ClusterID: "flat-upgrade", BuildID: 1001, IndexID: 10,
		CollectionID: 1, PartitionID: 2, SegmentID: 100, NumRows: 100, TaskSlot: 1,
		Field:         &schemapb.FieldSchema{FieldID: 101, Name: "json", DataType: fieldType},
		FieldID:       101,
		FieldType:     fieldType,
		StorageConfig: &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()},
		IndexParams:   []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "INVERTED"}},
	}
	if cast != "" {
		req.IndexParams = append(req.IndexParams, &commonpb.KeyValuePair{Key: common.JSONCastTypeKey, Value: cast})
	}
	if legacy {
		req.Field = nil // Older requests carry FieldID and FieldType separately.
	}
	manager := NewTaskManager(ctx)
	manager.LoadOrStoreIndexTask(req.GetClusterID(), req.GetBuildID(), &IndexTaskInfo{State: commonpb.IndexState_InProgress})
	return NewIndexBuildTask(ctx, cancel, req, nil, manager, nil, taskcommon.Resource{}), manager
}

func TestJSONFlatWorkerPreExecuteDefersLegacyBuilds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fieldType schemapb.DataType
		cast      string
		typeCast  string
		legacy    bool
		retry     bool
	}{
		{name: "flat", fieldType: schemapb.DataType_JSON, cast: "JSON", retry: true},
		{name: "legacy field metadata", fieldType: schemapb.DataType_JSON, cast: "JSON", legacy: true, retry: true},
		{name: "normalized cast", fieldType: schemapb.DataType_JSON, cast: " json ", retry: true},
		{name: "numeric cast", fieldType: schemapb.DataType_JSON, cast: strconv.Itoa(int(schemapb.DataType_JSON)), retry: true},
		{name: "effective type params", fieldType: schemapb.DataType_JSON, typeCast: "JSON", retry: true},
		{name: "index params override type params", fieldType: schemapb.DataType_JSON, typeCast: "JSON", cast: "DOUBLE"},
		{name: "ordinary scalar ignores JSON extras", fieldType: schemapb.DataType_Int64, cast: "JSON"},
		{name: "typed JSON path", fieldType: schemapb.DataType_JSON, cast: "DOUBLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, _ := newJSONFlatWorkerTask(t, tc.fieldType, tc.cast, tc.legacy)
			if tc.typeCast != "" {
				task.req.TypeParams = []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: tc.typeCast}}
			}
			err := task.PreExecute(context.Background())
			if !tc.retry {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.True(t, merr.IsRetryableErr(err))
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Equal(t, indexpb.JobState_JobStateRetry, getStateFromError(err))
			require.Nil(t, task.index, "retirement must precede native builder creation")
		})
	}
}

func TestJSONFlatWorkerReportsRetryToCoordinator(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(strconv.FormatBool(legacy), func(t *testing.T) {
			task, manager := newJSONFlatWorkerTask(t, schemapb.DataType_JSON, "JSON", legacy)
			scheduler := newPipelineScheduler(t)
			require.NoError(t, scheduler.TaskQueue.Enqueue(task))
			require.Eventually(t, func() bool {
				return task.GetState() != indexpb.JobState_JobStateInProgress
			}, 5*time.Second, time.Millisecond)
			scheduler.Close()
			scheduler.Wait()
			info := manager.GetIndexTaskInfo(task.req.GetClusterID(), task.req.GetBuildID())
			require.NotNil(t, info)
			require.Equal(t, commonpb.IndexState_Retry, info.State)
			require.Contains(t, info.FailReason, "JSON flat index building is retired on this worker")
			require.Zero(t, info.ExecStartMs, "a retired flat request must not start building")
			require.Empty(t, info.FileKeys)
			// Both QueryJobs and QueryTask use this projection for coordinator polling.
			result := info.ToIndexTaskInfo(task.req.GetBuildID())
			require.Equal(t, commonpb.IndexState_Retry, result.GetState())
			require.EqualValues(t, 1001, result.GetBuildID())
		})
	}
}
