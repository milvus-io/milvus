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

package rootcoord

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/stretchr/testify/assert"
)

type customKV struct {
	kv.MockMetaKV
}

func TestImportManager_NewImportManager(t *testing.T) {
	Params.RootCoordCfg.ImportTaskSubPath = "test_import_task"
	mockKv := &kv.MockMetaKV{}
	mockKv.InMemKv = make(map[string]string)
	ti1 := &datapb.ImportTaskInfo{
		Id: 100,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPending,
		},
	}
	ti2 := &datapb.ImportTaskInfo{
		Id: 200,
		State: &datapb.ImportTaskState{
			StateCode: commonpb.ImportState_ImportPersisted,
		},
	}
	taskInfo1, err := proto.Marshal(ti1)
	assert.NoError(t, err)
	taskInfo2, err := proto.Marshal(ti2)
	assert.NoError(t, err)
	mockKv.SaveWithLease(BuildImportTaskKey(1), "value", 1)
	mockKv.SaveWithLease(BuildImportTaskKey(2), string(taskInfo1), 2)
	mockKv.SaveWithLease(BuildImportTaskKey(3), string(taskInfo2), 3)
	fn := func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
	}
	mgr := newImportManager(context.TODO(), mockKv, fn)
	assert.NotNil(t, mgr)
	mgr.init(context.TODO())
}

func TestImportManager_ImportJob(t *testing.T) {
	Params.RootCoordCfg.ImportTaskSubPath = "test_import_task"
	colID := int64(100)
	mockKv := &kv.MockMetaKV{}
	mockKv.InMemKv = make(map[string]string)
	mgr := newImportManager(context.TODO(), mockKv, nil)
	resp := mgr.importJob(context.TODO(), nil, colID)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		RowBased:       true,
		Files:          []string{"f1", "f2", "f3"},
	}

	resp = mgr.importJob(context.TODO(), rowReq, colID)
	assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	colReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		RowBased:       false,
		Files:          []string{"f1", "f2"},
		Options: []*commonpb.KeyValuePair{
			{
				Key:   Bucket,
				Value: "mybucket",
			},
		},
	}

	fn := func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}
	}

	mgr = newImportManager(context.TODO(), mockKv, fn)
	resp = mgr.importJob(context.TODO(), rowReq, colID)
	assert.Equal(t, len(rowReq.Files), len(mgr.pendingTasks))
	assert.Equal(t, 0, len(mgr.workingTasks))

	mgr = newImportManager(context.TODO(), mockKv, fn)
	resp = mgr.importJob(context.TODO(), colReq, colID)
	assert.Equal(t, 1, len(mgr.pendingTasks))
	assert.Equal(t, 0, len(mgr.workingTasks))

	fn = func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
	}

	mgr = newImportManager(context.TODO(), mockKv, fn)
	resp = mgr.importJob(context.TODO(), rowReq, colID)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, len(rowReq.Files), len(mgr.workingTasks))

	mgr = newImportManager(context.TODO(), mockKv, fn)
	resp = mgr.importJob(context.TODO(), colReq, colID)
	assert.Equal(t, 0, len(mgr.pendingTasks))
	assert.Equal(t, 1, len(mgr.workingTasks))

	count := 0
	fn = func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		if count >= 2 {
			return &datapb.ImportTaskResponse{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
				},
			}
		}
		count++
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
	}

	mgr = newImportManager(context.TODO(), mockKv, fn)
	resp = mgr.importJob(context.TODO(), rowReq, colID)
	assert.Equal(t, len(rowReq.Files)-2, len(mgr.pendingTasks))
	assert.Equal(t, 2, len(mgr.workingTasks))
}

func TestImportManager_TaskState(t *testing.T) {
	Params.RootCoordCfg.ImportTaskSubPath = "test_import_task"
	colID := int64(100)
	mockKv := &kv.MockMetaKV{}
	mockKv.InMemKv = make(map[string]string)
	fn := func(ctx context.Context, req *datapb.ImportTaskRequest) *datapb.ImportTaskResponse {
		return &datapb.ImportTaskResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		}
	}

	rowReq := &milvuspb.ImportRequest{
		CollectionName: "c1",
		PartitionName:  "p1",
		RowBased:       true,
		Files:          []string{"f1", "f2", "f3"},
	}

	mgr := newImportManager(context.TODO(), mockKv, fn)
	mgr.importJob(context.TODO(), rowReq, colID)

	state := &rootcoordpb.ImportResult{
		TaskId: 10000,
	}
	_, err := mgr.updateTaskState(state)
	assert.NotNil(t, err)

	state = &rootcoordpb.ImportResult{
		TaskId:   1,
		RowCount: 1000,
		State:    commonpb.ImportState_ImportCompleted,
		Infos: []*commonpb.KeyValuePair{
			{
				Key:   "key1",
				Value: "value1",
			},
			{
				Key:   "failed_reason",
				Value: "some_reason",
			},
		},
	}
	ti, err := mgr.updateTaskState(state)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), ti.GetId())
	assert.Equal(t, int64(100), ti.GetCollectionId())
	assert.Equal(t, int64(100), ti.GetCollectionId())
	assert.Equal(t, int64(0), ti.GetPartitionId())
	assert.Equal(t, true, ti.GetRowBased())
	assert.Equal(t, []string{"f2"}, ti.GetFiles())
	assert.Equal(t, commonpb.ImportState_ImportCompleted, ti.GetState().GetStateCode())
	assert.Equal(t, int64(1000), ti.GetState().GetRowCount())

	resp := mgr.getTaskState(10000)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.Status.ErrorCode)

	resp = mgr.getTaskState(1)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, commonpb.ImportState_ImportCompleted, resp.State)

	resp = mgr.getTaskState(0)
	assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	assert.Equal(t, commonpb.ImportState_ImportPending, resp.State)
}
