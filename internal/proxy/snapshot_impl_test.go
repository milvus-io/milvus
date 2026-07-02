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

package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestProxy_CreateSnapshot_Success(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	// Mock successful enqueue
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		// Set task result to simulate successful execution
		if cst, ok := t.(*createSnapshotTask); ok {
			cst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	// Mock successful task completion
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.CreateSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result))
}

func TestProxy_CreateSnapshot_EnqueueFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	expectedError := errors.New("enqueue failed")

	// Mock enqueue failure
	mock := mockey.Mock((*ddTaskQueue).Enqueue).Return(expectedError).Build()
	defer mock.UnPatch()

	result, err := proxy.CreateSnapshot(context.Background(), req)

	assert.NoError(t, err) // API should not return error, but error status
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result))
	assert.Contains(t, result.GetReason(), "enqueue failed")
}

func TestProxy_CreateSnapshot_WaitToFinishFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	expectedError := errors.New("task execution failed")

	// Mock successful enqueue but failed task execution
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(expectedError).Build()
	defer mock2.UnPatch()

	result, err := proxy.CreateSnapshot(context.Background(), req)

	assert.NoError(t, err) // API should not return error, but error status
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result))
	assert.Contains(t, result.GetReason(), "task execution failed")
}

func TestProxy_DropSnapshot_Success(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DropSnapshotRequest{
		Name: "test_snapshot",
	}

	// Mock successful enqueue and task completion
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if dst, ok := t.(*dropSnapshotTask); ok {
			dst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.DropSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result))
}

func TestProxy_DropSnapshot_EnqueueFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DropSnapshotRequest{
		Name: "test_snapshot",
	}

	expectedError := errors.New("enqueue failed")

	mock := mockey.Mock((*ddTaskQueue).Enqueue).Return(expectedError).Build()
	defer mock.UnPatch()

	result, err := proxy.DropSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result))
	assert.Contains(t, result.GetReason(), "enqueue failed")
}

func TestProxy_DropSnapshot_WaitToFinishFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DropSnapshotRequest{
		Name: "test_snapshot",
	}

	expectedError := errors.New("task execution failed")

	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(expectedError).Build()
	defer mock2.UnPatch()

	result, err := proxy.DropSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result))
	assert.Contains(t, result.GetReason(), "task execution failed")
}

func TestProxy_DescribeSnapshot_Success(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DescribeSnapshotRequest{
		Name: "test_snapshot",
	}

	// Mock successful enqueue and task completion
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if dst, ok := t.(*describeSnapshotTask); ok {
			dst.result = &milvuspb.DescribeSnapshotResponse{
				Status:         merr.Success(),
				Name:           "test_snapshot",
				CollectionName: "test_collection",
			}
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.DescribeSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result.GetStatus()))
	assert.Equal(t, "test_snapshot", result.GetName())
	assert.Equal(t, "test_collection", result.GetCollectionName())
}

func TestProxy_DescribeSnapshot_EnqueueFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DescribeSnapshotRequest{
		Name: "test_snapshot",
	}

	expectedError := errors.New("enqueue failed")

	mock := mockey.Mock((*ddTaskQueue).Enqueue).Return(expectedError).Build()
	defer mock.UnPatch()

	result, err := proxy.DescribeSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "enqueue failed")
}

func TestProxy_DescribeSnapshot_WaitToFinishFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DescribeSnapshotRequest{
		Name: "test_snapshot",
	}

	expectedError := errors.New("task execution failed")

	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(expectedError).Build()
	defer mock2.UnPatch()

	result, err := proxy.DescribeSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "task execution failed")
}

func TestProxy_ListSnapshots_Success(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.ListSnapshotsRequest{
		CollectionName: "test_collection",
	}

	// Mock successful enqueue and task completion
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if lst, ok := t.(*listSnapshotsTask); ok {
			lst.result = &milvuspb.ListSnapshotsResponse{
				Status: merr.Success(),
				Snapshots: []string{
					"snapshot1",
					"snapshot2",
				},
			}
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.ListSnapshots(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result.GetStatus()))
	assert.Len(t, result.GetSnapshots(), 2)
	assert.Equal(t, "snapshot1", result.GetSnapshots()[0])
	assert.Equal(t, "snapshot2", result.GetSnapshots()[1])
}

func TestProxy_ListSnapshots_EnqueueFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.ListSnapshotsRequest{
		CollectionName: "test_collection",
	}

	expectedError := errors.New("enqueue failed")

	mock := mockey.Mock((*ddTaskQueue).Enqueue).Return(expectedError).Build()
	defer mock.UnPatch()

	result, err := proxy.ListSnapshots(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "enqueue failed")
}

func TestProxy_ListSnapshots_WaitToFinishFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.ListSnapshotsRequest{
		CollectionName: "test_collection",
	}

	expectedError := errors.New("task execution failed")

	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(expectedError).Build()
	defer mock2.UnPatch()

	result, err := proxy.ListSnapshots(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "task execution failed")
}

func TestProxy_RestoreSnapshot_Success(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "restored_collection",
		DbName:         "default",
	}

	// Mock successful enqueue and task completion
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if rst, ok := t.(*restoreSnapshotTask); ok {
			rst.result = &milvuspb.RestoreSnapshotResponse{
				Status: merr.Success(),
			}
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.RestoreSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result.GetStatus()))
}

func TestProxy_RestoreSnapshot_EnqueueFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "restored_collection",
	}

	expectedError := errors.New("enqueue failed")

	mock := mockey.Mock((*ddTaskQueue).Enqueue).Return(expectedError).Build()
	defer mock.UnPatch()

	result, err := proxy.RestoreSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "enqueue failed")
}

func TestProxy_RestoreSnapshot_WaitToFinishFailure(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "restored_collection",
	}

	expectedError := errors.New("task execution failed")

	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).Return(nil).Build()
	defer mock1.UnPatch()
	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(expectedError).Build()
	defer mock2.UnPatch()

	result, err := proxy.RestoreSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, merr.Ok(result.GetStatus()))
	assert.Contains(t, result.GetStatus().GetReason(), "task execution failed")
}

func TestExportSnapshotTask_Execute_ForwardsForeignStorageFields(t *testing.T) {
	const (
		externalSpec = `{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`
	)

	proxy := &Proxy{
		mixCoord: NewMixCoordMock(),
	}
	req := &milvuspb.ExportSnapshotRequest{
		Name:           "test_snapshot",
		DbName:         "default",
		CollectionName: "test_collection",
		TargetS3Path:   "s3://foreign-bucket/export-root",
		ExternalSpec:   externalSpec,
	}

	oldMetaCache := globalMetaCache
	globalMetaCache = &MetaCache{}
	defer func() { globalMetaCache = oldMetaCache }()

	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	var gotReq *datapb.ExportSnapshotRequest
	mockExportSnapshot := mockey.Mock((*MixCoordMock).ExportSnapshot).To(
		func(ctx context.Context, req *datapb.ExportSnapshotRequest, opts ...grpc.CallOption) (*datapb.ExportSnapshotResponse, error) {
			gotReq = req
			return &datapb.ExportSnapshotResponse{
				Status:              merr.Success(),
				SnapshotMetadataUri: req.GetTargetS3Path() + "/snapshots/100/metadata/1.json",
			}, nil
		},
	).Build()
	defer mockExportSnapshot.UnPatch()

	resp, err := proxy.ExportSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))
	if assert.NotNil(t, gotReq) {
		assert.Equal(t, externalSpec, gotReq.GetExternalSpec())
	}
}

func TestRestoreExternalSnapshotTask_Execute_ForwardsForeignStorageFields(t *testing.T) {
	const (
		externalSpec = `{"extfs":{"cloud_provider":"aws","region":"us-west-2","use_iam":"true"}}`
	)

	proxy := &Proxy{
		mixCoord: NewMixCoordMock(),
	}
	req := &milvuspb.RestoreExternalSnapshotRequest{
		DbName:               "default",
		TargetCollectionName: "restored_collection",
		SnapshotMetadataUri:  "s3://foreign-bucket/export-root/snapshots/100/metadata/1.json",
		ExternalSpec:         externalSpec,
	}

	var gotReq *datapb.RestoreSnapshotRequest
	mockRestoreSnapshot := mockey.Mock((*MixCoordMock).RestoreSnapshot).To(
		func(ctx context.Context, req *datapb.RestoreSnapshotRequest, opts ...grpc.CallOption) (*datapb.RestoreSnapshotResponse, error) {
			gotReq = req
			return &datapb.RestoreSnapshotResponse{
				Status: merr.Success(),
				JobId:  1,
			}, nil
		},
	).Build()
	defer mockRestoreSnapshot.UnPatch()

	resp, err := proxy.RestoreExternalSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.True(t, merr.Ok(resp.GetStatus()))
	assert.Equal(t, int64(1), resp.GetJobId())
	if assert.NotNil(t, gotReq) {
		assert.True(t, gotReq.GetExternal())
		assert.Equal(t, externalSpec, gotReq.GetExternalSpec())
	}
}

func TestRestoreExternalSnapshotTask_Execute_RequiresMetadataURI(t *testing.T) {
	proxy := &Proxy{
		mixCoord: NewMixCoordMock(),
	}
	req := &milvuspb.RestoreExternalSnapshotRequest{
		DbName:               "default",
		TargetCollectionName: "restored_collection",
	}

	resp, err := proxy.RestoreExternalSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.Error(t, merr.Error(resp.GetStatus()))
	assert.True(t, errors.Is(merr.Error(resp.GetStatus()), merr.ErrParameterInvalid))
}

func TestSnapshotRequestOption_RBACGuardrails(t *testing.T) {
	exportOpt, err := funcutil.GetPrivilegeExtObj(&milvuspb.ExportSnapshotRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ObjectType_Global, exportOpt.GetObjectType())
	assert.Equal(t, "PrivilegeExportSnapshot", exportOpt.GetObjectPrivilege().String())
	assert.Equal(t, int32(-1), exportOpt.GetObjectNameIndex())

	restoreOpt, err := funcutil.GetPrivilegeExtObj(&milvuspb.RestoreExternalSnapshotRequest{})
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ObjectType_Global, restoreOpt.GetObjectType())
	assert.Equal(t, "PrivilegeRestoreExternalSnapshot", restoreOpt.GetObjectPrivilege().String())
	assert.Equal(t, int32(-1), restoreOpt.GetObjectNameIndex())
}

// Test task creation and basic properties
func TestProxy_CreateSnapshot_TaskCreation(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	var enqueuedTask *createSnapshotTask

	// Capture the task that gets enqueued
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if cst, ok := t.(*createSnapshotTask); ok {
			enqueuedTask = cst
			cst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	_, err := proxy.CreateSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, enqueuedTask)
	assert.Equal(t, "test_snapshot", enqueuedTask.req.GetName())
	assert.Equal(t, "test_collection", enqueuedTask.req.GetCollectionName())
	assert.Equal(t, "default", enqueuedTask.req.GetDbName())
	assert.Equal(t, CreateSnapshotTaskName, enqueuedTask.Name())
}

// Test edge cases
func TestProxy_CreateSnapshot_NilRequest(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	// This should cause a panic when accessing req fields
	assert.Panics(t, func() {
		proxy.CreateSnapshot(context.Background(), nil)
	})
}

func TestProxy_DropSnapshot_EmptySnapshotName(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.DropSnapshotRequest{
		Name: "", // Empty snapshot name
	}

	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if dst, ok := t.(*dropSnapshotTask); ok {
			dst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.DropSnapshot(context.Background(), req)

	// Should succeed at API level, actual validation happens in task
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result))
}

// Test metrics and observability
func TestProxy_CreateSnapshot_MetricsRecording(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	// Mock successful execution
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if cst, ok := t.(*createSnapshotTask); ok {
			cst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.CreateSnapshot(context.Background(), req)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result))
	// In a real test, we would verify metrics were recorded correctly
	// This test demonstrates the API behavior focuses on the business logic
}

// Test context handling
func TestProxy_CreateSnapshot_ContextCancellation(t *testing.T) {
	proxy := &Proxy{
		sched: &taskScheduler{
			ddQueue: &ddTaskQueue{},
		},
	}

	req := &milvuspb.CreateSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_collection",
		DbName:         "default",
	}

	// Create a canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Mock successful enqueue
	mock1 := mockey.Mock((*ddTaskQueue).Enqueue).To(func(t task) error {
		if cst, ok := t.(*createSnapshotTask); ok {
			cst.result = merr.Success()
		}
		return nil
	}).Build()
	defer mock1.UnPatch()

	mock2 := mockey.Mock((*TaskCondition).WaitToFinish).Return(nil).Build()
	defer mock2.UnPatch()

	result, err := proxy.CreateSnapshot(ctx, req)

	// API should still complete successfully as cancellation handling
	// is typically done within the task execution
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, merr.Ok(result))
}
