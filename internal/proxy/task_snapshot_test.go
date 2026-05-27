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
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// =========================== CreateSnapshotTask Tests ===========================

func TestCreateSnapshotTask_OnEnqueue_Success(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "test_collection",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_CreateSnapshot, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestCreateSnapshotTask_OnEnqueue_BaseAlreadyExists(t *testing.T) {
	existingBase := &commonpb.MsgBase{
		MsgType:  commonpb.MsgType_Insert,
		SourceID: 123,
	}

	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Base:           existingBase,
			Name:           "test_snapshot",
			CollectionName: "test_collection",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.Equal(t, commonpb.MsgType_CreateSnapshot, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestCreateSnapshotTask_PreExecute_Success(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	// Mock globalMetaCache calls
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestCreateSnapshotTask_PreExecute_CollectionNotFound(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "nonexistent_collection",
		},
	}

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	expectedError := errors.New("collection not found")
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(0), expectedError).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection not found")
}

func TestCreateSnapshotTask_PreExecute_ProtectionNegative(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:                        "test_snapshot",
			DbName:                      "default",
			CollectionName:              "test_collection",
			CompactionProtectionSeconds: -1,
		},
	}

	err := task.PreExecute(context.Background())
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "non-negative"))
}

func TestCreateSnapshotTask_PreExecute_ProtectionExceedsMax(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:                        "test_snapshot",
			DbName:                      "default",
			CollectionName:              "test_collection",
			CompactionProtectionSeconds: 7*24*3600 + 1, // 7 days + 1 second
		},
	}

	err := task.PreExecute(context.Background())
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "must not exceed"))
}

func TestCreateSnapshotTask_PreExecute_ProtectionZero(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:                        "test_snapshot",
			DbName:                      "default",
			CollectionName:              "test_collection",
			CompactionProtectionSeconds: 0,
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())
	assert.NoError(t, err)
}

func TestCreateSnapshotTask_PreExecute_ProtectionValid(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:                        "test_snapshot",
			DbName:                      "default",
			CollectionName:              "test_collection",
			CompactionProtectionSeconds: 3600, // 1 hour
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestCreateSnapshotTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:        "test_snapshot",
			Description: "test description",
		},
		mixCoord:     mockMixCoord,
		collectionID: 100,
	}

	// Mock successful MixCoord call
	mockCreateSnapshot := mockey.Mock((*MixCoordMock).CreateSnapshot).Return(merr.Success(), nil).Build()
	defer mockCreateSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result))
}

func TestCreateSnapshotTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:        "test_snapshot",
			Description: "test description",
		},
		mixCoord:     mockMixCoord,
		collectionID: 100,
	}

	expectedError := errors.New("mixcoord create snapshot failed")
	mockCreateSnapshot := mockey.Mock((*MixCoordMock).CreateSnapshot).Return(nil, expectedError).Build()
	defer mockCreateSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord create snapshot failed")
}

func TestCreateSnapshotTask_PostExecute(t *testing.T) {
	task := &createSnapshotTask{}

	err := task.PostExecute(context.Background())

	assert.NoError(t, err)
}

func TestCreateSnapshotTask_TaskInterface(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Base: &commonpb.MsgBase{
				MsgID:     123,
				Timestamp: 456,
				MsgType:   commonpb.MsgType_CreateSnapshot,
			},
			Name:           "test_snapshot",
			CollectionName: "test_collection",
		},
		ctx: context.Background(),
	}

	// Test interface methods
	assert.Equal(t, CreateSnapshotTaskName, task.Name())
	assert.Equal(t, context.Background(), task.TraceCtx())
	assert.Equal(t, UniqueID(123), task.ID())
	assert.Equal(t, commonpb.MsgType_CreateSnapshot, task.Type())
	assert.Equal(t, Timestamp(456), task.BeginTs())
	assert.Equal(t, Timestamp(456), task.EndTs())

	// Test SetID and SetTs
	task.SetID(789)
	assert.Equal(t, UniqueID(789), task.ID())

	task.SetTs(999)
	assert.Equal(t, Timestamp(999), task.BeginTs())
}

// =========================== DropSnapshotTask Tests ===========================

func TestDropSnapshotTask_OnEnqueue_Success(t *testing.T) {
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name: "test_snapshot",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_DropSnapshot, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestDropSnapshotTask_PreExecute(t *testing.T) {
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestDropSnapshotTask_PreExecute_MissingCollectionName(t *testing.T) {
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name: "test_snapshot",
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection_name is required")
}

func TestDropSnapshotTask_PreExecute_CollectionNotFound(t *testing.T) {
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "nonexistent_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	expectedError := errors.New("collection not found")
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(0), expectedError).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection not found")
}

func TestDropSnapshotTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	mockDropSnapshot := mockey.Mock((*MixCoordMock).DropSnapshot).Return(merr.Success(), nil).Build()
	defer mockDropSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result))
}

func TestDropSnapshotTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &dropSnapshotTask{
		req: &milvuspb.DropSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("mixcoord drop snapshot failed")
	mockDropSnapshot := mockey.Mock((*MixCoordMock).DropSnapshot).Return(nil, expectedError).Build()
	defer mockDropSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord drop snapshot failed")
}

// =========================== DescribeSnapshotTask Tests ===========================

func TestDescribeSnapshotTask_OnEnqueue_Success(t *testing.T) {
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_DescribeSnapshot, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestDescribeSnapshotTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	// Mock MixCoord response
	mockResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		SnapshotInfo: &datapb.SnapshotInfo{
			Name:         "test_snapshot",
			Description:  "test description",
			CreateTs:     12345,
			CollectionId: 100,
			PartitionIds: []int64{1, 2},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock globalMetaCache calls
	mockGetCollectionName := mockey.Mock((*MetaCache).GetCollectionName).Return("test_collection", nil).Build()
	defer mockGetCollectionName.UnPatch()
	mockGetPartitionName := mockey.Mock((*MetaCache).GetPartitionName).To(func(ctx context.Context, database, collectionName string, partitionID int64) (string, error) {
		switch partitionID {
		case 1:
			return "partition1", nil
		case 2:
			return "partition2", nil
		default:
			return "", errors.New("partition not found")
		}
	}).Build()
	defer mockGetPartitionName.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Equal(t, "test_snapshot", task.result.GetName())
	assert.Equal(t, "test_collection", task.result.GetCollectionName())
	assert.Equal(t, []string{"partition1", "partition2"}, task.result.GetPartitionNames())
	assert.Equal(t, int64(12345), task.result.GetCreateTs())
}

func TestDescribeSnapshotTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("mixcoord describe snapshot failed")
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(nil, expectedError).Build()
	defer mockDescribeSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result.GetStatus()))
	assert.Contains(t, err.Error(), "mixcoord describe snapshot failed")
}

func TestDescribeSnapshotTask_Execute_CollectionNameResolutionError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	// Mock successful MixCoord response
	mockResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		SnapshotInfo: &datapb.SnapshotInfo{
			Name:         "test_snapshot",
			CollectionId: 100,
			PartitionIds: []int64{1},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock collection name resolution failure
	expectedError := errors.New("collection name resolution failed")
	mockGetCollectionName := mockey.Mock((*MetaCache).GetCollectionName).Return("", expectedError).Build()
	defer mockGetCollectionName.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection name resolution failed")
}

func TestDescribeSnapshotTask_Execute_PartitionNameResolutionError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name: "test_snapshot",
		},
		mixCoord: mockMixCoord,
	}

	// Mock successful MixCoord response
	mockResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		SnapshotInfo: &datapb.SnapshotInfo{
			Name:         "test_snapshot",
			CollectionId: 100,
			PartitionIds: []int64{1, 2},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock successful collection name resolution
	mockGetCollectionName := mockey.Mock((*MetaCache).GetCollectionName).Return("test_collection", nil).Build()
	defer mockGetCollectionName.UnPatch()

	// Mock partition name resolution failure (should not stop execution, just warn)
	mockGetPartitionName := mockey.Mock((*MetaCache).GetPartitionName).Return("", errors.New("partition name resolution failed")).Build()
	defer mockGetPartitionName.UnPatch()

	err := task.Execute(context.Background())

	// Should succeed but with empty partition names
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Equal(t, "test_collection", task.result.GetCollectionName())
	assert.Len(t, task.result.GetPartitionNames(), 2) // Should have empty strings
}

// =========================== ListSnapshotsTask Tests ===========================

func TestListSnapshotsTask_OnEnqueue_Success(t *testing.T) {
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			CollectionName: "test_collection",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_ListSnapshots, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestListSnapshotsTask_PreExecute_Success(t *testing.T) {
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{dbID: 1}, nil).Build()
	defer mockGetDBInfo.UnPatch()
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestListSnapshotsTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			CollectionName: "test_collection",
		},
		mixCoord:     mockMixCoord,
		collectionID: 100,
	}

	// Mock MixCoord response
	mockResponse := &datapb.ListSnapshotsResponse{
		Status:    merr.Success(),
		Snapshots: []string{"snapshot1", "snapshot2"},
	}
	mockListSnapshots := mockey.Mock((*MixCoordMock).ListSnapshots).Return(mockResponse, nil).Build()
	defer mockListSnapshots.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Equal(t, []string{"snapshot1", "snapshot2"}, task.result.GetSnapshots())
}

// =========================== RestoreSnapshotTask Tests ===========================

func TestRestoreSnapshotTask_OnEnqueue_Success(t *testing.T) {
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_RestoreSnapshot, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestRestoreSnapshotTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
			DbName:         "default",
		},
		mixCoord: mockMixCoord,
	}

	// Mock RestoreSnapshot - proxy directly calls DataCoord
	mockRestoreSnapshot := mockey.Mock((*MixCoordMock).RestoreSnapshot).Return(&datapb.RestoreSnapshotResponse{
		Status: merr.Success(),
		JobId:  1,
	}, nil).Build()
	defer mockRestoreSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Equal(t, int64(1), task.result.GetJobId())
}

func TestRestoreSnapshotTask_Execute_DataCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	// Mock RestoreSnapshot to return RPC error
	expectedError := errors.New("datacoord restore snapshot failed")
	mockRestoreSnapshot := mockey.Mock((*MixCoordMock).RestoreSnapshot).Return(nil, expectedError).Build()
	defer mockRestoreSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result.GetStatus()))
	assert.Contains(t, err.Error(), "datacoord restore snapshot failed")
}

func TestRestoreSnapshotTask_Execute_StatusError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	// Mock RestoreSnapshot to return error status
	mockRestoreSnapshot := mockey.Mock((*MixCoordMock).RestoreSnapshot).Return(&datapb.RestoreSnapshotResponse{
		Status: merr.Status(merr.WrapErrCollectionNotFound("test_collection")),
	}, nil).Build()
	defer mockRestoreSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result.GetStatus()))
}

// =========================== Task Lifecycle Integration Tests ===========================

func TestCreateSnapshotTask_FullLifecycle(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			DbName:         "default",
			CollectionName: "test_collection",
			Name:           "test_snapshot",
			Description:    "test description",
		},
		ctx:      context.Background(),
		mixCoord: mockMixCoord,
	}

	// Test OnEnqueue
	err := task.OnEnqueue()
	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock PreExecute dependencies
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	// Test PreExecute
	err = task.PreExecute(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)

	// Mock Execute dependencies
	mockCreateSnapshot := mockey.Mock((*MixCoordMock).CreateSnapshot).Return(merr.Success(), nil).Build()
	defer mockCreateSnapshot.UnPatch()

	// Test Execute
	err = task.Execute(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result))

	// Test PostExecute
	err = task.PostExecute(context.Background())
	assert.NoError(t, err)
}

// =========================== Edge Cases and Error Scenarios ===========================

func TestCreateSnapshotTask_EmptyPartitionNames(t *testing.T) {
	task := &createSnapshotTask{
		req: &milvuspb.CreateSnapshotRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

// =========================== Validation Tests for Issue #47068 ===========================

func TestSnapshotTasks_PreExecute_InvalidNames(t *testing.T) {
	// Common invalid snapshot name test cases
	invalidSnapshotNames := []struct {
		name           string
		snapshotName   string
		expectedErrMsg string
	}{
		// Empty/whitespace cases
		{"empty string", "", "snapshot name should be not empty"},
		{"single space", " ", "snapshot name should be not empty"},
		{"multiple spaces", "   ", "snapshot name should be not empty"},
		{"tab character", "\t", "snapshot name should be not empty"},
		// Invalid first character
		{"starts with number", "123snapshot", "the first character of snapshot name must be an underscore or letter"},
		{"starts with special char", "$snapshot", "the first character of snapshot name must be an underscore or letter"},
		{"starts with hyphen", "-snapshot", "the first character of snapshot name must be an underscore or letter"},
		// Invalid characters in name
		{"contains space", "snap shot", "snapshot name can only contain"},
		{"contains special char", "snap@shot", "snapshot name can only contain"},
		{"contains chinese", "快照test", "the first character of snapshot name must be an underscore or letter"},
		// Too long name (exceeds 255 characters)
		{"too long name", strings.Repeat("a", 256), "the length of snapshot name must be not greater than limit"},
	}

	// Test CreateSnapshotTask
	t.Run("CreateSnapshotTask", func(t *testing.T) {
		for _, tc := range invalidSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &createSnapshotTask{
					req: &milvuspb.CreateSnapshotRequest{
						Name:           tc.snapshotName,
						DbName:         "default",
						CollectionName: "test_collection",
					},
				}

				err := task.PreExecute(context.Background())

				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrMsg)
			})
		}
	})

	// Test DropSnapshotTask
	t.Run("DropSnapshotTask", func(t *testing.T) {
		for _, tc := range invalidSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &dropSnapshotTask{
					req: &milvuspb.DropSnapshotRequest{
						Name: tc.snapshotName,
					},
				}

				err := task.PreExecute(context.Background())

				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrMsg)
			})
		}
	})

	// Test DescribeSnapshotTask
	t.Run("DescribeSnapshotTask", func(t *testing.T) {
		for _, tc := range invalidSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &describeSnapshotTask{
					req: &milvuspb.DescribeSnapshotRequest{
						Name: tc.snapshotName,
					},
				}

				err := task.PreExecute(context.Background())

				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrMsg)
			})
		}
	})

	// Test RestoreSnapshotTask - snapshot name validation
	t.Run("RestoreSnapshotTask/SnapshotName", func(t *testing.T) {
		for _, tc := range invalidSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &restoreSnapshotTask{
					req: &milvuspb.RestoreSnapshotRequest{
						Name:           tc.snapshotName,
						CollectionName: "valid_collection",
					},
				}

				err := task.PreExecute(context.Background())

				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrMsg)
			})
		}
	})

	// Test RestoreSnapshotTask - collection name validation
	t.Run("RestoreSnapshotTask/CollectionName", func(t *testing.T) {
		invalidCollectionNames := []struct {
			name           string
			collectionName string
			expectedErrMsg string
		}{
			{"starts with number", "123collection", "the first character of collection name must be an underscore or letter"},
			{"starts with special char", "$collection", "the first character of collection name must be an underscore or letter"},
			{"contains space", "coll ection", "collection name can only contain"},
			{"contains special char", "coll@ection", "collection name can only contain"},
			{"too long name", strings.Repeat("a", 256), "the length of collection name must be not greater than limit"},
		}

		for _, tc := range invalidCollectionNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &restoreSnapshotTask{
					req: &milvuspb.RestoreSnapshotRequest{
						Name:                 "valid_snapshot",
						CollectionName:       "source_collection",
						TargetCollectionName: tc.collectionName,
					},
				}

				err := task.PreExecute(context.Background())

				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrMsg)
			})
		}
	})
}

func TestSnapshotTasks_PreExecute_ValidNames(t *testing.T) {
	// Common valid snapshot name test cases
	validSnapshotNames := []struct {
		name         string
		snapshotName string
	}{
		{"simple name", "snapshot"},
		{"with underscore prefix", "_snapshot"},
		{"with numbers", "snapshot123"},
		{"mixed", "_snap_shot_123"},
		{"uppercase", "Snapshot"},
		{"with dollar sign", "snapshot$test"}, // $ is allowed by default config
	}

	// Test CreateSnapshotTask
	t.Run("CreateSnapshotTask", func(t *testing.T) {
		for _, tc := range validSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &createSnapshotTask{
					req: &milvuspb.CreateSnapshotRequest{
						Name:           tc.snapshotName,
						DbName:         "default",
						CollectionName: "test_collection",
					},
				}

				// Mock globalMetaCache
				globalMetaCache = &MetaCache{}
				mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
				defer mockGetCollectionID.UnPatch()

				err := task.PreExecute(context.Background())

				assert.NoError(t, err)
			})
		}
	})

	// Test DropSnapshotTask
	t.Run("DropSnapshotTask", func(t *testing.T) {
		for _, tc := range validSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &dropSnapshotTask{
					req: &milvuspb.DropSnapshotRequest{
						Name:           tc.snapshotName,
						DbName:         "default",
						CollectionName: "test_collection",
					},
				}

				globalMetaCache = &MetaCache{}
				mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
				defer mockGetCollectionID.UnPatch()

				err := task.PreExecute(context.Background())

				assert.NoError(t, err)
			})
		}
	})

	// Test DescribeSnapshotTask
	t.Run("DescribeSnapshotTask", func(t *testing.T) {
		for _, tc := range validSnapshotNames {
			t.Run(tc.name, func(t *testing.T) {
				task := &describeSnapshotTask{
					req: &milvuspb.DescribeSnapshotRequest{
						Name:           tc.snapshotName,
						DbName:         "default",
						CollectionName: "test_collection",
					},
				}

				globalMetaCache = &MetaCache{}
				mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
				defer mockGetCollectionID.UnPatch()

				err := task.PreExecute(context.Background())

				assert.NoError(t, err)
			})
		}
	})

	// Test RestoreSnapshotTask
	t.Run("RestoreSnapshotTask", func(t *testing.T) {
		testCases := []struct {
			name                 string
			snapshotName         string
			collectionName       string
			targetCollectionName string
		}{
			{"both valid", "snapshot_1", "collection_1", "target_coll"},
			{"with underscores", "_snapshot", "_collection", "_target"},
			{"mixed", "Snap_123", "Coll_456", "Target_789"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				task := &restoreSnapshotTask{
					req: &milvuspb.RestoreSnapshotRequest{
						Name:                 tc.snapshotName,
						CollectionName:       tc.collectionName,
						TargetCollectionName: tc.targetCollectionName,
					},
				}

				globalMetaCache = &MetaCache{}
				mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
				defer mockGetCollectionID.UnPatch()

				err := task.PreExecute(context.Background())

				assert.NoError(t, err)
			})
		}
	})
}

// =========================== Test for Issue #47066 ===========================

func TestListSnapshotsTask_PreExecute_EmptyCollectionName(t *testing.T) {
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			DbName:         "default",
			CollectionName: "", // Empty collection name should be rejected
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).Return(&databaseInfo{dbID: 1}, nil).Build()
	defer mockGetDBInfo.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection_name is required")
}

// =========================== resolveCollectionNames Tests ===========================

func TestResolveCollectionNames_ZeroCollectionID(t *testing.T) {
	dbName, collName := resolveCollectionNames(context.Background(), 0)
	assert.Equal(t, "", dbName)
	assert.Equal(t, "", collName)
}

func TestResolveCollectionNames_GetCollectionInfoError(t *testing.T) {
	globalMetaCache = &MetaCache{}
	mockGetCollectionInfo := mockey.Mock((*MetaCache).GetCollectionInfo).
		Return(nil, errors.New("collection not found")).Build()
	defer mockGetCollectionInfo.UnPatch()

	dbName, collName := resolveCollectionNames(context.Background(), 100)
	assert.Equal(t, "", dbName)
	assert.Equal(t, "", collName)
}

func TestResolveCollectionNames_Success(t *testing.T) {
	globalMetaCache = &MetaCache{}
	mockGetCollectionInfo := mockey.Mock((*MetaCache).GetCollectionInfo).
		Return(&collectionInfo{
			dbName: "test_db",
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Name: "test_collection",
				},
			},
		}, nil).Build()
	defer mockGetCollectionInfo.UnPatch()

	dbName, collName := resolveCollectionNames(context.Background(), 100)
	assert.Equal(t, "test_db", dbName)
	assert.Equal(t, "test_collection", collName)
}

// =========================== restoreSnapshotTask.PreExecute Additional Tests ===========================

func TestRestoreSnapshotTask_PreExecute_MissingSourceCollectionName(t *testing.T) {
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:                 "valid_snapshot",
			CollectionName:       "", // empty source collection name
			TargetCollectionName: "target_collection",
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection_name is required")
}

func TestRestoreSnapshotTask_PreExecute_MissingTargetCollectionName(t *testing.T) {
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:                 "valid_snapshot",
			CollectionName:       "source_collection",
			TargetCollectionName: "", // empty target collection name
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "target_collection_name is required")
}

func TestRestoreSnapshotTask_PreExecute_GetCollectionIDError(t *testing.T) {
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:                 "valid_snapshot",
			CollectionName:       "source_collection",
			TargetCollectionName: "target_collection",
			DbName:               "default",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).
		Return(int64(0), errors.New("source collection not found")).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "source collection not found")
}

// =========================== describeSnapshotTask.PreExecute Additional Tests ===========================

func TestDescribeSnapshotTask_PreExecute_MissingCollectionName(t *testing.T) {
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name:           "valid_snapshot",
			CollectionName: "", // empty collection name
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection_name is required")
}

func TestDescribeSnapshotTask_PreExecute_GetCollectionIDError(t *testing.T) {
	task := &describeSnapshotTask{
		req: &milvuspb.DescribeSnapshotRequest{
			Name:           "valid_snapshot",
			DbName:         "default",
			CollectionName: "nonexistent_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).
		Return(int64(0), errors.New("collection not found")).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection not found")
}

// =========================== listSnapshotsTask.PreExecute Additional Tests ===========================

func TestListSnapshotsTask_PreExecute_GetDatabaseInfoError(t *testing.T) {
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			DbName: "nonexistent_db",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(nil, errors.New("database not found")).Build()
	defer mockGetDBInfo.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database not found")
}

func TestListSnapshotsTask_PreExecute_GetCollectionIDError(t *testing.T) {
	task := &listSnapshotsTask{
		req: &milvuspb.ListSnapshotsRequest{
			DbName:         "default",
			CollectionName: "nonexistent_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(&databaseInfo{dbID: 1}, nil).Build()
	defer mockGetDBInfo.UnPatch()
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).
		Return(int64(0), errors.New("collection not found")).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection not found")
}

// =========================== getRestoreSnapshotStateTask Tests ===========================

func TestGetRestoreSnapshotStateTask_OnEnqueue(t *testing.T) {
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: 1,
		},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_GetRestoreSnapshotState, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestGetRestoreSnapshotStateTask_PreExecute(t *testing.T) {
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: 1,
		},
	}

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
}

func TestGetRestoreSnapshotStateTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: 1,
		},
		mixCoord: mockMixCoord,
	}

	// Mock GetRestoreSnapshotState with info containing a collectionId
	mockResponse := &datapb.GetRestoreSnapshotStateResponse{
		Status: merr.Success(),
		Info: &datapb.RestoreSnapshotInfo{
			JobId:        1,
			SnapshotName: "snap1",
			CollectionId: 200,
			State:        datapb.RestoreSnapshotState_RestoreSnapshotCompleted,
			Progress:     100,
			StartTime:    12345,
			TimeCost:     5000,
		},
	}
	mockGetState := mockey.Mock((*MixCoordMock).GetRestoreSnapshotState).
		Return(mockResponse, nil).Build()
	defer mockGetState.UnPatch()

	// Mock resolveCollectionNames via globalMetaCache.GetCollectionInfo
	globalMetaCache = &MetaCache{}
	mockGetCollectionInfo := mockey.Mock((*MetaCache).GetCollectionInfo).
		Return(&collectionInfo{
			dbName: "test_db",
			schema: &schemaInfo{
				CollectionSchema: &schemapb.CollectionSchema{
					Name: "test_collection",
				},
			},
		}, nil).Build()
	defer mockGetCollectionInfo.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.NotNil(t, task.result.GetInfo())
	assert.Equal(t, int64(1), task.result.GetInfo().GetJobId())
	assert.Equal(t, "snap1", task.result.GetInfo().GetSnapshotName())
	assert.Equal(t, "test_db", task.result.GetInfo().GetDbName())
	assert.Equal(t, "test_collection", task.result.GetInfo().GetCollectionName())
	assert.Equal(t, int32(100), task.result.GetInfo().GetProgress())
}

func TestGetRestoreSnapshotStateTask_Execute_NilInfo(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: 1,
		},
		mixCoord: mockMixCoord,
	}

	// Mock GetRestoreSnapshotState with nil info
	mockResponse := &datapb.GetRestoreSnapshotStateResponse{
		Status: merr.Success(),
		Info:   nil,
	}
	mockGetState := mockey.Mock((*MixCoordMock).GetRestoreSnapshotState).
		Return(mockResponse, nil).Build()
	defer mockGetState.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Nil(t, task.result.GetInfo())
}

func TestGetRestoreSnapshotStateTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			JobId: 1,
		},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("mixcoord get restore snapshot state failed")
	mockGetState := mockey.Mock((*MixCoordMock).GetRestoreSnapshotState).
		Return(nil, expectedError).Build()
	defer mockGetState.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord get restore snapshot state failed")
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result.GetStatus()))
}

func TestGetRestoreSnapshotStateTask_TaskInterface(t *testing.T) {
	task := &getRestoreSnapshotStateTask{
		req: &milvuspb.GetRestoreSnapshotStateRequest{
			Base: &commonpb.MsgBase{
				MsgID:     123,
				Timestamp: 456,
				MsgType:   commonpb.MsgType_GetRestoreSnapshotState,
			},
			JobId: 1,
		},
		ctx: context.Background(),
	}

	assert.Equal(t, GetRestoreSnapshotStateTaskName, task.Name())
	assert.Equal(t, context.Background(), task.TraceCtx())
	assert.Equal(t, UniqueID(123), task.ID())
	assert.Equal(t, commonpb.MsgType_GetRestoreSnapshotState, task.Type())
	assert.Equal(t, Timestamp(456), task.BeginTs())
	assert.Equal(t, Timestamp(456), task.EndTs())

	task.SetID(789)
	assert.Equal(t, UniqueID(789), task.ID())

	task.SetTs(999)
	assert.Equal(t, Timestamp(999), task.BeginTs())

	err := task.PostExecute(context.Background())
	assert.NoError(t, err)
}

// =========================== listRestoreSnapshotJobsTask Tests ===========================

func TestListRestoreSnapshotJobsTask_OnEnqueue(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{},
	}

	err := task.OnEnqueue()

	assert.NoError(t, err)
	assert.NotNil(t, task.req.Base)
	assert.Equal(t, commonpb.MsgType_ListRestoreSnapshotJobs, task.req.Base.MsgType)
	assert.Equal(t, paramtable.GetNodeID(), task.req.Base.SourceID)
}

func TestListRestoreSnapshotJobsTask_PreExecute_DbNameError(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{
			DbName: "nonexistent_db",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(nil, errors.New("database not found")).Build()
	defer mockGetDBInfo.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database not found")
}

func TestListRestoreSnapshotJobsTask_PreExecute_CollectionNameError(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{
			DbName:         "default",
			CollectionName: "nonexistent_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(&databaseInfo{dbID: 1}, nil).Build()
	defer mockGetDBInfo.UnPatch()
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).
		Return(int64(0), errors.New("collection not found")).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection not found")
}

func TestListRestoreSnapshotJobsTask_PreExecute_BothDbAndCollectionSuccess(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(&databaseInfo{dbID: 5}, nil).Build()
	defer mockGetDBInfo.UnPatch()
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).
		Return(int64(200), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(5), task.dbID)
	assert.Equal(t, int64(200), task.collectionID)
}

func TestListRestoreSnapshotJobsTask_PreExecute_OnlyDbName(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{
			DbName: "default",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetDBInfo := mockey.Mock((*MetaCache).GetDatabaseInfo).
		Return(&databaseInfo{dbID: 5}, nil).Build()
	defer mockGetDBInfo.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(5), task.dbID)
	assert.Equal(t, int64(0), task.collectionID) // no collection filter
}

func TestListRestoreSnapshotJobsTask_PreExecute_Empty(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{},
	}

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(0), task.dbID)
	assert.Equal(t, int64(0), task.collectionID)
}

func TestListRestoreSnapshotJobsTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &listRestoreSnapshotJobsTask{
		req:          &milvuspb.ListRestoreSnapshotJobsRequest{},
		mixCoord:     mockMixCoord,
		collectionID: 0,
		dbID:         0,
	}

	// Mock ListRestoreSnapshotJobs with multiple jobs
	mockResponse := &datapb.ListRestoreSnapshotJobsResponse{
		Status: merr.Success(),
		Jobs: []*datapb.RestoreSnapshotInfo{
			{
				JobId:        1,
				SnapshotName: "snap1",
				CollectionId: 100,
				State:        datapb.RestoreSnapshotState_RestoreSnapshotCompleted,
				Progress:     100,
			},
			{
				JobId:        2,
				SnapshotName: "snap2",
				CollectionId: 200,
				State:        datapb.RestoreSnapshotState_RestoreSnapshotExecuting,
				Progress:     50,
			},
		},
	}
	mockListJobs := mockey.Mock((*MixCoordMock).ListRestoreSnapshotJobs).
		Return(mockResponse, nil).Build()
	defer mockListJobs.UnPatch()

	// Mock resolveCollectionNames for both collections
	globalMetaCache = &MetaCache{}
	callCount := 0
	mockGetCollectionInfo := mockey.Mock((*MetaCache).GetCollectionInfo).
		To(func(ctx context.Context, database string, collectionName string, collectionID int64) (*collectionInfo, error) {
			callCount++
			switch collectionID {
			case 100:
				return &collectionInfo{
					dbName: "db1",
					schema: &schemaInfo{
						CollectionSchema: &schemapb.CollectionSchema{Name: "coll1"},
					},
				}, nil
			case 200:
				return &collectionInfo{
					dbName: "db2",
					schema: &schemaInfo{
						CollectionSchema: &schemapb.CollectionSchema{Name: "coll2"},
					},
				}, nil
			default:
				return nil, errors.New("unknown collection")
			}
		}).Build()
	defer mockGetCollectionInfo.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Len(t, task.result.GetJobs(), 2)

	job1 := task.result.GetJobs()[0]
	assert.Equal(t, int64(1), job1.GetJobId())
	assert.Equal(t, "snap1", job1.GetSnapshotName())
	assert.Equal(t, "db1", job1.GetDbName())
	assert.Equal(t, "coll1", job1.GetCollectionName())
	assert.Equal(t, int32(100), job1.GetProgress())

	job2 := task.result.GetJobs()[1]
	assert.Equal(t, int64(2), job2.GetJobId())
	assert.Equal(t, "snap2", job2.GetSnapshotName())
	assert.Equal(t, "db2", job2.GetDbName())
	assert.Equal(t, "coll2", job2.GetCollectionName())
	assert.Equal(t, int32(50), job2.GetProgress())
}

func TestListRestoreSnapshotJobsTask_Execute_EmptyJobs(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &listRestoreSnapshotJobsTask{
		req:      &milvuspb.ListRestoreSnapshotJobsRequest{},
		mixCoord: mockMixCoord,
	}

	mockResponse := &datapb.ListRestoreSnapshotJobsResponse{
		Status: merr.Success(),
		Jobs:   []*datapb.RestoreSnapshotInfo{},
	}
	mockListJobs := mockey.Mock((*MixCoordMock).ListRestoreSnapshotJobs).
		Return(mockResponse, nil).Build()
	defer mockListJobs.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Empty(t, task.result.GetJobs())
}

func TestListRestoreSnapshotJobsTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &listRestoreSnapshotJobsTask{
		req:      &milvuspb.ListRestoreSnapshotJobsRequest{},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("mixcoord list restore jobs failed")
	mockListJobs := mockey.Mock((*MixCoordMock).ListRestoreSnapshotJobs).
		Return(nil, expectedError).Build()
	defer mockListJobs.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord list restore jobs failed")
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result.GetStatus()))
}

func TestListRestoreSnapshotJobsTask_Execute_ResolveCollectionNamesFallback(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &listRestoreSnapshotJobsTask{
		req:      &milvuspb.ListRestoreSnapshotJobsRequest{},
		mixCoord: mockMixCoord,
	}

	// Job with collectionId=0 should get empty names from resolveCollectionNames
	mockResponse := &datapb.ListRestoreSnapshotJobsResponse{
		Status: merr.Success(),
		Jobs: []*datapb.RestoreSnapshotInfo{
			{
				JobId:        1,
				SnapshotName: "snap1",
				CollectionId: 0, // zero means resolveCollectionNames returns empty
			},
		},
	}
	mockListJobs := mockey.Mock((*MixCoordMock).ListRestoreSnapshotJobs).
		Return(mockResponse, nil).Build()
	defer mockListJobs.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.Len(t, task.result.GetJobs(), 1)
	assert.Equal(t, "", task.result.GetJobs()[0].GetDbName())
	assert.Equal(t, "", task.result.GetJobs()[0].GetCollectionName())
}

func TestListRestoreSnapshotJobsTask_TaskInterface(t *testing.T) {
	task := &listRestoreSnapshotJobsTask{
		req: &milvuspb.ListRestoreSnapshotJobsRequest{
			Base: &commonpb.MsgBase{
				MsgID:     123,
				Timestamp: 456,
				MsgType:   commonpb.MsgType_ListRestoreSnapshotJobs,
			},
		},
		ctx: context.Background(),
	}

	assert.Equal(t, ListRestoreSnapshotJobsTaskName, task.Name())
	assert.Equal(t, context.Background(), task.TraceCtx())
	assert.Equal(t, UniqueID(123), task.ID())
	assert.Equal(t, commonpb.MsgType_ListRestoreSnapshotJobs, task.Type())
	assert.Equal(t, Timestamp(456), task.BeginTs())
	assert.Equal(t, Timestamp(456), task.EndTs())

	task.SetID(789)
	assert.Equal(t, UniqueID(789), task.ID())

	task.SetTs(999)
	assert.Equal(t, Timestamp(999), task.BeginTs())

	err := task.PostExecute(context.Background())
	assert.NoError(t, err)
}

// =========================== PinSnapshotDataTask Tests ===========================

func TestPinSnapshotDataTask_PreExecute_Success(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestPinSnapshotDataTask_PreExecute_EmptyCollectionName(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			CollectionName: "",
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection_name is required")
}

func TestPinSnapshotDataTask_PreExecute_EmptyName(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "",
			CollectionName: "test_collection",
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
}

func TestPinSnapshotDataTask_PreExecute_NegativeTTL(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			CollectionName: "test_collection",
			TtlSeconds:     -1,
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ttl_seconds must be non-negative")
}

func TestPinSnapshotDataTask_PreExecute_TTLExceedsMax(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			CollectionName: "test_collection",
			TtlSeconds:     maxPinTTLSeconds + 1,
		},
	}

	err := task.PreExecute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ttl_seconds exceeds maximum")
}

func TestPinSnapshotDataTask_PreExecute_TTLAtMaxBoundary(t *testing.T) {
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			DbName:         "default",
			CollectionName: "test_collection",
			TtlSeconds:     maxPinTTLSeconds,
		},
	}

	globalMetaCache = &MetaCache{}
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(100), nil).Build()
	defer mockGetCollectionID.UnPatch()

	err := task.PreExecute(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, int64(100), task.collectionID)
}

func TestPinSnapshotDataTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			CollectionName: "test_collection",
			TtlSeconds:     3600,
		},
		mixCoord:     mockMixCoord,
		collectionID: 100,
	}

	mockPin := mockey.Mock((*MixCoordMock).PinSnapshotData).Return(&datapb.PinSnapshotDataResponse{
		Status: merr.Success(),
		PinId:  12345,
	}, nil).Build()
	defer mockPin.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result.GetStatus()))
	assert.Equal(t, int64(12345), task.result.GetPinId())
}

func TestPinSnapshotDataTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &pinSnapshotDataTask{
		req: &milvuspb.PinSnapshotDataRequest{
			Name:           "test_snapshot",
			CollectionName: "test_collection",
		},
		mixCoord:     mockMixCoord,
		collectionID: 100,
	}

	expectedError := errors.New("mixcoord pin snapshot failed")
	mockPin := mockey.Mock((*MixCoordMock).PinSnapshotData).Return(nil, expectedError).Build()
	defer mockPin.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord pin snapshot failed")
}

// =========================== UnpinSnapshotDataTask Tests ===========================

func TestUnpinSnapshotDataTask_PreExecute_Success(t *testing.T) {
	task := &unpinSnapshotDataTask{
		req: &milvuspb.UnpinSnapshotDataRequest{
			PinId: 5001,
		},
	}

	err := task.PreExecute(context.Background())
	assert.NoError(t, err)
}

func TestUnpinSnapshotDataTask_PreExecute_ZeroPinID(t *testing.T) {
	task := &unpinSnapshotDataTask{
		req: &milvuspb.UnpinSnapshotDataRequest{
			PinId: 0,
		},
	}

	err := task.PreExecute(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pin_id is required")
}

func TestUnpinSnapshotDataTask_Execute_Success(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &unpinSnapshotDataTask{
		req: &milvuspb.UnpinSnapshotDataRequest{
			PinId: 5001,
		},
		mixCoord: mockMixCoord,
	}

	mockUnpin := mockey.Mock((*MixCoordMock).UnpinSnapshotData).Return(merr.Success(), nil).Build()
	defer mockUnpin.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result))
}

func TestUnpinSnapshotDataTask_Execute_MixCoordError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &unpinSnapshotDataTask{
		req: &milvuspb.UnpinSnapshotDataRequest{
			PinId: 5001,
		},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("mixcoord unpin failed")
	mockUnpin := mockey.Mock((*MixCoordMock).UnpinSnapshotData).Return(nil, expectedError).Build()
	defer mockUnpin.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mixcoord unpin failed")
}
