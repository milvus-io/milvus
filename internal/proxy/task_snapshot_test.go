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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
			Name: "test_snapshot",
		},
	}

	err := task.PreExecute(context.Background())

	assert.NoError(t, err) // PreExecute should always succeed for drop
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
	// Mock globalMetaCache calls
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

	// Mock DescribeSnapshot response
	mockSchema := &schemapb.CollectionSchema{
		Name: "restored_collection",
		Fields: []*schemapb.FieldSchema{
			{Name: "id", DataType: schemapb.DataType_Int64},
		},
	}
	mockDescribeResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		SnapshotInfo: &datapb.SnapshotInfo{
			Name:         "test_snapshot",
			CollectionId: 100,
		},
		CollectionInfo: &datapb.CollectionDescription{
			Schema:           mockSchema,
			NumShards:        2,
			NumPartitions:    1,
			ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockDescribeResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Mock proto.Marshal
	mockMarshal := mockey.Mock(proto.Marshal).Return([]byte("mock_schema"), nil).Build()
	defer mockMarshal.UnPatch()

	// Mock collection creation
	mockCreateCollection := mockey.Mock((*MixCoordMock).CreateCollection).Return(merr.Success(), nil).Build()
	defer mockCreateCollection.UnPatch()

	// Mock partition creation
	mockCreatePartition := mockey.Mock((*MixCoordMock).CreatePartition).Return(merr.Success(), nil).Build()
	defer mockCreatePartition.UnPatch()

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock globalMetaCache for getting collection ID after creation
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(200), nil).Build()
	defer mockGetCollectionID.UnPatch()
	mockGetPartitions := mockey.Mock((*MetaCache).GetPartitions).Return(map[string]int64{"partition1": 100}, nil).Build()
	defer mockGetPartitions.UnPatch()

	// Mock restore snapshot
	mockRestoreSnapshot := mockey.Mock((*MixCoordMock).RestoreSnapshot).Return(merr.Success(), nil).Build()
	defer mockRestoreSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.NoError(t, err)
	assert.NotNil(t, task.result)
	assert.True(t, merr.Ok(task.result))
}

func TestRestoreSnapshotTask_Execute_DescribeSnapshotError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	expectedError := errors.New("describe snapshot failed")
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(nil, expectedError).Build()
	defer mockDescribeSnapshot.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result))
	assert.Contains(t, err.Error(), "describe snapshot failed")
}

func TestRestoreSnapshotTask_Execute_SchemaMarshalError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	// Mock successful DescribeSnapshot
	mockDescribeResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		CollectionInfo: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test"},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockDescribeResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Mock proto.Marshal failure
	expectedError := errors.New("marshal failed")
	mockMarshal := mockey.Mock(proto.Marshal).Return(nil, expectedError).Build()
	defer mockMarshal.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result))
	assert.Contains(t, err.Error(), "marshal failed")
}

func TestRestoreSnapshotTask_Execute_CreateCollectionError(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	// Mock successful DescribeSnapshot
	mockDescribeResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		CollectionInfo: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test"},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockDescribeResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()

	// Mock successful marshal
	mockMarshal := mockey.Mock(proto.Marshal).Return([]byte("mock_schema"), nil).Build()
	defer mockMarshal.UnPatch()

	// Mock CreateCollection failure
	expectedError := errors.New("create collection failed")
	mockCreateCollection := mockey.Mock((*MixCoordMock).CreateCollection).Return(nil, expectedError).Build()
	defer mockCreateCollection.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.NotNil(t, task.result)
	assert.False(t, merr.Ok(task.result))
	assert.Contains(t, err.Error(), "create collection failed")
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

func TestRestoreSnapshotTask_CollectionCreationFailureWithCleanup(t *testing.T) {
	mockMixCoord := NewMixCoordMock()
	task := &restoreSnapshotTask{
		req: &milvuspb.RestoreSnapshotRequest{
			Name:           "test_snapshot",
			CollectionName: "restored_collection",
		},
		mixCoord: mockMixCoord,
	}

	// Mock successful DescribeSnapshot
	mockDescribeResponse := &datapb.DescribeSnapshotResponse{
		Status: merr.Success(),
		CollectionInfo: &datapb.CollectionDescription{
			Schema: &schemapb.CollectionSchema{Name: "test"},
		},
	}
	mockDescribeSnapshot := mockey.Mock((*MixCoordMock).DescribeSnapshot).Return(mockDescribeResponse, nil).Build()
	defer mockDescribeSnapshot.UnPatch()
	mockMarshal := mockey.Mock(proto.Marshal).Return([]byte("mock_schema"), nil).Build()
	defer mockMarshal.UnPatch()

	// Mock successful collection creation
	mockCreateCollection := mockey.Mock((*MixCoordMock).CreateCollection).Return(merr.Success(), nil).Build()
	defer mockCreateCollection.UnPatch()

	// Mock partition creation failure
	expectedError := errors.New("create partition failed")
	mockCreatePartition := mockey.Mock((*MixCoordMock).CreatePartition).Return(nil, expectedError).Build()
	defer mockCreatePartition.UnPatch()

	// Mock cleanup (DropCollection should be called)
	mockDropCollection := mockey.Mock((*MixCoordMock).DropCollection).Return(merr.Success(), nil).Build()
	defer mockDropCollection.UnPatch()

	// Initialize globalMetaCache
	globalMetaCache = &MetaCache{}
	// Mock globalMetaCache for getting collection ID after creation
	mockGetCollectionID := mockey.Mock((*MetaCache).GetCollectionID).Return(int64(200), nil).Build()
	defer mockGetCollectionID.UnPatch()
	mockGetPartitions := mockey.Mock((*MetaCache).GetPartitions).Return(map[string]int64{"partition1": 100}, nil).Build()
	defer mockGetPartitions.UnPatch()

	err := task.Execute(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create partition failed")
}
