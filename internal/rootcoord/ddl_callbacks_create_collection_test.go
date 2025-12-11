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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// --- Test restoreFromSnapshot ---
// Note: restoreFromSnapshot now reads snapshot metadata directly from header,
// so there's no DescribeSnapshot RPC error test needed.

func TestDDLCallback_RestoreFromSnapshot_NoPartitionsNoIndexes(t *testing.T) {
	ctx := context.Background()

	// Mock RestoreSnapshotData
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().RestoreSnapshotData(mock.Anything, mock.Anything).
		Return(&datapb.RestoreSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	callback := &DDLCallback{
		Core: newTestCore(withMixCoord(mockMixCoord), withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{},
		IndexInfos:            []*indexpb.IndexInfo{},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify - should succeed
	assert.NoError(t, err)
}

func TestDDLCallback_RestoreFromSnapshot_WithUserPartitions(t *testing.T) {
	ctx := context.Background()

	// Mock RestoreSnapshotData
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().RestoreSnapshotData(mock.Anything, mock.Anything).
		Return(&datapb.RestoreSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	callback := &DDLCallback{
		Core: newTestCore(withMixCoord(mockMixCoord), withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{101: "partition1", 102: "partition2"},
		IndexInfos:            []*indexpb.IndexInfo{},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Mock registry.CallMessageAckCallback to succeed
	callCount := 0
	mockCallback := mockey.Mock(registry.CallMessageAckCallback).To(func(ctx context.Context, msg message.BroadcastMutableMessage, results map[string]*message.AppendResult) error {
		callCount++
		return nil
	}).Build()
	defer mockCallback.UnPatch()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount) // Should be called twice for two partitions
}

func TestDDLCallback_RestoreFromSnapshot_PartitionAllocIDError(t *testing.T) {
	ctx := context.Background()

	callback := &DDLCallback{
		Core: newTestCore(withInvalidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{101: "partition1"},
		IndexInfos:            []*indexpb.IndexInfo{},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to allocate partition ID")
}

func TestDDLCallback_RestoreFromSnapshot_PartitionCallbackError(t *testing.T) {
	ctx := context.Background()

	callback := &DDLCallback{
		Core: newTestCore(withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{101: "partition1"},
		IndexInfos:            []*indexpb.IndexInfo{},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Mock registry.CallMessageAckCallback to fail
	mockCallback := mockey.Mock(registry.CallMessageAckCallback).
		Return(errors.New("partition callback failed")).Build()
	defer mockCallback.UnPatch()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to broadcast create partition message")
}

func TestDDLCallback_RestoreFromSnapshot_WithIndexes(t *testing.T) {
	ctx := context.Background()

	// Setup streaming WAL mock - required for index creation which calls streaming.WAL().ControlChannel()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	// Mock RestoreSnapshotData
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().RestoreSnapshotData(mock.Anything, mock.Anything).
		Return(&datapb.RestoreSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	callback := &DDLCallback{
		Core: newTestCore(withMixCoord(mockMixCoord), withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{},
		IndexInfos: []*indexpb.IndexInfo{
			{IndexID: 1001, IndexName: "vector_index", FieldID: 101},
			{IndexID: 1002, IndexName: "scalar_index", FieldID: 102},
		},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Mock registry.CallMessageAckCallback to succeed
	callCount := 0
	mockCallback := mockey.Mock(registry.CallMessageAckCallback).To(func(ctx context.Context, msg message.BroadcastMutableMessage, results map[string]*message.AppendResult) error {
		callCount++
		return nil
	}).Build()
	defer mockCallback.UnPatch()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount) // Should be called twice for two indexes
}

func TestDDLCallback_RestoreFromSnapshot_IndexCallbackError(t *testing.T) {
	ctx := context.Background()

	// Setup streaming WAL mock - required for index creation which calls streaming.WAL().ControlChannel()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	callback := &DDLCallback{
		Core: newTestCore(withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{},
		IndexInfos: []*indexpb.IndexInfo{
			{IndexID: 1001, IndexName: "vector_index", FieldID: 101},
		},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Mock registry.CallMessageAckCallback to fail
	mockCallback := mockey.Mock(registry.CallMessageAckCallback).
		Return(errors.New("index callback failed")).Build()
	defer mockCallback.UnPatch()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to broadcast create index message")
}

func TestDDLCallback_RestoreFromSnapshot_RestoreDataError(t *testing.T) {
	ctx := context.Background()

	// Mock RestoreSnapshotData to return error (but function should still succeed)
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().RestoreSnapshotData(mock.Anything, mock.Anything).
		Return(&datapb.RestoreSnapshotResponse{
			Status: merr.Status(errors.New("restore data error")),
		}, nil)

	callback := &DDLCallback{
		Core: newTestCore(withMixCoord(mockMixCoord), withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{},
		IndexInfos:            []*indexpb.IndexInfo{},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify - should still succeed (data restore is async)
	assert.NoError(t, err)
}

func TestDDLCallback_RestoreFromSnapshot_FullRestore(t *testing.T) {
	ctx := context.Background()

	// Setup streaming WAL mock - required for index creation which calls streaming.WAL().ControlChannel()
	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(funcutil.GetControlChannel("by-dev-rootcoord-dml_0")).Maybe()
	streaming.SetWALForTest(wal)

	// Mock RestoreSnapshotData
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().RestoreSnapshotData(mock.Anything, mock.Anything).
		Return(&datapb.RestoreSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	callback := &DDLCallback{
		Core: newTestCore(withMixCoord(mockMixCoord), withValidIDAllocator()),
	}

	// Snapshot metadata is now passed via header directly
	header := &message.CreateCollectionMessageHeader{
		CollectionId:          1001,
		DbId:                  1,
		SnapshotName:          "test_snapshot",
		JobId:                 2001,
		RestoreFromSnapshot:   true,
		UserCreatedPartitions: map[int64]string{101: "partition1"},
		IndexInfos: []*indexpb.IndexInfo{
			{IndexID: 1001, IndexName: "vector_index", FieldID: 101},
		},
	}
	body := &message.CreateCollectionRequest{
		DbName:         "default",
		CollectionName: "test_collection",
	}
	result := buildMockCreateCollectionResult()

	// Mock registry.CallMessageAckCallback to succeed
	callCount := 0
	mockCallback := mockey.Mock(registry.CallMessageAckCallback).To(func(ctx context.Context, msg message.BroadcastMutableMessage, results map[string]*message.AppendResult) error {
		callCount++
		return nil
	}).Build()
	defer mockCallback.UnPatch()

	// Execute
	err := callback.restoreFromSnapshot(ctx, header, body, result)

	// Verify
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount) // 1 partition + 1 index
}

// buildMockCreateCollectionResult builds a mock BroadcastResultCreateCollectionMessageV1 for testing
func buildMockCreateCollectionResult() message.BroadcastResultCreateCollectionMessageV1 {
	broadcastMsg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1001,
			PartitionIds: []int64{101},
			DbId:         1,
		}).
		WithBody(&message.CreateCollectionRequest{
			DbName:              "default",
			CollectionName:      "test_collection",
			VirtualChannelNames: []string{"ch1", "ch2"},
		}).
		WithBroadcast([]string{"control_channel", "ch1", "ch2"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastCreateCollectionMessageV1(broadcastMsg)
	return message.BroadcastResultCreateCollectionMessageV1{
		Message: typedMsg,
		Results: map[string]*message.AppendResult{
			"control_channel": {
				MessageID: nil,
				TimeTick:  100,
			},
			"ch1": {
				MessageID: nil,
				TimeTick:  100,
			},
			"ch2": {
				MessageID: nil,
				TimeTick:  100,
			},
		},
	}
}
