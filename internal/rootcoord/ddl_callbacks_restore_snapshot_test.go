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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// --- Test validateCollectionNotExists ---

func TestCore_ValidateCollectionNotExists_CollectionExists(t *testing.T) {
	ctx := context.Background()

	// Setup mock meta
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "", assert.AnError // Alias does not exist
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		// Collection exists
		return &model.Collection{
			Name: "test_collection",
		}, nil
	}

	core := newTestCore(withMeta(meta))

	// Execute
	err := core.validateCollectionNotExists(ctx, "test_db", "test_collection")

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCore_ValidateCollectionNotExists_AliasConflict(t *testing.T) {
	ctx := context.Background()

	// Setup mock meta
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		// Alias exists
		return "real_collection", nil
	}

	core := newTestCore(withMeta(meta))

	// Execute
	err := core.validateCollectionNotExists(ctx, "test_db", "test_alias")

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts with an existing alias")
}

func TestCore_ValidateCollectionNotExists_Success(t *testing.T) {
	ctx := context.Background()

	// Setup mock meta
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "", assert.AnError // Alias does not exist
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return nil, assert.AnError // Collection does not exist
	}

	core := newTestCore(withMeta(meta))

	// Execute
	err := core.validateCollectionNotExists(ctx, "test_db", "new_collection")

	// Verify
	assert.NoError(t, err)
}

// --- Test broadcastRestoreSnapshotV2 ---

func TestCore_BroadcastRestoreSnapshotV2_SnapshotNotFound(t *testing.T) {
	ctx := context.Background()

	// Mock mixCoord.DescribeSnapshot to return error (snapshot not found)
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).
		Return(&datapb.DescribeSnapshotResponse{
			Status: merr.Status(merr.WrapErrParameterInvalidMsg("snapshot not found")),
		}, nil)

	core := newTestCore(withMixCoord(mockMixCoord))

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "new_collection",
		DbName:         "default",
	}

	// Execute
	_, err := core.broadcastRestoreSnapshotV2(ctx, req)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "snapshot not found")
}

func TestCore_BroadcastRestoreSnapshotV2_CollectionAlreadyExists(t *testing.T) {
	ctx := context.Background()

	// Mock DescribeSnapshot success
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).
		Return(&datapb.DescribeSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	// Mock meta - collection exists
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "", assert.AnError // No alias conflict
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return &model.Collection{Name: collectionName}, nil // Collection exists
	}

	core := newTestCore(withMeta(meta), withMixCoord(mockMixCoord))

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "existing_collection",
		DbName:         "default",
	}

	// Execute
	_, err := core.broadcastRestoreSnapshotV2(ctx, req)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestCore_BroadcastRestoreSnapshotV2_AliasConflict(t *testing.T) {
	ctx := context.Background()

	// Mock DescribeSnapshot success
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).
		Return(&datapb.DescribeSnapshotResponse{
			Status: merr.Success(),
		}, nil)

	// Mock meta - alias exists with same name
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "real_collection", nil // Alias exists
	}

	core := newTestCore(withMeta(meta), withMixCoord(mockMixCoord))

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "test_alias", // This name conflicts with an alias
		DbName:         "default",
	}

	// Execute
	_, err := core.broadcastRestoreSnapshotV2(ctx, req)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts with an existing alias")
}

func TestCore_BroadcastRestoreSnapshotV2_AllocJobIDError(t *testing.T) {
	ctx := context.Background()

	// Mock DescribeSnapshot success
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).
		Return(&datapb.DescribeSnapshotResponse{Status: merr.Success()}, nil)

	// Mock meta - collection does not exist
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "", assert.AnError // No alias
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return nil, assert.AnError // No collection
	}

	core := newTestCore(withMeta(meta), withMixCoord(mockMixCoord), withInvalidIDAllocator())

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "new_collection",
		DbName:         "default",
	}

	// Execute
	_, err := core.broadcastRestoreSnapshotV2(ctx, req)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to allocate job ID")
}

func TestCore_BroadcastRestoreSnapshotV2_BroadcastStartError(t *testing.T) {
	ctx := context.Background()

	// Mock DescribeSnapshot success
	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeSnapshot(mock.Anything, mock.Anything).
		Return(&datapb.DescribeSnapshotResponse{Status: merr.Success()}, nil)

	// Mock meta - collection does not exist
	meta := newMockMetaTable()
	meta.DescribeAliasFunc = func(ctx context.Context, dbName, alias string, ts Timestamp) (string, error) {
		return "", assert.AnError
	}
	meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
		return nil, assert.AnError
	}

	core := newTestCore(withMeta(meta), withMixCoord(mockMixCoord), withValidIDAllocator())

	// Mock broadcast.StartBroadcastWithResourceKeys to fail
	mockBroadcast := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		Return(nil, errors.New("broadcast start failed")).Build()
	defer mockBroadcast.UnPatch()

	req := &milvuspb.RestoreSnapshotRequest{
		Name:           "test_snapshot",
		CollectionName: "new_collection",
		DbName:         "default",
	}

	// Execute
	_, err := core.broadcastRestoreSnapshotV2(ctx, req)

	// Verify
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broadcast start failed")
}
