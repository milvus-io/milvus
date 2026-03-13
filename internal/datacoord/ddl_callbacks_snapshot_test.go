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

package datacoord

import (
	"context"
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// --- Test createSnapshotV2AckCallback ---

func TestDDLCallbacks_CreateSnapshotV2AckCallback_Success(t *testing.T) {
	ctx := context.Background()

	// Track if CreateSnapshot was called
	createSnapshotCalled := false

	// Mock snapshotManager.CreateSnapshot using mockey
	mockCreateSnapshot := mockey.Mock((*snapshotManager).CreateSnapshot).To(func(
		sm *snapshotManager,
		ctx context.Context,
		collectionID int64,
		name, description string,
		compactionProtectionSeconds int64,
	) (int64, error) {
		createSnapshotCalled = true
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, "test_snapshot", name)
		assert.Equal(t, "test description", description)
		assert.Equal(t, int64(3600), compactionProtectionSeconds)
		return 1001, nil
	}).Build()
	defer mockCreateSnapshot.UnPatch()

	// Create DDLCallbacks with real snapshotManager (mocked methods)
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result using message builder
	broadcastMsg := message.NewCreateSnapshotMessageBuilderV2().
		WithHeader(&message.CreateSnapshotMessageHeader{
			CollectionId:                100,
			Name:                        "test_snapshot",
			Description:                 "test description",
			CompactionProtectionSeconds: 3600,
		}).
		WithBody(&message.CreateSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	// Convert to typed broadcast message
	typedMsg := message.MustAsBroadcastCreateSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultCreateSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.createSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.NoError(t, err)
	assert.True(t, createSnapshotCalled)
}

func TestDDLCallbacks_CreateSnapshotV2AckCallback_CreateError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("create snapshot error")

	// Mock snapshotManager.CreateSnapshot to return error
	mockCreateSnapshot := mockey.Mock((*snapshotManager).CreateSnapshot).To(func(
		sm *snapshotManager,
		ctx context.Context,
		collectionID int64,
		name, description string,
		compactionProtectionSeconds int64,
	) (int64, error) {
		return 0, expectedErr
	}).Build()
	defer mockCreateSnapshot.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result
	broadcastMsg := message.NewCreateSnapshotMessageBuilderV2().
		WithHeader(&message.CreateSnapshotMessageHeader{
			CollectionId: 100,
			Name:         "test_snapshot",
			Description:  "test description",
		}).
		WithBody(&message.CreateSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastCreateSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultCreateSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.createSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- Test dropSnapshotV2AckCallback ---

func TestDDLCallbacks_DropSnapshotV2AckCallback_Success(t *testing.T) {
	ctx := context.Background()

	// Track if DropSnapshot was called
	dropSnapshotCalled := false

	// Mock snapshotManager.DropSnapshot using mockey
	mockDropSnapshot := mockey.Mock((*snapshotManager).DropSnapshot).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) error {
		dropSnapshotCalled = true
		assert.Equal(t, "test_snapshot", name)
		return nil
	}).Build()
	defer mockDropSnapshot.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result
	broadcastMsg := message.NewDropSnapshotMessageBuilderV2().
		WithHeader(&message.DropSnapshotMessageHeader{
			Name: "test_snapshot",
		}).
		WithBody(&message.DropSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastDropSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultDropSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.dropSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.NoError(t, err)
	assert.True(t, dropSnapshotCalled)
}

func TestDDLCallbacks_DropSnapshotV2AckCallback_DropError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("drop snapshot error")

	// Mock snapshotManager.DropSnapshot to return error
	mockDropSnapshot := mockey.Mock((*snapshotManager).DropSnapshot).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) error {
		return expectedErr
	}).Build()
	defer mockDropSnapshot.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result
	broadcastMsg := message.NewDropSnapshotMessageBuilderV2().
		WithHeader(&message.DropSnapshotMessageHeader{
			Name: "test_snapshot",
		}).
		WithBody(&message.DropSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastDropSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultDropSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.dropSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- Test restoreSnapshotV2AckCallback ---

func TestDDLCallbacks_RestoreSnapshotV2AckCallback_Success(t *testing.T) {
	ctx := context.Background()

	// Track calls
	readSnapshotDataCalled := false
	restoreDataCalled := false
	getRestoreStateCalled := false

	// Mock snapshotManager.ReadSnapshotData
	mockReadSnapshotData := mockey.Mock((*snapshotManager).ReadSnapshotData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		name string,
	) (*SnapshotData, error) {
		readSnapshotDataCalled = true
		assert.Equal(t, "test_snapshot", name)
		return &SnapshotData{
			SnapshotInfo: &datapb.SnapshotInfo{Name: name},
		}, nil
	}).Build()
	defer mockReadSnapshotData.UnPatch()

	// Mock snapshotManager.RestoreData
	mockRestoreData := mockey.Mock((*snapshotManager).RestoreData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotName string,
		collectionID int64,
		jobID int64,
	) (int64, error) {
		restoreDataCalled = true
		assert.Equal(t, "test_snapshot", snapshotName)
		assert.Equal(t, int64(200), collectionID)
		assert.Equal(t, int64(12345), jobID) // Verify jobID is passed from header
		return jobID, nil
	}).Build()
	defer mockRestoreData.UnPatch()

	// Mock snapshotManager.GetRestoreState to return completed immediately
	mockGetRestoreState := mockey.Mock((*snapshotManager).GetRestoreState).To(func(
		sm *snapshotManager,
		ctx context.Context,
		jobID int64,
	) (*datapb.RestoreSnapshotInfo, error) {
		getRestoreStateCalled = true
		assert.Equal(t, int64(12345), jobID)
		return &datapb.RestoreSnapshotInfo{
			State:    datapb.RestoreSnapshotState_RestoreSnapshotCompleted,
			Progress: 100,
		}, nil
	}).Build()
	defer mockGetRestoreState.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result with pre-allocated jobID
	broadcastMsg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			SnapshotName: "test_snapshot",
			CollectionId: 200,
			JobId:        12345, // Pre-allocated jobID
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastRestoreSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultRestoreSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.restoreSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.NoError(t, err)
	// restoreSnapshotV2AckCallback no longer reads snapshot data directly.
	assert.False(t, readSnapshotDataCalled)
	assert.True(t, restoreDataCalled)
	assert.False(t, getRestoreStateCalled)
}

func TestDDLCallbacks_RestoreSnapshotV2AckCallback_RestoreDataError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("restore data error")

	// Mock snapshotManager.RestoreData to return error
	mockRestoreData := mockey.Mock((*snapshotManager).RestoreData).To(func(
		sm *snapshotManager,
		ctx context.Context,
		snapshotName string,
		collectionID int64,
		jobID int64,
	) (int64, error) {
		return 0, expectedErr
	}).Build()
	defer mockRestoreData.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		snapshotManager: &snapshotManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result
	broadcastMsg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			SnapshotName: "test_snapshot",
			CollectionId: 200,
			JobId:        12345,
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastRestoreSnapshotMessageV2(broadcastMsg)

	result := message.BroadcastResultRestoreSnapshotMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.restoreSnapshotV2AckCallback(ctx, result)

	// Verify
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// --- Test validateRestoreSnapshotResources ---

// newTestSnapshotMeta creates a snapshotMeta with initialized ConcurrentMaps for testing.
// If snapshots is non-nil, the entries are populated into the maps.
func newTestSnapshotMeta(snapshots map[string]*datapb.SnapshotInfo) *snapshotMeta {
	sm := &snapshotMeta{
		snapshotID2Info:        typeutil.NewConcurrentMap[UniqueID, *datapb.SnapshotInfo](),
		snapshotID2RefIndex:    typeutil.NewConcurrentMap[UniqueID, *SnapshotRefIndex](),
		snapshotName2ID:        typeutil.NewConcurrentMap[string, UniqueID](),
		collectionID2Snapshots: typeutil.NewConcurrentMap[UniqueID, typeutil.UniqueSet](),
	}
	for name, info := range snapshots {
		sm.snapshotName2ID.Insert(name, info.GetId())
		sm.snapshotID2Info.Insert(info.GetId(), info)
	}
	return sm
}

// buildValidateTestServer creates a Server with mocked meta for validateRestoreSnapshotResources tests.
// snapshotFound controls whether GetSnapshot will find the snapshot.
func buildValidateTestServer(t *testing.T, snapshotFound bool) *Server {
	mockBroker := broker.NewMockBroker(t)
	var sm *snapshotMeta
	if snapshotFound {
		sm = newTestSnapshotMeta(map[string]*datapb.SnapshotInfo{
			"snap1": {Id: 1, Name: "snap1", CollectionId: 100},
		})
	} else {
		sm = newTestSnapshotMeta(nil)
	}
	return &Server{
		broker: mockBroker,
		meta: &meta{
			snapshotMeta: sm,
			indexMeta:    &indexMeta{},
		},
	}
}

// buildBaseSnapshotData creates a SnapshotData with a default partition and one index.
func buildBaseSnapshotData() *SnapshotData {
	return &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{"_default": 1},
		},
		Indexes: []*indexpb.IndexInfo{
			{IndexID: 1001, IndexName: "vec_idx", FieldID: 100},
		},
	}
}

// mockDescribeCollection mocks DescribeCollectionInternal to return success.
func mockDescribeCollection() *mockey.Mocker {
	return mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeCollectionInternal")).To(
		func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				CollectionID:   collectionID,
				CollectionName: "test_coll",
				DbName:         "default",
			}, nil
		}).Build()
}

// mockShowPartitions mocks ShowPartitions to return the given partition names.
func mockShowPartitions(names []string) *mockey.Mocker {
	return mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(
		func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error) {
			ids := make([]int64, len(names))
			for i := range names {
				ids[i] = int64(i + 1)
			}
			return &milvuspb.ShowPartitionsResponse{
				Status:         merr.Success(),
				PartitionNames: names,
				PartitionIDs:   ids,
			}, nil
		}).Build()
}

// mockGetIndexes mocks GetIndexesForCollection to return the given indexes.
func mockGetIndexes(indexes []*model.Index) *mockey.Mocker {
	return mockey.Mock((*indexMeta).GetIndexesForCollection).To(
		func(_ *indexMeta, collectionID int64, indexName string) []*model.Index {
			return indexes
		}).Build()
}

func TestValidateRestoreSnapshotResources_SnapshotNotFound(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, false) // snapshot NOT in map

	snapshotData := buildBaseSnapshotData()
	err := server.validateRestoreSnapshotResources(ctx, 100, snapshotData)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "snap1")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestValidateRestoreSnapshotResources_CollectionNotFound(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "DescribeCollectionInternal")).To(
		func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			return nil, errors.New("collection gone")
		}).Build()
	defer m.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection 100 does not exist")
}

func TestValidateRestoreSnapshotResources_ShowPartitionsError(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()

	m2 := mockey.Mock(mockey.GetMethod(&broker.MockBroker{}, "ShowPartitions")).To(
		func(_ *broker.MockBroker, ctx context.Context, collectionID int64) (*milvuspb.ShowPartitionsResponse, error) {
			return nil, errors.New("rpc failure")
		}).Build()
	defer m2.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get partitions")
}

func TestValidateRestoreSnapshotResources_PartitionNotFound(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	// Return partitions that do NOT include "_default"
	m2 := mockShowPartitions([]string{"other_partition"})
	defer m2.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "_default")
	assert.Contains(t, err.Error(), "does not exist in collection")
}

func TestValidateRestoreSnapshotResources_IndexNotFound(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	m2 := mockShowPartitions([]string{"_default"})
	defer m2.UnPatch()
	// Return empty indexes — snapshot expects vec_idx
	m3 := mockGetIndexes([]*model.Index{})
	defer m3.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index vec_idx for field 100 does not exist")
}

func TestValidateRestoreSnapshotResources_IndexFieldMismatch(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	m2 := mockShowPartitions([]string{"_default"})
	defer m2.UnPatch()
	// Index name matches but field ID does not
	m3 := mockGetIndexes([]*model.Index{
		{FieldID: 999, IndexName: "vec_idx"},
	})
	defer m3.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "index vec_idx for field 100 does not exist")
}

func TestValidateRestoreSnapshotResources_Success(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	m2 := mockShowPartitions([]string{"_default"})
	defer m2.UnPatch()
	m3 := mockGetIndexes([]*model.Index{
		{FieldID: 100, IndexName: "vec_idx"},
	})
	defer m3.UnPatch()

	err := server.validateRestoreSnapshotResources(ctx, 100, buildBaseSnapshotData())

	assert.NoError(t, err)
}

func TestValidateRestoreSnapshotResources_SuccessNoIndexes(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	m2 := mockShowPartitions([]string{"_default"})
	defer m2.UnPatch()

	// No indexes in snapshot data
	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{"_default": 1},
		},
		Indexes: []*indexpb.IndexInfo{},
	}

	err := server.validateRestoreSnapshotResources(ctx, 100, snapshotData)

	assert.NoError(t, err)
}

func TestValidateRestoreSnapshotResources_MultiplePartitionsAndIndexes(t *testing.T) {
	ctx := context.Background()
	server := buildValidateTestServer(t, true)

	m1 := mockDescribeCollection()
	defer m1.UnPatch()
	m2 := mockShowPartitions([]string{"_default", "part_a", "part_b"})
	defer m2.UnPatch()
	m3 := mockGetIndexes([]*model.Index{
		{FieldID: 100, IndexName: "vec_idx"},
		{FieldID: 200, IndexName: "scalar_idx"},
	})
	defer m3.UnPatch()

	snapshotData := &SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{Name: "snap1"},
		Collection: &datapb.CollectionDescription{
			Partitions: map[string]int64{
				"_default": 1,
				"part_a":   2,
				"part_b":   3,
			},
		},
		Indexes: []*indexpb.IndexInfo{
			{IndexID: 1001, IndexName: "vec_idx", FieldID: 100},
			{IndexID: 1002, IndexName: "scalar_idx", FieldID: 200},
		},
	}

	err := server.validateRestoreSnapshotResources(ctx, 100, snapshotData)

	assert.NoError(t, err)
}
