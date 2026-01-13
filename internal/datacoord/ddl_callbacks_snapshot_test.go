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

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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
	) (int64, error) {
		createSnapshotCalled = true
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, "test_snapshot", name)
		assert.Equal(t, "test description", description)
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
			CollectionId: 100,
			Name:         "test_snapshot",
			Description:  "test description",
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
