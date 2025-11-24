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
