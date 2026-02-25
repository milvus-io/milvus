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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// --- Test refreshExternalCollectionV2AckCallback ---

func TestDDLCallbacks_RefreshExternalCollectionV2AckCallback_Success(t *testing.T) {
	ctx := context.Background()

	// Track if SubmitRefreshJobWithID was called
	submitCalled := false

	// Mock externalCollectionRefreshManager.SubmitRefreshJobWithID using mockey
	mockSubmit := mockey.Mock((*externalCollectionRefreshManager).SubmitRefreshJobWithID).To(func(
		m *externalCollectionRefreshManager,
		ctx context.Context,
		jobID int64,
		collectionID int64,
		collectionName string,
		externalSource, externalSpec string,
	) (int64, error) {
		submitCalled = true
		assert.Equal(t, int64(123), jobID)
		assert.Equal(t, int64(100), collectionID)
		assert.Equal(t, "test_collection", collectionName)
		assert.Equal(t, "s3://bucket/path", externalSource)
		assert.Equal(t, "iceberg", externalSpec)
		return jobID, nil
	}).Build()
	defer mockSubmit.UnPatch()

	// Create DDLCallbacks with mock refresh manager
	server := &Server{
		externalCollectionRefreshManager: &externalCollectionRefreshManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result using message builder
	broadcastMsg := message.NewRefreshExternalCollectionMessageBuilderV2().
		WithHeader(&message.RefreshExternalCollectionMessageHeader{
			JobId:          123,
			CollectionId:   100,
			CollectionName: "test_collection",
			ExternalSource: "s3://bucket/path",
			ExternalSpec:   "iceberg",
		}).
		WithBody(&message.RefreshExternalCollectionMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	// Convert to typed broadcast message
	typedMsg := message.MustAsBroadcastRefreshExternalCollectionMessageV2(broadcastMsg)

	result := message.BroadcastResultRefreshExternalCollectionMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.refreshExternalCollectionV2AckCallback(ctx, result)

	// Assert
	assert.NoError(t, err)
	assert.True(t, submitCalled, "SubmitRefreshJobWithID should have been called")
}

func TestDDLCallbacks_RefreshExternalCollectionV2AckCallback_SubmitFailed(t *testing.T) {
	ctx := context.Background()

	// Mock externalCollectionRefreshManager.SubmitRefreshJobWithID to return error
	mockSubmit := mockey.Mock((*externalCollectionRefreshManager).SubmitRefreshJobWithID).To(func(
		m *externalCollectionRefreshManager,
		ctx context.Context,
		jobID int64,
		collectionID int64,
		collectionName string,
		externalSource, externalSpec string,
	) (int64, error) {
		return 0, errors.New("submit failed: collection not found")
	}).Build()
	defer mockSubmit.UnPatch()

	// Create DDLCallbacks with mock refresh manager
	server := &Server{
		externalCollectionRefreshManager: &externalCollectionRefreshManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result
	broadcastMsg := message.NewRefreshExternalCollectionMessageBuilderV2().
		WithHeader(&message.RefreshExternalCollectionMessageHeader{
			JobId:          123,
			CollectionId:   100,
			CollectionName: "test_collection",
			ExternalSource: "s3://bucket/path",
		}).
		WithBody(&message.RefreshExternalCollectionMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastRefreshExternalCollectionMessageV2(broadcastMsg)

	result := message.BroadcastResultRefreshExternalCollectionMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.refreshExternalCollectionV2AckCallback(ctx, result)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "submit failed")
}

func TestDDLCallbacks_RefreshExternalCollectionV2AckCallback_EmptySource(t *testing.T) {
	ctx := context.Background()

	// Track parameters passed to SubmitRefreshJobWithID
	var capturedSource, capturedSpec string

	// Mock externalCollectionRefreshManager.SubmitRefreshJobWithID
	mockSubmit := mockey.Mock((*externalCollectionRefreshManager).SubmitRefreshJobWithID).To(func(
		m *externalCollectionRefreshManager,
		ctx context.Context,
		jobID int64,
		collectionID int64,
		collectionName string,
		externalSource, externalSpec string,
	) (int64, error) {
		capturedSource = externalSource
		capturedSpec = externalSpec
		return jobID, nil
	}).Build()
	defer mockSubmit.UnPatch()

	// Create DDLCallbacks
	server := &Server{
		externalCollectionRefreshManager: &externalCollectionRefreshManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	// Create test broadcast result with empty source and spec
	broadcastMsg := message.NewRefreshExternalCollectionMessageBuilderV2().
		WithHeader(&message.RefreshExternalCollectionMessageHeader{
			JobId:          123,
			CollectionId:   100,
			CollectionName: "test_collection",
			ExternalSource: "", // Empty source
			ExternalSpec:   "", // Empty spec
		}).
		WithBody(&message.RefreshExternalCollectionMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastRefreshExternalCollectionMessageV2(broadcastMsg)

	result := message.BroadcastResultRefreshExternalCollectionMessageV2{
		Message: typedMsg,
	}

	// Execute
	err := callbacks.refreshExternalCollectionV2AckCallback(ctx, result)

	// Assert
	assert.NoError(t, err)
	// Should pass empty source and spec (manager will use collection defaults)
	assert.Equal(t, "", capturedSource)
	assert.Equal(t, "", capturedSpec)
}

func TestDDLCallbacks_RefreshExternalCollectionV2AckCallback_NonRetriableError(t *testing.T) {
	ctx := context.Background()

	// Mock SubmitRefreshJobWithID to return a non-retriable error
	mockSubmit := mockey.Mock((*externalCollectionRefreshManager).SubmitRefreshJobWithID).To(func(
		m *externalCollectionRefreshManager,
		ctx context.Context,
		jobID int64,
		collectionID int64,
		collectionName string,
		externalSource, externalSpec string,
	) (int64, error) {
		return 0, merr.WrapErrCollectionNotFound(collectionID)
	}).Build()
	defer mockSubmit.UnPatch()

	server := &Server{
		externalCollectionRefreshManager: &externalCollectionRefreshManager{},
	}
	callbacks := &DDLCallbacks{Server: server}

	broadcastMsg := message.NewRefreshExternalCollectionMessageBuilderV2().
		WithHeader(&message.RefreshExternalCollectionMessageHeader{
			JobId:          123,
			CollectionId:   100,
			CollectionName: "test_collection",
			ExternalSource: "s3://bucket/path",
		}).
		WithBody(&message.RefreshExternalCollectionMessageBody{}).
		WithBroadcast([]string{"control_channel"}).
		MustBuildBroadcast()

	typedMsg := message.MustAsBroadcastRefreshExternalCollectionMessageV2(broadcastMsg)

	result := message.BroadcastResultRefreshExternalCollectionMessageV2{
		Message: typedMsg,
	}

	// Non-retriable error should return nil (not block WAL)
	err := callbacks.refreshExternalCollectionV2AckCallback(ctx, result)
	assert.NoError(t, err)
}

// --- Test isNonRetriableRefreshError ---

func TestIsNonRetriableRefreshError(t *testing.T) {
	t.Run("collection_not_found", func(t *testing.T) {
		err := merr.WrapErrCollectionNotFound(100)
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("collection_illegal_schema", func(t *testing.T) {
		err := merr.WrapErrCollectionIllegalSchema("test", "not external")
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("task_duplicate", func(t *testing.T) {
		err := merr.WrapErrTaskDuplicate("refresh", "already in progress")
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("generic_error_is_retriable", func(t *testing.T) {
		err := errors.New("connection timeout")
		assert.False(t, isNonRetriableRefreshError(err))
	})
}
