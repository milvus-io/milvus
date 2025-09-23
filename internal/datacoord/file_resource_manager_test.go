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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type FileResourceManagerSuite struct {
	suite.Suite

	ctx     context.Context
	manager *FileResourceManager

	// Test objects
	testMeta        *meta
	mockNodeManager *session.MockNodeManager
	mockDataNode    *mocks.MockDataNodeClient
}

func (suite *FileResourceManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *FileResourceManagerSuite) SetupTest() {
	suite.ctx = context.Background()

	// Create test meta with minimal initialization
	suite.testMeta = &meta{
		resourceMeta:    make(map[string]*internalpb.FileResourceInfo),
		resourceVersion: 0,
	}

	// Create mocks
	suite.mockNodeManager = session.NewMockNodeManager(suite.T())
	suite.mockDataNode = mocks.NewMockDataNodeClient(suite.T())

	// Create FileResourceManager
	suite.manager = NewFileResourceManager(suite.ctx, suite.testMeta, suite.mockNodeManager)
}

func (suite *FileResourceManagerSuite) TearDownTest() {
	// Assert mock expectations
	suite.mockNodeManager.AssertExpectations(suite.T())
	suite.mockDataNode.AssertExpectations(suite.T())
}

func (suite *FileResourceManagerSuite) TestNewFileResourceManager() {
	manager := NewFileResourceManager(suite.ctx, suite.testMeta, suite.mockNodeManager)
	suite.NotNil(manager)
	suite.Equal(suite.ctx, manager.ctx)
	suite.Equal(suite.testMeta, manager.meta)
	suite.Equal(suite.mockNodeManager, manager.nodeManager)
	suite.NotNil(manager.distribution)
	suite.NotNil(manager.notifyCh)
}

func (suite *FileResourceManagerSuite) TestNotify() {
	// Test notify without blocking
	suite.manager.Notify()

	// Verify notification was sent
	select {
	case <-suite.manager.notifyCh:
		// Expected
	case <-time.After(100 * time.Millisecond):
		suite.Fail("Expected notification but got none")
	}

	// Test notify when channel is full (should not block)
	suite.manager.Notify()
	suite.manager.Notify() // This should not block even if channel is full
}

func (suite *FileResourceManagerSuite) TestSync_Success() {
	// Prepare test data
	nodeID := int64(1)
	resources := []*internalpb.FileResourceInfo{
		{
			Id:   1,
			Name: "test.file",
			Path: "/test/test.file",
		},
	}
	version := uint64(100)

	// Setup meta state directly
	suite.testMeta.resourceMeta["test.file"] = resources[0]
	suite.testMeta.resourceVersion = version

	// Setup mocks
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID})
	suite.mockNodeManager.EXPECT().GetClient(nodeID).Return(suite.mockDataNode, nil)
	suite.mockDataNode.EXPECT().SyncFileResource(
		suite.ctx,
		&internalpb.SyncFileResourceRequest{
			Resources: resources,
			Version:   version,
		},
	).Return(merr.Success(), nil)

	// Execute sync
	err := suite.manager.sync()

	// Verify
	suite.NoError(err)
	suite.Equal(version, suite.manager.distribution[nodeID])
}

func (suite *FileResourceManagerSuite) TestSync_NodeClientError() {
	// Prepare test data
	nodeID := int64(1)
	version := uint64(100)

	// Setup meta state directly
	suite.testMeta.resourceVersion = version

	// Setup mocks - GetClient fails
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID})
	suite.mockNodeManager.EXPECT().GetClient(nodeID).Return(nil, merr.WrapErrNodeNotFound(nodeID))

	// Execute sync
	err := suite.manager.sync()

	// Verify error is returned and distribution not updated
	suite.Error(err)
	suite.Equal(uint64(0), suite.manager.distribution[nodeID])
}

func (suite *FileResourceManagerSuite) TestSync_SyncFileResourceError() {
	// Prepare test data
	nodeID := int64(1)
	version := uint64(100)

	// Setup meta state directly
	suite.testMeta.resourceVersion = version

	// Setup mocks - SyncFileResource fails
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID})
	suite.mockNodeManager.EXPECT().GetClient(nodeID).Return(suite.mockDataNode, nil)
	suite.mockDataNode.EXPECT().SyncFileResource(
		suite.ctx,
		mock.AnythingOfType("*internalpb.SyncFileResourceRequest"),
	).Return(nil, merr.WrapErrServiceInternal("sync failed"))

	// Execute sync
	err := suite.manager.sync()

	// Verify error is returned and distribution not updated
	suite.Error(err)
	suite.Equal(uint64(0), suite.manager.distribution[nodeID])
}

func (suite *FileResourceManagerSuite) TestSync_SyncFileResourceStatusError() {
	// Prepare test data
	nodeID := int64(1)
	version := uint64(100)

	// Setup mocks - SyncFileResource returns error status
	// Setup meta state directly
	suite.testMeta.resourceVersion = version
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID})
	suite.mockNodeManager.EXPECT().GetClient(nodeID).Return(suite.mockDataNode, nil)
	suite.mockDataNode.EXPECT().SyncFileResource(
		suite.ctx,
		mock.AnythingOfType("*internalpb.SyncFileResourceRequest"),
	).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "internal error",
	}, nil)

	// Execute sync
	err := suite.manager.sync()

	// Verify error is returned and distribution not updated
	suite.Error(err)
	suite.Equal(uint64(0), suite.manager.distribution[nodeID])
}

func (suite *FileResourceManagerSuite) TestSync_MultipleNodes() {
	// Prepare test data
	nodeID1, nodeID2 := int64(1), int64(2)
	version := uint64(100)

	// Setup existing distribution where node1 is already up to date
	suite.manager.distribution[nodeID1] = version

	// Setup mocks
	// Setup meta state directly
	suite.testMeta.resourceVersion = version
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID1, nodeID2})
	// Only node2 should be synced since node1 is already up to date
	suite.mockNodeManager.EXPECT().GetClient(nodeID2).Return(suite.mockDataNode, nil)
	suite.mockDataNode.EXPECT().SyncFileResource(
		suite.ctx,
		mock.AnythingOfType("*internalpb.SyncFileResourceRequest"),
	).Return(merr.Success(), nil)

	// Execute sync
	err := suite.manager.sync()

	// Verify
	suite.NoError(err)
	suite.Equal(version, suite.manager.distribution[nodeID1])
	suite.Equal(version, suite.manager.distribution[nodeID2])
}

func (suite *FileResourceManagerSuite) TestSync_PartialFailure() {
	// Prepare test data
	nodeID1, nodeID2 := int64(1), int64(2)
	version := uint64(100)

	// Setup mocks - node1 succeeds, node2 fails
	// Setup meta state directly
	suite.testMeta.resourceVersion = version
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{nodeID1, nodeID2})

	// Node1 succeeds
	suite.mockNodeManager.EXPECT().GetClient(nodeID1).Return(suite.mockDataNode, nil)
	suite.mockDataNode.EXPECT().SyncFileResource(
		suite.ctx,
		mock.AnythingOfType("*internalpb.SyncFileResourceRequest"),
	).Return(merr.Success(), nil).Once()

	// Node2 fails
	suite.mockNodeManager.EXPECT().GetClient(nodeID2).Return(nil, merr.WrapErrNodeNotFound(nodeID2))

	// Execute sync
	err := suite.manager.sync()

	// Verify error is returned but successful node is updated
	suite.Error(err)
	suite.Equal(version, suite.manager.distribution[nodeID1])
	suite.Equal(uint64(0), suite.manager.distribution[nodeID2])
}

func (suite *FileResourceManagerSuite) TestStart_SyncModeEnabled() {
	// Mock paramtable to enable sync mode
	paramtable.Get().DataCoordCfg.FileResourceMode.SwapTempValue("sync")

	// Setup mocks for initial sync
	// meta already initialized with empty state
	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{})

	// Start manager
	suite.manager.Start()

	// Wait a bit for goroutine to start
	time.Sleep(10 * time.Millisecond)

	// Verify sync was triggered by checking that ListFileResource was called
	// The mock expectations will be verified in TearDownTest
}

func (suite *FileResourceManagerSuite) TestStart_SyncModeDisabled() {
	// Mock paramtable to disable sync mode
	paramtable.Get().DataCoordCfg.FileResourceMode.SwapTempValue("async")

	// Start manager - no mocks should be called
	suite.manager.Start()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// No sync should have been triggered, so no expectations needed
}

func TestFileResourceManagerSuite(t *testing.T) {
	suite.Run(t, new(FileResourceManagerSuite))
}
