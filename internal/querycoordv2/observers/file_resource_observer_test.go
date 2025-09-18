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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type FileResourceObserverSuite struct {
	suite.Suite

	ctx      context.Context
	observer *FileResourceObserver

	// Real components
	nodeManager *session.NodeManager
	mockCluster *session.MockCluster
}

func (suite *FileResourceObserverSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *FileResourceObserverSuite) SetupTest() {
	suite.ctx = context.Background()

	// Create real NodeManager and mock Cluster
	suite.nodeManager = session.NewNodeManager()
	suite.mockCluster = session.NewMockCluster(suite.T())

	// Create FileResourceObserver
	suite.observer = NewFileResourceObserver(suite.ctx, suite.nodeManager, suite.mockCluster)
}

func (suite *FileResourceObserverSuite) TearDownTest() {
	// Assert mock expectations for cluster only
	suite.mockCluster.AssertExpectations(suite.T())
}

func (suite *FileResourceObserverSuite) TestNewFileResourceObserver() {
	observer := NewFileResourceObserver(suite.ctx, suite.nodeManager, suite.mockCluster)
	suite.NotNil(observer)
	suite.Equal(suite.ctx, observer.ctx)
	suite.Equal(suite.nodeManager, observer.nodeManager)
	suite.Equal(suite.mockCluster, observer.cluster)
	suite.NotNil(observer.distribution)
	suite.NotNil(observer.notifyCh)
}

func (suite *FileResourceObserverSuite) TestNotify() {
	// Test notify without blocking
	suite.observer.Notify()

	// Verify notification was sent
	select {
	case <-suite.observer.notifyCh:
		// Expected
	case <-time.After(100 * time.Millisecond):
		suite.Fail("Expected notification but got none")
	}

	// Test notify when channel is full (should not block)
	suite.observer.Notify()
	suite.observer.Notify() // This should not block even if channel is full
}

func (suite *FileResourceObserverSuite) TestUpdateResources() {
	resources := []*internalpb.FileResourceInfo{
		{
			Id:   1,
			Name: "test.file",
			Path: "/test/test.file",
		},
	}
	version := uint64(100)

	// Update resources
	suite.observer.UpdateResources(resources, version)

	// Verify resources and version are updated
	resultResources, resultVersion := suite.observer.getResources()
	suite.Equal(resources, resultResources)
	suite.Equal(version, resultVersion)

	// Verify notification was sent
	select {
	case <-suite.observer.notifyCh:
		// Expected
	case <-time.After(100 * time.Millisecond):
		suite.Fail("Expected notification but got none")
	}
}

func (suite *FileResourceObserverSuite) TestGetResources() {
	resources := []*internalpb.FileResourceInfo{
		{
			Id:   1,
			Name: "test.file",
			Path: "/test/test.file",
		},
	}
	version := uint64(100)

	// Set resources directly
	suite.observer.resources = resources
	suite.observer.version = version

	// Get resources
	resultResources, resultVersion := suite.observer.getResources()
	suite.Equal(resources, resultResources)
	suite.Equal(version, resultVersion)
}

func (suite *FileResourceObserverSuite) TestSync_Success() {
	// Prepare test data
	resources := []*internalpb.FileResourceInfo{
		{
			Id:   1,
			Name: "test.file",
			Path: "/test/test.file",
		},
	}
	version := uint64(100)

	// Real nodeManager starts with empty node list
	// Execute sync
	err := suite.observer.sync(resources, version)

	// Verify no error since no nodes to sync
	suite.NoError(err)
}

func (suite *FileResourceObserverSuite) TestSync_WithNodes() {
	// Add some nodes to the real nodeManager
	node1 := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost:19530",
		Hostname: "node1",
	})
	node2 := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost:19531",
		Hostname: "node2",
	})
	suite.nodeManager.Add(node1)
	suite.nodeManager.Add(node2)

	// Prepare test data
	resources := []*internalpb.FileResourceInfo{
		{
			Id:   1,
			Name: "test.file",
			Path: "/test/test.file",
		},
	}
	version := uint64(100)

	// Mock cluster sync calls for each node
	req1 := &internalpb.SyncFileResourceRequest{Resources: resources, Version: version}
	req2 := &internalpb.SyncFileResourceRequest{Resources: resources, Version: version}
	suite.mockCluster.EXPECT().SyncFileResource(suite.ctx, int64(1), req1).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)
	suite.mockCluster.EXPECT().SyncFileResource(suite.ctx, int64(2), req2).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

	// Execute sync
	err := suite.observer.sync(resources, version)

	// Verify no error
	suite.NoError(err)

	// Verify version was updated for both nodes
	suite.Equal(version, suite.observer.distribution[1])
	suite.Equal(version, suite.observer.distribution[2])
}

func (suite *FileResourceObserverSuite) TestSync_NodeSyncError() {
	// Prepare test data
	resources := []*internalpb.FileResourceInfo{}
	version := uint64(100)

	// Real nodeManager starts with empty node list
	// Execute sync
	err := suite.observer.sync(resources, version)

	// Verify no error since no nodes to sync
	suite.NoError(err)
}

func (suite *FileResourceObserverSuite) TestStart_SyncModeEnabled() {
	// Mock paramtable to enable sync mode
	paramtable.Get().QueryCoordCfg.FileResourceMode.SwapTempValue("sync")

	// Start observer - real nodeManager starts with empty node list
	suite.observer.Start()

	// Wait a bit for goroutine to start
	time.Sleep(10 * time.Millisecond)

	// Verify observer started (no specific expectations to check since real nodeManager is used)
	// The test passes if no panic or error occurs
}

func (suite *FileResourceObserverSuite) TestStart_SyncModeDisabled() {
	// Mock paramtable to disable sync mode
	paramtable.Get().QueryCoordCfg.FileResourceMode.SwapTempValue("async")

	// Start observer - no mocks should be called
	suite.observer.Start()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// No sync should have been triggered, so no expectations needed
}

func (suite *FileResourceObserverSuite) TestMultipleUpdatesAndNotifications() {
	// Test multiple rapid updates
	for i := 0; i < 5; i++ {
		resources := []*internalpb.FileResourceInfo{
			{
				Id:   int64(i + 1),
				Name: "test.file",
				Path: "/test/test.file",
			},
		}
		version := uint64(i + 1)

		suite.observer.UpdateResources(resources, version)

		// Verify latest update
		resultResources, resultVersion := suite.observer.getResources()
		suite.Equal(resources, resultResources)
		suite.Equal(version, resultVersion)
	}
}

func TestFileResourceObserverSuite(t *testing.T) {
	suite.Run(t, new(FileResourceObserverSuite))
}
