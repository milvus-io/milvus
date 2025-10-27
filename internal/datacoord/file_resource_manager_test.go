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
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	metamock "github.com/milvus-io/milvus/internal/metastore/mocks"
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
	mockCatalog     *metamock.DataCoordCatalog
}

func (suite *FileResourceManagerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *FileResourceManagerSuite) SetupTest() {
	suite.ctx = context.Background()

	// Create mocks
	suite.mockNodeManager = session.NewMockNodeManager(suite.T())
	suite.mockDataNode = mocks.NewMockDataNodeClient(suite.T())
	suite.mockCatalog = metamock.NewDataCoordCatalog(suite.T())

	// Create test meta with minimal initialization
	suite.testMeta = &meta{
		catalog:         suite.mockCatalog,
		resourceMeta:    make(map[string]*internalpb.FileResourceInfo),
		resourceVersion: 0,
	}

	// Create FileResourceManager
	suite.manager = NewFileResourceManager(suite.ctx, suite.testMeta, suite.mockNodeManager)
	suite.manager.Start()
}

func (suite *FileResourceManagerSuite) TearDownTest() {
	suite.manager.Close()
	// Assert mock expectations
	suite.mockNodeManager.AssertExpectations(suite.T())
	suite.mockDataNode.AssertExpectations(suite.T())
}

func (suite *FileResourceManagerSuite) TestNormal() {
	testResource := &internalpb.FileResourceInfo{
		Id:   1,
		Name: "test",
		Path: "/tmp/test",
	}

	suite.mockNodeManager.EXPECT().GetClientIDs().Return([]int64{1})
	suite.mockNodeManager.EXPECT().GetClient(int64(1)).Return(suite.mockDataNode, nil)

	syncCh := make(chan struct{}, 1)
	suite.mockDataNode.EXPECT().SyncFileResource(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, in *internalpb.SyncFileResourceRequest, opts ...grpc.CallOption) {
		suite.Equal(1, len(in.Resources))
		suite.Equal(testResource.Id, in.Resources[0].Id)
		suite.Equal(testResource.Name, in.Resources[0].Name)
		suite.Equal(testResource.Path, in.Resources[0].Path)
		syncCh <- struct{}{}
	}).Return(merr.Success(), nil).Once()
	suite.mockCatalog.EXPECT().SaveFileResource(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.testMeta.AddFileResource(suite.ctx, testResource)

	// notify sync
	suite.manager.Notify()

	suite.Eventually(func() bool {
		select {
		case <-syncCh:
			return true
		default:
			return false
		}
	}, 2*time.Second, 100*time.Millisecond)
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

func TestFileResourceManagerSuite(t *testing.T) {
	suite.Run(t, new(FileResourceManagerSuite))
}
