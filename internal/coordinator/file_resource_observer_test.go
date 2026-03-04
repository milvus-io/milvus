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

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	dcsession "github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/mocks"
	qcsession "github.com/milvus-io/milvus/internal/querycoordv2/session"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type FileResourceObserverSuite struct {
	suite.Suite

	ctx      context.Context
	observer *FileResourceObserver
}

func (s *FileResourceObserverSuite) SetupSuite() {
	paramtable.Init()
}

func (s *FileResourceObserverSuite) SetupTest() {
	s.ctx = context.Background()
	s.observer = NewFileResourceObserver(s.ctx)
}

func (s *FileResourceObserverSuite) TearDownTest() {
	if s.observer != nil {
		s.observer.Stop()
	}
}

func (s *FileResourceObserverSuite) TestNewFileResourceObserver() {
	observer := NewFileResourceObserver(s.ctx)
	s.NotNil(observer)
	s.NotNil(observer.distribution)
	s.NotNil(observer.notifyCh)
	s.NotNil(observer.closeCh)
}

func (s *FileResourceObserverSuite) TestStartStop() {
	s.Run("start_with_sync_mode", func() {
		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			notifyCh:     make(chan struct{}, 1),
			closeCh:      make(chan struct{}),
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,

			qnManager: qcsession.NewNodeManager(),
		}

		// Mock meta to avoid nil pointer
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(nil, uint64(0)).Maybe()
		observer.meta = mockMeta

		observer.Start()
		// Start should be idempotent
		observer.Start()

		// Give syncLoop time to start
		time.Sleep(50 * time.Millisecond)

		observer.Stop()
		// Stop should be idempotent
		observer.Stop()
	})

	s.Run("start_with_download_mode", func() {
		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			notifyCh:     make(chan struct{}, 1),
			closeCh:      make(chan struct{}),
			qnMode:       fileresource.CloseMode,
			dnMode:       fileresource.RefMode,
		}

		observer.Start()
		observer.Stop()
	})
}

func (s *FileResourceObserverSuite) TestNotify() {
	observer := &FileResourceObserver{
		ctx:          s.ctx,
		distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
		notifyCh:     make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
	}

	// First notify should succeed
	observer.Notify()
	s.Len(observer.notifyCh, 1)

	// Second notify should not block (channel already has a message)
	observer.Notify()
	s.Len(observer.notifyCh, 1)
}

func (s *FileResourceObserverSuite) TestCheckNodeSynced() {
	s.Run("meta_not_ready", func() {
		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         nil,
		}
		s.False(observer.CheckNodeSynced(1))
	})

	s.Run("no_resources", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(nil, uint64(0))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		s.True(observer.CheckNodeSynced(1))
	})

	s.Run("has_resources_node_not_synced", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return([]*internalpb.FileResourceInfo{
			{Name: "test"},
		}, uint64(1))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		s.False(observer.CheckNodeSynced(1))
	})

	s.Run("has_resources_node_synced", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return([]*internalpb.FileResourceInfo{
			{Name: "test"},
		}, uint64(1))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		observer.distribution.Insert(1, &NodeInfo{NodeID: 1, Version: 1})
		s.True(observer.CheckNodeSynced(1))
	})
}

func (s *FileResourceObserverSuite) TestCheckAllQnReady() {
	s.Run("meta_not_ready", func() {
		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         nil,
		}
		err := observer.CheckAllQnReady()
		s.Error(err)
	})

	s.Run("no_resources", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(nil, uint64(0))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		err := observer.CheckAllQnReady()
		s.NoError(err)
	})

	s.Run("all_query_nodes_synced", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return([]*internalpb.FileResourceInfo{
			{Name: "test"},
		}, uint64(2))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		observer.distribution.Insert(1, &NodeInfo{NodeID: 1, NodeType: QueryNode, Version: 2})
		observer.distribution.Insert(2, &NodeInfo{NodeID: 2, NodeType: QueryNode, Version: 3})

		err := observer.CheckAllQnReady()
		s.NoError(err)
	})

	s.Run("some_query_nodes_not_synced", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return([]*internalpb.FileResourceInfo{
			{Name: "test"},
		}, uint64(5))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		observer.distribution.Insert(1, &NodeInfo{NodeID: 1, NodeType: QueryNode, Version: 5})
		observer.distribution.Insert(2, &NodeInfo{NodeID: 2, NodeType: QueryNode, Version: 3}) // not synced

		err := observer.CheckAllQnReady()
		s.Error(err)
	})

	s.Run("data_nodes_not_checked", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return([]*internalpb.FileResourceInfo{
			{Name: "test"},
		}, uint64(5))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
		}
		// DataNode with old version should not cause error
		observer.distribution.Insert(1, &NodeInfo{NodeID: 1, NodeType: DataNode, Version: 1})

		err := observer.CheckAllQnReady()
		s.NoError(err)
	})
}

func (s *FileResourceObserverSuite) TestSync() {
	s.Run("sync_query_nodes_success", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockCluster := qcsession.NewMockCluster(s.T())
		mockCluster.EXPECT().SyncFileResource(mock.Anything, int64(1), mock.Anything).Return(merr.Success(), nil)

		qnManager := qcsession.NewNodeManager()
		qnManager.Add(qcsession.NewNodeInfo(qcsession.ImmutableNodeInfo{NodeID: 1}))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			qnManager:    qnManager,
			cluster:      mockCluster,
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,
		}

		err := observer.Sync()
		s.NoError(err)

		// Verify node was added to distribution
		info, ok := observer.distribution.Get(1)
		s.True(ok)
		s.Equal(int64(1), info.NodeID)
		s.Equal(QueryNode, info.NodeType)
		s.Equal(uint64(1), info.Version)
	})

	s.Run("sync_query_nodes_already_synced", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockCluster := qcsession.NewMockCluster(s.T())
		// SyncFileResource should NOT be called since node already synced

		qnManager := qcsession.NewNodeManager()
		qnManager.Add(qcsession.NewNodeInfo(qcsession.ImmutableNodeInfo{NodeID: 1}))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			qnManager:    qnManager,
			cluster:      mockCluster,
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,
		}
		// Pre-populate distribution with same version
		observer.distribution.Insert(1, &NodeInfo{NodeID: 1, Version: 1})

		err := observer.Sync()
		s.NoError(err)
	})

	s.Run("sync_query_nodes_rpc_error", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockCluster := qcsession.NewMockCluster(s.T())
		mockCluster.EXPECT().SyncFileResource(mock.Anything, int64(1), mock.Anything).Return(nil, errors.New("rpc error"))

		qnManager := qcsession.NewNodeManager()
		qnManager.Add(qcsession.NewNodeInfo(qcsession.ImmutableNodeInfo{NodeID: 1}))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			qnManager:    qnManager,
			cluster:      mockCluster,
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,
		}

		err := observer.Sync()
		s.Error(err)
	})

	s.Run("sync_query_nodes_status_error", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockCluster := qcsession.NewMockCluster(s.T())
		mockCluster.EXPECT().SyncFileResource(mock.Anything, int64(1), mock.Anything).Return(&commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "sync failed",
		}, nil)

		qnManager := qcsession.NewNodeManager()
		qnManager.Add(qcsession.NewNodeInfo(qcsession.ImmutableNodeInfo{NodeID: 1}))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			qnManager:    qnManager,
			cluster:      mockCluster,
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,
		}

		err := observer.Sync()
		s.Error(err)
	})

	s.Run("sync_data_nodes_success", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockDNClient := mocks.NewMockDataNodeClient(s.T())
		mockDNClient.EXPECT().SyncFileResource(mock.Anything, mock.Anything).Return(merr.Success(), nil)

		mockDNManager := dcsession.NewMockNodeManager(s.T())
		mockDNManager.EXPECT().GetClientIDs().Return([]int64{100})
		mockDNManager.EXPECT().GetClient(int64(100)).Return(mockDNClient, nil)

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			dnManager:    mockDNManager,
			qnMode:       fileresource.CloseMode,
			dnMode:       fileresource.SyncMode,
		}

		err := observer.Sync()
		s.NoError(err)

		// Verify node was added to distribution
		info, ok := observer.distribution.Get(100)
		s.True(ok)
		s.Equal(int64(100), info.NodeID)
		s.Equal(DataNode, info.NodeType)
		s.Equal(uint64(1), info.Version)
	})

	s.Run("sync_data_nodes_get_client_error", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockDNManager := dcsession.NewMockNodeManager(s.T())
		mockDNManager.EXPECT().GetClientIDs().Return([]int64{100})
		mockDNManager.EXPECT().GetClient(int64(100)).Return(nil, errors.New("client not found"))

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			dnManager:    mockDNManager,
			qnMode:       fileresource.CloseMode,
			dnMode:       fileresource.SyncMode,
		}

		err := observer.Sync()
		s.Error(err)
	})

	s.Run("cleanup_removed_nodes", func() {
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		resources := []*internalpb.FileResourceInfo{{Name: "test"}}
		mockMeta.EXPECT().ListFileResource(mock.Anything).Return(resources, uint64(1))

		mockCluster := qcsession.NewMockCluster(s.T())

		qnManager := qcsession.NewNodeManager()
		// Node 1 exists, Node 2 was removed

		observer := &FileResourceObserver{
			ctx:          s.ctx,
			distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
			meta:         mockMeta,
			qnManager:    qnManager,
			cluster:      mockCluster,
			qnMode:       fileresource.SyncMode,
			dnMode:       fileresource.CloseMode,
		}
		// Pre-populate with a node that no longer exists
		observer.distribution.Insert(2, &NodeInfo{NodeID: 2, Version: 1, NodeType: QueryNode})

		err := observer.Sync()
		s.NoError(err)

		// Node 2 should be removed from distribution
		_, ok := observer.distribution.Get(2)
		s.False(ok)
	})
}

func (s *FileResourceObserverSuite) TestInit() {
	s.Run("init_meta", func() {
		observer := &FileResourceObserver{ctx: s.ctx}
		mockMeta := mockrootcoord.NewIMetaTable(s.T())
		observer.InitMeta(mockMeta)
		s.Equal(mockMeta, observer.meta)
	})

	s.Run("init_query_coord", func() {
		observer := &FileResourceObserver{ctx: s.ctx}
		qnManager := qcsession.NewNodeManager()
		mockCluster := qcsession.NewMockCluster(s.T())
		observer.InitQueryCoord(qnManager, mockCluster)
		s.Equal(qnManager, observer.qnManager)
		s.Equal(mockCluster, observer.cluster)
	})

	s.Run("init_data_coord", func() {
		observer := &FileResourceObserver{ctx: s.ctx}
		mockDNManager := dcsession.NewMockNodeManager(s.T())
		observer.InitDataCoord(mockDNManager)
		s.Equal(mockDNManager, observer.dnManager)
	})
}

func (s *FileResourceObserverSuite) TestRetryNotify() {
	observer := &FileResourceObserver{
		ctx:          s.ctx,
		distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),
		notifyCh:     make(chan struct{}, 1),
		closeCh:      make(chan struct{}),
	}

	// Clear channel first
	select {
	case <-observer.notifyCh:
	default:
	}

	// RetryNotify should eventually notify
	observer.RetryNotify()

	// Wait for the retry (3 seconds delay in RetryNotify)
	select {
	case <-observer.notifyCh:
		// Success
	case <-time.After(5 * time.Second):
		s.Fail("RetryNotify did not send notification")
	}
}

func TestFileResourceObserverSuite(t *testing.T) {
	suite.Run(t, new(FileResourceObserverSuite))
}
