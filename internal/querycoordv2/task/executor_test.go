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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ExecutorTestSuite struct {
	suite.Suite

	// Dependencies
	kv      kv.MetaKv
	meta    *meta.Meta
	dist    *meta.DistributionManager
	target  *meta.TargetManager
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
	cluster *session.MockCluster

	// Test object
	executor *Executor
	ctx      context.Context
}

func (suite *ExecutorTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ExecutorTestSuite) SetupTest() {
	var err error
	config := params.GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	// Initialize components
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := params.RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.dist = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.target = meta.NewTargetManager(suite.broker, suite.meta)
	suite.cluster = session.NewMockCluster(suite.T())

	suite.executor = NewExecutor(suite.meta, suite.dist, suite.broker, suite.target, suite.cluster, suite.nodeMgr)
	suite.ctx = context.Background()
}

func (suite *ExecutorTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ExecutorTestSuite) TestReleaseSegmentWithServiceableLeader() {
	// Setup collection and replica
	collection := utils.CreateTestCollection(1, 1)
	suite.meta.CollectionManager.PutCollection(suite.ctx, collection)
	suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(1, 1))

	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	suite.meta.ReplicaManager.Put(suite.ctx, replica)

	// Setup nodes
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 2)

	// Create segment task
	action := NewSegmentAction(1, ActionTypeReduce, "test-channel", 100)
	task, err := NewSegmentTask(suite.ctx, time.Second*10, WrapIDSource(0), 1, replica, action)
	suite.NoError(err)

	// Setup serviceable leader view (UnServiceableError = nil means serviceable)
	serviceableLeaderView := utils.CreateTestLeaderView(1, 1, "test-channel", map[int64]int64{100: 1}, map[int64]*meta.Segment{})
	serviceableLeaderView.UnServiceableError = nil // serviceable
	suite.dist.LeaderViewManager.Update(1, serviceableLeaderView)

	// Setup non-serviceable leader view
	nonServiceableLeaderView := utils.CreateTestLeaderView(2, 1, "test-channel", map[int64]int64{100: 2}, map[int64]*meta.Segment{})
	nonServiceableLeaderView.UnServiceableError = errors.New("not serviceable") // not serviceable
	suite.dist.LeaderViewManager.Update(2, nonServiceableLeaderView)

	// Mock cluster response
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, int64(1), mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil).Once()

	// Execute release segment
	suite.executor.releaseSegment(task, 0)

	// Verify that serviceable leader (node 1) was chosen for release
	suite.cluster.AssertExpectations(suite.T())
}

func (suite *ExecutorTestSuite) TestReleaseSegmentFallbackToNonServiceableLeader() {
	// Setup collection and replica
	collection := utils.CreateTestCollection(1, 1)
	suite.meta.CollectionManager.PutCollection(suite.ctx, collection)
	suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(1, 1))

	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	suite.meta.ReplicaManager.Put(suite.ctx, replica)

	// Setup nodes
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 2)

	// Create segment task
	action := NewSegmentAction(1, ActionTypeReduce, "test-channel", 100)
	task, err := NewSegmentTask(suite.ctx, time.Second*10, WrapIDSource(0), 1, replica, action)
	suite.NoError(err)

	// Setup only non-serviceable leader view
	nonServiceableLeaderView := utils.CreateTestLeaderView(2, 1, "test-channel", map[int64]int64{100: 2}, map[int64]*meta.Segment{})
	nonServiceableLeaderView.UnServiceableError = errors.New("not serviceable") // not serviceable
	suite.dist.LeaderViewManager.Update(2, nonServiceableLeaderView)

	// Mock cluster response for fallback to non-serviceable leader
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, int64(2), mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil).Once()

	// Execute release segment
	suite.executor.releaseSegment(task, 0)

	// Verify that non-serviceable leader (node 2) was used as fallback
	suite.cluster.AssertExpectations(suite.T())
}

func (suite *ExecutorTestSuite) TestReleaseSegmentNoLeaderFound() {
	// Setup collection and replica
	collection := utils.CreateTestCollection(1, 1)
	suite.meta.CollectionManager.PutCollection(suite.ctx, collection)
	suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(1, 1))

	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	suite.meta.ReplicaManager.Put(suite.ctx, replica)

	// Setup nodes
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 2)

	// Create segment task
	action := NewSegmentAction(1, ActionTypeReduce, "test-channel", 100)
	task, err := NewSegmentTask(suite.ctx, time.Second*10, WrapIDSource(0), 1, replica, action)
	suite.NoError(err)

	// No leader views setup - should trigger early return

	// Execute release segment - should return early without calling cluster
	suite.executor.releaseSegment(task, 0)

	// Verify that no cluster calls were made
	suite.cluster.AssertExpectations(suite.T())
}

func (suite *ExecutorTestSuite) TestReleaseSegmentWithNodeNotInReplica() {
	// Setup collection and replica
	collection := utils.CreateTestCollection(1, 1)
	suite.meta.CollectionManager.PutCollection(suite.ctx, collection)
	suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(1, 1))

	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	suite.meta.ReplicaManager.Put(suite.ctx, replica)

	// Setup nodes
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   3, // Node 3 not in replica
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 3)

	// Create segment task with node not in replica
	action := NewSegmentAction(3, ActionTypeReduce, "test-channel", 100)
	task, err := NewSegmentTask(suite.ctx, time.Second*10, WrapIDSource(0), 1, replica, action)
	suite.NoError(err)

	// Mock cluster response for direct node release
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, int64(3), mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil).Once()

	// Execute release segment
	suite.executor.releaseSegment(task, 0)

	// Verify that direct node release was used
	suite.cluster.AssertExpectations(suite.T())
}

func (suite *ExecutorTestSuite) TestReleaseSegmentChannelSpecificLookup() {
	// Setup collection and replica
	collection := utils.CreateTestCollection(1, 1)
	suite.meta.CollectionManager.PutCollection(suite.ctx, collection)
	suite.meta.CollectionManager.PutPartition(suite.ctx, utils.CreateTestPartition(1, 1))

	replica := utils.CreateTestReplica(1, 1, []int64{1, 2})
	suite.meta.ReplicaManager.Put(suite.ctx, replica)

	// Setup nodes
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(suite.ctx, 2)

	// Create segment task
	action := NewSegmentAction(1, ActionTypeReduce, "test-channel", 100)
	task, err := NewSegmentTask(suite.ctx, time.Second*10, WrapIDSource(0), 1, replica, action)
	suite.NoError(err)

	// Setup leader view for a different channel (should not be found with channel filter)
	leaderViewDifferentChannel := utils.CreateTestLeaderView(1, 1, "other-channel", map[int64]int64{100: 1}, map[int64]*meta.Segment{})
	leaderViewDifferentChannel.UnServiceableError = errors.New("not serviceable")
	suite.dist.LeaderViewManager.Update(1, leaderViewDifferentChannel)

	// Setup leader view for the correct channel but non-serviceable
	leaderViewCorrectChannel := utils.CreateTestLeaderView(2, 1, "test-channel", map[int64]int64{100: 2}, map[int64]*meta.Segment{})
	leaderViewCorrectChannel.UnServiceableError = errors.New("not serviceable")
	suite.dist.LeaderViewManager.Update(2, leaderViewCorrectChannel)

	// Mock cluster response
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, int64(2), mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil).Once()

	// Execute release segment
	suite.executor.releaseSegment(task, 0)

	// Verify that channel-specific lookup worked correctly
	suite.cluster.AssertExpectations(suite.T())
}

func TestExecutorSuite(t *testing.T) {
	suite.Run(t, new(ExecutorTestSuite))
}
