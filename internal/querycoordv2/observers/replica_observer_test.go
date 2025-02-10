// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReplicaObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	// dependency
	meta    *meta.Meta
	distMgr *meta.DistributionManager

	nodeMgr  *session.NodeManager
	observer *ReplicaObserver

	collectionID int64
	partitionID  int64
	ctx          context.Context
}

func (suite *ReplicaObserverSuite) SetupSuite() {
	streamingutil.SetStreamingServiceEnabled()
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.CheckNodeInReplicaInterval.Key, "1")
}

func (suite *ReplicaObserverSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
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
	suite.ctx = context.Background()

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)

	suite.distMgr = meta.NewDistributionManager()
	suite.observer = NewReplicaObserver(suite.meta, suite.distMgr)
	suite.observer.Start()
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)
}

func (suite *ReplicaObserverSuite) TestCheckNodesInReplica() {
	ctx := suite.ctx
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg1", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})
	suite.meta.ResourceManager.AddResourceGroup(ctx, "rg2", &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 2},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 2},
	})
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost:8080",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "localhost:8080",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   3,
		Address:  "localhost:8080",
		Hostname: "localhost",
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   4,
		Address:  "localhost:8080",
		Hostname: "localhost",
	}))
	suite.meta.ResourceManager.HandleNodeUp(ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(ctx, 2)
	suite.meta.ResourceManager.HandleNodeUp(ctx, 3)
	suite.meta.ResourceManager.HandleNodeUp(ctx, 4)

	err := suite.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(suite.collectionID, 2))
	suite.NoError(err)
	replicas, err := suite.meta.Spawn(ctx, suite.collectionID, map[string]int{
		"rg1": 1,
		"rg2": 1,
	}, nil)
	suite.NoError(err)
	suite.Equal(2, len(replicas))

	suite.Eventually(func() bool {
		availableNodes := typeutil.NewUniqueSet()
		for _, r := range replicas {
			replica := suite.meta.ReplicaManager.Get(ctx, r.GetID())
			suite.NotNil(replica)
			if replica.RWNodesCount() != 2 {
				return false
			}
			if replica.RONodesCount() != 0 {
				return false
			}
			availableNodes.Insert(replica.GetNodes()...)
		}
		return availableNodes.Len() == 4
	}, 6*time.Second, 2*time.Second)

	// Add some segment on nodes.
	for nodeID := int64(1); nodeID <= 4; nodeID++ {
		suite.distMgr.ChannelDistManager.Update(
			nodeID,
			utils.CreateTestChannel(suite.collectionID, nodeID, 1, "test-insert-channel1"))
		suite.distMgr.SegmentDistManager.Update(
			nodeID,
			utils.CreateTestSegment(suite.collectionID, suite.partitionID, 1, nodeID, 1, "test-insert-channel1"))
	}

	// Do a replica transfer.
	suite.meta.ReplicaManager.TransferReplica(ctx, suite.collectionID, "rg1", "rg2", 1)

	// All replica should in the rg2 but not rg1
	// And some nodes will become ro nodes before all segment and channel on it is cleaned.
	suite.Eventually(func() bool {
		for _, r := range replicas {
			replica := suite.meta.ReplicaManager.Get(ctx, r.GetID())
			suite.NotNil(replica)
			suite.Equal("rg2", replica.GetResourceGroup())
			// all replica should have ro nodes.
			// transferred replica should have 2 ro nodes.
			// not transferred replica should have 1 ro nodes for balancing.
			if !(replica.RONodesCount()+replica.RWNodesCount() == 2 && replica.RONodesCount() > 0) {
				return false
			}
		}
		return true
	}, 30*time.Second, 2*time.Second)

	// Add some segment on nodes.
	for nodeID := int64(1); nodeID <= 4; nodeID++ {
		suite.distMgr.ChannelDistManager.Update(nodeID)
		suite.distMgr.SegmentDistManager.Update(nodeID)
	}

	suite.Eventually(func() bool {
		for _, r := range replicas {
			replica := suite.meta.ReplicaManager.Get(ctx, r.GetID())
			suite.NotNil(replica)
			suite.Equal("rg2", replica.GetResourceGroup())
			if replica.RONodesCount() > 0 {
				return false
			}
			if replica.RWNodesCount() != 1 {
				return false
			}
		}
		return true
	}, 30*time.Second, 2*time.Second)
}

func (suite *ReplicaObserverSuite) TestCheckSQnodesInReplica() {
	balancer := mock_balancer.NewMockBalancer(suite.T())
	change := make(chan struct{})
	balancer.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb func(typeutil.VersionInt64Pair, []types.PChannelInfoAssigned) error) error {
		versions := []typeutil.VersionInt64Pair{
			{Global: 1, Local: 2},
			{Global: 1, Local: 3},
		}
		pchans := [][]types.PChannelInfoAssigned{
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 2, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel3", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 3, Address: "localhost:1"},
				},
			},
			{
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel2", Term: 1},
					Node:    types.StreamingNodeInfo{ServerID: 2, Address: "localhost:1"},
				},
				types.PChannelInfoAssigned{
					Channel: types.PChannelInfo{Name: "pchannel3", Term: 2},
					Node:    types.StreamingNodeInfo{ServerID: 2, Address: "localhost:1"},
				},
			},
		}
		for i := 0; i < len(versions); i++ {
			cb(versions[i], pchans[i])
			<-change
		}
		<-ctx.Done()
		return context.Cause(ctx)
	})
	snmanager.StaticStreamingNodeManager.SetBalancerReady(balancer)

	ctx := context.Background()
	err := suite.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(suite.collectionID, 2))
	suite.NoError(err)
	replicas, err := suite.meta.Spawn(ctx, suite.collectionID, map[string]int{
		"rg1": 1,
		"rg2": 1,
	}, nil)
	suite.NoError(err)
	suite.Equal(2, len(replicas))

	suite.Eventually(func() bool {
		replica := suite.meta.ReplicaManager.GetByCollection(ctx, suite.collectionID)
		total := 0
		for _, r := range replica {
			total += r.RWSQNodesCount()
		}
		return total == 3
	}, 6*time.Second, 2*time.Second)
	replica := suite.meta.ReplicaManager.GetByCollection(ctx, suite.collectionID)
	nodes := typeutil.NewUniqueSet()
	for _, r := range replica {
		suite.LessOrEqual(r.RWSQNodesCount(), 2)
		suite.Equal(r.ROSQNodesCount(), 0)
		nodes.Insert(r.GetRWSQNodes()...)
	}
	suite.Equal(nodes.Len(), 3)

	close(change)

	suite.Eventually(func() bool {
		replica := suite.meta.ReplicaManager.GetByCollection(ctx, suite.collectionID)
		total := 0
		for _, r := range replica {
			total += r.RWSQNodesCount()
		}
		return total == 2
	}, 6*time.Second, 2*time.Second)
	replica = suite.meta.ReplicaManager.GetByCollection(ctx, suite.collectionID)
	nodes = typeutil.NewUniqueSet()
	for _, r := range replica {
		suite.Equal(r.RWSQNodesCount(), 1)
		suite.Equal(r.ROSQNodesCount(), 0)
		nodes.Insert(r.GetRWSQNodes()...)
	}
	suite.Equal(nodes.Len(), 2)
}

func (suite *ReplicaObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
	streamingutil.UnsetStreamingServiceEnabled()
}

func TestReplicaObserver(t *testing.T) {
	suite.Run(t, new(ReplicaObserverSuite))
}
