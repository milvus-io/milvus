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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type LeaderCacheObserverTestSuite struct {
	suite.Suite

	kv   kv.MetaKv
	meta *meta.Meta

	nodeManager   *session.NodeManager
	distManager   *meta.DistributionManager
	targetManager *meta.TargetManager
	broker        *meta.MockBroker

	mockProxyManager *proxyutil.MockProxyClientManager

	observer *LeaderCacheObserver
}

func (suite *LeaderCacheObserverTestSuite) SetupTest() {
	paramtable.Init()

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

	// meta
	store := querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = meta.NewMeta(idAllocator, store, session.NewNodeManager())

	suite.nodeManager = session.NewNodeManager()
	suite.distManager = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta)

	suite.mockProxyManager = proxyutil.NewMockProxyClientManager(suite.T())
	suite.observer = NewLeaderCacheObserver(suite.meta, suite.distManager, suite.targetManager, suite.nodeManager, suite.mockProxyManager)
}

func (suite *LeaderCacheObserverTestSuite) TearDownSuite() {
	suite.kv.Close()
}

func (suite *LeaderCacheObserverTestSuite) TestInvalidateShardLeaderCache() {
	Params.Save(Params.QueryCoordCfg.ShardLeaderCacheInterval.Key, "1")
	Params.Save(Params.QueryCoordCfg.HeartbeatAvailableInterval.Key, "1000000")
	suite.observer.Start(context.TODO())
	defer suite.observer.Stop()

	// setup meta
	suite.meta.PutCollection(utils.CreateTestCollection(1, 1))
	suite.meta.PutPartition(utils.CreateTestPartition(1, 1))
	suite.meta.CollectionManager.PutCollectionWithoutSave(&meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: 1,
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	})
	suite.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set up node manager
	node := session.NewNodeInfo(1, "localhost")
	node.SetLastHeartbeat(time.Now())
	suite.nodeManager.Add(node)
	node2 := session.NewNodeInfo(2, "localhost")
	node2.SetLastHeartbeat(time.Now())
	suite.nodeManager.Add(node2)

	// set up target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: 1,
			ChannelName:  "channel-2",
		},
	}
	segments := []*datapb.SegmentInfo{
		{
			ID:          1,
			PartitionID: 1,
		},
		{
			ID:          2,
			PartitionID: 1,
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(channels, segments, nil)
	suite.targetManager.UpdateCollectionNextTarget(1)
	suite.targetManager.UpdateCollectionCurrentTarget(1)

	counter := atomic.NewInt64(0)
	suite.mockProxyManager.EXPECT().InvalidateShardLeaderCache(mock.Anything, &proxypb.InvalidateShardLeaderCacheRequest{
		CollectionIDs: []int64{1},
	}).RunAndReturn(func(ctx context.Context, islcr *proxypb.InvalidateShardLeaderCacheRequest) error {
		counter.Add(1)
		return nil
	})
	// test sub channel
	leaderViews := []*meta.LeaderView{
		{
			ID:           1,
			CollectionID: 1,
			Channel:      "channel-1",
		},
		{
			ID:           1,
			CollectionID: 1,
			Channel:      "channel-2",
		},
	}
	suite.distManager.LeaderViewManager.Update(1, leaderViews...)
	suite.Eventually(func() bool {
		return counter.Load() == 1
	}, 3*time.Second, 1*time.Second)

	// test balance channel
	leaderViews = []*meta.LeaderView{
		{
			ID:           2,
			CollectionID: 1,
			Channel:      "channel-1",
		},
		{
			ID:           2,
			CollectionID: 1,
			Channel:      "channel-2",
		},
	}
	suite.distManager.LeaderViewManager.Update(1)
	suite.distManager.LeaderViewManager.Update(2, leaderViews...)
	suite.Eventually(func() bool {
		return counter.Load() == 2
	}, 3*time.Second, 1*time.Second)

	// test unsub channel

	suite.distManager.LeaderViewManager.Update(2)
	suite.Eventually(func() bool {
		return counter.Load() == 3
	}, 3*time.Second, 1*time.Second)
}

func TestLeaderCacheObserverTestSuite(t *testing.T) {
	suite.Run(t, new(LeaderCacheObserverTestSuite))
}
