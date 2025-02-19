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

package checkers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type ChannelCheckerTestSuite struct {
	suite.Suite
	kv      kv.MetaKv
	checker *ChannelChecker
	meta    *meta.Meta
	broker  *meta.MockBroker

	nodeMgr *session.NodeManager
}

func (suite *ChannelCheckerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ChannelCheckerTestSuite) SetupTest() {
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
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	targetManager := meta.NewTargetManager(suite.broker, suite.meta)

	distManager := meta.NewDistributionManager()

	balancer := suite.createMockBalancer()
	suite.checker = NewChannelChecker(suite.meta, distManager, targetManager, suite.nodeMgr, func() balance.Balance { return balancer })

	suite.broker.EXPECT().GetPartitions(mock.Anything, int64(1)).Return([]int64{1}, nil).Maybe()
}

func (suite *ChannelCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *ChannelCheckerTestSuite) setNodeAvailable(nodes ...int64) {
	for _, node := range nodes {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "",
			Hostname: "localhost",
		})
		nodeInfo.SetLastHeartbeat(time.Now())
		suite.nodeMgr.Add(nodeInfo)
	}
}

func (suite *ChannelCheckerTestSuite) createMockBalancer() balance.Balance {
	balancer := balance.NewMockBalancer(suite.T())
	balancer.EXPECT().AssignChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe().Return(func(ctx context.Context, collectionID int64, channels []*meta.DmChannel, nodes []int64, _ bool) []balance.ChannelAssignPlan {
		plans := make([]balance.ChannelAssignPlan, 0, len(channels))
		for i, c := range channels {
			plan := balance.ChannelAssignPlan{
				Channel: c,
				From:    -1,
				To:      nodes[i%len(nodes)],
				Replica: meta.NilReplica,
			}
			plans = append(plans, plan)
		}
		return plans
	})
	return balancer
}

func (suite *ChannelCheckerTestSuite) TestLoadChannel() {
	ctx := context.Background()
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	checker.meta.ResourceManager.HandleNodeUp(ctx, 1)

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func (suite *ChannelCheckerTestSuite) TestReduceChannel() {
	ctx := context.Background()
	checker := suite.checker
	checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	checker.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1}))

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel1",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.targetMgr.UpdateCollectionCurrentTarget(ctx, int64(1))

	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel1"))
	checker.dist.LeaderViewManager.Update(1, &meta.LeaderView{ID: 1, Channel: "test-insert-channel1"})
	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel2"))
	checker.dist.LeaderViewManager.Update(1, &meta.LeaderView{ID: 1, Channel: "test-insert-channel2"})
	suite.setNodeAvailable(1)
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel2", action.ChannelName())
}

func (suite *ChannelCheckerTestSuite) TestRepeatedChannels() {
	ctx := context.Background()
	checker := suite.checker
	err := checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	suite.NoError(err)
	err = checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
	suite.NoError(err)

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			InsertChannel: "test-insert-channel",
		},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 1, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 0)

	suite.setNodeAvailable(1, 2)
	checker.dist.LeaderViewManager.Update(1, &meta.LeaderView{ID: 1, Channel: "test-insert-channel"})
	checker.dist.LeaderViewManager.Update(2, &meta.LeaderView{ID: 2, Channel: "test-insert-channel"})
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func (suite *ChannelCheckerTestSuite) TestReleaseDirtyChannels() {
	ctx := context.Background()
	checker := suite.checker
	err := checker.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	suite.NoError(err)
	err = checker.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1}))
	suite.NoError(err)

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			InsertChannel: "test-insert-channel",
		},
	}

	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
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

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))
	checker.dist.ChannelDistManager.Update(1, utils.CreateTestChannel(1, 1, 2, "test-insert-channel"))
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 2, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(1, &meta.LeaderView{ID: 1, Channel: "test-insert-channel"})
	checker.dist.LeaderViewManager.Update(2, &meta.LeaderView{ID: 2, Channel: "test-insert-channel"})

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.EqualValues(-1, tasks[0].ReplicaID())
	suite.Len(tasks[0].Actions(), 1)
	suite.IsType((*task.ChannelAction)(nil), tasks[0].Actions()[0])
	action := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(int64(2), action.Node())
	suite.EqualValues("test-insert-channel", action.ChannelName())
}

func TestChannelCheckerSuite(t *testing.T) {
	suite.Run(t, new(ChannelCheckerTestSuite))
}
