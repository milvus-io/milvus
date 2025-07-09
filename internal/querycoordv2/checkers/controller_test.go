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
	"go.uber.org/atomic"

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
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CheckerControllerSuite struct {
	suite.Suite
	kv            kv.MetaKv
	meta          *meta.Meta
	broker        *meta.MockBroker
	nodeMgr       *session.NodeManager
	dist          *meta.DistributionManager
	targetManager *meta.TargetManager
	scheduler     *task.MockScheduler
	balancer      *balance.MockBalancer

	controller *CheckerController
}

func (suite *CheckerControllerSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *CheckerControllerSuite) SetupTest() {
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
	suite.dist = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetManager = meta.NewTargetManager(suite.broker, suite.meta)

	suite.balancer = balance.NewMockBalancer(suite.T())
	suite.scheduler = task.NewMockScheduler(suite.T())
	suite.controller = NewCheckerController(suite.meta, suite.dist, suite.targetManager, suite.nodeMgr, suite.scheduler, suite.broker, func() balance.Balance { return suite.balancer })
}

func (suite *CheckerControllerSuite) TestBasic() {
	ctx := context.Background()
	// set meta
	suite.meta.CollectionManager.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	suite.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1, 2}))
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
	suite.meta.ResourceManager.HandleNodeUp(ctx, 1)
	suite.meta.ResourceManager.HandleNodeUp(ctx, 2)

	// set target
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel2",
		},
	}

	segments := []*datapb.SegmentInfo{
		{
			ID:            3,
			PartitionID:   1,
			InsertChannel: "test-insert-channel2",
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	suite.targetManager.UpdateCollectionNextTarget(ctx, int64(1))

	// set dist
	suite.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
		Node:    2,
		Version: 1,
		// View:    utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, map[int64]*meta.Segment{}),
		View: &meta.LeaderView{
			ID:      2,
			Channel: "test-insert-channel",
			Version: 1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	})

	counter := atomic.NewInt64(0)
	suite.scheduler.EXPECT().Add(mock.Anything).Run(func(task task.Task) {
		counter.Inc()
	}).Return(nil)
	suite.scheduler.EXPECT().GetSegmentTaskNum().Return(0).Maybe()
	suite.scheduler.EXPECT().GetChannelTaskNum().Return(0).Maybe()

	assignSegCounter := atomic.NewInt32(0)
	assingChanCounter := atomic.NewInt32(0)
	suite.balancer.EXPECT().AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, i1 int64, s []*meta.Segment, i2 []int64, i4 bool) []balance.SegmentAssignPlan {
		assignSegCounter.Inc()
		return nil
	})
	suite.balancer.EXPECT().AssignChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, collectionID int64, dc []*meta.DmChannel, i []int64, _ bool) []balance.ChannelAssignPlan {
		assingChanCounter.Inc()
		return nil
	})
	suite.controller.Start()
	defer suite.controller.Stop()

	// expect assign channel first
	suite.Eventually(func() bool {
		suite.controller.Check()
		return counter.Load() > 0 && assingChanCounter.Load() > 0
	}, 3*time.Second, 1*time.Millisecond)

	// until new channel has been subscribed
	suite.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 1,
			ChannelName:  "test-insert-channel2",
		},
		Node:    1,
		Version: 1,
		View: &meta.LeaderView{
			ID:      1,
			Channel: "test-insert-channel2",
			Version: 1,
			Status: &querypb.LeaderViewStatus{
				Serviceable: true,
			},
		},
	})

	// expect assign segment after channel has been subscribed
	suite.Eventually(func() bool {
		suite.controller.Check()
		return counter.Load() > 0 && assignSegCounter.Load() > 0
	}, 3*time.Second, 1*time.Millisecond)
}

func TestCheckControllerSuite(t *testing.T) {
	suite.Run(t, new(CheckerControllerSuite))
}
