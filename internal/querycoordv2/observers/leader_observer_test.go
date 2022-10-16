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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

type LeaderObserverTestSuite struct {
	suite.Suite
	observer    *LeaderObserver
	kv          *etcdkv.EtcdKV
	mockCluster *session.MockCluster
}

func (suite *LeaderObserverTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *LeaderObserverTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	testMeta := meta.NewMeta(idAllocator, store)

	suite.mockCluster = session.NewMockCluster(suite.T())
	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager()
	suite.observer = NewLeaderObserver(distManager, testMeta, targetManager, suite.mockCluster)
}

func (suite *LeaderObserverTestSuite) TearDownTest() {
	suite.observer.Stop()
	suite.kv.Close()
}

func (suite *LeaderObserverTestSuite) TestSyncLoadedSegments() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	observer.target.AddSegment(utils.CreateTestSegmentInfo(1, 1, 1, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, []int64{}))
	expectReq := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SyncDistribution,
		},
		CollectionID: 1,
		Channel:      "test-insert-channel",
		Actions: []*querypb.SyncAction{
			{
				Type:        querypb.SyncType_Set,
				PartitionID: 1,
				SegmentID:   1,
				NodeID:      1,
				Version:     1,
			},
		},
	}
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(context.TODO(), int64(2), expectReq).Once().
		Run(func(args mock.Arguments) { called.Store(true) }).
		Return(&commonpb.Status{}, nil)

	observer.Start(context.TODO())

	suite.Eventually(
		func() bool {
			return called.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)
}

func (suite *LeaderObserverTestSuite) TestIgnoreBalancedSegment() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	observer.target.AddSegment(utils.CreateTestSegmentInfo(1, 1, 1, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))

	// The leader view saw the segment on new node,
	// but another nodes not yet
	leaderView := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, []int64{})
	leaderView.Segments[1] = &querypb.SegmentDist{
		NodeID:  2,
		Version: 2,
	}
	observer.dist.LeaderViewManager.Update(2, leaderView)
	observer.Start(context.TODO())

	// Nothing should happen
	time.Sleep(2 * time.Second)
}

func (suite *LeaderObserverTestSuite) TestSyncLoadedSegmentsWithReplicas() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(2, 1, []int64{3, 4}))
	observer.target.AddSegment(utils.CreateTestSegmentInfo(1, 1, 1, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(4, utils.CreateTestSegment(1, 1, 1, 4, 2, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, []int64{}))
	observer.dist.LeaderViewManager.Update(4, utils.CreateTestLeaderView(4, 1, "test-insert-channel", map[int64]int64{1: 4}, []int64{}))
	expectReq := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SyncDistribution,
		},
		CollectionID: 1,
		Channel:      "test-insert-channel",
		Actions: []*querypb.SyncAction{
			{
				Type:        querypb.SyncType_Set,
				PartitionID: 1,
				SegmentID:   1,
				NodeID:      1,
				Version:     1,
			},
		},
	}
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(context.TODO(), int64(2), expectReq).Once().
		Run(func(args mock.Arguments) { called.Store(true) }).
		Return(&commonpb.Status{}, nil)

	observer.Start(context.TODO())

	suite.Eventually(
		func() bool {
			return called.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)
}

func (suite *LeaderObserverTestSuite) TestSyncRemovedSegments() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, []int64{}))

	expectReq := &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_SyncDistribution,
		},
		CollectionID: 1,
		Channel:      "test-insert-channel",
		Actions: []*querypb.SyncAction{
			{
				Type:      querypb.SyncType_Remove,
				SegmentID: 3,
			},
		},
	}
	ch := make(chan struct{})
	suite.mockCluster.EXPECT().SyncDistribution(context.TODO(), int64(2), expectReq).Once().
		Run(func(args mock.Arguments) { close(ch) }).
		Return(&commonpb.Status{}, nil)

	observer.Start(context.TODO())

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
	}
}

func TestLeaderObserverSuite(t *testing.T) {
	suite.Run(t, new(LeaderObserverTestSuite))
}
