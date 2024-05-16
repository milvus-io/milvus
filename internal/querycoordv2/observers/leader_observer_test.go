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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	task2 "github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type LeaderObserverTestSuite struct {
	suite.Suite
	observer    *LeaderObserver
	kv          kv.MetaKv
	mockCluster *session.MockCluster

	meta      *meta.Meta
	broker    *meta.MockBroker
	scheduler *task2.MockScheduler
}

func (suite *LeaderObserverTestSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *LeaderObserverTestSuite) SetupTest() {
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
	nodeMgr := session.NewNodeManager()
	suite.meta = meta.NewMeta(idAllocator, store, nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())

	suite.mockCluster = session.NewMockCluster(suite.T())
	// suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(&commonpb.Status{
	// 	ErrorCode: commonpb.ErrorCode_Success,
	// }, nil).Maybe()
	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager(suite.broker, suite.meta)
	suite.scheduler = task2.NewMockScheduler(suite.T())
	suite.scheduler.EXPECT().Sync(mock.Anything, mock.Anything).Return(true).Maybe()
	suite.scheduler.EXPECT().RemoveSync(mock.Anything, mock.Anything).Maybe()
	suite.observer = NewLeaderObserver(distManager, suite.meta, targetManager, suite.broker, suite.mockCluster, nodeMgr, suite.scheduler)
}

func (suite *LeaderObserverTestSuite) TearDownTest() {
	suite.observer.Stop()
	suite.kv.Close()
}

func (suite *LeaderObserverTestSuite) TestSyncLoadedSegments() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	info := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  1,
		PartitionID:   1,
		InsertChannel: "test-insert-channel",
	}
	resp := &datapb.GetSegmentInfoResponse{
		Infos: []*datapb.SegmentInfo{info},
	}
	schema := utils.CreateTestSchema()
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, int64(1)).Return(schema, nil)
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, int64(1)).Return(
		&datapb.GetSegmentInfoResponse{Infos: []*datapb.SegmentInfo{info}}, nil)
	// will cause sync failed once
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("mock error")).Once()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{
		{IndexName: "test"},
	}, nil)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)
	loadInfo := utils.PackSegmentLoadInfo(resp, nil, nil)

	expectReqeustFunc := func(version int64, actionVersion int64) *querypb.SyncDistributionRequest {
		return &querypb.SyncDistributionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SyncDistribution,
			},
			CollectionID: 1,
			ReplicaID:    1,
			Channel:      "test-insert-channel",
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: 1,
					SegmentID:   1,
					NodeID:      1,
					Version:     actionVersion,
					Info:        loadInfo,
				},
			},
			Schema: schema,
			LoadMeta: &querypb.LoadMetaInfo{
				CollectionID: 1,
				PartitionIDs: []int64{1},
			},
			Version:       version,
			IndexInfoList: []*indexpb.IndexInfo{{IndexName: "test"}},
		}
	}

	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, int64(2),
		mock.AnythingOfType("*querypb.SyncDistributionRequest")).
		Run(func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) {
			assert.ElementsMatch(suite.T(), []*querypb.SyncDistributionRequest{req},
				[]*querypb.SyncDistributionRequest{expectReqeustFunc(req.GetVersion(), req.GetActions()[0].GetVersion())})
			called.Store(true)
		}).
		Return(&commonpb.Status{}, nil)

	observer.Start()

	suite.Eventually(
		func() bool {
			return called.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)
}

func (suite *LeaderObserverTestSuite) TestIgnoreSyncLoadedSegments() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	schema := utils.CreateTestSchema()
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, int64(1)).Return(schema, nil)
	info := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  1,
		PartitionID:   1,
		InsertChannel: "test-insert-channel",
	}
	resp := &datapb.GetSegmentInfoResponse{
		Infos: []*datapb.SegmentInfo{info},
	}
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, int64(1)).Return(
		&datapb.GetSegmentInfoResponse{Infos: []*datapb.SegmentInfo{info}}, nil)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{
		{IndexName: "test"},
	}, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 2, 1, "test-insert-channel"),
		utils.CreateTestSegment(1, 1, 2, 2, 1, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)
	loadInfo := utils.PackSegmentLoadInfo(resp, nil, nil)

	expectReqeustFunc := func(version int64, actionVersion int64) *querypb.SyncDistributionRequest {
		return &querypb.SyncDistributionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SyncDistribution,
			},
			CollectionID: 1,
			ReplicaID:    1,
			Channel:      "test-insert-channel",
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: 1,
					SegmentID:   1,
					NodeID:      1,
					Version:     actionVersion,
					Info:        loadInfo,
				},
			},
			Schema: schema,
			LoadMeta: &querypb.LoadMetaInfo{
				CollectionID: 1,
				PartitionIDs: []int64{1},
			},
			Version:       version,
			IndexInfoList: []*indexpb.IndexInfo{{IndexName: "test"}},
		}
	}
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, int64(2), mock.AnythingOfType("*querypb.SyncDistributionRequest")).
		Run(func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) {
			assert.ElementsMatch(suite.T(), []*querypb.SyncDistributionRequest{req},
				[]*querypb.SyncDistributionRequest{expectReqeustFunc(req.GetVersion(), req.GetActions()[0].GetVersion())})
			called.Store(true)
		}).
		Return(&commonpb.Status{}, nil)

	observer.Start()

	suite.Eventually(
		func() bool {
			return called.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)
}

func (suite *LeaderObserverTestSuite) TestSyncLoadedSegmentsWithReplicas() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(2, 1, []int64{3, 4}))
	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	info := &datapb.SegmentInfo{
		ID:            1,
		CollectionID:  1,
		PartitionID:   1,
		InsertChannel: "test-insert-channel",
	}
	resp := &datapb.GetSegmentInfoResponse{
		Infos: []*datapb.SegmentInfo{info},
	}
	schema := utils.CreateTestSchema()
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, int64(1)).Return(
		&datapb.GetSegmentInfoResponse{Infos: []*datapb.SegmentInfo{info}}, nil)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, int64(1)).Return(schema, nil)
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{{IndexName: "test"}}, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)
	observer.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	observer.dist.SegmentDistManager.Update(4, utils.CreateTestSegment(1, 1, 1, 4, 2, "test-insert-channel"))
	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)
	view2 := utils.CreateTestLeaderView(4, 1, "test-insert-channel", map[int64]int64{1: 4}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(4, view2)
	loadInfo := utils.PackSegmentLoadInfo(resp, nil, nil)

	expectReqeustFunc := func(version int64, actionVersion int64) *querypb.SyncDistributionRequest {
		return &querypb.SyncDistributionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SyncDistribution,
			},
			CollectionID: 1,
			ReplicaID:    1,
			Channel:      "test-insert-channel",
			Actions: []*querypb.SyncAction{
				{
					Type:        querypb.SyncType_Set,
					PartitionID: 1,
					SegmentID:   1,
					NodeID:      1,
					Version:     actionVersion,
					Info:        loadInfo,
				},
			},
			Schema: schema,
			LoadMeta: &querypb.LoadMetaInfo{
				CollectionID: 1,
				PartitionIDs: []int64{1},
			},
			Version:       version,
			IndexInfoList: []*indexpb.IndexInfo{{IndexName: "test"}},
		}
	}
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, int64(2),
		mock.AnythingOfType("*querypb.SyncDistributionRequest")).
		Run(func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) {
			assert.ElementsMatch(suite.T(), []*querypb.SyncDistributionRequest{req},
				[]*querypb.SyncDistributionRequest{expectReqeustFunc(req.GetVersion(), req.GetActions()[0].GetVersion())})
			called.Store(true)
		}).
		Return(&commonpb.Status{}, nil)

	observer.Start()

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
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	schema := utils.CreateTestSchema()
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, int64(1)).Return(schema, nil)
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{
		{IndexName: "test"},
	}, nil)
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, nil, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))
	observer.target.UpdateCollectionCurrentTarget(1)

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	view := utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, map[int64]*meta.Segment{})
	view.TargetVersion = observer.target.GetCollectionTargetVersion(1, meta.CurrentTarget)
	observer.dist.LeaderViewManager.Update(2, view)

	expectReqeustFunc := func(version int64) *querypb.SyncDistributionRequest {
		return &querypb.SyncDistributionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SyncDistribution,
			},
			CollectionID: 1,
			ReplicaID:    1,
			Channel:      "test-insert-channel",
			Actions: []*querypb.SyncAction{
				{
					Type:      querypb.SyncType_Remove,
					SegmentID: 3,
					NodeID:    2,
				},
			},
			Schema: schema,
			LoadMeta: &querypb.LoadMetaInfo{
				CollectionID: 1,
				PartitionIDs: []int64{1},
			},
			Version:       version,
			IndexInfoList: []*indexpb.IndexInfo{{IndexName: "test"}},
		}
	}
	ch := make(chan struct{})
	suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, int64(2),
		mock.AnythingOfType("*querypb.SyncDistributionRequest")).
		Run(func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) {
			assert.ElementsMatch(suite.T(), []*querypb.SyncDistributionRequest{req},
				[]*querypb.SyncDistributionRequest{expectReqeustFunc(req.GetVersion())})
			close(ch)
		}).
		Return(&commonpb.Status{}, nil)

	observer.Start()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
	}
}

func (suite *LeaderObserverTestSuite) TestIgnoreSyncRemovedSegments() {
	observer := suite.observer
	observer.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	observer.meta.CollectionManager.PutPartition(utils.CreateTestPartition(1, 1))
	observer.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segments := []*datapb.SegmentInfo{
		{
			ID:            2,
			PartitionID:   1,
			InsertChannel: "test-insert-channel",
		},
	}
	channels := []*datapb.VchannelInfo{
		{
			CollectionID: 1,
			ChannelName:  "test-insert-channel",
		},
	}
	schema := utils.CreateTestSchema()
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, int64(1)).Return(schema, nil)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		channels, segments, nil)
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return([]*indexpb.IndexInfo{
		{IndexName: "test"},
	}, nil)
	observer.target.UpdateCollectionNextTarget(int64(1))

	observer.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	observer.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2, 2: 2}, map[int64]*meta.Segment{}))

	expectReqeustFunc := func(version int64) *querypb.SyncDistributionRequest {
		return &querypb.SyncDistributionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SyncDistribution,
			},
			CollectionID: 1,
			ReplicaID:    1,
			Channel:      "test-insert-channel",
			Actions: []*querypb.SyncAction{
				{
					Type:      querypb.SyncType_Remove,
					SegmentID: 3,
					NodeID:    2,
				},
			},
			Schema: schema,
			LoadMeta: &querypb.LoadMetaInfo{
				CollectionID: 1,
				PartitionIDs: []int64{1},
			},
			Version:       version,
			IndexInfoList: []*indexpb.IndexInfo{{IndexName: "test"}},
		}
	}
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().SyncDistribution(mock.Anything, int64(2), mock.AnythingOfType("*querypb.SyncDistributionRequest")).
		Run(func(ctx context.Context, nodeID int64, req *querypb.SyncDistributionRequest) {
			assert.ElementsMatch(suite.T(), []*querypb.SyncDistributionRequest{req},
				[]*querypb.SyncDistributionRequest{expectReqeustFunc(req.GetVersion())})
			called.Store(true)
		}).
		Return(&commonpb.Status{}, nil)

	observer.Start()
	suite.Eventually(func() bool {
		return called.Load()
	},
		10*time.Second,
		500*time.Millisecond,
	)
}

func TestLeaderObserverSuite(t *testing.T) {
	suite.Run(t, new(LeaderObserverTestSuite))
}
