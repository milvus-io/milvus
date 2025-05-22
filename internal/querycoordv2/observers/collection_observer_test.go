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

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CollectionObserverSuite struct {
	suite.Suite

	// Data
	collections   []int64
	partitions    map[int64][]int64 // CollectionID -> PartitionIDs
	channels      map[int64][]*meta.DmChannel
	segments      map[int64][]*datapb.SegmentInfo // CollectionID -> []datapb.SegmentInfo
	loadTypes     map[int64]querypb.LoadType
	replicaNumber map[int64]int32
	nodes         []int64

	// Mocks
	idAllocator  func() (int64, error)
	etcd         *clientv3.Client
	kv           kv.MetaKv
	store        metastore.QueryCoordCatalog
	broker       *meta.MockBroker
	cluster      *session.MockCluster
	proxyManager *proxyutil.MockProxyClientManager

	// Dependencies
	dist              *meta.DistributionManager
	meta              *meta.Meta
	targetMgr         *meta.TargetManager
	targetObserver    *TargetObserver
	checkerController *checkers.CheckerController

	nodeMgr *session.NodeManager

	// Test object
	ob *CollectionObserver

	ctx context.Context
}

func (suite *CollectionObserverSuite) SetupSuite() {
	paramtable.Init()
	suite.ctx = context.Background()
	suite.collections = []int64{100, 101, 102, 103}
	suite.partitions = map[int64][]int64{
		100: {10},
		101: {11, 12},
		102: {13},
		103: {14},
	}
	suite.channels = map[int64][]*meta.DmChannel{
		100: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "100-dmc0",
			}),
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 100,
				ChannelName:  "100-dmc1",
			}),
		},
		101: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 101,
				ChannelName:  "101-dmc0",
			}),
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 101,
				ChannelName:  "101-dmc1",
			}),
		},
		102: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 102,
				ChannelName:  "102-dmc0",
			}),
		},
		103: {
			meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: 103,
				ChannelName:  "103-dmc0",
			}),
		},
	}
	suite.segments = map[int64][]*datapb.SegmentInfo{
		100: {
			&datapb.SegmentInfo{
				ID:            1,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "100-dmc0",
			},
			&datapb.SegmentInfo{
				ID:            2,
				CollectionID:  100,
				PartitionID:   10,
				InsertChannel: "100-dmc1",
			},
		},
		101: {
			&datapb.SegmentInfo{
				ID:            3,
				CollectionID:  101,
				PartitionID:   11,
				InsertChannel: "101-dmc0",
			},
			&datapb.SegmentInfo{
				ID:            4,
				CollectionID:  101,
				PartitionID:   12,
				InsertChannel: "101-dmc1",
			},
		},
		102: genSegmentsInfo(999, 5, 102, 13, "102-dmc0"),
		103: genSegmentsInfo(10, 2000, 103, 14, "103-dmc0"),
	}
	suite.loadTypes = map[int64]querypb.LoadType{
		100: querypb.LoadType_LoadCollection,
		101: querypb.LoadType_LoadPartition,
		102: querypb.LoadType_LoadCollection,
		103: querypb.LoadType_LoadCollection,
	}
	suite.replicaNumber = map[int64]int32{
		100: 1,
		101: 1,
		102: 1,
		103: 2,
	}
	suite.nodes = []int64{1, 2, 3}

	suite.proxyManager = proxyutil.NewMockProxyClientManager(suite.T())
	suite.proxyManager.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
}

func (suite *CollectionObserverSuite) SetupTest() {
	suite.ctx = context.Background()
	// Mocks
	var err error
	suite.idAllocator = RandomIncrementIDAllocator()
	log.Debug("create embedded etcd KV...")
	config := GenerateEtcdConfig()
	client, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(client, Params.EtcdCfg.MetaRootPath.GetValue()+"-"+RandomMetaRootPath())
	suite.Require().NoError(err)
	log.Debug("create meta store...")
	suite.store = querycoord.NewCatalog(suite.kv)

	// Dependencies
	suite.dist = meta.NewDistributionManager()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(suite.idAllocator, suite.store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.cluster = session.NewMockCluster(suite.T())
	suite.targetObserver = NewTargetObserver(suite.meta,
		suite.targetMgr,
		suite.dist,
		suite.broker,
		suite.cluster,
		suite.nodeMgr,
	)
	suite.checkerController = &checkers.CheckerController{}

	mockCluster := session.NewMockCluster(suite.T())
	mockCluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	// Test object
	suite.ob = NewCollectionObserver(
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.targetObserver,
		suite.checkerController,
		suite.proxyManager,
	)

	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
	}
	suite.targetObserver.Start()
	suite.ob.Start()
	suite.loadAll()

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 1,
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 2,
	}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 3,
	}))
}

func (suite *CollectionObserverSuite) TearDownTest() {
	suite.ob.Stop()
	suite.targetObserver.Stop()
	suite.kv.Close()
}

func (suite *CollectionObserverSuite) TestObserve() {
	const (
		timeout = 3 * time.Second
	)
	// time before load
	time := suite.meta.GetCollection(suite.ctx, suite.collections[2]).UpdatedAt
	// Not timeout
	paramtable.Get().Save(Params.QueryCoordCfg.LoadTimeoutSeconds.Key, "3")

	// Collection 100 loaded before timeout,
	// collection 101 timeout
	ch1 := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "100-dmc0",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "100-dmc0",
			Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
		},
	}
	suite.dist.ChannelDistManager.Update(1, ch1)

	ch2 := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 103,
			ChannelName:  "103-dmc0",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: 103,
			Channel:      "103-dmc0",
			Segments:     make(map[int64]*querypb.SegmentDist),
		},
	}

	ch3 := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "100-dmc1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: 100,
			Channel:      "100-dmc1",
			Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
		},
	}
	suite.dist.ChannelDistManager.Update(2, ch2, ch3)

	ch4 := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 102,
			ChannelName:  "102-dmc0",
		},
		Node: 3,
		View: &meta.LeaderView{
			ID:           3,
			CollectionID: 102,
			Channel:      "102-dmc0",
			Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 5, Version: 0}},
		},
	}

	ch5 := &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 103,
			ChannelName:  "103-dmc0",
		},
		Node: 3,
		View: &meta.LeaderView{
			ID:           3,
			CollectionID: 103,
			Channel:      "103-dmc0",
			Segments:     make(map[int64]*querypb.SegmentDist),
		},
	}

	segmentsInfo, ok := suite.segments[103]
	suite.True(ok)
	for _, segment := range segmentsInfo {
		ch2.View.Segments[segment.GetID()] = &querypb.SegmentDist{
			NodeID: 2, Version: 0,
		}
		ch5.View.Segments[segment.GetID()] = &querypb.SegmentDist{
			NodeID: 3, Version: 0,
		}
	}

	suite.dist.ChannelDistManager.Update(3, ch4, ch5)

	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()

	suite.Eventually(func() bool {
		return suite.isCollectionLoadedContinue(suite.collections[2], time)
	}, timeout-1, timeout/10)

	suite.Eventually(func() bool {
		return suite.isCollectionLoaded(suite.collections[0])
	}, timeout*2, timeout/10)

	suite.Eventually(func() bool {
		return suite.isCollectionTimeout(suite.collections[1])
	}, timeout*2, timeout/10)

	suite.Eventually(func() bool {
		return suite.isCollectionLoaded(suite.collections[3])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) TestObservePartition() {
	const (
		timeout = 3 * time.Second
	)
	paramtable.Get().Save(Params.QueryCoordCfg.LoadTimeoutSeconds.Key, "3")

	// Partition 10 loaded
	// Partition 11 timeout
	suite.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "100-dmc0",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: 100,
			Channel:      "100-dmc0",
			Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
		},
	}, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 101,
			ChannelName:  "101-dmc0",
		},
		Node: 1,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: 101,
			Channel:      "101-dmc0",
		},
	})

	suite.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: 100,
			ChannelName:  "100-dmc1",
		},
		Node: 2,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: 100,
			Channel:      "100-dmc1",
			Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
		},
	})

	suite.Eventually(func() bool {
		return suite.isPartitionLoaded(suite.partitions[100][0])
	}, timeout*2, timeout/10)

	suite.Eventually(func() bool {
		return suite.isPartitionTimeout(suite.collections[1], suite.partitions[101][0])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) isCollectionLoaded(collection int64) bool {
	ctx := suite.ctx
	exist := suite.meta.Exist(ctx, collection)
	percentage := suite.meta.CalculateLoadPercentage(ctx, collection)
	status := suite.meta.CalculateLoadStatus(ctx, collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(ctx, collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(ctx, collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetSealedSegmentsByCollection(ctx, collection, meta.CurrentTarget)

	return exist &&
		percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(replicas) == int(suite.replicaNumber[collection]) &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(suite.segments[collection])
}

func (suite *CollectionObserverSuite) isPartitionLoaded(partitionID int64) bool {
	ctx := suite.ctx
	partition := suite.meta.GetPartition(ctx, partitionID)
	if partition == nil {
		return false
	}
	collection := partition.GetCollectionID()
	percentage := suite.meta.GetPartitionLoadPercentage(ctx, partitionID)
	status := partition.GetStatus()
	channels := suite.targetMgr.GetDmChannelsByCollection(ctx, collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetSealedSegmentsByPartition(ctx, collection, partitionID, meta.CurrentTarget)
	expectedSegments := lo.Filter(suite.segments[collection], func(seg *datapb.SegmentInfo, _ int) bool {
		return seg.PartitionID == partitionID
	})
	return percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(expectedSegments)
}

func (suite *CollectionObserverSuite) isCollectionTimeout(collection int64) bool {
	ctx := suite.ctx
	exist := suite.meta.Exist(ctx, collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(ctx, collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(ctx, collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetSealedSegmentsByCollection(ctx, collection, meta.CurrentTarget)
	return !(exist ||
		len(replicas) > 0 ||
		len(channels) > 0 ||
		len(segments) > 0)
}

func (suite *CollectionObserverSuite) isPartitionTimeout(collection int64, partitionID int64) bool {
	ctx := suite.ctx
	partition := suite.meta.GetPartition(ctx, partitionID)
	segments := suite.targetMgr.GetSealedSegmentsByPartition(ctx, collection, partitionID, meta.CurrentTarget)
	return partition == nil && len(segments) == 0
}

func (suite *CollectionObserverSuite) isCollectionLoadedContinue(collection int64, beforeTime time.Time) bool {
	return suite.meta.GetCollection(suite.ctx, collection).UpdatedAt.After(beforeTime)
}

func (suite *CollectionObserverSuite) loadAll() {
	ctx := suite.ctx
	for _, collection := range suite.collections {
		suite.load(collection)
	}
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, suite.collections[0])
	suite.targetMgr.UpdateCollectionNextTarget(ctx, suite.collections[0])
	suite.targetMgr.UpdateCollectionCurrentTarget(ctx, suite.collections[2])
	suite.targetMgr.UpdateCollectionNextTarget(ctx, suite.collections[2])
}

func (suite *CollectionObserverSuite) load(collection int64) {
	ctx := suite.ctx
	// Mock meta data
	replicas, err := suite.meta.ReplicaManager.Spawn(ctx, collection, map[string]int{meta.DefaultResourceGroupName: int(suite.replicaNumber[collection])}, nil)
	suite.NoError(err)
	for _, replica := range replicas {
		replica.AddRWNode(suite.nodes...)
	}
	err = suite.meta.ReplicaManager.Put(ctx, replicas...)
	suite.NoError(err)

	suite.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collection,
			ReplicaNumber: suite.replicaNumber[collection],
			Status:        querypb.LoadStatus_Loading,
			LoadType:      suite.loadTypes[collection],
		},
		LoadPercentage: 0,
		CreatedAt:      time.Now(),
	})

	for _, partition := range suite.partitions[collection] {
		suite.meta.PutPartition(ctx, &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID:  collection,
				PartitionID:   partition,
				ReplicaNumber: suite.replicaNumber[collection],
				Status:        querypb.LoadStatus_Loading,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		})
	}

	dmChannels := make([]*datapb.VchannelInfo, 0)
	for _, channel := range suite.channels[collection] {
		dmChannels = append(dmChannels, &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel.GetChannelName(),
		})
	}

	allSegments := make([]*datapb.SegmentInfo, 0) // partitionID -> segments
	for _, segment := range suite.segments[collection] {
		allSegments = append(allSegments, &datapb.SegmentInfo{
			ID:            segment.GetID(),
			PartitionID:   segment.PartitionID,
			InsertChannel: segment.GetInsertChannel(),
		})
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collection).Return(dmChannels, allSegments, nil)
	suite.targetMgr.UpdateCollectionNextTarget(ctx, collection)

	suite.ob.LoadCollection(context.Background(), collection)
}

func TestCollectionObserver(t *testing.T) {
	suite.Run(t, new(CollectionObserverSuite))
}

func genSegmentsInfo(count int, start int, collID int64, partitionID int64, insertChannel string) []*datapb.SegmentInfo {
	ret := make([]*datapb.SegmentInfo, 0, count)
	for i := 0; i < count; i++ {
		segment := &datapb.SegmentInfo{
			ID:            int64(start + i),
			CollectionID:  collID,
			PartitionID:   partitionID,
			InsertChannel: insertChannel,
		}
		ret = append(ret, segment)
	}
	return ret
}
