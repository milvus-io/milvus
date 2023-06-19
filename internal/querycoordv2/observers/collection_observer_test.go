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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	idAllocator func() (int64, error)
	etcd        *clientv3.Client
	kv          kv.MetaKv
	store       meta.Store
	broker      *meta.MockBroker

	// Dependencies
	dist              *meta.DistributionManager
	meta              *meta.Meta
	targetMgr         *meta.TargetManager
	targetObserver    *TargetObserver
	leaderObserver    *LeaderObserver
	checkerController *checkers.CheckerController

	// Test object
	ob *CollectionObserver
}

func (suite *CollectionObserverSuite) SetupSuite() {
	Params.Init()

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
}

func (suite *CollectionObserverSuite) SetupTest() {
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
	suite.store = meta.NewMetaStore(suite.kv)

	// Dependencies
	suite.dist = meta.NewDistributionManager()
	suite.meta = meta.NewMeta(suite.idAllocator, suite.store, session.NewNodeManager())
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.targetObserver = NewTargetObserver(suite.meta,
		suite.targetMgr,
		suite.dist,
		suite.broker,
	)
	suite.checkerController = &checkers.CheckerController{}

	mockCluster := session.NewMockCluster(suite.T())
	suite.leaderObserver = NewLeaderObserver(suite.dist, suite.meta, suite.targetMgr, suite.broker, mockCluster)
	mockCluster.EXPECT().SyncDistribution(mock.Anything, mock.Anything, mock.Anything).Return(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil).Maybe()

	// Test object
	suite.ob = NewCollectionObserver(
		suite.dist,
		suite.meta,
		suite.targetMgr,
		suite.targetObserver,
		suite.leaderObserver,
		suite.checkerController,
	)

	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
	}
	suite.targetObserver.Start(context.Background())
	suite.leaderObserver.Start(context.TODO())
	suite.ob.Start(context.Background())
	suite.loadAll()
}

func (suite *CollectionObserverSuite) TearDownTest() {
	suite.targetObserver.Stop()
	suite.ob.Stop()
	suite.kv.Close()
}

func (suite *CollectionObserverSuite) TestObserve() {
	const (
		timeout = 3 * time.Second
	)
	// time before load
	time := suite.meta.GetCollection(suite.collections[2]).UpdatedAt
	// Not timeout
	paramtable.Get().Save(Params.QueryCoordCfg.LoadTimeoutSeconds.Key, "3")

	// Collection 100 loaded before timeout,
	// collection 101 timeout
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 100,
		Channel:      "100-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(3, &meta.LeaderView{
		ID:           3,
		CollectionID: 102,
		Channel:      "102-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 5, Version: 0}},
	})

	segmentsInfo, ok := suite.segments[103]
	suite.True(ok)
	lview := &meta.LeaderView{
		ID:           3,
		CollectionID: 13,
		Channel:      "103-dmc0",
		Segments:     make(map[int64]*querypb.SegmentDist),
	}
	for _, segment := range segmentsInfo {
		lview.Segments[segment.GetID()] = &querypb.SegmentDist{
			NodeID: 3, Version: 0,
		}
	}
	suite.dist.LeaderViewManager.Update(3, lview)

	suite.Eventually(func() bool {
		return suite.isCollectionLoaded(suite.collections[0])
	}, timeout*2, timeout/10)

	suite.Eventually(func() bool {
		return suite.isCollectionLoadedContinue(suite.collections[2], time)
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
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]*querypb.SegmentDist{1: {NodeID: 1, Version: 0}},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 100,
		Channel:      "100-dmc1",
		Segments:     map[int64]*querypb.SegmentDist{2: {NodeID: 2, Version: 0}},
	})
	// Partition 11 timeout
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 101,
		Channel:      "",
		Segments:     map[int64]*querypb.SegmentDist{},
	})

	suite.Eventually(func() bool {
		return suite.isPartitionLoaded(suite.partitions[100][0])
	}, timeout*2, timeout/10)

	suite.Eventually(func() bool {
		return suite.isPartitionTimeout(suite.collections[1], suite.partitions[101][0])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) isCollectionLoaded(collection int64) bool {
	exist := suite.meta.Exist(collection)
	percentage := suite.meta.CalculateLoadPercentage(collection)
	status := suite.meta.CalculateLoadStatus(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetHistoricalSegmentsByCollection(collection, meta.CurrentTarget)

	return exist &&
		percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(replicas) == int(suite.replicaNumber[collection]) &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(suite.segments[collection])
}

func (suite *CollectionObserverSuite) isPartitionLoaded(partitionID int64) bool {
	partition := suite.meta.GetPartition(partitionID)
	if partition == nil {
		return false
	}
	collection := partition.GetCollectionID()
	percentage := suite.meta.GetPartitionLoadPercentage(partitionID)
	status := partition.GetStatus()
	channels := suite.targetMgr.GetDmChannelsByCollection(collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetHistoricalSegmentsByPartition(collection, partitionID, meta.CurrentTarget)
	expectedSegments := lo.Filter(suite.segments[collection], func(seg *datapb.SegmentInfo, _ int) bool {
		return seg.PartitionID == partitionID
	})
	return percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(expectedSegments)
}

func (suite *CollectionObserverSuite) isCollectionTimeout(collection int64) bool {
	exist := suite.meta.Exist(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection, meta.CurrentTarget)
	segments := suite.targetMgr.GetHistoricalSegmentsByCollection(collection, meta.CurrentTarget)
	return !(exist ||
		len(replicas) > 0 ||
		len(channels) > 0 ||
		len(segments) > 0)
}

func (suite *CollectionObserverSuite) isPartitionTimeout(collection int64, partitionID int64) bool {
	partition := suite.meta.GetPartition(partitionID)
	segments := suite.targetMgr.GetHistoricalSegmentsByPartition(collection, partitionID, meta.CurrentTarget)
	return partition == nil && len(segments) == 0
}

func (suite *CollectionObserverSuite) isCollectionLoadedContinue(collection int64, beforeTime time.Time) bool {
	return suite.meta.GetCollection(collection).UpdatedAt.After(beforeTime)
}

func (suite *CollectionObserverSuite) loadAll() {
	for _, collection := range suite.collections {
		suite.load(collection)
	}
	suite.targetMgr.UpdateCollectionCurrentTarget(suite.collections[0])
	suite.targetMgr.UpdateCollectionCurrentTarget(suite.collections[2])
}

func (suite *CollectionObserverSuite) load(collection int64) {
	// Mock meta data
	replicas, err := suite.meta.ReplicaManager.Spawn(collection, suite.replicaNumber[collection], meta.DefaultResourceGroupName)
	suite.NoError(err)
	for _, replica := range replicas {
		replica.AddNode(suite.nodes...)
	}
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)

	suite.meta.PutCollection(&meta.Collection{
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
		suite.meta.PutPartition(&meta.Partition{
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

	partitions := suite.partitions[collection]
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collection).Return(dmChannels, allSegments, nil)
	suite.targetMgr.UpdateCollectionNextTargetWithPartitions(collection, partitions...)
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
