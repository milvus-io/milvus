package observers

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type CollectionObserverSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64 // CollectionID -> PartitionIDs
	channels       map[int64][]*meta.DmChannel
	segments       map[int64][]*datapb.SegmentInfo // CollectionID -> []datapb.SegmentInfo
	loadTypes      map[int64]querypb.LoadType
	replicaNumber  map[int64]int32
	loadPercentage map[int64]int32
	nodes          []int64

	// Mocks
	idAllocator func() (int64, error)
	etcd        *clientv3.Client
	kv          kv.MetaKv
	store       meta.Store

	// Dependencies
	dist      *meta.DistributionManager
	meta      *meta.Meta
	targetMgr *meta.TargetManager

	// Test object
	ob *CollectionObserver
}

func (suite *CollectionObserverSuite) SetupSuite() {
	Params.Init()

	suite.collections = []int64{100, 101}
	suite.partitions = map[int64][]int64{
		100: {10},
		101: {11, 12},
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
	}
	suite.loadTypes = map[int64]querypb.LoadType{
		100: querypb.LoadType_LoadCollection,
		101: querypb.LoadType_LoadPartition,
	}
	suite.replicaNumber = map[int64]int32{
		100: 1,
		101: 1,
	}
	suite.loadPercentage = map[int64]int32{
		100: 0,
		101: 50,
	}
	suite.nodes = []int64{1, 2, 3}
}

func (suite *CollectionObserverSuite) SetupTest() {
	// Mocks
	var err error
	suite.idAllocator = RandomIncrementIDAllocator()
	log.Debug("create embedded etcd KV...")
	config := GenerateEtcdConfig()
	client, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(client, Params.EtcdCfg.MetaRootPath+"-"+RandomMetaRootPath())
	suite.Require().NoError(err)
	log.Debug("create meta store...")
	suite.store = meta.NewMetaStore(suite.kv)

	// Dependencies
	suite.dist = meta.NewDistributionManager()
	suite.meta = meta.NewMeta(suite.idAllocator, suite.store)
	suite.targetMgr = meta.NewTargetManager()

	// Test object
	suite.ob = NewCollectionObserver(
		suite.dist,
		suite.meta,
		suite.targetMgr,
	)

	suite.loadAll()
}

func (suite *CollectionObserverSuite) TearDownTest() {
	suite.ob.Stop()
	suite.kv.Close()
}

func (suite *CollectionObserverSuite) TestObserve() {
	const (
		timeout = 2 * time.Second
	)
	// Not timeout
	Params.QueryCoordCfg.LoadTimeoutSeconds = timeout
	suite.ob.Start(context.Background())

	// Collection 100 loaded before timeout,
	// collection 101 timeout
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: 100,
		Channel:      "100-dmc0",
		Segments:     map[int64]int64{1: 1},
	})
	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: 100,
		Channel:      "100-dmc1",
		Segments:     map[int64]int64{2: 2},
	})
	suite.Eventually(func() bool {
		return suite.isCollectionLoaded(suite.collections[0]) &&
			suite.isCollectionTimeout(suite.collections[1])
	}, timeout*2, timeout/10)
}

func (suite *CollectionObserverSuite) isCollectionLoaded(collection int64) bool {
	exist := suite.meta.Exist(collection)
	percentage := suite.meta.GetLoadPercentage(collection)
	status := suite.meta.GetStatus(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection)
	segments := suite.targetMgr.GetSegmentsByCollection(collection)

	return exist &&
		percentage == 100 &&
		status == querypb.LoadStatus_Loaded &&
		len(replicas) == int(suite.replicaNumber[collection]) &&
		len(channels) == len(suite.channels[collection]) &&
		len(segments) == len(suite.segments[collection])
}

func (suite *CollectionObserverSuite) isCollectionTimeout(collection int64) bool {
	exist := suite.meta.Exist(collection)
	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	channels := suite.targetMgr.GetDmChannelsByCollection(collection)
	segments := suite.targetMgr.GetSegmentsByCollection(collection)

	return !(exist ||
		len(replicas) > 0 ||
		len(channels) > 0 ||
		len(segments) > 0)
}

func (suite *CollectionObserverSuite) loadAll() {
	for _, collection := range suite.collections {
		suite.load(collection)
	}
}

func (suite *CollectionObserverSuite) load(collection int64) {
	// Mock meta data
	replicas, err := suite.meta.ReplicaManager.Spawn(collection, suite.replicaNumber[collection])
	suite.NoError(err)
	for _, replica := range replicas {
		replica.AddNode(suite.nodes...)
	}
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)

	if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
		suite.meta.PutCollection(&meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[collection],
				Status:        querypb.LoadStatus_Loading,
			},
			LoadPercentage: 0,
			CreatedAt:      time.Now(),
		})
	} else {
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
	}

	suite.targetMgr.AddDmChannel(suite.channels[collection]...)
	suite.targetMgr.AddSegment(suite.segments[collection]...)
}

func TestCollectionObserver(t *testing.T) {
	suite.Run(t, new(CollectionObserverSuite))
}
