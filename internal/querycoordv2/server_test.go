package querycoordv2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

type ServerSuite struct {
	suite.Suite

	// Data
	collections   []int64
	partitions    map[int64][]int64
	channels      map[int64][]string
	segments      map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	loadTypes     map[int64]querypb.LoadType
	replicaNumber map[int64]int32

	// Mocks
	broker *meta.MockBroker

	server *Server
	nodes  []*mocks.MockQueryNode
}

func (suite *ServerSuite) SetupSuite() {
	Params.Init()
	Params.EtcdCfg = params.GenerateEtcdConfig()

	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101},
		1001: {102, 103},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
		},
		1001: {
			102: {5, 6},
			103: {7, 8},
		},
	}
	suite.loadTypes = map[int64]querypb.LoadType{
		1000: querypb.LoadType_LoadCollection,
		1001: querypb.LoadType_LoadPartition,
	}
	suite.replicaNumber = map[int64]int32{
		1000: 1,
		1001: 3,
	}
	suite.nodes = make([]*mocks.MockQueryNode, 3)
}

func (suite *ServerSuite) SetupTest() {
	var err error

	suite.server, err = newQueryCoord()
	suite.Require().NoError(err)
	suite.hackServer()
	err = suite.server.Start()
	suite.NoError(err)

	for i := range suite.nodes {
		suite.nodes[i] = mocks.NewMockQueryNode(suite.T(), suite.server.etcdCli)
		err := suite.nodes[i].Start()
		suite.Require().NoError(err)
		ok := suite.waitNodeUp(suite.nodes[i], 5*time.Second)
		suite.Require().True(ok)
	}

	suite.loadAll()
	for _, collection := range suite.collections {
		suite.assertLoaded(collection)
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
	}
}

func (suite *ServerSuite) TearDownTest() {
	err := suite.server.Stop()
	suite.Require().NoError(err)
	for _, node := range suite.nodes {
		if node != nil {
			node.Stop()
		}
	}
}

func (suite *ServerSuite) TestRecover() {
	err := suite.server.Stop()
	suite.NoError(err)

	suite.server, err = newQueryCoord()
	suite.NoError(err)
	suite.hackServer()
	err = suite.server.Start()
	suite.NoError(err)

	for _, collection := range suite.collections {
		suite.assertLoaded(collection)
	}
}

func (suite *ServerSuite) TestNodeUp() {
	newNode := mocks.NewMockQueryNode(suite.T(), suite.server.etcdCli)
	newNode.EXPECT().GetDataDistribution(mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{}, nil)
	err := newNode.Start()
	suite.NoError(err)
	defer newNode.Stop()

	suite.Eventually(func() bool {
		node := suite.server.nodeMgr.Get(newNode.ID)
		if node == nil {
			return false
		}
		for _, collection := range suite.collections {
			replica := suite.server.meta.ReplicaManager.GetByCollectionAndNode(collection, newNode.ID)
			if replica == nil {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Second)

}

func (suite *ServerSuite) TestNodeDown() {
	downNode := suite.nodes[0]
	downNode.Stop()
	suite.nodes[0] = nil

	suite.Eventually(func() bool {
		node := suite.server.nodeMgr.Get(downNode.ID)
		if node != nil {
			return false
		}
		for _, collection := range suite.collections {
			replica := suite.server.meta.ReplicaManager.GetByCollectionAndNode(collection, downNode.ID)
			if replica != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Second)
}

func (suite *ServerSuite) waitNodeUp(node *mocks.MockQueryNode, timeout time.Duration) bool {
	start := time.Now()
	for time.Since(start) < timeout {
		if suite.server.nodeMgr.Get(node.ID) != nil {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func (suite *ServerSuite) loadAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			req := &querypb.LoadCollectionRequest{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[collection],
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		} else {
			req := &querypb.LoadPartitionsRequest{
				CollectionID:  collection,
				PartitionIDs:  suite.partitions[collection],
				ReplicaNumber: suite.replicaNumber[collection],
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		}
	}
}

func (suite *ServerSuite) assertLoaded(collection int64) {
	suite.True(suite.server.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.server.targetMgr.GetDmChannel(channel))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.NotNil(suite.server.targetMgr.GetSegment(segment))
		}
	}
}

func (suite *ServerSuite) expectGetRecoverInfo(collection int64) {
	var (
		mu             sync.Mutex
		vChannels      []*datapb.VchannelInfo
		segmentBinlogs []*datapb.SegmentBinlogs
	)

	for partition, segments := range suite.segments[collection] {
		segments := segments
		suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, collection, partition).Maybe().Return(func(ctx context.Context, collectionID, partitionID int64) []*datapb.VchannelInfo {
			mu.Lock()
			vChannels = []*datapb.VchannelInfo{}
			for _, channel := range suite.channels[collection] {
				vChannels = append(vChannels, &datapb.VchannelInfo{
					CollectionID: collection,
					ChannelName:  channel,
				})
			}
			segmentBinlogs = []*datapb.SegmentBinlogs{}
			for _, segment := range segments {
				segmentBinlogs = append(segmentBinlogs, &datapb.SegmentBinlogs{
					SegmentID:     segment,
					InsertChannel: suite.channels[collection][segment%2],
				})
			}
			return vChannels
		},
			func(ctx context.Context, collectionID, partitionID int64) []*datapb.SegmentBinlogs {
				return segmentBinlogs
			},
			func(ctx context.Context, collectionID, partitionID int64) error {
				mu.Unlock()
				return nil
			},
		)
	}
}

func (suite *ServerSuite) updateCollectionStatus(collectionID int64, status querypb.LoadStatus) {
	collection := suite.server.meta.GetCollection(collectionID)
	if collection != nil {
		collection := collection.Clone()
		collection.LoadPercentage = 0
		if status == querypb.LoadStatus_Loaded {
			collection.LoadPercentage = 100
		}
		collection.CollectionLoadInfo.Status = status
		suite.server.meta.UpdateCollection(collection)
	} else {
		partitions := suite.server.meta.GetPartitionsByCollection(collectionID)
		for _, partition := range partitions {
			partition := partition.Clone()
			partition.LoadPercentage = 0
			if status == querypb.LoadStatus_Loaded {
				partition.LoadPercentage = 100
			}
			partition.PartitionLoadInfo.Status = status
			suite.server.meta.UpdatePartition(partition)
		}
	}
}

func (suite *ServerSuite) hackServer() {
	suite.broker = meta.NewMockBroker(suite.T())
	suite.server.broker = suite.broker
	suite.server.taskScheduler = task.NewScheduler(
		suite.server.ctx,
		suite.server.meta,
		suite.server.dist,
		suite.server.targetMgr,
		suite.broker,
		suite.server.cluster,
		suite.server.nodeMgr,
	)
	suite.server.handoffObserver = observers.NewHandoffObserver(
		suite.server.store,
		suite.server.meta,
		suite.server.dist,
		suite.server.targetMgr,
	)
	suite.server.distController = dist.NewDistController(
		suite.server.cluster,
		suite.server.nodeMgr,
		suite.server.dist,
		suite.server.targetMgr,
		suite.server.taskScheduler,
	)
	suite.server.checkerController = checkers.NewCheckerController(
		suite.server.meta,
		suite.server.dist,
		suite.server.targetMgr,
		suite.server.balancer,
		suite.server.taskScheduler,
	)

	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).Return(&schemapb.CollectionSchema{}, nil).Maybe()
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
		}
		suite.expectGetRecoverInfo(collection)
	}
	log.Debug("server hacked")
}

func newQueryCoord() (*Server, error) {
	server, err := NewQueryCoord(context.Background(), dependency.NewDefaultFactory(true))
	if err != nil {
		return nil, err
	}

	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	if err != nil {
		return nil, err
	}
	server.SetEtcdClient(etcdCli)

	err = server.Init()
	return server, err
}

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}
