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

package querycoordv2

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	coordMocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/merr"
	"github.com/milvus-io/milvus/internal/util/paramtable"
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
	params.GenerateEtcdConfig()

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
		suite.nodes[i] = mocks.NewMockQueryNode(suite.T(), suite.server.etcdCli, int64(i))
		err := suite.nodes[i].Start()
		suite.Require().NoError(err)
		ok := suite.waitNodeUp(suite.nodes[i], 5*time.Second)
		suite.Require().True(ok)
		suite.server.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, suite.nodes[i].ID)
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

func (suite *ServerSuite) TestRecoverFailed() {
	err := suite.server.Stop()
	suite.NoError(err)

	suite.server, err = newQueryCoord()
	suite.NoError(err)

	broker := meta.NewMockBroker(suite.T())
	broker.EXPECT().GetPartitions(context.TODO(), int64(1000)).Return(nil, errors.New("CollectionNotExist"))
	broker.EXPECT().GetRecoveryInfo(context.TODO(), int64(1001), mock.Anything).Return(nil, nil, errors.New("CollectionNotExist"))
	suite.server.targetMgr = meta.NewTargetManager(broker, suite.server.meta)
	err = suite.server.Start()
	suite.NoError(err)

	for _, collection := range suite.collections {
		suite.False(suite.server.meta.Exist(collection))
		suite.Nil(suite.server.targetMgr.GetDmChannelsByCollection(collection, meta.NextTarget))
	}
}

func (suite *ServerSuite) TestNodeUp() {
	newNode := mocks.NewMockQueryNode(suite.T(), suite.server.etcdCli, 100)
	newNode.EXPECT().GetDataDistribution(mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{Status: merr.Status(nil)}, nil)
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

func (suite *ServerSuite) TestNodeUpdate() {
	downNode := suite.nodes[0]
	downNode.Stopping()

	suite.Eventually(func() bool {
		node := suite.server.nodeMgr.Get(downNode.ID)
		return node.IsStoppingState()
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

func (suite *ServerSuite) TestDisableActiveStandby() {
	paramtable.Get().Save(Params.QueryCoordCfg.EnableActiveStandby.Key, "false")

	err := suite.server.Stop()
	suite.NoError(err)

	suite.server, err = newQueryCoord()
	suite.NoError(err)
	suite.Equal(commonpb.StateCode_Initializing, suite.server.status.Load().(commonpb.StateCode))
	suite.hackServer()
	err = suite.server.Start()
	suite.NoError(err)
	err = suite.server.Register()
	suite.NoError(err)
	suite.Equal(commonpb.StateCode_Healthy, suite.server.status.Load().(commonpb.StateCode))

	states, err := suite.server.GetComponentStates(context.Background())
	suite.NoError(err)
	suite.Equal(commonpb.StateCode_Healthy, states.GetState().GetStateCode())
}

func (suite *ServerSuite) TestEnableActiveStandby() {
	paramtable.Get().Save(Params.QueryCoordCfg.EnableActiveStandby.Key, "true")

	err := suite.server.Stop()
	suite.NoError(err)

	suite.server, err = newQueryCoord()
	suite.NoError(err)
	mockRootCoord := coordMocks.NewRootCoord(suite.T())
	mockDataCoord := coordMocks.NewDataCoord(suite.T())

	mockRootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Status(nil),
		Schema: &schemapb.CollectionSchema{},
	}, nil).Maybe()
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			req := &milvuspb.ShowPartitionsRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
				),
				CollectionID: collection,
			}
			mockRootCoord.EXPECT().ShowPartitionsInternal(mock.Anything, req).Return(&milvuspb.ShowPartitionsResponse{
				Status:       merr.Status(nil),
				PartitionIDs: suite.partitions[collection],
			}, nil).Maybe()
		}
		suite.expectGetRecoverInfoByMockDataCoord(collection, mockDataCoord)

	}
	err = suite.server.SetRootCoord(mockRootCoord)
	suite.NoError(err)
	err = suite.server.SetDataCoord(mockDataCoord)
	suite.NoError(err)
	//suite.hackServer()
	states1, err := suite.server.GetComponentStates(context.Background())
	suite.NoError(err)
	suite.Equal(commonpb.StateCode_StandBy, states1.GetState().GetStateCode())
	err = suite.server.Register()
	suite.NoError(err)
	err = suite.server.Start()
	suite.NoError(err)

	states2, err := suite.server.GetComponentStates(context.Background())
	suite.NoError(err)
	suite.Equal(commonpb.StateCode_Healthy, states2.GetState().GetStateCode())

	paramtable.Get().Save(Params.QueryCoordCfg.EnableActiveStandby.Key, "false")
}

func (suite *ServerSuite) TestStop() {
	suite.server.Stop()
	// Stop has to be idempotent
	suite.server.Stop()
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
		suite.NotNil(suite.server.targetMgr.GetDmChannel(collection, channel, meta.NextTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.NotNil(suite.server.targetMgr.GetHistoricalSegment(collection, segment, meta.NextTarget))
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

func (suite *ServerSuite) expectGetRecoverInfoByMockDataCoord(collection int64, dataCoord *coordMocks.DataCoord) {
	var (
		vChannels      []*datapb.VchannelInfo
		segmentBinlogs []*datapb.SegmentBinlogs
	)

	for partition, segments := range suite.segments[collection] {
		segments := segments
		getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
			),
			CollectionID: collection,
			PartitionID:  partition,
		}
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
		dataCoord.EXPECT().GetRecoveryInfo(mock.Anything, getRecoveryInfoRequest).Maybe().Return(&datapb.GetRecoveryInfoResponse{
			Status:   merr.Status(nil),
			Channels: vChannels,
			Binlogs:  segmentBinlogs,
		}, nil)
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
	suite.server.targetMgr = meta.NewTargetManager(suite.broker, suite.server.meta)
	suite.server.taskScheduler = task.NewScheduler(
		suite.server.ctx,
		suite.server.meta,
		suite.server.dist,
		suite.server.targetMgr,
		suite.broker,
		suite.server.cluster,
		suite.server.nodeMgr,
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
	suite.server.targetObserver = observers.NewTargetObserver(
		suite.server.meta,
		suite.server.targetMgr,
		suite.server.dist,
		suite.broker,
	)
	suite.server.collectionObserver = observers.NewCollectionObserver(
		suite.server.dist,
		suite.server.meta,
		suite.server.targetMgr,
		suite.server.targetObserver,
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
	server, err := NewQueryCoord(context.Background())
	if err != nil {
		return nil, err
	}

	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		return nil, err
	}
	server.SetEtcdClient(etcdCli)
	server.SetQueryNodeCreator(session.DefaultQueryNodeCreator)
	err = server.Init()
	return server, err
}

func TestServer(t *testing.T) {
	suite.Run(t, new(ServerSuite))
}
