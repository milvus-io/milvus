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
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	coordMocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tikv"
)

func TestMain(m *testing.M) {
	paramtable.Init()

	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}

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

	tikvCli *txnkv.Client
	server  *Server
	nodes   []*mocks.MockQueryNode
	ctx     context.Context
}

var testMeta string

func (suite *ServerSuite) SetupSuite() {
	paramtable.Init()
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
	suite.ctx = context.Background()
}

func (suite *ServerSuite) SetupTest() {
	var err error
	paramtable.Get().Save(paramtable.Get().MetaStoreCfg.MetaStoreType.Key, testMeta)
	suite.tikvCli = tikv.SetupLocalTxn()
	suite.server, err = suite.newQueryCoord()

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
		suite.server.meta.ResourceManager.HandleNodeUp(suite.ctx, suite.nodes[i].ID)
		suite.expectLoadAndReleasePartitions(suite.nodes[i])
	}

	suite.loadAll()
	for _, collection := range suite.collections {
		suite.True(suite.server.meta.Exist(suite.ctx, collection))
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
	paramtable.Get().Reset(paramtable.Get().MetaStoreCfg.MetaStoreType.Key)
}

func (suite *ServerSuite) TestRecover() {
	err := suite.server.Stop()
	suite.NoError(err)

	//  stopping querynode
	downNode := suite.nodes[0]
	downNode.Stopping()

	suite.server, err = suite.newQueryCoord()
	suite.NoError(err)
	suite.hackServer()
	err = suite.server.Start()
	suite.NoError(err)

	for _, collection := range suite.collections {
		suite.True(suite.server.meta.Exist(suite.ctx, collection))
	}

	suite.True(suite.server.nodeMgr.IsStoppingNode(suite.nodes[0].ID))
}

func (suite *ServerSuite) TestNodeUp() {
	node1 := mocks.NewMockQueryNode(suite.T(), suite.server.etcdCli, 100)
	node1.EXPECT().GetDataDistribution(mock.Anything, mock.Anything).Return(&querypb.GetDataDistributionResponse{Status: merr.Success()}, nil)
	err := node1.Start()
	suite.NoError(err)
	defer node1.Stop()

	suite.Eventually(func() bool {
		node := suite.server.nodeMgr.Get(node1.ID)
		if node == nil {
			return false
		}
		for _, collection := range suite.collections {
			replica := suite.server.meta.ReplicaManager.GetByCollectionAndNode(suite.ctx, collection, node1.ID)
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
			replica := suite.server.meta.ReplicaManager.GetByCollectionAndNode(suite.ctx, collection, downNode.ID)
			if replica != nil {
				return false
			}
		}
		return true
	}, 5*time.Second, time.Second)
}

// func (suite *ServerSuite) TestDisableActiveStandby() {
// 	paramtable.Get().Save(Params.QueryCoordCfg.EnableActiveStandby.Key, "false")

// 	err := suite.server.Stop()
// 	suite.NoError(err)

// 	suite.server, err = suite.newQueryCoord()
// 	suite.NoError(err)
// 	suite.Equal(commonpb.StateCode_Initializing, suite.server.State())
// 	suite.hackServer()
// 	err = suite.server.Start()
// 	suite.NoError(err)
// 	err = suite.server.Register()
// 	suite.NoError(err)
// 	suite.Equal(commonpb.StateCode_Healthy, suite.server.State())

// 	states, err := suite.server.GetComponentStates(context.Background(), nil)
// 	suite.NoError(err)
// 	suite.Equal(commonpb.StateCode_Healthy, states.GetState().GetStateCode())
// }

// func (suite *ServerSuite) TestEnableActiveStandby() {
// 	paramtable.Get().Save(Params.QueryCoordCfg.EnableActiveStandby.Key, "true")
// 	defer paramtable.Get().Reset(Params.QueryCoordCfg.EnableActiveStandby.Key)

// 	err := suite.server.Stop()
// 	suite.NoError(err)

// 	suite.server, err = suite.newQueryCoord()
// 	suite.NoError(err)
// 	mockRootCoord := coordMocks.NewMockRootCoordClient(suite.T())
// 	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
// 		State: &milvuspb.ComponentInfo{
// 			StateCode: commonpb.StateCode_Healthy,
// 		},
// 		Status: merr.Success(),
// 	}, nil).Maybe()
// 	mockDataCoord := coordMocks.NewMockDataCoordClient(suite.T())
// 	mockDataCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
// 		State: &milvuspb.ComponentInfo{
// 			StateCode: commonpb.StateCode_Healthy,
// 		},
// 		Status: merr.Success(),
// 	}, nil).Maybe()

// 	mockRootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
// 		Status: merr.Success(),
// 		Schema: &schemapb.CollectionSchema{},
// 	}, nil).Maybe()
// 	for _, collection := range suite.collections {
// 		req := &milvuspb.ShowPartitionsRequest{
// 			Base: commonpbutil.NewMsgBase(
// 				commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
// 			),
// 			CollectionID: collection,
// 		}
// 		mockRootCoord.EXPECT().ShowPartitions(mock.Anything, req).Return(&milvuspb.ShowPartitionsResponse{
// 			Status:       merr.Success(),
// 			PartitionIDs: suite.partitions[collection],
// 		}, nil).Maybe()
// 		suite.expectGetRecoverInfoByMockDataCoord(collection, mockDataCoord)
// 	}
// 	err = suite.server.SetRootCoordClient(mockRootCoord)
// 	suite.NoError(err)
// 	err = suite.server.SetDataCoordClient(mockDataCoord)
// 	suite.NoError(err)
// 	// suite.hackServer()
// 	states1, err := suite.server.GetComponentStates(context.Background(), nil)
// 	suite.NoError(err)
// 	suite.Equal(commonpb.StateCode_StandBy, states1.GetState().GetStateCode())
// 	err = suite.server.Register()
// 	suite.NoError(err)

// 	suite.Eventually(func() bool {
// 		state, err := suite.server.GetComponentStates(context.Background(), nil)
// 		return err == nil && state.GetState().GetStateCode() == commonpb.StateCode_Healthy
// 	}, time.Second*5, time.Millisecond*200)
// }

func (suite *ServerSuite) TestStop() {
	suite.server.Stop()
	// Stop has to be idempotent
	suite.server.Stop()
}

func (suite *ServerSuite) TestUpdateAutoBalanceConfigLoop() {
	suite.server.Stop()

	Params.Save(Params.QueryCoordCfg.CheckAutoBalanceConfigInterval.Key, "1")
	defer Params.Reset(Params.QueryCoordCfg.CheckAutoBalanceConfigInterval.Key)

	suite.Run("test old node exist", func() {
		Params.Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
		defer Params.Reset(Params.QueryCoordCfg.AutoBalance.Key)
		server := &Server{}
		mockSession := sessionutil.NewMockSession(suite.T())
		server.session = mockSession

		oldSessions := make(map[string]*sessionutil.Session)
		oldSessions["s1"] = sessionutil.NewSession(context.Background())
		mockSession.EXPECT().GetSessionsWithVersionRange(mock.Anything, mock.Anything).Return(oldSessions, 0, nil).Maybe()

		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(1500 * time.Millisecond)
			server.updateBalanceConfigLoop(ctx)
		}()
		// old query node exist, disable auto balance
		suite.Eventually(func() bool {
			return !Params.QueryCoordCfg.AutoBalance.GetAsBool()
		}, 5*time.Second, 1*time.Second)

		cancel()
		wg.Wait()
	})

	suite.Run("all old node down", func() {
		Params.Save(Params.QueryCoordCfg.AutoBalance.Key, "false")
		defer Params.Reset(Params.QueryCoordCfg.AutoBalance.Key)
		server := &Server{}
		mockSession := sessionutil.NewMockSession(suite.T())
		server.session = mockSession
		mockSession.EXPECT().GetSessionsWithVersionRange(mock.Anything, mock.Anything).Return(nil, 0, nil).Maybe()

		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(1500 * time.Millisecond)
			server.updateBalanceConfigLoop(ctx)
		}()
		// all old query node down, enable auto balance
		suite.Eventually(func() bool {
			return Params.QueryCoordCfg.AutoBalance.GetAsBool()
		}, 5*time.Second, 1*time.Second)

		cancel()
		wg.Wait()
	})
}

func TestApplyLoadConfigChanges(t *testing.T) {
	mockey.PatchConvey("TestApplyLoadConfigChanges", t, func() {
		ctx := context.Background()

		// Create mock server
		testServer := &Server{}
		testServer.meta = &meta.Meta{}
		testServer.ctx = ctx

		// Create mock collection with IsUserSpecifiedReplicaMode = false
		mockCollection1 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:             1001,
				UserSpecifiedReplicaMode: false,
			},
		}

		// Create mock collection with IsUserSpecifiedReplicaMode = true
		mockCollection2 := &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:             1002,
				UserSpecifiedReplicaMode: true,
			},
		}

		// Mock meta.CollectionManager.GetAll to return collection IDs
		mockey.Mock((*meta.CollectionManager).GetAll).Return([]int64{1001, 1002}).Build()

		// Mock meta.CollectionManager.GetCollection to return different collections
		mockey.Mock((*meta.CollectionManager).GetCollection).To(func(m *meta.CollectionManager, ctx context.Context, collectionID int64) *meta.Collection {
			if collectionID == 1001 {
				return mockCollection1
			} else if collectionID == 1002 {
				return mockCollection2
			}
			return nil
		}).Build()

		// Mock paramtable.ParamItem.GetAsUint32() for ClusterLevelLoadReplicaNumber
		mockey.Mock((*paramtable.ParamItem).GetAsUint32).Return(uint32(2)).Build()

		// Mock paramtable.ParamItem.GetAsStrings() for ClusterLevelLoadResourceGroups
		mockey.Mock((*paramtable.ParamItem).GetAsStrings).Return([]string{"default"}).Build()

		// Mock UpdateLoadConfig to capture the call
		var updateLoadConfigCalled bool
		var capturedCollectionIDs []int64
		var capturedReplicaNum int32
		var capturedRGs []string
		mockey.Mock((*Server).updateLoadConfig).To(func(s *Server, ctx context.Context, collectionIDs []int64, newReplicaNum int32, newRGs []string) error {
			updateLoadConfigCalled = true
			capturedCollectionIDs = collectionIDs
			capturedReplicaNum = newReplicaNum
			capturedRGs = newRGs
			return nil
		}).Build()

		replicaNum := paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsUint32()
		rgs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
		// Call applyLoadConfigChanges
		testServer.applyLoadConfigChanges(ctx, int32(replicaNum), rgs)

		// Verify UpdateLoadConfig was called
		assert.True(t, updateLoadConfigCalled, "UpdateLoadConfig should be called")

		// Verify that only collections with IsUserSpecifiedReplicaMode = false are included
		assert.Equal(t, []int64{1001}, capturedCollectionIDs, "Only collections with IsUserSpecifiedReplicaMode = false should be included")
		assert.Equal(t, int32(2), capturedReplicaNum, "ReplicaNumber should match cluster level config")
		assert.Equal(t, []string{"default"}, capturedRGs, "ResourceGroups should match cluster level config")
	})
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
				CollectionID:   collection,
				ReplicaNumber:  suite.replicaNumber[collection],
				ResourceGroups: []string{meta.DefaultResourceGroupName},
				LoadFields:     []int64{100, 101},
			}
			resp, err := suite.server.LoadCollection(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		} else {
			req := &querypb.LoadPartitionsRequest{
				CollectionID:   collection,
				PartitionIDs:   suite.partitions[collection],
				ReplicaNumber:  suite.replicaNumber[collection],
				ResourceGroups: []string{meta.DefaultResourceGroupName},
				LoadFields:     []int64{100, 101},
			}
			resp, err := suite.server.LoadPartitions(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		}
	}
}

func (suite *ServerSuite) expectGetRecoverInfo(collection int64) {
	vChannels := []*datapb.VchannelInfo{}
	for _, channel := range suite.channels[collection] {
		vChannels = append(vChannels, &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel,
		})
	}

	segmentInfos := []*datapb.SegmentInfo{}
	for _, segments := range suite.segments[collection] {
		for _, segment := range segments {
			segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
				ID:            segment,
				PartitionID:   suite.partitions[collection][0],
				InsertChannel: suite.channels[collection][segment%2],
			})
		}
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collection).Maybe().Return(vChannels, segmentInfos, nil)
}

func (suite *ServerSuite) expectLoadAndReleasePartitions(querynode *mocks.MockQueryNode) {
	querynode.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	querynode.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
}

func (suite *ServerSuite) expectGetRecoverInfoByMockDataCoord(collection int64, dataCoord *coordMocks.MockDataCoordClient) {
	var (
		vChannels    []*datapb.VchannelInfo
		segmentInfos []*datapb.SegmentInfo
	)

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequestV2{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collection,
	}

	vChannels = []*datapb.VchannelInfo{}
	for _, channel := range suite.channels[collection] {
		vChannels = append(vChannels, &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel,
		})
	}

	segmentInfos = []*datapb.SegmentInfo{}
	for _, segments := range suite.segments[collection] {
		for _, segment := range segments {
			segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
				ID:            segment,
				InsertChannel: suite.channels[collection][segment%2],
			})
		}
	}
	dataCoord.EXPECT().GetRecoveryInfoV2(mock.Anything, getRecoveryInfoRequest).Return(&datapb.GetRecoveryInfoResponseV2{
		Status:   merr.Success(),
		Channels: vChannels,
		Segments: segmentInfos,
	}, nil).Maybe()
}

func (suite *ServerSuite) updateCollectionStatus(collectionID int64, status querypb.LoadStatus) {
	collection := suite.server.meta.GetCollection(suite.ctx, collectionID)
	if collection != nil {
		collection := collection.Clone()
		collection.LoadPercentage = 0
		if status == querypb.LoadStatus_Loaded {
			collection.LoadPercentage = 100
		}
		collection.CollectionLoadInfo.Status = status
		suite.server.meta.PutCollection(suite.ctx, collection)

		partitions := suite.server.meta.GetPartitionsByCollection(suite.ctx, collectionID)
		for _, partition := range partitions {
			partition := partition.Clone()
			partition.LoadPercentage = 0
			if status == querypb.LoadStatus_Loaded {
				partition.LoadPercentage = 100
			}
			partition.PartitionLoadInfo.Status = status
			suite.server.meta.PutPartition(suite.ctx, partition)
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
		suite.server.leaderCacheObserver.RegisterEvent,
	)
	suite.server.checkerController = checkers.NewCheckerController(
		suite.server.meta,
		suite.server.dist,
		suite.server.targetMgr,
		suite.server.nodeMgr,
		suite.server.taskScheduler,
		suite.server.broker,
		suite.server.getBalancerFunc,
	)
	suite.server.targetObserver = observers.NewTargetObserver(
		suite.server.meta,
		suite.server.targetMgr,
		suite.server.dist,
		suite.server.broker,
		suite.server.cluster,
		suite.server.nodeMgr,
	)
	suite.server.collectionObserver = observers.NewCollectionObserver(
		suite.server.dist,
		suite.server.meta,
		suite.server.targetMgr,
		suite.server.targetObserver,
		suite.server.checkerController,
		suite.server.proxyClientManager,
	)

	suite.broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{}}, nil).Maybe()
	suite.broker.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{}, nil).Maybe()
	suite.broker.EXPECT().ListIndexes(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
		suite.expectGetRecoverInfo(collection)
	}
	log.Debug("server hacked")
}

func (suite *ServerSuite) hackBroker(server *Server) {
	mockRootCoord := coordMocks.NewMixCoord(suite.T())
	mockRootCoord.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(&milvuspb.ComponentStates{
		State: &milvuspb.ComponentInfo{
			StateCode: commonpb.StateCode_Healthy,
		},
		Status: merr.Success(),
	}, nil).Maybe()

	for _, collection := range suite.collections {
		mockRootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status: merr.Success(),
			Schema: &schemapb.CollectionSchema{},
		}, nil).Maybe()
		req := &milvuspb.ShowPartitionsRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
			),
			CollectionID: collection,
		}
		mockRootCoord.EXPECT().ShowPartitions(mock.Anything, req).Return(&milvuspb.ShowPartitionsResponse{
			Status:       merr.Success(),
			PartitionIDs: suite.partitions[collection],
		}, nil).Maybe()
	}
	server.SetMixCoord(mockRootCoord)
}

func (suite *ServerSuite) newQueryCoord() (*Server, error) {
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
	server.SetTiKVClient(suite.tikvCli)

	server.SetQueryNodeCreator(session.DefaultQueryNodeCreator)
	suite.hackBroker(server)
	err = server.Init()
	return server, err
}

// TestRewatchNodes tests the rewatchNodes function behavior
func TestRewatchNodes(t *testing.T) {
	// Arrange: Create simple server instance
	server := createSimpleTestServer()

	// Create test sessions
	sessions := map[string]*sessionutil.Session{
		"querynode-1001": createTestSession(1001, "localhost:19530", false),
		"querynode-1002": createTestSession(1002, "localhost:19531", false),
		"querynode-1003": createTestSession(1003, "localhost:19532", true), // stopping
	}

	// Pre-add some nodes to node manager to test removal logic
	server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1001,
		Address:  "localhost:19530",
		Hostname: "localhost",
	}))
	server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1004, // This node will be removed as it's not in sessions
		Address:  "localhost:19533",
		Hostname: "localhost",
	}))

	// Mock external calls
	mockHandleNodeUp := mockey.Mock((*Server).handleNodeUp).Return().Build()
	defer mockHandleNodeUp.UnPatch()

	mockHandleNodeDown := mockey.Mock((*Server).handleNodeDown).Return().Build()
	defer mockHandleNodeDown.UnPatch()

	mockHandleNodeStopping := mockey.Mock((*Server).handleNodeStopping).Return().Build()
	defer mockHandleNodeStopping.UnPatch()

	server.meta = &meta.Meta{
		ResourceManager: meta.NewResourceManager(nil, nil),
	}
	mockCheckNodesInResourceGroup := mockey.Mock((*meta.ResourceManager).CheckNodesInResourceGroup).Return().Build()
	defer mockCheckNodesInResourceGroup.UnPatch()

	// Act: Call rewatchNodes
	err := server.rewatchNodes(sessions)

	// Assert: Verify no error occurred
	assert.NoError(t, err)

	// Verify node 1004 was removed
	assert.Nil(t, server.nodeMgr.Get(1004), "Offline node should be removed")

	// Verify nodes 1001, 1002 exist
	assert.NotNil(t, server.nodeMgr.Get(1001), "Online node should exist")
	assert.NotNil(t, server.nodeMgr.Get(1002), "Online node should exist")
	assert.NotNil(t, server.nodeMgr.Get(1003), "Stopping node should exist")
}

// TestRewatchNodesWithEmptySessions tests rewatchNodes with empty sessions
func TestRewatchNodesWithEmptySessions(t *testing.T) {
	// Arrange: Create server with existing nodes
	server := createSimpleTestServer()

	// Add some existing nodes
	server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1001,
		Address:  "localhost:19530",
		Hostname: "localhost",
	}))
	server.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1002,
		Address:  "localhost:19531",
		Hostname: "localhost",
	}))

	// Mock external calls
	mockHandleNodeDown := mockey.Mock((*Server).handleNodeDown).Return().Build()
	defer mockHandleNodeDown.UnPatch()

	server.meta = &meta.Meta{
		ResourceManager: meta.NewResourceManager(nil, nil),
	}
	mockCheckNodesInResourceGroup := mockey.Mock((*meta.ResourceManager).CheckNodesInResourceGroup).Return().Build()
	defer mockCheckNodesInResourceGroup.UnPatch()

	// Act: Call rewatchNodes with empty sessions
	err := server.rewatchNodes(nil)

	// Assert: All nodes should be removed
	assert.NoError(t, err)
	assert.Nil(t, server.nodeMgr.Get(1001), "All nodes should be removed when no sessions exist")
	assert.Nil(t, server.nodeMgr.Get(1002), "All nodes should be removed when no sessions exist")
}

// TestHandleNodeUpWithMissingNode tests handleNodeUp when node doesn't exist
func TestHandleNodeUpWithMissingNode(t *testing.T) {
	// Arrange: Create server without adding the node
	server := createSimpleTestServer()

	nodeID := int64(1001)

	// Act: Handle node up for non-existent node
	server.handleNodeUp(nodeID)

	// Assert: Should handle gracefully (no panic, early return)
	// The function should return early when node is not found
}

// TestHandleNodeDownMetricsCleanup tests that handleNodeDown cleans up metrics properly
func TestHandleNodeDownMetricsCleanup(t *testing.T) {
	// Arrange: Set up metrics with test value
	nodeID := int64(1001)

	// Setup metrics with test value
	registry := prometheus.NewRegistry()
	metrics.RegisterQueryCoord(registry)

	// Set a test metric value
	metrics.QueryCoordLastHeartbeatTimeStamp.WithLabelValues(fmt.Sprint(nodeID)).Set(1640995200.0)

	// Verify metric exists before deletion
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)

	found := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "milvus_querycoord_last_heartbeat_timestamp" {
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "node_id" && label.GetValue() == fmt.Sprint(nodeID) {
						found = true
						break
					}
				}
			}
		}
	}
	assert.True(t, found, "Metric should exist before cleanup")

	// Create a minimal server
	ctx := context.Background()
	server := &Server{
		ctx:                 ctx,
		taskScheduler:       task.NewScheduler(ctx, nil, nil, nil, nil, nil, nil),
		dist:                meta.NewDistributionManager(session.NewNodeManager()),
		distController:      dist.NewDistController(nil, nil, nil, nil, nil, nil),
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		meta: &meta.Meta{
			ResourceManager: meta.NewResourceManager(nil, nil),
		},
	}

	mockRemoveExecutor := mockey.Mock((task.Scheduler).RemoveExecutor).Return().Build()
	defer mockRemoveExecutor.UnPatch()
	mockRemoveByNode := mockey.Mock((task.Scheduler).RemoveByNode).Return().Build()
	defer mockRemoveByNode.UnPatch()
	mockDistControllerRemove := mockey.Mock((*dist.ControllerImpl).Remove).Return().Build()
	defer mockDistControllerRemove.UnPatch()
	mockRemoveFromManager := mockey.Mock(server.dist.ChannelDistManager.Update).Return().Build()
	defer mockRemoveFromManager.UnPatch()
	mockRemoveFromManager = mockey.Mock(server.dist.SegmentDistManager.Update).Return().Build()
	defer mockRemoveFromManager.UnPatch()
	mockInvalidateSystemInfoMetrics := mockey.Mock((*metricsinfo.MetricsCacheManager).InvalidateSystemInfoMetrics).Return().Build()
	defer mockInvalidateSystemInfoMetrics.UnPatch()

	mockResourceManagerHandleNodeDown := mockey.Mock((*meta.ResourceManager).HandleNodeDown).Return().Build()
	defer mockResourceManagerHandleNodeDown.UnPatch()

	// Act: Call handleNodeDown which should clean up metrics
	server.handleNodeDown(nodeID)

	metricFamilies, err = registry.Gather()
	assert.NoError(t, err)

	// Check that the heartbeat metric for this node was deleted
	found = false
	for _, mf := range metricFamilies {
		if mf.GetName() == "milvus_querycoord_last_heartbeat_timestamp" {
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "node_id" && label.GetValue() == fmt.Sprint(nodeID) {
						found = true
						break
					}
				}
			}
		}
	}
	assert.False(t, found, "Metric should be cleaned up after handleNodeDown")
}

// TestNodeManagerStopping tests the node manager stopping functionality
func TestNodeManagerStopping(t *testing.T) {
	// Arrange: Create node manager and add a node
	nodeID := int64(1001)
	nodeMgr := session.NewNodeManager()

	nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   nodeID,
		Address:  "localhost:19530",
		Hostname: "localhost",
	})
	nodeMgr.Add(nodeInfo)

	// Verify node exists and is not stopping initially
	node := nodeMgr.Get(nodeID)
	assert.NotNil(t, node)
	assert.False(t, node.IsStoppingState(), "Node should not be stopping initially")

	// Act: Mark node as stopping
	nodeMgr.Stopping(nodeID)

	// Assert: Node should be in stopping state
	node = nodeMgr.Get(nodeID)
	assert.NotNil(t, node)
	assert.True(t, node.IsStoppingState(), "Node should be in stopping state after calling Stopping()")
}

// Helper function to create a simple test server
func createSimpleTestServer() *Server {
	ctx := context.Background()
	server := &Server{
		ctx:     ctx,
		nodeMgr: session.NewNodeManager(),
	}
	return server
}

// Helper function to create a test session
func createTestSession(nodeID int64, address string, stopping bool) *sessionutil.Session {
	session := &sessionutil.Session{
		SessionRaw: sessionutil.SessionRaw{
			ServerID: nodeID,
			Address:  address,
			Stopping: stopping,
			HostName: "localhost",
		},
	}
	return session
}

func TestServer(t *testing.T) {
	parameters := []string{"tikv", "etcd"}
	for _, v := range parameters {
		testMeta = v
		suite.Run(t, new(ServerSuite))
	}
}
