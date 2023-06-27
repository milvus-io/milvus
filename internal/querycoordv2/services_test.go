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
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/dist"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ServiceSuite struct {
	suite.Suite

	// Data
	collections   []int64
	partitions    map[int64][]int64
	channels      map[int64][]string
	segments      map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	loadTypes     map[int64]querypb.LoadType
	replicaNumber map[int64]int32
	nodes         []int64

	// Dependencies
	kv             kv.MetaKv
	store          meta.Store
	dist           *meta.DistributionManager
	meta           *meta.Meta
	targetMgr      *meta.TargetManager
	broker         *meta.MockBroker
	targetObserver *observers.TargetObserver
	cluster        *session.MockCluster
	nodeMgr        *session.NodeManager
	jobScheduler   *job.Scheduler
	taskScheduler  *task.MockScheduler
	balancer       balance.Balance

	distMgr        *meta.DistributionManager
	distController *dist.MockController

	// Test object
	server *Server
}

func (suite *ServiceSuite) SetupSuite() {
	paramtable.Init()

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
	suite.nodes = []int64{1, 2, 3, 4, 5,
		101, 102, 103, 104, 105}
}

func (suite *ServiceSuite) SetupTest() {
	config := params.GenerateEtcdConfig()
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

	suite.store = meta.NewMetaStore(suite.kv)
	suite.dist = meta.NewDistributionManager()
	suite.nodeMgr = session.NewNodeManager()
	suite.meta = meta.NewMeta(params.RandomIncrementIDAllocator(), suite.store, suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.targetObserver = observers.NewTargetObserver(
		suite.meta,
		suite.targetMgr,
		suite.dist,
		suite.broker,
	)
	suite.targetObserver.Start(context.Background())
	for _, node := range suite.nodes {
		suite.nodeMgr.Add(session.NewNodeInfo(node, "localhost"))
		err := suite.meta.ResourceManager.AssignNode(meta.DefaultResourceGroupName, node)
		suite.NoError(err)
	}
	suite.cluster = session.NewMockCluster(suite.T())
	suite.jobScheduler = job.NewScheduler()
	suite.taskScheduler = task.NewMockScheduler(suite.T())
	suite.jobScheduler.Start(context.Background())
	suite.balancer = balance.NewRowCountBasedBalancer(
		suite.taskScheduler,
		suite.nodeMgr,
		suite.dist,
		suite.meta,
		suite.targetMgr,
	)
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	suite.distMgr = meta.NewDistributionManager()
	suite.distController = dist.NewMockController(suite.T())

	suite.server = &Server{
		kv:                  suite.kv,
		store:               suite.store,
		session:             sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath.GetValue(), cli),
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		dist:                suite.dist,
		meta:                suite.meta,
		targetMgr:           suite.targetMgr,
		broker:              suite.broker,
		targetObserver:      suite.targetObserver,
		nodeMgr:             suite.nodeMgr,
		cluster:             suite.cluster,
		jobScheduler:        suite.jobScheduler,
		taskScheduler:       suite.taskScheduler,
		balancer:            suite.balancer,
		distController:      suite.distController,
		ctx:                 context.Background(),
	}
	suite.server.collectionObserver = observers.NewCollectionObserver(
		suite.server.dist,
		suite.server.meta,
		suite.server.targetMgr,
		suite.targetObserver,
		suite.server.leaderObserver,
		&checkers.CheckerController{},
	)

	suite.server.UpdateStateCode(commonpb.StateCode_Healthy)
}

func (suite *ServiceSuite) TestShowCollections() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server
	collectionNum := len(suite.collections)

	// Test get all collections
	req := &querypb.ShowCollectionsRequest{}
	resp, err := server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	suite.Len(resp.CollectionIDs, collectionNum)
	for _, collection := range suite.collections {
		suite.Contains(resp.CollectionIDs, collection)
	}

	// Test get 1 collection
	collection := suite.collections[0]
	req.CollectionIDs = []int64{collection}
	resp, err = server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	suite.Len(resp.CollectionIDs, 1)
	suite.Equal(collection, resp.CollectionIDs[0])

	// Test insufficient memory
	colBak := suite.meta.CollectionManager.GetCollection(collection)
	err = suite.meta.CollectionManager.RemoveCollection(collection)
	suite.NoError(err)
	meta.GlobalFailedLoadCache.Put(collection, merr.WrapErrServiceMemoryLimitExceeded(100, 10))
	resp, err = server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_InsufficientMemoryToLoad, resp.GetStatus().GetErrorCode())
	meta.GlobalFailedLoadCache.Remove(collection)
	err = suite.meta.CollectionManager.PutCollection(colBak)
	suite.NoError(err)

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	resp, err = server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestShowPartitions() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	for _, collection := range suite.collections {
		partitions := suite.partitions[collection]
		partitionNum := len(partitions)

		// Test get all partitions
		req := &querypb.ShowPartitionsRequest{
			CollectionID: collection,
		}
		resp, err := server.ShowPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.Len(resp.PartitionIDs, partitionNum)
		for _, partition := range partitions {
			suite.Contains(resp.PartitionIDs, partition)
		}

		// Test get 1 partition
		req = &querypb.ShowPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: partitions[0:1],
		}
		resp, err = server.ShowPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.Len(resp.PartitionIDs, 1)
		for _, partition := range partitions[0:1] {
			suite.Contains(resp.PartitionIDs, partition)
		}

		// Test insufficient memory
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			colBak := suite.meta.CollectionManager.GetCollection(collection)
			err = suite.meta.CollectionManager.RemoveCollection(collection)
			suite.NoError(err)
			meta.GlobalFailedLoadCache.Put(collection, merr.WrapErrServiceMemoryLimitExceeded(100, 10))
			resp, err = server.ShowPartitions(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_InsufficientMemoryToLoad, resp.GetStatus().GetErrorCode())
			meta.GlobalFailedLoadCache.Remove(collection)
			err = suite.meta.CollectionManager.PutCollection(colBak)
			suite.NoError(err)
		} else {
			partitionID := partitions[0]
			parBak := suite.meta.CollectionManager.GetPartition(partitionID)
			err = suite.meta.CollectionManager.RemovePartition(partitionID)
			suite.NoError(err)
			meta.GlobalFailedLoadCache.Put(collection, merr.WrapErrServiceMemoryLimitExceeded(100, 10))
			resp, err = server.ShowPartitions(ctx, req)
			suite.NoError(err)
			suite.Equal(commonpb.ErrorCode_InsufficientMemoryToLoad, resp.GetStatus().GetErrorCode())
			meta.GlobalFailedLoadCache.Remove(collection)
			err = suite.meta.CollectionManager.PutPartition(parBak)
			suite.NoError(err)
		}
	}

	// Test when server is not healthy
	req := &querypb.ShowPartitionsRequest{
		CollectionID: suite.collections[0],
	}
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	resp, err := server.ShowPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestLoadCollection() {
	ctx := context.Background()
	server := suite.server

	// Test load all collections
	for _, collection := range suite.collections {
		suite.expectGetRecoverInfo(collection)
		suite.expectLoadPartitions()

		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		resp, err := server.LoadCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.assertLoaded(collection)
	}

	// Test load again
	for _, collection := range suite.collections {
		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		resp, err := server.LoadCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.LoadCollectionRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.LoadCollection(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestResourceGroup() {
	ctx := context.Background()
	server := suite.server

	createRG := &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: "rg1",
	}

	resp, err := server.CreateResourceGroup(ctx, createRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)

	resp, err = server.CreateResourceGroup(ctx, createRG)
	suite.NoError(err)
	suite.False(merr.Ok(resp))

	listRG := &milvuspb.ListResourceGroupsRequest{}
	resp1, err := server.ListResourceGroups(ctx, listRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp1.GetStatus().GetErrorCode())
	suite.Len(resp1.ResourceGroups, 2)

	server.nodeMgr.Add(session.NewNodeInfo(1011, "localhost"))
	server.nodeMgr.Add(session.NewNodeInfo(1012, "localhost"))
	server.nodeMgr.Add(session.NewNodeInfo(1013, "localhost"))
	server.nodeMgr.Add(session.NewNodeInfo(1014, "localhost"))
	server.meta.ResourceManager.AddResourceGroup("rg11")
	server.meta.ResourceManager.AssignNode("rg11", 1011)
	server.meta.ResourceManager.AssignNode("rg11", 1012)
	server.meta.ResourceManager.AddResourceGroup("rg12")
	server.meta.ResourceManager.AssignNode("rg12", 1013)
	server.meta.ResourceManager.AssignNode("rg12", 1014)
	server.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	server.meta.CollectionManager.PutCollection(utils.CreateTestCollection(2, 1))
	server.meta.ReplicaManager.Put(meta.NewReplica(&querypb.Replica{
		ID:            1,
		CollectionID:  1,
		Nodes:         []int64{1011, 1013},
		ResourceGroup: "rg11"},
		typeutil.NewUniqueSet(1011, 1013)),
	)
	server.meta.ReplicaManager.Put(meta.NewReplica(&querypb.Replica{
		ID:            2,
		CollectionID:  2,
		Nodes:         []int64{1012, 1014},
		ResourceGroup: "rg12"},
		typeutil.NewUniqueSet(1012, 1014)),
	)

	describeRG := &querypb.DescribeResourceGroupRequest{
		ResourceGroup: "rg11",
	}
	resp2, err := server.DescribeResourceGroup(ctx, describeRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp2.GetStatus().GetErrorCode())
	suite.Equal("rg11", resp2.GetResourceGroup().GetName())
	suite.Equal(int32(2), resp2.GetResourceGroup().GetCapacity())
	suite.Equal(int32(2), resp2.GetResourceGroup().GetNumAvailableNode())
	suite.Equal(map[int64]int32{1: 1}, resp2.GetResourceGroup().GetNumLoadedReplica())
	suite.Equal(map[int64]int32{2: 1}, resp2.GetResourceGroup().GetNumIncomingNode())
	suite.Equal(map[int64]int32{1: 1}, resp2.GetResourceGroup().GetNumOutgoingNode())

	dropRG := &milvuspb.DropResourceGroupRequest{
		ResourceGroup: "rg1",
	}

	resp3, err := server.DropResourceGroup(ctx, dropRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp3.ErrorCode)

	resp4, err := server.ListResourceGroups(ctx, listRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp4.GetStatus().GetErrorCode())
	suite.Len(resp4.GetResourceGroups(), 3)
}

func (suite *ServiceSuite) TestResourceGroupFailed() {
	ctx := context.Background()
	server := suite.server

	// illegal argument
	describeRG := &querypb.DescribeResourceGroupRequest{
		ResourceGroup: "rfffff",
	}
	resp, err := server.DescribeResourceGroup(ctx, describeRG)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.GetStatus().GetErrorCode())

	// server unhealthy
	server.UpdateStateCode(commonpb.StateCode_Abnormal)

	createRG := &milvuspb.CreateResourceGroupRequest{
		ResourceGroup: "rg1",
	}

	resp1, err := server.CreateResourceGroup(ctx, createRG)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp1), merr.ErrServiceNotReady)

	listRG := &milvuspb.ListResourceGroupsRequest{}
	resp2, err := server.ListResourceGroups(ctx, listRG)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp2.GetStatus()), merr.ErrServiceNotReady)

	describeRG = &querypb.DescribeResourceGroupRequest{
		ResourceGroup: "rg1",
	}
	resp3, err := server.DescribeResourceGroup(ctx, describeRG)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp3.GetStatus()), merr.ErrServiceNotReady)

	dropRG := &milvuspb.DropResourceGroupRequest{
		ResourceGroup: "rg1",
	}
	resp4, err := server.DropResourceGroup(ctx, dropRG)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp4), merr.ErrServiceNotReady)

	resp5, err := server.ListResourceGroups(ctx, listRG)
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp5.GetStatus()), merr.ErrServiceNotReady)
}

func (suite *ServiceSuite) TestTransferNode() {
	ctx := context.Background()
	server := suite.server

	err := server.meta.ResourceManager.AddResourceGroup("rg1")
	suite.NoError(err)
	err = server.meta.ResourceManager.AddResourceGroup("rg2")
	suite.NoError(err)
	suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 2))
	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg1",
		},
		typeutil.NewUniqueSet(),
	))
	// test transfer node
	resp, err := server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg1",
		NumNode:             1,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	nodes, err := server.meta.ResourceManager.GetNodes("rg1")
	suite.NoError(err)
	suite.Len(nodes, 1)
	nodesInReplica := server.meta.ReplicaManager.Get(1).GetNodes()
	suite.Len(nodesInReplica, 1)

	suite.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            2,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: "rg2",
		},
		typeutil.NewUniqueSet(),
	))
	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: "rg1",
		TargetResourceGroup: "rg2",
		NumNode:             1,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
	suite.Contains(resp.Reason, "can't transfer node")

	// test transfer node meet non-exist source rg
	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: "rgggg",
		TargetResourceGroup: meta.DefaultResourceGroupName,
	})
	suite.NoError(err)
	suite.Contains(resp.Reason, meta.ErrRGNotExist.Error())
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)

	// test transfer node meet non-exist target rg
	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rgggg",
	})
	suite.NoError(err)
	suite.Contains(resp.Reason, meta.ErrRGNotExist.Error())
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)

	err = server.meta.ResourceManager.AddResourceGroup("rg3")
	suite.NoError(err)
	err = server.meta.ResourceManager.AddResourceGroup("rg4")
	suite.NoError(err)
	suite.nodeMgr.Add(session.NewNodeInfo(11, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(12, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(13, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(14, "localhost"))
	suite.meta.ResourceManager.AssignNode("rg3", 11)
	suite.meta.ResourceManager.AssignNode("rg3", 12)
	suite.meta.ResourceManager.AssignNode("rg3", 13)
	suite.meta.ResourceManager.AssignNode("rg3", 14)

	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: "rg3",
		TargetResourceGroup: "rg4",
		NumNode:             3,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	nodes, err = server.meta.ResourceManager.GetNodes("rg3")
	suite.NoError(err)
	suite.Len(nodes, 1)
	nodes, err = server.meta.ResourceManager.GetNodes("rg4")
	suite.NoError(err)
	suite.Len(nodes, 3)
	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: "rg3",
		TargetResourceGroup: "rg4",
		NumNode:             3,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)

	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg1",
		NumNode:             -1,
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)

	// server unhealthy
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = server.TransferNode(ctx, &milvuspb.TransferNodeRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg1",
		NumNode:             3,
	})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp), merr.ErrServiceNotReady)
}

func (suite *ServiceSuite) TestTransferReplica() {
	ctx := context.Background()
	server := suite.server

	err := server.meta.ResourceManager.AddResourceGroup("rg1")
	suite.NoError(err)
	err = server.meta.ResourceManager.AddResourceGroup("rg2")
	suite.NoError(err)
	err = server.meta.ResourceManager.AddResourceGroup("rg3")
	suite.NoError(err)

	resp, err := suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: "rg2",
		TargetResourceGroup: "rg1",
		CollectionID:        1,
		NumReplica:          2,
	})
	suite.NoError(err)
	suite.Contains(resp.Reason, "only found [0] replicas in source resource group")

	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: "rgg",
		TargetResourceGroup: meta.DefaultResourceGroupName,
		CollectionID:        1,
		NumReplica:          2,
	})
	suite.NoError(err)
	suite.Equal(resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)

	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rgg",
		CollectionID:        1,
		NumReplica:          2,
	})
	suite.NoError(err)
	suite.Equal(resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)

	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg1",
		CollectionID:        1,
		NumReplica:          0,
	})
	suite.NoError(err)
	suite.Equal(resp.ErrorCode, commonpb.ErrorCode_IllegalArgument)

	suite.server.meta.Put(meta.NewReplica(&querypb.Replica{
		CollectionID:  1,
		ID:            111,
		ResourceGroup: meta.DefaultResourceGroupName,
	}, typeutil.NewUniqueSet(1)))
	suite.server.meta.Put(meta.NewReplica(&querypb.Replica{
		CollectionID:  1,
		ID:            222,
		ResourceGroup: meta.DefaultResourceGroupName,
	}, typeutil.NewUniqueSet(2)))
	suite.server.meta.Put(meta.NewReplica(&querypb.Replica{
		CollectionID:  1,
		ID:            333,
		ResourceGroup: meta.DefaultResourceGroupName,
	}, typeutil.NewUniqueSet(3)))

	suite.server.nodeMgr.Add(session.NewNodeInfo(1001, "localhost"))
	suite.server.nodeMgr.Add(session.NewNodeInfo(1002, "localhost"))
	suite.server.nodeMgr.Add(session.NewNodeInfo(1003, "localhost"))
	suite.server.nodeMgr.Add(session.NewNodeInfo(1004, "localhost"))
	suite.server.nodeMgr.Add(session.NewNodeInfo(1005, "localhost"))
	suite.server.meta.AssignNode("rg1", 1001)
	suite.server.meta.AssignNode("rg2", 1002)
	suite.server.meta.AssignNode("rg3", 1003)
	suite.server.meta.AssignNode("rg3", 1004)
	suite.server.meta.AssignNode("rg3", 1005)

	suite.server.meta.Put(meta.NewReplica(&querypb.Replica{
		CollectionID:  2,
		ID:            444,
		ResourceGroup: meta.DefaultResourceGroupName,
	}, typeutil.NewUniqueSet(3)))
	suite.server.meta.Put(meta.NewReplica(&querypb.Replica{
		CollectionID:  2,
		ID:            555,
		ResourceGroup: "rg2",
	}, typeutil.NewUniqueSet(4)))
	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg2",
		CollectionID:        2,
		NumReplica:          1,
	})
	suite.NoError(err)
	suite.Contains(resp.Reason, "dynamically increase replica num is unsupported")

	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg1",
		CollectionID:        1,
		NumReplica:          1,
	})
	suite.NoError(err)
	suite.Contains(resp.Reason, "transfer replica will cause replica loaded in both default rg and other rg")

	replicaNum := len(suite.server.meta.ReplicaManager.GetByCollection(1))
	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg3",
		CollectionID:        1,
		NumReplica:          int64(replicaNum),
	})
	suite.NoError(err)
	suite.Equal(resp.ErrorCode, commonpb.ErrorCode_Success)
	suite.Len(suite.server.meta.GetByResourceGroup("rg3"), 3)

	// server unhealthy
	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = suite.server.TransferReplica(ctx, &querypb.TransferReplicaRequest{
		SourceResourceGroup: meta.DefaultResourceGroupName,
		TargetResourceGroup: "rg3",
		CollectionID:        1,
		NumReplica:          2,
	})
	suite.NoError(err)
	suite.ErrorIs(merr.Error(resp), merr.ErrServiceNotReady)
}

func (suite *ServiceSuite) TestLoadCollectionFailed() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test load with different replica number
	for _, collection := range suite.collections {
		req := &querypb.LoadCollectionRequest{
			CollectionID:  collection,
			ReplicaNumber: suite.replicaNumber[collection] + 1,
		}
		resp, err := server.LoadCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, job.ErrLoadParameterMismatched.Error())
	}

	req := &querypb.LoadCollectionRequest{
		CollectionID:   0,
		ReplicaNumber:  2,
		ResourceGroups: []string{meta.DefaultResourceGroupName, "rg"},
	}
	resp, err := server.LoadCollection(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)

	// Test load with partitions loaded
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}

		req := &querypb.LoadCollectionRequest{
			CollectionID: collection,
		}
		resp, err := server.LoadCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, job.ErrLoadParameterMismatched.Error())
	}

	// Test load with wrong rg num
	for _, collection := range suite.collections {
		req := &querypb.LoadCollectionRequest{
			CollectionID:   collection,
			ReplicaNumber:  suite.replicaNumber[collection] + 1,
			ResourceGroups: []string{"rg1", "rg2"},
		}
		resp, err := server.LoadCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, ErrLoadUseWrongRG.Error())
	}
}

func (suite *ServiceSuite) TestLoadPartition() {
	ctx := context.Background()
	server := suite.server

	// Test load all partitions
	for _, collection := range suite.collections {
		suite.expectLoadPartitions()
		suite.expectGetRecoverInfo(collection)

		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := server.LoadPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.assertLoaded(collection)
	}

	// Test load again
	for _, collection := range suite.collections {
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := server.LoadPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	req := &querypb.LoadPartitionsRequest{
		CollectionID:   suite.collections[0],
		PartitionIDs:   suite.partitions[suite.collections[0]],
		ResourceGroups: []string{meta.DefaultResourceGroupName, "rg"},
	}
	resp, err := server.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)

	// Test load with collection loaded
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadCollection {
			continue
		}
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := server.LoadPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	// Test load with more partitions
	for _, collection := range suite.collections {
		if suite.loadTypes[collection] != querypb.LoadType_LoadPartition {
			continue
		}
		req := &querypb.LoadPartitionsRequest{
			CollectionID: collection,
			PartitionIDs: append(suite.partitions[collection], 999),
		}
		resp, err := server.LoadPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req = &querypb.LoadPartitionsRequest{
		CollectionID: suite.collections[0],
		PartitionIDs: suite.partitions[suite.collections[0]],
	}
	resp, err = server.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestLoadPartitionFailed() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test load with different replica number
	for _, collection := range suite.collections {
		req := &querypb.LoadPartitionsRequest{
			CollectionID:  collection,
			PartitionIDs:  suite.partitions[collection],
			ReplicaNumber: suite.replicaNumber[collection] + 1,
		}
		resp, err := server.LoadPartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, job.ErrLoadParameterMismatched.Error())
	}
}

func (suite *ServiceSuite) TestReleaseCollection() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	suite.cluster.EXPECT().ReleasePartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	// Test release all collections
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		resp, err := server.ReleaseCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.assertReleased(collection)
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleaseCollectionRequest{
			CollectionID: collection,
		}
		resp, err := server.ReleaseCollection(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.ReleaseCollectionRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.ReleaseCollection(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestReleasePartition() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test release all partitions
	suite.cluster.EXPECT().ReleasePartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][0:1],
		}
		resp, err := server.ReleasePartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.assertPartitionLoaded(collection, suite.partitions[collection][1:]...)
	}

	// Test release again
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][0:1],
		}
		resp, err := server.ReleasePartitions(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.assertPartitionLoaded(collection, suite.partitions[collection][1:]...)
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.ReleasePartitionsRequest{
		CollectionID: suite.collections[0],
		PartitionIDs: suite.partitions[suite.collections[0]][0:1],
	}
	resp, err := server.ReleasePartitions(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestRefreshCollection() {
	server := suite.server

	server.collectionObserver.Start(context.Background())

	// Test refresh all collections.
	for _, collection := range suite.collections {
		err := server.refreshCollection(collection)
		// Collection not loaded error.
		suite.ErrorIs(err, merr.ErrCollectionNotLoaded)
	}

	// Test load all collections
	suite.loadAll()

	// Test refresh all collections again when collections are loaded. This time should fail with collection not 100% loaded.
	for _, collection := range suite.collections {
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loading)
		err := server.refreshCollection(collection)
		suite.ErrorIs(err, merr.ErrCollectionNotLoaded)
	}

	// Test refresh all collections
	for _, id := range suite.collections {
		// Load and explicitly mark load percentage to 100%.
		suite.updateChannelDist(id)
		suite.updateSegmentDist(id, suite.nodes[0])
		suite.updateCollectionStatus(id, querypb.LoadStatus_Loaded)

		err := server.refreshCollection(id)
		suite.NoError(err)

		readyCh, err := server.targetObserver.UpdateNextTarget(id)
		suite.NoError(err)
		<-readyCh

		// Now the refresh must be done
		collection := server.meta.CollectionManager.GetCollection(id)
		suite.True(collection.IsRefreshed())
	}

	// Test refresh not ready
	for _, id := range suite.collections {
		suite.updateChannelDistWithoutSegment(id)
		err := server.refreshCollection(id)
		suite.NoError(err)

		// Now the refresh must be not done
		collection := server.meta.CollectionManager.GetCollection(id)
		suite.False(collection.IsRefreshed())
	}
}

func (suite *ServiceSuite) TestGetPartitionStates() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test get partitions' state
	for _, collection := range suite.collections {
		req := &querypb.GetPartitionStatesRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection],
		}
		resp, err := server.GetPartitionStates(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.Len(resp.PartitionDescriptions, len(suite.partitions[collection]))
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.GetPartitionStatesRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetPartitionStates(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestGetSegmentInfo() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test get all segments
	for i, collection := range suite.collections {
		suite.updateSegmentDist(collection, int64(i))
		req := &querypb.GetSegmentInfoRequest{
			CollectionID: collection,
		}
		resp, err := server.GetSegmentInfo(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.assertSegments(collection, resp.GetInfos())
	}

	// Test get given segments
	for _, collection := range suite.collections {
		req := &querypb.GetSegmentInfoRequest{
			CollectionID: collection,
			SegmentIDs:   suite.getAllSegments(collection),
		}
		resp, err := server.GetSegmentInfo(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.assertSegments(collection, resp.GetInfos())
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.GetSegmentInfoRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetSegmentInfo(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestLoadBalance() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test get balance first segment
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		nodes := replicas[0].GetNodes()
		srcNode := nodes[0]
		dstNode := nodes[1]
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateSegmentDist(collection, srcNode)
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			SourceNodeIDs:    []int64{srcNode},
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		suite.taskScheduler.ExpectedCalls = make([]*mock.Call, 0)
		suite.taskScheduler.EXPECT().Add(mock.Anything).Run(func(task task.Task) {
			actions := task.Actions()
			suite.Len(actions, 2)
			growAction, reduceAction := actions[0], actions[1]
			suite.Equal(dstNode, growAction.Node())
			suite.Equal(srcNode, reduceAction.Node())
			task.Cancel(nil)
		}).Return(nil)
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.taskScheduler.AssertExpectations(suite.T())
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.LoadBalanceRequest{
		CollectionID:  suite.collections[0],
		SourceNodeIDs: []int64{1},
		DstNodeIDs:    []int64{100 + 1},
	}
	resp, err := server.LoadBalance(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestLoadBalanceWithEmptySegmentList() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	srcNode := int64(1001)
	dstNode := int64(1002)
	metaSegments := make([]*meta.Segment, 0)
	segmentOnCollection := make(map[int64][]int64)

	// update two collection's dist
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		replicas[0].AddNode(srcNode)
		replicas[0].AddNode(dstNode)
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)

		for partition, segments := range suite.segments[collection] {
			for _, segment := range segments {
				metaSegments = append(metaSegments,
					utils.CreateTestSegment(collection, partition, segment, srcNode, 1, "test-channel"))

				segmentOnCollection[collection] = append(segmentOnCollection[collection], segment)
			}
		}
	}
	suite.nodeMgr.Add(session.NewNodeInfo(1001, "localhost"))
	suite.nodeMgr.Add(session.NewNodeInfo(1002, "localhost"))
	defer func() {
		for _, collection := range suite.collections {
			replicas := suite.meta.ReplicaManager.GetByCollection(collection)
			replicas[0].RemoveNode(srcNode)
			replicas[0].RemoveNode(dstNode)
		}
		suite.nodeMgr.Remove(1001)
		suite.nodeMgr.Remove(1002)
	}()
	suite.dist.SegmentDistManager.Update(srcNode, metaSegments...)

	// expect each collection can only trigger its own segment's balance
	for _, collection := range suite.collections {
		req := &querypb.LoadBalanceRequest{
			CollectionID:  collection,
			SourceNodeIDs: []int64{srcNode},
			DstNodeIDs:    []int64{dstNode},
		}
		suite.taskScheduler.ExpectedCalls = make([]*mock.Call, 0)
		suite.taskScheduler.EXPECT().Add(mock.Anything).Run(func(t task.Task) {
			actions := t.Actions()
			suite.Len(actions, 2)
			growAction := actions[0].(*task.SegmentAction)
			reduceAction := actions[1].(*task.SegmentAction)
			suite.True(lo.Contains(segmentOnCollection[collection], growAction.SegmentID()))
			suite.True(lo.Contains(segmentOnCollection[collection], reduceAction.SegmentID()))
			suite.Equal(dstNode, growAction.Node())
			suite.Equal(srcNode, reduceAction.Node())
			t.Cancel(nil)
		}).Return(nil)
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		suite.taskScheduler.AssertExpectations(suite.T())
	}
}

func (suite *ServiceSuite) TestLoadBalanceFailed() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test load balance without source node
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		dstNode := replicas[0].GetNodes()[1]
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.Contains(resp.Reason, "source nodes can only contain 1 node")
	}

	// Test load balance with not fully loaded
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		nodes := replicas[0].GetNodes()
		srcNode := nodes[0]
		dstNode := nodes[1]
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loading)
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			SourceNodeIDs:    []int64{srcNode},
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.Contains(resp.Reason, "can't balance segments of not fully loaded collection")
	}

	// Test load balance with source node and dest node not in the same replica
	for _, collection := range suite.collections {
		if suite.replicaNumber[collection] <= 1 {
			continue
		}

		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		srcNode := replicas[0].GetNodes()[0]
		dstNode := replicas[1].GetNodes()[0]
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateSegmentDist(collection, srcNode)
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			SourceNodeIDs:    []int64{srcNode},
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.Contains(resp.Reason, "destination nodes have to be in the same replica of source node")
	}

	// Test balance task failed
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		nodes := replicas[0].GetNodes()
		srcNode := nodes[0]
		dstNode := nodes[1]
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateSegmentDist(collection, srcNode)
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			SourceNodeIDs:    []int64{srcNode},
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		suite.taskScheduler.EXPECT().Add(mock.Anything).Run(func(balanceTask task.Task) {
			balanceTask.Cancel(errors.New("mock error"))
		}).Return(nil)
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.Contains(resp.Reason, "failed to balance segments")
		suite.Contains(resp.Reason, "mock error")

		suite.meta.ReplicaManager.AddNode(replicas[0].ID, 10)
		req.SourceNodeIDs = []int64{10}
		resp, err = server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)

		req.SourceNodeIDs = []int64{srcNode}
		req.DstNodeIDs = []int64{10}
		resp, err = server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)

		suite.nodeMgr.Add(session.NewNodeInfo(10, "localhost"))
		suite.nodeMgr.Stopping(10)
		resp, err = server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.nodeMgr.Remove(10)
		suite.meta.ReplicaManager.RemoveNode(replicas[0].ID, 10)
	}
}

func (suite *ServiceSuite) TestShowConfigurations() {
	ctx := context.Background()
	server := suite.server

	req := &internalpb.ShowConfigurationsRequest{
		Pattern: "querycoord.Port",
	}
	resp, err := server.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	suite.Len(resp.Configuations, 1)
	suite.Equal("querycoord.port", resp.Configuations[0].Key)

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req = &internalpb.ShowConfigurationsRequest{
		Pattern: "querycoord.Port",
	}
	resp, err = server.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestGetMetrics() {
	ctx := context.Background()
	server := suite.server

	for _, node := range suite.nodes {
		suite.cluster.EXPECT().GetMetrics(ctx, node, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:        merr.Status(nil),
			ComponentName: "QueryNode",
		}, nil)
	}

	metricReq := make(map[string]string)
	metricReq[metricsinfo.MetricTypeKey] = "system_info"
	req, err := json.Marshal(metricReq)
	suite.NoError(err)
	resp, err := server.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: string(req),
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	resp, err = server.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: string(req),
	})
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestGetReplicas() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	for _, collection := range suite.collections {
		suite.updateChannelDist(collection)
		req := &milvuspb.GetReplicasRequest{
			CollectionID: collection,
		}
		resp, err := server.GetReplicas(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.EqualValues(suite.replicaNumber[collection], len(resp.Replicas))
	}

	// Test get with shard nodes
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		for _, replica := range replicas {
			suite.updateSegmentDist(collection, replica.GetNodes()[0])
		}
		suite.updateChannelDist(collection)
		req := &milvuspb.GetReplicasRequest{
			CollectionID:   collection,
			WithShardNodes: true,
		}
		resp, err := server.GetReplicas(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.EqualValues(suite.replicaNumber[collection], len(resp.Replicas))

		// Test no dup nodeIDs in shardReplica
		for _, replica := range resp.GetReplicas() {
			suite.Equal(collection, replica.CollectionID)
			for _, shardReplica := range replica.GetShardReplicas() {
				gotNodeIDsSet := typeutil.NewUniqueSet(shardReplica.GetNodeIds()...)
				suite.Equal(len(shardReplica.GetNodeIds()), gotNodeIDsSet.Len())
				if len(resp.GetReplicas()) > 0 {
					suite.True(gotNodeIDsSet.Len() < len(replica.GetNodeIds()))
				}
			}

			suite.Equal(len(replica.GetNodeIds()), len(suite.meta.ReplicaManager.Get(replica.ReplicaID).GetNodes()))
		}
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &milvuspb.GetReplicasRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetReplicas(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestGetReplicasFailed() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	replicas := suite.meta.ReplicaManager.GetByCollection(suite.collections[0])
	for _, replica := range replicas {
		suite.updateSegmentDist(suite.collections[0], replica.GetNodes()[0])
	}
	suite.updateChannelDist(suite.collections[0])

	suite.meta.ReplicaManager.Put(utils.CreateTestReplica(100001, suite.collections[0], []int64{}))

	req := &milvuspb.GetReplicasRequest{
		CollectionID:   suite.collections[0],
		WithShardNodes: true,
	}
	resp, err := server.GetReplicas(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_MetaFailed, resp.GetStatus().GetErrorCode())
	suite.EqualValues(resp.GetStatus().GetReason(), "failed to get replica info, err=replica=100001: no available node in replica")
}

func (suite *ServiceSuite) TestCheckHealth() {
	ctx := context.Background()
	server := suite.server

	// Test for server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	resp, err := server.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	suite.NoError(err)
	suite.Equal(resp.IsHealthy, false)
	suite.NotEmpty(resp.Reasons)

	// Test for components state fail
	for _, node := range suite.nodes {
		suite.cluster.EXPECT().GetComponentStates(mock.Anything, node).Return(
			&milvuspb.ComponentStates{
				State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Abnormal},
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			},
			nil).Once()
	}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	resp, err = server.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	suite.NoError(err)
	suite.Equal(resp.IsHealthy, false)
	suite.NotEmpty(resp.Reasons)

	// Test for server is healthy
	for _, node := range suite.nodes {
		suite.cluster.EXPECT().GetComponentStates(mock.Anything, node).Return(
			&milvuspb.ComponentStates{
				State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			},
			nil).Once()
	}
	resp, err = server.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	suite.NoError(err)
	suite.Equal(resp.IsHealthy, true)
	suite.Empty(resp.Reasons)
}

func (suite *ServiceSuite) TestGetShardLeaders() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	for _, collection := range suite.collections {
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateChannelDist(collection)
		req := &querypb.GetShardLeadersRequest{
			CollectionID: collection,
		}

		suite.fetchHeartbeats(time.Now())
		resp, err := server.GetShardLeaders(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
		suite.Len(resp.Shards, len(suite.channels[collection]))
		for _, shard := range resp.Shards {
			suite.Len(shard.NodeIds, int(suite.replicaNumber[collection]))
		}
	}

	// Test when server is not healthy
	server.UpdateStateCode(commonpb.StateCode_Initializing)
	req := &querypb.GetShardLeadersRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetShardLeaders(ctx, req)
	suite.NoError(err)
	suite.Equal(resp.GetStatus().GetCode(), merr.Code(merr.ErrServiceNotReady))
}

func (suite *ServiceSuite) TestGetShardLeadersFailed() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	for _, collection := range suite.collections {
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateChannelDist(collection)
		req := &querypb.GetShardLeadersRequest{
			CollectionID: collection,
		}

		// Node offline
		suite.fetchHeartbeats(time.Now())
		for _, node := range suite.nodes {
			suite.nodeMgr.Remove(node)
		}
		resp, err := server.GetShardLeaders(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_NoReplicaAvailable, resp.GetStatus().GetErrorCode())
		for _, node := range suite.nodes {
			suite.nodeMgr.Add(session.NewNodeInfo(node, "localhost"))
		}

		// Last heartbeat response time too old
		suite.fetchHeartbeats(time.Now().Add(-Params.QueryCoordCfg.HeartbeatAvailableInterval.GetAsDuration(time.Millisecond) - 1))
		resp, err = server.GetShardLeaders(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_NoReplicaAvailable, resp.GetStatus().GetErrorCode())

		// Segment not fully loaded
		for _, node := range suite.nodes {
			suite.dist.SegmentDistManager.Update(node)
			suite.dist.ChannelDistManager.Update(node)
			suite.dist.LeaderViewManager.Update(node)
		}
		suite.updateChannelDistWithoutSegment(collection)
		suite.fetchHeartbeats(time.Now())
		resp, err = server.GetShardLeaders(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_NoReplicaAvailable, resp.GetStatus().GetErrorCode())
	}

	// collection not loaded
	req := &querypb.GetShardLeadersRequest{
		CollectionID: -1,
	}
	resp, err := server.GetShardLeaders(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.True(errors.Is(merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded))
}

func (suite *ServiceSuite) TestHandleNodeUp() {
	server := suite.server
	suite.server.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	suite.server.meta.ReplicaManager.Put(meta.NewReplica(
		&querypb.Replica{
			ID:            1,
			CollectionID:  1,
			Nodes:         []int64{},
			ResourceGroup: meta.DefaultResourceGroupName,
		},
		typeutil.NewUniqueSet(),
	))

	suite.taskScheduler.EXPECT().AddExecutor(mock.Anything)
	suite.distController.EXPECT().StartDistInstance(mock.Anything, mock.Anything)

	suite.nodeMgr.Add(session.NewNodeInfo(111, "localhost"))
	server.handleNodeUp(111)
	nodes := suite.server.meta.ReplicaManager.Get(1).GetNodes()
	suite.Len(nodes, 1)
	suite.Equal(int64(111), nodes[0])
	log.Info("handleNodeUp")

	// when more rg exist, new node shouldn't be assign to replica in default rg in handleNodeUp
	suite.server.meta.ResourceManager.AddResourceGroup("rg")
	suite.nodeMgr.Add(session.NewNodeInfo(222, "localhost"))
	server.handleNodeUp(222)
	nodes = suite.server.meta.ReplicaManager.Get(1).GetNodes()
	suite.Len(nodes, 2)
	suite.Contains(nodes, int64(111))
	suite.Contains(nodes, int64(222))
}

func (suite *ServiceSuite) loadAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		suite.expectLoadPartitions()
		suite.expectGetRecoverInfo(collection)
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			req := &querypb.LoadCollectionRequest{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[collection],
			}
			job := job.NewLoadCollectionJob(
				ctx,
				req,
				suite.dist,
				suite.meta,
				suite.broker,
				suite.cluster,
				suite.targetMgr,
				suite.targetObserver,
				suite.nodeMgr,
			)
			suite.jobScheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(suite.replicaNumber[collection], suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetCollection(collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(collection)
		} else {
			req := &querypb.LoadPartitionsRequest{
				CollectionID:  collection,
				PartitionIDs:  suite.partitions[collection],
				ReplicaNumber: suite.replicaNumber[collection],
			}
			job := job.NewLoadPartitionJob(
				ctx,
				req,
				suite.dist,
				suite.meta,
				suite.broker,
				suite.cluster,
				suite.targetMgr,
				suite.targetObserver,
				suite.nodeMgr,
			)
			suite.jobScheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(suite.replicaNumber[collection], suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetPartitionsByCollection(collection))
			suite.targetMgr.UpdateCollectionCurrentTarget(collection)
		}
	}
}

func (suite *ServiceSuite) assertLoaded(collection int64) {
	suite.True(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(collection, channel, meta.NextTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.NotNil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.NextTarget))
		}
	}
}

func (suite *ServiceSuite) assertPartitionLoaded(collection int64, partitions ...int64) {
	suite.True(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	partitionSet := typeutil.NewUniqueSet(partitions...)
	for partition, segments := range suite.segments[collection] {
		if !partitionSet.Contain(partition) {
			continue
		}
		for _, segment := range segments {
			suite.NotNil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
		}
	}
}

func (suite *ServiceSuite) assertReleased(collection int64) {
	suite.False(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.Nil(suite.targetMgr.GetDmChannel(collection, channel, meta.CurrentTarget))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.Nil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.CurrentTarget))
			suite.Nil(suite.targetMgr.GetHistoricalSegment(collection, segment, meta.NextTarget))
		}
	}
}

func (suite *ServiceSuite) assertSegments(collection int64, segments []*querypb.SegmentInfo) bool {
	segmentSet := typeutil.NewUniqueSet(
		suite.getAllSegments(collection)...)
	if !suite.Len(segments, segmentSet.Len()) {
		return false
	}
	for _, segment := range segments {
		if !suite.Contains(segmentSet, segment.GetSegmentID()) {
			return false
		}
	}

	return true
}

func (suite *ServiceSuite) expectGetRecoverInfo(collection int64) {
	suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil).Maybe()
	vChannels := []*datapb.VchannelInfo{}
	for _, channel := range suite.channels[collection] {
		vChannels = append(vChannels, &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel,
		})
	}

	segmentBinlogs := []*datapb.SegmentInfo{}
	for partition, segments := range suite.segments[collection] {
		for _, segment := range segments {
			segmentBinlogs = append(segmentBinlogs, &datapb.SegmentInfo{
				ID:            segment,
				InsertChannel: suite.channels[collection][segment%2],
				PartitionID:   partition,
				CollectionID:  collection,
			})
		}
	}
	suite.broker.EXPECT().
		GetRecoveryInfoV2(mock.Anything, collection, mock.Anything, mock.Anything).
		Return(vChannels, segmentBinlogs, nil)
}

func (suite *ServiceSuite) expectLoadPartitions() {
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything).
		Return(nil, nil)
	suite.broker.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
		Return(nil, nil)
	suite.cluster.EXPECT().LoadPartitions(mock.Anything, mock.Anything, mock.Anything).
		Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)
}

func (suite *ServiceSuite) getAllSegments(collection int64) []int64 {
	allSegments := make([]int64, 0)
	for _, segments := range suite.segments[collection] {
		allSegments = append(allSegments, segments...)
	}
	return allSegments
}

func (suite *ServiceSuite) updateSegmentDist(collection, node int64) {
	metaSegments := make([]*meta.Segment, 0)
	for partition, segments := range suite.segments[collection] {
		for _, segment := range segments {
			metaSegments = append(metaSegments,
				utils.CreateTestSegment(collection, partition, segment, node, 1, "test-channel"))
		}
	}
	suite.dist.SegmentDistManager.Update(node, metaSegments...)
}

func (suite *ServiceSuite) updateChannelDist(collection int64) {
	channels := suite.channels[collection]
	segments := lo.Flatten(lo.Values(suite.segments[collection]))

	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	for _, replica := range replicas {
		i := 0
		for _, node := range replica.GetNodes() {
			suite.dist.ChannelDistManager.Update(node, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channels[i],
			}))
			suite.dist.LeaderViewManager.Update(node, &meta.LeaderView{
				ID:           node,
				CollectionID: collection,
				Channel:      channels[i],
				Segments: lo.SliceToMap(segments, func(segment int64) (int64, *querypb.SegmentDist) {
					return segment, &querypb.SegmentDist{
						NodeID:  node,
						Version: time.Now().Unix(),
					}
				}),
			})
			i++
			if i >= len(channels) {
				break
			}
		}
	}
}

func (suite *ServiceSuite) updateChannelDistWithoutSegment(collection int64) {
	channels := suite.channels[collection]

	replicas := suite.meta.ReplicaManager.GetByCollection(collection)
	for _, replica := range replicas {
		i := 0
		for _, node := range replica.GetNodes() {
			suite.dist.ChannelDistManager.Update(node, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
				CollectionID: collection,
				ChannelName:  channels[i],
			}))
			suite.dist.LeaderViewManager.Update(node, &meta.LeaderView{
				ID:           node,
				CollectionID: collection,
				Channel:      channels[i],
			})
			i++
			if i >= len(channels) {
				break
			}
		}
	}
}

func (suite *ServiceSuite) updateCollectionStatus(collectionID int64, status querypb.LoadStatus) {
	collection := suite.meta.GetCollection(collectionID)
	if collection != nil {
		collection := collection.Clone()
		collection.LoadPercentage = 0
		if status == querypb.LoadStatus_Loaded {
			collection.LoadPercentage = 100
		}
		collection.CollectionLoadInfo.Status = status
		suite.meta.PutCollection(collection)

		partitions := suite.meta.GetPartitionsByCollection(collectionID)
		for _, partition := range partitions {
			partition := partition.Clone()
			partition.LoadPercentage = 0
			if status == querypb.LoadStatus_Loaded {
				partition.LoadPercentage = 100
			}
			partition.PartitionLoadInfo.Status = status
			suite.meta.PutPartition(partition)
		}
	}
}

func (suite *ServiceSuite) fetchHeartbeats(time time.Time) {
	for _, node := range suite.nodes {
		node := suite.nodeMgr.Get(node)
		node.SetLastHeartbeat(time)
	}
}

func (suite *ServiceSuite) TearDownTest() {
	suite.targetObserver.Stop()
}

func TestService(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}
