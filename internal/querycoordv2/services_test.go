package querycoordv2

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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
	kv              kv.MetaKv
	store           meta.Store
	dist            *meta.DistributionManager
	meta            *meta.Meta
	targetMgr       *meta.TargetManager
	broker          *meta.MockBroker
	cluster         *session.MockCluster
	nodeMgr         *session.NodeManager
	jobScheduler    *job.Scheduler
	taskScheduler   *task.MockScheduler
	handoffObserver *observers.HandoffObserver
	balancer        balance.Balance

	// Test object
	server *Server
}

func (suite *ServiceSuite) SetupSuite() {
	Params.Init()

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
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	suite.store = meta.NewMetaStore(suite.kv)
	suite.dist = meta.NewDistributionManager()
	suite.meta = meta.NewMeta(params.RandomIncrementIDAllocator(), suite.store)
	suite.targetMgr = meta.NewTargetManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.nodeMgr = session.NewNodeManager()
	for _, node := range suite.nodes {
		suite.nodeMgr.Add(session.NewNodeInfo(node, "localhost"))
	}
	suite.cluster = session.NewMockCluster(suite.T())
	suite.jobScheduler = job.NewScheduler()
	suite.taskScheduler = task.NewMockScheduler(suite.T())
	suite.jobScheduler.Start(context.Background())
	suite.handoffObserver = observers.NewHandoffObserver(
		suite.store,
		suite.meta,
		suite.dist,
		suite.targetMgr,
	)
	suite.balancer = balance.NewRowCountBasedBalancer(
		suite.taskScheduler,
		suite.nodeMgr,
		suite.dist,
		suite.meta,
	)

	suite.server = &Server{
		kv:                  suite.kv,
		store:               suite.store,
		session:             sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, cli),
		metricsCacheManager: metricsinfo.NewMetricsCacheManager(),
		dist:                suite.dist,
		meta:                suite.meta,
		targetMgr:           suite.targetMgr,
		broker:              suite.broker,
		nodeMgr:             suite.nodeMgr,
		cluster:             suite.cluster,
		jobScheduler:        suite.jobScheduler,
		taskScheduler:       suite.taskScheduler,
		balancer:            suite.balancer,
		handoffObserver:     suite.handoffObserver,
	}
	suite.server.UpdateStateCode(internalpb.StateCode_Healthy)
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
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	suite.Len(resp.CollectionIDs, collectionNum)
	for _, collection := range suite.collections {
		suite.Contains(resp.CollectionIDs, collection)
	}

	// Test get 1 collection
	collection := suite.collections[0]
	req.CollectionIDs = []int64{collection}
	resp, err = server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	suite.Len(resp.CollectionIDs, 1)
	suite.Equal(collection, resp.CollectionIDs[0])

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	resp, err = server.ShowCollections(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		suite.Len(resp.PartitionIDs, 1)
		for _, partition := range partitions[0:1] {
			suite.Contains(resp.PartitionIDs, partition)
		}
	}

	// Test when server is not healthy
	req := &querypb.ShowPartitionsRequest{
		CollectionID: suite.collections[0],
	}
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	resp, err := server.ShowPartitions(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
}

func (suite *ServiceSuite) TestLoadCollection() {
	ctx := context.Background()
	server := suite.server

	// Test load all collections
	for _, collection := range suite.collections {
		suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)
		suite.expectGetRecoverInfo(collection)

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
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.LoadCollectionRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.LoadCollection(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Reason, ErrNotHealthy.Error())
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
}

func (suite *ServiceSuite) TestLoadPartition() {
	ctx := context.Background()
	server := suite.server

	// Test load all partitions
	for _, collection := range suite.collections {
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

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.LoadPartitionsRequest{
		CollectionID: suite.collections[0],
		PartitionIDs: suite.partitions[suite.collections[0]],
	}
	resp, err := server.LoadPartitions(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Reason, ErrNotHealthy.Error())
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
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, job.ErrLoadParameterMismatched.Error())
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
		suite.Equal(commonpb.ErrorCode_IllegalArgument, resp.ErrorCode)
		suite.Contains(resp.Reason, job.ErrLoadParameterMismatched.Error())
	}
}

func (suite *ServiceSuite) TestReleaseCollection() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

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
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.ReleaseCollectionRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.ReleaseCollection(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Reason, ErrNotHealthy.Error())
}

func (suite *ServiceSuite) TestReleasePartition() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test release all partitions
	for _, collection := range suite.collections {
		req := &querypb.ReleasePartitionsRequest{
			CollectionID: collection,
			PartitionIDs: suite.partitions[collection][0:1],
		}
		resp, err := server.ReleasePartitions(ctx, req)
		suite.NoError(err)
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		} else {
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		}
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
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		} else {
			suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
		}
		suite.assertPartitionLoaded(collection, suite.partitions[collection][1:]...)
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.ReleasePartitionsRequest{
		CollectionID: suite.collections[0],
		PartitionIDs: suite.partitions[suite.collections[0]][0:1],
	}
	resp, err := server.ReleasePartitions(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Reason, ErrNotHealthy.Error())
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		suite.Len(resp.PartitionDescriptions, len(suite.partitions[collection]))
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.GetPartitionStatesRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetPartitionStates(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		suite.assertSegments(collection, resp.GetInfos())
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.GetSegmentInfoRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetSegmentInfo(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
}

func (suite *ServiceSuite) TestLoadBalance() {
	suite.loadAll()
	ctx := context.Background()
	server := suite.server

	// Test get balance first segment
	for _, collection := range suite.collections {
		replicas := suite.meta.ReplicaManager.GetByCollection(collection)
		srcNode := replicas[0].GetNodes()[0]
		dstNode := replicas[0].GetNodes()[1]
		suite.updateCollectionStatus(collection, querypb.LoadStatus_Loaded)
		suite.updateSegmentDist(collection, srcNode)
		segments := suite.getAllSegments(collection)
		req := &querypb.LoadBalanceRequest{
			CollectionID:     collection,
			SourceNodeIDs:    []int64{srcNode},
			DstNodeIDs:       []int64{dstNode},
			SealedSegmentIDs: segments,
		}
		suite.taskScheduler.EXPECT().Add(mock.Anything).Run(func(task task.Task) {
			task.Cancel()
		}).Return(nil)
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.ErrorCode)
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.LoadBalanceRequest{
		CollectionID:  suite.collections[0],
		SourceNodeIDs: []int64{1},
		DstNodeIDs:    []int64{100 + 1},
	}
	resp, err := server.LoadBalance(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Reason, ErrNotHealthy.Error())
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
		srcNode := replicas[0].GetNodes()[0]
		dstNode := replicas[0].GetNodes()[1]
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
		srcNode := replicas[0].GetNodes()[0]
		dstNode := replicas[0].GetNodes()[1]
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
			balanceTask.SetErr(task.ErrTaskCanceled)
			balanceTask.Cancel()
		}).Return(nil)
		resp, err := server.LoadBalance(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
		suite.Contains(resp.Reason, "failed to balance segments")
		suite.Contains(resp.Reason, task.ErrTaskCanceled.Error())
	}
}

func (suite *ServiceSuite) TestShowConfigurations() {
	ctx := context.Background()
	server := suite.server

	req := &internalpb.ShowConfigurationsRequest{
		Pattern: "Port",
	}
	resp, err := server.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	suite.Len(resp.Configuations, 1)
	suite.Equal("querycoord.port", resp.Configuations[0].Key)

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req = &internalpb.ShowConfigurationsRequest{
		Pattern: "Port",
	}
	resp, err = server.ShowConfigurations(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
}

func (suite *ServiceSuite) TestGetMetrics() {
	ctx := context.Background()
	server := suite.server

	for _, node := range suite.nodes {
		suite.cluster.EXPECT().GetMetrics(ctx, node, mock.Anything).Return(&milvuspb.GetMetricsResponse{
			Status:        successStatus,
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
	suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	resp, err = server.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
		Base:    &commonpb.MsgBase{},
		Request: string(req),
	})
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
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
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		suite.EqualValues(suite.replicaNumber[collection], len(resp.Replicas))
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &milvuspb.GetReplicasRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetReplicas(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
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
		resp, err := server.GetShardLeaders(ctx, req)
		suite.NoError(err)
		suite.Equal(commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		suite.Len(resp.Shards, len(suite.channels[collection]))
		for _, shard := range resp.Shards {
			suite.Len(shard.NodeIds, int(suite.replicaNumber[collection]))
		}
	}

	// Test when server is not healthy
	server.UpdateStateCode(internalpb.StateCode_Initializing)
	req := &querypb.GetShardLeadersRequest{
		CollectionID: suite.collections[0],
	}
	resp, err := server.GetShardLeaders(ctx, req)
	suite.NoError(err)
	suite.Contains(resp.Status.Reason, ErrNotHealthy.Error())
}

func (suite *ServiceSuite) loadAll() {
	ctx := context.Background()
	for _, collection := range suite.collections {
		suite.expectGetRecoverInfo(collection)
		if suite.loadTypes[collection] == querypb.LoadType_LoadCollection {
			suite.broker.EXPECT().GetPartitions(mock.Anything, collection).Return(suite.partitions[collection], nil)

			req := &querypb.LoadCollectionRequest{
				CollectionID:  collection,
				ReplicaNumber: suite.replicaNumber[collection],
			}
			job := job.NewLoadCollectionJob(
				ctx,
				req,
				suite.dist,
				suite.meta,
				suite.targetMgr,
				suite.broker,
				suite.nodeMgr,
				suite.handoffObserver,
			)
			suite.jobScheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(suite.replicaNumber[collection], suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetCollection(collection))
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
				suite.targetMgr,
				suite.broker,
				suite.nodeMgr,
				suite.handoffObserver,
			)
			suite.jobScheduler.Add(job)
			err := job.Wait()
			suite.NoError(err)
			suite.EqualValues(suite.replicaNumber[collection], suite.meta.GetReplicaNumber(collection))
			suite.True(suite.meta.Exist(collection))
			suite.NotNil(suite.meta.GetPartitionsByCollection(collection))
		}
	}
}

func (suite *ServiceSuite) assertLoaded(collection int64) {
	suite.True(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(channel))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.NotNil(suite.targetMgr.GetSegment(segment))
		}
	}
}

func (suite *ServiceSuite) assertPartitionLoaded(collection int64, partitions ...int64) {
	suite.True(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.NotNil(suite.targetMgr.GetDmChannel(channel))
	}
	partitionSet := typeutil.NewUniqueSet(partitions...)
	for partition, segments := range suite.segments[collection] {
		if !partitionSet.Contain(partition) {
			continue
		}
		for _, segment := range segments {
			suite.NotNil(suite.targetMgr.GetSegment(segment))
		}
	}
}

func (suite *ServiceSuite) assertReleased(collection int64) {
	suite.False(suite.meta.Exist(collection))
	for _, channel := range suite.channels[collection] {
		suite.Nil(suite.targetMgr.GetDmChannel(channel))
	}
	for _, partitions := range suite.segments[collection] {
		for _, segment := range partitions {
			suite.Nil(suite.targetMgr.GetSegment(segment))
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
	vChannels := []*datapb.VchannelInfo{}
	for _, channel := range suite.channels[collection] {
		vChannels = append(vChannels, &datapb.VchannelInfo{
			CollectionID: collection,
			ChannelName:  channel,
		})
	}

	for partition, segments := range suite.segments[collection] {
		segmentBinlogs := []*datapb.SegmentBinlogs{}
		for _, segment := range segments {
			segmentBinlogs = append(segmentBinlogs, &datapb.SegmentBinlogs{
				SegmentID:     segment,
				InsertChannel: suite.channels[collection][segment%2],
			})
		}

		suite.broker.EXPECT().
			GetRecoveryInfo(mock.Anything, collection, partition).
			Return(vChannels, segmentBinlogs, nil)
	}
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
		suite.meta.UpdateCollection(collection)
	} else {
		partitions := suite.meta.GetPartitionsByCollection(collectionID)
		for _, partition := range partitions {
			partition := partition.Clone()
			partition.LoadPercentage = 0
			if status == querypb.LoadStatus_Loaded {
				partition.LoadPercentage = 100
			}
			partition.PartitionLoadInfo.Status = status
			suite.meta.UpdatePartition(partition)
		}
	}
}

func TestService(t *testing.T) {
	suite.Run(t, new(ServiceSuite))
}
