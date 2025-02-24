// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package proxy

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type LBPolicySuite struct {
	suite.Suite
	rc types.RootCoordClient
	qc *mocks.MockQueryCoordClient
	qn *mocks.MockQueryNodeClient

	mgr        *MockShardClientManager
	lbBalancer *MockLBBalancer
	lbPolicy   *LBPolicyImpl

	nodeIDs  []int64
	nodes    []nodeInfo
	channels []string
	qnList   []*mocks.MockQueryNode

	collectionName string
	collectionID   int64
}

func (s *LBPolicySuite) SetupSuite() {
	paramtable.Init()
}

func (s *LBPolicySuite) SetupTest() {
	s.nodeIDs = make([]int64, 0)
	for i := 1; i <= 5; i++ {
		s.nodeIDs = append(s.nodeIDs, int64(i))
		s.nodes = append(s.nodes, nodeInfo{
			nodeID:  int64(i),
			address: "localhost",
		})
	}
	s.channels = []string{"channel1", "channel2"}
	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc := mocks.NewMockQueryCoordClient(s.T())
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&successStatus, nil)
	qc.EXPECT().ShowCollections(mock.Anything, mock.Anything).Return(&querypb.ShowCollectionsResponse{}, nil).Maybe()

	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: s.channels[0],
				NodeIds:     s.nodeIDs,
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004"},
			},
			{
				ChannelName: s.channels[1],
				NodeIds:     s.nodeIDs,
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004"},
			},
		},
	}, nil).Maybe()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status:       merr.Success(),
		PartitionIDs: []int64{1, 2, 3},
	}, nil).Maybe()

	s.qc = qc
	s.rc = NewRootCoordMock()

	s.qn = mocks.NewMockQueryNodeClient(s.T())
	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	s.mgr = NewMockShardClientManager(s.T())
	s.lbBalancer = NewMockLBBalancer(s.T())
	s.lbBalancer.EXPECT().Start(context.Background()).Maybe()
	s.lbPolicy = NewLBPolicyImpl(s.mgr)
	s.lbPolicy.Start(context.Background())
	s.lbPolicy.getBalancer = func() LBBalancer {
		return s.lbBalancer
	}

	err := InitMetaCache(context.Background(), s.rc, s.qc, s.mgr)
	s.NoError(err)

	s.collectionName = "test_lb_policy"
	s.loadCollection()
}

func (s *LBPolicySuite) loadCollection() {
	fieldName2Types := map[string]schemapb.DataType{
		testBoolField:     schemapb.DataType_Bool,
		testInt32Field:    schemapb.DataType_Int32,
		testInt64Field:    schemapb.DataType_Int64,
		testFloatField:    schemapb.DataType_Float,
		testDoubleField:   schemapb.DataType_Double,
		testFloatVecField: schemapb.DataType_FloatVector,
	}
	if enableMultipleVectorFields {
		fieldName2Types[testBinaryVecField] = schemapb.DataType_BinaryVector
	}

	schema := constructCollectionSchemaByDataType(s.collectionName, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	ctx := context.Background()
	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: s.collectionName,
			DbName:         dbName,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		},
		ctx:       ctx,
		rootCoord: s.rc,
	}

	s.NoError(createColT.OnEnqueue())
	s.NoError(createColT.PreExecute(ctx))
	s.NoError(createColT.Execute(ctx))
	s.NoError(createColT.PostExecute(ctx))

	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, s.collectionName)
	s.NoError(err)

	status, err := s.qc.LoadCollection(ctx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:  commonpb.MsgType_LoadCollection,
			SourceID: paramtable.GetNodeID(),
		},
		CollectionID: collectionID,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, status.ErrorCode)
	s.collectionID = collectionID
}

func (s *LBPolicySuite) TestSelectNode() {
	ctx := context.Background()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(5, nil)
	targetNode, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
	}, typeutil.NewUniqueSet())
	s.NoError(err)
	s.Equal(int64(5), targetNode.nodeID)

	// test select node failed, then update shard leader cache and retry, expect success
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, errors.New("fake err")).Times(1)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(3, nil)
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   []nodeInfo{},
		nq:             1,
	}, typeutil.NewUniqueSet())
	s.NoError(err)
	s.Equal(int64(3), targetNode.nodeID)

	// test select node always fails, expected failure
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNodeNotAvailable)
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   []nodeInfo{},
		nq:             1,
	}, typeutil.NewUniqueSet())
	s.ErrorIs(err, merr.ErrNodeNotAvailable)

	// test all nodes has been excluded, expected failure
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNodeNotAvailable)
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
	}, typeutil.NewUniqueSet(s.nodeIDs...))
	s.ErrorIs(err, merr.ErrChannelNotAvailable)

	// test get shard leaders failed, retry to select node failed
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNodeNotAvailable)
	s.qc.ExpectedCalls = nil
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnavailable)
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
	}, typeutil.NewUniqueSet())
	s.ErrorIs(err, merr.ErrServiceUnavailable)
}

func (s *LBPolicySuite) TestExecuteWithRetry() {
	ctx := context.Background()

	// test execute success
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.NoError(err)

	// test select node failed, expected error
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNodeNotAvailable)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.ErrorIs(err, merr.ErrNodeNotAvailable)

	// test get client failed, and retry failed, expected success
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Times(1)
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.Error(err)

	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Times(1)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
		retryTimes: 2,
	})
	s.NoError(err)

	// test exec failed, then retry success
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	counter := 0
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			counter++
			if counter == 1 {
				return errors.New("fake error")
			}
			return nil
		},
		retryTimes: 2,
	})
	s.NoError(err)

	// test exec timeout
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, context.Canceled).Times(1)
	s.qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		channel:        s.channels[0],
		shardLeaders:   s.nodes,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			_, err := qn.Search(ctx, nil)
			return err
		},
		retryTimes: 2,
	})
	s.True(merr.IsCanceledOrTimeout(err))
}

func (s *LBPolicySuite) TestExecute() {
	ctx := context.Background()
	mockErr := errors.New("mock error")
	// test  all channel success
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.NoError(err)

	// test some channel failed
	counter := atomic.NewInt64(0)
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			// succeed in first execute
			if counter.Add(1) == 1 {
				return nil
			}

			return mockErr
		},
	})
	s.Error(err)
	s.Equal(int64(11), counter.Load())

	// test get shard leader failed
	s.qc.ExpectedCalls = nil
	globalMetaCache.DeprecateShardCache(dbName, s.collectionName)
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(nil, mockErr)
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:             dbName,
		collectionName: s.collectionName,
		collectionID:   s.collectionID,
		nq:             1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.ErrorIs(err, mockErr)
}

func (s *LBPolicySuite) TestUpdateCostMetrics() {
	s.lbBalancer.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything)
	s.lbPolicy.UpdateCostMetrics(1, &internalpb.CostAggregation{})
}

func (s *LBPolicySuite) TestNewLBPolicy() {
	policy := NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*proxy.LookAsideBalancer")
	policy.Close()

	Params.Save(Params.ProxyCfg.ReplicaSelectionPolicy.Key, "round_robin")
	policy = NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*proxy.RoundRobinBalancer")
	policy.Close()

	Params.Save(Params.ProxyCfg.ReplicaSelectionPolicy.Key, "look_aside")
	policy = NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*proxy.LookAsideBalancer")
	policy.Close()
}

func (s *LBPolicySuite) TestGetShardLeaders() {
	ctx := context.Background()

	// ErrCollectionNotFullyLoaded is retriable, expected to retry until ctx done or success
	counter := atomic.NewInt64(0)
	globalMetaCache.DeprecateShardCache(dbName, s.collectionName)
	s.qc.ExpectedCalls = nil
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			counter.Inc()
			return nil, merr.ErrCollectionNotFullyLoaded
		}).Times(5)
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			log.Info("return rpc success")
			return nil, nil
		}).Times(5)
	_, err := s.lbPolicy.GetShardLeaders(ctx, dbName, s.collectionName, s.collectionID, true)
	s.NoError(err)
	s.Equal(int64(5), counter.Load())

	// ErrServiceUnavailable is not retriable, expected to fail fast
	counter.Store(0)
	globalMetaCache.DeprecateShardCache(dbName, s.collectionName)
	s.qc.ExpectedCalls = nil
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *querypb.GetShardLeadersRequest, opts ...grpc.CallOption) (*querypb.GetShardLeadersResponse, error) {
			counter.Inc()
			return nil, merr.ErrCollectionNotLoaded
		})
	_, err = s.lbPolicy.GetShardLeaders(ctx, dbName, s.collectionName, s.collectionID, true)
	log.Info("check err", zap.Error(err))
	s.Error(err)
	s.Equal(int64(1), counter.Load())
}

func TestLBPolicySuite(t *testing.T) {
	suite.Run(t, new(LBPolicySuite))
}
