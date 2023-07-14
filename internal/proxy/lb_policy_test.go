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
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LBPolicySuite struct {
	suite.Suite
	rc types.RootCoord
	qc *mocks.MockQueryCoord
	qn *mocks.MockQueryNode

	mgr        *MockShardClientManager
	lbBalancer *MockLBBalancer
	lbPolicy   *LBPolicyImpl

	nodes    []int64
	channels []string
	qnList   []*mocks.MockQueryNode

	collection string
}

func (s *LBPolicySuite) SetupSuite() {
	Params.Init()
}

func (s *LBPolicySuite) SetupTest() {
	s.nodes = []int64{1, 2, 3, 4, 5}
	s.channels = []string{"channel1", "channel2"}
	successStatus := commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}
	qc := mocks.NewMockQueryCoord(s.T())
	qc.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(&successStatus, nil)

	qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(&querypb.GetShardLeadersResponse{
		Status: &successStatus,
		Shards: []*querypb.ShardLeadersList{
			{
				ChannelName: s.channels[0],
				NodeIds:     s.nodes,
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004"},
			},
			{
				ChannelName: s.channels[1],
				NodeIds:     s.nodes,
				NodeAddrs:   []string{"localhost:9000", "localhost:9001", "localhost:9002", "localhost:9003", "localhost:9004"},
			},
		},
	}, nil).Maybe()
	qc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&querypb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: []int64{1, 2, 3},
	}, nil).Maybe()

	s.qc = qc
	s.rc = NewRootCoordMock()
	s.rc.Start()

	s.qn = mocks.NewMockQueryNode(s.T())
	s.qn.EXPECT().GetAddress().Return("localhost").Maybe()
	s.qn.EXPECT().GetComponentStates(mock.Anything).Return(nil, nil).Maybe()

	s.mgr = NewMockShardClientManager(s.T())
	s.mgr.EXPECT().UpdateShardLeaders(mock.Anything, mock.Anything).Return(nil).Maybe()
	s.lbBalancer = NewMockLBBalancer(s.T())
	s.lbBalancer.EXPECT().Start(context.Background()).Maybe()
	s.lbPolicy = NewLBPolicyImpl(s.mgr)
	s.lbPolicy.Start(context.Background())
	s.lbPolicy.balancer = s.lbBalancer

	err := InitMetaCache(context.Background(), s.rc, s.qc, s.mgr)
	s.NoError(err)

	s.collection = "test_lb_policy"
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

	schema := constructCollectionSchemaByDataType(s.collection, fieldName2Types, testInt64Field, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	ctx := context.Background()
	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			CollectionName: s.collection,
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

	collectionID, err := globalMetaCache.GetCollectionID(ctx, dbName, s.collection)
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
}

func (s *LBPolicySuite) TestSelectNode() {
	ctx := context.Background()
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(5, nil)
	targetNode, err := s.lbPolicy.selectNode(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
	}, typeutil.NewUniqueSet())
	s.NoError(err)
	s.Equal(int64(5), targetNode)

	// test select node failed, then update shard leader cache and retry, expect success
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, errors.New("fake err")).Times(1)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(3, nil)
	targetNode, err = s.lbPolicy.selectNode(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: []int64{},
		nq:           1,
	}, typeutil.NewUniqueSet())
	s.NoError(err)
	s.Equal(int64(3), targetNode)

	// test select node always fails, expected failure
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNoAvailableNode)
	targetNode, err = s.lbPolicy.selectNode(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: []int64{},
		nq:           1,
	}, typeutil.NewUniqueSet())
	s.ErrorIs(err, merr.ErrNoAvailableNode)
	s.Equal(int64(-1), targetNode)

	// test all nodes has been excluded, expected failure
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNoAvailableNode)
	targetNode, err = s.lbPolicy.selectNode(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
	}, typeutil.NewUniqueSet(s.nodes...))
	s.ErrorIs(err, merr.ErrNoAvailableNode)
	s.Equal(int64(-1), targetNode)

	// test get shard leaders failed, retry to select node failed
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNoAvailableNode)
	s.qc.ExpectedCalls = nil
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(nil, merr.ErrNoAvailableNodeInReplica)
	targetNode, err = s.lbPolicy.selectNode(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
	}, typeutil.NewUniqueSet())
	s.ErrorIs(err, merr.ErrNoAvailableNodeInReplica)
	s.Equal(int64(-1), targetNode)
}

func (s *LBPolicySuite) TestExecuteWithRetry() {
	ctx := context.Background()

	// test execute success
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.NoError(err)

	// test select node failed, expected error
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(-1, merr.ErrNoAvailableNode)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.ErrorIs(err, merr.ErrNoAvailableNode)

	// test get client failed, and retry failed, expected success
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Times(1)
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
		retryTimes: 1,
	})
	s.Error(err)

	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Times(1)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
		retryTimes: 2,
	})
	s.NoError(err)

	// test exec failed, then retry success
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.ExpectedCalls = nil
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	counter := 0
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		db:           dbName,
		collection:   s.collection,
		channel:      s.channels[0],
		shardLeaders: s.nodes,
		nq:           1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			counter++
			if counter == 1 {
				return errors.New("fake error")
			}
			return nil
		},
		retryTimes: 2,
	})
	s.NoError(err)
}

func (s *LBPolicySuite) TestExecute() {
	ctx := context.Background()
	// test  all channel success
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(1, nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:         dbName,
		collection: s.collection,
		nq:         1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
	})
	s.NoError(err)

	// test some channel failed
	counter := atomic.NewInt64(0)
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:         dbName,
		collection: s.collection,
		nq:         1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			if counter.Add(1) == 1 {
				return nil
			}

			return errors.New("fake error")
		},
	})
	s.Error(err)

	// test get shard leader failed
	s.qc.ExpectedCalls = nil
	globalMetaCache.DeprecateShardCache(dbName, s.collection)
	s.qc.EXPECT().GetShardLeaders(mock.Anything, mock.Anything).Return(nil, errors.New("fake error"))
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		db:         dbName,
		collection: s.collection,
		nq:         1,
		exec: func(ctx context.Context, ui UniqueID, qn types.QueryNode, s ...string) error {
			return nil
		},
	})
	s.Error(err)
}

func (s *LBPolicySuite) TestUpdateCostMetrics() {
	s.lbBalancer.EXPECT().UpdateCostMetrics(mock.Anything, mock.Anything)
	s.lbPolicy.UpdateCostMetrics(1, &internalpb.CostAggregation{})
}

func (s *LBPolicySuite) TestNewLBPolicy() {
	policy := NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.balancer).String(), "*proxy.LookAsideBalancer")
	policy.Close()

	Params.Save(Params.ProxyCfg.ReplicaSelectionPolicy.Key, "round_robin")
	policy = NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.balancer).String(), "*proxy.RoundRobinBalancer")
	policy.Close()

	Params.Save(Params.ProxyCfg.ReplicaSelectionPolicy.Key, "look_aside")
	policy = NewLBPolicyImpl(s.mgr)
	s.Equal(reflect.TypeOf(policy.balancer).String(), "*proxy.LookAsideBalancer")
	policy.Close()
}

func TestLBPolicySuite(t *testing.T) {
	suite.Run(t, new(LBPolicySuite))
}
