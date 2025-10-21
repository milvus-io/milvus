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

package shardclient

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type LBPolicySuite struct {
	suite.Suite
	qn *mocks.MockQueryNodeClient

	mgr        *MockShardClientManager
	lbBalancer *MockLBBalancer
	lbPolicy   *LBPolicyImpl

	nodeIDs  []int64
	nodes    []NodeInfo
	channels []string

	dbName         string
	collectionName string
	collectionID   int64
}

func (s *LBPolicySuite) SetupSuite() {
	paramtable.Init()
}

func (s *LBPolicySuite) SetupTest() {
	s.nodeIDs = make([]int64, 0)
	s.nodes = make([]NodeInfo, 0)
	for i := 1; i <= 5; i++ {
		s.nodeIDs = append(s.nodeIDs, int64(i))
		s.nodes = append(s.nodes, NodeInfo{
			NodeID:      int64(i),
			Address:     "localhost",
			Serviceable: true,
		})
	}
	s.channels = []string{"channel1", "channel2"}

	s.qn = mocks.NewMockQueryNodeClient(s.T())
	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	s.mgr = NewMockShardClientManager(s.T())
	s.lbBalancer = NewMockLBBalancer(s.T())
	s.lbBalancer.EXPECT().Start(mock.Anything).Maybe()
	s.lbBalancer.EXPECT().Close().Maybe()

	s.lbPolicy = NewLBPolicyImpl(s.mgr)
	s.lbPolicy.Start(context.Background())
	s.lbPolicy.getBalancer = func() LBBalancer {
		return s.lbBalancer
	}

	s.dbName = "test_lb_policy"
	s.collectionName = "test_lb_policy"
	s.collectionID = 100
}

func (s *LBPolicySuite) TearDownTest() {
	s.lbPolicy.Close()
}

func (s *LBPolicySuite) TestSelectNode() {
	ctx := context.Background()

	// test select node success
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(5), nil)
	excludeNodes := typeutil.NewUniqueSet()
	targetNode, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(5), targetNode.NodeID)

	// test select node failed, then update shard leader cache and retry, expect success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	// First call with cache fails
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), errors.New("fake err")).Once()
	// Second call without cache succeeds
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(3), nil).Once()
	excludeNodes = typeutil.NewUniqueSet()
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(3), targetNode.NodeID)

	// test select node always fails, expected failure
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), merr.ErrNodeNotAvailable)
	excludeNodes = typeutil.NewUniqueSet()
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrNodeNotAvailable)

	// test all nodes has been excluded, expected clear excludeNodes and try to select node again
	excludeNodes = typeutil.NewUniqueSet(s.nodeIDs...)
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), merr.ErrNodeNotAvailable)
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrNodeNotAvailable)

	// test get shard leaders failed, retry to select node failed
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(nil, merr.ErrCollectionNotLoaded).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(nil, merr.ErrCollectionNotLoaded).Once()
	excludeNodes = typeutil.NewUniqueSet()
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrCollectionNotLoaded)
}

func (s *LBPolicySuite) TestExecuteWithRetry() {
	ctx := context.Background()

	// test execute success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.NoError(err)

	// test select node failed, expected error
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Maybe()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), merr.ErrNodeNotAvailable)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.ErrorIs(err, merr.ErrNodeNotAvailable)

	// test get client failed, and retry failed, expected error
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error"))
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.Error(err)

	// test get client failed once, then retry success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(nil, errors.New("fake error")).Once()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.NoError(err)

	// test exec failed, then retry success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	counter := 0
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			counter++
			if counter == 1 {
				return errors.New("fake error")
			}
			return nil
		},
	})
	s.NoError(err)

	// test exec timeout
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	s.qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, context.Canceled).Once()
	s.qn.EXPECT().Search(mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
	err = s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			_, err := qn.Search(ctx, nil)
			return err
		},
	})
	s.True(merr.IsCanceledOrTimeout(err))
}

func (s *LBPolicySuite) TestExecuteOneChannel() {
	ctx := context.Background()
	mockErr := errors.New("mock error")

	// test all channel success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(s.channels, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.NoError(err)

	// test get shard leader failed
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(nil, mockErr)
	err = s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.ErrorIs(err, mockErr)
}

func (s *LBPolicySuite) TestExecute() {
	ctx := context.Background()
	mockErr := errors.New("mock error")

	// test all channel success
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(s.channels, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, mock.Anything).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, mock.Anything).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			return nil
		},
	})
	s.NoError(err)

	// test some channel failed
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(s.channels, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, mock.Anything).Return(s.nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, mock.Anything).Return(s.nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
		return availableNodes[0], nil
	})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	counter := atomic.NewInt64(0)
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
			// succeed in first execute
			if counter.Add(1) == 1 {
				return nil
			}

			return mockErr
		},
	})
	s.Error(err)
	s.Equal(int64(6), counter.Load())

	// test get shard leader failed
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(nil, mockErr)
	err = s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		Exec: func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error {
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
	mgr := NewMockShardClientManager(s.T())
	policy := NewLBPolicyImpl(mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*shardclient.LookAsideBalancer")
	policy.Close()

	params := paramtable.Get()

	params.Save(params.ProxyCfg.ReplicaSelectionPolicy.Key, "round_robin")
	policy = NewLBPolicyImpl(mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*shardclient.RoundRobinBalancer")
	policy.Close()

	params.Save(params.ProxyCfg.ReplicaSelectionPolicy.Key, "look_aside")
	policy = NewLBPolicyImpl(mgr)
	s.Equal(reflect.TypeOf(policy.getBalancer()).String(), "*shardclient.LookAsideBalancer")
	policy.Close()
}

func (s *LBPolicySuite) TestGetShard() {
	ctx := context.Background()

	// ErrCollectionNotLoaded is not retriable, expected to fail fast
	counter := atomic.NewInt64(0)
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).RunAndReturn(
		func(ctx context.Context, withCache bool, database, collectionName string, collectionID int64, channel string) ([]NodeInfo, error) {
			counter.Inc()
			return nil, merr.ErrCollectionNotLoaded
		})
	_, err := s.lbPolicy.GetShard(ctx, s.dbName, s.collectionName, s.collectionID, s.channels[0], true)
	s.Error(err)
	s.Equal(int64(1), counter.Load())

	// Normal case - success
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	shardLeaders, err := s.lbPolicy.GetShard(ctx, s.dbName, s.collectionName, s.collectionID, s.channels[0], true)
	s.NoError(err)
	s.Equal(len(s.nodes), len(shardLeaders))
}

func (s *LBPolicySuite) TestSelectNodeEdgeCases() {
	ctx := context.Background()

	// Test case 1: Empty shard leaders after refresh, should fail gracefully
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return([]NodeInfo{}, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return([]NodeInfo{}, nil).Once()

	excludeNodes := typeutil.NewUniqueSet(s.nodeIDs...)
	_, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.Error(err)

	// Test case 2: Single replica scenario - exclude it, refresh shows same single replica, should clear and succeed
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	singleNode := []NodeInfo{{NodeID: 1, Address: "localhost:9000", Serviceable: true}}
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(singleNode, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(singleNode, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()

	excludeNodes = typeutil.NewUniqueSet(int64(1)) // Exclude the single node
	targetNode, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(1), targetNode.NodeID)
	s.Equal(0, excludeNodes.Len()) // Should be cleared

	// Test case 3: Mixed serviceable nodes - prefer serviceable over non-serviceable
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	mixedNodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: false},
		{NodeID: 3, Address: "localhost:9002", Serviceable: true},
	}
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(mixedNodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	// Should select from serviceable nodes only (node 3, since node 1 is excluded)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.MatchedBy(func(nodes []int64) bool {
		return len(nodes) == 1 && nodes[0] == 3 // Only node 3 is serviceable and not excluded
	}), mock.Anything).Return(int64(3), nil).Once()

	excludeNodes = typeutil.NewUniqueSet(int64(1)) // Exclude node 1, node 3 should be available
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(3), targetNode.NodeID)
	s.Equal(1, excludeNodes.Len()) // Should NOT be cleared as not all replicas were excluded
}

func (s *LBPolicySuite) TestGetShardLeaderList() {
	ctx := context.Background()

	// Test normal scenario with cache
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(s.channels, nil)
	channelList, err := s.lbPolicy.GetShardLeaderList(ctx, s.dbName, s.collectionName, s.collectionID, true)
	s.NoError(err)
	s.Equal(len(s.channels), len(channelList))
	s.Contains(channelList, s.channels[0])
	s.Contains(channelList, s.channels[1])

	// Test without cache - should refresh from coordinator
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, false).Return(s.channels, nil)
	channelList, err = s.lbPolicy.GetShardLeaderList(ctx, s.dbName, s.collectionName, s.collectionID, false)
	s.NoError(err)
	s.Equal(len(s.channels), len(channelList))

	// Test error case - collection not loaded
	counter := atomic.NewInt64(0)
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).RunAndReturn(
		func(ctx context.Context, database, collectionName string, collectionID int64, withCache bool) ([]string, error) {
			counter.Inc()
			return nil, merr.ErrCollectionNotLoaded
		})
	_, err = s.lbPolicy.GetShardLeaderList(ctx, s.dbName, s.collectionName, s.collectionID, true)
	s.Error(err)
	s.ErrorIs(err, merr.ErrCollectionNotLoaded)
	s.Equal(int64(1), counter.Load())
}

func (s *LBPolicySuite) TestSelectNodeWithExcludeClearing() {
	ctx := context.Background()

	// Test exclude nodes clearing when all replicas are excluded after cache refresh
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	twoNodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
	}
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(twoNodes, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(twoNodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()

	excludeNodes := typeutil.NewUniqueSet(int64(1), int64(2)) // Exclude all available nodes
	targetNode, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)

	s.NoError(err)
	s.Equal(int64(1), targetNode.NodeID)
	s.Equal(0, excludeNodes.Len()) // Should be cleared when all replicas were excluded

	// Test exclude nodes NOT cleared when only partial replicas are excluded
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	threeNodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
		{NodeID: 3, Address: "localhost:9002", Serviceable: true},
	}
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(threeNodes, nil).Once()
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil).Once()

	excludeNodes = typeutil.NewUniqueSet(int64(1)) // Only exclude node 1
	targetNode, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)

	s.NoError(err)
	s.Equal(int64(2), targetNode.NodeID)
	s.Equal(1, excludeNodes.Len()) // Should NOT be cleared as not all replicas were excluded

	// Test empty shard leaders scenario
	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return([]NodeInfo{}, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return([]NodeInfo{}, nil).Once()

	excludeNodes = typeutil.NewUniqueSet(int64(1))
	_, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)

	s.Error(err)
	s.Equal(1, excludeNodes.Len()) // Should NOT be cleared for empty shard leaders
}

func TestLBPolicySuite(t *testing.T) {
	suite.Run(t, new(LBPolicySuite))
}
