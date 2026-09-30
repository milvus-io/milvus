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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	targetNode, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrCollectionNotLoaded)
}

func (s *LBPolicySuite) TestPreferredNodeHint() {
	ctx := context.Background()

	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	excludeNodes := typeutil.NewUniqueSet()
	targetNode, selectedByBalancer, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:              s.dbName,
		CollectionName:  s.collectionName,
		CollectionID:    s.collectionID,
		Channel:         s.channels[0],
		Nq:              1,
		PreferredNodeID: 3,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(3), targetNode.NodeID)
	s.False(selectedByBalancer)
}

func (s *LBPolicySuite) TestPreferredNodeHintFallback() {
	ctx := context.Background()
	nodes := []NodeInfo{
		{NodeID: 1, Address: "localhost", Serviceable: true},
		{NodeID: 2, Address: "localhost", Serviceable: false},
		{NodeID: 3, Address: "localhost", Serviceable: true},
	}

	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(nodes, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.MatchedBy(func(nodes []int64) bool {
		return !lo.Contains(nodes, int64(2)) && lo.Contains(nodes, int64(1)) && lo.Contains(nodes, int64(3))
	}), int64(1)).Return(int64(1), nil)
	excludeNodes := typeutil.NewUniqueSet()
	targetNode, selectedByBalancer, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:              s.dbName,
		CollectionName:  s.collectionName,
		CollectionID:    s.collectionID,
		Channel:         s.channels[0],
		Nq:              1,
		PreferredNodeID: 2,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(1), targetNode.NodeID)
	s.True(selectedByBalancer)
}

func (s *LBPolicySuite) TestExecuteUsesPreferredNodeHint() {
	ctx := context.Background()

	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return([]string{s.channels[0]}, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.MatchedBy(func(node NodeInfo) bool {
		return node.NodeID == 3
	})).Return(s.qn, nil)

	var executedNodeID int64
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		PreferredNodes: map[string]int64{s.channels[0]: 3},
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executedNodeID = nodeID
			return nil
		},
	})
	s.NoError(err)
	s.Equal(int64(3), executedNodeID)
}

func (s *LBPolicySuite) TestPreferredNodeHintMetrics() {
	ctx := context.Background()
	collectionID := int64(99001)
	channel := "preferred-metric-channel-hit"
	before := testutil.ToFloat64(metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(
		metrics.PreferredNodeHitLabel,
	))

	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, collectionID, channel).Return(s.nodes, nil)
	excludeNodes := typeutil.NewUniqueSet()
	targetNode, selectedByBalancer, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:              s.dbName,
		CollectionName:  s.collectionName,
		CollectionID:    collectionID,
		Channel:         channel,
		Nq:              1,
		PreferredNodeID: 3,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(3), targetNode.NodeID)
	s.False(selectedByBalancer)

	after := testutil.ToFloat64(metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(
		metrics.PreferredNodeHitLabel,
	))
	s.Equal(float64(1), after-before)
}

func (s *LBPolicySuite) TestPreferredNodeHintMetricsDisabledForNormalWorkload() {
	ctx := context.Background()
	collectionID := int64(99002)
	channel := "preferred-metric-channel-disabled"
	before := testutil.ToFloat64(metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(
		metrics.PreferredNodeMissLabel,
	))

	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, collectionID, channel).Return(s.nodes, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, int64(1)).Return(int64(1), nil)
	excludeNodes := typeutil.NewUniqueSet()
	targetNode, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   collectionID,
		Channel:        channel,
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(1), targetNode.NodeID)

	after := testutil.ToFloat64(metrics.ProxyShardLeaderPreferredNodeCount.WithLabelValues(
		metrics.PreferredNodeMissLabel,
	))
	s.Equal(float64(0), after-before)
}

func (s *LBPolicySuite) TestPreferredNodeFailureFallsBackToOtherReplica() {
	ctx := context.Background()
	channel := "preferred-node-fallback-channel"
	nodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
	}
	s.lbPolicy.retryOnReplica = 1

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return([]string{channel}, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, []int64{int64(2)}, int64(1)).Return(int64(2), nil).Once()
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	executedNodes := make([]int64, 0, 2)
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		PreferredNodes: map[string]int64{channel: 1},
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executedNodes = append(executedNodes, nodeID)
			if nodeID == 1 {
				return merr.ErrServiceUnavailable
			}
			return nil
		},
	})
	s.NoError(err)
	s.Equal([]int64{1, 2}, executedNodes)
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

func (s *LBPolicySuite) TestExecuteWithRetryFieldNotLoadedUsesRequestLevelRetry() {
	ctx := context.Background()
	channel := s.channels[0]
	nodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
	}
	s.lbPolicy.retryOnReplica = 2

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
			return availableNodes[0], nil
		})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	executedNodes := make([]int64, 0, 3)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        channel,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executedNodes = append(executedNodes, nodeID)
			if len(executedNodes) <= len(nodes) {
				// C++ FieldNotLoaded(2027) is projected through merr as a
				// retriable system error. It must exclude the node only for this
				// request, never add the healthy node to the cross-request channel
				// blacklist.
				return merr.SegcoreError(2027, "bloom_match field is not loaded")
			}
			return nil
		},
	})

	s.NoError(err)
	s.Len(executedNodes, 3)
	s.NotEqual(executedNodes[0], executedNodes[1])
	s.Empty(s.lbPolicy.blacklist.GetBlacklistedNodes(channel))
}

func (s *LBPolicySuite) TestExecuteWithRetryRetriableErrorRetriesAfterAllReplicasFail() {
	ctx := context.Background()
	channel := s.channels[0]
	nodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
	}
	s.lbPolicy.retryOnReplica = 2

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
			return availableNodes[0], nil
		})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	executedNodes := make([]int64, 0, 4)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        channel,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executedNodes = append(executedNodes, nodeID)
			if len(executedNodes) <= len(nodes)+1 {
				return errors.Wrapf(merr.ErrServiceUnavailable, "fail on QueryNode %d", nodeID)
			}
			return nil
		},
	})

	s.NoError(err)
	s.Len(executedNodes, 4)
	s.ElementsMatch([]int64{int64(1), int64(2)}, executedNodes[:2])
	s.NotEqual(executedNodes[2], executedNodes[3])
	s.Empty(s.lbPolicy.blacklist.GetBlacklistedNodes(channel))
}

func (s *LBPolicySuite) TestExecuteWithRetryRetriableErrorRefreshesStaleShardLeaderCache() {
	ctx := context.Background()
	channel := s.channels[0]
	cachedNodes := []NodeInfo{{NodeID: 1, Address: "localhost:9000", Serviceable: true}}
	freshNodes := []NodeInfo{
		{NodeID: 1, Address: "localhost:9000", Serviceable: true},
		{NodeID: 2, Address: "localhost:9001", Serviceable: true},
	}
	shardLeaders := cachedNodes
	s.lbPolicy.retryOnReplica = 2

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).RunAndReturn(
		func(context.Context, bool, string, string, int64, string) ([]NodeInfo, error) {
			return shardLeaders, nil
		})
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).RunAndReturn(
		func(context.Context, bool, string, string, int64, string) ([]NodeInfo, error) {
			shardLeaders = freshNodes
			return shardLeaders, nil
		}).Once()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, availableNodes []int64, nq int64) (int64, error) {
			return availableNodes[0], nil
		})
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	executedNodes := make([]int64, 0, 2)
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        channel,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executedNodes = append(executedNodes, nodeID)
			if nodeID == cachedNodes[0].NodeID {
				return errors.Wrapf(merr.ErrServiceUnavailable, "fail on QueryNode %d", nodeID)
			}
			return nil
		},
	})

	s.NoError(err)
	s.Equal([]int64{1, 2}, executedNodes)
	s.Empty(s.lbPolicy.blacklist.GetBlacklistedNodes(channel))
}

func (s *LBPolicySuite) TestExecuteWithRetryNonRetriableErrorUsesBlacklist() {
	ctx := context.Background()
	channel := s.channels[0]
	nodes := []NodeInfo{{NodeID: 1, Address: "localhost:9000", Serviceable: true}}
	s.lbPolicy.retryOnReplica = 1

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        channel,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			return errors.Wrapf(merr.ErrServiceInternal, "fail on QueryNode %d", nodeID)
		},
	})

	s.Error(err)
	s.Contains(s.lbPolicy.blacklist.GetBlacklistedNodes(channel), int64(1))
}

// TestExecuteWithRetryInputErrorSkipsBlacklist verifies that an input error
// (the request's own fault) does not blacklist the serving node nor get retried
// across replicas.
func (s *LBPolicySuite) TestExecuteWithRetryInputErrorSkipsBlacklist() {
	ctx := context.Background()
	channel := s.channels[0]
	nodes := []NodeInfo{{NodeID: 1, Address: "localhost:9000", Serviceable: true}}
	s.lbPolicy.retryOnReplica = 3

	s.mgr.ExpectedCalls = nil
	s.lbBalancer.ExpectedCalls = nil
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, channel).Return(nodes, nil).Maybe()
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	execCount := 0
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        channel,
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			execCount++
			return errors.Wrapf(merr.ErrParameterInvalid, "bad request on QueryNode %d", nodeID)
		},
	})

	s.Error(err)
	s.ErrorIs(err, merr.ErrParameterInvalid)
	// not retried across replicas despite retryOnReplica=3
	s.Equal(1, execCount)
	// serving node not blacklisted for the request's own fault
	s.NotContains(s.lbPolicy.blacklist.GetBlacklistedNodes(channel), int64(1))
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
	s.lbPolicy.retryOnReplica = 1
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
	s.Equal(int64(7), counter.Load())

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
	_, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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
	_, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
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

// rgNodes is a fixture of five leaders on one channel spanning two resource
// groups plus one whose tag is unknown, as an old coordinator would leave it.
func rgNodes() []NodeInfo {
	return []NodeInfo{
		{NodeID: 1, Address: "localhost", Serviceable: true, ResourceGroup: "rg-a"},
		{NodeID: 2, Address: "localhost", Serviceable: true, ResourceGroup: "rg-b"},
		{NodeID: 3, Address: "localhost", Serviceable: true, ResourceGroup: "rg-b"},
		{NodeID: 4, Address: "localhost", Serviceable: true, ResourceGroup: ""},
		{NodeID: 5, Address: "localhost", Serviceable: true, ResourceGroup: "rg-a"},
	}
}

// TestSelectNodeScopedToResourceGroup pins that a scoped workload builds its
// candidate set from the leaders of that group alone: the balancer is
// registered with, and asked to choose among, exactly those, and an unknown
// tag is not admitted. An unscoped workload on the same leaders still sees
// all five, which is the pre-existing behavior.
func (s *LBPolicySuite) TestSelectNodeScopedToResourceGroup() {
	ctx := context.Background()
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(rgNodes(), nil)

	onlyRGB := func(nodes []NodeInfo) bool {
		return len(nodes) == 2 && lo.EveryBy(nodes, func(n NodeInfo) bool { return n.ResourceGroup == "rg-b" })
	}
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.MatchedBy(onlyRGB)).Once()
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.MatchedBy(func(ids []int64) bool {
		return len(ids) == 2 && lo.Contains(ids, int64(2)) && lo.Contains(ids, int64(3))
	}), mock.Anything).Return(int64(3), nil).Once()

	excludeNodes := typeutil.NewUniqueSet()
	targetNode, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(3), targetNode.NodeID)
	s.Equal("rg-b", targetNode.ResourceGroup)

	// Unscoped: the same leaders, all five are candidates.
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.MatchedBy(func(nodes []NodeInfo) bool { return len(nodes) == 5 })).Once()
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.MatchedBy(func(ids []int64) bool { return len(ids) == 5 }), mock.Anything).Return(int64(4), nil).Once()
	targetNode, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
	}, &excludeNodes)
	s.NoError(err)
	s.Equal(int64(4), targetNode.NodeID)
}

// TestSelectNodeScopedWithoutLeaderIsRetriable pins the error a scoped
// request gets when its group has no leader on the channel: the group holds a
// replica whose delegator is not serviceable yet, a state that heals in
// seconds to minutes, so it must be ErrCollectionNotFullyLoaded (retriable)
// and not ErrChannelNotAvailable (503, non-retriable), which would tell every
// upper layer to stop. The cache is refreshed once before giving up, so a
// stale cache is not what produces the refusal. Unknown tags (an old
// coordinator) get the same answer: they are not admitted into a named group.
func (s *LBPolicySuite) TestSelectNodeScopedWithoutLeaderIsRetriable() {
	ctx := context.Background()
	onlyRGA := lo.Filter(rgNodes(), func(n NodeInfo, _ int) bool { return n.ResourceGroup == "rg-a" })
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(onlyRGA, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(onlyRGA, nil).Once()

	excludeNodes := typeutil.NewUniqueSet()
	_, _, err := s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrCollectionNotFullyLoaded)
	s.True(merr.IsRetryableErr(err), "a group still coming up must be reported as retriable")
	s.NotErrorIs(err, merr.ErrChannelNotAvailable)

	// An old coordinator leaves every tag unknown; a named scope must not
	// admit those, and the answer is the same retriable refusal.
	untagged := lo.Map(rgNodes(), func(n NodeInfo, _ int) NodeInfo { n.ResourceGroup = ""; return n })
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(untagged, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, false, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(untagged, nil).Once()
	_, _, err = s.lbPolicy.selectNode(ctx, s.lbBalancer, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
	}, &excludeNodes)
	s.ErrorIs(err, merr.ErrCollectionNotFullyLoaded)
}

// TestExecuteScopedFanOutCoversEveryShard pins the constraint the resource
// group scope rests on: the fan-out is the UNSCOPED channel list, so a shard
// the group cannot serve is still visited and fails its channel with a
// retriable error. Filtering the channel map instead would make this a
// successful query over a subset of the shards -- Execute never cross-checks
// the channel count against the collection's shard number, so nothing would
// report it. Here rg-b leads channel1 only; channel2 must fail, not vanish.
func (s *LBPolicySuite) TestExecuteScopedFanOutCoversEveryShard() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1 // keep the doomed channel's retry budget short

	channel1Leaders := rgNodes()
	channel2Leaders := lo.Filter(rgNodes(), func(n NodeInfo, _ int) bool { return n.ResourceGroup == "rg-a" })
	s.mgr.EXPECT().GetShardLeaderList(mock.Anything, s.dbName, s.collectionName, s.collectionID, true).Return(s.channels, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(channel1Leaders, nil)
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[1]).Return(channel2Leaders, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	executed := typeutil.NewConcurrentSet[string]()
	err := s.lbPolicy.Execute(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		ResourceGroup:  "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			s.Equal(int64(2), nodeID, "only an rg-b leader may execute")
			executed.Insert(channel)
			return nil
		},
	})

	s.ErrorIs(err, merr.ErrCollectionNotFullyLoaded, "the shard rg-b cannot serve must fail the request, not be skipped")
	s.True(merr.IsRetryableErr(err))
	s.True(executed.Contain(s.channels[0]), "the shard rg-b can serve is executed")
	s.False(executed.Contain(s.channels[1]), "the shard rg-b cannot serve is never executed on another group's leader")
	s.mgr.AssertCalled(s.T(), "GetShard", mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[1])
}

// TestCollectionWorkLoadForChannelCarriesScope pins the one way a
// ChannelWorkload may be built from a CollectionWorkLoad: every
// collection-level field, the resource-group scope included, is carried, and
// the preferred node is whatever the caller resolved. This is what keeps the
// namespace single-shard fast paths from silently running unscoped.
func (s *LBPolicySuite) TestCollectionWorkLoadForChannelCarriesScope() {
	exec := func(ctx context.Context, ui UniqueID, qn types.QueryNodeClient, channel string) error { return nil }
	collection := CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             7,
		Exec:           exec,
		ResourceGroup:  "rg-b",
		PreferredNodes: map[string]int64{s.channels[0]: 3},
	}

	got := collection.ForChannel(s.channels[0], 42)

	s.Equal(s.dbName, got.Db)
	s.Equal(s.collectionName, got.CollectionName)
	s.Equal(s.collectionID, got.CollectionID)
	s.Equal(s.channels[0], got.Channel)
	s.EqualValues(7, got.Nq)
	s.NotNil(got.Exec)
	s.Equal("rg-b", got.ResourceGroup, "the scope must reach the per-channel workload")
	s.EqualValues(42, got.PreferredNodeID, "the preferred node is the caller's, not derived")

	s.Equal("", CollectionWorkLoad{}.ForChannel("c", 0).ResourceGroup, "no scope stays no scope")
}

// TestExecuteWithRetryScopedRecoversAfterGroupExhausted pins the request-level
// exclusion recovery under a scope. Both rg-b leaders fail with a retriable
// error and are excluded; the group is then exhausted while the channel still
// has unexcluded rg-a leaders, so the "every leader excluded" recovery has to
// be judged on the scoped set or it never fires. The third attempt must land
// on an rg-b leader again and succeed -- never on an rg-a one.
func (s *LBPolicySuite) TestExecuteWithRetryScopedRecoversAfterGroupExhausted() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(rgNodes(), nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)
	// The balancer picks the first offered rg-b leader each time; the
	// exclusion set decides which ones are offered.
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, ids []int64, nq int64) (int64, error) {
			s.Subset([]int64{2, 3}, ids, "only rg-b leaders may be offered")
			s.NotEmpty(ids)
			return lo.Min(ids), nil
		})

	var attempts []int64
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			attempts = append(attempts, nodeID)
			if len(attempts) <= 2 {
				return merr.ErrServiceUnavailable // retriable: excludes the node for this request
			}
			return nil
		},
	})

	s.NoError(err)
	s.Equal([]int64{2, 3, 2}, attempts, "both rg-b leaders excluded, then the exclusion cleared and rg-b tried again")
}

// TestExecuteWithRetryScopedRetriableSelectErrorIsNotMasked pins the ordering
// fix on the retry loop: the group's only leader fails with a NON-retriable
// error and then disappears from the channel, so the next selection is the
// retriable ErrCollectionNotFullyLoaded. The request must end on that, not
// on the earlier exec error -- reporting the non-retriable one would tell
// the layer waiting for the group to stop, undoing the code in exactly the
// case it was added for.
func (s *LBPolicySuite) TestExecuteWithRetryScopedRetriableSelectErrorIsNotMasked() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	withLeader := lo.Filter(rgNodes(), func(n NodeInfo, _ int) bool { return n.NodeID == 1 || n.NodeID == 2 })
	withoutLeader := lo.Filter(rgNodes(), func(n NodeInfo, _ int) bool { return n.NodeID == 1 })
	// The retry-budget read plus the first selection see the rg-b leader;
	// every read after that sees it gone.
	s.mgr.EXPECT().GetShard(mock.Anything, true, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(withLeader, nil).Times(2)
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(withoutLeader, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	execErr := merr.WrapErrSegmentNotLoaded(1, "not retriable")
	s.Require().False(merr.IsRetryableErr(execErr))
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			return execErr
		},
	})

	s.ErrorIs(err, merr.ErrCollectionNotFullyLoaded, "the fresh retriable refusal must win over the stale non-retriable exec error")
	s.True(merr.IsRetryableErr(err))
}

// TestExecuteOneChannelScopedPrefersAServableChannel pins that a scoped
// ExecuteOneChannel does not hand back the group's refusal for the first
// channel in map order when a sibling channel could have served: channel1 has
// no rg-b leader, channel2 does, and the request must succeed on channel2.
// The pre-pass reads ONE fresh copy of the whole leader table (a single
// uncached GetShardLeaders), so the unservable channel is never tried for
// real -- a per-channel GetShard on it fails the mock. Unscoped keeps its
// take-the-first behavior.
func (s *LBPolicySuite) TestExecuteOneChannelScopedPrefersAServableChannel() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	onlyRGA := lo.Filter(rgNodes(), func(n NodeInfo, _ int) bool { return n.ResourceGroup == "rg-a" })
	// No GetShardLeaderList expectation on purpose: the scoped path must not
	// read the cached channel list at all (on a cold cache that would be a
	// second coordinator RPC on top of the pre-pass's one), and the mock
	// fails on an unexpected call.
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(map[string][]NodeInfo{s.channels[0]: onlyRGA, s.channels[1]: rgNodes()}, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[1]).Return(rgNodes(), nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	var executed []string
	err := s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Nq:             1,
		ResourceGroup:  "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executed = append(executed, channel)
			return nil
		},
	})

	s.NoError(err)
	s.Equal([]string{s.channels[1]}, executed, "the channel rg-b cannot serve is skipped for the one it can")
	s.mgr.AssertNotCalled(s.T(), "GetShard", mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0])

	// The scoped channel list is derived from the FRESH table, not from the
	// cached channel list: a channel the fresh table knows and the cache does
	// not is still tried, and one the cache lists but the fresh table lacks
	// is not.
	s.mgr.ExpectedCalls = nil
	s.mgr.Calls = nil
	executed = nil
	// A stale cached list naming "stale-only" would be read by an unscoped
	// request; the scoped one never asks for it (unexpected call = failure).
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(map[string][]NodeInfo{"fresh-only": rgNodes()}, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, "fresh-only").Return(rgNodes(), nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	err = s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db: s.dbName, CollectionName: s.collectionName, CollectionID: s.collectionID, Nq: 1, ResourceGroup: "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			executed = append(executed, channel)
			return nil
		},
	})
	s.NoError(err)
	s.Equal([]string{"fresh-only"}, executed, "the fresh table, not the cached key set, decides what is tried")
	s.mgr.AssertNotCalled(s.T(), "GetShardLeaderList", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

	// When the FRESH table shows no channel the group can serve, the refusal
	// comes back immediately -- zero retry budgets, no per-channel attempt --
	// and it is the same retriable code a full sweep would have ended on.
	// The read was uncached, which is what entitles it to refuse.
	s.mgr.ExpectedCalls = nil
	s.mgr.Calls = nil // the AssertNotCalled below must not see the first half's legitimate GetShard
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(map[string][]NodeInfo{s.channels[0]: onlyRGA, s.channels[1]: onlyRGA}, nil).Once()
	err = s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db: s.dbName, CollectionName: s.collectionName, CollectionID: s.collectionID, Nq: 1, ResourceGroup: "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error { return nil },
	})
	s.ErrorIs(err, merr.ErrCollectionNotFullyLoaded)
	s.True(merr.IsRetryableErr(err))
	s.mgr.AssertNotCalled(s.T(), "GetShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestExecuteWithRetryUnscopedKeepsTheExecError pins the unscoped half of the
// masking rule, so the scope gate on it cannot be removed without a red test.
// The sequence is the ordinary one: an exec attempt fails with a non-retriable
// error, and on the next attempt the balancer finds every remaining candidate
// unreachable and answers a RETRIABLE ErrServiceUnavailable. Unscoped, the
// request must still end on the exec error -- it names the cause, and it is
// what the request ended on before the scope existed. Only the scoped form
// lets a fresh retriable refusal win.
func (s *LBPolicySuite) TestExecuteWithRetryUnscopedKeepsTheExecError() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(s.nodes, nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(1), nil).Once()
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), merr.WrapErrServiceUnavailable("all available nodes are unreachable"))
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	execErr := merr.WrapErrSegmentNotLoaded(1, "not retriable")
	s.Require().False(merr.IsRetryableErr(execErr))
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			return execErr
		},
	})

	s.ErrorIs(err, merr.ErrSegmentNotLoaded, "unscoped, the exec error still wins over a later retriable selection error")
	s.False(merr.IsRetryableErr(err))
	s.NotErrorIs(err, merr.ErrServiceUnavailable)
}

// TestExecuteOneChannelScopedPrePassRetriesTransientErrors pins that the
// pre-pass read goes through the same retry wrapper as the other two manager
// reads: a transient coordinator error (ErrServiceNotReady during a restart)
// is retried until it clears, while ErrCollectionNotLoaded is not retried --
// exactly GetShard's and GetShardLeaderList's policy. Calling the manager
// directly would fail the whole scoped request on the first blip, which is
// backwards for the path whose contract is "hand the caller a state it can
// poll through".
func (s *LBPolicySuite) TestExecuteOneChannelScopedPrePassRetriesTransientErrors() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(nil, merr.WrapErrServiceNotReadyMsg("coordinator restarting")).Once()
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(map[string][]NodeInfo{s.channels[0]: rgNodes()}, nil).Once()
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(rgNodes(), nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil)
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	err := s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db: s.dbName, CollectionName: s.collectionName, CollectionID: s.collectionID, Nq: 1, ResourceGroup: "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error { return nil },
	})
	s.NoError(err, "a transient coordinator error on the pre-pass read is retried through, not surfaced")

	// The one error the wrapper does not retry.
	s.mgr.ExpectedCalls = nil
	s.mgr.EXPECT().GetShardLeaders(mock.Anything, false, s.dbName, s.collectionName, s.collectionID).
		Return(nil, merr.WrapErrCollectionNotLoaded(s.collectionID)).Once()
	err = s.lbPolicy.ExecuteOneChannel(ctx, CollectionWorkLoad{
		Db: s.dbName, CollectionName: s.collectionName, CollectionID: s.collectionID, Nq: 1, ResourceGroup: "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error { return nil },
	})
	s.ErrorIs(err, merr.ErrCollectionNotLoaded, "collection-not-loaded is not retried, matching the sibling reads")
}

// TestExecuteWithRetryScopedKeepsTheExecErrorOnBalancerFailure is the scoped
// counterpart of TestExecuteWithRetryUnscopedKeepsTheExecError: the masking
// exception is for the empty-scoped-set refusal only, not for any retriable
// selection error. The group's leader fails with a non-retriable exec error
// and is blacklisted; the scoped candidate set is still non-empty (a second
// rg-b leader), so the refusal does not fire, and the balancer reports every
// remaining candidate unreachable with a RETRIABLE ErrServiceUnavailable. A
// rule keyed on "any retriable error" would end the request on that and
// discard the terminal cause; the request must still end on the exec error.
func (s *LBPolicySuite) TestExecuteWithRetryScopedKeepsTheExecErrorOnBalancerFailure() {
	ctx := context.Background()
	s.lbPolicy.retryOnReplica = 1
	s.mgr.EXPECT().GetShard(mock.Anything, mock.Anything, s.dbName, s.collectionName, s.collectionID, s.channels[0]).Return(rgNodes(), nil)
	s.mgr.EXPECT().GetClient(mock.Anything, mock.Anything).Return(s.qn, nil)
	s.lbBalancer.EXPECT().RegisterNodeInfo(mock.Anything)
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(2), nil).Once()
	s.lbBalancer.EXPECT().SelectNode(mock.Anything, mock.Anything, mock.Anything).Return(int64(-1), merr.WrapErrServiceUnavailable("all available nodes are unreachable"))
	s.lbBalancer.EXPECT().CancelWorkload(mock.Anything, mock.Anything)

	execErr := merr.WrapErrSegmentNotLoaded(1, "not retriable")
	s.Require().False(merr.IsRetryableErr(execErr))
	err := s.lbPolicy.ExecuteWithRetry(ctx, ChannelWorkload{
		Db:             s.dbName,
		CollectionName: s.collectionName,
		CollectionID:   s.collectionID,
		Channel:        s.channels[0],
		Nq:             1,
		ResourceGroup:  "rg-b",
		Exec: func(ctx context.Context, nodeID UniqueID, qn types.QueryNodeClient, channel string) error {
			return execErr
		},
	})

	s.ErrorIs(err, merr.ErrSegmentNotLoaded, "scoped, a balancer-side retriable error must not mask the terminal exec error")
	s.False(merr.IsRetryableErr(err))
	s.NotErrorIs(err, merr.ErrServiceUnavailable)
}
