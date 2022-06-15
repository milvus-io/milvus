package proxy

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/mock"

	"go.uber.org/zap"
)

func TestUpdateShardsWithRoundRobin(t *testing.T) {
	list := map[string][]nodeInfo{
		"channel-1": {
			{1, "addr1"},
			{2, "addr2"},
		},
		"channel-2": {
			{20, "addr20"},
			{21, "addr21"},
		},
	}

	updateShardsWithRoundRobin(list)

	assert.Equal(t, int64(2), list["channel-1"][0].nodeID)
	assert.Equal(t, "addr2", list["channel-1"][0].address)
	assert.Equal(t, int64(21), list["channel-2"][0].nodeID)
	assert.Equal(t, "addr21", list["channel-2"][0].address)

	t.Run("check print", func(t *testing.T) {
		qns := []nodeInfo{
			{1, "addr1"},
			{2, "addr2"},
			{20, "addr20"},
			{21, "addr21"},
		}

		res := fmt.Sprintf("list: %v", qns)

		log.Debug("Check String func",
			zap.Any("Any", qns),
			zap.Any("ok", qns[0]),
			zap.String("ok2", res),
		)

	})
}

func TestRoundRobinPolicy(t *testing.T) {
	var (
		ctx = context.TODO()
	)

	mockCreator := func(ctx context.Context, addr string) (types.QueryNode, error) {
		return &mock.QueryNodeClient{}, nil
	}

	mgr := newShardClientMgr(withShardClientCreator(mockCreator))
	dummyLeaders := genShardLeaderInfo("c1", []UniqueID{-1, 1, 2, 3})
	mgr.UpdateShardLeaders(nil, dummyLeaders)
	t.Run("All fails", func(t *testing.T) {
		allFailTests := []struct {
			leaderIDs []UniqueID

			description string
		}{
			{[]UniqueID{1}, "one invalid shard leader"},
			{[]UniqueID{1, 2}, "two invalid shard leaders"},
			{[]UniqueID{1, 1}, "two invalid same shard leaders"},
		}

		for _, test := range allFailTests {
			t.Run(test.description, func(t *testing.T) {
				query := (&mockQuery{isvalid: false}).query

				leaders := make([]nodeInfo, 0, len(test.leaderIDs))
				for _, ID := range test.leaderIDs {
					leaders = append(leaders, nodeInfo{ID, "random-addr"})

				}

				err := roundRobinPolicy(ctx, mgr, query, leaders)
				require.Error(t, err)
			})
		}
	})

	t.Run("Pass at the first try", func(t *testing.T) {
		allPassTests := []struct {
			leaderIDs []UniqueID

			description string
		}{
			{[]UniqueID{1}, "one valid shard leader"},
			{[]UniqueID{1, 2}, "two valid shard leaders"},
			{[]UniqueID{1, 1}, "two valid same shard leaders"},
		}

		for _, test := range allPassTests {
			query := (&mockQuery{isvalid: true}).query
			leaders := make([]nodeInfo, 0, len(test.leaderIDs))
			for _, ID := range test.leaderIDs {
				leaders = append(leaders, nodeInfo{ID, "random-addr"})

			}
			err := roundRobinPolicy(ctx, mgr, query, leaders)
			require.NoError(t, err)
		}
	})

	t.Run("Pass at the second try", func(t *testing.T) {
		passAtLast := []struct {
			leaderIDs []UniqueID

			description string
		}{
			{[]UniqueID{-1, 2}, "invalid vs valid shard leaders"},
			{[]UniqueID{-1, -1, 3}, "invalid, invalid, and valid shard leaders"},
		}

		for _, test := range passAtLast {
			query := (&mockQuery{isvalid: true}).query
			leaders := make([]nodeInfo, 0, len(test.leaderIDs))
			for _, ID := range test.leaderIDs {
				leaders = append(leaders, nodeInfo{ID, "random-addr"})

			}
			err := roundRobinPolicy(ctx, mgr, query, leaders)
			require.NoError(t, err)
		}
	})
}

func TestGroupShardLeadersWithSameQueryNode(t *testing.T) {
	var err error

	Params.Init()
	var (
		ctx = context.TODO()
	)

	mgr := newShardClientMgr()

	shard2leaders := map[string][]nodeInfo{
		"c0": {{nodeID: 0, address: "fake"}, {nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}},
		"c1": {{nodeID: 1, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c2": {{nodeID: 0, address: "fake"}, {nodeID: 2, address: "fake"}, {nodeID: 3, address: "fake"}},
		"c3": {{nodeID: 1, address: "fake"}, {nodeID: 3, address: "fake"}, {nodeID: 4, address: "fake"}},
	}
	mgr.UpdateShardLeaders(nil, shard2leaders)

	node2Dmls, node2QN, err := groupShardLeadersWithSameQueryNode(ctx, roundRobinPolicy, mgr, shard2leaders)
	require.NoError(t, err)
	for nodeID := range node2Dmls {
		sort.Slice(node2Dmls[nodeID], func(i, j int) bool { return node2Dmls[nodeID][i] < node2Dmls[nodeID][j] })
	}
	assert.Equal(t, node2Dmls, map[int64][]string{0: {"c0", "c2"}, 1: {"c1", "c3"}})

	cli0, err := mgr.GetClient(ctx, 0)
	require.NoError(t, err)
	cli1, err := mgr.GetClient(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, node2QN, map[int64]types.QueryNode{0: cli0, 1: cli1})
}

func mockQueryNodeCreator(ctx context.Context, address string) (types.QueryNode, error) {
	return &QueryNodeMock{address: address}, nil
}

type mockQuery struct {
	isvalid bool
}

func (m *mockQuery) query(nodeID UniqueID, qn types.QueryNode) error {
	if nodeID == -1 {
		return fmt.Errorf("error at condition")
	}

	if m.isvalid {
		return nil
	}

	return fmt.Errorf("mock error in query, NodeID=%d", nodeID)
}
