package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
)

func TestUpdateShardsWithRoundRobin(t *testing.T) {
	list := map[string][]queryNode{
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
		qns := []queryNode{
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
		getQueryNodePolicy = mockGetQueryNodePolicy
		ctx                = context.TODO()
	)

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

				leaders := make([]queryNode, 0, len(test.leaderIDs))
				for _, ID := range test.leaderIDs {
					leaders = append(leaders, queryNode{ID, "random-addr"})

				}

				err := roundRobinPolicy(ctx, getQueryNodePolicy, query, leaders)
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
			leaders := make([]queryNode, 0, len(test.leaderIDs))
			for _, ID := range test.leaderIDs {
				leaders = append(leaders, queryNode{ID, "random-addr"})

			}
			err := roundRobinPolicy(ctx, getQueryNodePolicy, query, leaders)
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
			leaders := make([]queryNode, 0, len(test.leaderIDs))
			for _, ID := range test.leaderIDs {
				leaders = append(leaders, queryNode{ID, "random-addr"})

			}
			err := roundRobinPolicy(ctx, getQueryNodePolicy, query, leaders)
			require.NoError(t, err)
		}
	})
}

func mockGetQueryNodePolicy(ctx context.Context, address string) (types.QueryNode, error) {
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
