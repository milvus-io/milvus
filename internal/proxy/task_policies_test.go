package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/stretchr/testify/require"
)

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

				leaders := &querypb.ShardLeadersList{
					ChannelName: t.Name(),
					NodeIds:     test.leaderIDs,
					NodeAddrs:   make([]string, len(test.leaderIDs)),
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
			leaders := &querypb.ShardLeadersList{
				ChannelName: t.Name(),
				NodeIds:     test.leaderIDs,
				NodeAddrs:   make([]string, len(test.leaderIDs)),
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
			leaders := &querypb.ShardLeadersList{
				ChannelName: t.Name(),
				NodeIds:     test.leaderIDs,
				NodeAddrs:   make([]string, len(test.leaderIDs)),
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
