package snmanager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// withStreamingNodes makes the balancer report exactly the given nodes for the
// duration of the test.
func withStreamingNodes(t *testing.T, nodes map[int64]*types.StreamingNodeInfoWithResourceGroup) {
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().GetAvailableStreamingNodes(mock.Anything).Return(nodes, nil).Maybe()
	// The manager starts a background watcher; block it rather than let it
	// churn, so the assertions below see only the fixture above.
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ balancer.WatchChannelAssignmentsCallback) error {
			<-ctx.Done()
			return ctx.Err()
		}).Maybe()
	b.EXPECT().RegisterStreamingEnabledNotifier(mock.Anything).Return().Maybe()

	patch := mockey.Mock(balance.GetWithContext).Return(balancer.Balancer(b), nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func node(id int64, rg string, noQuery bool) *types.StreamingNodeInfoWithResourceGroup {
	return &types.StreamingNodeInfoWithResourceGroup{
		StreamingNodeInfo: types.StreamingNodeInfo{ServerID: id},
		ResourceGroup:     rg,
		NoQueryService:    noQuery,
	}
}

// A streaming node that does not serve queries must not reach the grouping
// that feeds a replica's streaming query nodes, because that is the only thing
// a shard delegator is ever placed from.
func TestNoQueryServiceNodeIsNotGroupedByResourceGroup(t *testing.T) {
	withStreamingNodes(t, map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: node(1, "rg_a", true),
		2: node(2, "rg_a", false),
	})
	m := newStreamingNodeManager()

	byRG := m.GetStreamingQueryNodeIDsByResourceGroup()

	assert.Len(t, byRG, 1)
	assert.ElementsMatch(t, []int64{2}, byRG["rg_a"].Collect())
}

// A resource group whose only streaming node serves no queries must not appear
// at all: an empty entry would read as "this group has streaming query nodes"
// to a caller that only checks for the key.
func TestResourceGroupWithOnlyNoQueryServiceNodesIsAbsent(t *testing.T) {
	withStreamingNodes(t, map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: node(1, "rg_a", true),
	})
	m := newStreamingNodeManager()

	byRG := m.GetStreamingQueryNodeIDsByResourceGroup()

	assert.Empty(t, byRG)
}

// The ungrouped set answers "which nodes own a write ahead log", so it keeps
// the node the grouping leaves out. Callers that count streaming nodes before
// admitting a load, and the analyzer dispatch, depend on this.
func TestNoQueryServiceNodeIsStillAStreamingNode(t *testing.T) {
	withStreamingNodes(t, map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: node(1, "rg_a", true),
	})
	m := newStreamingNodeManager()

	assert.ElementsMatch(t, []int64{1}, m.GetStreamingQueryNodeIDs().Collect())
}

// Nothing declares the field unless a deployment labels its streaming node, so
// a stock binary groups exactly the nodes it always did.
func TestUnlabelledNodesAreGroupedAsBefore(t *testing.T) {
	withStreamingNodes(t, map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: node(1, "rg_a", false),
		2: node(2, "rg_b", false),
	})
	m := newStreamingNodeManager()

	byRG := m.GetStreamingQueryNodeIDsByResourceGroup()

	assert.Len(t, byRG, 2)
	assert.ElementsMatch(t, []int64{1}, byRG["rg_a"].Collect())
	assert.ElementsMatch(t, []int64{2}, byRG["rg_b"].Collect())
	assert.ElementsMatch(t, []int64{1, 2}, m.GetStreamingQueryNodeIDs().Collect())
}

// NoQueryServiceResourceGroups is the positive signal the checkers' fallback
// keys on: a group appears only when it has streaming nodes and none serves
// queries. A group with a mix, a group with only serving nodes, and a group
// the balancer has never reported all stay out.
func TestNoQueryServiceResourceGroupsIsAPositiveDeclaration(t *testing.T) {
	withStreamingNodes(t, map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: node(1, "rg_declared", true),
		2: node(2, "rg_mixed", true),
		3: node(3, "rg_mixed", false),
		4: node(4, "rg_serving", false),
	})

	declared := StaticStreamingNodeManager.NoQueryServiceResourceGroups()
	assert.True(t, declared.Contain("rg_declared"))
	assert.False(t, declared.Contain("rg_mixed"),
		"one serving node means the group's delegators belong on streaming nodes")
	assert.False(t, declared.Contain("rg_serving"))
	assert.False(t, declared.Contain("rg_unknown"),
		"a group with no streaming nodes at all is a restart window, not a declaration")
}
