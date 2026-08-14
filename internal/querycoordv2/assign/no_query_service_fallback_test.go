package assign

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// withServingStreamingNodes makes the streaming node manager report the given
// query serving nodes, grouped as the manager groups them.
func withServingStreamingNodes(t *testing.T, byRG map[string][]int64) {
	grouped := make(map[string]typeutil.UniqueSet, len(byRG))
	for rg, ids := range byRG {
		grouped[rg] = typeutil.NewUniqueSet(ids...)
	}
	patch := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDsByResourceGroup).
		Return(grouped).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func withStreamingService(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	t.Cleanup(streamingutil.UnsetStreamingServiceEnabled)
}

// With the streaming service off nothing is filtered, as before.
func TestFilterSQNKeepsEveryNodeWhenStreamingIsOff(t *testing.T) {
	streamingutil.UnsetStreamingServiceEnabled()
	withServingStreamingNodes(t, map[string][]int64{"rg_a": {1}})

	assert.Equal(t, []int64{7, 8}, filterSQNIfStreamingServiceEnabled([]int64{7, 8}))
}

// The native path: streaming query nodes exist, so only they may carry a
// delegator and everything else is dropped.
func TestFilterSQNKeepsOnlyServingStreamingNodes(t *testing.T) {
	withStreamingService(t)
	withServingStreamingNodes(t, map[string][]int64{"rg_a": {1, 2}})

	assert.Equal(t, []int64{1, 2}, filterSQNIfStreamingServiceEnabled([]int64{1, 2, 7}))
}

// No streaming node serves queries, so the candidates are regular query nodes.
// Filtering would leave the channel with nowhere to go.
func TestFilterSQNFallsBackWhenNoStreamingNodeServesQueries(t *testing.T) {
	withStreamingService(t)
	withServingStreamingNodes(t, map[string][]int64{})

	assert.Equal(t, []int64{7, 8}, filterSQNIfStreamingServiceEnabled([]int64{7, 8}))
}

// A node the grouping left out is not a delegator candidate even when the
// caller offers it, as long as some other node does serve queries.
func TestFilterSQNDropsANodeTheGroupingLeftOut(t *testing.T) {
	withStreamingService(t)
	withServingStreamingNodes(t, map[string][]int64{"rg_a": {2}})

	assert.Equal(t, []int64{2}, filterSQNIfStreamingServiceEnabled([]int64{1, 2}))
}

// QueryServingStreamingNodes flattens across resource groups: a delegator may
// go on a serving node of any group the caller offers.
func TestQueryServingStreamingNodesFlattensGroups(t *testing.T) {
	withServingStreamingNodes(t, map[string][]int64{"rg_a": {1}, "rg_b": {2, 3}})

	assert.ElementsMatch(t, []int64{1, 2, 3}, QueryServingStreamingNodes().Collect())
}
