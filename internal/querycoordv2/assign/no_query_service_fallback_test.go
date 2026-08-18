package assign

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
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

// A batch with not one serving streaming node in it is a checker's deliberate
// fallback for a no-query resource group; dropping it because some OTHER
// resource group has serving streaming nodes would leave the no-query
// replica's channels unassignable forever. A mixed batch keeps the native
// filtering.
func TestFilterSQNPassesAnAllRegularFallbackBatch(t *testing.T) {
	withStreamingService(t)
	withServingStreamingNodes(t, map[string][]int64{"rg_a": {2}})

	assert.Equal(t, []int64{7, 8}, filterSQNIfStreamingServiceEnabled([]int64{7, 8}),
		"an all-regular batch must pass even while rg_a has serving streaming nodes")
	assert.Equal(t, []int64{2}, filterSQNIfStreamingServiceEnabled([]int64{2, 7}),
		"a mixed batch keeps the native filtering")
}

// ResourceGroupServesNoQueries is the three-way delegator-placement decision.
func TestResourceGroupServesNoQueriesThreeWays(t *testing.T) {
	withAllStreamingNodes(t, map[string][]bool{
		"rg_declared": {true, true},
		"rg_mixed":    {true, false},
	})

	assert.True(t, ResourceGroupServesNoQueries("rg_declared"),
		"all streaming nodes declaring no-query-service is the positive signal")
	assert.False(t, ResourceGroupServesNoQueries("rg_mixed"),
		"one serving streaming node keeps native placement, mid-restart windows included")

	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	assert.False(t, ResourceGroupServesNoQueries("rg_absent"),
		"a stock binary keeps its native reading: no streaming nodes, no delegators")

	require.NoError(t, extension.SetProvider(engineOnlyProvider{}))
	assert.True(t, ResourceGroupServesNoQueries("rg_absent"),
		"a form whose engine manages streaming-node-less query clusters places delegators on regular query nodes there")
}

// withAllStreamingNodes reports groups with their nodes' no-query declarations.
func withAllStreamingNodes(t *testing.T, byRG map[string][]bool) {
	saw := typeutil.NewSet[string]()
	declared := typeutil.NewSet[string]()
	for rg, flags := range byRG {
		saw.Insert(rg)
		all := true
		for _, noQuery := range flags {
			if !noQuery {
				all = false
			}
		}
		if all && len(flags) > 0 {
			declared.Insert(rg)
		}
	}
	p1 := mockey.Mock((*snmanager.StreamingNodeManager).StreamingNodeResourceGroups).Return(saw).Build()
	p2 := mockey.Mock((*snmanager.StreamingNodeManager).NoQueryServiceResourceGroups).Return(declared).Build()
	t.Cleanup(func() { p1.UnPatch(); p2.UnPatch() })
}

type engineOnlyProvider struct{}

func (engineOnlyProvider) Name() string                       { return "test" }
func (engineOnlyProvider) Requires() []extension.CapabilityID { return nil }
func (engineOnlyProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{CoordinatorEngine: fallbackTestEngine{}}
}

type fallbackTestEngine struct{}

func (fallbackTestEngine) RegisterOnCoordinator(grpc.ServiceRegistrar)     {}
func (fallbackTestEngine) Start(context.Context, extension.MixCoord) error { return nil }
func (fallbackTestEngine) Stop() error                                     { return nil }
