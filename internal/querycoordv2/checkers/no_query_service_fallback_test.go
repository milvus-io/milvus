package checkers

import (
	"context"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// replicaWithSQNodes builds a replica whose read-write nodes and streaming
// query nodes are stated separately, which is what the fallback turns on.
func replicaWithSQNodes(id, collectionID int64, nodes, sqNodes []int64) *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{
			ID:            id,
			CollectionID:  collectionID,
			Nodes:         nodes,
			RwSqNodes:     sqNodes,
			ResourceGroup: meta.DefaultResourceGroupName,
		},
		typeutil.NewUniqueSet(nodes...),
	)
}

// A replica with no streaming query node must still get its delegator placed:
// on a regular read-write node, which is where the delegator goes with the
// streaming service off. Without the fallback the channel has nowhere to go and
// no task is ever produced, so the collection never becomes queryable.
func (suite *ChannelCheckerTestSuite) TestChannelFallsBackWhenNoStreamingQueryNode() {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()
	suite.withServingStreamingNodes(map[string][]int64{})
	// The fallback keys on the DECLARATION, not on the set being empty.
	suite.withNoQueryServiceResourceGroups(meta.DefaultResourceGroupName)

	action := suite.channelGrowActionFor(replicaWithSQNodes(1, 1, []int64{1}, nil))

	suite.EqualValues(1, action.Node(), "the delegator must land on the read-write query node")
}

// An empty streaming-query-node set WITHOUT the no-query declaration is a
// streaming node mid-restart, not a no-query resource group. Placing the
// delegator on a regular query node then would strand it there - nothing
// migrates it back when the streaming node returns - so the checker must do
// what it always did in that window: nothing.
func (suite *ChannelCheckerTestSuite) TestChannelDoesNotFallBackInATransientEmptyWindow() {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()
	suite.withServingStreamingNodes(map[string][]int64{})
	suite.withNoQueryServiceResourceGroups() // nothing declared

	suite.Empty(suite.channelGrowActionsFor(replicaWithSQNodes(1, 1, []int64{1}, nil)),
		"a transient empty window must produce no plan, or the delegator is stranded on a regular query node")
}

// The native path is untouched: a replica that does have a streaming query node
// puts the delegator there, not on its regular nodes.
func (suite *ChannelCheckerTestSuite) TestChannelKeepsStreamingQueryNodeWhenThereIsOne() {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()
	suite.withServingStreamingNodes(map[string][]int64{meta.DefaultResourceGroupName: {2}})

	action := suite.channelGrowActionFor(replicaWithSQNodes(1, 1, []int64{1}, []int64{2}))

	suite.EqualValues(2, action.Node(), "the delegator must stay on the streaming query node")
}

// withServingStreamingNodes makes the streaming node manager report the given
// query serving nodes for the rest of the test.
func (suite *ChannelCheckerTestSuite) withServingStreamingNodes(byRG map[string][]int64) {
	grouped := make(map[string]typeutil.UniqueSet, len(byRG))
	for rg, ids := range byRG {
		grouped[rg] = typeutil.NewUniqueSet(ids...)
	}
	patch := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDsByResourceGroup).
		Return(grouped).Build()
	suite.T().Cleanup(func() { patch.UnPatch() })

	// With the streaming service on, channel assignment first asks where each
	// channel's write ahead log lives, and that call blocks until an assignment
	// arrives - which never happens against the test balancer. Answer with a
	// node no replica holds, so every channel falls through to the regular
	// policy, which is what the assertions below are about.
	walPatch := mockey.Mock((*snmanager.StreamingNodeManager).GetWALLocated).Return(int64(-1)).Build()
	suite.T().Cleanup(func() { walPatch.UnPatch() })
}

// withNoQueryServiceResourceGroups makes the streaming node manager declare
// exactly these resource groups as serving no queries.
func (suite *ChannelCheckerTestSuite) withNoQueryServiceResourceGroups(rgs ...string) {
	declared := typeutil.NewSet(rgs...)
	patch := mockey.Mock((*snmanager.StreamingNodeManager).NoQueryServiceResourceGroups).
		Return(declared).Build()
	suite.T().Cleanup(func() { patch.UnPatch() })
}

// channelGrowActionFor runs the checker over one collection holding one channel
// and returns the single channel grow action it produced.
func (suite *ChannelCheckerTestSuite) channelGrowActionFor(replica *meta.Replica) *task.ChannelAction {
	ctx := context.Background()
	checker := suite.checker
	checker.meta.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.Put(ctx, replica)
	for _, nodeID := range append(replica.GetNodes(), replica.GetRWSQNodes()...) {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "localhost",
			Hostname: "localhost",
		}))
	}
	checker.meta.HandleNodeUp(ctx, replica.GetNodes()[0])

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: "test-insert-channel"}}, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	tasks := checker.Check(ctx)
	suite.Require().Len(tasks, 1, "the channel must be assigned somewhere")
	suite.Require().Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.ChannelAction)
	suite.Require().True(ok)
	suite.Equal(task.ActionTypeGrow, action.Type())
	return action
}

// channelGrowActionsFor is channelGrowActionFor without the one-task
// requirement, for the cases whose point is that NO plan is produced.
func (suite *ChannelCheckerTestSuite) channelGrowActionsFor(replica *meta.Replica) []task.Task {
	ctx := context.Background()
	checker := suite.checker
	checker.meta.PutCollection(ctx, utils.CreateTestCollection(1, 1))
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1))
	checker.meta.Put(ctx, replica)
	for _, nodeID := range append(replica.GetNodes(), replica.GetRWSQNodes()...) {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "localhost",
			Hostname: "localhost",
		}))
	}
	checker.meta.HandleNodeUp(ctx, replica.GetNodes()[0])

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(
		[]*datapb.VchannelInfo{{CollectionID: 1, ChannelName: "test-insert-channel"}}, nil, nil)
	checker.targetMgr.UpdateCollectionNextTarget(ctx, int64(1))

	return checker.Check(ctx)
}
