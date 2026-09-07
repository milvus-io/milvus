// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	sqnShard      = "by-dev-rootcoord-dml_0_100v0"
	sqnGroup      = "rg-streaming"
	sqnCollection = int64(100)
	sqnStreaming  = int64(7)  // the streaming node's embedded query node
	sqnRegular    = int64(11) // a regular query node
)

// formHook is the smallest thing a distribution can install: the placement
// only asks whether a hook is there, never what it does.
type formHook struct{ hook.Hook }

// setForm makes this test's binary one a distribution compiled itself into,
// or a stock one, and restores a stock binary when the test ends.
func setForm(t *testing.T, installed bool) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	if installed {
		ext.SetHook(formHook{})
	}
}

// sqnFixture is the smallest coordinator state the streaming-node placement
// reads: a resource manager that knows the replica's group, a node manager,
// and a distribution.
type sqnFixture struct {
	meta    *meta.Meta
	nodeMgr *session.NodeManager
	dist    *meta.DistributionManager
}

func newSQNFixture(t *testing.T) *sqnFixture {
	t.Helper()
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr)
	return &sqnFixture{meta: m, nodeMgr: nodeMgr, dist: meta.NewDistributionManager(nodeMgr)}
}

// addGroup registers the replica's resource group asking for `requests`
// regular query nodes. A group built to run on a streaming node alone asks
// for none; a group that runs regular query nodes asks for as many as it has.
func (f *sqnFixture) addGroup(t *testing.T, requests int32) {
	t.Helper()
	_, err := f.meta.AddResourceGroup(context.Background(), sqnGroup, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: requests},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: requests},
	})
	require.NoError(t, err)
}

// addRegularNode brings a regular query node up; the resource manager hands
// it to the group that is missing one.
func (f *sqnFixture) addRegularNode(t *testing.T, nodeID int64) {
	t.Helper()
	f.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID, Address: "localhost", Hostname: "localhost"}))
	f.meta.HandleNodeUp(context.Background(), nodeID)
	require.True(t, f.meta.ContainsNode(context.Background(), sqnGroup, nodeID), "the node must land in the group under test")
}

// replica builds a replica of the group with the given regular and streaming
// query nodes.
func (f *sqnFixture) replica(regular, streaming []int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID: 1, CollectionID: sqnCollection, ResourceGroup: sqnGroup, Nodes: regular, RwSqNodes: streaming,
	})
}

// firstNodePolicy is an assignment policy that places every segment on the
// first node it is offered and records the nodes it was offered. What is
// under test is which nodes reach the policy, not what it does with them.
func firstNodePolicy(t *testing.T, offered *[]int64) assign.AssignPolicy {
	t.Helper()
	policy := assign.NewMockAssignPolicy(t)
	policy.EXPECT().
		AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, segments []*meta.Segment, nodes []int64, _ bool) []assign.SegmentAssignPlan {
			*offered = nodes
			if len(nodes) == 0 {
				return nil
			}
			plans := make([]assign.SegmentAssignPlan, 0, len(segments))
			for _, segment := range segments {
				plans = append(plans, assign.SegmentAssignPlan{Segment: segment, From: -1, To: nodes[0]})
			}
			return plans
		}).Maybe()
	return policy
}

// sealedSegmentPlacement runs createSegmentLoadTasks against replica with one
// sealed segment to place, and reports the nodes the assignment policy was
// offered and the tasks that came out.
func (f *sqnFixture) sealedSegmentPlacement(t *testing.T, replica *meta.Replica, streaming bool) (offered []int64, tasks []task.Task) {
	t.Helper()
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(streaming).Build()
	defer enabled.UnPatch()
	// A shard leader must exist, or the loop skips before it picks nodes.
	leader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(&meta.DmChannel{}).Build()
	defer leader.UnPatch()

	c := &SegmentChecker{
		meta:         f.meta,
		dist:         f.dist,
		assignPolicy: firstNodePolicy(t, &offered),
	}
	tasks = c.createSegmentLoadTasks(context.Background(),
		[]*datapb.SegmentInfo{{ID: 1, CollectionID: sqnCollection, InsertChannel: sqnShard}},
		[]commonpb.LoadPriority{commonpb.LoadPriority_HIGH},
		replica)
	return offered, tasks
}

// A replica whose resource group's only compute is a streaming node has no
// regular query node at all - milvus keeps the query node embedded in a
// streaming node out of the resource manager - and the group asks for none.
// For an installed form, its sealed segments must still have somewhere to go,
// or the load is accepted and never converges: the segment is placed on the
// streaming node's query node.
func TestSealedSegmentsReachAStreamingQueryNodeWhenThereIsNoOther(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 0)
	replica := f.replica(nil, []int64{sqnStreaming})
	require.Empty(t, replica.GetRWNodes(), "the case under test is a replica with no regular node")

	offered, tasks := f.sealedSegmentPlacement(t, replica, true)
	assert.Equal(t, []int64{sqnStreaming}, offered,
		"the group's streaming query node is the only compute the replica has")
	require.Len(t, tasks, 1, "the sealed segment must be placed, not silently dropped")
	require.Len(t, tasks[0].Actions(), 1)
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node(), "and placed on the streaming node's query node")
	assert.EqualValues(t, 1, tasks[0].(*task.SegmentTask).SegmentID())
}

// A group the resource manager does not know cannot be asked for its regular
// nodes; the replica's own node sets are the only evidence, and they say the
// streaming node is all there is.
func TestSealedSegmentsReachAStreamingQueryNodeOfAnUnknownGroup(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	require.False(t, f.meta.ContainResourceGroup(context.Background(), sqnGroup))

	offered, _ := f.sealedSegmentPlacement(t, f.replica(nil, []int64{sqnStreaming}), true)
	assert.Equal(t, []int64{sqnStreaming}, offered)
}

// The reviewer's failure: a MIXED group - a regular query node next to the
// streaming node - whose regular node restarts. The node leaves the resource
// group and the replica at once, so for the length of the restart the
// replica's RW set is empty exactly as a streaming-only replica's is. The
// group still asks for its regular node, and that is what tells the two
// apart: the sealed segments belong on it and wait for it, as they do on
// master, rather than being loaded onto the streaming node's query node.
func TestSealedSegmentsWaitForARegularNodeThatIsRestarting(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	replica := f.replica(nil, []int64{sqnStreaming})
	require.Empty(t, replica.GetRWNodes(), "the regular node is away")
	rg := f.meta.GetResourceGroup(context.Background(), sqnGroup)
	require.Zero(t, rg.NodeNum(), "and the group has no regular node while it is away")

	offered, tasks := f.sealedSegmentPlacement(t, replica, true)
	assert.Empty(t, offered, "the streaming node's query node must not be offered while a regular node is expected")
	assert.Empty(t, tasks)
}

// The same, one observer tick earlier: the regular node is back in the
// resource group but the replica observer has not handed it to the replica
// yet. The group holds a regular node, so the sealed segments wait for it.
func TestSealedSegmentsWaitForARegularNodeTheReplicaHasNotReceived(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	replica := f.replica(nil, []int64{sqnStreaming})

	offered, tasks := f.sealedSegmentPlacement(t, replica, true)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}

// A stock binary keeps the empty candidate set it always had: a replica with
// no regular node produces no plan and no task, and nothing lands on a
// streaming node's query node, where the balancers would never find it again.
func TestAStockBinaryPlacesNoSealedSegmentOnAStreamingQueryNode(t *testing.T) {
	setForm(t, false)
	f := newSQNFixture(t)
	f.addGroup(t, 0)

	offered, tasks := f.sealedSegmentPlacement(t, f.replica(nil, []int64{sqnStreaming}), true)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}

// With a regular query node present, nothing changes even for a form: sealed
// segments stay off the streaming node, which is the split milvus intends.
func TestSealedSegmentsStayOffTheStreamingNodeWhenARegularOneExists(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)

	offered, tasks := f.sealedSegmentPlacement(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), true)
	assert.Equal(t, []int64{sqnRegular}, offered)
	require.Len(t, tasks, 1)
	assert.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node())
}

// With the streaming service off there are no streaming query nodes to fall
// back to, and the empty candidate set is what it always was.
func TestSealedSegmentCandidatesAreUnchangedWithTheStreamingServiceOff(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 0)

	offered, _ := f.sealedSegmentPlacement(t, f.replica(nil, []int64{sqnStreaming}), true)
	require.NotEmpty(t, offered, "sanity: with the service on the streaming node is offered")

	offered, tasks := f.sealedSegmentPlacement(t, f.replica(nil, []int64{sqnStreaming}), false)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}

// checkReplicaWithASealedSegmentOnTheStreamingNode runs a full checkReplica
// round for a replica whose one target segment is resident on the streaming
// node's query node, and returns the tasks the round produced. The target
// holds exactly that segment, so nothing is lacking and nothing is redundant:
// whatever comes out is about where the segment sits.
func (f *sqnFixture) checkReplicaWithASealedSegmentOnTheStreamingNode(t *testing.T, replica *meta.Replica, leader *meta.DmChannel) []task.Task {
	t.Helper()
	ctx := context.Background()
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	defer enabled.UnPatch()
	shardLeader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(leader).Build()
	defer shardLeader.UnPatch()

	require.NoError(t, f.meta.PutCollection(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: sqnCollection, ReplicaNumber: 1},
	}))
	segment := &datapb.SegmentInfo{ID: 1, CollectionID: sqnCollection, PartitionID: 10, InsertChannel: sqnShard, NumOfRows: 1}
	f.dist.SegmentDistManager.Update(sqnStreaming, &meta.Segment{SegmentInfo: segment, Node: sqnStreaming, Version: 1})

	targets := meta.NewMockTargetManager(t)
	targets.EXPECT().IsNextTargetExist(mock.Anything, sqnCollection).Return(true).Maybe()
	targets.EXPECT().IsCurrentTargetExist(mock.Anything, sqnCollection, mock.Anything).Return(false).Maybe()
	targets.EXPECT().GetSealedSegmentsByCollection(mock.Anything, sqnCollection, meta.CurrentTarget).Return(nil).Maybe()
	targets.EXPECT().GetSealedSegmentsByCollection(mock.Anything, sqnCollection, mock.Anything).
		Return(map[int64]*datapb.SegmentInfo{segment.GetID(): segment}).Maybe()
	targets.EXPECT().GetCollectionTargetVersion(mock.Anything, sqnCollection, mock.Anything).Return(int64(1)).Maybe()

	var offered []int64
	c := &SegmentChecker{
		meta:         f.meta,
		dist:         f.dist,
		targetMgr:    targets,
		assignPolicy: firstNodePolicy(t, &offered),
	}
	return c.checkReplica(ctx, replica)
}

// The other half of the reviewer's failure: once a regular node is back, a
// sealed segment resident on the streaming node's query node is misplaced,
// and nothing on master moves it - the balancers walk regular nodes only.
// The checker moves it itself: one task that loads the segment on the regular
// node and releases it from the streaming node, so the replica never serves
// from neither.
func TestSealedSegmentsMisplacedOnAStreamingQueryNodeMoveToARegularNode(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	replica := f.replica([]int64{sqnRegular}, []int64{sqnStreaming})

	tasks := f.checkReplicaWithASealedSegmentOnTheStreamingNode(t, replica, &meta.DmChannel{})
	require.Len(t, tasks, 1, "exactly one task, for the one misplaced segment")
	move := tasks[0]
	assert.Equal(t, task.TaskTypeMove, task.GetTaskType(move), "a move, not a bare release: the replica keeps serving the segment throughout")
	assert.EqualValues(t, 1, move.(*task.SegmentTask).SegmentID())
	nodesByAction := make(map[task.ActionType]int64)
	for _, action := range move.Actions() {
		nodesByAction[action.Type()] = action.Node()
	}
	assert.EqualValues(t, sqnRegular, nodesByAction[task.ActionTypeGrow], "loaded on the regular node")
	assert.EqualValues(t, sqnStreaming, nodesByAction[task.ActionTypeReduce], "released from the streaming node's query node")
}

// While the group has no regular node the segment is where it must be, and
// nothing moves it.
func TestSealedSegmentsStayOnTheStreamingQueryNodeWhileThereIsNoRegularNode(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 0)

	tasks := f.checkReplicaWithASealedSegmentOnTheStreamingNode(t, f.replica(nil, []int64{sqnStreaming}), &meta.DmChannel{})
	assert.Empty(t, tasks)
}

// A move needs the shard's delegator, exactly as a load does; without one the
// segment waits on the streaming node rather than being released into nowhere.
func TestMisplacedSealedSegmentsWaitForAShardLeader(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)

	tasks := f.checkReplicaWithASealedSegmentOnTheStreamingNode(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), nil)
	assert.Empty(t, tasks)
}

// A stock binary never placed a sealed segment on a streaming node's query
// node and does not start moving them either: its checker round is exactly
// what it was.
func TestAStockBinaryLeavesSealedSegmentsOnAStreamingQueryNodeAlone(t *testing.T) {
	setForm(t, false)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)

	tasks := f.checkReplicaWithASealedSegmentOnTheStreamingNode(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), &meta.DmChannel{})
	assert.Empty(t, tasks)
}
