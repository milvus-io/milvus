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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	sqnShard      = "by-dev-rootcoord-dml_0_100v0"
	sqnGroup      = "rg-streaming"
	sqnCollection = int64(100)
	sqnReplica    = int64(1)
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

// sqnFixture is one segment checker over the smallest coordinator state the
// streaming-node placement reads: a resource manager that knows the replica's
// group, a node manager, a distribution, and a target holding exactly one
// sealed segment. The checker persists across calls, as it does across check
// rounds, because what it remembers about a replica is under test.
type sqnFixture struct {
	meta    *meta.Meta
	nodeMgr *session.NodeManager
	dist    *meta.DistributionManager
	segment *datapb.SegmentInfo

	checker *SegmentChecker
	// offered is the node set the assignment policy was last given.
	offered []int64
}

func newSQNFixture(t *testing.T) *sqnFixture {
	t.Helper()
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ReleaseReplica(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	nodeMgr := session.NewNodeManager()
	f := &sqnFixture{
		meta:    meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr),
		nodeMgr: nodeMgr,
		dist:    meta.NewDistributionManager(nodeMgr),
		segment: &datapb.SegmentInfo{ID: 1, CollectionID: sqnCollection, PartitionID: 10, InsertChannel: sqnShard, NumOfRows: 1},
	}
	require.NoError(t, f.meta.PutCollection(context.Background(), &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: sqnCollection, ReplicaNumber: 1},
	}))

	// The target holds the one segment, in the next target only: nothing is
	// redundant, and the segment is lacking exactly when it is not in dist.
	targets := meta.NewMockTargetManager(t)
	targets.EXPECT().IsNextTargetExist(mock.Anything, sqnCollection).Return(true).Maybe()
	targets.EXPECT().IsCurrentTargetExist(mock.Anything, sqnCollection, mock.Anything).Return(false).Maybe()
	targets.EXPECT().GetSealedSegmentsByCollection(mock.Anything, sqnCollection, meta.CurrentTarget).Return(nil).Maybe()
	targets.EXPECT().GetSealedSegmentsByCollection(mock.Anything, sqnCollection, mock.Anything).
		Return(map[int64]*datapb.SegmentInfo{f.segment.GetID(): f.segment}).Maybe()
	targets.EXPECT().GetCollectionTargetVersion(mock.Anything, sqnCollection, mock.Anything).Return(int64(1)).Maybe()

	f.checker = &SegmentChecker{
		meta:                     f.meta,
		dist:                     f.dist,
		targetMgr:                targets,
		assignPolicy:             f.firstNodePolicy(t),
		replicasWithRegularNodes: typeutil.NewUniqueSet(),
	}
	return f
}

// firstNodePolicy is an assignment policy that places every segment on the
// first node it is offered and records the nodes it was offered. What is
// under test is which nodes reach the policy, not what it does with them.
func (f *sqnFixture) firstNodePolicy(t *testing.T) assign.AssignPolicy {
	t.Helper()
	policy := assign.NewMockAssignPolicy(t)
	policy.EXPECT().
		AssignSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ int64, segments []*meta.Segment, nodes []int64, _ bool) []assign.SegmentAssignPlan {
			f.offered = nodes
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

// addGroup registers the replica's resource group asking for `nodes` regular
// query nodes, requests and limits alike, which is the shape a form's running
// query cluster has: it asks for its replica count while its compute is
// streaming nodes alone.
func (f *sqnFixture) addGroup(t *testing.T, nodes int32) {
	t.Helper()
	_, err := f.meta.AddResourceGroup(context.Background(), sqnGroup, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: nodes},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: nodes},
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

// replica builds the replica of the group with the given regular and
// streaming query nodes.
func (f *sqnFixture) replica(regular, streaming []int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{
		ID: sqnReplica, CollectionID: sqnCollection, ResourceGroup: sqnGroup, Nodes: regular, RwSqNodes: streaming,
	})
}

// putSealedSegmentOn makes the fixture's segment resident on nodeID.
func (f *sqnFixture) putSealedSegmentOn(nodeID int64) {
	f.dist.SegmentDistManager.Update(nodeID, &meta.Segment{SegmentInfo: f.segment, Node: nodeID, Version: 1})
}

// placeSealedSegment runs createSegmentLoadTasks for the fixture's segment
// against replica, and returns the nodes the policy was offered and the
// tasks that came out.
func (f *sqnFixture) placeSealedSegment(t *testing.T, replica *meta.Replica, streaming bool) (offered []int64, tasks []task.Task) {
	t.Helper()
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(streaming).Build()
	defer enabled.UnPatch()
	// A shard leader must exist, or the loop skips before it picks nodes.
	leader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(&meta.DmChannel{}).Build()
	defer leader.UnPatch()

	f.offered = nil
	tasks = f.checker.createSegmentLoadTasks(context.Background(),
		[]*datapb.SegmentInfo{f.segment},
		[]commonpb.LoadPriority{commonpb.LoadPriority_HIGH},
		replica)
	return f.offered, tasks
}

// check runs a full checkReplica round for replica, with the streaming
// service on and the shard's delegator as given, and returns its tasks.
func (f *sqnFixture) check(t *testing.T, replica *meta.Replica, leader *meta.DmChannel) []task.Task {
	t.Helper()
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	defer enabled.UnPatch()
	shardLeader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(leader).Build()
	defer shardLeader.UnPatch()

	f.offered = nil
	return f.checker.checkReplica(context.Background(), replica)
}

// A replica whose resource group's only compute is a streaming node has no
// regular query node at all - milvus keeps the query node embedded in a
// streaming node out of the resource manager - and never had one. For an
// installed form, its sealed segments must still have somewhere to go, or the
// load is accepted and never converges: the segment is placed on the
// streaming node's query node. The group is shaped as a form's running query
// cluster is, asking for its replica count; that count says nothing about
// regular nodes and must not turn the placement off.
func TestSealedSegmentsReachAStreamingQueryNodeWhenThereIsNoOther(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 2)
	replica := f.replica(nil, []int64{sqnStreaming})
	require.Empty(t, replica.GetRWNodes(), "the case under test is a replica with no regular node")
	require.Positive(t, f.meta.GetResourceGroup(context.Background(), sqnGroup).MissingNumOfNodes(),
		"and a group that asks for regular nodes it does not have")

	offered, tasks := f.placeSealedSegment(t, replica, true)
	assert.Equal(t, []int64{sqnStreaming}, offered,
		"the group's streaming query node is the only compute the replica has")
	require.Len(t, tasks, 1, "the sealed segment must be placed, not silently dropped")
	require.Len(t, tasks[0].Actions(), 1)
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node(), "and placed on the streaming node's query node")
	assert.EqualValues(t, 1, tasks[0].(*task.SegmentTask).SegmentID())
}

// The reviewer's failure: a MIXED group - a regular query node next to the
// streaming node - whose regular node restarts. The node leaves the resource
// group and the replica at once, so for the length of the restart the
// replica reads exactly like a streaming-only one. What tells the two apart
// is that this replica has been seen with a regular node: its sealed
// segments belong on one and wait for it, as they do on master, rather than
// being loaded onto the streaming node's query node.
func TestSealedSegmentsWaitForARegularNodeThatIsRestarting(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)

	// One check round with the regular node present.
	f.check(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), &meta.DmChannel{})
	assert.Equal(t, []int64{sqnRegular}, f.offered, "sanity: the lacking segment goes to the regular node")

	// The regular node restarts: gone from the group and from the replica.
	f.meta.HandleNodeDown(context.Background(), sqnRegular)
	away := f.replica(nil, []int64{sqnStreaming})
	require.Empty(t, away.GetRWNodes())

	offered, tasks := f.placeSealedSegment(t, away, true)
	assert.Empty(t, offered, "the streaming node's query node must not be offered while the regular node is away")
	assert.Empty(t, tasks)
	tasks = f.check(t, away, &meta.DmChannel{})
	assert.Empty(t, tasks, "and a full check round places nothing either")
}

// The bounded case: after a coordinator restart the checker's memory is
// empty, so a mixed group whose regular node is down at that moment gets one
// placement on the streaming node's query node. Once the regular node is
// back, the move pass brings the segment onto it.
func TestAFreshCheckerPlacesOnceOnTheStreamingNodeAndThenDrains(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	away := f.replica(nil, []int64{sqnStreaming})

	tasks := f.check(t, away, &meta.DmChannel{})
	require.Len(t, tasks, 1, "a checker that has never seen the regular node places on the streaming node")
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node())
	f.putSealedSegmentOn(sqnStreaming)

	f.addRegularNode(t, sqnRegular)
	tasks = f.check(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), &meta.DmChannel{})
	require.Len(t, tasks, 1, "with the regular node back, the one task is the move")
	assert.Equal(t, task.TaskTypeMove, task.GetTaskType(tasks[0]))
	nodesByAction := make(map[task.ActionType]int64)
	for _, action := range tasks[0].Actions() {
		nodesByAction[action.Type()] = action.Node()
	}
	assert.EqualValues(t, sqnRegular, nodesByAction[task.ActionTypeGrow])
	assert.EqualValues(t, sqnStreaming, nodesByAction[task.ActionTypeReduce])

	// And from now on the replica is known to have a regular node: a later
	// restart of it waits.
	f.meta.HandleNodeDown(context.Background(), sqnRegular)
	offered, tasks := f.placeSealedSegment(t, away, true)
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

	offered, tasks := f.placeSealedSegment(t, f.replica(nil, []int64{sqnStreaming}), true)
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

	offered, tasks := f.placeSealedSegment(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), true)
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

	offered, _ := f.placeSealedSegment(t, f.replica(nil, []int64{sqnStreaming}), true)
	require.NotEmpty(t, offered, "sanity: with the service on the streaming node is offered")

	offered, tasks := f.placeSealedSegment(t, f.replica(nil, []int64{sqnStreaming}), false)
	assert.Empty(t, offered)
	assert.Empty(t, tasks)
}

// The record a checker keeps of a replica's regular node goes with the
// replica: a released replica's ID is not carried forever.
func TestTheRegularNodeRecordIsForgottenWithTheReplica(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	ctx := context.Background()

	replica := f.replica([]int64{sqnRegular}, []int64{sqnStreaming})
	require.NoError(t, f.meta.Put(ctx, replica))
	f.check(t, replica, &meta.DmChannel{})
	require.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))

	f.checker.forgetReleasedReplicas(ctx)
	assert.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica), "a replica that exists keeps its record")

	require.NoError(t, f.meta.RemoveReplicas(ctx, sqnCollection, sqnReplica))
	f.checker.forgetReleasedReplicas(ctx)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica), "a released replica's record goes with it")
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
	f.putSealedSegmentOn(sqnStreaming)

	tasks := f.check(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), &meta.DmChannel{})
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
	f.putSealedSegmentOn(sqnStreaming)

	tasks := f.check(t, f.replica(nil, []int64{sqnStreaming}), &meta.DmChannel{})
	assert.Empty(t, tasks)
}

// A move needs the shard's delegator, exactly as a load does; without one the
// segment waits on the streaming node rather than being released into nowhere.
func TestMisplacedSealedSegmentsWaitForAShardLeader(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	f.putSealedSegmentOn(sqnStreaming)

	tasks := f.check(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), nil)
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
	f.putSealedSegmentOn(sqnStreaming)

	tasks := f.check(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}), &meta.DmChannel{})
	assert.Empty(t, tasks)
}
