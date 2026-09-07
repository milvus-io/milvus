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
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
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

	// Groups an operator moves things into, for the transfer cases.
	sqnStreamingOnlyGroup = "rg-streaming-only"
	sqnOtherGroup         = "rg-other"
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
	// scheduled is what the last full check round handed the scheduler.
	scheduled []task.Task
	// targetVersion is the version the target manager reports for the
	// collection's next target; a test bumps it when a new sealed segment
	// enters the target.
	targetVersion int64
}

func newSQNFixture(t *testing.T) *sqnFixture {
	t.Helper()
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	// A transfer between groups saves both in one call.
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
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
	f.targetVersion = 1
	targets.EXPECT().GetCollectionTargetVersion(mock.Anything, sqnCollection, mock.Anything).
		RunAndReturn(func(context.Context, int64, int32) int64 { return f.targetVersion }).Maybe()

	f.checker = &SegmentChecker{
		checkerActivation:        newCheckerActivation(),
		meta:                     f.meta,
		dist:                     f.dist,
		targetMgr:                targets,
		nodeMgr:                  nodeMgr,
		scheduler:                f.recordingScheduler(t),
		assignPolicy:             f.firstNodePolicy(t),
		versionCache:             make(map[int64]*collectionVersionCache),
		replicasWithRegularNodes: typeutil.NewUniqueSet(),
	}
	return f
}

// recordingScheduler is a scheduler that keeps every task a check round hands
// it, which is how a full round's load tasks are read: Check hands them over
// as they are made and returns only its own release tasks.
func (f *sqnFixture) recordingScheduler(t *testing.T) task.Scheduler {
	t.Helper()
	scheduler := task.NewMockScheduler(t)
	scheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(added task.Task) error {
		f.scheduled = append(f.scheduled, added)
		return nil
	}).Maybe()
	return scheduler
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
	f.addNamedGroup(t, sqnGroup, nodes)
}

// addNamedGroup is addGroup for any group name.
func (f *sqnFixture) addNamedGroup(t *testing.T, name string, nodes int32) {
	t.Helper()
	_, err := f.meta.AddResourceGroup(context.Background(), name, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: nodes},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: nodes},
	})
	require.NoError(t, err)
}

// addRegularNode brings a regular query node up; the resource manager hands
// it to the group that is missing one.
func (f *sqnFixture) addRegularNode(t *testing.T, nodeID int64) {
	t.Helper()
	f.addRegularNodeTo(t, nodeID, sqnGroup)
}

// addRegularNodeTo is addRegularNode asserting which group the node lands in.
func (f *sqnFixture) addRegularNodeTo(t *testing.T, nodeID int64, group string) {
	t.Helper()
	f.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID, Address: "localhost", Hostname: "localhost"}))
	f.meta.HandleNodeUp(context.Background(), nodeID)
	require.True(t, f.meta.ContainsNode(context.Background(), group, nodeID), "the node must land in the group under test")
}

// removeRegularNode takes a regular query node away for good as far as this
// coordinator can tell - a crash, a restart and a scale-down all look the
// same to it: the session is gone, and the resource manager unassigns the
// node from its group.
func (f *sqnFixture) removeRegularNode(t *testing.T, nodeID int64) {
	t.Helper()
	f.nodeMgr.Remove(nodeID)
	f.meta.HandleNodeDown(context.Background(), nodeID)
	require.False(t, f.meta.ContainsNode(context.Background(), sqnGroup, nodeID))
}

// putReplica puts the replica into meta, where a full check round reads it.
func (f *sqnFixture) putReplica(t *testing.T, replica *meta.Replica) {
	t.Helper()
	require.NoError(t, f.meta.Put(context.Background(), replica))
}

// replicaObserverRound does what the replica observer does to the fixture's
// replica once its regular node is no longer its group's: the node is
// flipped rw->ro (utils.RecoverReplicaOfCollection) and, holding nothing in
// the distribution, removed. The streaming query nodes are not its business
// here and stay.
func (f *sqnFixture) replicaObserverRound(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	utils.RecoverReplicaOfCollection(ctx, f.meta, sqnCollection)
	replica := f.meta.Get(ctx, sqnReplica)
	require.NotNil(t, replica)
	if ro := replica.GetRONodes(); len(ro) > 0 {
		require.NoError(t, f.meta.RemoveNode(ctx, sqnCollection, sqnReplica, ro...))
	}
}

// newSealedSegmentEntersTheTarget is the next target being pulled again with
// a fresh sealed segment: the target version moves, which is also what lets
// a full check round look at the collection again rather than skip it as
// unchanged.
func (f *sqnFixture) newSealedSegmentEntersTheTarget() {
	f.targetVersion++
}

// round runs one full check round - Check, with the streaming service on and
// the shard's delegator present - and returns every task it produced: the
// ones handed to the scheduler as they were made, and the ones returned.
func (f *sqnFixture) round(t *testing.T) []task.Task {
	t.Helper()
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	defer enabled.UnPatch()
	shardLeader := mockey.Mock((*meta.ChannelDistManager).GetShardLeader).Return(&meta.DmChannel{}).Build()
	defer shardLeader.UnPatch()

	f.scheduled = nil
	f.offered = nil
	returned := f.checker.Check(context.Background())
	return append(f.scheduled, returned...)
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

// The reviewer's failure, in its permanent form: the operator moves a replica
// that has been seen with a regular node into a group whose only compute is a
// streaming node (TransferReplica keeps the replica's ID), and the record
// made in the mixed group would gate the placement for good - empty RW set,
// record hit, no fallback, no task, no log, until a coordinator restart. The
// record reflects membership, not history alone: a group that holds no
// regular node and asks for none has given them up, and the replica's sealed
// segments go to its streaming query node the very next round.
func TestAReplicaMovedToAStreamingOnlyGroupPlacesOnItsStreamingNode(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	f.putReplica(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}))

	tasks := f.round(t)
	require.Len(t, tasks, 1)
	assert.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node(), "sanity: in the mixed group the segment goes to the regular node")
	require.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))

	f.addNamedGroup(t, sqnStreamingOnlyGroup, 0)
	require.NoError(t, f.meta.TransferReplica(ctx, sqnCollection, sqnGroup, sqnStreamingOnlyGroup, 1))
	f.replicaObserverRound(t)
	moved := f.meta.Get(ctx, sqnReplica)
	require.Equal(t, sqnStreamingOnlyGroup, moved.GetResourceGroup())
	require.Empty(t, moved.GetRWNodes(), "the regular node was the old group's, and is stripped")
	require.Equal(t, []int64{sqnStreaming}, moved.GetRWSQNodes())

	f.newSealedSegmentEntersTheTarget()
	tasks = f.round(t)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica), "the record goes with the regular nodes the group gave up")
	require.Len(t, tasks, 1, "the very next round places the segment")
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node(), "on the streaming node's query node")
}

// A regular node that is gone while its group still asks for one is merely
// away - the resource manager pulls a node in for a group that is missing
// one - and the replica waits for it, as on master. Only once the operator
// declares the group regular-node-free is the record dropped and the segment
// placed on the streaming query node.
func TestAReplicaWaitsForARegularNodeUntilItsGroupGivesRegularNodesUp(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	f.putReplica(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}))

	tasks := f.round(t)
	require.Len(t, tasks, 1)
	require.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node())

	f.removeRegularNode(t, sqnRegular)
	f.replicaObserverRound(t)
	group := f.meta.GetResourceGroup(ctx, sqnGroup)
	require.Zero(t, group.NodeNum())
	require.Equal(t, 1, group.MissingNumOfNodes(), "the group still asks for its regular node")
	require.Empty(t, f.meta.Get(ctx, sqnReplica).GetRWNodes())

	f.newSealedSegmentEntersTheTarget()
	tasks = f.round(t)
	assert.Empty(t, tasks, "the segment waits for the regular node the group is owed")
	assert.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))

	require.NoError(t, f.meta.AlterResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{sqnGroup: {
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 0},
	}}))
	require.Zero(t, f.meta.GetResourceGroup(ctx, sqnGroup).MissingNumOfNodes(), "the group has given regular nodes up")

	f.newSealedSegmentEntersTheTarget()
	tasks = f.round(t)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))
	require.Len(t, tasks, 1, "the very next round places the segment")
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node())
}

// The operator hands the mixed group's regular node to another group
// (TransferNode). The node is alive and serving elsewhere, and the group it
// left holds none and asks for none: the replica's record is dropped and its
// sealed segments go to the streaming query node.
func TestAReplicaWhoseRegularNodeWasTransferredAwayPlacesOnItsStreamingNode(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	f.putReplica(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}))

	tasks := f.round(t)
	require.Len(t, tasks, 1)
	require.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node())

	f.addNamedGroup(t, sqnOtherGroup, 0)
	require.NoError(t, f.meta.TransferNode(ctx, sqnGroup, sqnOtherGroup, 1))
	require.True(t, f.meta.ContainsNode(ctx, sqnOtherGroup, sqnRegular), "the node now serves the other group")
	group := f.meta.GetResourceGroup(ctx, sqnGroup)
	require.Zero(t, group.NodeNum())
	require.Zero(t, group.MissingNumOfNodes())
	f.replicaObserverRound(t)
	require.Empty(t, f.meta.Get(ctx, sqnReplica).GetRWNodes())

	f.newSealedSegmentEntersTheTarget()
	tasks = f.round(t)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))
	require.Len(t, tasks, 1)
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node())
}

// The default group asks for no regular node (its requests are 0) and holds
// whatever arrives. A replica in it keeps its record for as long as the group
// holds a regular node; once the group holds none, it reads exactly like a
// group that gave regular nodes up - the coordinator cannot tell a restart
// from a scale-down there - and a restart of that node is the bounded
// one-window placement on the streaming query node that the move pass
// repairs, as after a coordinator restart.
func TestTheDefaultGroupKeepsTheRecordWhileItHoldsARegularNode(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.addRegularNodeTo(t, sqnRegular, meta.DefaultResourceGroupName)
	group := f.meta.GetResourceGroup(ctx, meta.DefaultResourceGroupName)
	require.Equal(t, 1, group.NodeNum())
	require.Zero(t, group.MissingNumOfNodes(), "the default group asks for no regular node yet holds one")
	f.putReplica(t, meta.NewReplica(&querypb.Replica{
		ID: sqnReplica, CollectionID: sqnCollection, ResourceGroup: meta.DefaultResourceGroupName,
		Nodes: []int64{sqnRegular}, RwSqNodes: []int64{sqnStreaming},
	}))

	tasks := f.round(t)
	require.Len(t, tasks, 1)
	require.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node())
	f.checker.forgetReleasedReplicas(ctx)
	assert.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica), "a group that holds a regular node has not given them up, whatever it asks for")

	f.nodeMgr.Remove(sqnRegular)
	f.meta.HandleNodeDown(ctx, sqnRegular)
	f.replicaObserverRound(t)
	require.Empty(t, f.meta.Get(ctx, sqnReplica).GetRWNodes())

	f.newSealedSegmentEntersTheTarget()
	tasks = f.round(t)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica), "a group that holds none and asks for none has")
	require.Len(t, tasks, 1)
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node())
}

// A replica whose group the resource manager does not know is not judged:
// there is no membership to read, so the record stays and the replica keeps
// waiting, as it would on master.
func TestAReplicaInAGroupTheResourceManagerDoesNotKnowKeepsItsRecord(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.putReplica(t, meta.NewReplica(&querypb.Replica{
		ID: sqnReplica, CollectionID: sqnCollection, ResourceGroup: "rg-nobody-knows",
		Nodes: []int64{sqnRegular}, RwSqNodes: []int64{sqnStreaming},
	}))
	require.False(t, f.meta.ContainResourceGroup(ctx, "rg-nobody-knows"))
	f.checker.replicasWithRegularNodes.Insert(sqnReplica)

	f.checker.forgetReleasedReplicas(ctx)
	assert.True(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))
}

// The reviewer's overlay: the group gives regular nodes up on a QUIET
// cluster - neither the target nor the distribution moves afterwards. The
// waiting round before the removal marked the collection synced, and the
// version cache would keep skipping it until the next target re-pull; the
// record drop must invalidate the cache, so the round right after the
// removal places with no other change at all.
func TestARecordDropPlacesEvenWhenNeitherTargetNorDistributionMoves(t *testing.T) {
	setForm(t, true)
	f := newSQNFixture(t)
	ctx := context.Background()
	f.addGroup(t, 1)
	f.addRegularNode(t, sqnRegular)
	f.putReplica(t, f.replica([]int64{sqnRegular}, []int64{sqnStreaming}))

	tasks := f.round(t)
	require.Len(t, tasks, 1)
	require.EqualValues(t, sqnRegular, tasks[0].Actions()[0].Node())

	f.removeRegularNode(t, sqnRegular)
	f.replicaObserverRound(t)
	tasks = f.round(t)
	require.Empty(t, tasks, "the segment waits for the regular node the group is owed")
	require.Contains(t, f.checker.versionCache, sqnCollection, "and the waiting round marked the collection synced")

	require.NoError(t, f.meta.AlterResourceGroups(ctx, map[string]*rgpb.ResourceGroupConfig{sqnGroup: {
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 0},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 0},
	}}))

	// No new sealed segment, no distribution report: nothing but the group.
	tasks = f.round(t)
	assert.False(t, f.checker.replicasWithRegularNodes.Contain(sqnReplica))
	require.Len(t, tasks, 1, "the round right after the removal places, with no other change")
	assert.EqualValues(t, sqnStreaming, tasks[0].Actions()[0].Node())
}
