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

package utils

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	ext "github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// formHook is the smallest thing a distribution can install: the admission
// only asks whether a hook is there, never what it does.
type formHook struct{ hook.Hook }

// installForm turns this test's binary into one a distribution has compiled
// itself into, and turns it back into a stock binary when the test ends.
func installForm(t *testing.T) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	ext.SetHook(formHook{})
}

// stockBinary makes sure nothing is installed, so the test speaks for a stock
// deployment whatever an earlier test left behind.
func stockBinary(t *testing.T) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
}

// metaWithResourceGroup builds a meta holding one resource group whose
// resource manager holds exactly queryNodes. With none, it is the shape a
// group whose only compute is a streaming node has.
func metaWithResourceGroup(t *testing.T, rgName string, queryNodes ...int64) (context.Context, *meta.Meta) {
	t.Helper()
	paramtable.Init()
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	t.Cleanup(func() { cli.Close() })

	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(),
		querycoord.NewCatalog(etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())),
		nodeMgr)
	wanted := int32(max(len(queryNodes), 1))
	_, err = m.AddResourceGroup(ctx, rgName, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: wanted},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: wanted},
	})
	require.NoError(t, err)
	for _, nodeID := range queryNodes {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "localhost",
			Hostname: "localhost",
		}))
		m.HandleNodeUp(ctx, nodeID)
	}
	nodes, err := m.GetNodes(ctx, rgName)
	require.NoError(t, err)
	require.Len(t, nodes, len(queryNodes), "the resource manager must hold exactly the query nodes given")
	return ctx, m
}

// withStreamingQueryNodes turns the streaming service on and makes the
// streaming node manager answer from byRG, so a test can state which resource
// group holds which streaming query nodes without one running.
//
// The cluster-wide set is derived from byRG rather than given separately: the
// two are the same nodes, and letting them disagree would test a state that
// cannot happen. It is needed as well as the per-group map, because
// AssignReplica's first check - the one milvus has always had - refuses a
// replica count above the number of streaming nodes in the whole cluster.
func withStreamingQueryNodes(byRG map[string]typeutil.UniqueSet) func() {
	all := typeutil.NewUniqueSet()
	for _, nodes := range byRG {
		all.Insert(nodes.Collect()...)
	}
	enabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
	byGroup := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDsByResourceGroup).
		Return(byRG).Build()
	cluster := mockey.Mock((*snmanager.StreamingNodeManager).GetStreamingQueryNodeIDs).
		Return(all).Build()
	return func() {
		cluster.UnPatch()
		byGroup.UnPatch()
		enabled.UnPatch()
	}
}

// admit runs the admission the DDL callbacks run for a request that states
// the whole placement: AssignReplica's per-group rule, then the delegator
// capacity over the collection's whole layout. The two are one decision to
// a caller, and most tests here are about that decision, not about which of
// the two took it.
func admit(ctx context.Context, m *meta.Meta, resourceGroups []string, replicaNumber int32) (map[string]int, error) {
	assignment, err := AssignReplica(ctx, m, resourceGroups, replicaNumber, true)
	if err != nil {
		return nil, err
	}
	if err := CheckDelegatorCapacity(ctx, m, admittedCollection, assignment, false); err != nil {
		return nil, err
	}
	return assignment, nil
}

// admittedCollection is the collection every admit call speaks for; nothing
// puts a replica of it into meta, so its layout is the request alone.
const admittedCollection = int64(1)

// A resource group whose compute is a streaming node has no node in the
// resource manager - milvus keeps the query node embedded in a streaming node
// out of it deliberately, and hands it to a replica through the streaming node
// manager instead. For an installed form, counting only the resource manager's
// nodes would refuse a load the spawn immediately after would have placed
// perfectly well.
func TestALoadIsAdmittedIntoAResourceGroupServedByAStreamingNode(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_streaming"}, 1, true)
	assert.NoError(t, err,
		"the group's streaming query node is compute the spawn will use, so the admission must count it")
}

// The same request on a stock binary is refused, exactly as it always was: a
// stock deployment never runs a resource group on streaming nodes alone, and
// its balancers would never move a segment off one.
func TestAStockBinaryRefusesALoadIntoAResourceGroupServedByAStreamingNode(t *testing.T) {
	stockBinary(t)
	ctx, m := metaWithResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_streaming"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// The reviewer's cluster: one query node and two streaming nodes, all in one
// resource group, asked for two replicas. A stock binary counts the one query
// node and refuses, as it always has; a form counts the streaming nodes too and
// admits the load, since each replica can have a streaming node of its own.
func TestAStockBinaryCountsOnlyTheQueryNodesOfAMixedResourceGroup(t *testing.T) {
	stockBinary(t)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed": typeutil.NewUniqueSet(101, 102),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_mixed"}, 2, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"one query node cannot carry two replicas on a stock binary, whatever streaming nodes sit beside it")
}

func TestAFormCountsTheStreamingNodesOfAMixedResourceGroup(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed": typeutil.NewUniqueSet(101, 102),
	})()

	assignment, err := admit(ctx, m, []string{"rg_mixed"}, 2)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_mixed": 2}, assignment)
}

// The reviewer's failure: one query node and ONE streaming node in the
// group, asked for two replicas. With the streaming service on, every replica
// needs a streaming node for its delegator - the channel checker places
// delegators on the replica's streaming nodes only, and those are handed out
// without overlap between the replicas of a pool - so a group served from
// its own streaming node holds one replica, never one per node of either
// kind. Summed, the second replica is admitted, never receives a streaming
// node, is marked unplaced by the channel checker every tick, and a scoped
// expansion waiting on it never completes and never times out.
func TestAFormDoesNotSumRegularAndStreamingNodesOfAMixedResourceGroup(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1)

	// A second streaming node elsewhere keeps the cluster-wide check, which
	// milvus has always had, out of the way: what refuses is the group.
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed":     typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := admit(ctx, m, []string{"rg_mixed"}, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"one streaming node gives one replica a delegator; the regular node beside it does not give a second")
}

// The per-group rule of AssignReplica is master's regular-node bound, with
// one waiver: a group that has streaming nodes passes it, since it may have
// no regular node at all and still serve a replica. The delegator capacity
// is not its business - that is judged over the collection's whole layout
// by CheckDelegatorCapacity, with the pooling the assignment uses - so the
// same two-replica request passes AssignReplica alone and is refused by the
// pool check.
func TestAssignReplicaWaivesTheRegularBoundForAGroupWithStreamingNodes(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed":     typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	assignment, err := AssignReplica(ctx, m, []string{"rg_mixed"}, 2, true)
	require.NoError(t, err, "the regular bound is waived for a group with streaming nodes")
	assert.Equal(t, map[string]int{"rg_mixed": 2}, assignment)

	err = CheckDelegatorCapacity(ctx, m, admittedCollection, assignment, false)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "and the pool check refuses the second replica")
}

// The waiver is only for a group that has streaming nodes. One that has none
// keeps master's bound exactly, form or not: its regular nodes are its
// compute, and the delegators come from a pool the pool check bounds.
func TestAssignReplicaKeepsMastersBoundForAGroupWithoutStreamingNodes(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_regular", 1)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(201, 202),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_regular"}, 2, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "two replicas on one regular node, as master refuses")

	assignment, err := AssignReplica(ctx, m, []string{"rg_regular"}, 1, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_regular": 1}, assignment)
}

// A group whose only compute is streaming nodes admits as many replicas as it
// has streaming nodes: each gets a delegator, and the sealed segments go to
// the same streaming query nodes.
func TestAFormAdmitsAsManyReplicasAsAStreamingOnlyGroupHasStreamingNodes(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101, 102),
	})()

	assignment, err := admit(ctx, m, []string{"rg_streaming"}, 2)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_streaming": 2}, assignment)
}

// And one more than that is refused, whatever the cluster holds elsewhere.
func TestAFormRefusesMoreReplicasThanAStreamingOnlyGroupHasStreamingNodes(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := admit(ctx, m, []string{"rg_streaming"}, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// Regular nodes alone give master's answer: a group of two query nodes and no
// streaming node admits two replicas on a form exactly as it does on a stock
// binary, so a deployment that never runs a group on streaming nodes sees no
// change.
func TestAFormAdmitsARegularOnlyGroupAsMasterDoes(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_regular", 1, 2)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(201, 202),
	})()

	assignment, err := AssignReplica(ctx, m, []string{"rg_regular"}, 2, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_regular": 2}, assignment)
}

// The count is per resource group, not cluster-wide: a streaming node in
// SOME other group is not compute this one can spawn a replica on, so the
// refusal stands even for a form.
func TestALoadIsStillRefusedWhenTheGroupsOwnComputeIsMissing(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_empty")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_empty"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// With the streaming service off, nothing changes even for a form: the count
// is the resource manager's, exactly as it always was.
func TestTheAdmissionIsUnchangedWithTheStreamingServiceOff(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_no_streaming")

	disabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(false).Build()
	defer disabled.UnPatch()

	_, err := AssignReplica(ctx, m, []string{"rg_no_streaming"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// withStrictResourceGroupIsolation sets the streaming query node assignment
// mode for the test and restores the default when it ends.
func withStrictResourceGroupIsolation(t *testing.T, enabled bool) {
	t.Helper()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key, strconv.FormatBool(enabled)))
	t.Cleanup(func() { p.Reset(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key) })
}

// The reviewer's shape: two regular nodes and one streaming node in the
// group, asked for two replicas. Under strict isolation every replica's
// delegator must come from a streaming node of its OWN group, and the group
// has one: the second replica would never get one. The larger of the two
// counts over-admits here, so under strict isolation the bound is the group's
// own streaming-node count.
func TestUnderStrictIsolationAFormBoundsAGroupByItsOwnStreamingNodes(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1, 2)

	// A second streaming node elsewhere keeps the cluster-wide check out of
	// the way: what refuses is the group.
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed":     typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := admit(ctx, m, []string{"rg_mixed"}, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"two regular nodes do not give a second replica a delegator when only its own group's streaming nodes may")
}

// The same group admits the one replica its streaming node can serve.
func TestUnderStrictIsolationAMixedGroupAdmitsAsManyReplicasAsItsStreamingNodes(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1, 2)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed": typeutil.NewUniqueSet(101),
	})()

	assignment, err := admit(ctx, m, []string{"rg_mixed"}, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_mixed": 1}, assignment)
}

// A regular-only group cannot host a delegator under strict isolation: no
// streaming node of its own, no replica, whatever the cluster holds elsewhere.
func TestUnderStrictIsolationARegularOnlyGroupHostsNoReplica(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_regular", 1, 2)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_regular"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"under strict isolation a group with no streaming node of its own cannot give a replica a delegator")
}

// A streaming-only group admits as many replicas as it has streaming nodes,
// under strict isolation as under the pooled modes.
func TestUnderStrictIsolationAStreamingOnlyGroupAdmitsOneReplicaPerStreamingNode(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101, 102),
	})()

	assignment, err := admit(ctx, m, []string{"rg_streaming"}, 2)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_streaming": 2}, assignment)
}

// The flag does not change the answer for a group that has streaming nodes of
// its own: with strict isolation off, the replica manager still serves such a
// group from its own streaming nodes (isolation, or the legacy default pool,
// which takes in only the replicas of groups that have none), so the
// reviewer's shape is refused for two replicas and admitted for one exactly
// as under strict isolation. The regular nodes beside the streaming node do
// not give the second replica a delegator.
func TestWithStrictIsolationOffAMixedGroupIsBoundedByItsOwnStreamingNodes(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1, 2)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed":     typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := admit(ctx, m, []string{"rg_mixed"}, 2)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"one streaming node of its own gives one replica a delegator, whatever the flag says")

	assignment, err := admit(ctx, m, []string{"rg_mixed"}, 1)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_mixed": 1}, assignment)
}

// A stock binary is unchanged by the mode: it counts the resource manager's
// nodes and nothing else, strict isolation or not.
func TestAStockBinaryIsUnchangedByStrictIsolation(t *testing.T) {
	stockBinary(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_mixed", 1, 2)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_mixed":     typeutil.NewUniqueSet(101),
		"rg_elsewhere": typeutil.NewUniqueSet(202, 203),
	})()

	assignment, err := AssignReplica(ctx, m, []string{"rg_mixed"}, 2, true)
	require.NoError(t, err, "two regular nodes admit two replicas on a stock binary, as master does")
	assert.Equal(t, map[string]int{"rg_mixed": 2}, assignment)

	_, err = AssignReplica(ctx, m, []string{"rg_mixed"}, 3, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}
