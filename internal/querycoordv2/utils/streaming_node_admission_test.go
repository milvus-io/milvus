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
// admits the load.
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

	assignment, err := AssignReplica(ctx, m, []string{"rg_mixed"}, 2, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"rg_mixed": 2}, assignment)
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
