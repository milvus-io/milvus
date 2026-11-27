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

// installForm turns this test's binary into one with a form installed, and
// turns it back into a stock binary when the test ends.
func installForm(t *testing.T) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	ext.SetForm()
}

// stockBinary makes sure nothing is installed, so the test speaks for a stock
// deployment whatever an earlier test left behind.
func stockBinary(t *testing.T) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
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

// metaWithQueryClusters builds a meta holding one resource group per name,
// each with one regular query node: the shape of a form's query clusters.
func metaWithQueryClusters(t *testing.T, rgNames ...string) (context.Context, *meta.Meta) {
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
	for i, rgName := range rgNames {
		_, err = m.AddResourceGroup(ctx, rgName, &rgpb.ResourceGroupConfig{
			Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
			Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
		})
		require.NoError(t, err)
		nodeID := int64(100 + i)
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: nodeID, Address: "localhost", Hostname: "localhost"}))
		m.HandleNodeUp(ctx, nodeID)
		nodes, err := m.GetNodes(ctx, rgName)
		require.NoError(t, err)
		require.Len(t, nodes, 1, "each query cluster holds its one regular query node")
	}
	return ctx, m
}

// A form's instance runs ONE streaming node, for DDL and the write ahead log,
// and serves queries from resource groups of regular query nodes, several of
// which load the same collection. Milvus's own bound - no more replicas than
// streaming query nodes, because that is where a delegator goes - would refuse
// the second one. Under a form the delegator goes onto the replica's regular
// query nodes, so that bound does not apply and the regular one decides.
func TestAFormLoadsACollectionIntoMoreQueryClustersThanItHasStreamingNodes(t *testing.T) {
	ctx, m := metaWithQueryClusters(t, "cluster_a", "cluster_b")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"internal": typeutil.NewUniqueSet(7), // the instance's only streaming node, in neither cluster
	})()

	installForm(t)
	assignment, err := AssignReplica(ctx, m, []string{"cluster_a", "cluster_b"}, 2, true)
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"cluster_a": 1, "cluster_b": 1}, assignment)
}

// The same request on a stock binary keeps the refusal it has always had.
func TestAStockBinaryStillBoundsReplicasByStreamingNodes(t *testing.T) {
	ctx, m := metaWithQueryClusters(t, "cluster_a", "cluster_b")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"internal": typeutil.NewUniqueSet(7),
	})()

	stockBinary(t)
	_, err := AssignReplica(ctx, m, []string{"cluster_a", "cluster_b"}, 2, true)
	assert.ErrorIs(t, err, merr.ErrServiceResourceInsufficient, "two replicas, one streaming node")
}

// The regular bound still stands under a form: a query cluster with one
// regular query node takes one replica, not two.
func TestAFormStillBoundsAQueryClusterByItsRegularNodes(t *testing.T) {
	ctx, m := metaWithQueryClusters(t, "cluster_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"internal": typeutil.NewUniqueSet(7),
	})()

	installForm(t)
	_, err := AssignReplica(ctx, m, []string{"cluster_a"}, 2, true)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}
