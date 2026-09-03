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
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// metaWithEmptyResourceGroup builds a meta holding one resource group that the
// resource manager has no node in, which is the shape a group whose only
// compute is a streaming node has.
func metaWithEmptyResourceGroup(t *testing.T, rgName string) (context.Context, *meta.Meta) {
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
	m := meta.NewMeta(RandomIncrementIDAllocator(),
		querycoord.NewCatalog(etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())),
		session.NewNodeManager())
	_, err = m.AddResourceGroup(ctx, rgName, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: 1},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: 1},
	})
	require.NoError(t, err)
	return ctx, m
}

// withStreamingQueryNodes turns the streaming service on and makes the
// streaming node manager answer from byRG, so a test can state which resource
// group holds which streaming query nodes without one running.
//
// The cluster-wide set is derived from byRG rather than given separately: the
// two are the same nodes, and letting them disagree would test a state that
// cannot happen.
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
// manager instead. Counting only the resource manager's nodes therefore
// refuses a load the spawn immediately after would have placed perfectly well.
func TestALoadIsAdmittedIntoAResourceGroupServedByAStreamingNode(t *testing.T) {
	ctx, m := metaWithEmptyResourceGroup(t, "rg_streaming")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_streaming": typeutil.NewUniqueSet(101),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_streaming"}, 1, true)
	assert.NoError(t, err,
		"the group's streaming query node is compute the spawn will use, so the admission must count it")
}

// The count is per resource group, not cluster-wide: a streaming node in
// SOME other group is not compute this one can spawn a replica on, so the
// refusal stands.
func TestALoadIsStillRefusedWhenTheGroupsOwnComputeIsMissing(t *testing.T) {
	ctx, m := metaWithEmptyResourceGroup(t, "rg_empty")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(202),
	})()

	_, err := AssignReplica(ctx, m, []string{"rg_empty"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// With the streaming service off, nothing changes: the count is the resource
// manager's, exactly as it always was.
func TestTheAdmissionIsUnchangedWithTheStreamingServiceOff(t *testing.T) {
	ctx, m := metaWithEmptyResourceGroup(t, "rg_no_streaming")

	disabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(false).Build()
	defer disabled.UnPatch()

	_, err := AssignReplica(ctx, m, []string{"rg_no_streaming"}, 1, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}
