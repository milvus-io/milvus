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

package querycoordv2

import (
	"context"
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// admissionFixture is a meta whose resource groups hold real nodes, so that
// the LoadCollection callback's admission - AssignReplica - runs for real
// against them instead of being replaced by a mock. Nothing here touches etcd:
// the catalog is a mock that accepts every write.
type admissionFixture struct {
	meta    *meta.Meta
	nodeMgr *session.NodeManager
	broker  *meta.MockBroker
}

func newAdmissionFixture(t *testing.T) *admissionFixture {
	t.Helper()
	paramtable.Init()
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveResourceGroup", mock.Anything, mock.Anything).Return(nil).Maybe()

	nodeMgr := session.NewNodeManager()
	m := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(catalog),
		ReplicaManager:    meta.NewReplicaManager(params.RandomIncrementIDAllocator(), catalog),
		ResourceManager:   meta.NewResourceManager(catalog, nodeMgr),
	}
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{CollectionID: collectionID}, nil
		}).Maybe()
	broker.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return([]int64{1}, nil).Maybe()
	return &admissionFixture{meta: m, nodeMgr: nodeMgr, broker: broker}
}

// putResourceGroup registers rgName holding exactly the given regular query
// nodes; with none, the group is one whose only compute could be a streaming
// node, or one that has no compute at all.
func (f *admissionFixture) putResourceGroup(t *testing.T, rgName string, nodeIDs ...int64) {
	t.Helper()
	ctx := context.Background()
	wanted := int32(len(nodeIDs))
	_, err := f.meta.AddResourceGroup(ctx, rgName, &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{NodeNum: wanted},
		Limits:   &rgpb.ResourceGroupLimit{NodeNum: wanted},
	})
	require.NoError(t, err)
	for _, nodeID := range nodeIDs {
		f.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   nodeID,
			Address:  "localhost",
			Hostname: "localhost",
		}))
		f.meta.HandleNodeUp(ctx, nodeID)
	}
	nodes, err := f.meta.GetNodes(ctx, rgName)
	require.NoError(t, err)
	require.Len(t, nodes, len(nodeIDs), "the resource manager must hold exactly the query nodes given")
}

// loadedIn registers collectionID as loaded with one replica in each of the
// given resource groups, which is what makes the next LoadCollection on it a
// request on a loaded collection rather than a first load.
func (f *admissionFixture) loadedIn(t *testing.T, collectionID int64, rgNames ...string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: int32(len(rgNames)),
			Status:        querypb.LoadStatus_Loaded,
		},
	}))
	for i, rgName := range rgNames {
		if !f.meta.ContainResourceGroup(ctx, rgName) {
			f.putResourceGroup(t, rgName)
		}
		require.NoError(t, f.meta.Put(ctx, meta.NewReplica(&querypb.Replica{
			ID:            collectionID*100 + int64(i+1),
			CollectionID:  collectionID,
			ResourceGroup: rgName,
		})))
	}
}

// withStreamingQueryNodes turns the streaming service on and makes the
// streaming node manager answer from byRG, so a test can state which resource
// group holds which streaming query nodes without one running. The
// cluster-wide set is derived from byRG: the two are the same nodes.
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

// loadCollection runs the LoadCollection callback against the fixture with the
// real admission, and reports whether the callback asked the admission to check
// node numbers. The broadcast itself is stubbed: what is under test is the
// decision taken before it.
func (f *admissionFixture) loadCollection(t *testing.T, req *querypb.LoadCollectionRequest) (checkedNodeNum bool, err error) {
	t.Helper()
	s := &Server{meta: f.meta, broker: f.broker}
	var origin func(context.Context, *meta.Meta, []string, int32, bool) (map[string]int, error)
	broadcast := mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build()
	defer broadcast.UnPatch()
	admission := mockey.Mock(utils.AssignReplica).To(
		func(ctx context.Context, m *meta.Meta, resourceGroups []string, replicaNumber int32, checkNodeNum bool) (map[string]int, error) {
			checkedNodeNum = checkNodeNum
			return origin(ctx, m, resourceGroups, replicaNumber, checkNodeNum)
		}).Origin(&origin).Build()
	defer admission.UnPatch()
	generate := mockey.Mock(job.GenerateAlterLoadConfigMessage).Return(nil, nil).Build()
	defer generate.UnPatch()
	err = s.broadcastAlterLoadConfigCollectionV2ForLoadCollection(context.Background(), req)
	return checkedNodeNum, err
}

// loadPartitions is loadCollection for the LoadPartitions callback, which
// checks node numbers on every request.
func (f *admissionFixture) loadPartitions(t *testing.T, req *querypb.LoadPartitionsRequest) (checkedNodeNum bool, err error) {
	t.Helper()
	s := &Server{meta: f.meta, broker: f.broker}
	var origin func(context.Context, *meta.Meta, []string, int32, bool) (map[string]int, error)
	broadcast := mockey.Mock((*Server).startBroadcastWithCollectionIDLock).Return(stubBroadcaster{}, nil).Build()
	defer broadcast.UnPatch()
	admission := mockey.Mock(utils.AssignReplica).To(
		func(ctx context.Context, m *meta.Meta, resourceGroups []string, replicaNumber int32, checkNodeNum bool) (map[string]int, error) {
			checkedNodeNum = checkNodeNum
			return origin(ctx, m, resourceGroups, replicaNumber, checkNodeNum)
		}).Origin(&origin).Build()
	defer admission.UnPatch()
	generate := mockey.Mock(job.GenerateAlterLoadConfigMessage).Return(nil, nil).Build()
	defer generate.UnPatch()
	err = s.broadcastAlterLoadConfigCollectionV2ForLoadPartitions(context.Background(), req)
	return checkedNodeNum, err
}

// withStrictResourceGroupIsolation sets the streaming query node assignment
// mode for the test and restores the default when it ends.
func withStrictResourceGroupIsolation(t *testing.T, enabled bool) {
	t.Helper()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key, strconv.FormatBool(enabled)))
	t.Cleanup(func() { p.Reset(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key) })
}

// LoadPartitions checks node numbers on every request, and under strict
// isolation the per-group rule refuses a group with no streaming node of
// its own whatever its regular count. A collection loaded into rg_b with two
// replicas on its two regular and two streaming nodes, both streaming nodes
// restarting at once, and the identical LoadPartitions re-sent: the request
// adds no replica and is not admitted against anything, while the same
// request asking a third replica is.
func TestAnIdenticalScopedLoadPartitionsReSendIsNotRefusedByTheStrictRule(t *testing.T) {
	setForm(t, true)
	withStrictResourceGroupIsolation(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_b", 1, 2)
	f.loadedIn(t, 17, "rg_b", "rg_b")
	// Streaming nodes elsewhere keep the cluster-wide check out of the way.
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(301, 302, 303),
	})()

	checked, err := f.loadPartitions(t, &querypb.LoadPartitionsRequest{
		CollectionID:   17,
		PartitionIDs:   []int64{1},
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.False(t, checked, "a re-send adds no replica, so there is nothing to admit")
	assert.NoError(t, err)

	checked, err = f.loadPartitions(t, &querypb.LoadPartitionsRequest{
		CollectionID:   17,
		PartitionIDs:   []int64{1},
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked, "a third replica is an expansion")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "under strict isolation rg_b has no streaming node to give it a delegator")
}

// A stock binary checks node numbers on every load_partitions, as it always
// has, whatever the request repeats.
func TestAStockLoadPartitionsAlwaysChecksNodeNumbers(t *testing.T) {
	setForm(t, false)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_b", 1, 2)
	f.loadedIn(t, 18, "rg_b", "rg_b")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(301, 302),
	})()

	checked, err := f.loadPartitions(t, &querypb.LoadPartitionsRequest{
		CollectionID:   18,
		PartitionIDs:   []int64{1},
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked, "master's rule: every load_partitions is checked")
	assert.NoError(t, err, "two replicas on two regular nodes, as master admits")
}

// The reviewer's failure, through the callback. With strict isolation off,
// rg_a holds one regular node and no streaming node, and the default group
// holds two streaming nodes. The collection is loaded into rg_a, whose
// replica the replica manager serves from the default group's pool. A
// scoped load of two replicas into the default group would put three
// replicas on that pool's two nodes: one would never receive a delegator,
// its group's scoped task would read unknown forever and the load would
// neither complete nor time out. It is refused; one replica is admitted.
func TestAScopedExpansionIsAdmittedAgainstThePoolItsReplicasShare(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.loadedIn(t, 13, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   13,
		ReplicaNumber:  2,
		ResourceGroups: []string{meta.DefaultResourceGroupName},
	})
	assert.True(t, checked)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"rg_a's replica already sits in the default pool: three replicas on two streaming nodes")

	_, err = f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   13,
		ReplicaNumber:  1,
		ResourceGroups: []string{meta.DefaultResourceGroupName},
	})
	assert.NoError(t, err, "two replicas on two streaming nodes")
}

// LoadPartitions checks node numbers on every request and takes the same
// bound, or a placement the pool cannot serve would be admitted through it.
func TestAScopedLoadPartitionsIsAdmittedAgainstThePoolItsReplicasShare(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.loadedIn(t, 14, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902),
	})()

	_, err := f.loadPartitions(t, &querypb.LoadPartitionsRequest{
		CollectionID:   14,
		PartitionIDs:   []int64{1},
		ReplicaNumber:  2,
		ResourceGroups: []string{meta.DefaultResourceGroupName},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)

	_, err = f.loadPartitions(t, &querypb.LoadPartitionsRequest{
		CollectionID:   14,
		PartitionIDs:   []int64{1},
		ReplicaNumber:  1,
		ResourceGroups: []string{meta.DefaultResourceGroupName},
	})
	assert.NoError(t, err)
}

// The reviewer's finding: a client re-sending the load that placed a
// collection's two replicas in rg_b, while one of rg_b's two streaming nodes
// restarts, names a group on a loaded collection and so was admitted like an
// expansion: two replicas against one node, refused, thirty lines before the
// callback would have recognized a request that changes nothing. Admission
// runs only for what a request adds; an identical re-send adds nothing and
// reaches the no-op, and the same request asking a third replica is refused.
func TestAnIdenticalScopedReSendIsANoOpNotARefusal(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 15, "rg_b", "rg_b")
	// Streaming nodes elsewhere keep the cluster-wide check, which milvus
	// has always had, out of the way: what would refuse is rg_b's pool.
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b":         typeutil.NewUniqueSet(201),
		"rg_elsewhere": typeutil.NewUniqueSet(301, 302, 303),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   15,
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.False(t, checked, "a re-send adds no replica, so there is nothing to admit")
	assert.NoError(t, err, "the collection is still serving; the re-send is the no-op it always was")

	checked, err = f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   15,
		ReplicaNumber:  3,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked, "a third replica is an expansion")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// With BOTH of rg_b's streaming nodes away, the per-group rule falls back to
// the regular-node bound and would refuse two replicas on none; the re-send
// still adds nothing and is still the no-op.
func TestAnIdenticalScopedReSendIsANoOpWhileTheGroupHasNoStreamingNode(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 16, "rg_b", "rg_b")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_elsewhere": typeutil.NewUniqueSet(301, 302),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   16,
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.False(t, checked)
	assert.NoError(t, err)
}

// The reviewer's finding: master checks node numbers only for a first load,
// and a config update on a loaded collection skips the check. With a form, a
// LoadCollection naming resource groups on a loaded collection is a scoped
// expansion into those groups - a placement decision like a first load - and
// it was admitted with no bound at all. A group with no node would receive a
// replica that never gets a delegator, the scoped task's clock would pause
// forever, and ShowLoadCollections scoped to the group would report 0 for as
// long as anybody polled it.
func TestAScopedExpansionIntoAGroupWithNoNodeIsRefused(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 7, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   7,
		ReplicaNumber:  1,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked, "a scoped expansion is a placement decision and is checked like a first load")
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// One streaming node in the group gives one replica a delegator; asking two
// is refused up front rather than leaving the second waiting forever.
func TestAScopedExpansionAskingMoreReplicasThanTheGroupCanDelegateIsRefused(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 8, "rg_a")
	// A second streaming node elsewhere keeps the cluster-wide check out of
	// the way: what refuses is the group.
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
		"rg_b": typeutil.NewUniqueSet(102),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   8,
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
}

// A group with enough compute admits the expansion, exactly as a first load
// into it would be admitted.
func TestAScopedExpansionIntoAGroupWithEnoughNodesIsAdmitted(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 9, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
		"rg_b": typeutil.NewUniqueSet(102, 103),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   9,
		ReplicaNumber:  2,
		ResourceGroups: []string{"rg_b"},
	})
	assert.True(t, checked)
	assert.NoError(t, err)
}

// A form's request that names no resource group on a loaded collection is a
// config update, not a placement into a new group, and keeps master's rule:
// no node check.
func TestAFormsConfigUpdateOnALoadedCollectionSkipsTheNodeCheck(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.loadedIn(t, 10, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:  10,
		ReplicaNumber: 1,
	})
	assert.False(t, checked, "a config update on a loaded collection skips the node check, as master does")
	assert.NoError(t, err)
}

// The stock binary is unchanged: a LoadCollection naming resource groups on a
// loaded collection is a config update there, and master never checks node
// numbers for one, whatever the named group holds.
func TestAStockLoadOnALoadedCollectionSkipsTheNodeCheckWhateverGroupsItNames(t *testing.T) {
	setForm(t, false)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 11, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
	})()

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   11,
		ReplicaNumber:  1,
		ResourceGroups: []string{"rg_b"},
	})
	assert.False(t, checked, "on a stock binary a load on a loaded collection never checks node numbers")
	assert.NoError(t, err, "the empty group is not checked, exactly as before")
}

// A cluster-level force override discards the groups the request named and
// makes the load state the whole placement; on a loaded collection that is a
// config update, and the scoping decision the placement follows says so. The
// node check follows the same decision, not a second reading of the request.
func TestAFormsForceOverriddenLoadOnALoadedCollectionSkipsTheNodeCheck(t *testing.T) {
	setForm(t, true)
	f := newAdmissionFixture(t)
	f.putResourceGroup(t, "rg_a", 1)
	f.putResourceGroup(t, "rg_b")
	f.loadedIn(t, 12, "rg_a")
	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
	})()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key, "true"))
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key, "1"))
	require.NoError(t, p.Save(p.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key, "rg_a"))
	t.Cleanup(func() {
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.Key)
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadReplicaNumber.Key)
		p.Reset(p.QueryCoordCfg.ClusterLevelLoadResourceGroups.Key)
	})

	checked, err := f.loadCollection(t, &querypb.LoadCollectionRequest{
		CollectionID:   12,
		ReplicaNumber:  1,
		ResourceGroups: []string{"rg_b"},
	})
	assert.False(t, checked, "the override made this a whole-placement config update")
	assert.NoError(t, err)
}
