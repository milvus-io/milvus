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

package meta

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// poolView is what a pool looks like to both the assignment helpers and the
// pure grouping: the streaming query nodes it hands out and the replicas it
// serves, in the order it serves them.
type poolView struct {
	nodes    []int64
	replicas []int64
}

// viewOfHelpers reads the assignment helpers the replica manager builds into
// the shape viewOfPools reads the pure grouping into.
func viewOfHelpers(helpers map[string]*replicasInSameRGAssignmentHelper) map[string]poolView {
	views := make(map[string]poolView, len(helpers))
	for name, helper := range helpers {
		view := poolView{nodes: sortedNodes(helper.nodesInRG)}
		for _, assignment := range helper.replicas {
			view.replicas = append(view.replicas, assignment.GetReplica().GetID())
		}
		views[name] = view
	}
	return views
}

func viewOfPools(replicas []*Replica, pools map[string]SQNodePool) map[string]poolView {
	views := make(map[string]poolView, len(pools))
	for name, pool := range pools {
		view := poolView{nodes: sortedNodes(pool.Nodes)}
		for _, i := range pool.Replicas {
			view.replicas = append(view.replicas, replicas[i].GetID())
		}
		views[name] = view
	}
	return views
}

func sortedNodes(nodes typeutil.UniqueSet) []int64 {
	sorted := nodes.Collect()
	slices.Sort(sorted)
	return sorted
}

func replicasIn(rgNames ...string) []*Replica {
	replicas := make([]*Replica, 0, len(rgNames))
	for i, rgName := range rgNames {
		replicas = append(replicas, newReplica(&querypb.Replica{
			ID:            int64(i + 1),
			CollectionID:  100,
			ResourceGroup: rgName,
		}))
	}
	return replicas
}

func resourceGroupsOf(replicas []*Replica) []string {
	rgNames := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		rgNames = append(rgNames, replica.GetResourceGroup())
	}
	return rgNames
}

func withStrictIsolation(t *testing.T, enabled bool) {
	t.Helper()
	p := paramtable.Get()
	require.NoError(t, p.Save(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key, strconv.FormatBool(enabled)))
	t.Cleanup(func() { p.Reset(p.StreamingCfg.StrictResourceGroupIsolationEnabled.Key) })
}

// The admission of a load and the assignment of streaming query nodes must
// read the cluster the same way, or a load is admitted against a pool the
// replica is never assigned from. This pins the two to one function: for
// every mode the replica manager knows, the helpers it builds and the pure
// grouping name the same pools, hand out the same nodes and serve the same
// replicas in the same order.
func TestGroupIntoSQNodePoolsAgreesWithTheAssignmentHelpers(t *testing.T) {
	paramtable.Init()
	mgr := NewReplicaManager(RandomIncrementIDAllocator(), nil)

	cases := []struct {
		name     string
		strict   bool
		replicas []*Replica
		nodes    map[string]typeutil.UniqueSet
		// pools is the grouping in the reviewer's words, so that the
		// agreement is not an agreement on something wrong.
		pools    map[string]poolView
		unpooled []int64
	}{
		{
			name:     "isolation: every group has streaming nodes and is served from its own",
			replicas: replicasIn("rg_a", "rg_b", "rg_a"),
			nodes: map[string]typeutil.UniqueSet{
				"rg_a": typeutil.NewUniqueSet(101, 102),
				"rg_b": typeutil.NewUniqueSet(201),
			},
			pools: map[string]poolView{
				"rg_a": {nodes: []int64{101, 102}, replicas: []int64{1, 3}},
				"rg_b": {nodes: []int64{201}, replicas: []int64{2}},
			},
		},
		{
			name:     "legacy default pool: the default group takes every replica of a group without streaming nodes",
			replicas: replicasIn("rg_a", DefaultResourceGroupName, "rg_b", "rg_a"),
			nodes: map[string]typeutil.UniqueSet{
				DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902),
				"rg_b":                   typeutil.NewUniqueSet(201),
			},
			pools: map[string]poolView{
				DefaultResourceGroupName: {nodes: []int64{901, 902}, replicas: []int64{2, 1, 4}},
				"rg_b":                   {nodes: []int64{201}, replicas: []int64{3}},
			},
		},
		{
			name:     "flat allocation: no default pool, so every replica shares every streaming node",
			replicas: replicasIn("rg_a", "rg_b", "rg_c"),
			nodes: map[string]typeutil.UniqueSet{
				"rg_b": typeutil.NewUniqueSet(201, 202),
				"rg_c": typeutil.NewUniqueSet(301),
			},
			pools: map[string]poolView{
				DefaultResourceGroupName: {nodes: []int64{201, 202, 301}, replicas: []int64{1, 2, 3}},
			},
		},
		{
			name:     "strict isolation: a replica in a group without streaming nodes belongs to no pool",
			strict:   true,
			replicas: replicasIn("rg_a", "rg_b", DefaultResourceGroupName),
			nodes: map[string]typeutil.UniqueSet{
				DefaultResourceGroupName: typeutil.NewUniqueSet(901),
				"rg_b":                   typeutil.NewUniqueSet(201),
			},
			pools: map[string]poolView{
				DefaultResourceGroupName: {nodes: []int64{901}, replicas: []int64{3}},
				"rg_b":                   {nodes: []int64{201}, replicas: []int64{2}},
			},
			unpooled: []int64{1},
		},
		{
			name:     "no streaming node anywhere",
			replicas: replicasIn("rg_a"),
			nodes:    map[string]typeutil.UniqueSet{},
			pools: map[string]poolView{
				DefaultResourceGroupName: {nodes: []int64{}, replicas: []int64{1}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withStrictIsolation(t, tc.strict)

			pools, unpooled := GroupIntoSQNodePools(resourceGroupsOf(tc.replicas), tc.nodes, tc.strict)
			got := viewOfPools(tc.replicas, pools)
			require.Len(t, got, len(tc.pools), "pool names: %v", got)
			for name, want := range tc.pools {
				assert.ElementsMatch(t, want.nodes, got[name].nodes, "nodes of pool %s", name)
				assert.Equal(t, want.replicas, got[name].replicas, "replicas of pool %s, in order", name)
			}
			unpooledIDs := make([]int64, 0, len(unpooled))
			for _, i := range unpooled {
				unpooledIDs = append(unpooledIDs, tc.replicas[i].GetID())
			}
			assert.ElementsMatch(t, tc.unpooled, unpooledIDs)

			assert.Equal(t, got, viewOfHelpers(mgr.buildSQNodeAssignmentHelpers(tc.replicas, tc.nodes)),
				"the assignment helpers must be built from exactly these pools")
		})
	}
}
