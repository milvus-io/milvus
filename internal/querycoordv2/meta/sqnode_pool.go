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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SQNodePool is one pool of streaming query nodes and the replicas that are
// served from it: the delegators of those replicas are placed on these nodes
// and on no other, and the nodes are handed out among them without overlap.
//
// Replicas are indices into the slice GroupIntoSQNodePools was given, in the
// order the pool serves them, so that a caller holding replicas can map them
// back and a caller holding only counts can count them.
type SQNodePool struct {
	Nodes    typeutil.UniqueSet
	Replicas []int
}

// GroupIntoSQNodePools decides which pool of streaming query nodes each
// replica is served from. It is the one reading of the cluster that both the
// assignment (ReplicaManager.buildSQNodeAssignmentHelpers) and the admission
// of a load (utils.CheckDelegatorCapacity) use, so that a load is admitted
// against exactly the pool its replicas will be assigned from.
//
// rgOfReplicas holds the resource group of each replica, sqnNodesByRG the
// streaming query nodes of each resource group that has any. A group is
// covered when it has a key there. The pools are keyed by name:
//
//   - A covered group is served from its own nodes and nothing else, whatever
//     strictIsolation says. This is the isolation mode.
//   - When some replica lives in a group that is not covered and
//     strictIsolation is off, the default group's pool, if there is one, takes
//     in every such replica beside the default group's own (the legacy default
//     pool, for streaming nodes that carry no group label). The default
//     group's node count is therefore not its capacity: it is shared.
//   - When there is no default pool for them, every streaming node of the
//     cluster is pooled for EVERY replica of the collection, covered or not
//     (flat allocation), under DefaultResourceGroupName.
//   - Under strictIsolation a replica in a group that is not covered is served
//     from nothing: it is returned in unpooled, and its group has no pool
//     unless another replica of the collection lives in a covered one.
//
// The replica order inside a pool is the input order, with the default
// group's own replicas before the ones it takes in.
func GroupIntoSQNodePools(
	rgOfReplicas []string,
	sqnNodesByRG map[string]typeutil.UniqueSet,
	strictIsolation bool,
) (pools map[string]SQNodePool, unpooled []int) {
	byRG := make(map[string][]int)
	uncovered := make([]int, 0)
	for i, rgName := range rgOfReplicas {
		if _, ok := sqnNodesByRG[rgName]; ok {
			byRG[rgName] = append(byRG[rgName], i)
		} else {
			uncovered = append(uncovered, i)
		}
	}

	pools = make(map[string]SQNodePool)
	if len(uncovered) > 0 && !strictIsolation {
		if _, ok := sqnNodesByRG[DefaultResourceGroupName]; ok {
			byRG[DefaultResourceGroupName] = append(byRG[DefaultResourceGroupName], uncovered...)
		} else {
			allSQNodes := typeutil.NewUniqueSet()
			for _, nodes := range sqnNodesByRG {
				allSQNodes.Insert(nodes.Collect()...)
			}
			all := make([]int, len(rgOfReplicas))
			for i := range all {
				all[i] = i
			}
			pools[DefaultResourceGroupName] = SQNodePool{Nodes: allSQNodes, Replicas: all}
			return pools, nil
		}
		uncovered = nil
	}
	for rgName, replicas := range byRG {
		pools[rgName] = SQNodePool{Nodes: sqnNodesByRG[rgName], Replicas: replicas}
	}
	return pools, uncovered
}
