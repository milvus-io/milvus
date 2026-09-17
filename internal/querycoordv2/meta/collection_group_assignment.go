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
	"time"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func groupPlanKey(replicas []*Replica, nodes []int64, rows map[int64]int64) string {
	type member struct{ ID, Collection, Rows int64 }
	members := make([]member, 0, len(replicas))
	for _, r := range replicas {
		members = append(members, member{r.GetID(), r.GetCollectionID(), rows[r.GetCollectionID()]})
	}
	// Fixed structs and sorted slices make the key independent of map iteration or trigger order.
	key, _ := json.Marshal(struct {
		Members []member
		Nodes   []int64
	}{members, nodes})
	return string(key)
}

// groupNodeCounts apportions a node budget proportional to each replica's data.
// The minimum of one can oversubscribe across collections, never within one.
func groupNodeCounts(replicas []*Replica, nodeCount int, rows map[int64]int64) map[int64]int {
	counts := make(map[int64]int)
	perCollection := make(map[int64]int)
	total := float64(0)
	for _, r := range replicas {
		total += float64(max(rows[r.GetCollectionID()], 1))
	}
	budget := nodeCount
	for _, r := range replicas {
		ideal := float64(nodeCount) * float64(max(rows[r.GetCollectionID()], 1)) / total
		count := int(ideal)
		counts[r.GetID()] = count
		perCollection[r.GetCollectionID()] += count
		budget -= count
	}
	// Largest deficit against the proportional ideal, with replica ID tie-breaking.
	// Sorted input makes every rounding decision deterministic.
	for budget > 0 {
		var best *Replica
		deficit := float64(-1e300)
		for _, r := range replicas {
			if perCollection[r.GetCollectionID()] >= nodeCount {
				continue
			}
			ideal := float64(nodeCount) * float64(max(rows[r.GetCollectionID()], 1)) / total
			d := ideal - float64(counts[r.GetID()])
			if best == nil || d > deficit {
				best, deficit = r, d
			}
		}
		if best == nil {
			break
		}
		counts[best.GetID()]++
		perCollection[best.GetCollectionID()]++
		budget--
	}
	// Apply minimum-one only after weighted apportionment. Small collections
	// share nodes instead of consuming the large collections' proportional
	// budget. Never exceed the physical capacity within one collection.
	for _, r := range replicas {
		if counts[r.GetID()] == 0 && perCollection[r.GetCollectionID()] < nodeCount {
			counts[r.GetID()] = 1
			perCollection[r.GetCollectionID()]++
		}
	}
	return counts
}

// assignCollectionGroup computes node eligibility only. Existing balancers still
// choose and move the individual segments. Different collections may share nodes.
func assignCollectionGroup(replicas []*Replica, nodes []int64, rows map[int64]int64) map[int64][]int64 {
	counts := groupNodeCounts(replicas, len(nodes), rows)
	result := make(map[int64][]int64)
	used := make(map[int64]typeutil.Set[int64])
	load := make(map[int64]float64)
	for _, r := range replicas {
		used[r.GetCollectionID()] = typeutil.NewSet[int64]()
	}
	// Place by predicted row load, retaining current RW nodes on ties. This
	// also spreads small collections after an RG grows from a single node.
	for _, r := range replicas {
		id, coll := r.GetID(), r.GetCollectionID()
		for len(result[id]) < counts[id] {
			var best int64
			found := false
			for _, node := range nodes {
				if used[coll].Contain(node) {
					continue
				}
				if !found || load[node] < load[best] ||
					(load[node] == load[best] && slices.Contains(r.GetRWNodes(), node) && !slices.Contains(r.GetRWNodes(), best)) {
					best, found = node, true
				}
			}
			if !found {
				break
			}
			result[id] = append(result[id], best)
			used[coll].Insert(best)
			load[best] += float64(max(rows[coll], 1)) / float64(counts[id])
		}
	}
	for id := range result {
		slices.Sort(result[id])
	}
	return result
}

func applyCollectionGroupPlan(replica *Replica, siblings []*Replica, nodes []int64, plan map[int64][]int64) *Replica {
	available := typeutil.NewSet(nodes...)
	blocked := typeutil.NewSet[int64]()
	reserved := typeutil.NewSet[int64]()
	for _, sibling := range siblings {
		if sibling.GetID() != replica.GetID() {
			blocked.Insert(sibling.GetRWNodes()...)
			blocked.Insert(sibling.GetRONodes()...)
			reserved.Insert(plan[sibling.GetID()]...)
		}
	}
	rw := typeutil.NewSet[int64]()
	for _, node := range plan[replica.GetID()] {
		if available.Contain(node) && !blocked.Contain(node) {
			rw.Insert(node)
		}
	}
	if rw.Len() == 0 {
		// Never proactively drain a serving replica to zero while its destination
		// remains occupied. A failed/out-of-RG node is not a serving fallback.
		current := slices.Clone(replica.GetRWNodes())
		slices.Sort(current)
		for _, node := range current {
			if available.Contain(node) {
				rw.Insert(node)
				break
			}
		}
		// Fault recovery must not wait forever for a blocked preferred destination.
		if rw.Len() == 0 {
			for _, node := range nodes {
				if !blocked.Contain(node) && !reserved.Contain(node) {
					rw.Insert(node)
					break
				}
			}
		}
	}
	add, remove := make([]int64, 0), make([]int64, 0)
	for node := range rw {
		if !slices.Contains(replica.GetRWNodes(), node) {
			add = append(add, node)
		}
	}
	for _, node := range replica.GetRWNodes() {
		if !rw.Contain(node) {
			remove = append(remove, node)
		}
	}
	if len(add) == 0 && len(remove) == 0 {
		return nil
	}
	slices.Sort(add)
	slices.Sort(remove)
	mutable := replica.CopyForWrite()
	mutable.AddRONode(remove...)
	mutable.AddRWNode(add...)
	if mutable.RWNodesCount() > 0 && mutable.NeedWaitRGReady() {
		mutable.SetWaitRGReadyAt(time.Time{})
	}
	return mutable.IntoReplica()
}
