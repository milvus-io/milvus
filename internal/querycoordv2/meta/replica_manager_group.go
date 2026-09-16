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
	"context"
	"sort"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CollectionRowCount is an allocation input prepared by the caller. Valid
// distinguishes zero rows from unknown data; Fresh controls optional rebalancing.
type CollectionRowCount struct {
	Rows  int64
	Valid bool
	Fresh bool
}

// RecoverNodesInCollections allocates nodes for exactly the supplied collection
// batch. The caller owns batching and row statistics; the manager only plans and
// persists RW/RO assignments. A singleton batch uses equal replica weights and
// needs no row statistics.
func (m *ReplicaManager) RecoverNodesInCollections(ctx context.Context, collectionIDs []int64, rgs map[string]*ResourceGroup, rows map[int64]CollectionRowCount) error {
	ids := typeutil.NewUniqueSet(collectionIDs...).Collect()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		m.collLock.Lock(id)
	}
	defer func() {
		for i := len(ids) - 1; i >= 0; i-- {
			m.collLock.Unlock(ids[i])
		}
	}()
	all := make(map[int64][]*Replica, len(ids))
	nodeSets := make(map[string]typeutil.UniqueSet)
	for _, id := range ids {
		replicas, ok := m.coll2Replicas.Get(id)
		if !ok {
			return merr.WrapErrCollectionNotLoaded(id)
		}
		all[id] = replicas
		for _, replica := range replicas {
			rgName := replica.GetResourceGroup()
			rg, supplied := rgs[rgName]
			if !supplied {
				return merr.WrapErrServiceInternalMsg("lost resource group info for replica %d: %s", replica.GetID(), rgName)
			}
			nodeSets[rgName] = typeutil.NewUniqueSet()
			if rg != nil {
				nodeSets[rgName].Insert(rg.GetNodes()...)
			}
		}
	}
	if err := m.validateResourceGroups(nodeSets); err != nil {
		return err
	}
	if len(ids) == 1 {
		rows = map[int64]CollectionRowCount{ids[0]: {Rows: 1, Valid: true, Fresh: true}}
	}
	rgNames := make([]string, 0, len(nodeSets))
	for name := range nodeSets {
		rgNames = append(rgNames, name)
	}
	sort.Strings(rgNames)
	updates := make(map[int64][]*Replica)
	for _, name := range rgNames {
		desired := planReplicaGroup(name, all, nodeSets[name], rows, rgs[name])
		replicaIDs := make([]int64, 0, len(desired))
		for id := range desired {
			replicaIDs = append(replicaIDs, id)
		}
		sort.Slice(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] })
		for _, id := range replicaIDs {
			replica, _ := m.flatReplicas.Get(id)
			add, remove := make([]int64, 0), make([]int64, 0)
			for _, node := range replica.GetRWNodes() {
				if !desired[id].Contain(node) {
					remove = append(remove, node)
				}
			}
			for node := range desired[id] {
				if !replica.ContainRWNode(node) {
					add = append(add, node)
				}
			}
			if len(add)+len(remove) == 0 {
				continue
			}
			mutable := replica.CopyForWrite()
			mutable.AddRONode(remove...)
			mutable.AddRWNode(add...)
			if mutable.RWNodesCount() > 0 {
				mutable.SetWaitRGReadyAt(time.Time{})
			}
			updates[replica.GetCollectionID()] = append(updates[replica.GetCollectionID()], mutable.IntoReplica())
			mlog.Info(ctx, "assigned replica nodes in collection batch", mlog.Int64s("collections", ids),
				mlog.String("resourceGroup", name), mlog.Int64("replicaID", id),
				mlog.Int64s("newRWNodes", add), mlog.Int64s("newRONodes", remove))
		}
	}
	for _, id := range ids {
		if err := m.put(ctx, id, updates[id]...); err != nil {
			return err
		}
	}
	return nil
}

func waitForGroupRG(replica *Replica, rg *ResourceGroup) bool {
	return replica.NeedWaitRGReady() && rg != nil && rg.MissingNumOfNodes() > 0
}

// groupReplicaAssignment holds planner-only state; it never changes Replica.
type groupReplicaAssignment struct {
	replica   *Replica
	rows      int64
	quota     int
	available typeutil.UniqueSet
	desired   typeutil.UniqueSet
}

func planReplicaGroup(rgName string, all map[int64][]*Replica, nodes typeutil.UniqueSet,
	stats map[int64]CollectionRowCount, rg *ResourceGroup,
) map[int64]typeutil.UniqueSet {
	members := make([]*groupReplicaAssignment, 0)
	capacity := make(map[int64]int)
	fresh := true
	draining := false
	for collectionID, replicas := range all {
		outsiders := typeutil.NewUniqueSet()
		for _, replica := range replicas {
			if replica.GetResourceGroup() == rgName {
				continue
			}
			for _, node := range replica.GetNodes() {
				if nodes.Contain(node) {
					outsiders.Insert(node)
				}
			}
		}
		capacity[collectionID] = max(0, nodes.Len()-outsiders.Len())
		for _, replica := range replicas {
			if replica.GetResourceGroup() != rgName || waitForGroupRG(replica, rg) {
				continue
			}
			stat := stats[collectionID]
			// An unknown NEW replica has no allocation weight. Preserve existing
			// replicas during a source outage; never reinterpret missing as zero.
			if !stat.Valid && replica.RWNodesCount() == 0 && replica.RONodesCount() == 0 {
				continue
			}
			fresh = fresh && stat.Valid && stat.Fresh
			draining = draining || replica.RONodesCount() > 0
			available := nodes.Clone()
			for _, other := range replicas {
				if other.GetID() != replica.GetID() {
					available.Remove(other.GetNodes()...)
				}
			}
			members = append(members, &groupReplicaAssignment{
				replica: replica, rows: stat.Rows, available: available,
				desired: typeutil.NewUniqueSet(),
			})
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].replica.GetID() < members[j].replica.GetID() })
	assignGroupQuotas(members, nodes.Len())
	if !fresh {
		for _, member := range members {
			member.quota = max(1, member.replica.RWNodesCount())
		}
	}
	// Enforce collection-level capacity, including ownership in other RGs.
	// Replicas of a collection have the same row weight.
	for collectionID, limit := range capacity {
		trimGroupCollectionQuota(members, collectionID, limit)
	}
	projected := make(map[int64]float64)
	// Retain current placements first. This avoids reshuffling on every observer
	// tick and lets existing donors shrink before recipients claim drained nodes.
	for _, member := range members {
		rw := append([]int64(nil), member.replica.GetRWNodes()...)
		sort.Slice(rw, func(i, j int) bool { return rw[i] < rw[j] })
		for _, node := range rw {
			if !member.available.Contain(node) {
				continue
			}
			if member.desired.Len() < member.quota || !paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
				member.desired.Insert(node)
				projected[node] += groupNodeRows(member)
			}
		}
	}
	// Current ownership is already excluded; record newly allocated ownership to
	// prevent two replicas of one collection choosing the same free node.
	claimed := make(map[int64]typeutil.UniqueSet)
	for _, member := range members {
		id := member.replica.GetCollectionID()
		if claimed[id] == nil {
			claimed[id] = typeutil.NewUniqueSet()
		}
		claimed[id].Insert(member.desired.Collect()...)
	}
	sort.SliceStable(members, func(i, j int) bool {
		if members[i].available.Len() != members[j].available.Len() {
			return members[i].available.Len() < members[j].available.Len()
		}
		return groupNodeRows(members[i]) > groupNodeRows(members[j])
	})
	for _, member := range members {
		for member.desired.Len() < member.quota {
			if !placeGroupNode(member, members, projected, claimed, typeutil.NewUniqueSet()) {
				break
			}
		}
	}
	if fresh && !draining && paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetAsBool() {
		improveGroupPlacement(members, projected, claimed)
	}
	result := make(map[int64]typeutil.UniqueSet)
	for _, member := range members {
		result[member.replica.GetID()] = member.desired
	}
	return result
}

func groupNodeRows(member *groupReplicaAssignment) float64 {
	return float64(member.rows) / float64(max(1, member.quota))
}

func trimGroupCollectionQuota(members []*groupReplicaAssignment, collectionID int64, capacity int) {
	total := 0
	for _, member := range members {
		if member.replica.GetCollectionID() == collectionID {
			total += member.quota
		}
	}
	for total > capacity {
		var donor *groupReplicaAssignment
		for _, member := range members {
			if member.replica.GetCollectionID() != collectionID || member.quota == 0 {
				continue
			}
			if member.quota == 1 {
				serving := false
				for _, node := range member.replica.GetRWNodes() {
					serving = serving || member.available.Contain(node)
				}
				if serving {
					continue
				}
			}
			if donor == nil || member.quota > donor.quota ||
				(member.quota == donor.quota && member.replica.RWNodesCount() < donor.replica.RWNodesCount()) {
				donor = member
			}
		}
		if donor == nil {
			break
		} // Insufficient nodes: preserve serving replicas.
		donor.quota--
		total--
	}
}

// A move strictly reduces squared projected load. Only move when the reduction
// exceeds a 10% per-node-row margin, and never while a prior move is draining.
func improveGroupPlacement(members []*groupReplicaAssignment, projected map[int64]float64, claimed map[int64]typeutil.UniqueSet) {
	for _, member := range members {
		weight := groupNodeRows(member)
		if weight == 0 {
			continue
		}
		sources := member.desired.Collect()
		sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
		candidates := member.available.Collect()
		sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
		for _, from := range sources {
			best := from
			for _, to := range candidates {
				if claimed[member.replica.GetCollectionID()].Contain(to) {
					continue
				}
				if projected[to] < projected[best] {
					best = to
				}
			}
			if projected[from]-projected[best] <= weight*1.1 {
				continue
			}
			member.desired.Remove(from)
			member.desired.Insert(best)
			claimed[member.replica.GetCollectionID()].Remove(from)
			claimed[member.replica.GetCollectionID()].Insert(best)
			projected[from] -= weight
			projected[best] += weight
		}
	}
}

// placeGroupNode augments the assignment of free nodes of one collection. A
// greedy choice may otherwise consume the only candidate of a later replica.
// Existing ownership is excluded from available, so augmentation never steals
// another replica's RW/RO node before it has drained.
func placeGroupNode(member *groupReplicaAssignment, members []*groupReplicaAssignment,
	projected map[int64]float64, claimed map[int64]typeutil.UniqueSet, visited typeutil.UniqueSet,
) bool {
	candidates := member.available.Collect()
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if projected[a] != projected[b] {
			return projected[a] < projected[b]
		}
		if member.replica.roNodes.Contain(a) != member.replica.roNodes.Contain(b) {
			return member.replica.roNodes.Contain(a)
		}
		return a < b
	})
	collectionID := member.replica.GetCollectionID()
	for _, node := range candidates {
		if member.desired.Contain(node) || visited.Contain(node) {
			continue
		}
		visited.Insert(node)
		if claimed[collectionID].Contain(node) {
			var owner *groupReplicaAssignment
			for _, other := range members {
				if other.replica.GetCollectionID() == collectionID && other.desired.Contain(node) {
					owner = other
					break
				}
			}
			if !placeGroupNode(owner, members, projected, claimed, visited) {
				continue
			}
			owner.desired.Remove(node)
			projected[node] -= groupNodeRows(owner)
		}
		member.desired.Insert(node)
		claimed[collectionID].Insert(node)
		projected[node] += groupNodeRows(member)
		return true
	}
	return false
}
