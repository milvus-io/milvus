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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const collectionRowsRefreshInterval = 30 * time.Second

type recoveryInfoFunc func(context.Context, int64, ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error)

type collectionRowStats struct {
	rows    int64
	valid   bool
	retryAt time.Time
	err     error
}

type replicaGroupKey struct {
	group string
	rg    string
}

// InitCollectionGroups is called once, before recovering metadata or starting
// observers. Configuration only determines the binding of newly created replicas.
func (m *ReplicaManager) InitCollectionGroups(config string, recoveryInfo recoveryInfoFunc) error {
	var groups []struct {
		ID            string   `json:"id"`
		CollectionIDs []string `json:"collectionIds"`
	}
	if err := json.Unmarshal([]byte(config), &groups); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid queryCoord.collectionGroups JSON")
	}
	bindings := make(map[int64]string)
	names := typeutil.NewSet[string]()
	for _, group := range groups {
		if strings.TrimSpace(group.ID) == "" || names.Contain(group.ID) {
			return merr.WrapErrParameterInvalidMsg("collection group ID must be nonempty and unique: %q", group.ID)
		}
		names.Insert(group.ID)
		for _, value := range group.CollectionIDs {
			id, err := strconv.ParseInt(value, 10, 64)
			if err != nil || id <= 0 {
				return merr.WrapErrParameterInvalidMsg("invalid collection ID in group %q: %q", group.ID, value)
			}
			if _, exists := bindings[id]; exists {
				return merr.WrapErrParameterInvalidMsg("collection %d belongs to multiple group entries", id)
			}
			bindings[id] = group.ID
		}
	}
	m.collectionGroups = bindings
	m.recoveryInfo = recoveryInfo
	return nil
}

// getCollectionRows coalesces fills per collection. Neither allocation locks
// nor the cache mutex are held across the DataCoord request.
func (m *ReplicaManager) getCollectionRows(ctx context.Context, collectionID int64) collectionRowStats {
	result, _, _ := m.rowFlights.Do(strconv.FormatInt(collectionID, 10), func() (any, error) {
		m.rowStatsMu.Lock()
		stats := m.rowStats[collectionID]
		m.rowStatsMu.Unlock()
		if time.Now().Before(stats.retryAt) {
			return stats, nil
		}
		if m.recoveryInfo == nil {
			stats.err = merr.WrapErrServiceUnavailableMsg("collection group recovery source is not initialized")
		} else {
			_, segments, err := m.recoveryInfo(ctx, collectionID)
			stats.err = err
			if err == nil {
				var rows int64
				seen := typeutil.NewUniqueSet()
				for _, segment := range segments {
					if seen.Contain(segment.GetID()) {
						continue
					}
					seen.Insert(segment.GetID())
					count := segment.GetNumOfRows()
					if count < 0 || count > math.MaxInt64-rows {
						stats.err = merr.WrapErrServiceInternalMsg("invalid recovery row count for collection %d", collectionID)
						break
					}
					rows += count
				}
				if stats.err == nil {
					stats.rows, stats.valid = rows, true
				}
			}
		}
		stats.retryAt = time.Now().Add(collectionRowsRefreshInterval)
		if stats.err != nil {
			mlog.Warn(ctx, "failed to refresh collection group rows", mlog.FieldCollectionID(collectionID), mlog.Err(stats.err))
		}
		m.rowStatsMu.Lock()
		if _, exists := m.coll2Replicas.Get(collectionID); exists {
			m.rowStats[collectionID] = stats
		}
		m.rowStatsMu.Unlock()
		return stats, nil
	})
	return result.(collectionRowStats)
}

// RecoverNodesInCollections preserves the single-collection caller contract.
// Group membership comes from Replica metadata, never from current configuration.
// Other collections in the same group/RG are expanded here, inside the manager.
func (m *ReplicaManager) RecoverNodesInCollections(ctx context.Context, collectionIDs []int64, rgs map[string]*ResourceGroup) error {
	keys := typeutil.NewSet[replicaGroupKey]()
	requested := typeutil.NewUniqueSet(collectionIDs...)
	for _, id := range requested.Collect() {
		replicas, ok := m.coll2Replicas.Get(id)
		if !ok {
			return merr.WrapErrCollectionNotLoaded(id)
		}
		for _, replica := range replicas {
			if group := replica.GetCollectionGroupID(); group != "" {
				if _, supplied := rgs[replica.GetResourceGroup()]; supplied {
					keys.Insert(replicaGroupKey{group, replica.GetResourceGroup()})
				}
			}
		}
	}
	if keys.Len() == 0 {
		for _, id := range requested.Collect() {
			if err := m.recoverLegacyNodesInCollection(ctx, id, rgs); err != nil {
				return err
			}
		}
		return nil
	}

	nodeSets := make(map[string]typeutil.UniqueSet, len(rgs))
	for name, rg := range rgs {
		nodeSets[name] = typeutil.NewUniqueSet()
		if rg != nil {
			nodeSets[name].Insert(rg.GetNodes()...)
		}
	}
	if err := m.validateResourceGroups(nodeSets); err != nil {
		return err
	}

	// Keep a snapshot of ALL replicas of participating collections, including
	// legacy replicas and RO nodes in other RGs, for collection-level exclusion.
	snapshots := make(map[int64][]*Replica)
	m.coll2Replicas.Range(func(id int64, replicas []*Replica) bool {
		for _, replica := range replicas {
			if requested.Contain(id) || keys.Contain(replicaGroupKey{replica.GetCollectionGroupID(), replica.GetResourceGroup()}) {
				snapshots[id] = replicas
				break
			}
		}
		return true
	})
	ids := make([]int64, 0, len(snapshots))
	stats := make(map[int64]collectionRowStats)
	for id, replicas := range snapshots {
		ids = append(ids, id)
		for _, replica := range replicas {
			if keys.Contain(replicaGroupKey{replica.GetCollectionGroupID(), replica.GetResourceGroup()}) {
				stats[id] = m.getCollectionRows(ctx, id)
				break
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	m.groupMu.Lock()
	defer m.groupMu.Unlock()
	for _, id := range ids {
		m.collLock.Lock(id)
	}
	defer func() {
		for i := len(ids) - 1; i >= 0; i-- {
			m.collLock.Unlock(ids[i])
		}
	}()
	for _, id := range ids {
		current, _ := m.coll2Replicas.Get(id)
		previous := snapshots[id]
		if len(current) != len(previous) {
			return merr.WrapErrServiceUnavailableMsg("replicas changed during collection group recovery")
		}
		for i := range current {
			if current[i] != previous[i] {
				return merr.WrapErrServiceUnavailableMsg("replica changed during collection group recovery")
			}
		}
	}

	// Compute legacy quotas with the original collection helper, keeping grouped
	// replicas in its denominator. Only legacy assignments are applied here.
	legacyQuotas := make(map[int64]int)
	for _, id := range ids {
		allRGs := make(map[string]typeutil.UniqueSet)
		byRG := make(map[string][]*Replica)
		for _, replica := range snapshots[id] {
			rg := replica.GetResourceGroup()
			byRG[rg] = append(byRG[rg], replica)
			allRGs[rg] = nodeSets[rg]
			if allRGs[rg] == nil {
				allRGs[rg] = typeutil.NewUniqueSet()
			}
		}
		helper := newCollectionAssignmentHelper(id, byRG, allRGs)
		var applyErr error
		helper.RangeOverResourceGroup(func(rgHelper *replicasInSameRGAssignmentHelper) {
			if _, supplied := rgs[rgHelper.rgName]; !supplied || applyErr != nil {
				return
			}
			for _, assignment := range rgHelper.replicas {
				replica := assignment.replica
				if replica.GetCollectionGroupID() != "" {
					continue
				}
				legacyQuotas[replica.GetID()] = assignment.expectedNodeCount
				if waitForGroupRG(replica, rgs[rgHelper.rgName]) {
					continue
				}
				ro := assignment.GetNewRONodes()
				recoverable, incoming := assignment.GetRecoverNodesAndIncomingNodeCount()
				rw := append(recoverable, rgHelper.AllocateIncomingNodes(incoming)...)
				if len(ro)+len(rw) == 0 {
					continue
				}
				mutable := replica.CopyForWrite()
				mutable.AddRONode(ro...)
				mutable.AddRWNode(rw...)
				if mutable.RWNodesCount() > 0 {
					mutable.SetWaitRGReadyAt(time.Time{})
				}
				if applyErr = m.put(ctx, id, mutable.IntoReplica()); applyErr != nil {
					return
				}
			}
		})
		if applyErr != nil {
			return applyErr
		}
	}

	orderedKeys := keys.Collect()
	sort.Slice(orderedKeys, func(i, j int) bool {
		if orderedKeys[i].group != orderedKeys[j].group {
			return orderedKeys[i].group < orderedKeys[j].group
		}
		return orderedKeys[i].rg < orderedKeys[j].rg
	})
	for _, key := range orderedKeys {
		all := make(map[int64][]*Replica)
		for _, id := range ids {
			all[id], _ = m.coll2Replicas.Get(id)
		}
		desired := planReplicaGroup(key, all, nodeSets[key.rg], legacyQuotas, stats, rgs[key.rg])
		replicaIDs := make([]int64, 0, len(desired))
		for id := range desired {
			replicaIDs = append(replicaIDs, id)
		}
		sort.Slice(replicaIDs, func(i, j int) bool { return replicaIDs[i] < replicaIDs[j] })
		for _, id := range replicaIDs {
			replica, _ := m.flatReplicas.Get(id)
			nodes := desired[id]
			mutable := replica.CopyForWrite()
			remove := make([]int64, 0)
			add := make([]int64, 0)
			for _, node := range replica.GetRWNodes() {
				if !nodes.Contain(node) {
					remove = append(remove, node)
				}
			}
			for node := range nodes {
				if !replica.rwNodes.Contain(node) {
					add = append(add, node)
				}
			}
			if len(remove)+len(add) == 0 {
				continue
			}
			mutable.AddRONode(remove...)
			mutable.AddRWNode(add...)
			if mutable.RWNodesCount() > 0 {
				mutable.SetWaitRGReadyAt(time.Time{})
			}
			// A single Replica per write avoids the catalog's chunked multi-write
			// semantics. No same-collection ownership is transferred before drain.
			if err := m.put(ctx, replica.GetCollectionID(), mutable.IntoReplica()); err != nil {
				return err
			}
			mlog.Info(ctx, "assigned collection group replica nodes",
				mlog.String("group", key.group), mlog.String("resourceGroup", key.rg),
				mlog.FieldCollectionID(replica.GetCollectionID()), mlog.Int64("replicaID", id),
				mlog.Int64s("newRWNodes", add), mlog.Int64s("newRONodes", remove))
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

func planReplicaGroup(key replicaGroupKey, all map[int64][]*Replica, nodes typeutil.UniqueSet,
	legacyQuotas map[int64]int, stats map[int64]collectionRowStats, rg *ResourceGroup,
) map[int64]typeutil.UniqueSet {
	members := make([]*groupReplicaAssignment, 0)
	capacity := make(map[int64]int)
	fresh := true
	draining := false
	for collectionID, replicas := range all {
		outsiders := typeutil.NewUniqueSet()
		reserved := 0
		for _, replica := range replicas {
			if replica.GetCollectionGroupID() == key.group && replica.GetResourceGroup() == key.rg {
				continue
			}
			occupied := 0
			for _, node := range replica.GetNodes() {
				if nodes.Contain(node) {
					outsiders.Insert(node)
					occupied++
				}
			}
			reserved += max(0, legacyQuotas[replica.GetID()]-occupied)
		}
		capacity[collectionID] = max(0, nodes.Len()-outsiders.Len()-reserved)
		for _, replica := range replicas {
			if replica.GetCollectionGroupID() != key.group || replica.GetResourceGroup() != key.rg || waitForGroupRG(replica, rg) {
				continue
			}
			stat := stats[collectionID]
			// An unknown NEW replica has no allocation weight. Preserve existing
			// replicas during a source outage; never reinterpret missing as zero.
			if !stat.valid && replica.RWNodesCount() == 0 && replica.RONodesCount() == 0 {
				continue
			}
			fresh = fresh && stat.valid && stat.err == nil
			draining = draining || replica.RONodesCount() > 0
			available := nodes.Clone()
			for _, other := range replicas {
				if other.GetID() != replica.GetID() {
					available.Remove(other.GetNodes()...)
				}
			}
			members = append(members, &groupReplicaAssignment{
				replica: replica, rows: stat.rows, available: available,
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
	// Enforce each collection's aggregate capacity, including reserved legacy
	// quotas. Replicas of a collection have the same row weight.
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
