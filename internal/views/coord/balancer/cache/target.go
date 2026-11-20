package cache

import (
	"encoding/binary"
	"maps"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

type targetContribution struct{ rows int64 }

type (
	targetLoad   struct{ rows int64 }
	demandBucket struct{ replicas, rows int64 }
)

func (n *NodeEntry) TargetSize() int64 { return n.info.TargetLogicalSize }
func (n *NodeEntry) TargetContribution(id qviews.ShardID) int64 {
	v, _ := n.targets.getShard(id)
	return v.rows
}
func (s *ShardEntry) Target() *coordview.ViewPlacement { return s.target }
func (c *CollectionEntry) ReplicaSize(replica, node int64) int64 {
	key := replicaNodeKey(replica, node)
	v, _ := c.targetLoads.get(key[:])
	return v.rows
}

// Demand visits replica-count buckets, not collection or segment membership.
func (r *ResourceGroupEntry) Demand(nodes int) int64 {
	var rows int64
	r.demand.each(func(b demandBucket) bool {
		rows += b.rows * min(b.replicas, int64(nodes))
		return true
	})
	return rows
}

func replicaNodeKey(replica, node int64) [16]byte {
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], uint64(replica))
	binary.BigEndian.PutUint64(key[8:], uint64(node))
	return key
}

func demandGroups(c *CollectionEntry) (map[string]int64, int64) {
	groups := make(map[string]int64)
	var rows int64
	if c.config != nil && c.data != nil {
		for _, replica := range c.config.Replicas {
			groups[replica.ResourceGroup]++
		}
		rows = c.LoadedSize()
	}
	return groups, rows
}

func (c *Cache) replaceDemand(old, next *CollectionEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, entry := range []*CollectionEntry{old, next} {
		groups, rows := demandGroups(entry)
		if i == 0 {
			rows = -rows
		}
		for name, replicas := range groups {
			group := c.groupLocked(name)
			key := idKey(replicas)
			bucket, _ := group.demand.get(key[:])
			bucket.replicas = replicas
			bucket.rows += rows
			if bucket.rows == 0 {
				group.demand = group.demand.remove(key[:])
			} else {
				group.demand = group.demand.set(key[:], bucket)
			}
			c.storeGroupLocked(name, group)
		}
	}
}

// PublishReplicaActivity publishes only the retained layout's active selector,
// never speculative segment assignments. It is a no-op for an unchanged layout.
func (c *Cache) PublishReplicaActivity(collection int64, active map[int64]bool) bool {
	slot := c.lockCollection(collection)
	defer c.finishCollection(collection, slot)
	next := *slot.value.Load()
	var suspended immutableIndex[bool]
	for replica, enabled := range active {
		if !enabled {
			key := idKey(replica)
			suspended = suspended.set(key[:], true)
		}
	}
	changed := suspended.len() != next.suspended.len()
	for replica, enabled := range active {
		key := idKey(replica)
		old, _ := next.suspended.get(key[:])
		changed = changed || old == enabled
	}
	if !changed {
		return false
	}
	next.suspended = suspended
	c.refreshTargets(&next, slot.rows)
	slot.value.Store(&next)
	return true
}

func (c *Cache) selectTarget(entry *CollectionEntry, id qviews.ShardID, stats *coordview.ShardStats, fallback, rows map[int64]int64) *coordview.ViewPlacement {
	if stats == nil || entry.config == nil {
		return nil
	}
	key := idKey(id.ReplicaID)
	if suspended, _ := entry.suspended.get(key[:]); suspended {
		return nil
	}
	group, found := "", false
	for _, r := range entry.config.Replicas {
		if r.ReplicaID == id.ReplicaID {
			group, found = r.ResourceGroup, true
			break
		}
	}
	if !found {
		return nil
	}
	eligible := func(node int64) bool {
		n := c.GetNode(node)
		return n != nil && n.info.Alive && !n.info.Stopping && n.info.ResourceGroup == group
	}
	target := stats.PreparingPlacement
	if target != nil {
		for _, node := range stats.PreparingNodes {
			if !eligible(node) {
				target = nil
				break
			}
		}
	}
	if target == nil {
		target = stats.UpPlacement
	}
	if target == nil {
		return nil
	}
	for partition, byNode := range target.PartitionSizes {
		if !entry.LoadsPartition(partition) {
			continue
		}
		for node, count := range byNode {
			rows[node] += count
		}
	}
	for _, segment := range target.LostStats {
		node := target.Assignments[segment]
		if entry.LoadsPartition(stats.Segments[segment].PartitionID) && eligible(node) {
			rows[node] += fallback[segment]
		}
	}
	for node := range rows {
		if !eligible(node) {
			delete(rows, node)
		}
	}
	return target
}

func (c *Cache) refreshTargets(entry *CollectionEntry, fallback map[int64]int64) {
	// Scratch belongs only to this publication. Changed shards receive their own
	// map; unchanged shards retain their immutable object and index path.
	rows := make(map[int64]int64)
	entry.shards.each(func(old *ShardEntry) bool {
		clear(rows)
		c.refreshShardTarget(entry, old, fallback, rows)
		return true
	})
}

func (c *Cache) refreshShardTarget(entry *CollectionEntry, old *ShardEntry, fallback, rows map[int64]int64) bool {
	target := c.selectTarget(entry, old.id, old.stats, fallback, rows)
	if target == old.target && maps.Equal(rows, old.targetRows) {
		return false
	}
	next := *old
	next.target, next.targetRows = target, maps.Clone(rows)
	c.replaceTargetRows(entry, old.id, old.targetRows, next.targetRows)
	entry.shards = entry.shards.set(shardKey(old.id), &next)
	return true
}

func (c *Cache) replaceTarget(entry *CollectionEntry, id qviews.ShardID, old, next *ShardEntry, fallback map[int64]int64) {
	var previous, rows map[int64]int64
	if old != nil {
		previous = old.targetRows
	}
	if next != nil {
		rows = make(map[int64]int64)
		next.target = c.selectTarget(entry, id, next.stats, fallback, rows)
		if maps.Equal(previous, rows) {
			rows = previous
		}
		next.targetRows = rows
	}
	c.replaceTargetRows(entry, id, previous, rows)
}

func (c *Cache) replaceTargetRows(entry *CollectionEntry, id qviews.ShardID, previous, rows map[int64]int64) {
	for node, count := range previous {
		if _, exists := rows[node]; !exists {
			c.replaceTargetContribution(entry, id, node, count, 0)
		}
	}
	for node, count := range rows {
		c.replaceTargetContribution(entry, id, node, previous[node], count)
	}
}

func (c *Cache) replaceTargetContribution(entry *CollectionEntry, id qviews.ShardID, node, previous, rows int64) {
	if previous == rows {
		return
	}
	slot := c.lockNode(node)
	n := *slot.value.Load()
	oldContribution := n.TargetContribution(id)
	n.info.TargetLogicalSize += rows - oldContribution
	if rows == 0 {
		n.targets = n.targets.remove(shardKey(id))
	} else {
		n.targets = n.targets.set(shardKey(id), targetContribution{rows: rows})
	}
	slot.value.Store(&n)
	c.finishNode(node, slot)
	key := replicaNodeKey(id.ReplicaID, node)
	load, _ := entry.targetLoads.get(key[:])
	load.rows += rows - previous
	if load.rows == 0 {
		entry.targetLoads = entry.targetLoads.remove(key[:])
	} else {
		entry.targetLoads = entry.targetLoads.set(key[:], load)
	}
}

// TargetSize shares this immutable shard publication's selected contribution.
func (s *ShardEntry) TargetSize() map[int64]int64 { return s.targetRows }
