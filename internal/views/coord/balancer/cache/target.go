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

func (n *NodeEntry) TargetRows() int64 { return n.info.TargetRowCount }
func (n *NodeEntry) TargetContribution(id qviews.ShardID) int64 {
	v, _ := n.targets.get(shardKey(id))
	return v.rows
}
func (s *ShardEntry) Target() *coordview.ViewPlacement { return s.target }
func (c *CollectionEntry) ReplicaRows(replica, node int64) int64 {
	v, _ := c.targetLoads.get(replicaNodeKey(replica, node))
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

func replicaNodeKey(replica, node int64) []byte {
	var key [16]byte
	binary.BigEndian.PutUint64(key[:8], uint64(replica))
	binary.BigEndian.PutUint64(key[8:], uint64(node))
	return key[:]
}

func demandGroups(c *CollectionEntry) (map[string]int64, int64) {
	groups := make(map[string]int64)
	var rows int64
	if c.config != nil && c.data != nil {
		for _, replica := range c.config.Replicas {
			groups[replica.ResourceGroup]++
		}
		rows = c.data.TotalRows
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
			bucket, _ := group.demand.get(idKey(replicas))
			bucket.replicas = replicas
			bucket.rows += rows
			if bucket.rows == 0 {
				group.demand = group.demand.remove(idKey(replicas))
			} else {
				group.demand = group.demand.set(idKey(replicas), bucket)
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
			suspended = suspended.set(idKey(replica), true)
		}
	}
	changed := suspended.len() != next.suspended.len()
	for replica, enabled := range active {
		old, _ := next.suspended.get(idKey(replica))
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

func (c *Cache) selectTarget(entry *CollectionEntry, id qviews.ShardID, stats *coordview.ShardStats, fallback map[int64]int64) (*coordview.ViewPlacement, map[int64]int64) {
	if stats == nil || entry.config == nil {
		return nil, nil
	}
	if suspended, _ := entry.suspended.get(idKey(id.ReplicaID)); suspended {
		return nil, nil
	}
	group, found := "", false
	for _, r := range entry.config.Replicas {
		if r.ReplicaID == id.ReplicaID {
			group, found = r.ResourceGroup, true
			break
		}
	}
	if !found {
		return nil, nil
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
		return nil, nil
	}
	rows := maps.Clone(target.Rows)
	if rows == nil {
		rows = make(map[int64]int64)
	}
	for _, segment := range target.UnknownRows {
		node := target.Assignments[segment]
		if eligible(node) {
			rows[node] += fallback[segment]
		}
	}
	for node := range rows {
		if !eligible(node) {
			delete(rows, node)
		}
	}
	return target, rows
}

func (c *Cache) refreshTargets(entry *CollectionEntry, fallback map[int64]int64) {
	entry.shards.each(func(old *ShardEntry) bool {
		next := *old
		c.replaceTarget(entry, old.id, old, &next, fallback)
		entry.shards = entry.shards.set(shardKey(old.id), &next)
		return true
	})
}

func (c *Cache) replaceTarget(entry *CollectionEntry, id qviews.ShardID, old, next *ShardEntry, fallback map[int64]int64) {
	var previous, rows map[int64]int64
	if old != nil {
		previous = old.targetRows
	}
	if next != nil {
		next.target, rows = c.selectTarget(entry, id, next.stats, fallback)
		next.targetRows = rows
	}
	changed := make(map[int64]struct{}, len(previous)+len(rows))
	for node := range previous {
		changed[node] = struct{}{}
	}
	for node := range rows {
		changed[node] = struct{}{}
	}
	for node := range changed {
		if previous[node] == rows[node] {
			continue
		}
		slot := c.lockNode(node)
		n := *slot.value.Load()
		oldContribution := n.TargetContribution(id)
		n.info.TargetRowCount += rows[node] - oldContribution
		if rows[node] == 0 {
			n.targets = n.targets.remove(shardKey(id))
		} else {
			n.targets = n.targets.set(shardKey(id), targetContribution{rows: rows[node]})
		}
		slot.value.Store(&n)
		c.finishNode(node, slot)
		key := replicaNodeKey(id.ReplicaID, node)
		load, _ := entry.targetLoads.get(key)
		load.rows += rows[node] - previous[node]
		if load.rows == 0 {
			entry.targetLoads = entry.targetLoads.remove(key)
		} else {
			entry.targetLoads = entry.targetLoads.set(key, load)
		}
	}
}

// TargetRows shares this immutable shard publication's selected contribution.
func (s *ShardEntry) TargetRows() map[int64]int64 { return s.targetRows }
