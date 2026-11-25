package cache

import (
	"encoding/binary"
	"slices"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ReplicaFootprint counts shard references, not rows or unique physical bytes.
type ReplicaFootprint struct {
	ReplicaID               int64
	Up, Preparing, Resident int
}

type resourceReference struct {
	key   coordview.ResourceKey
	count int
}

// PlacementNode is immutable and structurally shares unchanged children.
type PlacementNode struct {
	id        int64
	replicas  immutableIndex[ReplicaFootprint]
	resources immutableIndex[resourceReference]
}

func (n *PlacementNode) ID() int64                                    { return n.id }
func (n *PlacementNode) RangeReplicas(fn func(ReplicaFootprint) bool) { n.replicas.each(fn) }
func (n *PlacementNode) ResourceCount() int                           { return n.resources.len() }
func (n *PlacementNode) HasResource(key coordview.ResourceKey) bool {
	encoded := resourceKey(key)
	_, ok := n.resources.get(encoded[:])
	return ok
}

func (n *PlacementNode) RangeResources(fn func(coordview.ResourceKey) bool) {
	n.resources.each(func(r resourceReference) bool { return fn(r.key) })
}

func (c *CollectionEntry) PlacementNode(id int64) *PlacementNode {
	key := idKey(id)
	node, _ := c.placements.get(key[:])
	return node
}

func (c *CollectionEntry) RangePlacementNodes(fn func(*PlacementNode) bool) { c.placements.each(fn) }

func resourceKey(key coordview.ResourceKey) [40]byte {
	var b [40]byte
	for i, value := range []uint64{uint64(key.PartitionID), uint64(key.SegmentID), uint64(key.DataVersion.StreamingVersion), uint64(key.DataVersion.CompactVersion), key.LoadInfoVersion} {
		binary.BigEndian.PutUint64(b[i*8:], value)
	}
	return b
}

func (s *ShardEntry) UpNodes() []int64        { return s.upNodes }
func (s *ShardEntry) PreparingNodes() []int64 { return s.preparingNodes }
func (s *ShardEntry) ResidentNodes() []int64  { return s.residentNodes }

func (c *CollectionEntry) replacePlacements(id qviews.ShardID, old, next *ShardEntry) {
	const (
		up = iota
		preparing
		resident
		footprintCount
	)
	var before, after [footprintCount][]int64
	if old != nil {
		before = [footprintCount][]int64{up: old.upNodes, preparing: old.preparingNodes, resident: old.residentNodes}
	}
	if next != nil {
		after = [footprintCount][]int64{up: next.upNodes, preparing: next.preparingNodes, resident: next.residentNodes}
	}
	replicaDeltas := make(map[int64][footprintCount]int)
	for kind, oldNodes := range before {
		newNodes := after[kind]
		if slices.Equal(oldNodes, newNodes) {
			continue
		}
		for _, node := range oldNodes {
			delta := replicaDeltas[node]
			delta[kind]--
			replicaDeltas[node] = delta
		}
		for _, node := range newNodes {
			delta := replicaDeltas[node]
			delta[kind]++
			replicaDeltas[node] = delta
		}
	}

	changed := make(map[int64]*PlacementNode)
	get := func(id int64) *PlacementNode {
		if n := changed[id]; n != nil {
			return n
		}
		n := &PlacementNode{id: id}
		if current := c.PlacementNode(id); current != nil {
			*n = *current
		}
		changed[id] = n
		return n
	}
	applyResources := func(shard, counterpart *ShardEntry, delta int) {
		if shard == nil {
			return
		}
		for node, resources := range shard.stats.Resources {
			for resource := range resources {
				if counterpart != nil {
					if _, unchanged := counterpart.stats.Resources[node][resource]; unchanged {
						continue
					}
				}
				n := get(node)
				key := resourceKey(resource)
				ref, _ := n.resources.get(key[:])
				ref.key = resource
				ref.count += delta
				if ref.count == 0 {
					n.resources = n.resources.remove(key[:])
				} else {
					n.resources = n.resources.set(key[:], ref)
				}
			}
		}
	}
	applyResources(old, next, -1)
	applyResources(next, old, 1)
	// Apply each node's net change with one write to the immutable replica index.
	for node, delta := range replicaDeltas {
		if delta == ([footprintCount]int{}) {
			continue
		}
		n := get(node)
		key := idKey(id.ReplicaID)
		ref, _ := n.replicas.get(key[:])
		ref.ReplicaID = id.ReplicaID
		ref.Up += delta[up]
		ref.Preparing += delta[preparing]
		ref.Resident += delta[resident]
		if ref.Up == 0 && ref.Preparing == 0 && ref.Resident == 0 {
			n.replicas = n.replicas.remove(key[:])
		} else {
			n.replicas = n.replicas.set(key[:], ref)
		}
	}
	for id, n := range changed {
		key := idKey(id)
		if n.replicas.len() == 0 && n.resources.len() == 0 {
			c.placements = c.placements.remove(key[:])
		} else {
			c.placements = c.placements.set(key[:], n)
		}
	}
}
