package cache

import (
	"encoding/binary"

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
	_, ok := n.resources.get(resourceKey(key))
	return ok
}

func (n *PlacementNode) RangeResources(fn func(coordview.ResourceKey) bool) {
	n.resources.each(func(r resourceReference) bool { return fn(r.key) })
}

func (c *CollectionEntry) PlacementNode(id int64) *PlacementNode {
	node, _ := c.placements.get(idKey(id))
	return node
}
func (c *CollectionEntry) RangePlacementNodes(fn func(*PlacementNode) bool) { c.placements.each(fn) }

func resourceKey(key coordview.ResourceKey) []byte {
	var b [40]byte
	for i, value := range []uint64{uint64(key.PartitionID), uint64(key.SegmentID), uint64(key.DataVersion.StreamingVersion), uint64(key.DataVersion.CompactVersion), key.LoadInfoVersion} {
		binary.BigEndian.PutUint64(b[i*8:], value)
	}
	return b[:]
}

func (s *ShardEntry) UpNodes() []int64        { return s.upNodes }
func (s *ShardEntry) PreparingNodes() []int64 { return s.preparingNodes }
func (s *ShardEntry) ResidentNodes() []int64  { return s.residentNodes }

func (c *CollectionEntry) replacePlacements(id qviews.ShardID, old, next *ShardEntry) {
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
	apply := func(shard, counterpart *ShardEntry, delta int) {
		if shard == nil {
			return
		}
		adjust := func(nodes []int64, kind int) {
			for _, node := range nodes {
				n := get(node)
				key := idKey(id.ReplicaID)
				ref, _ := n.replicas.get(key)
				ref.ReplicaID = id.ReplicaID
				switch kind {
				case 0:
					ref.Up += delta
				case 1:
					ref.Preparing += delta
				case 2:
					ref.Resident += delta
				}
				if ref.Up == 0 && ref.Preparing == 0 && ref.Resident == 0 {
					n.replicas = n.replicas.remove(key)
				} else {
					n.replicas = n.replicas.set(key, ref)
				}
			}
		}
		adjust(shard.upNodes, 0)
		adjust(shard.preparingNodes, 1)
		adjust(shard.residentNodes, 2)
		for node, resources := range shard.stats.Resources {
			for resource := range resources {
				if counterpart != nil {
					if _, unchanged := counterpart.stats.Resources[node][resource]; unchanged {
						continue
					}
				}
				n := get(node)
				key := resourceKey(resource)
				ref, _ := n.resources.get(key)
				ref.key = resource
				ref.count += delta
				if ref.count == 0 {
					n.resources = n.resources.remove(key)
				} else {
					n.resources = n.resources.set(key, ref)
				}
			}
		}
	}
	apply(old, next, -1)
	apply(next, old, 1)
	for id, n := range changed {
		if n.replicas.len() == 0 && n.resources.len() == 0 {
			c.placements = c.placements.remove(idKey(id))
		} else {
			c.placements = c.placements.set(idKey(id), n)
		}
	}
}
