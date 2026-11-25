package balancer

import (
	"slices"
	"sort"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const (
	layoutEdgeBudget     = 65536
	layoutResourceBudget = 1_000_000
)

// prepareLayouts reuses each pinned collection's targets across batches.
func prepareLayouts(p *planningContext, dirty []qviews.ShardID) []qviews.ShardID {
	collections := make(map[int64]*balancercache.CollectionEntry)
	for _, id := range dirty {
		if c := p.collectionForShard(id); c != nil {
			collections[c.ID()] = c
		}
	}
	for id, c := range collections {
		groups := make(map[string][]int64)
		if c.LoadConfig() != nil {
			for _, r := range c.LoadConfig().Replicas {
				groups[r.ResourceGroup] = append(groups[r.ResourceGroup], r.ReplicaID)
			}
		}
		changed := false
		for group, replicas := range groups {
			slices.Sort(replicas)
			nodes := slices.Clone(p.CandidateNodes(group))
			slices.Sort(nodes)
			previous := c.Planning().Layout(group)
			layout := previous
			if previous == nil || !slices.Equal(previous.Nodes, nodes) || !slices.Equal(previous.Replicas, replicas) {
				layout = buildReplicaLayout(c, previous, replicas, nodes)
				c.Planning().StoreLayout(group, layout)
				changed = true
			}
			for replica, nodes := range layout.Targets {
				p.targets[replica] = nodes
			}
		}
		if changed && p.boundedBatch {
			p.changedLayouts = append(p.changedLayouts, id)
		} else if changed {
			c.RangeShards(func(s *balancercache.ShardEntry) bool { dirty = append(dirty, s.ID()); return true })
			if c.DataView() != nil {
				for _, replicas := range groups {
					for _, replica := range replicas {
						for _, shard := range c.DataView().Shards {
							dirty = append(dirty, qviews.ShardID{ReplicaID: replica, VChannel: shard.VChannel})
						}
					}
				}
			}
		}
	}
	if publisher, ok := p.Reader.(interface {
		PublishReplicaActivity(*balancercache.CollectionEntry, map[int64]bool) bool
	}); ok {
		changed := false
		for _, c := range collections {
			active := make(map[int64]bool)
			if c.LoadConfig() != nil {
				for _, r := range c.LoadConfig().Replicas {
					active[r.ReplicaID] = len(p.targets[r.ReplicaID]) > 0
				}
			}
			changed = publisher.PublishReplicaActivity(c, active) || changed
		}
		if changed {
			p.refreshTargetInputs()
		}
	}

	return dirty
}

func buildReplicaLayout(c *balancercache.CollectionEntry, previous *balancercache.ReplicaLayout, replicas, nodes []int64) *balancercache.ReplicaLayout {
	layout := &balancercache.ReplicaLayout{Replicas: replicas, Nodes: nodes, Owners: make(map[int64]int64), Targets: make(map[int64][]int64)}
	counts := make(map[int64]int, len(replicas))
	for _, r := range replicas {
		counts[r] = 0
		layout.Targets[r] = nil
	}
	for _, n := range nodes {
		var owner int64
		found := false
		if previous != nil {
			owner, found = previous.Owners[n]
			if _, valid := counts[owner]; !valid {
				found = false
			}
		} else if placement := c.PlacementNode(n); placement != nil {
			var best balancercache.ReplicaFootprint
			placement.RangeReplicas(func(ref balancercache.ReplicaFootprint) bool {
				if _, valid := counts[ref.ReplicaID]; !valid || (ref.Up == 0 && ref.Preparing == 0) {
					return true
				}
				if !found || ref.Up > best.Up || (ref.Up == best.Up && (ref.Preparing > best.Preparing || (ref.Preparing == best.Preparing && layoutTie(c.ID(), ref.ReplicaID, n) < layoutTie(c.ID(), best.ReplicaID, n)))) {
					owner, found, best = ref.ReplicaID, true, ref
				}
				return true
			})
		}
		if found {
			layout.Owners[n] = owner
			counts[owner]++
		}
	}
	base, extra := len(nodes)/len(replicas), len(nodes)%len(replicas)
	serving := make(map[int64]bool, len(replicas))
	eligible := make(map[int64]bool, len(nodes))
	for _, node := range nodes {
		eligible[node] = true
	}
	for _, replica := range replicas {
		serving[replica] = replicaServing(c, replica, eligible)
	}
	rank := slices.Clone(replicas)
	sort.Slice(rank, func(i, j int) bool {
		a, b := rank[i], rank[j]
		ga, gb := counts[a] > base, counts[b] > base
		if ga != gb {
			return ga
		}
		if previous != nil {
			oldBase := len(previous.Nodes) / len(previous.Replicas)
			pa, pb := len(previous.Targets[a]) > oldBase, len(previous.Targets[b]) > oldBase
			if pa != pb {
				return pa
			}
		}
		if serving[a] != serving[b] {
			return serving[a]
		}
		return layoutTie(c.ID(), a, 0) < layoutTie(c.ID(), b, 0)
	})
	quotas := make(map[int64]int, len(replicas))
	for i, r := range rank {
		quotas[r] = base
		if i < extra {
			quotas[r]++
		}
	}
	var free []int64
	for _, n := range nodes {
		if _, ok := layout.Owners[n]; !ok {
			free = append(free, n)
		}
	}
	assignLayoutNodes(c, layout, counts, quotas, free, false)
	var donors []int64
	for _, n := range nodes {
		if counts[layout.Owners[n]] > quotas[layout.Owners[n]] {
			donors = append(donors, n)
		}
	}
	assignLayoutNodes(c, layout, counts, quotas, donors, true)
	for _, n := range nodes {
		r := layout.Owners[n]
		layout.Targets[r] = append(layout.Targets[r], n)
	}
	return layout
}

func replicaServing(c *balancercache.CollectionEntry, replica int64, eligible map[int64]bool) bool {
	if c.DataView() == nil || len(c.DataView().Shards) == 0 {
		return false
	}
	for _, data := range c.DataView().Shards {
		shard := c.GetShard(qviews.ShardID{ReplicaID: replica, VChannel: data.VChannel})
		if shard == nil || shard.Stats().UpVersion == nil {
			return false
		}
		for _, node := range shard.UpNodes() {
			if !eligible[node] {
				return false
			}
		}
	}
	return true
}

// SplitMix's integer mixer gives reproducible ties without favoring low IDs.
func layoutTie(collection, replica, node int64) uint64 {
	x := uint64(collection)*0x9e3779b97f4a7c15 ^ uint64(replica)*0xbf58476d1ce4e5b9 ^ uint64(node)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

type layoutEdge struct {
	node, replica int64
	cost          float64
	tie           uint64
}

func assignLayoutNodes(c *balancercache.CollectionEntry, layout *balancercache.ReplicaLayout, counts, quotas map[int64]int, candidates []int64, transfer bool) {
	var receivers []int64
	for _, r := range layout.Replicas {
		if counts[r] < quotas[r] {
			receivers = append(receivers, r)
		}
	}
	if len(candidates) == 0 || len(receivers) == 0 {
		return
	}
	accept := func(node, replica int64) bool {
		if counts[replica] >= quotas[replica] {
			return false
		}
		owner, assigned := layout.Owners[node]
		if transfer {
			if counts[owner] <= quotas[owner] {
				return false
			}
			counts[owner]--
		} else if assigned {
			return false
		}
		layout.Owners[node] = replica
		counts[replica]++
		return true
	}
	resources := 0
	for _, node := range layout.Nodes {
		if n := c.PlacementNode(node); n != nil {
			resources += n.ResourceCount()
		}
	}
	if len(candidates) > layoutEdgeBudget/len(receivers) || resources > layoutResourceBudget/(len(receivers)+1) {
		// Budget exhaustion only sacrifices the last-priority loading estimate.
		receiver := 0
		for _, node := range candidates {
			for receiver < len(receivers) && counts[receivers[receiver]] == quotas[receivers[receiver]] {
				receiver++
			}
			if receiver == len(receivers) {
				break
			}
			accept(node, receivers[receiver])
		}
		return
	}
	cover := make(map[int64]map[coordview.ResourceKey]int)
	for _, node := range layout.Nodes {
		owner, ok := layout.Owners[node]
		if !ok {
			continue
		}
		n := c.PlacementNode(node)
		if n == nil {
			continue
		}
		if cover[owner] == nil {
			cover[owner] = make(map[coordview.ResourceKey]int)
		}
		n.RangeResources(func(key coordview.ResourceKey) bool {
			if resourceRows(c, key) >= 0 {
				cover[owner][key]++
			}
			return true
		})
	}
	edges := make([]layoutEdge, 0, len(candidates)*len(receivers))
	for _, node := range candidates {
		for _, receiver := range receivers {
			cost := float64(0)
			if n := c.PlacementNode(node); n != nil {
				n.RangeResources(func(key coordview.ResourceKey) bool {
					rows := resourceRows(c, key)
					if rows < 0 {
						return true
					}
					if transfer && cover[layout.Owners[node]][key] == 1 {
						cost += float64(rows)
					}
					if cover[receiver][key] == 0 {
						cost -= float64(rows)
					}
					return true
				})
			}
			edges = append(edges, layoutEdge{node: node, replica: receiver, cost: cost, tie: layoutTie(c.ID(), receiver, node)})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.cost != b.cost {
			return a.cost < b.cost
		}
		if a.tie != b.tie {
			return a.tie < b.tie
		}
		if a.node != b.node {
			return a.node < b.node
		}
		return a.replica < b.replica
	})
	for _, edge := range edges {
		accept(edge.node, edge.replica)
	}
}

func resourceRows(c *balancercache.CollectionEntry, key coordview.ResourceKey) int64 {
	if !c.LoadsPartition(key.PartitionID) || c.DataView() == nil || c.DataView().DataVersion != key.DataVersion || c.ConfigVersion() != key.LoadInfoVersion {
		return -1
	}
	segment, _ := c.DataView().Segment(key.SegmentID)
	if segment == nil || segment.PartitionID != key.PartitionID {
		return -1
	}
	return max(segment.LogicalSize, 0)
}
