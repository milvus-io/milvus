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

type replicaLayout struct {
	replicas, nodes []int64
	owners          map[int64]int64
	targets         map[int64][]int64
}
type layoutManager struct {
	domains map[int64]map[string]*replicaLayout
}

// prepare pins targets across progress callbacks and failed/partial Apply. Only
// desired replica membership and eligible topology invalidate a domain.
func (m *layoutManager) prepare(p *planningContext, dirty []qviews.ShardID) []qviews.ShardID {
	if m.domains == nil {
		m.domains = make(map[int64]map[string]*replicaLayout)
	}
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
		domains := m.domains[id]
		if domains == nil {
			domains = make(map[string]*replicaLayout)
			m.domains[id] = domains
		}
		for group := range domains {
			if _, ok := groups[group]; !ok {
				delete(domains, group)
				changed = true
			}
		}
		if len(groups) == 0 {
			delete(m.domains, id)
		}
		for group, replicas := range groups {
			slices.Sort(replicas)
			nodes := slices.Clone(p.CandidateNodes(group))
			slices.Sort(nodes)
			previous := domains[group]
			layout := previous
			if previous == nil || !slices.Equal(previous.nodes, nodes) || !slices.Equal(previous.replicas, replicas) {
				layout = buildReplicaLayout(c, previous, replicas, nodes)
				domains[group] = layout
				changed = true
			}
			for replica, nodes := range layout.targets {
				p.targets[replica] = nodes
			}
		}
		if changed {
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
	// A newly Up survivor can unblock a suspended sibling's drain immediately.
	initial := len(dirty)
	for _, id := range dirty[:initial] {
		if c := p.collectionForShard(id); c != nil && c.LoadConfig() != nil {
			for _, r := range c.LoadConfig().Replicas {
				if nodes, known := p.targets[r.ReplicaID]; known && len(nodes) == 0 {
					dirty = append(dirty, qviews.ShardID{ReplicaID: r.ReplicaID, VChannel: id.VChannel})
				}
			}
		}
	}
	return dirty
}

func buildReplicaLayout(c *balancercache.CollectionEntry, previous *replicaLayout, replicas, nodes []int64) *replicaLayout {
	layout := &replicaLayout{replicas: replicas, nodes: nodes, owners: make(map[int64]int64), targets: make(map[int64][]int64)}
	counts := make(map[int64]int, len(replicas))
	for _, r := range replicas {
		counts[r] = 0
		layout.targets[r] = nil
	}
	for _, n := range nodes {
		var owner int64
		found := false
		if previous != nil {
			owner, found = previous.owners[n]
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
			layout.owners[n] = owner
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
			oldBase := len(previous.nodes) / len(previous.replicas)
			pa, pb := len(previous.targets[a]) > oldBase, len(previous.targets[b]) > oldBase
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
		if _, ok := layout.owners[n]; !ok {
			free = append(free, n)
		}
	}
	assignLayoutNodes(c, layout, counts, quotas, free, false)
	var donors []int64
	for _, n := range nodes {
		if counts[layout.owners[n]] > quotas[layout.owners[n]] {
			donors = append(donors, n)
		}
	}
	assignLayoutNodes(c, layout, counts, quotas, donors, true)
	for _, n := range nodes {
		r := layout.owners[n]
		layout.targets[r] = append(layout.targets[r], n)
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

func assignLayoutNodes(c *balancercache.CollectionEntry, layout *replicaLayout, counts, quotas map[int64]int, candidates []int64, transfer bool) {
	var receivers []int64
	for _, r := range layout.replicas {
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
		owner, assigned := layout.owners[node]
		if transfer {
			if counts[owner] <= quotas[owner] {
				return false
			}
			counts[owner]--
		} else if assigned {
			return false
		}
		layout.owners[node] = replica
		counts[replica]++
		return true
	}
	resources := 0
	for _, node := range layout.nodes {
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
	for _, node := range layout.nodes {
		owner, ok := layout.owners[node]
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
					if transfer && cover[layout.owners[node]][key] == 1 {
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
	if c.DataView() == nil || c.DataView().DataVersion != key.DataVersion || c.ConfigVersion() != key.LoadInfoVersion {
		return -1
	}
	segment, _ := c.DataView().Segment(key.SegmentID)
	if segment == nil || segment.PartitionID != key.PartitionID {
		return -1
	}
	return max(segment.RowNum, 0)
}
