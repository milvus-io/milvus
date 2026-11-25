package balancer

import (
	"math"
	"sort"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const scoreEpsilon = 1e-9

type placementScore struct{ global, local, fanout float64 }

func (s placementScore) energy(c *BalanceConfig) float64 {
	return c.ResourceGroupWeight*s.global + s.local + c.FanoutWeight*s.fanout
}

// Squaring distance after conversion avoids integer multiplication overflow.
func bandPenalty(rows, mean, tolerance float64) float64 {
	if mean <= 0 {
		return 0
	}
	distance := math.Max(0, math.Abs(rows-mean)-tolerance)
	return distance * (distance / (2 * mean))
}

func concentrationPenalty(rows, limit float64) float64 {
	if limit <= 0 {
		return 0
	}
	excess := math.Max(0, rows-limit)
	return excess * (excess / (2 * limit))
}

func calculateFanoutBudget(nodes, segments int, rows, scale int64) int {
	if nodes == 0 || segments == 0 {
		return 0
	}
	count := 1
	if rows > 0 {
		count = int(1 + (rows-1)/scale)
	}
	return min(nodes, segments, count)
}

func preferredFanout(previous, nodes, segments int, rows int64, cfg *BalanceConfig) int {
	cap := min(nodes, segments)
	if cap == 0 {
		return 0
	}
	if previous == 0 {
		return calculateFanoutBudget(nodes, segments, rows, cfg.TargetLogicalSizePerNode)
	}
	result := min(previous, cap)
	scale, size := float64(cfg.TargetLogicalSizePerNode), float64(rows)
	for result < cap && size > float64(result)*scale*(1+cfg.FanoutHysteresis) {
		result++
	}
	for result > 1 && size < float64(result-1)*scale*(1-cfg.FanoutHysteresis) {
		result--
	}
	return result
}

type allocationContext struct {
	opened                                       int
	config                                       *BalanceConfig
	nodes                                        []int64
	base, collectionBase                         map[int64]int64
	rows                                         map[int64]int64
	counts                                       map[int64]int
	assignments                                  map[int64]int64
	segments                                     map[int64]*SegmentDataView
	reusable                                     map[int64]map[int64]bool
	mean, tolerance, shardLimit, collectionLimit float64
	fanout                                       int
}

func newAllocationContext(p *planningContext, id qviews.ShardID, base map[int64]int64, entries []*SegmentDataView, previousFanout int) *allocationContext {
	c := p.collectionForShard(id)
	replica := findReplica(c.LoadConfig(), id.ReplicaID)
	nodes, _ := p.TargetNodes(id)
	eligible := make([]int64, 0, len(nodes))
	for _, node := range nodes {
		if info := p.nodes[node]; info != nil && info.Alive && !info.Stopping && info.ResourceGroup == replica.ResourceGroup {
			eligible = append(eligible, node)
		}
	}
	nodes = eligible
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	cfg := p.GetBalanceConfig()
	ctx := &allocationContext{
		config: cfg, nodes: nodes, base: base, collectionBase: make(map[int64]int64),
		rows: make(map[int64]int64), counts: make(map[int64]int), assignments: make(map[int64]int64),
		segments: make(map[int64]*SegmentDataView), reusable: make(map[int64]map[int64]bool),
	}
	rgNodes := p.CandidateNodes(replica.ResourceGroup)
	if group := p.GetResourceGroup(replica.ResourceGroup); group != nil && len(rgNodes) > 0 {
		ctx.mean = float64(group.Demand(len(rgNodes))) / float64(len(rgNodes))
	}
	ctx.tolerance = math.Max(float64(cfg.ResourceGroupAbsoluteTolerance), cfg.ResourceGroupRelativeTolerance*ctx.mean)
	shardRows := c.ShardLoadedSize(id.VChannel)
	if p.recovery != nil && p.recovery.shardID == id {
		shardRows = p.recovery.shard.LogicalSize
	}
	ctx.fanout = preferredFanout(previousFanout, len(nodes), len(entries), shardRows, cfg)
	if ctx.fanout > 0 {
		ctx.shardLimit = (1 + cfg.ConcentrationRelativeTolerance) * float64(shardRows) / float64(ctx.fanout)
	}
	collectionRows := float64(c.LoadedSize())
	if len(nodes) > 0 {
		ctx.collectionLimit = (1 + cfg.ConcentrationRelativeTolerance) * math.Max(float64(cfg.TargetLogicalSizePerNode), collectionRows/float64(len(nodes)))
	}
	var old map[int64]int64
	if entry := c.GetShard(id); entry != nil {
		old = entry.TargetSize()
	}
	for _, n := range nodes {
		ctx.collectionBase[n] = c.ReplicaSize(id.ReplicaID, n) + p.replicaDelta[id.ReplicaID][n] - old[n]
	}
	// Reuse evidence and its version must come from the same pinned collection.
	// Published-schema recovery can still allocate without a current DataView,
	// but cannot claim compatible resources in that case.
	data := c.DataView()
	key := coordview.ResourceKey{LoadInfoVersion: c.ConfigVersion()}
	var placements []*balancercache.PlacementNode
	if data != nil {
		key.DataVersion = data.DataVersion
		// Pin each eligible node once instead of looking it up for every segment.
		placements = make([]*balancercache.PlacementNode, len(nodes))
		for i, node := range nodes {
			placements[i] = c.PlacementNode(node)
		}
	}
	for _, segment := range entries {
		ctx.segments[segment.SegmentID] = segment
		if data != nil {
			key.PartitionID, key.SegmentID = segment.PartitionID, segment.SegmentID
			ctx.reusable[segment.SegmentID] = reusableResources(placements, key)
		}
	}
	return ctx
}

func (ctx *allocationContext) nodeScore(node int64, rows int64) placementScore {
	r := float64(rows)
	return placementScore{
		global: bandPenalty(float64(ctx.base[node])+r, ctx.mean, ctx.tolerance),
		local: ctx.config.ShardConcentrationWeight*concentrationPenalty(r, ctx.shardLimit) +
			ctx.config.CollectionConcentrationWeight*concentrationPenalty(float64(ctx.collectionBase[node])+r, ctx.collectionLimit),
	}
}

func (ctx *allocationContext) score() placementScore {
	var score placementScore
	for _, n := range ctx.nodes {
		s := ctx.nodeScore(n, ctx.rows[n])
		score.global += s.global
		score.local += s.local
	}

	score.fanout = float64(max(0, ctx.opened-ctx.fanout)) * float64(ctx.config.TargetLogicalSizePerNode)
	return score
}

// evaluate calculates exact marginal penalties on changed nodes. It never
// mutates assignments or bills temporary search prefixes as actual loading.
func (ctx *allocationContext) evaluate(segments []*SegmentDataView, destination int64) (float64, float64) {
	rows := make(map[int64]int64)
	counts := make(map[int64]int)
	var cost float64
	for _, segment := range segments {
		old, exists := ctx.assignments[segment.SegmentID]
		if exists && old == destination {
			continue
		}
		if exists {
			rows[old] -= segment.LogicalSize
			counts[old]--
			cost += ctx.config.MoveCostPerUnit * float64(segment.LogicalSize)
		}
		rows[destination] += segment.LogicalSize
		counts[destination]++
		if !ctx.reusable[segment.SegmentID][destination] {
			cost += ctx.config.LoadCostPerUnit * float64(segment.LogicalSize)
		}
	}
	var global, local float64
	for node, delta := range rows {
		before, after := ctx.nodeScore(node, ctx.rows[node]), ctx.nodeScore(node, ctx.rows[node]+delta)
		global += before.global - after.global
		local += before.local - after.local
	}
	afterOpened := ctx.opened
	for node, delta := range counts {
		if ctx.counts[node] > 0 && ctx.counts[node]+delta == 0 {
			afterOpened--
		}
		if ctx.counts[node] == 0 && ctx.counts[node]+delta > 0 {
			afterOpened++
		}
	}
	fanout := float64(max(0, ctx.opened-ctx.fanout)-max(0, afterOpened-ctx.fanout)) * float64(ctx.config.TargetLogicalSizePerNode)
	return ctx.config.ResourceGroupWeight*global + local + ctx.config.FanoutWeight*fanout - cost, global
}

func (ctx *allocationContext) assign(segments []*SegmentDataView, node int64) {
	for _, segment := range segments {
		if previous, ok := ctx.assignments[segment.SegmentID]; ok {
			ctx.rows[previous] -= segment.LogicalSize
			ctx.counts[previous]--
			if ctx.counts[previous] == 0 {
				ctx.opened--
			}
		}
		ctx.assignments[segment.SegmentID] = node
		ctx.rows[node] += segment.LogicalSize
		if ctx.counts[node] == 0 {
			ctx.opened++
		}
		ctx.counts[node]++
	}
}

// optimize considers compound moves before individual segments. Entries are
// in publication order (largest first), including after partition filtering.
func (ctx *allocationContext) optimize(entries []*SegmentDataView) {
	if len(entries) == 0 || len(ctx.nodes) < 2 {
		return
	}
	byNode := make(map[int64][]*SegmentDataView)
	for _, segment := range entries {
		node := ctx.assignments[segment.SegmentID]
		byNode[node] = append(byNode[node], segment)
	}

	ctx.tryMoves(entries)
	for _, node := range ctx.nodes {
		if group := byNode[node]; len(group) > 0 {
			ctx.tryMoves(group)
		}
	}
	for i, segment := range entries {
		if (i == 0 || segment.LogicalSize != entries[i-1].LogicalSize) &&
			ctx.singleMoveGainBound(segment.LogicalSize) <= ctx.config.MinScoreGain {
			break
		}
		ctx.tryMoves(entries[i : i+1])
	}
}

func (ctx *allocationContext) tryMoves(segments []*SegmentDataView) {
	for _, destination := range ctx.nodes {
		gain, global := ctx.evaluate(segments, destination)
		if global >= -scoreEpsilon && gain > ctx.config.MinScoreGain {
			ctx.assign(segments, destination)
		}
	}
}

// singleMoveGainBound bounds the gain from any single move of at most size
// at the current placement. Only two nodes can improve the RG penalty, only
// the source can improve concentration, and at most one node can close.
// Omitting nonnegative move/load costs makes the bound conservative.
func (ctx *allocationContext) singleMoveGainBound(size int64) float64 {
	var global, local float64
	for _, node := range ctx.nodes {
		rows := ctx.rows[node]
		before := ctx.nodeScore(node, rows)
		low := rows - min(size, rows)
		load := ctx.base[node] + rows
		// Saturate the hypothetical increase for destinations already full.
		high := load + min(size, math.MaxInt64-max(int64(0), load))
		nearest := min(max(ctx.mean, float64(ctx.base[node]+low)), float64(high))
		global = max(global, before.global-bandPenalty(nearest, ctx.mean, ctx.tolerance))
		local = max(local, before.local-ctx.nodeScore(node, low).local)
	}
	bound := ctx.config.ResourceGroupWeight*(2*global) + local
	if ctx.opened > ctx.fanout {
		bound += ctx.config.FanoutWeight * float64(ctx.config.TargetLogicalSizePerNode)
	}
	return bound
}

func (ctx *allocationContext) migrationCost(original map[int64]int64) float64 {
	var cost float64
	for segment, node := range ctx.assignments {
		if old, exists := original[segment]; exists && old == node {
			continue
		}
		rows := float64(ctx.segments[segment].LogicalSize)
		if _, exists := original[segment]; exists {
			cost += ctx.config.MoveCostPerUnit * rows
		}
		if !ctx.reusable[segment][node] {
			cost += ctx.config.LoadCostPerUnit * rows
		}
	}
	return cost
}

// reusableResources only credits confirmed, protected, exact-compatible loads.
func reusableResources(placements []*balancercache.PlacementNode, key coordview.ResourceKey) map[int64]bool {
	states := make(map[int64]bool)
	for _, node := range placements {
		if node != nil && node.HasResource(key) {
			states[node.ID()] = true
		}
	}
	return states
}
