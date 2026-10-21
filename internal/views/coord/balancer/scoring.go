package balancer

import (
	"math"
	"sort"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const scoreEpsilon = 1e-9

type searchCursor struct{ next, remaining int }

type placementScore struct{ global, local, fanout float64 }

func (s placementScore) energy(c *BalanceConfig) float64 {
	return c.GlobalWeight*s.global + s.local + c.FanoutPenaltyWeight*s.fanout
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
		return calculateFanoutBudget(nodes, segments, rows, cfg.TargetRowsPerShardNode)
	}
	result := min(previous, cap)
	scale, size := float64(cfg.TargetRowsPerShardNode), float64(rows)
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

func newAllocationContext(p *planningContext, id qviews.ShardID, base map[int64]int64, entries []*SegmentDataView) *allocationContext {
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
	ctx.tolerance = math.Max(float64(cfg.AbsoluteToleranceRows), cfg.RelativeTolerance*ctx.mean)
	shard := p.DataViewForShard(id)
	ctx.fanout = preferredFanout(p.fanouts[id], len(nodes), len(entries), shard.TotalRows, cfg)
	p.fanouts[id] = ctx.fanout
	if ctx.fanout > 0 {
		ctx.shardLimit = (1 + cfg.LocalTolerance) * float64(shard.TotalRows) / float64(ctx.fanout)
	}
	collectionRows := float64(c.DataView().TotalRows)
	if len(nodes) > 0 {
		ctx.collectionLimit = (1 + cfg.LocalTolerance) * math.Max(float64(cfg.TargetRowsPerShardNode), collectionRows/float64(len(nodes)))
	}
	var old map[int64]int64
	if entry := c.GetShard(id); entry != nil {
		old = entry.TargetRows()
	}
	for _, n := range nodes {
		ctx.collectionBase[n] = c.ReplicaRows(id.ReplicaID, n) + p.replicaDelta[id.ReplicaID][n] - old[n]
	}
	for _, segment := range entries {
		ctx.segments[segment.SegmentID] = segment
		ready := make(map[int64]bool)
		for node := range reusableResources(p, id, segment, nodes) {
			ready[node] = true
		}
		ctx.reusable[segment.SegmentID] = ready
	}
	return ctx
}

func (ctx *allocationContext) nodeScore(node int64, rows int64) placementScore {
	r := float64(rows)
	return placementScore{
		global: bandPenalty(float64(ctx.base[node])+r, ctx.mean, ctx.tolerance),
		local: ctx.config.ShardWeight*concentrationPenalty(r, ctx.shardLimit) +
			ctx.config.CollectionWeight*concentrationPenalty(float64(ctx.collectionBase[node])+r, ctx.collectionLimit),
	}
}

func (ctx *allocationContext) score() placementScore {
	var score placementScore
	for _, n := range ctx.nodes {
		s := ctx.nodeScore(n, ctx.rows[n])
		score.global += s.global
		score.local += s.local
	}

	score.fanout = float64(max(0, ctx.opened-ctx.fanout)) * float64(ctx.config.TargetRowsPerShardNode)
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
			rows[old] -= segment.RowNum
			counts[old]--
			cost += ctx.config.MovePrice * float64(segment.RowNum)
		}
		rows[destination] += segment.RowNum
		counts[destination]++
		if !ctx.reusable[segment.SegmentID][destination] {
			cost += ctx.config.LoadPrice * float64(segment.RowNum)
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
	fanout := float64(max(0, ctx.opened-ctx.fanout)-max(0, afterOpened-ctx.fanout)) * float64(ctx.config.TargetRowsPerShardNode)
	return ctx.config.GlobalWeight*global + local + ctx.config.FanoutPenaltyWeight*fanout - cost, global
}

func (ctx *allocationContext) assign(segments []*SegmentDataView, node int64) {
	for _, segment := range segments {
		if previous, ok := ctx.assignments[segment.SegmentID]; ok {
			ctx.rows[previous] -= segment.RowNum
			ctx.counts[previous]--
			if ctx.counts[previous] == 0 {
				ctx.opened--
			}
		}
		ctx.assignments[segment.SegmentID] = node
		ctx.rows[node] += segment.RowNum
		if ctx.counts[node] == 0 {
			ctx.opened++
		}
		ctx.counts[node]++
	}
}

// Search cycles over compound and individual candidates without starving the
// tail when its work budget is smaller than the candidate space.
func (ctx *allocationContext) optimize(entries []*SegmentDataView, cursor *searchCursor) bool {
	if len(entries) == 0 || len(ctx.nodes) == 0 {
		return false
	}
	groups := [][]*SegmentDataView{entries}
	byNode := make(map[int64][]*SegmentDataView)
	for _, segment := range entries {
		node := ctx.assignments[segment.SegmentID]
		byNode[node] = append(byNode[node], segment)
	}
	for _, node := range ctx.nodes {
		if group := byNode[node]; len(group) > 0 {
			groups = append(groups, group)
		}
	}

	total := (len(groups) + len(entries)) * len(ctx.nodes)
	if cursor.remaining <= 0 || cursor.remaining > total {
		cursor.remaining = total
	}
	cursor.next %= total
	limit := min(cursor.remaining, int(ctx.config.MaxCandidateEvaluations))
	for i := 0; i < limit; i++ {
		index := cursor.next
		group, destination := index/len(ctx.nodes), ctx.nodes[index%len(ctx.nodes)]
		var segments []*SegmentDataView
		if group < len(groups) {
			segments = groups[group]
		} else {
			segments = entries[group-len(groups) : group-len(groups)+1]
		}
		gain, global := ctx.evaluate(segments, destination)
		if global >= -scoreEpsilon && gain > ctx.config.MinGainRows {
			ctx.assign(segments, destination)
		}
		cursor.next = (cursor.next + 1) % total
	}
	cursor.remaining -= limit
	return cursor.remaining > 0
}

func (ctx *allocationContext) migrationCost(original map[int64]int64) float64 {
	var cost float64
	for segment, node := range ctx.assignments {
		if old, exists := original[segment]; exists && old == node {
			continue
		}
		rows := float64(ctx.segments[segment].RowNum)
		if _, exists := original[segment]; exists {
			cost += ctx.config.MovePrice * rows
		}
		if !ctx.reusable[segment][node] {
			cost += ctx.config.LoadPrice * rows
		}
	}
	return cost
}

// reusableResources only credits confirmed, protected, exact-compatible loads.
func reusableResources(p *planningContext, id qviews.ShardID, segment *SegmentDataView, nodes []int64) map[int64]coordview.SegmentState {
	c := p.collectionForShard(id)
	states := make(map[int64]coordview.SegmentState)
	if c == nil || c.DataView() == nil {
		return states
	}
	key := coordview.ResourceKey{PartitionID: segment.PartitionID, SegmentID: segment.SegmentID, DataVersion: c.DataView().DataVersion, LoadInfoVersion: c.ConfigVersion()}
	for _, n := range nodes {
		if resources := c.PlacementNode(n); resources != nil && resources.HasResource(key) {
			states[n] = coordview.SegmentStateReady
		}
	}
	return states
}
