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

package assign

// Placement matching TwoLevelBalancer, for new segments (new
// tables, compaction results), recovery and node draining, so a segment is
// placed where balance would keep it.
//
// Largest segment first, each to the node with the lowest final fill
// among those where the table stays within its fair share of segments and
// bytes; else within its tolerance; else the lowest final fill of all. Filling to the share
// first leaves the table even, so balance has nothing to fix afterwards.
// Nodes the load pacer would admit the load to now come first at each
// step, and among them nodes not above the candidates' mean fill by more
// than half the balance tolerance (a node a huge segment leaves
// structurally over the mean should not collect the tables that are short
// there, or leveling takes them off again): a recovery of many segments spreads over nodes instead of queueing
// on the emptiest one, so it finishes sooner. Placement is never throttled
// by balance and does not check the QueryNode's reject line: the QueryNode
// protects itself, and a node that refuses a load is skipped for a while.

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

const PolicyTypeTwoLevel = "two_level"

type twoLevelAssignPolicy struct {
	nodeManager *session.NodeManager
	dist        *meta.DistributionManager
	meta        *meta.Meta
	channels    AssignPolicy
}

func newTwoLevelAssignPolicy(nodeManager *session.NodeManager, scheduler task.Scheduler, dist *meta.DistributionManager, m *meta.Meta) *twoLevelAssignPolicy {
	GetTwoLevelState().SetScheduler(scheduler)
	return &twoLevelAssignPolicy{
		nodeManager: nodeManager,
		dist:        dist,
		meta:        m,
		channels:    newScoreBasedAssignPolicy(nodeManager, scheduler, dist, m),
	}
}

// TableShare is a table's fair share on each node of one replica, in
// segments first and in memory second. Segment counts (the fan-out: each
// segment is a search unit with its own overhead) may stray from the share
// by TableSegmentCountTolerance of it, at least one segment: a table of two segments per
// node is held to one to four, while a table whose segments differ tenfold
// in size needs a node with many small ones to hold more of them for
// memory to level at all. Memory (bytes, as the node level counts it)
// splits in proportion to node capacity, with a tolerance of TableByteTolerance
// of the share and at least the table's largest segment: segments are
// whole, so a node is off its share by up to one segment whatever balance
// does, and a compaction lands a segment several times the average on one
// node; a tolerance below the largest segment makes every compaction cost
// moves of other segments.
type TableShare struct {
	Nodes          []*ViewNode
	FairBytes      []float64 // bytes
	BytesTolerance []float64 // bytes
	FairCount      []float64 // segments, in proportion to capacity
	MinCount       []int     // fewer segments than this is short
	MaxCount       []int     // more segments than this is over
}

// placementFillQuantum: fills closer than this fraction of capacity are
// the same to placement, which then prefers the node with fewer segments.
const placementFillQuantum = 0.005

// NewTableShare computes coll's share over nodes, counting extraCount
// segments of extra bytes about to be placed, the largest maxSeg.
func (v *ClusterView) NewTableShare(coll int64, nodes []*ViewNode, extraBytes float64, extraCount int, maxSeg float64, cfg TwoLevelConfig) TableShare {
	total, capSum, count := extraBytes, 0.0, extraCount
	for _, n := range nodes {
		if cn := v.CollOn(coll, n.ID); cn != nil {
			total += cn.Bytes
			count += cn.Count
		}
		capSum += n.Capacity
	}
	if e := v.Collections[coll]; e != nil {
		maxSeg = max(maxSeg, e.LargestSegment)
	}
	ts := TableShare{
		Nodes: nodes, FairBytes: make([]float64, len(nodes)), BytesTolerance: make([]float64, len(nodes)),
		FairCount: make([]float64, len(nodes)), MinCount: make([]int, len(nodes)), MaxCount: make([]int, len(nodes)),
	}
	for i, n := range nodes {
		part := 1 / float64(max(len(nodes), 1))
		if capSum > 0 {
			part = n.Capacity / capSum
		}
		ts.FairBytes[i] = total * part
		ts.BytesTolerance[i] = max(cfg.TableByteTolerance*ts.FairBytes[i], maxSeg)
		ts.FairCount[i] = float64(count) * part
		slack := max(1, int(math.Ceil(cfg.TableSegmentCountTolerance*ts.FairCount[i])))
		ts.MinCount[i] = max(0, int(math.Floor(ts.FairCount[i]))-slack)
		ts.MaxCount[i] = int(math.Ceil(ts.FairCount[i])) + slack
	}
	return ts
}

// BytesOn and CountOn are coll's final bytes and segments on node.
func (v *ClusterView) BytesOn(coll, node int64) float64 {
	if cn := v.CollOn(coll, node); cn != nil {
		return cn.Bytes
	}
	return 0
}

func (v *ClusterView) CountOn(coll, node int64) int {
	if cn := v.CollOn(coll, node); cn != nil {
		return cn.Count
	}
	return 0
}

func (p *twoLevelAssignPolicy) AssignSegment(ctx context.Context, collectionID int64, segments []*meta.Segment, nodes []int64, forceAssign bool) []SegmentAssignPlan {
	if len(nodes) == 0 || len(segments) == 0 {
		return nil
	}
	cfg := GetTwoLevelConfig()
	st := GetTwoLevelState()
	st.Lock()
	defer st.Unlock()
	view := st.ViewLocked(p.nodeManager, p.dist, p.meta, cfg, false)

	candidates := make([]*ViewNode, 0, len(nodes))
	for _, id := range nodes {
		n, ok := view.Nodes[id]
		if !ok || n.Stopping || n.Streaming {
			continue
		}
		if n.CapacityUnknown {
			// Wait for every node to report its capacity: placing on the
			// nodes known so far would have to be spread again moments later.
			return nil
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		return nil
	}
	// A node that just refused a load (out of memory) is skipped for a while.
	open := make([]*ViewNode, 0, len(candidates))
	for _, n := range candidates {
		if !p.nodeManager.IsResourceExhausted(n.ID) {
			open = append(open, n)
		}
	}
	if len(open) > 0 {
		candidates = open
	}

	sizes := make([]float64, len(segments))
	var extra, maxSeg float64
	extraCount := 0
	for i, s := range segments {
		sizes[i] = SegmentBytes(s)
		maxSeg = max(maxSeg, sizes[i])
		if _, ok := st.PendingPlacement(s.GetID(), candidates); !ok {
			extra += sizes[i]
			extraCount++
		}
	}
	share := view.NewTableShare(collectionID, candidates, extra, extraCount, maxSeg, cfg)

	order := make([]int, len(segments))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return sizes[order[a]] > sizes[order[b]] })

	var mean float64
	for _, n := range candidates {
		mean += n.Fill()
	}
	ceiling := mean / float64(len(candidates)) * (1 + cfg.NodeTolerance*cfg.CeilingRatio)

	plans := make([]SegmentAssignPlan, 0, len(segments))
	now := time.Now()
	for _, i := range order {
		s, sz := segments[i], sizes[i]
		// The segment checker asks again every round until the load is done;
		// answer with the placement already in flight.
		if to, ok := st.PendingPlacement(s.GetID(), candidates); ok {
			plans = append(plans, SegmentAssignPlan{Segment: s, From: -1, To: to})
			continue
		}
		// A tier is how far the table may grow on a node: within its fair
		// share in segments and bytes, then within its tolerance, then
		// anywhere. pick is the lowest final fill among nodes in the
		// tier, skipping nodes with a full load queue unless queued is set,
		// and nodes above the ceiling unless high is.
		type tier struct {
			slack    func(j int) float64
			maxCount func(j int) int
		}
		tiers := []tier{
			{func(int) float64 { return 0 }, func(j int) int { return int(math.Ceil(share.FairCount[j])) }},
			{func(j int) float64 { return share.BytesTolerance[j] }, func(j int) int { return share.MaxCount[j] }},
			{func(int) float64 { return math.Inf(1) }, func(int) int { return math.MaxInt }},
		}
		pick := func(t tier, queued, high bool) *ViewNode {
			var best *ViewNode
			var bestFill float64
			for j, n := range candidates {
				if (!queued && !n.CanTake(sz)) || (!high && n.Fill() > ceiling) ||
					view.CountOn(collectionID, n.ID)+1 > t.maxCount(j) ||
					view.BytesOn(collectionID, n.ID)+sz > share.FairBytes[j]+t.slack(j) {
					continue
				}
				// Once every node's load queue is full (queued), the node with
				// the shortest queue takes the segment: queues drain at the
				// speed of their node, so a fast node keeps getting more and a
				// slow one (a straggler) only as much as it finishes. Queue
				// length is measured in rounds of the node's own budget, so a
				// node the pacer has throttled (small budget) does not look
				// idle next to one running many loads at once. Ranking by fill
				// here would hand the straggler the most, since its fill rises
				// slowest.
				if queued && best != nil {
					if q, bq := queueRounds(n, cfg), queueRounds(best, cfg); q != bq {
						if q < bq {
							best, bestFill = n, (n.Final+sz)/n.Capacity
						}
						continue
					}
				}
				// Lowest final fill; nodes within placementFillQuantum of each
				// other count as equally full and the one with fewer segments
				// wins. Without this, tiny segments (thousands of small tables)
				// all land on one node: each barely moves its fill, so the same
				// node stays the emptiest while its segment count runs away.
				f := (n.Final + sz) / n.Capacity
				if best == nil || f < bestFill-placementFillQuantum ||
					(f <= bestFill+placementFillQuantum && n.Segments < best.Segments) {
					best, bestFill = n, f
				}
			}
			return best
		}
		var to *ViewNode
		pass := "slot"
	passes:
		for _, queued := range []bool{false, true} {
			if queued {
				pass = "queued"
			}
			for _, high := range []bool{false, true} {
				for _, t := range tiers {
					if to = pick(t, queued, high); to != nil {
						break passes
					}
				}
			}
		}
		metrics.QueryCoordPlacementTotal.WithLabelValues(pass).Inc()
		st.RecordPlan(&LedgerEntry{Collection: collectionID, Segment: s.GetID(), From: -1, To: to.ID, Bytes: sz, At: now})
		plans = append(plans, SegmentAssignPlan{Segment: s, From: -1, To: to.ID})
	}
	return plans
}

func (p *twoLevelAssignPolicy) AssignChannel(ctx context.Context, collectionID int64, channels []*meta.DmChannel, nodes []int64, forceAssign bool) []ChannelAssignPlan {
	return p.channels.AssignChannel(ctx, collectionID, channels, nodes, forceAssign)
}

func queueRounds(n *ViewNode, _ TwoLevelConfig) float64 { return n.QueueRounds() }
