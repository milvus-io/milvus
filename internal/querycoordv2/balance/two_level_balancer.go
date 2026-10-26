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

package balance

// TwoLevelBalancer, planned once per round for the whole cluster,
// in bytes of estimated segment memory (see assign/two_level_view.go for the
// two numbers each node has: peak and final).
//
// The cluster is judged by three terms, in priority order:
//
//  1. node memory: within a resource group every node's final fill should
//     be within the band (NodeTolerance of the mean fill) around the mean;
//  2. table segments: on every node of a replica a table should hold its
//     fair number of segments (in proportion to the node's capacity), give
//     or take TableSegmentCountTolerance of it and at least one;
//  3. table memory: likewise its fair share of bytes, give or take
//     TableByteTolerance of it and at least the table's largest segment.
//
// The potential of the cluster is the sum of how far each node is outside
// its band, how many segments each table has outside its bounds and how
// many bytes outside its tolerance, compared term by term. An action is a
// move of one segment or a swap of two: an exchange within a table (its
// larger segment out, a smaller one in: bytes move, counts stay) or a swap
// across tables. Each round the planner applies, for each group and then
// for each table with a violation, the action that lowers the potential
// most, as long as some action lowers it at all; ties go to the action
// that moves fewer bytes. Lowering the potential strictly at every step
// means balance in a static cluster converges and then stops: no action
// undoes another.
//
// One potential, not two, keeps the terms from fighting at the edge of
// the band: leveling and table actions see the same node term, and a table
// action may not raise it by any amount (a quantum of leak per move would
// still let the tables walk a node out of its band by the width of a small
// segment at a time, for leveling to walk it back). A node that table
// actions have pushed to the edge of its band blocks the tables on it, so
// the round moves such a node back into the inner half of its band, with
// actions that keep every node inside its band and every table at its
// share: the inner band is a second, lower term of the potential, and the
// tables go on from there next round. Leveling proper (a node outside the
// band) may hurt tables, since memory comes first.
//
// A move only targets a node the load pacer would admit it to now (its
// bytes budget and load slots; any loads count, so recovery keeps its
// bandwidth; recovery itself is never limited), never takes the target past
// the QueryNode's reject line, and
// never touches a segment balance moved within Cooldown or one a task is
// acting on. When the node a table is shortest on has its budget in
// flight, the table waits for it rather than filling a node that is nearly
// at its share.

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// GlobalBalancer plans segment moves for the whole cluster in one call. The
// balance checker calls it once per round; BalanceReplica then only balances
// channels.
type GlobalBalancer interface {
	BalanceGlobal(ctx context.Context, m *meta.Meta) []assign.SegmentAssignPlan
}

type TwoLevelBalancer struct {
	*ScoreBasedBalancer
	nodeManager *session.NodeManager
}

func NewTwoLevelBalancer(scheduler task.Scheduler, nodeManager *session.NodeManager, dist *meta.DistributionManager,
	targetMgr meta.TargetManagerInterface,
) *TwoLevelBalancer {
	assign.GetTwoLevelState().SetScheduler(scheduler)
	return &TwoLevelBalancer{
		ScoreBasedBalancer: NewScoreBasedBalancer(scheduler, nodeManager, dist, targetMgr),
		nodeManager:        nodeManager,
	}
}

func (b *TwoLevelBalancer) GetAssignPolicy() assign.AssignPolicy {
	return assign.GetGlobalAssignPolicyFactory().GetPolicy(assign.PolicyTypeTwoLevel)
}

// BalanceReplica only balances channels; segments are planned globally.
func (b *TwoLevelBalancer) BalanceReplica(ctx context.Context, replica *meta.Replica) ([]assign.SegmentAssignPlan, []assign.ChannelAssignPlan) {
	if replica.NodesCount() < 2 || !paramtable.Get().QueryCoordCfg.AutoBalanceChannel.GetAsBool() {
		return nil, nil
	}
	return nil, b.balanceChannels(ctx, NewBalanceReport(), replica)
}

func (b *TwoLevelBalancer) BalanceGlobal(ctx context.Context, m *meta.Meta) (plans []assign.SegmentAssignPlan) {
	cfg := assign.GetTwoLevelConfig()
	st := assign.GetTwoLevelState()
	start := time.Now()
	// Balancing is optional work: a planning bug costs one round, never the
	// checker loop, placement or recovery.
	defer func() {
		if r := recover(); r != nil {
			mlog.Warn(ctx, "two level balance planning panicked, round skipped", mlog.Any("panic", r))
			plans = nil
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			mlog.Warn(ctx, "two level balance planning slow", mlog.Duration("elapsed", elapsed))
		}
	}()
	// The lock is held only to read: a fresh view of its own and the move
	// history for cooldowns. Planning runs off the lock, so placement and
	// recovery (which share the state) never wait on a long round; the
	// plans are recorded under the lock at the end.
	st.Lock()
	view := st.BuildViewLocked(b.nodeManager, b.dist, m, cfg)
	cool := st.CooldownSnapshot()
	cursor := st.TableCursor()
	st.Unlock()
	p := &planner{
		ctx:         ctx,
		cfg:         cfg,
		st:          st,
		view:        view,
		cool:        cool,
		cursor:      cursor,
		m:           m,
		target:      b.targetMgr,
		nodeManager: b.nodeManager,
		now:         time.Now(),
		loaded:      map[int64]bool{},
		reject:      assign.RejectRatio(),
		reasons:     make(map[string]int),
		skips:       make(map[string]int),
		moved:       make(map[[2]int64]bool),
		cands:       make(map[int64][]cand),
		tcands:      make(map[[2]int64][]cand),
		replicas:    make(map[[2]int64]*meta.Replica),
		repNodes:    make(map[int64][]*assign.ViewNode),
		gstats:      make(map[string]*groupStat),
		shares:      make(map[[2]int64]*tableShare),
		blocked:     make(map[int64]bool),
	}
	p.groupNodes = p.groups()
	names := make([]string, 0, len(p.groupNodes))
	for g := range p.groupNodes {
		names = append(names, g)
	}
	sort.Strings(names)
	// Memory first, then tables: a table action never raises the node term.
	for _, g := range names {
		p.levelNodes(g)
	}
	p.balanceTables()
	// A node parked at the edge of its band blocks the tables on it (their
	// moves would take it outside) and does not trigger leveling (it is not
	// outside yet). Moving it back toward the mean gives the tables room.
	if len(p.blocked) > 0 {
		for _, g := range names {
			p.roomMoves(g)
		}
	}

	st.Lock()
	for _, e := range p.entries {
		st.RecordPlan(e)
	}
	st.SetTableCursor(p.cursor)
	p.publishMetrics(time.Since(start))
	st.Unlock()
	if len(p.plans) > 0 {
		mlog.Info(ctx, "two level balance planned",
			mlog.Int("plans", len(p.plans)),
			mlog.Any("reasons", p.reasons),
			mlog.Any("skips", p.skips),
			mlog.Duration("elapsed", time.Since(start)),
			mlog.Int("collections", len(view.Collections)))
	} else if p.skips["level_stuck"]+p.skips["table_stuck"] > 0 {
		mlog.RatedInfo(ctx, rate.Limit(1.0/60), "two level balance cannot improve further",
			mlog.Any("skips", p.skips), mlog.Int("collections", len(view.Collections)))
	}
	return p.plans
}

// publishMetrics turns the round's counters and the view into metrics:
// plans and skips by reason, planning time, each node's deviation from its
// group in bands, its segments and loads in flight, and the tables found
// off their share.
func (p *planner) publishMetrics(elapsed time.Duration) {
	for reason, n := range p.reasons {
		metrics.QueryCoordBalancePlanTotal.WithLabelValues(reason).Add(float64(n))
	}
	for cause, n := range p.skips {
		metrics.QueryCoordBalanceSkipTotal.WithLabelValues(cause).Add(float64(n))
	}
	metrics.QueryCoordBalancePlanLatency.Observe(float64(elapsed.Milliseconds()))
	metrics.QueryCoordBalanceTablesOffShare.Set(float64(p.tablesOffShare))
	seen := make(map[int64]bool, len(p.view.Nodes))
	for _, n := range p.view.Nodes {
		seen[n.ID] = true
		id := fmt.Sprint(n.ID)
		metrics.QueryCoordBalanceNodeSegments.WithLabelValues(id).Set(float64(n.Segments))
		metrics.QueryCoordBalanceNodeInflightLoads.WithLabelValues(id).Set(float64(n.Inflight))
		if !n.Usable() {
			metrics.QueryCoordBalanceNodeDeviation.DeleteLabelValues(id)
			continue
		}
		g := p.groupStat(n.Group)
		if g.band > 0 {
			metrics.QueryCoordBalanceNodeDeviation.WithLabelValues(id).Set((n.Fill() - g.mean) / g.band)
		}
	}
	for id := range p.st.NodesWithMetrics(seen) {
		metrics.CleanQueryCoordMetricsWithNodeID(id)
	}
}

// planner holds one round's working state.
type planner struct {
	ctx         context.Context
	cfg         assign.TwoLevelConfig
	st          *assign.TwoLevelState
	view        *assign.ClusterView
	m           *meta.Meta
	target      meta.TargetManagerInterface
	nodeManager *session.NodeManager
	now         time.Time
	reject      float64

	plans    []assign.SegmentAssignPlan
	reasons  map[string]int
	skips    map[string]int
	moved    map[[2]int64]bool            // (segment, node) copies moved this round
	cands    map[int64][]cand             // node -> its segments, largest first
	tcands   map[[2]int64][]cand          // (node, collection) -> its segments, largest first
	replicas map[[2]int64]*meta.Replica   // (collection, node) -> replica
	repNodes map[int64][]*assign.ViewNode // replica -> its usable RW nodes

	groupNodes map[string][]*assign.ViewNode // resource group -> usable nodes
	gstats     map[string]*groupStat         // resource group -> mean and band
	shares     map[[2]int64]*tableShare      // (collection, replica) -> share
	blocked    map[int64]bool                // nodes whose term stopped a table action

	tablesOffShare int // (table, replica) pairs found outside their share this round

	loaded map[int64]bool // collection -> its load has completed (unknown counts as loaded)

	cool    map[[2]int64]time.Time // (segment, node) -> when balance moved the copy there
	cursor  int64                  // round-robin position over tables
	entries []*assign.LedgerEntry  // this round's plans, recorded under the lock at the end
}

// inCooldown reports whether balance moved this copy within the cooldown.
func (p *planner) inCooldown(s *meta.Segment) bool {
	t, ok := p.cool[[2]int64{s.GetID(), s.Node}]
	return ok && p.now.Sub(t) < p.cfg.Cooldown
}

// isLoaded reports whether coll's load has completed. A table still loading
// is left alone, as master's balance checker leaves it: its share is not
// known yet, and moving its segments competes with the loads that bring
// the rest of it in. A collection the meta does not know (tests) is taken
// as loaded.
func (p *planner) isLoaded(coll int64) bool {
	if v, ok := p.loaded[coll]; ok {
		return v
	}
	v := true
	if p.m != nil && p.m.CollectionManager != nil {
		if c := p.m.GetCollection(p.ctx, coll); c != nil {
			v = c.GetStatus() == querypb.LoadStatus_Loaded
		}
	}
	p.loaded[coll] = v
	return v
}

type cand struct {
	s    *meta.Segment
	coll int64
	sz   float64 // bytes
}

// groups returns the usable nodes of each resource group.
func (p *planner) groups() map[string][]*assign.ViewNode {
	out := make(map[string][]*assign.ViewNode)
	for _, n := range p.view.Nodes {
		if n.Usable() {
			out[n.Group] = append(out[n.Group], n)
		}
	}
	for _, nodes := range out {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	}
	return out
}

// band is the tolerated spread of final fill among nodes.
func (p *planner) band(nodes []*assign.ViewNode) float64 {
	var mean, top float64
	for _, n := range nodes {
		mean += n.Fill()
		top = math.Max(top, n.Fill())
	}
	mean /= float64(len(nodes))
	tol := p.cfg.NodeTolerance
	if top > p.cfg.HighWater {
		tol = p.cfg.HighWaterTolerance
	}
	// A nearly empty cluster has no memory to level: differences below
	// bandFloor of capacity never matter, so the band is at least that.
	// Not larger: a cluster at a few percent of capacity still wants its
	// nodes within a tenth of each other (a scale-out that leaves the new
	// nodes at half the others' rows is unbalanced however small the
	// numbers), and swaps level finer than a segment.
	return math.Max(tol*mean, bandFloor)
}

// bandFloor is the least band, as a fraction of capacity.
const bandFloor = 0.001

func (p *planner) meanFill(nodes []*assign.ViewNode) float64 {
	var mean float64
	for _, n := range nodes {
		mean += n.Fill()
	}
	return mean / float64(max(len(nodes), 1))
}

// canTakeMove: balance only adds a load to a node the pacer would admit it
// to now (a free load slot and bytes budget), so a move starts at once
// instead of queueing behind other loads, and not to one that just refused
// a load.
func (p *planner) canTakeMove(n *assign.ViewNode) bool {
	return n.CanTake(0) && !p.nodeManager.IsResourceExhausted(n.ID)
}

func (p *planner) movable(s *meta.Segment, node int64) bool {
	return !p.moved[[2]int64{s.GetID(), node}] && !p.view.Busy(s.GetID(), node) && !p.view.Planned(s.GetID(), node) &&
		!p.view.Unsized(s) && p.isLoaded(s.GetCollectionID()) &&
		!p.inCooldown(s) &&
		p.target.CanSegmentBeMoved(p.ctx, s.GetCollectionID(), s.GetID())
}

// levelCands is the slice of a node's candidates leveling looks at: the
// largest LevelCandidates of them. A node with a hundred thousand tiny
// segments is leveled by its big ones; evaluating every small one against
// every target would cost a round seconds for nothing.
func (p *planner) levelCands(node int64) []cand {
	c := p.candidates(node)
	if k := p.cfg.LevelCandidates; k > 0 && len(c) > k {
		return c[:k]
	}
	return c
}

// candidates returns node's segments, largest first (built once per round).
func (p *planner) candidates(node int64) []cand {
	if c, ok := p.cands[node]; ok {
		return c
	}
	var c []cand
	for _, coll := range p.view.NodeColls[node] {
		cn := p.view.CollOn(coll, node)
		if cn == nil {
			continue
		}
		for _, s := range cn.Segments {
			c = append(c, cand{s: s, coll: coll, sz: p.view.SizeOf(s)})
		}
	}
	sort.Slice(c, func(i, j int) bool { return c[i].sz > c[j].sz })
	p.cands[node] = c
	return c
}

func (p *planner) replicaOf(coll, node int64) *meta.Replica {
	k := [2]int64{coll, node}
	if r, ok := p.replicas[k]; ok {
		return r
	}
	r := p.m.GetByCollectionAndNode(p.ctx, coll, node)
	p.replicas[k] = r
	return r
}

// replicaNodes are the replica's RW nodes that are usable, in its group.
func (p *planner) replicaNodes(r *meta.Replica, group string) []*assign.ViewNode {
	if nodes, ok := p.repNodes[r.GetID()]; ok {
		return nodes
	}
	var nodes []*assign.ViewNode
	r.RangeOverRWNodes(func(id int64) bool {
		if n, ok := p.view.Nodes[id]; ok && n.Usable() && n.Group == group {
			nodes = append(nodes, n)
		}
		return true
	})
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	p.repNodes[r.GetID()] = nodes
	return nodes
}

// plan applies an action to the view and records it.
func (p *planner) plan(x cand, from, to *assign.ViewNode, reason string) {
	r := p.replicaOf(x.coll, from.ID)
	p.entries = append(p.entries, &assign.LedgerEntry{
		Collection: x.coll, Segment: x.s.GetID(), From: from.ID, To: to.ID, Bytes: x.sz, At: p.now,
	})
	p.view.ApplyPlan(x.coll, x.s.GetID(), from.ID, to.ID, x.sz)
	p.moved[[2]int64{x.s.GetID(), from.ID}] = true
	p.reasons[reason]++
	p.plans = append(p.plans, assign.SegmentAssignPlan{Segment: x.s, From: from.ID, To: to.ID, Replica: r, Reason: reason})
	delete(p.shares, [2]int64{x.coll, r.GetID()})
}

// --- potential ----------------------------------------------------------

// potential is how far the cluster is from balanced: nodes outside their
// band (in bands), segments outside their tables' bounds, bytes outside
// their tables' tolerances (in tolerances). Compared term by term.
type potential struct{ node, count, bytes float64 }

// quanta below which a change in a term does not count: a move must close
// at least this much of a band or a tolerance to be worth a task.
const (
	nodeQuantum  = 0.01
	bytesQuantum = 0.01
)

func (d potential) add(o potential) potential {
	return potential{d.node + o.node, d.count + o.count, d.bytes + o.bytes}
}

// better reports whether the change d lowers the potential more than o
// does; a change lowers it when its first non-negligible term is negative.
func (d potential) better(o potential) bool {
	for _, t := range [3][3]float64{{d.node, o.node, nodeQuantum}, {d.count, o.count, 0.5}, {d.bytes, o.bytes, bytesQuantum}} {
		a, b := quantize(t[0], t[2]), quantize(t[1], t[2])
		if a != b {
			return a < b
		}
	}
	return false
}

// lowers reports whether d lowers the potential at all.
func (d potential) lowers() bool { return d.better(potential{}) }

// tableClass grades what d does to the tables: 0 helps one, 1 hurts none,
// 2 hurts one. Leveling prefers the lower class: a level move that also
// brings a table to its share is one table balance need not make, and one
// that pushes a table past its share is one it cannot undo (it may not
// raise the node term), so the table would stay off its share.
func (d potential) tableClass() int {
	c, b := quantize(d.count, 0.5), quantize(d.bytes, bytesQuantum)
	switch {
	case c < 0 || (c == 0 && b < 0):
		return 0
	case c == 0 && b == 0:
		return 1
	}
	return 2
}

func quantize(x, q float64) float64 {
	if math.Abs(x) < q {
		return 0
	}
	return x
}

// nodeTerm is how far a fill is outside the band (half-width slack around
// the mean), in bands. Leveling and table actions use the same slack: two
// zones would be two potentials, and the one could undo the other.
func nodeTerm(fill, mean, band, slack float64) float64 {
	return math.Max(0, math.Abs(fill-mean)-slack) / band
}

// groupStat is a resource group's mean fill and band for the round. Moves
// within a group keep its mean, so only the two nodes' terms change.
type groupStat struct {
	nodes []*assign.ViewNode
	mean  float64
	band  float64
}

func (p *planner) groupStat(name string) *groupStat {
	if g, ok := p.gstats[name]; ok {
		return g
	}
	nodes := p.groupNodes[name]
	g := &groupStat{nodes: nodes, mean: p.meanFill(nodes), band: p.band(nodes)}
	p.gstats[name] = g
	return g
}

// deltaNode is the change of the node term when net bytes go from one
// node to another of the same group. A table action that would raise it
// marks the node it would push out as blocked (see roomMoves).
func (p *planner) deltaNode(from, to *assign.ViewNode, net float64, leveling bool) float64 {
	g := p.groupStat(from.Group)
	if g.band <= 0 || len(g.nodes) < 2 {
		return 0
	}
	slack := g.band / 2
	f0, t0 := nodeTerm(from.Fill(), g.mean, g.band, slack), nodeTerm(to.Fill(), g.mean, g.band, slack)
	f1, t1 := nodeTerm((from.Final-net)/from.Capacity, g.mean, g.band, slack), nodeTerm((to.Final+net)/to.Capacity, g.mean, g.band, slack)
	if !leveling && f1+t1 > f0+t0 {
		if f1 > f0 && f0 == 0 {
			p.blocked[from.ID] = true
		}
		if t1 > t0 && t0 == 0 {
			p.blocked[to.ID] = true
		}
	}
	return f1 + t1 - f0 - t0
}

// tableShare is a table's share over a replica's nodes for the round.
type tableShare struct {
	share assign.TableShare
	idx   map[int64]int // node -> index in share.Nodes
}

func (p *planner) tableShare(coll int64, r *meta.Replica, group string) *tableShare {
	k := [2]int64{coll, r.GetID()}
	if ts, ok := p.shares[k]; ok {
		return ts
	}
	nodes := p.replicaNodes(r, group)
	ts := &tableShare{share: p.view.NewTableShare(coll, nodes, 0, 0, 0, p.cfg), idx: make(map[int64]int, len(nodes))}
	for i, n := range nodes {
		ts.idx[n.ID] = i
	}
	p.shares[k] = ts
	return ts
}

// term is a node's table terms: segments outside the bounds, bytes outside
// the tolerance in tolerances. With outer set the bounds are the trigger
// bounds (half a tolerance more, and one more segment where the bounds
// have room): balance starts on a table only past those, and then brings
// it inside the plain bounds, so the small drift of ingestion and
// compaction does not keep it busy.
func (ts *tableShare) term(i int, count int, bytes float64, outer bool) (float64, float64) {
	sh := &ts.share
	extra, slack := 0, 0.0
	if outer {
		slack = 0.5
		// One more segment of hysteresis, unless the bounds are already
		// at their floor of one segment: for a small table one segment is
		// the whole resolution (5/5/5 against 0/0/0 must be fixed).
		if sh.MaxCount[i]-int(math.Ceil(sh.FairCount[i])) > 1 {
			extra = 1
		}
	}
	tol := math.Max(sh.BytesTolerance[i], 1)
	c := math.Max(0, float64(count-sh.MaxCount[i]-extra)) + math.Max(0, float64(sh.MinCount[i]-extra-count))
	b := math.Max(0, math.Abs(bytes-sh.FairBytes[i])-tol*(1+slack)) / tol
	return c, b
}

// deltaTable is the change of a table's terms when count segments and
// bytes go from one node to another of its replica.
// small reports whether coll is a small table on replica r: fewer than
// SmallTableSegmentsPerNode segments per node, so its share is one or two
// segments a node and there is nothing for balance to even out. Placement
// still spreads a small table's segments as they arrive; balance never
// moves them for the table's sake, and node leveling moves them freely.
// With a million tiny tables this is what keeps a round bounded.
func (p *planner) small(coll int64, r *meta.Replica, group string) bool {
	if p.cfg.SmallTableSegmentsPerNode <= 0 {
		return false
	}
	ts := p.tableShare(coll, r, group)
	for _, fc := range ts.share.FairCount {
		if fc >= p.cfg.SmallTableSegmentsPerNode {
			return false
		}
	}
	return true
}

func (p *planner) deltaTable(coll int64, r *meta.Replica, from, to *assign.ViewNode, count int, bytes float64) potential {
	if p.small(coll, r, from.Group) {
		return potential{}
	}
	ts := p.tableShare(coll, r, from.Group)
	fi, ok1 := ts.idx[from.ID]
	ti, ok2 := ts.idx[to.ID]
	if !ok1 || !ok2 {
		return potential{node: 0, count: math.Inf(1), bytes: math.Inf(1)} // not a replica node: never
	}
	cf, bf := p.view.CountOn(coll, from.ID), p.view.BytesOn(coll, from.ID)
	ct, bt := p.view.CountOn(coll, to.ID), p.view.BytesOn(coll, to.ID)
	c0f, b0f := ts.term(fi, cf, bf, false)
	c0t, b0t := ts.term(ti, ct, bt, false)
	c1f, b1f := ts.term(fi, cf-count, bf-bytes, false)
	c1t, b1t := ts.term(ti, ct+count, bt+bytes, false)
	return potential{count: c1f + c1t - c0f - c0t, bytes: b1f + b1t - b0f - b0t}
}

// --- actions --------------------------------------------------------------

// action is a move of x from a to b, or with y set a swap that brings y
// back from b to a.
type action struct {
	x, y  *cand
	from  *assign.ViewNode
	to    *assign.ViewNode
	delta potential
	moved float64 // bytes moved, the tie breaker
}

// evaluate computes the change of potential an action makes, or false
// when it is not feasible or does not lower the potential.
func (p *planner) evaluate(x, y *cand, from, to *assign.ViewNode, leveling bool) (action, bool) {
	kind := "table_"
	if leveling {
		kind = "level_"
	}
	a, why := p.score(x, y, from, to, leveling)
	if why == "" && !leveling && a.delta.node > 0 {
		why = "node_term" // no leak: a table action never pushes a node out of its band
	}
	if why == "" && !a.delta.lowers() {
		switch {
		case quantize(a.delta.node, nodeQuantum) > 0:
			why = "node_term"
		case quantize(a.delta.count, 0.5) > 0:
			why = "count_term"
		case quantize(a.delta.bytes, bytesQuantum) > 0:
			why = "bytes_term"
		default:
			why = "no_gain"
		}
	}
	if why != "" {
		p.skips[kind+why]++ // why actions are rejected, counted per round for the planning log
		return a, false
	}
	return a, true
}

// score computes the change of potential an action makes, or why it is
// not feasible.
func (p *planner) score(x, y *cand, from, to *assign.ViewNode, leveling bool) (action, string) {
	a := action{x: x, y: y, from: from, to: to}
	reject := func(why string) (action, string) { return a, why }
	if !p.movable(x.s, from.ID) || (y != nil && !p.movable(y.s, to.ID)) {
		return reject("immovable")
	}
	rx := p.replicaOf(x.coll, from.ID)
	if rx == nil || !rx.ContainRWNode(to.ID) {
		return reject("replica")
	}
	net, ysz := x.sz, 0.0
	if y != nil {
		ysz = y.sz
		net = x.sz - y.sz
		ry := p.replicaOf(y.coll, to.ID)
		if ry == nil || !ry.ContainRWNode(from.ID) {
			return reject("replica")
		}
	}
	// Peaks: each side must hold what it receives before it releases.
	if !to.FitsPeak(x.sz, p.reject) || (y != nil && !from.FitsPeak(ysz, p.reject)) {
		return reject("peak")
	}
	// A leveling move may leave the receiver fuller than the giver: the
	// potential alone decides (it must still fall), which is what lets two
	// nodes holding one and two giant segments trade toward the best split
	// a single move at a time (prod_4_a5f7b0c5: 1.56 -> 1.31 max/mean).
	a.delta.node = p.deltaNode(from, to, net, leveling)
	if y != nil && y.coll == x.coll {
		a.delta = a.delta.add(p.deltaTable(x.coll, rx, from, to, 0, net))
	} else {
		a.delta = a.delta.add(p.deltaTable(x.coll, rx, from, to, 1, x.sz))
		if y != nil {
			dy := p.deltaTable(y.coll, p.replicaOf(y.coll, to.ID), to, from, 1, y.sz)
			// A table's fix may not break a bystander: a swap partner's table
			// stays as good as it was, or that table would swap it back.
			if !leveling && (quantize(dy.count, 0.5) > 0 || quantize(dy.bytes, bytesQuantum) > 0) {
				return reject("bystander")
			}
			a.delta = a.delta.add(dy)
		}
	}
	a.moved = x.sz + ysz
	return a, ""
}

// consider keeps the better of best and a (an action with no segment is
// no action). Leveling ranks actions by what they do to the tables first
// (see tableClass), then by the potential.
func consider(best *action, a action, leveling bool) {
	if a.x == nil || !a.delta.lowers() {
		return
	}
	if best.x == nil {
		*best = a
		return
	}
	if leveling && a.delta.tableClass() != best.delta.tableClass() {
		if a.delta.tableClass() < best.delta.tableClass() {
			*best = a
		}
		return
	}
	if a.delta.better(best.delta) || (!best.delta.better(a.delta) && a.moved < best.moved) {
		*best = a
	}
}

func (p *planner) apply(a action, reason string) {
	p.plan(*a.x, a.from, a.to, reason)
	if a.y != nil {
		p.plan(*a.y, a.to, a.from, reason)
	}
	delete(p.gstats, a.from.Group)
}

// scanLimit bounds the candidates examined for one action.
const scanLimit = 2048

// tableCands returns a table's segments on node, largest first (built once
// per round from candidates).
func (p *planner) tableCands(node, coll int64) []cand {
	k := [2]int64{node, coll}
	if c, ok := p.tcands[k]; ok {
		return c
	}
	var c []cand
	for _, x := range p.candidates(node) {
		if x.coll == coll {
			c = append(c, x)
		}
	}
	p.tcands[k] = c
	return c
}

// bestMove finds the single move from one node to another that lowers the
// potential most, among the given segments (largest first).
func (p *planner) bestMove(segs []cand, from, to *assign.ViewNode, leveling bool) action {
	var best action
	for i := range segs {
		x := &segs[i]
		if i >= scanLimit {
			break
		}
		if a, ok := p.evaluate(x, nil, from, to, leveling); ok {
			consider(&best, a, leveling)
		}
	}
	return best
}

// bestSwap finds the swap of a segment going from one node for one coming
// back that lowers the potential most: the first SwapCandidates of xs
// (largest first) against the ys nearest to each in size.
func (p *planner) bestSwap(xs, ys []cand, from, to *assign.ViewNode, leveling bool) action {
	var best action
	k := p.cfg.SwapCandidates
	for i := range xs {
		x := &xs[i]
		if i >= k {
			break
		}
		// ys nearest in size to x, both sides.
		j := sort.Search(len(ys), func(j int) bool { return ys[j].sz < x.sz })
		lo, hi := j-1, j
		for n := 0; n < k && (lo >= 0 || hi < len(ys)); n++ {
			var y *cand
			if hi >= len(ys) || (lo >= 0 && x.sz-ys[lo].sz < ys[hi].sz-x.sz) {
				y, lo = &ys[lo], lo-1
			} else {
				y, hi = &ys[hi], hi+1
			}
			if y.s.GetID() == x.s.GetID() {
				continue
			}
			if a, ok := p.evaluate(x, y, from, to, leveling); ok {
				consider(&best, a, leveling)
			}
		}
	}
	return best
}

// bestExchange finds the exchange within one table that lowers the
// potential most: its larger segments on from against its smaller ones on
// to. Counts stay; bytes move. This is how a node holding a table's
// largest segments gives up bytes while its segment count is at the bound.
func (p *planner) bestExchange(from, to *assign.ViewNode, coll int64) action {
	var best action
	xs, ys := p.tableCands(from.ID, coll), p.tableCands(to.ID, coll)
	k := p.cfg.SwapCandidates
	for i := 0; i < len(xs) && i < k; i++ {
		for j := len(ys) - 1; j >= 0 && j >= len(ys)-k; j-- {
			if ys[j].sz >= xs[i].sz {
				break
			}
			if a, ok := p.evaluate(&xs[i], &ys[j], from, to, false); ok {
				consider(&best, a, false)
			}
		}
	}
	return best
}

// --- 1. node leveling -------------------------------------------------------

// levelNodes lowers the node term of a group: while a node is outside the
// band, from nodes above the mean to nodes below it, the move or swap that
// lowers the potential most, until none does.
func (p *planner) levelNodes(name string) {
	for iter := 0; iter < 8*len(p.groupNodes[name])+32; iter++ {
		g := p.groupStat(name)
		if len(g.nodes) < 2 || g.band <= 0 {
			return
		}
		// While any node is outside the band, every node above the mean may
		// give and every node below it may take.
		var his, los []*assign.ViewNode
		outside := false
		for _, n := range g.nodes {
			outside = outside || nodeTerm(n.Fill(), g.mean, g.band, g.band/2) > 0
			if n.Fill() > g.mean {
				his = append(his, n)
			} else if n.Fill() < g.mean && p.canTakeMove(n) {
				los = append(los, n)
			}
		}
		if !outside || len(his) == 0 {
			return
		}
		sort.Slice(his, func(i, j int) bool { return his[i].Fill() > his[j].Fill() })
		sort.Slice(los, func(i, j int) bool { return los[i].Fill() < los[j].Fill() })
		var best action
		for _, hi := range his[:min(4, len(his))] {
			for _, lo := range los[:min(4, len(los))] {
				consider(&best, p.bestMove(p.levelCands(hi.ID), hi, lo, true), true)
				if best.x == nil && p.canTakeMove(hi) {
					consider(&best, p.bestSwap(p.levelCands(hi.ID), p.levelCands(lo.ID), hi, lo, true), true)
				}
			}
		}
		if best.x == nil {
			if len(los) == 0 {
				p.skips["level_target_busy"]++
			} else {
				p.skips["level_stuck"]++
			}
			return
		}
		if best.delta.tableClass() == 2 {
			p.skips["level_hurt_table"]++ // memory first: a table left its share for a node outside the band
		}
		if best.y == nil {
			p.apply(best, "level")
		} else {
			p.apply(best, "level_swap")
		}
	}
}

// roomMoves gives the blocked nodes of a group room. A node that table
// actions have parked at the edge of its band (their next move would take
// it outside, so they wait) is moved back toward the mean, into the inner
// half of the band, with moves that keep every node inside its band (the
// node term stays zero) and every table at its share: tables come first
// inside the band, so the move must not undo what they did, and it costs
// them nothing. The inner band is a second, lower potential term: a move
// must lower it by a quantum, so a node already inside it is left alone,
// and the pass ends when no move lowers it further.
func (p *planner) roomMoves(name string) {
	for iter := 0; iter < 8*len(p.groupNodes[name])+32; iter++ {
		g := p.groupStat(name)
		if len(g.nodes) < 2 || g.band <= 0 {
			return
		}
		var best action
		bestGain := 0.0
		for _, n := range g.nodes {
			if !p.blocked[n.ID] {
				continue
			}
			for _, m := range g.nodes {
				if m.ID == n.ID {
					continue
				}
				var a action
				var gain float64
				switch {
				case n.Fill() > g.mean && m.Fill() < g.mean && p.canTakeMove(m):
					a, gain = p.bestRoom(n, m)
				case n.Fill() < g.mean && m.Fill() > g.mean && p.canTakeMove(n):
					a, gain = p.bestRoom(m, n)
				default:
					continue
				}
				if a.x != nil && (gain > bestGain || gain == bestGain && a.moved < best.moved) {
					best, bestGain = a, gain
				}
			}
		}
		if best.x == nil {
			p.skips["room_none"]++
			return
		}
		p.apply(best, "level_room")
	}
}

// bestRoom is the move from one node to another that lowers the inner
// band term (how far the two are outside the inner half of the band) most,
// by at least a quantum, while keeping the node term at zero and hurting
// no table; ties go to fewer bytes. It returns the move and the gain.
func (p *planner) bestRoom(from, to *assign.ViewNode) (action, float64) {
	g := p.groupStat(from.Group)
	inner := func(fill float64) float64 { return nodeTerm(fill, g.mean, g.band, g.band/4) }
	before := inner(from.Fill()) + inner(to.Fill())
	var best action
	bestGain := 0.0
	segs := p.levelCands(from.ID)
	for i := range segs {
		x := &segs[i]
		if i >= scanLimit {
			break
		}
		gain := before - inner((from.Final-x.sz)/from.Capacity) - inner((to.Final+x.sz)/to.Capacity)
		if gain < nodeQuantum || gain < bestGain || gain == bestGain && best.x != nil && x.sz >= best.moved {
			continue
		}
		a, why := p.score(x, nil, from, to, true)
		if why != "" || a.delta.node != 0 || a.delta.tableClass() == 2 {
			continue
		}
		best, bestGain = a, gain
	}
	return best, bestGain
}

// --- 2. table balance ---------------------------------------------------------

// balanceTables visits collections round robin, TablesPerRound per round.
func (p *planner) balanceTables() {
	ids := make([]int64, 0, len(p.view.Collections))
	for id := range p.view.Collections {
		if p.isLoaded(id) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	cursor := p.cursor
	start := sort.Search(len(ids), func(i int) bool { return ids[i] > cursor })
	n := min(len(ids), max(p.cfg.TablesPerRound, 1))
	last := cursor
	for k := 0; k < n; k++ {
		coll := ids[(start+k)%len(ids)]
		p.balanceTable(coll)
		last = coll
	}
	p.cursor = last
}

func (p *planner) balanceTable(coll int64) {
	e := p.view.Collections[coll]
	seen := make(map[int64]bool, 2)
	for _, node := range e.Nodes {
		n, ok := p.view.Nodes[node]
		if !ok || !n.Usable() {
			continue
		}
		r := p.replicaOf(coll, node)
		if r == nil || seen[r.GetID()] {
			continue
		}
		seen[r.GetID()] = true
		if p.small(coll, r, n.Group) {
			continue
		}
		if len(p.replicaNodes(r, n.Group)) >= 2 {
			p.balanceTableReplica(coll, r, n.Group)
		}
	}
}

// balanceTableReplica lowers a table's terms on one replica: from the
// nodes holding too much of it to the others, the move, exchange within
// the table or swap with another table that lowers the potential most,
// until none does. If the node shortest of the table has its budget in
// flight the table waits for it.
func (p *planner) balanceTableReplica(coll int64, r *meta.Replica, group string) {
	nodes := p.replicaNodes(r, group)
	for iter := 0; iter < 2*len(nodes)+4; iter++ {
		ts := p.tableShare(coll, r, group)
		// Each node's standing: segments beyond the bounds (signed), bytes
		// beyond the tolerance (signed, in tolerances) and the plain
		// deviation from the share, which orders nodes within tolerance.
		type scored struct {
			n    *assign.ViewNode
			c, b float64
			dev  float64
		}
		var over, rest []scored
		violated := false
		for i, n := range nodes {
			cnt, b := p.view.CountOn(coll, n.ID), p.view.BytesOn(coll, n.ID)
			sh := &ts.share
			s := scored{n: n, dev: (b - sh.FairBytes[i]) / math.Max(sh.BytesTolerance[i], 1)}
			s.c = math.Max(0, float64(cnt-sh.MaxCount[i])) - math.Max(0, float64(sh.MinCount[i]-cnt))
			s.b = math.Max(0, s.dev-1) - math.Max(0, -s.dev-1)
			// Start only past the trigger bounds (first round), but once
			// started go on until the table is inside the plain bounds.
			if iter == 0 {
				oc, ob := ts.term(i, cnt, b, true)
				violated = violated || oc > 0 || ob > 0
			} else {
				violated = violated || s.c != 0 || s.b != 0
			}
			if s.c > 0 || s.dev > 0 {
				over = append(over, s)
			} else {
				rest = append(rest, s)
			}
		}
		if !violated {
			return
		}
		if iter == 0 {
			p.tablesOffShare++
		}
		// Sources most over first, targets most short first; the potential
		// decides whether a pair helps.
		sort.Slice(over, func(i, j int) bool {
			if over[i].c != over[j].c {
				return over[i].c > over[j].c
			}
			return over[i].dev > over[j].dev
		})
		sort.Slice(rest, func(i, j int) bool {
			if rest[i].c != rest[j].c {
				return rest[i].c < rest[j].c
			}
			return rest[i].dev < rest[j].dev
		})
		if len(rest) > 0 && (rest[0].c < 0 || rest[0].b < 0) && !p.canTakeMove(rest[0].n) {
			p.skips["table_wait"]++
			return
		}
		var best action
		for _, o := range over[:min(4, len(over))] {
			segs := p.tableCands(o.n.ID, coll)
			for _, u := range rest[:min(6, len(rest))] {
				if !p.canTakeMove(u.n) {
					continue
				}
				consider(&best, p.bestMove(segs, o.n, u.n, false), false)
			}
		}
		if best.x == nil {
			for _, o := range over[:min(4, len(over))] {
				if !p.canTakeMove(o.n) {
					continue
				}
				for _, u := range rest[:min(6, len(rest))] {
					if !p.canTakeMove(u.n) {
						continue
					}
					consider(&best, p.bestExchange(o.n, u.n, coll), false)
				}
			}
		}
		if best.x == nil {
			for _, o := range over[:min(4, len(over))] {
				if !p.canTakeMove(o.n) {
					continue
				}
				segs := p.tableCands(o.n.ID, coll)
				for _, u := range rest[:min(6, len(rest))] {
					if !p.canTakeMove(u.n) {
						continue
					}
					consider(&best, p.bestSwap(segs, p.candidates(u.n.ID), o.n, u.n, false), false)
				}
			}
		}
		if best.x == nil {
			p.skips["table_stuck"]++
			return
		}
		switch {
		case best.y == nil:
			p.apply(best, "table_move")
		case best.y.coll == coll:
			p.apply(best, "table_exchange")
		default:
			p.apply(best, "table_swap")
		}
	}
}
