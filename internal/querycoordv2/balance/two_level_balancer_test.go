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

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// tlEnv is a cluster with a distribution the test edits directly and plans
// "executed" instantly (the target copy appears loaded now, the source copy
// disappears).
type tlEnv struct {
	t       *testing.T
	ctx     context.Context
	nodeMgr *session.NodeManager
	m       *meta.Meta
	dist    *meta.DistributionManager
	b       *TwoLevelBalancer
	segs    map[int64][]*meta.Segment // node -> segments
	nodes   typeutil.UniqueSet
	tasks   []task.Task
	nextSeg int64
}

func newTLEnv(t *testing.T, nodes int, capMB float64) *tlEnv {
	paramtable.Init()
	assign.ResetTwoLevelStateForTest()
	e := &tlEnv{t: t, ctx: context.Background(), nodeMgr: session.NewNodeManager(), segs: map[int64][]*meta.Segment{}, nodes: typeutil.NewUniqueSet()}
	for i := int64(1); i <= int64(nodes); i++ {
		e.addNode(i, capMB, false)
	}
	var id int64 = 1 << 40
	e.m = meta.NewMeta(func() (int64, error) { id++; return id, nil }, nopCatalog{}, e.nodeMgr, metacache.NewMetaStore(nil))
	e.dist = meta.NewDistributionManager(e.nodeMgr)
	sched := task.NewMockScheduler(t)
	sched.EXPECT().GetSegmentTaskNum(mock.Anything).RunAndReturn(func(filters ...task.TaskFilter) int {
		for _, tk := range e.tasks {
			for _, f := range filters {
				f(tk)
			}
		}
		return len(e.tasks)
	}).Maybe()
	sched.EXPECT().GetSegmentTaskNum().Return(0).Maybe()
	sched.EXPECT().LoadBudget(mock.Anything).Return(64*1024*float64(1<<20), 4).Maybe() // the pacer's budget on these nodes: 4 loads, bytes unlimited
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(sched, e.nodeMgr, e.dist, e.m, movableTarget{})
	e.b = NewTwoLevelBalancer(sched, e.nodeMgr, e.dist, movableTarget{})
	return e
}

func (e *tlEnv) addNode(id int64, capMB float64, streaming bool) {
	labels := map[string]string{}
	if streaming {
		labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] = "1"
	}
	info := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id, Address: "n" + strconv.FormatInt(id, 10), Labels: labels})
	info.UpdateStats(session.WithMemCapacity(capMB), session.WithCPUNum(8))
	e.nodeMgr.Add(info)
	e.nodes.Insert(id)
}

func (e *tlEnv) addCollection(coll int64, nodes typeutil.UniqueSet) *meta.Replica {
	r := meta.NewReplica(&querypb.Replica{ID: coll, CollectionID: coll, ResourceGroup: meta.DefaultResourceGroupName}, nodes)
	require.NoError(e.t, e.m.ReplicaManager.Put(e.ctx, r))
	return r
}

// addSegment puts a segment of coll on node, loaded long ago; rows follow
// bytes (a thousand rows per MB), as within one table they do.
func (e *tlEnv) addSegment(coll, node int64, mb int64) *meta.Segment {
	e.nextSeg++
	s := &meta.Segment{
		SegmentInfo: &datapb.SegmentInfo{
			ID: e.nextSeg, CollectionID: coll, NumOfRows: mb * 1000, InsertChannel: "ch",
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: mb << 20}}}},
		},
		Node: node, Version: 1,
	}
	e.segs[node] = append(e.segs[node], s)
	return s
}

func (e *tlEnv) sync() {
	for n := range e.nodes {
		e.dist.SegmentDistManager.Update(n, e.segs[n]...)
	}
}

// execute lands every plan now: the target copy is loaded (so it cools
// down), the source copy is released.
func (e *tlEnv) execute(plans []assign.SegmentAssignPlan) {
	for _, p := range plans {
		kept := e.segs[p.From][:0]
		var moved *meta.Segment
		for _, s := range e.segs[p.From] {
			if s.GetID() == p.Segment.GetID() && moved == nil {
				moved = s
				continue
			}
			kept = append(kept, s)
		}
		require.NotNil(e.t, moved, "plan moves a segment not on its source")
		e.segs[p.From] = kept
		c := *moved
		c.Node = p.To
		c.Version = time.Now().UnixNano()
		e.segs[p.To] = append(e.segs[p.To], &c)
	}
	e.sync()
}

// run executes rounds until a round plans nothing; it returns the moves.
func (e *tlEnv) run(maxRounds int) int {
	moves := 0
	for i := 0; i < maxRounds; i++ {
		plans := e.b.BalanceGlobal(e.ctx, e.m)
		if len(plans) == 0 {
			return moves
		}
		moves += len(plans)
		e.execute(plans)
	}
	e.t.Fatalf("balance did not converge in %d rounds", maxRounds)
	return 0
}

// cool makes every segment old, as if all cooldowns had passed.
func (e *tlEnv) cool() {
	assign.AgeHistoryForTest(time.Hour)
	for _, ss := range e.segs {
		for _, s := range ss {
			s.Version = 1
		}
	}
	e.sync()
}

func (e *tlEnv) nodeMB() map[int64]float64 {
	out := map[int64]float64{}
	for n := range e.nodes {
		for _, s := range e.segs[n] {
			out[n] += assign.SegmentBytes(s) / (1 << 20)
		}
	}
	return out
}

func (e *tlEnv) collMB(coll int64) map[int64]float64 {
	out := map[int64]float64{}
	for n := range e.nodes {
		for _, s := range e.segs[n] {
			if s.GetCollectionID() == coll {
				out[n] += assign.SegmentBytes(s) / (1 << 20)
			}
		}
	}
	return out
}

// fills is each node's bytes over capacity.
func (e *tlEnv) fills(nodes ...int64) map[int64]float64 {
	mb := e.nodeMB()
	out := map[int64]float64{}
	for _, n := range nodes {
		out[n] = mb[n] / e.nodeMgr.Get(n).MemCapacity()
	}
	return out
}

func spreadOfFills(f map[int64]float64) (spread, mean float64) {
	lo, hi := math.MaxFloat64, 0.0
	for _, v := range f {
		lo, hi = math.Min(lo, v), math.Max(hi, v)
		mean += v
	}
	return hi - lo, mean / float64(len(f))
}

// requireTablesSettled checks every table is within tolerance of its fair
// share on every node (equal capacities).
// requireTablesSettled checks every table with at least two segments per
// node (fewer cannot be spread evenly) is within tolerance of its share.
func (e *tlEnv) requireTablesSettled(colls []int64, nodes []int64) {
	tol := assign.GetTwoLevelConfig().TableByteTolerance
	for _, c := range colls {
		mb := e.collMB(c)
		var total, maxSeg float64
		count := 0
		for _, n := range nodes {
			total += mb[n]
			for _, s := range e.segs[n] {
				if s.GetCollectionID() == c {
					count++
					maxSeg = math.Max(maxSeg, assign.SegmentBytes(s)/(1<<20))
				}
			}
		}
		if count < 2*len(nodes) {
			continue
		}
		fair := total / float64(len(nodes))
		for _, n := range nodes {
			// Balance starts on a table only past 1.5 tolerances (hysteresis
			// against the drift of ingestion), so settled means within that.
			require.LessOrEqual(e.t, math.Abs(mb[n]-fair), 1.5*math.Max(tol*fair, maxSeg)+1,
				"collection %d on node %d: %.0f MB vs fair %.0f (%v)", c, n, mb[n], fair, mb)
		}
	}
}

func ids(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i + 1)
	}
	return out
}

// A skewed cluster converges with both goals met, and then stays still.
func TestTwoLevel_ConvergesAndStaysStill(t *testing.T) {
	e := newTLEnv(t, 4, 64*1024)
	rng := rand.New(rand.NewSource(7))
	colls := make([]int64, 0, 10)
	for c := int64(10); c < 20; c++ {
		e.addCollection(c, e.nodes)
		colls = append(colls, c)
		for i := 0; i < 20; i++ {
			e.addSegment(c, int64(1+rng.Intn(2)), int64(50+rng.Intn(450))) // all on nodes 1-2
		}
	}
	e.sync()
	// Moves blocked by a cooldown (a segment moved once may not move again
	// at once) are made once it passes: settle through a few cooldowns.
	moves := 0
	for round := 0; ; round++ {
		n := e.run(200)
		moves += n
		if n == 0 {
			break
		}
		require.Less(t, round, 5, "still moving after %d cooldowns", round)
		e.cool()
	}
	spread, mean := spreadOfFills(e.fills(ids(4)...))
	require.LessOrEqual(t, spread, 0.1*mean+0.01, "node spread")
	e.requireTablesSettled(colls, ids(4))
	// About half the data had to move (~100 of 200 segments).
	require.Less(t, moves, 150, "moves")
	// Static: nothing more to do, even once every cooldown has passed.
	e.cool()
	require.Zero(t, e.run(10), "a converged cluster plans nothing")
}

// Scale-out fills the new nodes with roughly the minimum data movement.
func TestTwoLevel_ScaleOut(t *testing.T) {
	e := newTLEnv(t, 4, 64*1024)
	colls := []int64{10, 11, 12}
	for _, c := range colls {
		e.addCollection(c, e.nodes)
	}
	for i := 0; i < 120; i++ {
		e.addSegment(colls[i%3], int64(1+i%4), 200)
	}
	e.sync()
	require.Zero(t, e.run(10), "balanced to start with")
	e.addNode(5, 64*1024, false)
	e.addNode(6, 64*1024, false)
	for _, c := range colls {
		e.addCollection(c, e.nodes)
	}
	e.sync()
	moves := e.run(200)
	spread, mean := spreadOfFills(e.fills(ids(6)...))
	require.LessOrEqual(t, spread, 0.1*mean+0.01)
	e.requireTablesSettled(colls, ids(6))
	// The new nodes' share is 1/3 of 120 segments: 40. Allow a little.
	require.LessOrEqual(t, moves, 46, "moves")
}

// Scale-out with many tables of different sizes: spreading the tables first
// levels the nodes with moves needed anyway. Leveling first fills the new
// nodes with the largest segments and leaves the tables to be fixed by
// further moves and swaps (375 moves here, against 306).
func TestTwoLevel_ScaleOutManyTables(t *testing.T) {
	e := newTLEnv(t, 4, 64*1024)
	var colls []int64
	minimal := 0
	rng := rand.New(rand.NewSource(7))
	for c := int64(1); c <= 40; c++ {
		e.addCollection(c, e.nodes)
		colls = append(colls, c)
		segs := 8 + int(c%5)*4
		base := 50 + (c%7)*40
		for i := 0; i < segs; i++ {
			// Sizes vary within a table too (compaction leaves mixed sizes).
			e.addSegment(c, 1+int64(i%4), base/2+rng.Int63n(base))
		}
		minimal += int(math.Round(float64(segs) * 2 / 6))
	}
	e.sync()
	e.run(100) // settle the uneven sizes first
	e.cool()
	require.Zero(t, e.run(10), "balanced to start with")
	e.addNode(5, 64*1024, false)
	e.addNode(6, 64*1024, false)
	for _, c := range colls {
		e.addCollection(c, e.nodes)
	}
	e.sync()
	moves := e.run(400)
	t.Logf("moves %d, minimum %d", moves, minimal)
	spread, mean := spreadOfFills(e.fills(ids(6)...))
	require.LessOrEqual(t, spread, 0.1*mean+0.01)
	e.requireTablesSettled(colls, ids(6))
	// minimal counts segments, not bytes: with sizes varying it is a floor.
	require.LessOrEqual(t, moves, minimal*3/2, "moves %d vs minimum %d", moves, minimal)
	e.cool()
	require.Zero(t, e.run(10), "static afterwards")
}

// Right after a restart the distribution may not match segments to a target
// and carries bare stubs (no binlogs, no rows). They must not count as
// empty: node 1 is not taken for an empty node, and nothing moves.
func TestTwoLevel_StubSegmentInfoAfterRestart(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	for i := 0; i < 20; i++ {
		e.addSegment(1, 1+int64(i%2), 500)
	}
	for i, s := range e.segs[1] {
		if i%2 == 0 {
			continue // some copies are matched, so the collection's size is known
		}
		e.segs[1][i] = &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{ID: s.GetID(), CollectionID: s.GetCollectionID(), InsertChannel: "ch"},
			Node:        s.Node, Version: s.Version,
		}
	}
	e.sync()
	assign.ResetTwoLevelStateForTest() // restart: no sizes cached
	e.cool()
	require.Empty(t, e.b.BalanceGlobal(e.ctx, e.m), "stubs count as the collection's average segment")
}

// drain empties node id the way stopping balance does: its segments are
// placed by the assign policy on the replica's other nodes, then the node
// leaves the replicas and the cluster.
func (e *tlEnv) drain(id int64) {
	e.nodeMgr.Stopping(id)
	e.nodes.Remove(id)
	byColl := map[int64][]*meta.Segment{}
	for _, s := range e.segs[id] {
		byColl[s.GetCollectionID()] = append(byColl[s.GetCollectionID()], s)
	}
	for coll, segs := range byColl {
		e.addCollection(coll, e.nodes)
		plans := e.b.GetAssignPolicy().AssignSegment(e.ctx, coll, segs, e.nodes.Collect(), true)
		require.Len(e.t, plans, len(segs), "drain placed every segment")
		for i := range plans {
			plans[i].From = id
		}
		e.execute(plans)
	}
	require.Empty(e.t, e.segs[id])
	delete(e.segs, id)
	e.nodeMgr.Remove(id)
	e.sync()
}

// Rolling replacement of three nodes, drained one at a time with a fresh
// node added after each: balance must settle after the last change and not
// shuttle segments back and forth meanwhile.
func TestTwoLevel_RollingReplaceSettles(t *testing.T) {
	e := newTLEnv(t, 8, 64*1024)
	rng := rand.New(rand.NewSource(5))
	var colls []int64
	for c := int64(1); c <= 120; c++ {
		e.addCollection(c, e.nodes)
		colls = append(colls, c)
		segs := 2 + rng.Intn(10)
		if c%10 == 0 {
			segs = 40 + rng.Intn(40)
		}
		for i := 0; i < segs; i++ {
			e.addSegment(c, 1+rng.Int63n(8), 20+rng.Int63n(200))
		}
	}
	e.sync()

	// Within one settle (constant membership) a segment must not return to a
	// node it left: left[seg] is every node a copy of seg has left since the
	// last topology change. Across changes a return is natural: a table's
	// share on a node falls when a node is added and rises when one leaves.
	left := map[int64]map[int64]bool{}
	reversals, moves := 0, 0
	// settle runs balance, with cooldowns expiring between runs, until a run
	// plans nothing; moves blocked by a cooldown are made once it passes.
	settle := func(what string) {
		left = map[int64]map[int64]bool{}
		for round := 0; ; round++ {
			require.Less(t, round, 20, "%s: still moving after %d cooldowns", what, round)
			n := 0
			for i := 0; i < 400; i++ {
				plans := e.b.BalanceGlobal(e.ctx, e.m)
				if len(plans) == 0 {
					break
				}
				for _, p := range plans {
					n++
					id := p.Segment.GetID()
					if left[id][p.To] {
						reversals++
						t.Logf("%s: reversal %s seg %d coll %d %d->%d (left %v)", what, p.Reason, id, p.Segment.GetCollectionID(), p.From, p.To, left[id])
					}
					if left[id] == nil {
						left[id] = map[int64]bool{}
					}
					left[id][p.From] = true
				}
				e.execute(plans)
			}
			t.Logf("%s: cooldown round %d, %d moves", what, round, n)
			moves += n
			if n == 0 {
				return
			}
			e.cool()
		}
	}
	settle("initial")
	moves, reversals = 0, 0
	for i := int64(0); i < 3; i++ {
		e.drain(1 + i)
		e.addNode(101+i, 64*1024, false)
		for _, c := range colls {
			e.addCollection(c, e.nodes)
		}
		e.sync()
		settle(fmt.Sprintf("replace %d", i+1))
	}
	t.Logf("moves %d, reversals %d", moves, reversals)
	require.LessOrEqual(t, reversals, moves/100, "reversals within constant membership")
	spread, mean := spreadOfFills(e.fills(e.nodes.Collect()...))
	require.LessOrEqual(t, spread, 0.1*mean+0.01)
	e.requireTablesSettled(colls, e.nodes.Collect())
}

// Placement spreads a burst of loads (a recovery) by queue depth: once the
// emptiest node has its load slots in flight (the pacer's, 4 here),
// the next go elsewhere rather than queueing behind them.
func TestTwoLevel_PlacementSpreadsRecovery(t *testing.T) {
	e := newTLEnv(t, 3, 64*1024)
	e.addCollection(1, e.nodes)
	for i := 0; i < 8; i++ {
		e.addSegment(1, 1+int64(i%2), 500) // node 3 is empty
	}
	e.sync()
	var segs []*meta.Segment
	for i := int64(0); i < 12; i++ {
		segs = append(segs, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
			ID: 100 + i, CollectionID: 1, NumOfRows: 10,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 500 << 20}}}},
		}})
	}
	plans := e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, segs, e.nodes.Collect(), true)
	require.Len(t, plans, 12)
	per := map[int64]int{}
	for _, p := range plans {
		per[p.To]++
	}
	require.Equal(t, 4, per[3], "the empty node takes a queue's worth: %v", per)
	require.GreaterOrEqual(t, per[1], 2, "the rest spreads: %v", per)
	require.GreaterOrEqual(t, per[2], 2, "the rest spreads: %v", per)
}

// One table of huge segments leaves a node structurally over the mean (it
// holds one huge segment more than the others). Leveling takes small
// tables' segments off that node; table balance must not put them back,
// or the two cycle once per cooldown. Granularity wins: the small tables
// stay a little under on that node, and the cluster is still.
func TestTwoLevel_GranularityDoesNotChurn(t *testing.T) {
	e := newTLEnv(t, 5, 64*1024)
	e.addCollection(1, e.nodes)
	for i := 0; i < 6; i++ {
		e.addSegment(1, 1+int64(i%5), 8000) // node 1 holds two huge segments
	}
	for c := int64(2); c <= 7; c++ {
		e.addCollection(c, e.nodes)
		for i := 0; i < 20; i++ {
			e.addSegment(c, 1+int64(i%5), 400)
		}
	}
	e.sync()
	total := 0
	for round := 0; round < 6; round++ {
		n := e.run(400)
		t.Logf("cooldown round %d: %d moves", round, n)
		total += n
		if n == 0 {
			break
		}
		e.cool()
	}
	require.Zero(t, e.run(10), "still moving after six cooldowns")
	require.LessOrEqual(t, total, 20, "moves %d", total)
	// New segments of a table that is short on node 1 go elsewhere while
	// node 1 is above the mean: placement does not feed the cycle either.
	fills := e.fills(ids(5)...)
	var mean float64
	for _, f := range fills {
		mean += f / 5
	}
	// Memory comes first: leveling may take small segments off node 1 until
	// it is within the band of the others; what matters is that it stops.
	t.Logf("fills after leveling around the huge segment: %v (mean %.3f)", fills, mean)
	var segs []*meta.Segment
	for i := int64(0); i < 3; i++ {
		segs = append(segs, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
			ID: 900 + i, CollectionID: 2, NumOfRows: 10,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 400 << 20}}}},
		}})
	}
	for _, p := range e.b.GetAssignPolicy().AssignSegment(e.ctx, 2, segs, e.nodes.Collect(), true) {
		t.Logf("placement %d -> %d", p.Segment.GetID(), p.To)
	}
}

// A huge segment that fits nowhere leaves its node over; leveling must not
// chase the gap by moving the tiny segments beside it one by one.
func TestTwoLevel_HugeSegmentNoDustMoves(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	e.addCollection(2, e.nodes)
	e.addSegment(1, 1, 8000)
	for i := 0; i < 100; i++ {
		e.addSegment(2, 1+int64(i%2), 2)
	}
	e.sync()
	require.Zero(t, e.run(10), "moving 2MB segments beside an 8GB one is churn for nothing")
}

// A table short on a node that is above the mean (another table's segments
// fill it) gets there by swap: its segment in, a segment at least as large
// of the other table out, so the node's memory does not rise.
func TestTwoLevel_TableSwapIntoFullNode(t *testing.T) {
	e := newTLEnv(t, 3, 64*1024)
	e.addCollection(1, e.nodes)
	e.addCollection(8, e.nodes)
	e.addCollection(9, e.nodes)
	for i := 0; i < 46; i++ {
		e.addSegment(9, 1, 500) // node 1: 23000 MB of table 9, none of table 1
	}
	for n := int64(2); n <= 3; n++ {
		for i := 0; i < 36; i++ {
			e.addSegment(8, n, 500) // 18000 MB
		}
		for i := 0; i < 6; i++ {
			e.addSegment(1, n, 500) // 3000 MB: table 1 lives on nodes 2 and 3 only
		}
	}
	e.sync()
	// Node 1 is above the mean by more than half the band, within the band.
	fills := e.fills(1, 2, 3)
	mean := (fills[1] + fills[2] + fills[3]) / 3
	require.Greater(t, fills[1], mean*1.05)
	require.LessOrEqual(t, fills[1]-fills[2], 0.1*mean)

	plans := e.b.BalanceGlobal(e.ctx, e.m)
	in, out := 0, 0
	for _, p := range plans {
		if p.To == 1 {
			require.Equal(t, "table_swap", p.Reason, "only a swap brings a segment onto the full node: %v", p)
		}
		if p.Reason == "table_swap" && p.To == 1 && p.Segment.GetCollectionID() == 1 {
			in++
		}
		if p.Reason == "table_swap" && p.From == 1 {
			require.EqualValues(t, 9, p.Segment.GetCollectionID())
			out++
		}
	}
	require.GreaterOrEqual(t, in, 1, "segments of table 1 swapped in: %v", plans)
	require.GreaterOrEqual(t, out, in, "as many segments of table 9 swapped out: %v", plans)
	e.execute(plans)
	e.run(20)
	require.GreaterOrEqual(t, e.collMB(1)[1], 1500.0, "table 1 reaches its share on node 1 by swaps: %v", e.collMB(1))
}

// Table 228 from the TB1 simulation: 25 segments of 4k to 67k rows on 12
// nodes, piled 5 / 5 / 5 / 4 on four nodes and none on three. By rows every
// node is within a tolerance of one largest segment (67k against a share
// of 50k), so rows alone leave it; the segment counts (share 2.08, so 1 to
// 4 allowed) do not. The fix is a few moves, and then nothing.
func TestTwoLevel_SegmentCountSpreadsSmallTable(t *testing.T) {
	e := newTLEnv(t, 12, 64*1024)
	e.addCollection(99, e.nodes)
	for n := int64(1); n <= 12; n++ {
		for i := 0; i < 20; i++ {
			e.addSegment(99, n, 500) // a level background: 10GB per node
		}
	}
	e.addCollection(228, e.nodes)
	layout := map[int64][]int64{
		1: {28, 42}, 2: {28}, 3: {36}, 4: {33}, 8: {67},
		9: {4, 17, 22, 23, 23}, 10: {10, 16, 16, 19}, 11: {13, 16, 22, 25, 30}, 12: {14, 18, 20, 27, 32},
	}
	for n, sizes := range layout {
		for _, mb := range sizes {
			e.addSegment(228, n, mb)
		}
	}
	e.sync()
	moves := e.run(50)
	counts := map[int64]int{}
	for n, ss := range e.segs {
		for _, s := range ss {
			if s.GetCollectionID() == 228 {
				counts[n]++
			}
		}
	}
	for n := int64(1); n <= 12; n++ {
		require.GreaterOrEqual(t, counts[n], 1, "every node serves the table: %v", counts)
		require.LessOrEqual(t, counts[n], 4, "no node carries a pile: %v", counts)
	}
	require.LessOrEqual(t, moves, 8, "moves %d: %v", moves, counts)
	e.cool()
	require.Zero(t, e.run(10), "static afterwards")
}

// A node at 90% with others at 82% is brought within the tight band.
func TestTwoLevel_HotNodeTightBand(t *testing.T) {
	e := newTLEnv(t, 5, 10*1024)
	e.addCollection(1, e.nodes)
	for n := int64(1); n <= 5; n++ {
		mb := 8200
		if n == 1 {
			mb = 9000
		}
		for i := 0; i < mb/100; i++ {
			e.addSegment(1, n, 100)
		}
	}
	e.sync()
	e.run(50)
	spread, mean := spreadOfFills(e.fills(ids(5)...))
	require.LessOrEqual(t, spread, 0.05*mean+0.01, "tight band above the hot threshold: %v", e.fills(ids(5)...))
}

// Bigger nodes get proportionally more.
func TestTwoLevel_ByCapacity(t *testing.T) {
	e := newTLEnv(t, 4, 32*1024)
	e.nodeMgr.Get(3).UpdateStats(session.WithMemCapacity(64 * 1024))
	e.nodeMgr.Get(4).UpdateStats(session.WithMemCapacity(64 * 1024))
	e.addCollection(1, e.nodes)
	for i := 0; i < 60; i++ {
		e.addSegment(1, 1, 200)
	}
	e.sync()
	e.run(100)
	mb := e.collMB(1)
	require.InDelta(t, 2000, mb[1], 400, "%v", mb)
	require.InDelta(t, 4000, mb[3], 400, "%v", mb)
}

// A table piled on one node is spread even when the nodes are level: its
// segments are swapped against other tables' to keep the nodes level.
func TestTwoLevel_TableSpreadKeepsNodesLevel(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	e.addCollection(2, e.nodes)
	for i := 0; i < 20; i++ {
		e.addSegment(1, 1, 300) // table 1 all on node 1
		e.addSegment(2, 2, 300) // table 2 all on node 2
	}
	e.sync()
	moves := e.run(100)
	spread, mean := spreadOfFills(e.fills(1, 2))
	require.LessOrEqual(t, spread, 0.1*mean+0.01)
	e.requireTablesSettled([]int64{1, 2}, []int64{1, 2})
	require.LessOrEqual(t, moves, 24, "about 10 segments of each table swap")
}

// When every single segment would overshoot, a larger one is swapped for a
// smaller one.
func TestTwoLevel_SwapWhenSingleMoveOvershoots(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	e.addCollection(2, e.nodes)
	e.addSegment(1, 1, 10*1024)
	e.addSegment(1, 1, 10*1024)
	e.addSegment(2, 2, 7*1024)
	e.addSegment(2, 2, 5*1024)
	e.sync()
	before := e.nodeMB()
	plans := e.b.BalanceGlobal(e.ctx, e.m)
	require.Len(t, plans, 2, "one swap")
	e.execute(plans)
	after := e.nodeMB()
	require.Less(t, math.Abs(after[1]-after[2]), math.Abs(before[1]-before[2]))
}

// A QueryCoord restart loses the move history and freezes nothing: balance
// goes on at once from the state the old QueryCoord left.
func TestTwoLevel_RestartKeepsBalancing(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	for i := 0; i < 10; i++ {
		e.addSegment(1, 1, 500).Version = time.Now().UnixNano()
	}
	e.sync()
	assign.ResetTwoLevelStateForTest() // restart
	require.NotEmpty(t, e.b.BalanceGlobal(e.ctx, e.m), "fresh loads do not cool, even after a restart")
}

// Only moves cool down: a segment this QueryCoord placed may move at once,
// but one it moved does not move again within the cooldown.
func TestTwoLevel_CooldownOnlyAfterMove(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addCollection(1, e.nodes)
	for i := 0; i < 10; i++ {
		e.addSegment(1, 1, 500).Version = time.Now().UnixNano()
	}
	e.sync()
	plans := e.b.BalanceGlobal(e.ctx, e.m)
	require.NotEmpty(t, plans, "fresh loads do not cool")
	e.execute(plans)

	// Make node 2 the heavier one: the moved segments are the obvious
	// candidates to send back, and must stay.
	for i := 0; i < 10; i++ {
		e.addSegment(1, 2, 500).Version = 1
	}
	e.sync()
	movedIDs := map[int64]bool{}
	for _, p := range plans {
		movedIDs[p.Segment.GetID()] = true
	}
	for _, p := range e.b.BalanceGlobal(e.ctx, e.m) {
		require.False(t, movedIDs[p.Segment.GetID()], "segment %d moved back within cooldown", p.Segment.GetID())
	}
}

// Copies being released (a reduce task acts on them) count in the peak but not
// in the final value: node 1 does not look heavier during a compaction hand-off.
func TestTwoLevel_LeavingNotInFinal(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	r := e.addCollection(1, e.nodes)
	for i := 0; i < 10; i++ {
		e.addSegment(1, 1, 500)
		e.addSegment(1, 2, 500)
		old := e.addSegment(1, 1, 500) // compacted away, being released
		tk, err := task.NewSegmentTask(e.ctx, time.Minute, task.WrapIDSource(0), 1, r, 0,
			task.NewSegmentAction(1, task.ActionTypeReduce, "ch", old.GetID()))
		require.NoError(t, err)
		e.tasks = append(e.tasks, tk)
	}
	e.sync()
	require.Empty(t, e.b.BalanceGlobal(e.ctx, e.m))
}

// Balance does not add to a node with its load slots (the pacer's, 4 here)
// in flight; placement only prefers nodes with a free load queue and is
// never blocked.
func TestTwoLevel_InflightLimitOnlyBalance(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	r := e.addCollection(1, e.nodes)
	for i := 0; i < 20; i++ {
		e.addSegment(1, 1, 500)
	}
	for i := 0; i < 4; i++ {
		tk, err := task.NewSegmentTask(e.ctx, time.Minute, task.WrapIDSource(0), 1, r, 0,
			task.NewSegmentAction(2, task.ActionTypeGrow, "ch", int64(9000+i)))
		require.NoError(t, err)
		e.tasks = append(e.tasks, tk)
	}
	e.sync()
	require.Empty(t, e.b.BalanceGlobal(e.ctx, e.m), "node 2 is busy loading")
	seg := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID: 99, CollectionID: 1, NumOfRows: 10,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 500 << 20}}}},
	}}
	plans := e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, []*meta.Segment{seg}, e.nodes.Collect(), true)
	require.Len(t, plans, 1)
	require.EqualValues(t, 1, plans[0].To, "placement prefers the node with a free load queue, even above the mean")
	// With every queue full, placement still happens, on the emptiest node.
	for i := 0; i < 4; i++ {
		tk, err := task.NewSegmentTask(e.ctx, time.Minute, task.WrapIDSource(0), 1, r, 0,
			task.NewSegmentAction(1, task.ActionTypeGrow, "ch", int64(9100+i)))
		require.NoError(t, err)
		e.tasks = append(e.tasks, tk)
	}
	e.sync()
	assign.AgeHistoryForTest(time.Second) // past the view TTL: see the new tasks
	seg2 := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID: 98, CollectionID: 1, NumOfRows: 10,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 500 << 20}}}},
	}}
	plans = e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, []*meta.Segment{seg2}, e.nodes.Collect(), true)
	require.Len(t, plans, 1)
	require.EqualValues(t, 2, plans[0].To, "placement is never blocked")
}

// A streaming node's embedded QueryNode never receives sealed segments, and
// does not drag the mean down.
func TestTwoLevel_StreamingNodeIgnored(t *testing.T) {
	e := newTLEnv(t, 2, 64*1024)
	e.addNode(3, 64*1024, true)
	e.addCollection(1, e.nodes)
	for i := 0; i < 20; i++ {
		e.addSegment(1, 1+int64(i%2), 500)
	}
	e.sync()
	require.Zero(t, e.run(10), "the two query nodes are level")
}

// Placement: largest first, each to the emptiest node where the table stays
// within its fair share (then its tolerance), among nodes not far above the
// mean fill: node 1, four times fuller than the rest, gets none.
func TestTwoLevel_PlacementSpreadsTable(t *testing.T) {
	e := newTLEnv(t, 4, 64*1024)
	e.addCollection(1, e.nodes)
	e.addCollection(2, e.nodes)
	for i := 0; i < 8; i++ {
		e.addSegment(2, 1, 1000) // node 1 already fuller
	}
	e.sync()
	var segs []*meta.Segment
	for i := int64(0); i < 9; i++ {
		segs = append(segs, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
			ID: 100 + i, CollectionID: 1, NumOfRows: 10,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 300 << 20}}}},
		}})
	}
	plans := e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, segs, e.nodes.Collect(), true)
	require.Len(t, plans, 9)
	per := map[int64]int{}
	for _, p := range plans {
		per[p.To]++
	}
	for n := int64(2); n <= 4; n++ {
		require.Equal(t, 3, per[n], "table spread: %v", per)
	}
	require.Zero(t, per[1], "the node far above the mean gets none: %v", per)
	// Asked again (the checker retries until loaded): same answers.
	again := e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, segs, e.nodes.Collect(), true)
	for i := range plans {
		require.Equal(t, plans[i].To, again[i].To)
	}
}

// Placement waits for nodes that have not reported capacity.
func TestTwoLevel_PlacementWaitsForCapacity(t *testing.T) {
	e := newTLEnv(t, 2, 6*1024)
	e.nodeMgr.Get(2).UpdateStats(session.WithMemCapacity(0))
	e.addCollection(1, e.nodes)
	e.sync()
	seg := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID: 99, CollectionID: 1, NumOfRows: 10,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 500 << 20}}}},
	}}
	require.Empty(t, e.b.GetAssignPolicy().AssignSegment(e.ctx, 1, []*meta.Segment{seg}, e.nodes.Collect(), true))
}

// A compaction (three segments replaced by one) in a converged cluster
// costs at most a move or two, not a reshuffle.
func TestTwoLevel_CompactionCausesLittleChurn(t *testing.T) {
	e := newTLEnv(t, 4, 64*1024)
	colls := []int64{10, 11, 12, 13}
	for _, c := range colls {
		e.addCollection(c, e.nodes)
	}
	for i := 0; i < 160; i++ {
		e.addSegment(colls[i%4], int64(1+(i/4)%4), 200)
	}
	e.sync()
	require.Zero(t, e.run(10))
	// Compact three segments of table 10 on node 1 into one.
	kept := e.segs[1][:0]
	removed := 0
	for _, s := range e.segs[1] {
		if s.GetCollectionID() == 10 && removed < 3 {
			removed++
			continue
		}
		kept = append(kept, s)
	}
	e.segs[1] = kept
	merged := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID: 5000, CollectionID: 10, NumOfRows: 600_000, // rows follow bytes, as in addSegment
		Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 600 << 20}}}},
	}}
	plans := e.b.GetAssignPolicy().AssignSegment(e.ctx, 10, []*meta.Segment{merged}, e.nodes.Collect(), true)
	require.Len(t, plans, 1)
	m := *merged
	m.Node, m.Version = plans[0].To, time.Now().UnixNano()
	e.segs[m.Node] = append(e.segs[m.Node], &m)
	e.sync()
	moves := 0
	for i := 0; i < 10; i++ {
		plans := e.b.BalanceGlobal(e.ctx, e.m)
		if len(plans) == 0 {
			break
		}
		for _, p := range plans {
			t.Logf("round %d: %s coll %d seg %d %d->%d", i, p.Reason, p.Segment.GetCollectionID(), p.Segment.GetID(), p.From, p.To)
		}
		moves += len(plans)
		e.execute(plans)
	}
	require.LessOrEqual(t, moves, 2)
}

// Repro of day_10: a hot table over in bytes on two nodes (3 and 7) with
// many tiny segments; the right move is a large segment to the node short
// of bytes (8), not tiny segments between 3 and 7.
func TestTwoLevel_ReproHotTableBounce(t *testing.T) {
	e := newTLEnv(t, 8, 64*1024)
	e.addCollection(99, e.nodes)
	for n := int64(1); n <= 8; n++ {
		for i := 0; i < 20; i++ {
			e.addSegment(99, n, 500)
		}
	}
	e.addCollection(3, e.nodes)
	layout := map[int64][]int64{
		1: {90, 90, 90, 90, 90, 90, 90, 90, 90, 90, 90},
		2: {82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82, 82},
		3: {300, 300, 300, 300, 300, 23, 23, 23},
		4: {81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81, 81},
		5: {93, 93, 93, 93, 93, 93, 93, 93, 93, 93, 93},
		6: {70, 70, 70, 70, 70, 70, 70, 70, 70, 70, 70, 70, 70, 70},
		7: {300, 300, 300, 300, 300, 51, 51, 51},
		8: {53, 53, 53, 53, 53, 53, 53, 53, 53, 53, 53, 53, 53, 53},
	}
	for n, sizes := range layout {
		for _, mb := range sizes {
			e.addSegment(3, n, mb)
		}
	}
	e.sync()
	for i := 0; i < 20; i++ {
		plans := e.b.BalanceGlobal(e.ctx, e.m)
		if len(plans) == 0 {
			break
		}
		for _, p := range plans {
			t.Logf("round %d: %s coll %d seg %d %.0fMB %d->%d", i, p.Reason, p.Segment.GetCollectionID(), p.Segment.GetID(), assign.SegmentBytes(p.Segment)/(1<<20), p.From, p.To)
		}
		e.execute(plans)
	}
	t.Logf("table 3 MB per node: %v", e.collMB(3))
}

// A table action never pushes a node out of its band, not even by a small
// segment: the node is blocked instead, the round moves it back toward the
// mean with a move that hurts no table, and the table goes on next round.
func TestTwoLevel_TableActionNeverLeavesBandRoomMoveFollows(t *testing.T) {
	e := newTLEnv(t, 3, 1000)
	// Mean 600, band 60: the band is [570, 630]. Node 1 sits at its upper
	// edge (628) with too few of table 7, which lives on nodes 1 and 2
	// only, so its moves can only come from node 2. The single-segment
	// fillers are left alone by table balance; the 30MB one (table 8) lives
	// on nodes 1 and 3, so it cannot be swapped against table 7 either.
	e.addCollection(7, typeutil.NewUniqueSet(1, 2))
	for i := 0; i < 2; i++ {
		e.addSegment(7, 1, 20)
	}
	for i := 0; i < 8; i++ {
		e.addSegment(7, 2, 20)
	}
	e.addCollection(8, typeutil.NewUniqueSet(1, 3))
	e.addSegment(8, 1, 30)
	for i, f := range []struct{ node, mb int64 }{{1, 558}, {2, 440}, {3, 572}} {
		e.addCollection(int64(100+i), e.nodes)
		e.addSegment(int64(100+i), f.node, f.mb)
	}
	e.sync()
	plans := e.b.BalanceGlobal(e.ctx, e.m)
	require.Len(t, plans, 1, "table 7 waits; the blocked node gets room: %+v", plans)
	require.Equal(t, "level_room", plans[0].Reason)
	require.EqualValues(t, 1, plans[0].From)
	require.EqualValues(t, 3, plans[0].To)
	require.EqualValues(t, 8, plans[0].Segment.GetCollectionID(), "the room move hurts no table")
	e.execute(plans)
	e.cool()
	plans = e.b.BalanceGlobal(e.ctx, e.m)
	require.NotEmpty(t, plans)
	for _, p := range plans {
		require.Equal(t, "table_move", p.Reason)
		require.EqualValues(t, 7, p.Segment.GetCollectionID())
		require.EqualValues(t, 1, p.To, "with room, table 7 gets a segment on node 1")
	}
	e.execute(plans)
	for id, mb := range e.nodeMB() {
		require.InDelta(t, 600, mb, 30, "node %d stays inside its band", id)
	}
}
