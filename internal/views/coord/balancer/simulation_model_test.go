package balancer

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Numerical model only: no controller, RPC, storage, wall-clock scheduling or
// resource manager. Publications represent accepted views and protected readiness.
type balanceSimulation struct {
	t           *testing.T
	cache       *balancercache.Cache
	policy      *DefaultBalancePolicy
	nodes       map[int64]*NodeInfo
	collections []int64
	views       map[qviews.ShardID]*simulationShard
	sequence    int64
	counters    simulationCounters
}
type simulationView struct {
	assignments, rows map[int64]int64
	ready             map[int64]bool
	version           qviews.QueryViewVersion
	loadVersion       uint64
}
type (
	simulationShard    struct{ up, preparing, failed *simulationView }
	simulationCounters struct {
		Calls, Prepares, Releases, Rejected, Retries, Continuations int
		MovedRows, LoadRows                                         int64
		PlanMS                                                      float64
	}
)

type simulationRG struct {
	Rows                     map[int64]int64
	Demand                   int64
	CV, MaxMean, BandPenalty float64
}
type simulationSample struct {
	DesiredReplicas, ActiveReplicas, SuspendedReplicas int
	Groups                                             map[string]simulationRG
	FanoutMean                                         float64
	FanoutMax, ExcessFanout, ActiveShards, Pending     int
	CollectionMaxMean, ShardMaxMean                    float64
	UnplacedRows                                       int64
}
type simulationResult struct {
	Scenario, Phase, Outcome string
	Sweeps, Scope, Batch     int
	Counters                 simulationCounters
	Before, After            simulationSample
	Trace                    []simulationSample
	Config                   BalanceConfig
}

func newBalanceSimulation(t *testing.T, nodeCount int) *balanceSimulation {
	s := &balanceSimulation{t: t, cache: balancercache.New(nil), policy: NewDefaultBalancePolicy(), nodes: map[int64]*NodeInfo{}, views: map[qviews.ShardID]*simulationShard{}}
	for n := 1; n <= nodeCount; n++ {
		s.node(int64(n), "rg1", true, false)
	}
	return s
}

func (s *balanceSimulation) node(id int64, group string, alive, stopping bool) {
	info := &NodeInfo{NodeID: id, ResourceGroup: group, Alive: alive, Stopping: stopping}
	s.nodes[id] = info
	s.cache.PublishNode(id, info)
	// Model node-loss callbacks invalidate Preparing, preserving only ready
	// resources on surviving nodes. Up still describes the serving version.
	if !alive {
		for id, v := range s.views {
			if v.preparing != nil {
				for _, n := range v.preparing.assignments {
					if n == info.NodeID {
						v.failed, v.preparing = v.preparing, nil
						break
					}
				}
			}
			s.publish(id)
		}
	}
}

func (s *balanceSimulation) addCollection(id int64, groups []string, rows [][]int64) {
	s.collections = append(s.collections, id)
	s.data(id, rows, false)
	s.replicas(id, groups)
}

func (s *balanceSimulation) replicas(id int64, groups []string) {
	c := s.cache.GetCollection(id)
	version := uint64(1)
	if c != nil {
		version = c.ConfigVersion() + 1
	}
	if len(groups) == 0 {
		s.cache.PublishLoadConfig(id, nil, version)
		return
	}
	cfg := &loadmgr.LoadConfig{CollectionID: id, PartitionIDs: []int64{1}}
	for i, g := range groups {
		cfg.Replicas = append(cfg.Replicas, &loadmgr.ReplicaAssignment{ReplicaID: id*10 + int64(i), ResourceGroup: g})
	}
	s.cache.PublishLoadConfig(id, cfg, version)
}

func (s *balanceSimulation) data(id int64, rows [][]int64, replaceIDs bool) {
	version := qviews.DataVersion{StreamingVersion: 1}
	if c := s.cache.GetCollection(id); c != nil && c.DataView() != nil {
		version = c.DataView().DataVersion
		version.CompactVersion++
	}
	data := &CollectionDataView{CollectionID: id, DataVersion: version}
	offset := int64(0)
	if replaceIDs {
		offset = version.CompactVersion * 100_000
	}
	for sh, values := range rows {
		part := &PartitionDataView{PartitionID: 1}
		for i, r := range values {
			part.Segments = append(part.Segments, &SegmentDataView{SegmentID: id*1_000_000_000 + offset + int64(sh)*10_000 + int64(i), PartitionID: 1, RowNum: r})
		}
		data.Shards = append(data.Shards, &ShardDataView{VChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv%d", id, sh), Partitions: []*PartitionDataView{part}})
	}
	s.cache.PublishDataView(id, api.PrepareCollectionDataView(data))
}

func (s *balanceSimulation) scope(collections []int64) []qviews.ShardID {
	out := map[qviews.ShardID]bool{}
	for _, id := range collections {
		c := s.cache.GetCollection(id)
		if c == nil {
			continue
		}
		c.RangeShards(func(sh *balancercache.ShardEntry) bool { out[sh.ID()] = true; return true })
		if c.LoadConfig() == nil || c.DataView() == nil {
			continue
		}
		for _, r := range c.LoadConfig().Replicas {
			for _, sh := range c.DataView().Shards {
				out[qviews.ShardID{ReplicaID: r.ReplicaID, VChannel: sh.VChannel}] = true
			}
		}
	}
	ids := make([]qviews.ShardID, 0, len(out))
	for id := range out {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return shardLess(ids[i], ids[j]) })
	return ids
}

func (s *balanceSimulation) publish(id qviews.ShardID) {
	v := s.views[id]
	if v == nil {
		s.cache.PublishShard(id, nil)
		return
	}
	stats := &coordview.ShardStats{Segments: map[int64]*coordview.SegmentStats{}, Resources: map[int64]map[coordview.ResourceKey]struct{}{}}
	resident := map[int64]bool{}
	add := func(view *simulationView, state coordview.SegmentState) *coordview.ViewPlacement {
		if view == nil {
			return nil
		}
		p := &coordview.ViewPlacement{Assignments: map[int64]int64{}, Rows: map[int64]int64{}}
		for seg, node := range view.assignments {
			p.Assignments[seg] = node
			p.Rows[node] += view.rows[seg]
			resident[node] = true
			if stats.Segments[seg] == nil {
				stats.Segments[seg] = &coordview.SegmentStats{SegmentID: seg, PartitionID: 1, RowNum: view.rows[seg], HasRowNum: true, Nodes: map[int64]coordview.SegmentState{}}
			}
			st := state
			if state != coordview.SegmentStateUp && view.ready[seg] {
				st = coordview.SegmentStateReady
			}
			old, exists := stats.Segments[seg].Nodes[node]
			if !exists || st > old {
				stats.Segments[seg].Nodes[node] = st
			}
			if view.ready[seg] && s.nodes[node] != nil && s.nodes[node].Alive {
				if stats.Resources[node] == nil {
					stats.Resources[node] = map[coordview.ResourceKey]struct{}{}
				}
				stats.Resources[node][coordview.ResourceKey{PartitionID: 1, SegmentID: seg, DataVersion: view.version.DataVersion, LoadInfoVersion: view.loadVersion}] = struct{}{}
			}
		}
		return p
	}
	add(v.failed, coordview.SegmentStateUnrecoverable)
	stats.UpPlacement = add(v.up, coordview.SegmentStateUp)
	if v.up != nil {
		version := v.up.version
		stats.UpVersion = &version
		stats.UpLoadInfoVersion = v.up.loadVersion
		for node := range stats.UpPlacement.Rows {
			stats.UpNodes = append(stats.UpNodes, node)
		}
	}
	stats.PreparingPlacement = add(v.preparing, coordview.SegmentStatePreparing)
	if v.preparing != nil {
		version := v.preparing.version
		stats.PreparingVersion = &version
		for node := range stats.PreparingPlacement.Rows {
			stats.PreparingNodes = append(stats.PreparingNodes, node)
		}
	}
	for node := range resident {
		stats.ResidentNodes = append(stats.ResidentNodes, node)
	}
	s.cache.PublishShard(id, stats)
}

func (s *balanceSimulation) complete(id qviews.ShardID) {
	v := s.views[id]
	require.NotNil(s.t, v.preparing)
	for seg := range v.preparing.assignments {
		v.preparing.ready[seg] = true
	}
	v.up, v.preparing, v.failed = v.preparing, nil, nil
	s.publish(id)
}

// step applies a deterministic subset of a real plan. cap=-1 accepts all.
func (s *balanceSimulation) step(collections []int64, cap int, complete bool) *BalancePlan {
	dirty := s.scope(collections)
	started := time.Now()
	plan := s.policy.Plan(s.cache, dirty)
	s.counters.PlanMS += float64(time.Since(started).Nanoseconds()) / 1e6
	s.counters.Calls++
	s.counters.Continuations += len(plan.Continues)
	s.counters.Retries += len(plan.Retries)
	for _, id := range plan.Releases {
		delete(s.views, id)
		s.cache.PublishShard(id, nil)
		s.counters.Releases++
	}
	ids := make([]qviews.ShardID, 0, len(plan.Prepares))
	for id := range plan.Prepares {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return shardLess(ids[i], ids[j]) })
	for i, id := range ids {
		if cap >= 0 && i >= cap {
			s.counters.Rejected++
			continue
		}
		collection, _ := parseShardCollection(id)
		c := s.cache.GetCollection(collection)
		builder := plan.Prepares[id]
		wire := builder.Build()
		assignments := flattenAssignments(wire)
		wireCount := 0
		for _, n := range wire.GetQueryNode() {
			for _, p := range n.GetPartitions() {
				wireCount += len(p.GetSegmentIds())
			}
		}
		require.Equal(s.t, len(assignments), wireCount, "each segment appears exactly once")
		desired := c.DataView().Shard(id.VChannel)
		require.Equal(s.t, desired.SegmentCount, len(assignments))
		v := s.views[id]
		if v == nil {
			v = &simulationShard{}
			s.views[id] = v
		}
		old := v.up
		if v.preparing != nil {
			old = v.preparing
		}
		s.sequence++
		next := &simulationView{assignments: assignments, rows: map[int64]int64{}, ready: map[int64]bool{}, version: qviews.QueryViewVersion{DataVersion: builder.DataVersion(), QueryVersion: s.sequence}, loadVersion: c.ConfigVersion()}
		target := s.policy.layouts.domains[collection][findReplica(c.LoadConfig(), id.ReplicaID).ResourceGroup].targets[id.ReplicaID]
		for seg, node := range assignments {
			info, ok := c.DataView().Segment(seg)
			require.True(s.t, ok)
			require.Contains(s.t, target, node)
			require.True(s.t, s.nodes[node].Alive)
			require.False(s.t, s.nodes[node].Stopping)
			next.rows[seg] = info.RowNum
			if old != nil {
				if previous, exists := old.assignments[seg]; exists && previous != node {
					s.counters.MovedRows += info.RowNum
				}
			}
			key := coordview.ResourceKey{SegmentID: seg, PartitionID: info.PartitionID, DataVersion: next.version.DataVersion, LoadInfoVersion: next.loadVersion}
			resources := c.PlacementNode(node)
			if resources != nil && resources.HasResource(key) {
				next.ready[seg] = true
			} else {
				s.counters.LoadRows += info.RowNum
			}
		}
		v.preparing = next
		s.publish(id)
		s.counters.Prepares++
		if complete {
			s.complete(id)
		}
	}
	return plan
}

// sample recomputes demand and target rows from the model, independently of the
// cached aggregates and production score helpers. It does not affect Plan timing.
func (s *balanceSimulation) sample(check bool) simulationSample {
	sample := simulationSample{Groups: map[string]simulationRG{}}
	for id, n := range s.nodes {
		if n.Alive && !n.Stopping {
			g := sample.Groups[n.ResourceGroup]
			if g.Rows == nil {
				g.Rows = map[int64]int64{}
			}
			g.Rows[id] = 0
			sample.Groups[n.ResourceGroup] = g
		}
	}
	for _, id := range s.collections {
		c := s.cache.GetCollection(id)
		if c == nil || c.LoadConfig() == nil {
			continue
		}
		counts := map[string]int{}
		occupiedTargets := map[int64]int64{}
		for _, r := range c.LoadConfig().Replicas {
			counts[r.ResourceGroup]++
		}
		for group, count := range counts {
			g := sample.Groups[group]
			g.Demand += c.DataView().TotalRows * int64(min(count, len(g.Rows)))
			sample.Groups[group] = g
		}
		for _, r := range c.LoadConfig().Replicas {
			sample.DesiredReplicas++
			layout := s.policy.layouts.domains[id][r.ResourceGroup]
			if layout == nil {
				continue
			}
			targets := layout.targets[r.ReplicaID]
			if len(targets) == 0 {
				sample.SuspendedReplicas++
				continue
			}
			sample.ActiveReplicas++
			if check {
				for _, node := range targets {
					previous, exists := occupiedTargets[node]
					require.False(s.t, exists, "node %d shared by replica %d and %d", node, previous, r.ReplicaID)
					occupiedTargets[node] = r.ReplicaID
				}
			}
			replicaRows := map[int64]int64{}
			for _, sh := range c.DataView().Shards {
				sid := qviews.ShardID{ReplicaID: r.ReplicaID, VChannel: sh.VChannel}
				v := s.views[sid]
				if v == nil {
					if check {
						require.NotNil(s.t, v, "active shard must have a view")
					}
					continue
				}
				view := v.preparing
				if view == nil {
					view = v.up
				}
				if view == nil {
					continue
				}
				if v.preparing != nil {
					sample.Pending++
				}
				rows := map[int64]int64{}
				for seg, node := range view.assignments {
					n := s.nodes[node]
					if n == nil || !n.Alive || n.Stopping || n.ResourceGroup != r.ResourceGroup {
						continue
					}
					rows[node] += view.rows[seg]
					replicaRows[node] += view.rows[seg]
					g := sample.Groups[r.ResourceGroup]
					g.Rows[node] += view.rows[seg]
					sample.Groups[r.ResourceGroup] = g
				}
				sample.ActiveShards++
				sample.FanoutMean += float64(len(rows))
				sample.FanoutMax = max(sample.FanoutMax, len(rows))
				k := s.policy.fanouts[sid]
				sample.ExcessFanout += max(0, len(rows)-k)
				if sh.TotalRows > 0 && k > 0 {
					for _, n := range rows {
						sample.ShardMaxMean = math.Max(sample.ShardMaxMean, float64(n)*float64(k)/float64(sh.TotalRows))
					}
				}
				if check {
					require.Nil(s.t, v.preparing)
					require.Equal(s.t, sh.SegmentCount, len(view.assignments))
					require.Equal(s.t, c.DataView().DataVersion, view.version.DataVersion)
					require.Equal(s.t, c.ConfigVersion(), view.loadVersion)
					for _, part := range sh.Partitions {
						for _, seg := range part.Segments {
							node, ok := view.assignments[seg.SegmentID]
							require.True(s.t, ok)
							require.Contains(s.t, targets, node)
						}
					}
				}
			}
			if c.DataView().TotalRows > 0 {
				for _, r := range replicaRows {
					sample.CollectionMaxMean = math.Max(sample.CollectionMaxMean, float64(r)*float64(len(targets))/float64(c.DataView().TotalRows))
				}
			}
			if check {
				for node, rows := range replicaRows {
					require.Equal(s.t, rows, c.ReplicaRows(r.ReplicaID, node))
				}
			}
		}
	}
	if sample.ActiveShards > 0 {
		sample.FanoutMean /= float64(sample.ActiveShards)
	}
	cfg := s.cache.GetBalanceConfig()
	for name, g := range sample.Groups {
		var sum int64
		for _, r := range g.Rows {
			sum += r
		}
		sample.UnplacedRows += max(int64(0), g.Demand-sum)
		if len(g.Rows) > 0 && g.Demand > 0 {
			mean := float64(g.Demand) / float64(len(g.Rows))
			tolerance := math.Max(float64(cfg.AbsoluteToleranceRows), cfg.RelativeTolerance*mean)
			for _, r := range g.Rows {
				value := float64(r)
				diff := value - mean
				g.CV += diff * diff
				g.MaxMean = math.Max(g.MaxMean, value/mean)
				distance := math.Max(0, math.Abs(diff)-tolerance)
				g.BandPenalty += distance * distance / (2 * mean)
			}
			g.CV = math.Sqrt(g.CV/float64(len(g.Rows))) / mean
		}
		sample.Groups[name] = g
		if check {
			require.Zero(s.t, sample.UnplacedRows)
			if rg := s.cache.GetResourceGroup(name); rg != nil {
				require.Equal(s.t, g.Demand, rg.Demand(len(g.Rows)))
			}
			for node, rows := range g.Rows {
				require.Equal(s.t, rows, s.cache.GetNode(node).TargetRows())
			}
		}
	}
	return sample
}

func simulationCounterDelta(a, b simulationCounters) simulationCounters {
	return simulationCounters{Calls: a.Calls - b.Calls, Prepares: a.Prepares - b.Prepares, Releases: a.Releases - b.Releases, Rejected: a.Rejected - b.Rejected, Retries: a.Retries - b.Retries, Continuations: a.Continuations - b.Continuations, MovedRows: a.MovedRows - b.MovedRows, LoadRows: a.LoadRows - b.LoadRows, PlanMS: a.PlanMS - b.PlanMS}
}

func (s *balanceSimulation) settle(phase string, scope []int64, batch, limit int) simulationResult {
	if scope == nil {
		scope = s.collections
	}
	if batch <= 0 {
		batch = max(1, len(scope))
	}
	result := simulationResult{Scenario: s.t.Name(), Phase: phase, Scope: len(scope), Batch: batch, Before: s.sample(false), Config: *s.cache.GetBalanceConfig()}
	before := s.counters
	idle := 0
	for sweep := 0; sweep < limit; sweep++ {
		busy, continuing := false, false
		for start := 0; start < len(scope); start += batch {
			p := s.step(scope[start:min(start+batch, len(scope))], -1, true)
			busy = busy || len(p.Prepares)+len(p.Releases)+len(p.Retries) > 0
			continuing = continuing || len(p.Continues) > 0
		}
		result.Sweeps++
		result.Trace = append(result.Trace, s.sample(false))
		if busy {
			idle = 0
		} else if !continuing {
			idle++
		}
		if idle >= 2 {
			break
		}
	}
	result.After = s.sample(true)
	result.Counters = simulationCounterDelta(s.counters, before)
	result.Outcome = "stable_in_band"
	for _, g := range result.After.Groups {
		if g.BandPenalty > 1e-6 {
			result.Outcome = "stable_outside_band"
		}
	}
	if result.After.DesiredReplicas > 0 && result.After.ActiveReplicas == 0 {
		result.Outcome = "no_capacity"
	}
	if idle < 2 {
		result.Outcome = "exhausted"
	}
	s.emit(result)
	require.GreaterOrEqual(s.t, idle, 2, "simulation search exhausted its sweep budget")
	return result
}

func (s *balanceSimulation) emit(result simulationResult) {
	data, err := json.Marshal(result)
	require.NoError(s.t, err)
	s.t.Logf("SIM_RESULT %s", data)
}
