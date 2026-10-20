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

//go:build test

package simulation

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

const (
	narrowRow = 512      // ~128-dim float vector + scalars
	wideRow   = 16 << 10 // ~4096-dim float vector
	nodeMem   = 8 << 30  // small nodes so memory pressure is cheap to reach
	memLimit  = 0.9      // QueryNode overload threshold
)

func memConfig() NodeConfig {
	cfg := CalibratedNodeConfig()
	cfg.MemoryBytes = nodeMem
	cfg.LoadJitterSigma = 0
	return cfg
}

// memoryMetrics reports per-node memory use and the rejections seen.
func memoryMetrics(sim *Sim, m map[string]float64) {
	var used []float64
	for _, v := range sim.Fleet.Snapshot() {
		if v.Up && !sim.Fleet.IsStopping(v.ID) {
			used = append(used, float64(v.UsedMemory)/nodeMem)
		}
	}
	sort.Float64s(used)
	if len(used) > 0 {
		m["mem_util_min"] = used[0]
		m["mem_util_max"] = used[len(used)-1]
	}
	c := sim.FleetCounters()
	m["oom_rejections"] = c["loads_rejected_oom"]
	m["oom_rejections_penalized"] = c["loads_rejected_threshold"]
	m["loads_failed"] = c["loads_failed"]
}

// collectionsMissing counts loaded collections with at least one segment
// served by no replica node, i.e. collections not fully queryable.
func collectionsMissing(sim *Sim) int {
	qc := sim.current()
	views := sim.Fleet.Snapshot()
	n := 0
	for _, id := range qc.meta.GetAll(qc.ctx) {
		target := qc.targetMgr.GetSealedSegmentsByCollection(qc.ctx, id, meta.CurrentTarget)
		have := map[int64]bool{}
		for _, v := range views {
			if !v.Up {
				continue
			}
			for seg, coll := range v.Collection {
				if coll == id {
					have[seg] = true
				}
			}
		}
		for seg := range target {
			if !have[seg] {
				n++
				break
			}
		}
	}
	return n
}

// S14a Collections with very different row widths. QueryCoord balances by
// rows, QueryNodes fill by bytes: does memory stay even, and does anything
// hit the overload threshold while the cluster is at ~70% of memory?
func TestSimMaster_S14a_WideRowsMemorySkew(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 4, memConfig())
	// 6 narrow collections (~6GB total) and 1 wide one with 14 segments
	// (~11.5GB): 14 does not divide by 4 nodes.
	for id := int64(1); id <= 6; id++ {
		sim.Broker.CreateWideCollection(id, 2, 10, 100_000, narrowRow)
		sim.loadExisting(id, 1)
	}
	sim.Broker.CreateWideCollection(7, 2, 7, 50_000, wideRow)
	sim.loadExisting(7, 1)
	ids := collectionIDs(1, 7)
	loaded := sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) })
	_, settled := waitSettled(sim, 2*time.Minute, 3*time.Second)
	m := map[string]float64{"loaded": boolMetric(loaded), "settled": boolMetric(settled)}
	memoryMetrics(sim, m)
	// Then lose one node: demand becomes ~94% of the remaining memory.
	sim.KillNode(qnBase + 1)
	time.Sleep(90 * time.Second)
	m["after_kill_collections_missing"] = float64(collectionsMissing(sim))
	m["after_kill_missing_segments"] = float64(sim.Observe().MissingSegments)
	after := map[string]float64{}
	memoryMetrics(sim, after)
	merge(m, "after_kill_", after)
	sim.Report(Result{Scenario: "S14a_wide_rows_memory_skew", Metrics: m})
	require.True(t, loaded, "initial load at ~70% memory did not finish")
}

// S14b Not enough memory after a crash: QueryCoord cannot place everything.
// Does it keep the shortfall in a few collections or spread it so that many
// collections are partially unavailable, and does it retry forever?
func TestSimMaster_S14b_CapacityShortfall(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 4, memConfig())
	// 12 collections x 24 x 76MB = ~21.9GB: 68% of 32GB, 95% of 24GB.
	ids := collectionIDs(1, 12)
	for _, id := range ids {
		sim.Broker.CreateWideCollection(id, 2, 12, 150_000, narrowRow)
		sim.loadExisting(id, 1)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) }))
	_, ok := waitSettled(sim, 2*time.Minute, 3*time.Second)
	require.True(t, ok)
	before := sim.FleetCounters()
	rec := sim.StartRecorder(500 * time.Millisecond)
	sim.KillNode(qnBase)
	time.Sleep(120 * time.Second)
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"collections_missing":  float64(collectionsMissing(sim)),
		"collections_total":    float64(len(ids)),
		"missing_segments":     float64(sim.Observe().MissingSegments),
		"oom_rejections_2min":  d["loads_rejected_oom"],
		"penalized_rejections": d["loads_rejected_threshold"],
		"loads_failed_2min":    d["loads_failed"],
		"loads_succeeded_2min": d["loads_succeeded"],
		"releases_2min":        d["releases"],
	}
	memoryMetrics(sim, m)
	merge(m, "", Summary(rec.Samples()))
	sim.Report(Result{Scenario: "S14b_capacity_shortfall", Metrics: m})
}

// S14c Fragmentation: enough free memory in total but on no single node.
// Each node is ~76% full of small segments; a new collection has two 1.5GB
// segments, each larger than any node's headroom (~1.1GB). QueryCoord does
// not move small segments away to make room.
func TestSimMaster_S14c_Fragmentation(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 4, memConfig())
	sim.Broker.CreateWideCollection(1, 1, 192, 250_000, narrowRow) // 192 x 128MB
	sim.loadExisting(1, 1)
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(1) }))
	_, ok := waitSettled(sim, 2*time.Minute, 3*time.Second)
	require.True(t, ok)
	before := sim.FleetCounters()
	sim.Broker.CreateWideCollection(2, 1, 2, 96_000, wideRow) // 2 x 1.5GB
	sim.loadExisting(2, 1)
	start := time.Now()
	loaded := sim.WaitUntil(3*time.Minute, func() bool { return sim.Loaded(2) })
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"loaded":          boolMetric(loaded),
		"wait_seconds":    time.Since(start).Seconds(),
		"oom_rejections":  d["loads_rejected_oom"],
		"penalized":       d["loads_rejected_threshold"],
		"moves_of_others": d["releases"],
	}
	var free float64
	for _, v := range sim.Fleet.Snapshot() {
		free += (float64(nodeMem)*memLimit - float64(v.UsedMemory)) / (1 << 30)
	}
	m["free_total_gb"] = free
	memoryMetrics(sim, m)
	sim.Report(Result{Scenario: "S14c_fragmentation", Metrics: m})
}

// S15 Big and tiny collections together. A tiny collection's node score is
// almost all 0.1 x global rows and the 5% tolerance is 5% of that, so a tiny
// collection may sit entirely on one node (a query hot spot) and never be
// spread. Measured after initial load and after a scale-out.
func TestSimMaster_S15_TinyCollectionsBesideBigOne(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	sim.AddNodes(qnBase, 4, cfg)
	sim.Broker.CreateCollection(1, 2, 50, 500_000) // 50M rows
	sim.loadExisting(1, 1)
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(1) }))
	tiny := collectionIDs(100, 20)
	for _, id := range tiny {
		sim.Broker.CreateCollection(id, 1, 8, 2_000) // 16k rows each
		sim.loadExisting(id, 1)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(tiny...) }))
	_, ok := waitSettled(sim, 2*time.Minute, 5*time.Second)
	require.True(t, ok)
	m := map[string]float64{}
	merge(m, "initial_", tinySpread(sim, tiny, 4))
	sim.AddNodes(qnBase+4, 1, cfg)
	time.Sleep(5 * time.Second)
	_, ok = waitSettled(sim, 3*time.Minute, 5*time.Second)
	m["scaleout_settled"] = boolMetric(ok)
	merge(m, "after_scaleout_", tinySpread(sim, tiny, 5))
	// Tiny collections loaded after the scale-out.
	late := collectionIDs(200, 20)
	for _, id := range late {
		sim.Broker.CreateCollection(id, 1, 8, 2_000)
		sim.loadExisting(id, 1)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(late...) }))
	_, _ = waitSettled(sim, 2*time.Minute, 5*time.Second)
	merge(m, "late_", tinySpread(sim, late, 5))
	sim.Report(Result{Scenario: "S15_tiny_beside_big", Metrics: m})
}

// tinySpread measures how concentrated each tiny collection is: the share of
// its segments on its busiest node (ideal 1/nodes rounded up), and how many
// collections sit entirely on one node.
func tinySpread(sim *Sim, ids []int64, nodes int) map[string]float64 {
	views := sim.Fleet.Snapshot()
	var worst, sum float64
	allOnOne := 0
	for _, id := range ids {
		per := map[int64]int{}
		total := 0
		for node, v := range views {
			if !v.Up {
				continue
			}
			for _, coll := range v.Collection {
				if coll == id {
					per[node]++
					total++
				}
			}
		}
		if total == 0 {
			continue
		}
		top := 0
		for _, c := range per {
			top = max(top, c)
		}
		share := float64(top) / float64(total)
		worst = max(worst, share)
		sum += share
		if len(per) == 1 {
			allOnOne++
		}
	}
	return map[string]float64{
		"max_share_mean":        sum / float64(len(ids)),
		"max_share_worst":       worst,
		"ideal_max_share":       float64((8+nodes-1)/nodes) / 8, // 8 segments per tiny collection
		"collections_on_1_node": float64(allOnOne),
		"collections":           float64(len(ids)),
		"nodes":                 float64(nodes),
	}
}
