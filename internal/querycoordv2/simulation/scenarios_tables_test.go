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
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Table-level scenarios: many tables of very different sizes on a cluster
// whose memory is far from full, so node memory is easy to level and the
// question is whether each table ends spread over its nodes, and at what
// cost in moves. tableSpread (via memSpread) reports the per-table shape.

// tableShape draws a table's segments per channel from a heavy tail: most
// tables are small, a few are large, as in multi-tenant clusters.
func tableShape(rng *rand.Rand) (channels, segsPerChannel int, replicas int32) {
	channels = 1 + rng.Intn(2)
	switch p := rng.Float64(); {
	case p < 0.45:
		segsPerChannel = 1 + rng.Intn(2)
	case p < 0.75:
		segsPerChannel = 3 + rng.Intn(6)
	case p < 0.93:
		segsPerChannel = 10 + rng.Intn(20)
	default:
		segsPerChannel = 40 + rng.Intn(60)
	}
	replicas = 1
	if rng.Float64() < 0.1 {
		replicas = 2
	}
	return channels, segsPerChannel, replicas
}

// loadTables loads n tables starting at id first and waits for them.
func loadTables(t *testing.T, sim *Sim, rng *rand.Rand, first int64, n int) []int64 {
	ids := collectionIDs(first, n)
	sim.Broker.RowsSigma = 0.5
	for _, id := range ids {
		ch, segs, rep := tableShape(rng)
		sim.LoadCollection(id, rep, ch, segs, 20_000)
	}
	sim.Broker.RowsSigma = 0
	require.True(t, sim.WaitUntil(8*time.Minute, func() bool { return sim.Loaded(ids...) }), "tables did not load")
	return ids
}

// observe lets balance run for window (longer than the scaled cooldown, so
// a balancer that cools moves still finishes), then waits for quiet.
func observe(sim *Sim, window time.Duration) map[string]float64 {
	time.Sleep(window)
	quiet, ok := waitSettled(sim, 5*time.Minute, 5*time.Second)
	m := map[string]float64{"settled": boolMetric(ok), "quiet_seconds": seconds(quiet)}
	merge(m, "", memSpread(sim))
	return m
}

func tableNodeConfig() NodeConfig {
	cfg := DefaultNodeConfig()
	cfg.MemoryBytes = 16 << 30
	return cfg
}

// giantNodeConfig has the load bandwidth giant segments need: four 4GB
// loads sharing 100MB/s would outlast the task timeout and never land.
func giantNodeConfig() NodeConfig {
	cfg := tableNodeConfig()
	cfg.LoadBandwidthBytes = 800 << 20
	return cfg
}

// TB1 The cluster grew while tables kept arriving: 40 tables on 4 nodes,
// then 4 more nodes and 40 more tables, twice. Tables loaded early sit on
// the first nodes unless balance spreads them.
func TestSimTables_TB1_LayoutHistory(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	rng := rand.New(rand.NewSource(11))
	sim.AddNodes(qnBase, 4, tableNodeConfig())
	loadTables(t, sim, rng, 1, 40)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("grow twice")
	for step := 0; step < 2; step++ {
		sim.AddNodes(qnBase+4+int64(4*step), 4, tableNodeConfig())
		loadTables(t, sim, rng, int64(100*(step+1)), 40)
		time.Sleep(20 * time.Second)
	}
	m := observe(sim, 90*time.Second)
	reportPhase(sim, "TB1_layout_history", m, phase, rec, at)
}

// TB2 400 tables with heavy-tailed sizes (a tenth with two replicas) on 8
// nodes, then 4 nodes are added.
func TestSimTables_TB2_SkewedTablesScaleOut(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	rng := rand.New(rand.NewSource(22))
	sim.AddNodes(qnBase, 8, tableNodeConfig())
	loadTables(t, sim, rng, 1, 400)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	initial := tableSpread(sim)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("scale out 8->12")
	sim.AddNodes(qnBase+8, 4, tableNodeConfig())
	m := observe(sim, 120*time.Second)
	merge(m, "initial_", initial)
	reportPhase(sim, "TB2_skewed_tables_scale_out", m, phase, rec, at)
}

// TB3 Four of 60 tables ingest hard (flush every second, compact every 8s):
// their new segments must land spread, without churning the other tables.
func TestSimTables_TB3_HotTablesGrow(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 8, tableNodeConfig())
	ids := collectionIDs(1, 60)
	for _, id := range ids {
		sim.LoadCollection(id, 1, 2, 4, 20_000)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) }))
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("hot tables ingest")
	hot := ids[:4]
	stop := time.After(120 * time.Second)
	flush := time.NewTicker(time.Second)
	defer flush.Stop()
	tick := 0
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-flush.C:
		}
		tick++
		for _, id := range hot {
			sim.Broker.Flush(id, 20_000)
			if tick%8 == 0 {
				sim.Broker.Compact(id, 4)
			}
		}
	}
	m := observe(sim, 30*time.Second)
	reportPhase(sim, "TB3_hot_tables_grow", m, phase, rec, at)
}

// TB4 New small tables keep arriving (one every 2s for two minutes) beside
// 50 settled ones: each should land spread, and old tables stay put.
func TestSimTables_TB4_ContinuousOnboarding(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	rng := rand.New(rand.NewSource(44))
	sim.AddNodes(qnBase, 8, tableNodeConfig())
	loadTables(t, sim, rng, 1, 50)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("onboarding")
	var late []int64
	for i := 0; i < 60; i++ {
		id := int64(1000 + i)
		sim.LoadCollection(id, 1, 1, 2+rng.Intn(7), 20_000)
		late = append(late, id)
		time.Sleep(2 * time.Second)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(late...) }))
	m := observe(sim, 30*time.Second)
	merge(m, "late_", tinySpread(sim, late, 8))
	reportPhase(sim, "TB4_continuous_onboarding", m, phase, rec, at)
}

// TB5 Three nodes are replaced one after another (drain, then a fresh node)
// on a 200-table cluster: drained tables should land spread, and the fresh
// nodes take a fair part of every table.
func TestSimTables_TB5_RollingReplace(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	rng := rand.New(rand.NewSource(55))
	sim.AddNodes(qnBase, 8, tableNodeConfig())
	loadTables(t, sim, rng, 1, 200)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("rolling replace")
	m := map[string]float64{"drain_timeouts": 0}
	for i := 0; i < 3; i++ {
		if !sim.StopNode(qnBase+int64(i), 3*time.Minute) {
			m["drain_timeouts"]++
		}
		sim.AddNodes(qnBase+100+int64(i), 1, tableNodeConfig())
		time.Sleep(15 * time.Second)
	}
	merge(m, "", observe(sim, 90*time.Second))
	reportPhase(sim, "TB5_rolling_replace", m, phase, rec, at)
}

// legacyBalancer runs build under the default (pre-two-level) balancer, as a
// cluster that grew up before an upgrade, then restarts QueryCoord with the
// balancer under test. For the master suite both phases are the same.
func legacyBalancer(sim *Sim, build func()) {
	pt := paramtable.Get()
	keys := []string{pt.QueryCoordCfg.Balancer.Key, pt.QueryCoordCfg.StoppingBalanceAssignPolicy.Key}
	saved := make([]string, len(keys))
	for i, k := range keys {
		saved[i] = pt.GetWithDefault(k, "")
	}
	pt.Save(pt.QueryCoordCfg.Balancer.Key, pt.QueryCoordCfg.Balancer.DefaultValue)
	pt.Save(pt.QueryCoordCfg.StoppingBalanceAssignPolicy.Key, pt.QueryCoordCfg.StoppingBalanceAssignPolicy.DefaultValue)
	sim.RestartQueryCoord()
	build()
	for i, k := range keys {
		if saved[i] == "" {
			pt.Reset(k)
		} else {
			pt.Save(k, saved[i])
		}
	}
	sim.RestartQueryCoord()
}

// loadShaped loads count tables of segs segments (rows lognormal around
// rows with sigma), one channel each, starting at id.
func loadShaped(sim *Sim, id int64, count, segs int, rows int64, sigma float64, rgs ...string) []int64 {
	if len(rgs) == 0 {
		rgs = []string{meta.DefaultResourceGroupName}
	}
	sim.Broker.RowsSigma = sigma
	defer func() { sim.Broker.RowsSigma = 0 }()
	ids := collectionIDs(id, count)
	for _, c := range ids {
		sim.LoadCollectionInRGs(c, rgs, 1, 1, segs, rows)
	}
	return ids
}

func waitLoaded(t *testing.T, sim *Sim, ids []int64) {
	require.True(t, sim.WaitUntil(10*time.Minute, func() bool { return sim.Loaded(ids...) }), "tables did not load")
}

// TB6 Shaped on in01-16bd387abf89103 (8 QN, 1580 tables, 30-39% memory, 278k
// moves in 7 days): three large tables (313/265/156 segments), a few tables
// of one or two ~600MiB segments, and hundreds of tiny ones. The layout grew
// under the old balancer on 4 nodes, then 4 more; then QueryCoord upgrades.
// How much does the new balancer move to spread every table, and does it
// then stay still?
func TestSimTables_TB6_LegacyLayoutUpgrade(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := DefaultNodeConfig()
	cfg.MemoryBytes = 32 << 30
	var ids []int64
	legacyBalancer(sim, func() {
		sim.AddNodes(qnBase, 4, cfg)
		// 512 bytes per row: 49k rows ~ 24MiB, 1.2M rows ~ 600MiB.
		ids = append(ids, loadShaped(sim, 1, 1, 313, 49_000, 0.4)...)
		ids = append(ids, loadShaped(sim, 2, 1, 265, 19_000, 0.4)...)
		ids = append(ids, loadShaped(sim, 3, 1, 156, 16_000, 0.4)...)
		ids = append(ids, loadShaped(sim, 10, 6, 2, 1_200_000, 0.2)...)
		ids = append(ids, loadShaped(sim, 100, 300, 2, 2_000, 1.5)...)
		waitLoaded(t, sim, ids)
		sim.AddNodes(qnBase+4, 4, cfg)
		ids = append(ids, loadShaped(sim, 1000, 300, 2, 2_000, 1.5)...)
		waitLoaded(t, sim, ids)
		time.Sleep(30 * time.Second)
		_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	})
	legacy := tableSpread(sim)
	legacyMem := memSpread(sim)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("upgraded")
	m := observe(sim, 150*time.Second)
	merge(m, "legacy_", legacy)
	m["legacy_mem_max_over_mean"] = legacyMem["mem_max_over_mean"]
	m["legacy_mem_min_over_mean"] = legacyMem["mem_min_over_mean"]
	// Static afterwards: no further moves.
	quiet := sim.StartPhase()
	time.Sleep(60 * time.Second)
	m["static_moves_after"] = quiet.Metrics()["segment_loads"]
	reportPhase(sim, "TB6_legacy_layout_upgrade", m, phase, rec, at)
}

// TB7 Shaped on in01-60b4c0b69cb4da7 (11 QN in two resource groups of 6 and
// 5, 1556 tables, mostly tiny segments, a few large tables): each table
// lives in one group; then each group gains a node.
func TestSimTables_TB7_TwoGroupsManyTables(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := DefaultNodeConfig()
	cfg.MemoryBytes = 32 << 30
	sim.AddNodes(qnBase, 11, cfg)
	sim.CreateResourceGroup("rg_a", 6)
	sim.CreateResourceGroup("rg_b", 5)
	var ids []int64
	ids = append(ids, loadShaped(sim, 1, 1, 331, 35_000, 0.4, "rg_b")...)
	ids = append(ids, loadShaped(sim, 2, 1, 155, 74_000, 0.4, "rg_a")...)
	ids = append(ids, loadShaped(sim, 3, 1, 631, 1_200, 0.4, "rg_a")...)
	ids = append(ids, loadShaped(sim, 10, 10, 30, 30_000, 0.5, "rg_a")...)
	ids = append(ids, loadShaped(sim, 30, 10, 30, 30_000, 0.5, "rg_b")...)
	ids = append(ids, loadShaped(sim, 100, 300, 3, 400, 1.5, "rg_a")...)
	ids = append(ids, loadShaped(sim, 1000, 300, 3, 400, 1.5, "rg_b")...)
	waitLoaded(t, sim, ids)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	initial := tableSpread(sim)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("each group gains a node")
	sim.AddNodes(qnBase+11, 2, cfg)
	require.NoError(t, sim.Meta().ResourceManager.AlterResourceGroups(sim.Ctx(), map[string]*rgpb.ResourceGroupConfig{
		"rg_a": meta.NewResourceGroupConfig(7, 7),
		"rg_b": meta.NewResourceGroupConfig(6, 6),
	}))
	m := observe(sim, 120*time.Second)
	merge(m, "initial_", initial)
	m["rg_a_row_cv"] = sim.RGRowCV("rg_a")
	m["rg_b_row_cv"] = sim.RGRowCV("rg_b")
	reportPhase(sim, "TB7_two_groups_many_tables", m, phase, rec, at)
}

// TB8 Shaped on in01-f9f1eb17e982160 (29 QN x 64G, 27 tables, 45-74%
// memory): few tables, many nodes, tables of 30-380 segments with sizes
// varying within a table. Grown from 20 to 29 nodes.
func TestSimTables_TB8_ManyNodesFewTables(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 20, tableNodeConfig())
	var ids []int64
	for i, segs := range []int{376, 186, 246, 209, 84, 57, 48, 32, 30, 34} {
		ids = append(ids, loadShaped(sim, int64(1+i), 1, segs, 30_000, 0.6)...)
	}
	ids = append(ids, loadShaped(sim, 100, 17, 8, 10_000, 0.6)...)
	waitLoaded(t, sim, ids)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("scale out 20->29")
	sim.AddNodes(qnBase+20, 9, tableNodeConfig())
	m := observe(sim, 120*time.Second)
	reportPhase(sim, "TB8_many_nodes_few_tables", m, phase, rec, at)
}

// TB9 Four big tables of 2GB segments (ten each, 80GB on 8 x 16GB nodes,
// 62%) beside 20 small ones, then three nodes join. Every 2GB segment is
// an eighth of a node's band, so memory can only level if the big tables
// each give the new nodes their share, and a table's segment count must
// stay even while another table's giant segments are in the way.
func TestSimTables_TB9_GiantSegmentsScaleOut(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 8, giantNodeConfig())
	ids := loadShaped(sim, 1, 4, 10, 4_000_000, 0)
	ids = append(ids, loadShaped(sim, 10, 20, 4, 200_000, 0.3)...)
	waitLoaded(t, sim, ids)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	initial := tableSpread(sim)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("scale out 8->11")
	sim.AddNodes(qnBase+8, 3, giantNodeConfig())
	m := observe(sim, 150*time.Second)
	merge(m, "initial_", initial)
	reportPhase(sim, "TB9_giant_segments_scale_out", m, phase, rec, at)
}

// TB10 Three tables of equal size but segments of 4GB, 1GB and 256MB on 6
// x 16GB nodes (50%): a node is replaced, then one of the big tables
// ingests and compacts into fresh 4GB segments. The coarse table must not
// push the fine ones off their shares, nor the fine ones churn to make
// room for each new giant segment.
func TestSimTables_TB10_CoarseAndFineTables(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 6, giantNodeConfig())
	ids := loadShaped(sim, 1, 1, 4, 8_000_000, 0)
	ids = append(ids, loadShaped(sim, 2, 1, 16, 2_000_000, 0)...)
	ids = append(ids, loadShaped(sim, 3, 1, 64, 500_000, 0)...)
	waitLoaded(t, sim, ids)
	_, _ = waitSettled(sim, 3*time.Minute, 5*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	phase := sim.StartPhase()
	at := rec.Mark("replace node, then giant compactions")
	m := map[string]float64{"drain_timeouts": 0}
	if !sim.StopNode(qnBase, 3*time.Minute) {
		m["drain_timeouts"]++
	}
	sim.AddNodes(qnBase+100, 1, giantNodeConfig())
	time.Sleep(30 * time.Second)
	// Table 1 grows by 4GB every 20s: eight 500MB flushes compacted into one.
	for i := 0; i < 4; i++ {
		for j := 0; j < 8; j++ {
			sim.Broker.Flush(1, 1_000_000)
			time.Sleep(2 * time.Second)
		}
		sim.Broker.Compact(1, 8)
		time.Sleep(4 * time.Second)
	}
	merge(m, "", observe(sim, 90*time.Second))
	reportPhase(sim, "TB10_coarse_and_fine_tables", m, phase, rec, at)
}
