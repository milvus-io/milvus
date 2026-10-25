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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/rgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
)

// Every scenario asserts invariants (hard failures) and reports benefit
// metrics (compared across branches with MILVUS_SIM_REPORT). Liveness bounds
// are deliberately generous: they fail only when the cluster never converges.

func requireInvariants(t *testing.T, sim *Sim, rec *Recorder) map[string]float64 {
	t.Helper()
	m := Summary(rec.Samples())
	for _, smp := range rec.Samples() {
		if smp.Excess != "" {
			t.Logf("DEBUG excess copies at %s: %s", smp.At, smp.Excess)
			break
		}
	}
	require.LessOrEqual(t, m["max_replica_copies"], 2.0, "a segment had more than two live copies in one replica")
	require.Zero(t, sim.Fleet.Stats.LoadsToStopping.Load(), "QueryCoord loaded data onto a stopping node")
	return m
}

// 1.1 Many small collections.
func TestSimP0_1_1_ManyCollections(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(1, scaled(20), DefaultNodeConfig())
	rec := sim.StartRecorder(100 * time.Millisecond)
	ids := collectionIDs(1, scaled(500))
	loadTime := loadAll(t, sim, ids, 2, 1, 2, 5_000, 5*time.Minute)
	settle, ok := waitSettled(sim, 2*time.Minute, time.Second)
	require.True(t, ok, "cluster did not settle")
	m := requireInvariants(t, sim, rec)
	m["load_seconds"] = loadTime.Seconds()
	m["settle_seconds"] = seconds(settle)
	merge(m, "", sim.FleetCounters())
	sim.Report(Result{Scenario: "1.1_many_collections", Metrics: m})
}

// 1.2 One collection with many segments.
func TestSimP0_1_2_HugeCollection(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(1, scaled(20), DefaultNodeConfig())
	rec := sim.StartRecorder(100 * time.Millisecond)
	segmentsPerChannel := scaled(2_500)
	loadTime := loadAll(t, sim, []int64{1}, 1, 8, segmentsPerChannel, 2_000, 10*time.Minute)
	m := requireInvariants(t, sim, rec)
	m["load_seconds"] = loadTime.Seconds()
	m["segments_per_second"] = float64(8*segmentsPerChannel) / loadTime.Seconds()
	merge(m, "", sim.FleetCounters())
	sim.Report(Result{Scenario: "1.2_huge_collection", Metrics: m})
}

// skewedCluster loads collections onto `initial` nodes, then adds `extra`
// empty nodes so ordinary balance has work to do.
func skewedCluster(t *testing.T, initial, extra, collections int, replicas int32) (*Sim, *Recorder, []int64) {
	sim := New(t, Options{})
	sim.AddNodes(1, initial, DefaultNodeConfig())
	ids := collectionIDs(1, collections)
	loadAll(t, sim, ids, replicas, 2, 8, 20_000, 5*time.Minute)
	_, ok := waitSettled(sim, time.Minute, 500*time.Millisecond)
	require.True(t, ok, "initial load did not settle")
	rec := sim.StartRecorder(50 * time.Millisecond)
	sim.AddNodes(int64(initial+1), extra, DefaultNodeConfig())
	rec.Mark("scale-out")
	return sim, rec, ids
}

// 2.1 Continuous ingestion must not starve ordinary balance.
func TestSimP0_2_1_BalanceUnderIngestion(t *testing.T) {
	requireSim(t)
	sim, rec, ids := skewedCluster(t, 6, 2, scaled(20), 1)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	// One 10MB segment per collection per second: ~25% of the fleet's load
	// bandwidth, so the balancer, not the nodes, is what is measured.
	fl := startFlusher(sim, ids, time.Second, 20_000, 0)
	balanced, ok := waitRowBalanced(sim, 0.15, 2*time.Minute)
	fl.Stop()
	m := requireInvariants(t, sim, rec)
	m["balanced"] = boolMetric(ok)
	m["balance_seconds"] = seconds(balanced)
	merge(m, "", delta(sim.FleetCounters(), before))
	sim.Report(Result{Scenario: "2.1_balance_under_ingestion", Metrics: m})
	require.True(t, ok, "rows never balanced while ingestion continued")
}

// 2.2 Ingestion plus compaction: no duplicate copies, no stale segments.
func TestSimP0_2_2_IngestionAndCompaction(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(1, 8, DefaultNodeConfig())
	ids := collectionIDs(1, scaled(10))
	loadAll(t, sim, ids, 2, 2, 8, 20_000, 5*time.Minute)
	rec := sim.StartRecorder(50 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	fl := startFlusher(sim, ids, 200*time.Millisecond, 10_000, 5)
	time.Sleep(20 * time.Second)
	fl.Stop()
	settle, ok := waitSettled(sim, time.Minute, 2*time.Second)
	require.True(t, ok, "cluster did not settle after ingestion stopped")
	stale := staleSegments(sim)
	m := requireInvariants(t, sim, rec)
	m["settle_seconds"] = seconds(settle)
	m["stale_segments"] = float64(stale)
	merge(m, "", delta(sim.FleetCounters(), before))
	sim.Report(Result{Scenario: "2.2_ingestion_and_compaction", Metrics: m})
	require.Zero(t, stale, "segments outside the target stayed loaded")
}

// staleSegments counts loaded segments that DataCoord no longer has.
func staleSegments(sim *Sim) int {
	live := make(map[int64]bool)
	for _, id := range sim.Meta().GetAll(sim.Ctx()) {
		for seg := range sim.Broker.SegmentRows(id) {
			live[seg] = true
		}
	}
	stale := 0
	for _, v := range sim.Fleet.Snapshot() {
		for seg := range v.Segments {
			if !live[seg] {
				stale++
			}
		}
	}
	return stale
}

func boolMetric(ok bool) float64 {
	if ok {
		return 1
	}
	return 0
}

// recoveryCluster is a settled 2-replica cluster used by failure scenarios.
func recoveryCluster(t *testing.T, nodes, collections int) (*Sim, *Recorder) {
	sim := New(t, Options{})
	sim.AddNodes(1, nodes, DefaultNodeConfig())
	loadAll(t, sim, collectionIDs(1, collections), 2, 2, 8, 20_000, 5*time.Minute)
	_, ok := waitSettled(sim, time.Minute, 500*time.Millisecond)
	require.True(t, ok, "initial load did not settle")
	return sim, sim.StartRecorder(50 * time.Millisecond)
}

func recoveryMetrics(t *testing.T, sim *Sim, rec *Recorder, from time.Duration, before map[string]float64) map[string]float64 {
	settle, ok := waitSettled(sim, 2*time.Minute, time.Second)
	require.True(t, ok, "cluster did not settle after recovery")
	recovered := FirstTime(rec.Samples(), from, func(s Sample) bool { return s.MissingSegments == 0 && s.LeaderlessChannels == 0 })
	m := requireInvariants(t, sim, rec)
	merge(m, "", Summary(rec.Since(from)))
	m["recovery_seconds"] = seconds(recovered)
	m["settle_seconds"] = seconds(settle)
	if recovered >= 0 {
		// Unavailability after recovery completed comes from later balance or
		// churn, which a correct Move (load before release) never causes.
		m["missing_after_recovery_samples"] = Summary(rec.Since(from + recovered))["missing_segment_samples"]
	}
	merge(m, "", delta(sim.FleetCounters(), before))
	return m
}

// 3.1 One QueryNode crashes.
func TestSimP0_3_1_SingleNodeCrash(t *testing.T) {
	requireSim(t)
	sim, rec := recoveryCluster(t, 10, scaled(30))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("kill 3")
	sim.KillNode(3)
	sim.Report(Result{Scenario: "3.1_single_node_crash", Metrics: recoveryMetrics(t, sim, rec, at, before)})
}

// 3.2 Several QueryNodes crash at once.
func TestSimP0_3_2_MultiNodeCrash(t *testing.T) {
	requireSim(t)
	sim, rec := recoveryCluster(t, 10, scaled(30))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("kill 2,5,8")
	for _, id := range []int64{2, 5, 8} {
		sim.KillNode(id)
	}
	sim.Report(Result{Scenario: "3.2_multi_node_crash", Metrics: recoveryMetrics(t, sim, rec, at, before)})
}

// 3.3 QueryCoord restarts while balance Moves are in flight. The new
// incarnation must not plan a second Move for the same data.
func TestSimP0_3_3_RestartDuringMove(t *testing.T) {
	requireSim(t)
	sim, rec, _ := skewedCluster(t, 6, 2, scaled(20), 1)
	// Make loads onto the new nodes slow so Moves are still running.
	sim.Fleet.SetSlowdown(7, 40)
	sim.Fleet.SetSlowdown(8, 40)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	require.True(t, sim.WaitUntil(30*time.Second, func() bool { s, _ := sim.PendingTasks(); return s > 0 }), "no balance Move started")
	at := rec.Mark("restart querycoord")
	sim.RestartQueryCoord()
	sim.Fleet.SetSlowdown(7, 1)
	sim.Fleet.SetSlowdown(8, 1)
	balanced, ok := waitBalanced(sim, 0.15, 2*time.Minute)
	m := requireInvariants(t, sim, rec)
	merge(m, "", Summary(rec.Since(at)))
	m["balanced"] = boolMetric(ok)
	m["balance_seconds"] = seconds(balanced)
	merge(m, "", delta(sim.FleetCounters(), before))
	sim.Report(Result{Scenario: "3.3_restart_during_move", Metrics: m})
	require.True(t, ok, "cluster did not rebalance after restart")
}

// 3.4 QueryCoord restarts while recovering from a node crash.
func TestSimP0_3_4_RestartDuringRecovery(t *testing.T) {
	requireSim(t)
	sim, rec := recoveryCluster(t, 10, scaled(30))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("kill 4")
	sim.KillNode(4)
	time.Sleep(300 * time.Millisecond)
	rec.Mark("restart querycoord")
	sim.RestartQueryCoord()
	sim.Report(Result{Scenario: "3.4_restart_during_recovery", Metrics: recoveryMetrics(t, sim, rec, at, before)})
}

// 4.1 A node crashes while ordinary balance is moving data.
func TestSimP0_4_1_CrashDuringBalance(t *testing.T) {
	requireSim(t)
	sim, rec, _ := skewedCluster(t, 6, 2, scaled(20), 2)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	require.True(t, sim.WaitUntil(30*time.Second, func() bool { s, _ := sim.PendingTasks(); return s > 0 }), "no balance Move started")
	at := rec.Mark("kill 2")
	sim.KillNode(2)
	sim.Report(Result{Scenario: "4.1_crash_during_balance", Metrics: recoveryMetrics(t, sim, rec, at, before)})
}

// 4.2 A second node crashes while the first one is being recovered.
func TestSimP0_4_2_SecondCrashDuringRecovery(t *testing.T) {
	requireSim(t)
	sim, rec := recoveryCluster(t, 10, scaled(30))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("kill 3")
	sim.KillNode(3)
	time.Sleep(200 * time.Millisecond)
	rec.Mark("kill 6")
	sim.KillNode(6)
	sim.Report(Result{Scenario: "4.2_second_crash_during_recovery", Metrics: recoveryMetrics(t, sim, rec, at, before)})
}

// 4.3 A node is drained (rolling upgrade) while ordinary balance is running.
// Draining must not wait for the ordinary balance round.
func TestSimP0_4_3_DrainDuringBalance(t *testing.T) {
	requireSim(t)
	sim, rec, _ := skewedCluster(t, 6, 2, scaled(20), 2)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	require.True(t, sim.WaitUntil(30*time.Second, func() bool { s, _ := sim.PendingTasks(); return s > 0 }), "no balance Move started")
	at := rec.Mark("stop 1")
	start := time.Now()
	drained := sim.StopNode(1, 2*time.Minute)
	drainTime := time.Since(start)
	m := recoveryMetrics(t, sim, rec, at, before)
	m["drained"] = boolMetric(drained)
	m["drain_seconds"] = drainTime.Seconds()
	sim.Report(Result{Scenario: "4.3_drain_during_balance", Metrics: m})
	require.True(t, drained, "node was not drained before its stop timeout")
}

// 5.1 A large cluster gains one node. Data should move onto it, not shuffle
// between existing nodes.
func TestSimP0_5_1_ScaleOutOne(t *testing.T) {
	requireSim(t)
	initial := scaled(30)
	sim, rec, _ := skewedCluster(t, initial, 1, scaled(60), 1)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	loadsBefore := sim.Fleet.Stats.LoadsByNode()
	newNode := int64(initial + 1)
	balanced, ok := waitBalanced(sim, 0.15, 3*time.Minute)
	loadsAfter := sim.Fleet.Stats.LoadsByNode()
	var toNew, toOld int64
	for node, n := range loadsAfter {
		if node == newNode {
			toNew += n - loadsBefore[node]
		} else {
			toOld += n - loadsBefore[node]
		}
	}
	m := requireInvariants(t, sim, rec)
	m["balanced"] = boolMetric(ok)
	m["balance_seconds"] = seconds(balanced)
	m["loads_to_new_node"] = float64(toNew)
	m["loads_between_old_nodes"] = float64(toOld)
	merge(m, "", delta(sim.FleetCounters(), before))
	sim.Report(Result{Scenario: "5.1_scale_out_one", Metrics: m})
	require.True(t, ok, "new node was never filled")
}

// 5.3 Graceful scale-in of one node.
func TestSimP0_5_3_ScaleIn(t *testing.T) {
	requireSim(t)
	sim, rec := recoveryCluster(t, 10, scaled(30))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("stop 5")
	start := time.Now()
	drained := sim.StopNode(5, 2*time.Minute)
	drainTime := time.Since(start)
	m := recoveryMetrics(t, sim, rec, at, before)
	m["drained"] = boolMetric(drained)
	m["drain_seconds"] = drainTime.Seconds()
	sim.Report(Result{Scenario: "5.3_scale_in", Metrics: m})
	require.True(t, drained, "node was not drained before its stop timeout")
	require.Zero(t, m["missing_segment_samples"], "graceful stop made data unavailable")
}

// 5.4 Rolling upgrade: every node is drained, restarted empty and rejoins.
func TestSimP0_5_4_RollingUpgrade(t *testing.T) {
	requireSim(t)
	nodes := 8
	sim, rec := recoveryCluster(t, nodes, scaled(20))
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("rolling upgrade")
	start := time.Now()
	var maxDrain time.Duration
	allDrained := true
	for id := int64(1); id <= int64(nodes); id++ {
		t0 := time.Now()
		if !sim.StopNode(id, 2*time.Minute) {
			allDrained = false
		}
		maxDrain = max(maxDrain, time.Since(t0))
		sim.RestartNode(id)
	}
	upgrade := time.Since(start)
	m := recoveryMetrics(t, sim, rec, at, before)
	m["all_drained"] = boolMetric(allDrained)
	m["upgrade_seconds"] = upgrade.Seconds()
	m["max_drain_seconds"] = maxDrain.Seconds()
	sim.Report(Result{Scenario: "5.4_rolling_upgrade", Metrics: m})
	require.True(t, allDrained, "a node was not drained before its stop timeout")
}

// 7.1 One resource group recovers while another must keep balancing.
func TestSimP0_7_1_ResourceGroupIsolation(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(1, 10, DefaultNodeConfig())
	sim.CreateResourceGroup("rg_a", 5)
	sim.CreateResourceGroup("rg_b", 4)
	for id := int64(1); id <= int64(scaled(10)); id++ {
		sim.LoadCollectionInRGs(id, []string{"rg_a"}, 1, 2, 8, 20_000)
		sim.LoadCollectionInRGs(100+id, []string{"rg_b"}, 1, 2, 8, 20_000)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool {
		for _, id := range sim.Meta().GetAll(sim.Ctx()) {
			if !sim.Loaded(id) {
				return false
			}
		}
		return true
	}))
	_, ok := waitSettled(sim, time.Minute, 500*time.Millisecond)
	require.True(t, ok)
	rec := sim.StartRecorder(50 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	// rg_b gains an empty node (needs balance) while rg_a loses one (needs
	// recovery). Balance in rg_b must not wait for rg_a.
	sim.AddNodes(11, 1, DefaultNodeConfig())
	require.NoError(t, sim.Meta().AlterResourceGroups(sim.Ctx(), map[string]*rgpb.ResourceGroupConfig{
		"rg_b": meta.NewResourceGroupConfig(5, 5),
	}))
	require.True(t, sim.WaitUntil(10*time.Second, func() bool { return len(sim.RGNodes("rg_b")) == 5 }))
	at := rec.Mark("rg_a loses a node, rg_b gains one")
	victim := sim.RGNodes("rg_a")[0]
	start := time.Now()
	sim.KillNode(victim)
	var rgB time.Duration = -1
	if sim.WaitUntil(2*time.Minute, func() bool { return sim.RGRowCV("rg_b") <= 0.15 }) {
		rgB = time.Since(start)
	}
	m := recoveryMetrics(t, sim, rec, at, before)
	m["rg_b_balance_seconds"] = seconds(rgB)
	sim.Report(Result{Scenario: "7.1_resource_group_isolation", Metrics: m})
	require.GreaterOrEqual(t, rgB, time.Duration(0), "rg_b never balanced")
}
