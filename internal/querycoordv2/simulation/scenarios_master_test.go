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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// These scenarios probe balancer quality rather than only correctness: how
// much data moves compared with the minimum, whether objects move more than
// once or back and forth, how delegators follow WAL placement, and how the
// balancer behaves when loads are slow.

const (
	snBase = int64(1000) // streaming node ids
	qnBase = int64(1)    // regular QueryNode ids
)

type topology struct {
	streaming   bool
	sns, qns    int
	collections int
	channels    int
	segments    int // per channel
	rows        int64
	replicas    int32
	qnConfig    NodeConfig
	snConfig    NodeConfig
}

func defaultTopology(streaming bool) topology {
	return topology{
		streaming: streaming, sns: 4, qns: 8, collections: scaled(30), channels: 2, segments: 8,
		rows: 20_000, replicas: 2, qnConfig: DefaultNodeConfig(), snConfig: DefaultNodeConfig(),
	}
}

func buildCluster(t *testing.T, topo topology) (*Sim, *Recorder) {
	opts := Options{}
	if topo.streaming {
		opts.Streaming = &StreamingOptions{PChannels: 16, WALReassignDelay: 200 * time.Millisecond}
	}
	sim := New(t, opts)
	if topo.streaming {
		sim.AddStreamingNodes(snBase, topo.sns, topo.snConfig, true)
	}
	sim.AddNodes(qnBase, topo.qns, topo.qnConfig)
	ids := collectionIDs(1, topo.collections)
	loadAll(t, sim, ids, topo.replicas, topo.channels, topo.segments, topo.rows, 5*time.Minute)
	_, ok := waitSettled(sim, 2*time.Minute, time.Second)
	require.True(t, ok, "initial load did not settle")
	return sim, sim.StartRecorder(50 * time.Millisecond)
}

// waitOffWAL waits until every delegator sits on its WAL node.
func waitOffWAL(sim *Sim, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	ok := sim.WaitUntil(timeout, func() bool {
		s := sim.Observe()
		return settled(s) && s.DelegatorsOffWAL == 0
	})
	return time.Since(start), ok
}

// segmentsOnStreamingNodes counts sealed segments hosted by streaming nodes.
func segmentsOnStreamingNodes(sim *Sim) int {
	n := 0
	for id, v := range sim.Fleet.Snapshot() {
		if id >= snBase && v.Up {
			n += len(v.Segments)
		}
	}
	return n
}

// channelSpread returns the delegator count per live streaming node.
func channelSpread(sim *Sim) map[int64]int {
	out := map[int64]int{}
	for id, v := range sim.Fleet.Snapshot() {
		if id >= snBase && v.Up {
			out[id] = len(v.Delegators)
		}
	}
	return out
}

func spreadCV(counts map[int64]int) float64 {
	values := make([]float64, 0, len(counts))
	for _, c := range counts {
		values = append(values, float64(c))
	}
	if len(values) < 2 {
		return 0
	}
	return cv(values)
}

// overshoot is the peak rows on `nodes` during the samples divided by the
// final per-node mean of all live non-streaming nodes.
func overshoot(rec *Recorder, from time.Duration, nodes []int64) float64 {
	samples := rec.Since(from)
	if len(samples) == 0 {
		return -1
	}
	var peak int64
	for _, s := range samples {
		for _, n := range nodes {
			peak = max(peak, s.NodeRows[n])
		}
	}
	last := samples[len(samples)-1]
	var total int64
	var count int
	for n, r := range last.NodeRows {
		if n < snBase {
			total += r
			count++
		}
	}
	if count == 0 || total == 0 {
		return -1
	}
	return float64(peak) / (float64(total) / float64(count))
}

func reportPhase(sim *Sim, name string, m map[string]float64, phase *Phase, rec *Recorder, from time.Duration) {
	merge(m, "", tableSpread(sim))
	merge(m, "", phase.Metrics())
	merge(m, "", Summary(rec.Since(from)))
	for _, h := range phase.ChannelHistories(8) {
		sim.t.Logf("%s churn %s", name, h)
	}
	for _, h := range phase.SegmentHistories(12) {
		sim.t.Logf("%s churn %s", name, h)
	}
	sim.Report(Result{Scenario: name, Metrics: m})
}

// S1 Streaming initial load: delegators should land on WAL nodes and sealed
// segments on regular QueryNodes, without redundant channel watches.
func TestSimMaster_S1_StreamingLoad(t *testing.T) {
	requireSim(t)
	topo := defaultTopology(true)
	opts := Options{Streaming: &StreamingOptions{PChannels: 16}}
	sim := New(t, opts)
	sim.AddStreamingNodes(snBase, topo.sns, topo.snConfig, true)
	sim.AddNodes(qnBase, topo.qns, topo.qnConfig)
	rec := sim.StartRecorder(50 * time.Millisecond)
	phase := sim.StartPhase()
	ids := collectionIDs(1, topo.collections)
	loadTime := loadAll(t, sim, ids, topo.replicas, topo.channels, topo.segments, topo.rows, 5*time.Minute)
	settle, ok := waitSettled(sim, 2*time.Minute, time.Second)
	require.True(t, ok)
	spread := channelSpread(sim)
	m := map[string]float64{
		"load_seconds":              loadTime.Seconds(),
		"settle_seconds":            seconds(settle),
		"segments_on_sn":            float64(segmentsOnStreamingNodes(sim)),
		"delegator_spread_cv":       spreadCV(spread),
		"required_channel_replicas": float64(topo.collections * topo.channels * int(topo.replicas)),
	}
	reportPhase(sim, "S1_streaming_load", m, phase, rec, 0)
	t.Logf("delegators per streaming node: %v", spread)
}

// S2 StreamingCoord moves WALs: delegators should follow, once, without
// leaving channels leaderless.
func TestSimMaster_S2_WALMigration(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(true))
	at := rec.Mark("move WALs")
	phase := sim.StartPhase()
	assign := sim.WAL().Assignment()
	pchannels := make([]string, 0, len(assign))
	for p := range assign {
		pchannels = append(pchannels, p)
	}
	sort.Strings(pchannels)
	moved := 0
	for _, p := range pchannels {
		if moved >= 4 {
			break
		}
		from := assign[p]
		to := snBase + (from-snBase+1)%4
		sim.WAL().Move(p, to)
		moved++
	}
	followed, ok := waitOffWAL(sim, time.Minute)
	m := map[string]float64{"wal_moves": float64(moved), "follow_seconds": seconds(followed), "followed": boolMetric(ok)}
	reportPhase(sim, "S2_wal_migration", m, phase, rec, at)
}

// S3 A streaming node crashes: its delegators must be rebuilt on the WALs'
// new homes.
func TestSimMaster_S3_StreamingNodeCrash(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(true))
	at := rec.Mark("kill streaming node")
	phase := sim.StartPhase()
	sim.KillStreamingNode(snBase + 1)
	var recovered time.Duration = -1
	start := time.Now()
	if sim.WaitUntil(2*time.Minute, func() bool { s := sim.Observe(); return s.LeaderlessChannels == 0 && s.MissingSegments == 0 }) {
		recovered = time.Since(start)
	}
	settle, _ := waitSettled(sim, 2*time.Minute, time.Second)
	_, onWAL := waitOffWAL(sim, 30*time.Second)
	m := map[string]float64{"recovery_seconds": seconds(recovered), "settle_seconds": seconds(settle), "delegators_back_on_wal": boolMetric(onWAL)}
	reportPhase(sim, "S3_streaming_node_crash", m, phase, rec, at)
	require.GreaterOrEqual(t, recovered, time.Duration(0), "channels never recovered")
}

// S4 A streaming node joins and StreamingCoord spreads WALs onto it.
func TestSimMaster_S4_StreamingScaleOut(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(true))
	at := rec.Mark("add streaming node")
	phase := sim.StartPhase()
	sim.AddStreamingNodes(snBase+10, 1, DefaultNodeConfig(), true)
	// Let the replica observer add the new streaming node to replicas.
	var inReplicas time.Duration = -1
	start := time.Now()
	if sim.WaitUntil(30*time.Second, func() bool {
		for _, id := range sim.Meta().GetAll(sim.Ctx()) {
			for _, r := range sim.Meta().GetByCollection(sim.Ctx(), id) {
				for _, n := range r.GetRWSQNodes() {
					if n == snBase+10 {
						return true
					}
				}
			}
		}
		return false
	}) {
		inReplicas = time.Since(start)
	}
	time.Sleep(10 * time.Second)
	followed, ok := waitOffWAL(sim, time.Minute)
	spread := channelSpread(sim)
	walOnNew := 0
	for _, node := range sim.WAL().Assignment() {
		if node == snBase+10 {
			walOnNew++
		}
	}
	t.Logf("new streaming node joined replicas after %s; WALs on it: %d", inReplicas, walOnNew)
	m := map[string]float64{
		"follow_seconds": seconds(followed), "followed": boolMetric(ok), "delegator_spread_cv": spreadCV(spread),
		"joined_replicas_seconds": seconds(inReplicas), "wals_on_new_node": float64(walOnNew), "delegators_on_new_node": float64(spread[snBase+10]),
	}
	reportPhase(sim, "S4_streaming_scale_out", m, phase, rec, at)
	t.Logf("delegators per streaming node: %v", spread)
}

func slowConfig() NodeConfig {
	cfg := DefaultNodeConfig()
	cfg.LoadBaseLatency = 300 * time.Millisecond
	cfg.LoadConcurrency = 2
	return cfg
}

// scaleOutSlow adds two empty QueryNodes to a cluster whose loads are slow,
// and measures how far data movement exceeds the minimum.
func scaleOutSlow(t *testing.T, name string, streaming bool) {
	scaleOutWith(t, name, streaming, slowConfig())
}

func scaleOutWith(t *testing.T, name string, streaming bool, cfg NodeConfig) {
	topo := defaultTopology(streaming)
	topo.qnConfig = cfg
	topo.replicas = 1
	sim, rec := buildCluster(t, topo)
	newNodes := []int64{qnBase + int64(topo.qns), qnBase + int64(topo.qns) + 1}
	sim.AddNodes(newNodes[0], len(newNodes), cfg)
	require.True(t, sim.WaitUntil(10*time.Second, func() bool {
		in := 0
		for _, id := range sim.Meta().GetAll(sim.Ctx()) {
			for _, r := range sim.Meta().GetByCollection(sim.Ctx(), id) {
				for _, n := range newNodes {
					if r.Contains(n) {
						in++
					}
				}
			}
		}
		return in > 0
	}))
	at := rec.Mark("scale out")
	phase := sim.StartPhase()
	balanced, ok := waitBalanced(sim, 0.15, 5*time.Minute)
	// Keep counting moves until the balancer goes quiet: moves after the CV
	// threshold are part of the cost.
	quiet, quietOK := waitSettled(sim, 3*time.Minute, 3*time.Second)
	m := map[string]float64{
		"balanced": boolMetric(ok), "balance_seconds": seconds(balanced),
		"quiet_seconds": seconds(quiet), "quiet": boolMetric(quietOK),
		"overshoot_new_nodes": overshoot(rec, at, newNodes),
	}
	reportPhase(sim, name, m, phase, rec, at)
	require.True(t, ok, "cluster did not balance")
}

// S5/S6 Scale-out when loads are slow (streaming and classic topologies).
func TestSimMaster_S5_SlowLoadScaleOutStreaming(t *testing.T) {
	requireSim(t)
	scaleOutSlow(t, "S5_slow_scale_out_streaming", true)
}

func TestSimMaster_S6_SlowLoadScaleOutClassic(t *testing.T) {
	requireSim(t)
	scaleOutSlow(t, "S6_slow_scale_out_classic", false)
}

// S6b Same scale-out with near-instant loads: separates the balancer's own
// move pattern from effects of slow loads.
func TestSimMaster_S6b_FastLoadScaleOutClassic(t *testing.T) {
	requireSim(t)
	cfg := DefaultNodeConfig()
	cfg.LoadBaseLatency = 5 * time.Millisecond
	cfg.LoadBandwidthBytes = 100 << 30
	cfg.LoadConcurrency = 8
	scaleOutWith(t, "S6b_fast_scale_out_classic", false, cfg)
}

// S7 One straggler QueryNode loads 20x slower. Recovery after another node
// crashes should not wait on the straggler.
func TestSimMaster_S7_StragglerRecovery(t *testing.T) {
	requireSim(t)
	for _, straggler := range []bool{false, true} {
		t.Run(fmt.Sprintf("straggler=%v", straggler), func(t *testing.T) {
			topo := defaultTopology(false)
			sim, rec := buildCluster(t, topo)
			if straggler {
				sim.Fleet.SetSlowdown(qnBase+7, 20)
			}
			before := sim.Fleet.Stats.LoadsByNode()
			at := rec.Mark("kill node")
			phase := sim.StartPhase()
			sim.KillNode(qnBase + 2)
			start := time.Now()
			var recovered time.Duration = -1
			if sim.WaitUntil(3*time.Minute, func() bool { s := sim.Observe(); return s.MissingSegments == 0 && s.LeaderlessChannels == 0 }) {
				recovered = time.Since(start)
			}
			after := sim.Fleet.Stats.LoadsByNode()
			m := map[string]float64{
				"recovery_seconds":      seconds(recovered),
				"loads_to_straggler":    float64(after[qnBase+7] - before[qnBase+7]),
				"loads_to_other_nodes":  float64(sum(after) - sum(before) - (after[qnBase+7] - before[qnBase+7])),
				"straggler_slowdown_on": boolMetric(straggler),
			}
			reportPhase(sim, fmt.Sprintf("S7_straggler_recovery_%v", straggler), m, phase, rec, at)
		})
	}
}

func sum(m map[int64]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

// S8 Highly skewed segment sizes: a few huge segments among many small ones.
// Balance should converge without oscillating huge segments.
func TestSimMaster_S8_SkewedSegments(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	sim.AddNodes(qnBase, 6, DefaultNodeConfig())
	ids := collectionIDs(1, scaled(10))
	for _, id := range ids {
		sim.Broker.CreateCollection(id, 2, 0, 0)
		for i := 0; i < 2; i++ {
			sim.Broker.Flush(id, 1_000_000) // huge
		}
		for i := 0; i < 20; i++ {
			sim.Broker.Flush(id, 5_000)
		}
	}
	for _, id := range ids {
		sim.loadExisting(id, 1)
	}
	require.True(t, sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) }))
	_, ok := waitSettled(sim, 2*time.Minute, time.Second)
	require.True(t, ok)
	rec := sim.StartRecorder(50 * time.Millisecond)
	sim.AddNodes(qnBase+6, 2, DefaultNodeConfig())
	time.Sleep(500 * time.Millisecond)
	at := rec.Mark("scale out")
	phase := sim.StartPhase()
	// Huge segments make exact balance impossible; measure what happens over
	// a fixed window instead of waiting for a CV threshold.
	time.Sleep(30 * time.Second)
	_, settledOK := waitSettled(sim, time.Minute, time.Second)
	m := map[string]float64{"settled": boolMetric(settledOK)}
	reportPhase(sim, "S8_skewed_segments", m, phase, rec, at)
}

// S9 Streaming topology with continuous ingestion during a QueryNode
// scale-out: handoff and balance interleave.
func TestSimMaster_S9_IngestionScaleOutStreaming(t *testing.T) {
	requireSim(t)
	topo := defaultTopology(true)
	topo.replicas = 1
	sim, rec := buildCluster(t, topo)
	ids := collectionIDs(1, topo.collections)
	// ~30 segments/s of 10MB: ~35% of the fleet's load bandwidth.
	fl := startFlusher(sim, ids, time.Second, 20_000, 0)
	sim.AddNodes(qnBase+int64(topo.qns), 2, DefaultNodeConfig())
	time.Sleep(500 * time.Millisecond)
	at := rec.Mark("scale out under ingestion")
	phase := sim.StartPhase()
	balanced, ok := waitRowBalanced(sim, 0.15, 3*time.Minute)
	fl.Stop()
	m := map[string]float64{"balanced": boolMetric(ok), "balance_seconds": seconds(balanced)}
	reportPhase(sim, "S9_ingestion_scale_out_streaming", m, phase, rec, at)
}

// S10 QueryCoord restarts while slow balance Moves are in flight (classic).
// A restarted coordinator does not know the in-flight Moves.
func TestSimMaster_S10_RestartDuringSlowMoves(t *testing.T) {
	requireSim(t)
	topo := defaultTopology(false)
	topo.replicas = 1
	topo.qnConfig = slowConfig()
	sim, rec := buildCluster(t, topo)
	newNodes := []int64{qnBase + int64(topo.qns), qnBase + int64(topo.qns) + 1}
	sim.AddNodes(newNodes[0], 2, slowConfig())
	require.True(t, sim.WaitUntil(30*time.Second, func() bool { s, _ := sim.PendingTasks(); return s > 0 }))
	time.Sleep(200 * time.Millisecond)
	at := rec.Mark("restart querycoord")
	phase := sim.StartPhase()
	sim.RestartQueryCoord()
	balanced, ok := waitBalanced(sim, 0.15, 5*time.Minute)
	m := map[string]float64{"balanced": boolMetric(ok), "balance_seconds": seconds(balanced), "overshoot_new_nodes": overshoot(rec, at, newNodes)}
	reportPhase(sim, "S10_restart_during_slow_moves", m, phase, rec, at)
}

// delegatorsByPChannel helps diagnose WAL-follow behavior.
func delegatorsByPChannel(sim *Sim) map[string][]int64 {
	out := map[string][]int64{}
	for node, v := range sim.Fleet.Snapshot() {
		for ch := range v.Delegators {
			p := funcutil.ToPhysicalChannel(ch)
			out[p] = append(out[p], node)
		}
	}
	return out
}
