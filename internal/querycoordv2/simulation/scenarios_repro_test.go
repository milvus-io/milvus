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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Reproductions of production QueryCoord placement problems (the "R"
// scenarios). None of them pins a balancer except R1, which reproduces the
// old #52092 tolerance bug, so MILVUS_SIM_PARAMS can rerun the whole set
// against another balancer. Every scenario reports numeric metrics.
//
// Time is compressed about 10x: 1s in the simulator is ~10s in production
// (SegmentTaskTimeout 6s here = 60s there).

// ---------------------------------------------------------------------------
// helpers

// softSettle waits for waitSettled but only reports the outcome: a balancer
// under test that never goes quiet should still produce a report.
func softSettle(sim *Sim, timeout, stable time.Duration) (time.Duration, bool) {
	return waitSettled(sim, timeout, stable)
}

// waitRecovered waits until no segment is missing and every channel has a
// leader, returning the elapsed time or -1.
func waitRecovered(sim *Sim, timeout time.Duration) time.Duration {
	start := time.Now()
	if sim.WaitUntil(timeout, func() bool {
		s := sim.Observe()
		return s.MissingSegments == 0 && s.LeaderlessChannels == 0
	}) {
		return time.Since(start)
	}
	return -1
}

// inflightMetrics reports the peak loads held at once (queued + loading) by
// the given nodes since the last ResetPeaks.
func inflightMetrics(sim *Sim, m map[string]float64, prefix string, nodes []int64) {
	peaks := sim.Fleet.Stats.PeakInflightByNode()
	var mx, sum float64
	for _, n := range nodes {
		v := float64(peaks[n])
		mx = max(mx, v)
		sum += v
	}
	m[prefix+"max_inflight_per_node"] = mx
	if len(nodes) > 0 {
		m[prefix+"mean_peak_inflight_per_node"] = sum / float64(len(nodes))
	}
}

// liveUsedBytes is the memory held on live QueryNodes.
func liveUsedBytes(sim *Sim) int64 {
	var total int64
	for _, v := range sim.Fleet.Snapshot() {
		if v.Up {
			total += v.UsedMemory
		}
	}
	return total
}

func nodeRange(first int64, n int) []int64 { return collectionIDs(first, n) }

// movesPerRealHour converts moves in a simulated window into the production
// rate (the simulator runs ~10x faster).
func movesPerRealHour(moves int, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return float64(moves) / window.Seconds() * 3600 / 10
}

// ingester keeps adding growing rows to collections and seals them every
// flushEvery ticks (0: never flush).
type ingester struct {
	stop chan struct{}
	done chan struct{}
}

func startIngest(sim *Sim, ids []int64, rowsPerTick int64, flushEvery int) *ingester {
	in := &ingester{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(in.done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for i := 1; ; i++ {
			select {
			case <-in.stop:
				return
			case <-tick.C:
			}
			for _, id := range ids {
				sim.Broker.Ingest(id, rowsPerTick)
				if flushEvery > 0 && i%flushEvery == 0 {
					sim.Broker.FlushGrowing(id)
				}
			}
		}
	}()
	return in
}

func (in *ingester) Stop() {
	close(in.stop)
	<-in.done
}

// releaseCollections follows ReleaseCollectionJob: drop the collection
// from meta and target, wait until QueryNodes hold none of it, drop replicas.
func releaseCollections(sim *Sim, ids []int64, timeout time.Duration) bool {
	qc := sim.current()
	for _, id := range ids {
		sim.must(qc.meta.CollectionManager.RemoveCollection(qc.ctx, id))
		qc.targetObs.ReleaseCollection(id)
	}
	ok := sim.WaitUntil(timeout, func() bool {
		qc := sim.current()
		for _, id := range ids {
			if len(qc.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(id)))+
				len(qc.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(id))) > 0 {
				qc.checkers.Check()
				return false
			}
		}
		return true
	})
	for _, id := range ids {
		sim.must(sim.current().meta.ReplicaManager.RemoveCollection(qc.ctx, id))
	}
	return ok
}

// updateReplicaNumber runs the real UpdateLoadConfigJob (what a load
// request with a new replica number does for a loaded collection).
func updateReplicaNumber(sim *Sim, id int64, replicas int32) error {
	qc := sim.current()
	req := &querypb.UpdateLoadConfigRequest{
		Base:           &commonpb.MsgBase{},
		CollectionIDs:  []int64{id},
		ReplicaNumber:  replicas,
		ResourceGroups: []string{meta.DefaultResourceGroupName},
	}
	j := job.NewUpdateLoadConfigJob(qc.ctx, req, qc.meta, qc.targetMgr, qc.targetObs, qc.collObs, noopProxyManager{}, true, false)
	return j.Execute()
}

// walMoves is the number of WAL (re)assignments the simulated
// StreamingCoord has made, including failover of orphaned WALs: the sum of
// pchannel terms.
func walMoves(sim *Sim) int {
	sim.wal.mu.Lock()
	defer sim.wal.mu.Unlock()
	n := 0
	for _, term := range sim.wal.term {
		n += int(term)
	}
	return n
}

// timeBalancer calls the configured balancer's BalanceReplica directly on
// up to sample collections (always including `always`) and reports its cost.
// Plans are discarded. Call it last: a stateful balancer may remember the
// call.
func timeBalancer(sim *Sim, ids []int64, always []int64, sample int) map[string]float64 {
	qc := sim.current()
	b := balance.GetGlobalBalancerFactory().GetBalancer()
	pick := append([]int64(nil), always...)
	step := max(1, len(ids)/max(1, sample))
	for i := 0; i < len(ids); i += step {
		pick = append(pick, ids[i])
	}
	var total, worst time.Duration
	calls := 0
	for _, id := range pick {
		for _, r := range qc.meta.GetByCollection(qc.ctx, id) {
			t0 := time.Now()
			b.BalanceReplica(qc.ctx, r)
			d := time.Since(t0)
			total += d
			worst = max(worst, d)
			calls++
		}
	}
	m := map[string]float64{}
	if calls == 0 {
		return m
	}
	meanMs := total.Seconds() * 1000 / float64(calls)
	replicas := 0
	for _, id := range qc.meta.GetAll(qc.ctx) {
		replicas += len(qc.meta.GetByCollection(qc.ctx, id))
	}
	maxCheck := paramtable.Get().QueryCoordCfg.BalanceCheckCollectionMaxCount.GetAsInt()
	m["balance_replica_mean_ms"] = meanMs
	m["balance_replica_max_ms"] = worst.Seconds() * 1000
	m["balance_replica_calls"] = float64(calls)
	m["est_balance_round_ms"] = meanMs * float64(min(maxCheck, replicas))
	m["est_full_sweep_seconds"] = meanMs * float64(replicas) / 1000
	return m
}

// timeCheckers stops the checker loops and then runs every checker's Check
// by hand `rounds` times, reporting mean wall time per round. It ends the
// scenario's QueryCoord activity, so call it last. Balance tasks the balance
// checker submits are left to the scheduler.
func timeCheckers(sim *Sim, rounds int) map[string]float64 {
	qc := sim.current()
	qc.checkers.Stop()
	m := map[string]float64{}
	for _, c := range qc.checkers.Checkers() {
		name := strings.ToLower(strings.ReplaceAll(c.ID().String(), " ", "_"))
		var total time.Duration
		for i := 0; i < rounds; i++ {
			// Let the auto-balance interval pass so the balance checker runs.
			time.Sleep(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.GetAsDuration(time.Millisecond) + 20*time.Millisecond)
			t0 := time.Now()
			c.Check(qc.ctx)
			total += time.Since(t0)
		}
		m["checker_ms_"+name] = total.Seconds() * 1000 / float64(rounds)
	}
	return m
}

// loadEventsOnto counts load events at or after since on the given nodes.
func loadEventsOnto(sim *Sim, since time.Time, nodes map[int64]bool) (loads int, rows int64) {
	for _, e := range sim.Fleet.Stats.Events(since) {
		if e.Kind == EventLoad && nodes[e.Node] {
			loads++
			rows += e.Rows
		}
	}
	return
}

func releaseEventsOn(sim *Sim, since time.Time, node int64) (releases int, rows int64) {
	for _, e := range sim.Fleet.Stats.Events(since) {
		if e.Kind == EventRelease && e.Node == node {
			releases++
			rows += e.Rows
		}
	}
	return
}

func setOf(ids ...int64) map[int64]bool {
	out := make(map[int64]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// nodeUtil is used/capacity of one node.
func nodeUtil(sim *Sim, id int64) float64 {
	v, ok := sim.Fleet.Snapshot()[id]
	capacity := sim.Fleet.MemoryCapacity(id)
	if !ok || capacity == 0 {
		return 0
	}
	return float64(v.UsedMemory) / float64(capacity)
}

// overloadMetrics reports memory skew and how many live nodes exceed the
// QueryNode overload threshold (90%).
func overloadMetrics(sim *Sim, m map[string]float64) {
	merge(m, "", memSpread(sim))
	var mx, mn float64 = 0, 2
	over := 0
	for id, v := range sim.Fleet.Snapshot() {
		if !v.Up || sim.Fleet.IsStopping(id) || id >= snBase {
			continue
		}
		u := float64(v.UsedMemory) / float64(sim.Fleet.MemoryCapacity(id))
		mx = max(mx, u)
		mn = min(mn, u)
		if u > memLimit {
			over++
		}
	}
	m["mem_util_max"] = mx
	m["mem_util_min"] = mn
	if mn > 0 {
		m["mem_max_over_min"] = mx / mn
	}
	m["nodes_over_90pct"] = float64(over)
	c := sim.FleetCounters()
	m["oom_rejections"] = c["loads_rejected_oom"]
	m["loads_failed"] = c["loads_failed"]
}

// ---------------------------------------------------------------------------
// R1 (#52092): with scoreUnbalanceTolerationFactor=0 (the pre-#52093 2.6
// hotfix build) any score difference triggers a move, so a stable cluster of
// many collections keeps moving segments. This is the only scenario that
// pins a parameter; the control variant uses the 0.05 default. See also the
// production replays testdata/cases/legacy_1_* (bug) vs prod_1_* (fixed).

func TestSimRepro_R1_ZeroToleranceChurn(t *testing.T) {
	requireSim(t)
	key := paramtable.Get().QueryCoordCfg.ScoreUnbalanceTolerationFactor.Key
	for _, tc := range []struct {
		name   string
		tol    string
		ingest bool
	}{
		{"tol0_idle", "0", false},
		{"tol0_ingest", "0", true},
		{"tol0.05_ingest", "0.05", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { paramtable.Get().Reset(key) })
			sim := New(t, Options{Params: map[string]string{key: tc.tol}})
			sim.Broker.RowsSigma = 1.0
			sim.AddNodes(qnBase, 5, CalibratedNodeConfig())
			ids := collectionIDs(1, 40)
			loadAll(t, sim, ids, 1, 1, 20, 20_000, 10*time.Minute)
			sim.Broker.RowsSigma = 0
			settle, settledOK := softSettle(sim, 3*time.Minute, 5*time.Second)

			var in *ingester
			if tc.ingest {
				// 40 channels x 10k rows/s x 512B = ~200MB/s, ~16% of load bandwidth.
				in = startIngest(sim, ids, 1_000, 30)
			}
			rec := sim.StartRecorder(200 * time.Millisecond)
			before := sim.FleetCounters()
			at := rec.Mark("steady state")
			phase := sim.StartPhase()
			const window = 90 * time.Second
			time.Sleep(window)
			if in != nil {
				in.Stop()
			}
			mv := phase.Analyze()
			m := map[string]float64{
				"tolerance":                            mustFloat(tc.tol),
				"initial_settled":                      boolMetric(settledOK),
				"initial_settle_seconds":               seconds(settle),
				"window_seconds":                       window.Seconds(),
				"existing_segment_moves_per_real_hour": movesPerRealHour(mv.SegmentLoads, window),
				"channel_moves_per_real_hour":          movesPerRealHour(mv.ChannelWatches, window),
			}
			if mv.NewSegmentLoads > 0 {
				m["moves_per_new_segment"] = float64(mv.SegmentLoads) / float64(mv.NewSegmentLoads)
			}
			merge(m, "delta_", delta(sim.FleetCounters(), before))
			reportPhase(sim, "R1_zero_tolerance_churn_"+tc.name, m, phase, rec, at)
		})
	}
}

func mustFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// ---------------------------------------------------------------------------
// R2 reload storms. Every node's load bandwidth is finite; loads beyond the
// node's concurrency wait in its queue, and that wait counts against the
// task timeout. Reported against the bandwidth lower bound.

// R2a Rollout: every QueryNode is replaced by a fresh empty one within ~2s
// (all pods restarted at once). Everything must be reloaded.
func TestSimRepro_R2a_RolloutAllNodes(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	const nodes = 8
	sim.AddNodes(qnBase, nodes, cfg)
	// 24 collections x 16 segments of 200MB = 76.8GB: 9.6GB per node, ~38s
	// of cluster bandwidth (~6.4 production minutes).
	ids := collectionIDs(1, 24)
	c0 := sim.FleetCounters()
	loadTime := loadAll(t, sim, ids, 1, 2, 8, 400_000, 15*time.Minute)
	loadDelta := delta(sim.FleetCounters(), c0)
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)
	total := liveUsedBytes(sim)
	segments := 0
	for _, v := range sim.Fleet.Snapshot() {
		segments += len(v.Segments)
	}

	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("rollout")
	phase := sim.StartPhase()
	start := time.Now()
	var fresh []int64
	for i := 0; i < nodes; i++ {
		sim.KillNode(qnBase + int64(i))
		fresh = append(fresh, sim.AddNodes(100+int64(i), 1, cfg)...)
		time.Sleep(250 * time.Millisecond)
	}
	var recovered time.Duration = -1
	if sim.WaitUntil(15*time.Minute, func() bool {
		s := sim.Observe()
		return s.MissingSegments == 0 && s.LeaderlessChannels == 0
	}) {
		recovered = time.Since(start)
	}
	lowerBound := float64(total) / float64(int64(nodes)*cfg.LoadBandwidthBytes)
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"initial_settled":           boolMetric(settledOK),
		"initial_load_seconds":      loadTime.Seconds(),
		"initial_load_lower_bound":  lowerBound,
		"initial_loads_canceled":    loadDelta["loads_canceled"],
		"recovery_seconds":          seconds(recovered),
		"lower_bound_seconds":       lowerBound,
		"total_gb":                  float64(total) / (1 << 30),
		"segments":                  float64(segments),
		"loads_canceled":            d["loads_canceled"],
		"loads_failed":              d["loads_failed"],
		"loads_started_per_segment": d["loads_succeeded"] / max(1, float64(segments)),
		"max_queue_wait_seconds":    d["max_queue_wait_seconds"],
		"mean_queue_wait_seconds":   d["mean_queue_wait_seconds"],
		"max_node_queued":           d["max_node_queued"],
	}
	if recovered > 0 {
		m["recovery_over_lower_bound"] = recovered.Seconds() / lowerBound
	}
	inflightMetrics(sim, m, "", fresh)
	reportPhase(sim, "R2a_rollout_all_nodes", m, phase, rec, at)
}

// R2b A user releases three big collections and loads them again at once.
func TestSimRepro_R2b_ReleaseReloadBig(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	const nodes = 6
	sim.AddNodes(qnBase, nodes, cfg)
	big := []int64{1, 2, 3}
	for _, id := range big {
		sim.Broker.CreateCollection(id, 2, 40, 400_000) // 80 x 200MB = 16GB
		sim.loadExisting(id, 1)
	}
	small := collectionIDs(10, 12)
	loadAll(t, sim, small, 1, 2, 8, 20_000, 15*time.Minute)
	require.True(t, sim.WaitUntil(15*time.Minute, func() bool { return sim.Loaded(big...) }), "big collections did not load")
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)

	var bigBytes int64
	for _, id := range big {
		bigBytes += int64(len(sim.Broker.SegmentRows(id))) * 400_000 * sim.Broker.BytesPerRow
	}
	t0 := time.Now()
	released := releaseCollections(sim, big, 3*time.Minute)
	releaseTime := time.Since(t0)

	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("reload")
	phase := sim.StartPhase()
	start := time.Now()
	for _, id := range big {
		sim.loadExisting(id, 1)
	}
	loaded := sim.WaitUntil(15*time.Minute, func() bool { return sim.Loaded(big...) })
	reload := time.Since(start)
	lowerBound := float64(bigBytes) / float64(int64(nodes)*cfg.LoadBandwidthBytes)
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"initial_settled":         boolMetric(settledOK),
		"released":                boolMetric(released),
		"release_seconds":         releaseTime.Seconds(),
		"reloaded":                boolMetric(loaded),
		"reload_seconds":          reload.Seconds(),
		"lower_bound_seconds":     lowerBound,
		"reload_over_lower_bound": reload.Seconds() / lowerBound,
		"reload_gb":               float64(bigBytes) / (1 << 30),
		"loads_canceled":          d["loads_canceled"],
		"loads_failed":            d["loads_failed"],
		"loads_succeeded":         d["loads_succeeded"],
		"max_queue_wait_seconds":  d["max_queue_wait_seconds"],
		"mean_queue_wait_seconds": d["mean_queue_wait_seconds"],
		"max_node_queued":         d["max_node_queued"],
	}
	inflightMetrics(sim, m, "", nodeRange(qnBase, nodes))
	reportPhase(sim, "R2b_release_reload_3_big", m, phase, rec, at)
}

// R2c A temporary replica: raise a big collection from 1 to 2 replicas,
// wait until settled, then lower it back to 1. Raising splits the nodes
// between replicas, so the old replica also has to move half its data;
// lowering drops copies that the remaining replica then reloads elsewhere.
func TestSimRepro_R2c_TemporaryReplica(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	const nodes = 8
	sim.AddNodes(qnBase, nodes, cfg)
	sim.Broker.CreateCollection(1, 2, 40, 400_000) // 80 x 200MB = 16GB
	sim.loadExisting(1, 1)
	small := collectionIDs(10, 10)
	loadAll(t, sim, small, 1, 2, 8, 20_000, 15*time.Minute)
	require.True(t, sim.WaitUntil(15*time.Minute, func() bool { return sim.Loaded(1) }))
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)
	segs := float64(len(sim.Broker.SegmentRows(1)))
	segBytes := 400_000 * sim.Broker.BytesPerRow
	m := map[string]float64{"initial_settled": boolMetric(settledOK), "collection_segments": segs}
	rec := sim.StartRecorder(100 * time.Millisecond)

	step := func(prefix string, replicas int32) {
		sim.Fleet.Stats.ResetPeaks()
		before := sim.FleetCounters()
		at := rec.Mark(prefix)
		start := time.Now()
		require.NoError(t, updateReplicaNumber(sim, 1, replicas))
		phase := sim.StartPhase()
		// Copies on nodes that left their replica must move.
		qc := sim.current()
		views := sim.Fleet.Snapshot()
		mustMove := 0
		for _, r := range qc.meta.GetByCollection(qc.ctx, 1) {
			for _, node := range r.GetRONodes() {
				for _, coll := range views[node].Collection {
					if coll == 1 {
						mustMove++
					}
				}
			}
		}
		served := waitRecovered(sim, 10*time.Minute)
		const stable = 3 * time.Second
		_, ok := softSettle(sim, 10*time.Minute, stable)
		settle := time.Since(start) - stable
		d := delta(sim.FleetCounters(), before)
		pm := phase.Metrics()
		m[prefix+"served_seconds"] = seconds(served)
		m[prefix+"settle_seconds"] = settle.Seconds()
		if !ok {
			m[prefix+"settle_seconds"] = -1
		}
		m[prefix+"settled"] = boolMetric(ok)
		m[prefix+"ro_copies_to_move"] = float64(mustMove)
		m[prefix+"segment_loads"] = pm["segment_loads"] + pm["new_segment_loads"]
		m[prefix+"releases"] = d["releases"]
		m[prefix+"pingpong_segments"] = pm["pingpong_segments"]
		m[prefix+"loads_canceled"] = d["loads_canceled"]
		m[prefix+"loads_failed"] = d["loads_failed"]
		m[prefix+"max_queue_wait_seconds"] = d["max_queue_wait_seconds"]
		inflightMetrics(sim, m, prefix, nodeRange(qnBase, nodes))
		m[prefix+"missing_segment_samples"] = Summary(rec.Since(at))["missing_segment_samples"]
		m[prefix+"replicas"] = float64(len(sim.current().meta.GetByCollection(sim.Ctx(), 1)))
	}
	step("add_", 2)
	// Lower bound for the raise: the new replica's 16GB over its 4 nodes.
	m["add_lower_bound_seconds"] = segs * float64(segBytes) / float64(4*cfg.LoadBandwidthBytes)
	m["add_min_loads"] = segs + m["add_ro_copies_to_move"]
	step("remove_", 1)
	// Ideal lowering reuses the dropped replica's copies (0 loads); a naive
	// spread of one replica over 8 nodes moves half of it.
	m["remove_naive_min_loads"] = segs / 2
	sim.Report(Result{Scenario: "R2c_temporary_replica", Metrics: m})
}

// ---------------------------------------------------------------------------
// R5 Bytes vs rows.

func fastLoadMemConfig(memory int64) NodeConfig {
	cfg := CalibratedNodeConfig()
	cfg.MemoryBytes = memory
	cfg.LoadJitterSigma = 0
	// Placement, not load speed, is under test: big segments must not time out.
	cfg.LoadBandwidthBytes = 2 << 30
	return cfg
}

// R5a Like production 8600fd5f: one collection on 8 nodes whose segments all
// have the same row count but memory differing up to 3x (512..1536 B/row).
// QueryCoord balances rows, so memory is left to chance.
func TestSimRepro_R5a_EqualRowsUnequalBytes(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	const nodes = 8
	sim.AddNodes(qnBase, nodes, fastLoadMemConfig(6<<30))
	// 40 x 1M rows, ~1GB average (~37GiB) on 48GiB: ~77% mean utilization.
	sim.Broker.CreateCollectionWidths(1, 1, 40, 1_000_000, 512, 1536)
	sim.loadExisting(1, 1)
	loaded := sim.WaitUntil(10*time.Minute, func() bool { return sim.Loaded(1) })
	_, ok := softSettle(sim, 5*time.Minute, 5*time.Second)
	m := map[string]float64{"loaded": boolMetric(loaded), "settled": boolMetric(ok)}
	overloadMetrics(sim, m)
	sim.Report(Result{Scenario: "R5a_equal_rows_unequal_bytes", Metrics: m})
}

// R5b Mixed tables: one wide table (16KB/row) beside ten narrow ones
// (512B/row) on 8 nodes.
func TestSimRepro_R5b_WideBesideNarrow(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	const nodes = 8
	sim.AddNodes(qnBase, nodes, fastLoadMemConfig(5<<30))
	// Wide: 12 x 655MB = 7.9GB (480k rows). Narrow: 200 x 100MB = 20GB
	// (40M rows). ~28GB on 40GB = ~70% mean utilization.
	sim.Broker.CreateWideCollection(1, 2, 6, 40_000, wideRow)
	sim.loadExisting(1, 1)
	ids := []int64{1}
	for id := int64(2); id <= 11; id++ {
		sim.Broker.CreateWideCollection(id, 2, 10, 200_000, narrowRow)
		sim.loadExisting(id, 1)
		ids = append(ids, id)
	}
	loaded := sim.WaitUntil(10*time.Minute, func() bool { return sim.Loaded(ids...) })
	_, ok := softSettle(sim, 5*time.Minute, 5*time.Second)
	m := map[string]float64{"loaded": boolMetric(loaded), "settled": boolMetric(ok)}
	overloadMetrics(sim, m)
	// Wide segments per node: the table's own skew.
	perNode := map[int64]int{}
	for id, v := range sim.Fleet.Snapshot() {
		for _, coll := range v.Collection {
			if coll == 1 {
				perNode[id]++
			}
		}
	}
	mx, mn := 0, 1<<30
	for id := qnBase; id < qnBase+nodes; id++ {
		mx = max(mx, perNode[id])
		mn = min(mn, perNode[id])
	}
	m["wide_segments_max_per_node"] = float64(mx)
	m["wide_segments_min_per_node"] = float64(mn)
	sim.Report(Result{Scenario: "R5b_wide_beside_narrow", Metrics: m})
}

// ---------------------------------------------------------------------------
// R6 Many tiny collections (strategic goal: up to a million tables). N tiny
// collections with 1-2 segments each beside 2 big collections on 8 nodes.
// MILVUS_SIM_R6_N="2000,10000" chooses the sizes.

func TestSimRepro_R6_TinyCollectionsScale(t *testing.T) {
	requireSim(t)
	sizes := []int{2000, 10000}
	if v := os.Getenv("MILVUS_SIM_R6_N"); v != "" {
		sizes = nil
		for _, f := range strings.Split(v, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && n > 0 {
				sizes = append(sizes, n)
			}
		}
	}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) { runR6(t, n) })
	}
}

// cheapCoverage is Observe's missing/leaderless count without per-replica
// work (every collection here has one replica), cheap enough for 10^4+
// collections.
func cheapCoverage(sim *Sim) (missing, leaderless int) {
	qc := sim.current()
	have := map[int64]bool{}
	watched := map[string]bool{}
	for _, v := range sim.Fleet.Snapshot() {
		if !v.Up {
			continue
		}
		for seg := range v.Segments {
			have[seg] = true
		}
		for ch, d := range v.Delegators {
			if d.Watched {
				watched[ch] = true
			}
		}
	}
	for _, id := range qc.meta.GetAll(qc.ctx) {
		for seg := range qc.targetMgr.GetSealedSegmentsByCollection(qc.ctx, id, meta.CurrentTarget) {
			if !have[seg] {
				missing++
			}
		}
		for ch := range qc.targetMgr.GetDmChannelsByCollection(qc.ctx, id, meta.CurrentTarget) {
			if !watched[ch] {
				leaderless++
			}
		}
	}
	return
}

func waitEvery(timeout, period time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(period)
	}
}

func cheapSettle(sim *Sim, timeout, stable time.Duration) bool {
	var since time.Time
	return waitEvery(timeout, 500*time.Millisecond, func() bool {
		segTasks, chTasks := sim.PendingTasks()
		if segTasks+chTasks > 0 {
			since = time.Time{}
			return false
		}
		if missing, leaderless := cheapCoverage(sim); missing+leaderless > 0 {
			since = time.Time{}
			return false
		}
		if since.IsZero() {
			since = time.Now()
		}
		return time.Since(since) >= stable
	})
}

func allLoaded(sim *Sim, ids []int64) bool {
	qc := sim.current()
	for _, id := range ids {
		c := qc.meta.GetCollection(qc.ctx, id)
		if c == nil || c.GetStatus() != querypb.LoadStatus_Loaded {
			return false
		}
	}
	return true
}

// quietLogs raises the log level to warn for one test: at 10^4 collections
// QueryCoord's info/debug logging alone is gigabytes and dominates CPU.
func quietLogs(t *testing.T) {
	prev := mlog.GetLevel()
	mlog.SetLevel(mlog.WarnLevel)
	t.Cleanup(func() { mlog.SetLevel(prev) })
}

func runR6(t *testing.T, n int) {
	sim := New(t, Options{})
	quietLogs(t)
	cfg := CalibratedNodeConfig()
	cfg.LoadJitterSigma = 0
	const nodes = 8
	sim.AddNodes(qnBase, nodes, cfg)
	cpu0 := ProcessCPUSeconds()
	start := time.Now()
	big := []int64{1, 2}
	for _, id := range big {
		sim.LoadCollection(id, 1, 2, 50, 500_000) // 100 x 256MB each
	}
	tiny := collectionIDs(1000, n)
	for i, id := range tiny {
		sim.LoadCollection(id, 1, 1, 1+i%2, 2_000)
	}
	submit := time.Since(start)
	all := append(append([]int64(nil), big...), tiny...)
	loaded := waitEvery(60*time.Minute, 500*time.Millisecond, func() bool { return allLoaded(sim, all) })
	loadTime := time.Since(start)
	settledOK := cheapSettle(sim, 20*time.Minute, 3*time.Second)
	settleTime := time.Since(start)
	m := map[string]float64{
		"tiny_collections":        float64(n),
		"submit_seconds":          submit.Seconds(),
		"loaded":                  boolMetric(loaded),
		"initial_load_seconds":    loadTime.Seconds(),
		"settled":                 boolMetric(settledOK),
		"settle_seconds":          settleTime.Seconds(),
		"load_process_cpu_sec":    ProcessCPUSeconds() - cpu0,
		"load_process_cpu_cores":  (ProcessCPUSeconds() - cpu0) / settleTime.Seconds(),
		"loads_canceled_at_load":  sim.FleetCounters()["loads_canceled"],
		"loads_succeeded_at_load": sim.FleetCounters()["loads_succeeded"],
	}
	segments := 0
	for _, id := range all {
		segments += len(sim.Broker.SegmentRows(id))
	}
	m["segments"] = float64(segments)
	// Loads beyond one per segment before the cluster settled: balance moves
	// that followed the RoundRobin initial placement.
	m["moves_until_settled"] = m["loads_succeeded_at_load"] - float64(segments)

	// Quiet window: process CPU (QueryCoord loops plus simulated nodes) and
	// moves after settle.
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	c0, w0 := ProcessCPUSeconds(), time.Now()
	const window = 30 * time.Second
	time.Sleep(window)
	m["quiet_process_cpu_cores"] = (ProcessCPUSeconds() - c0) / time.Since(w0).Seconds()
	mv := phase.Analyze()
	m["moves_after_settle"] = float64(mv.SegmentLoads)
	m["channel_moves_after_settle"] = float64(mv.ChannelWatches)
	m["moves_after_settle_per_real_hour"] = movesPerRealHour(mv.SegmentLoads, window)
	merge(m, "quiet_delta_", delta(sim.FleetCounters(), before))

	// Memory skew and concentration of tiny collections.
	merge(m, "", memSpread(sim))
	tinySet := setOf(tiny...)
	perNodeSegs := map[int64]int{}
	perNodeDelegators := map[int64]int{}
	totalSegs, totalDelegators := 0, 0
	for id, v := range sim.Fleet.Snapshot() {
		if !v.Up {
			continue
		}
		for _, coll := range v.Collection {
			if tinySet[coll] {
				perNodeSegs[id]++
				totalSegs++
			}
		}
		for _, d := range v.Delegators {
			if tinySet[d.Collection] {
				perNodeDelegators[id]++
				totalDelegators++
			}
		}
	}
	maxSegs, maxDel := 0, 0
	for _, c := range perNodeSegs {
		maxSegs = max(maxSegs, c)
	}
	for _, c := range perNodeDelegators {
		maxDel = max(maxDel, c)
	}
	m["tiny_segment_max_node_share"] = float64(maxSegs) / float64(max(1, totalSegs))
	m["tiny_delegator_max_node_share"] = float64(maxDel) / float64(max(1, totalDelegators))
	m["ideal_node_share"] = 1.0 / nodes
	merge(m, "", timeBalancer(sim, tiny, big, 300))
	merge(m, "", timeCheckers(sim, 5))
	sim.Report(Result{Scenario: fmt.Sprintf("R6_tiny_collections_n%d", n), Metrics: m})
}

// ---------------------------------------------------------------------------
// R7 A target about to be removed: 3 of 8 nodes are stopped gracefully one
// by one, 20s apart (~3 production minutes). Data drained from the first
// should not land on the next ones.

func TestSimRepro_R7_SequentialScaleIn(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	const nodes = 8
	sim.AddNodes(qnBase, nodes, cfg)
	ids := collectionIDs(1, 20)
	loadAll(t, sim, ids, 1, 2, 8, 200_000, 15*time.Minute) // 320 x 100MB
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)
	doomed := []int64{qnBase + 5, qnBase + 6, qnBase + 7}
	doomedSet := setOf(doomed...)
	views := sim.Fleet.Snapshot()
	onDoomed := map[int64]bool{}
	channelsOnDoomed := 0
	for _, id := range doomed {
		for seg := range views[id].Segments {
			onDoomed[seg] = true
		}
		channelsOnDoomed += len(views[id].Delegators)
	}

	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("sequential scale-in")
	phase := sim.StartPhase()
	start := time.Now()
	var wg sync.WaitGroup
	drain := make([]float64, len(doomed))
	var timeouts atomic.Int64
	for i, id := range doomed {
		wg.Add(1)
		go func(i int, id int64) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 20 * time.Second)
			t0 := time.Now()
			if !sim.StopNode(id, 2*time.Minute) {
				timeouts.Inc()
			}
			drain[i] = time.Since(t0).Seconds()
		}(i, id)
	}
	wg.Wait()
	settle, ok := softSettle(sim, 5*time.Minute, 3*time.Second)
	mv := phase.Analyze()
	var loadsToDoomed, extraMoves int
	for _, e := range sim.Fleet.Stats.Events(phase.start) {
		if e.Kind != EventLoad {
			continue
		}
		if doomedSet[e.Node] {
			loadsToDoomed++
		}
		if !onDoomed[e.Segment] {
			extraMoves++
		}
	}
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"initial_settled":           boolMetric(settledOK),
		"settled":                   boolMetric(ok),
		"total_seconds":             time.Since(start).Seconds(),
		"settle_tail_seconds":       seconds(settle),
		"min_segment_moves":         float64(len(onDoomed)),
		"min_channel_moves":         float64(channelsOnDoomed),
		"segment_moves":             float64(mv.SegmentLoads),
		"segment_move_amplif":       float64(mv.SegmentLoads) / max(1, float64(len(onDoomed))),
		"segments_moved_twice_plus": float64(mv.ReMovedSegments),
		"loads_onto_doomed_nodes":   float64(loadsToDoomed),
		"moves_of_undoomed_data":    float64(extraMoves),
		"channel_moves":             float64(mv.ChannelWatches),
		"channels_rewatched":        float64(mv.ReWatchedChans),
		"drain_timeouts":            float64(timeouts.Load()),
		"drain_seconds_1":           drain[0],
		"drain_seconds_2":           drain[1],
		"drain_seconds_3":           drain[2],
		"loads_canceled":            d["loads_canceled"],
	}
	reportPhase(sim, "R7_sequential_scale_in", m, phase, rec, at)
}

// ---------------------------------------------------------------------------
// R8 Slow delegator watch: WatchDmChannels takes longer than the channel
// task timeout (12s here, 120s production). Does QueryCoord retry forever?

func TestSimRepro_R8_SlowWatch(t *testing.T) {
	requireSim(t)
	for _, tc := range []struct {
		name  string
		watch time.Duration
	}{{"watch40s", 40 * time.Second}, {"watch8s", 8 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			sim := New(t, Options{})
			const nodes = 4
			sim.AddNodes(qnBase, nodes, DefaultNodeConfig())
			ids := collectionIDs(1, 8)
			loadAll(t, sim, ids, 1, 2, 4, 20_000, 5*time.Minute)
			_, settledOK := softSettle(sim, 2*time.Minute, time.Second)
			for _, id := range nodeRange(qnBase, nodes) {
				sim.Fleet.SetWatchLatency(id, tc.watch)
			}
			victim := qnBase + 1
			lost := len(sim.Fleet.Snapshot()[victim].Delegators)
			rec := sim.StartRecorder(200 * time.Millisecond)
			before := sim.FleetCounters()
			at := rec.Mark("kill")
			phase := sim.StartPhase()
			sim.KillNode(victim)
			recovered := waitRecovered(sim, 150*time.Second)
			d := delta(sim.FleetCounters(), before)
			_, chTasks := sim.PendingTasks()
			last := sim.Observe()
			m := map[string]float64{
				"initial_settled":       boolMetric(settledOK),
				"watch_latency_seconds": tc.watch.Seconds(),
				"channel_task_timeout_seconds": paramtable.Get().QueryCoordCfg.ChannelTaskTimeout.GetAsDuration(time.Millisecond).
					Seconds(),
				"channels_lost":          float64(lost),
				"recovered":              boolMetric(recovered >= 0),
				"recovery_seconds":       seconds(recovered),
				"watches_started":        d["watches"],
				"watches_failed":         d["watches_failed"],
				"redundant_watch_rpcs":   d["redundant_watch_rpcs"],
				"watch_attempts_per_ch":  d["watches"] / max(1, float64(lost)),
				"leaderless_at_end":      float64(last.LeaderlessChannels),
				"missing_at_end":         float64(last.MissingSegments),
				"channel_tasks_at_end":   float64(chTasks),
				"loads_failed":           d["loads_failed"],
				"segment_loads_attempts": d["loads_succeeded"] + d["loads_failed"],
			}
			reportPhase(sim, "R8_slow_watch_"+tc.name, m, phase, rec, at)
		})
	}
}

// ---------------------------------------------------------------------------
// R10 RoundRobin herding in recovery: 200 collections x 4 segments on 6
// nodes, one node dies. The segment checker places lost segments with
// RoundRobin; if every collection starts at the same node, that node takes
// far more than its share and balance moves it again afterwards.

func TestSimRepro_R10_RoundRobinHerding(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	cfg.LoadJitterSigma = 0
	const nodes = 6
	sim.AddNodes(qnBase, nodes, cfg)
	ids := collectionIDs(1, 200)
	loadAll(t, sim, ids, 1, 1, 4, 20_000, 10*time.Minute)
	_, settledOK := softSettle(sim, 5*time.Minute, 3*time.Second)
	victim := qnBase + 2
	lostSegs := len(sim.Fleet.Snapshot()[victim].Segments)
	survivors := []int64{}
	for _, id := range nodeRange(qnBase, nodes) {
		if id != victim {
			survivors = append(survivors, id)
		}
	}

	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	loadsBefore := sim.Fleet.Stats.LoadsByNode()
	at := rec.Mark("kill")
	sim.KillNode(victim)
	recovered := waitRecovered(sim, 5*time.Minute)
	loadsAt := sim.Fleet.Stats.LoadsByNode()
	m := map[string]float64{
		"initial_settled":  boolMetric(settledOK),
		"recovery_seconds": seconds(recovered),
		"lost_segments":    float64(lostSegs),
	}
	var mx, mn, sum float64 = 0, 1e18, 0
	for _, id := range survivors {
		v := float64(loadsAt[id] - loadsBefore[id])
		m[fmt.Sprintf("recovery_loads_node_%d", id)] = v
		mx = max(mx, v)
		mn = min(mn, v)
		sum += v
	}
	mean := sum / float64(len(survivors))
	m["recovery_loads_max"] = mx
	m["recovery_loads_min"] = mn
	if mean > 0 {
		m["recovery_loads_max_over_mean"] = mx / mean
	}
	inflightMetrics(sim, m, "recovery_", survivors)

	// After recovery: how much does balance move again?
	phase := sim.StartPhase()
	after := rec.Mark("recovered")
	_, ok := softSettle(sim, 3*time.Minute, 5*time.Second)
	time.Sleep(10 * time.Second)
	mv := phase.Analyze()
	m["settled_after"] = boolMetric(ok)
	m["moves_after_recovery"] = float64(mv.SegmentLoads)
	m["moves_after_recovery_over_lost"] = float64(mv.SegmentLoads) / max(1, float64(lostSegs))
	merge(m, "", Summary(rec.Since(at)))
	m["final_segment_cv"] = Summary(rec.Since(after))["final_segment_cv"]
	sim.Report(Result{Scenario: "R10_roundrobin_herding", Metrics: m})
}

// ---------------------------------------------------------------------------
// R17 Flip-flop resize: all 6 nodes are replaced by bigger ones (rolling,
// 1s apart: new pod up, old pod drained), and 3 simulated minutes (18s)
// later replaced back. The second swap starts before the first finishes.

func TestSimRepro_R17_FlipFlopResize(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	small := CalibratedNodeConfig()
	bigCfg := small
	bigCfg.MemoryBytes = 128 << 30
	const nodes = 6
	sim.AddNodes(qnBase, nodes, small)
	ids := collectionIDs(1, 20)
	loadAll(t, sim, ids, 1, 2, 8, 200_000, 15*time.Minute) // 320 x 100MB = 32GB
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)
	copies := 0
	for _, v := range sim.Fleet.Snapshot() {
		copies += len(v.Segments)
	}
	total := liveUsedBytes(sim)
	m := map[string]float64{
		"initial_settled":           boolMetric(settledOK),
		"segment_copies":            float64(copies),
		"lower_bound_seconds_swap":  float64(total) / float64(nodes*small.LoadBandwidthBytes),
		"swap_gap_seconds":          18,
		"min_segment_moves_per_swp": float64(copies),
	}
	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("swap 1")

	var timeouts atomic.Int64
	swap := func(old []int64, firstNew int64, cfg NodeConfig, done []time.Duration, start time.Time, wg *sync.WaitGroup) []int64 {
		var fresh []int64
		for i, id := range old {
			fresh = append(fresh, sim.AddNodes(firstNew+int64(i), 1, cfg)...)
			wg.Add(1)
			go func(i int, id int64) {
				defer wg.Done()
				if !sim.StopNode(id, 2*time.Minute) {
					timeouts.Inc()
				}
				done[i] = time.Since(start)
			}(i, id)
			time.Sleep(time.Second)
		}
		return fresh
	}
	var wg sync.WaitGroup
	done1 := make([]time.Duration, nodes)
	done2 := make([]time.Duration, nodes)
	start1 := time.Now()
	phase1 := sim.StartPhase()
	mid := swap(nodeRange(qnBase, nodes), 100, bigCfg, done1, start1, &wg)
	time.Sleep(18*time.Second - time.Since(start1))
	served1 := sim.Observe()
	pm1 := phase1.Metrics()
	start2 := time.Now()
	at2 := rec.Mark("swap 2")
	phase2 := sim.StartPhase()
	swap(mid, 200, small, done2, start2, &wg)
	wg.Wait()
	settle, ok := softSettle(sim, 5*time.Minute, 3*time.Second)
	pm2 := phase2.Metrics()
	maxDone := func(ds []time.Duration) float64 {
		var mx time.Duration
		for _, d := range ds {
			mx = max(mx, d)
		}
		return mx.Seconds()
	}
	d := delta(sim.FleetCounters(), before)
	m["swap1_old_nodes_drained_seconds"] = maxDone(done1)
	m["swap1_done_before_swap2"] = boolMetric(maxDone(done1) <= 18 && served1.SegmentTasks == 0)
	m["swap1_missing_at_swap2"] = float64(served1.MissingSegments)
	m["swap1_segment_loads"] = pm1["segment_loads"]
	m["swap2_drained_seconds"] = maxDone(done2)
	m["swap2_settle_seconds"] = time.Since(start2).Seconds()
	m["swap2_segment_loads"] = pm2["segment_loads"]
	m["total_segment_loads"] = d["loads_succeeded"]
	m["load_amplification"] = d["loads_succeeded"] / float64(2*copies)
	m["settled"] = boolMetric(ok)
	m["settle_tail_seconds"] = seconds(settle)
	m["drain_timeouts_total"] = float64(timeouts.Load())
	m["loads_canceled"] = d["loads_canceled"]
	m["loads_to_stopping"] = d["loads_to_stopping"]
	m["max_queue_wait_seconds"] = d["max_queue_wait_seconds"]
	m["swap1_missing_segment_samples"] = Summary(rec.Since(at))["missing_segment_samples"] - Summary(rec.Since(at2))["missing_segment_samples"]
	m["swap2_missing_segment_samples"] = Summary(rec.Since(at2))["missing_segment_samples"]
	merge(m, "", Summary(rec.Since(at)))
	sim.Report(Result{Scenario: "R17_flip_flop_resize", Metrics: m})
}

// ---------------------------------------------------------------------------
// R18 Scale-out spike: 4 nodes join a loaded 4-node cluster at once.

func TestSimRepro_R18_ScaleOutSpike(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	sim.AddNodes(qnBase, 4, cfg)
	ids := collectionIDs(1, 20)
	loadAll(t, sim, ids, 1, 2, 8, 200_000, 15*time.Minute) // 320 x 100MB = 32GB
	_, settledOK := softSettle(sim, 5*time.Minute, 2*time.Second)
	total := liveUsedBytes(sim)
	rec := sim.StartRecorder(100 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	start := time.Now()
	fresh := sim.AddNodes(qnBase+4, 4, cfg)
	sim.WaitUntil(10*time.Second, func() bool {
		qc := sim.current()
		for _, id := range ids {
			for _, r := range qc.meta.GetByCollection(qc.ctx, id) {
				for _, n := range fresh {
					if !r.Contains(n) {
						return false
					}
				}
			}
		}
		return true
	})
	at := rec.Mark("scale out")
	phase := sim.StartPhase()
	balanced, balOK := waitBalanced(sim, 0.15, 5*time.Minute)
	quiet, quietOK := softSettle(sim, 5*time.Minute, 3*time.Second)
	freshSet := setOf(fresh...)
	var toOld int
	for _, e := range sim.Fleet.Stats.Events(start) {
		if e.Kind == EventLoad && !freshSet[e.Node] {
			toOld++
		}
	}
	d := delta(sim.FleetCounters(), before)
	m := map[string]float64{
		"initial_settled":         boolMetric(settledOK),
		"balanced":                boolMetric(balOK),
		"balance_seconds":         seconds(balanced),
		"quiet":                   boolMetric(quietOK),
		"settle_seconds":          time.Since(start).Seconds(),
		"quiet_tail_seconds":      seconds(quiet),
		"lower_bound_seconds":     float64(total) / 2 / float64(4*cfg.LoadBandwidthBytes),
		"loads_between_old_nodes": float64(toOld),
		"loads_canceled":          d["loads_canceled"],
		"max_queue_wait_seconds":  d["max_queue_wait_seconds"],
		"overshoot_new_nodes":     overshoot(rec, at, fresh),
	}
	inflightMetrics(sim, m, "new_", fresh)
	reportPhase(sim, "R18_scale_out_spike", m, phase, rec, at)
}

// ---------------------------------------------------------------------------
// R20 StreamingNode replacement by surge: a new StreamingNode joins (the
// simulated StreamingCoord spreads WALs onto it), and the old one is removed
// a production 30s (3s here) or 300s (30s here) later. Count channel moves
// against the minimum and channels that move A->B->A.

func TestSimRepro_R20_StreamingNodeSurge(t *testing.T) {
	requireSim(t)
	for _, tc := range []struct {
		name string
		gap  time.Duration
	}{{"gap3s", 3 * time.Second}, {"gap30s", 30 * time.Second}} {
		t.Run(tc.name, func(t *testing.T) {
			topo := defaultTopology(true)
			topo.replicas = 1
			sim, rec := buildCluster(t, topo)
			old := snBase
			views := sim.Fleet.Snapshot()
			delegatorsOnOld := len(views[old].Delegators)
			walsOnOld := 0
			for _, node := range sim.WAL().Assignment() {
				if node == old {
					walsOnOld++
				}
			}
			w0 := walMoves(sim)
			before := sim.FleetCounters()
			at := rec.Mark("surge")
			phase := sim.StartPhase()
			start := time.Now()
			sim.AddStreamingNodes(snBase+10, 1, DefaultNodeConfig(), true)
			wJoin := walMoves(sim) - w0
			time.Sleep(tc.gap)
			sim.WAL().RemoveNode(old)
			drained := sim.StopNode(old, 2*time.Minute)
			followed, ok := waitOffWAL(sim, 2*time.Minute)
			mv := phase.Analyze()
			d := delta(sim.FleetCounters(), before)
			m := map[string]float64{
				"gap_seconds":             tc.gap.Seconds(),
				"delegators_on_old":       float64(delegatorsOnOld),
				"wals_on_old":             float64(walsOnOld),
				"wal_moves_on_join":       float64(wJoin),
				"wal_moves_total":         float64(walMoves(sim) - w0),
				"channel_moves":           float64(mv.ChannelWatches),
				"channel_move_amplif":     float64(mv.ChannelWatches) / max(1, float64(delegatorsOnOld)),
				"pingpong_channels":       float64(mv.PingPongChannels),
				"rewatched_channels":      float64(mv.ReWatchedChans),
				"old_drained":             boolMetric(drained),
				"followed_wal":            boolMetric(ok),
				"total_seconds":           time.Since(start).Seconds(),
				"follow_seconds":          seconds(followed),
				"segment_loads":           float64(mv.SegmentLoads),
				"redundant_watch_rpcs":    d["redundant_watch_rpcs"],
				"watches_failed":          d["watches_failed"],
				"delegators_on_new_final": float64(len(sim.Fleet.Snapshot()[snBase+10].Delegators)),
			}
			reportPhase(sim, "R20_sn_surge_"+tc.name, m, phase, rec, at)
		})
	}
}

// ---------------------------------------------------------------------------
// R21 An overloaded node: start balanced at ~60% memory, then one node's
// memory grows past 90% while QueryCoord's row view stays the same
// ("inflate": segments there become 1.7x bigger, e.g. indexes or lazily
// loaded fields becoming resident) or grows only through delegator growing
// rows ("growing": ingest without flush into the channels led there; the
// growing rows are reported to QueryCoord). Does balance move data off it?

func TestSimRepro_R21_OverloadedNode(t *testing.T) {
	requireSim(t)
	for _, mode := range []string{"inflate", "growing"} {
		t.Run(mode, func(t *testing.T) {
			sim := New(t, Options{})
			cfg := fastLoadMemConfig(nodeMem)
			const nodes = 6
			sim.AddNodes(qnBase, nodes, cfg)
			ids := collectionIDs(1, 12)
			// 12 x 12 x 200MB = 28.8GB on 48GB: ~60% per node.
			loadAll(t, sim, ids, 1, 1, 12, 400_000, 10*time.Minute)
			_, settledOK := softSettle(sim, 5*time.Minute, 3*time.Second)
			if mode == "growing" {
				sim.Fleet.ChargeGrowingMemory.Store(true)
			}
			// The hot node: the one leading the most channels.
			views := sim.Fleet.Snapshot()
			hot := qnBase
			for id, v := range views {
				if len(v.Delegators) > len(views[hot].Delegators) || (len(v.Delegators) == len(views[hot].Delegators) && id < hot) {
					hot = id
				}
			}
			var led []int64
			for _, d := range views[hot].Delegators {
				led = append(led, d.Collection)
			}
			sort.Slice(led, func(i, j int) bool { return led[i] < led[j] })
			m := map[string]float64{
				"initial_settled":   boolMetric(settledOK),
				"hot_util_before":   nodeUtil(sim, hot),
				"hot_led_channels":  float64(len(led)),
				"hot_segments_befr": float64(len(views[hot].Segments)),
			}
			rec := sim.StartRecorder(200 * time.Millisecond)
			before := sim.FleetCounters()
			at := rec.Mark("overload")
			phase := sim.StartPhase()
			start := time.Now()
			if mode == "inflate" {
				sim.Fleet.InflateSegments(hot, 1.7)
			} else {
				// 50k rows (25MB) per led channel per 100ms until >92%.
				in := startIngest(sim, led, 50_000, 0)
				sim.WaitUntil(30*time.Second, func() bool { return nodeUtil(sim, hot) > 0.92 })
				in.Stop()
			}
			m["hot_util_peak"] = nodeUtil(sim, hot)
			// Ten production minutes.
			time.Sleep(60 * time.Second)
			released, releasedRows := releaseEventsOn(sim, start, hot)
			loadsOnto, _ := loadEventsOnto(sim, start, setOf(hot))
			m["hot_util_end"] = nodeUtil(sim, hot)
			m["moved_off_hot_segments"] = float64(released)
			m["moved_off_hot_rows"] = float64(releasedRows)
			m["loads_onto_hot"] = float64(loadsOnto)
			m["hot_still_over_90pct"] = boolMetric(nodeUtil(sim, hot) > memLimit)
			d := delta(sim.FleetCounters(), before)
			m["window_oom_rejections"] = d["loads_rejected_oom"]
			overloadMetrics(sim, m)
			reportPhase(sim, "R21_overloaded_node_"+mode, m, phase, rec, at)
		})
	}
}
