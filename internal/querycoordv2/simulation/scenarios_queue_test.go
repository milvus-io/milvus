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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// S11 Recovery backlog larger than SegmentTaskTimeout. Loads queue on the
// surviving QueryNodes; tasks whose queue wait exceeds the timeout are
// killed and rebuilt. Compare against a timeout that never fires and against
// the bandwidth lower bound (lost bytes / surviving bandwidth).
func TestSimMaster_S11_RecoveryBacklogTimeout(t *testing.T) {
	requireSim(t)
	for _, tc := range []struct {
		name    string
		timeout string
	}{{"timeout30s", "30000"}, {"timeout300s", "300000"}} {
		t.Run(tc.name, func(t *testing.T) {
			qc := &paramtable.Get().QueryCoordCfg
			sim := New(t, Options{Params: map[string]string{
				qc.SegmentTaskTimeout.Key:      tc.timeout,
				qc.SegmentTaskRetryTimeout.Key: tc.timeout,
			}})
			cfg := DefaultNodeConfig()
			const nodes = 4
			sim.AddNodes(qnBase, nodes, cfg)
			// 16 collections x 2 channels x 8 segments of 200MB: ~12.8GB per
			// node, so one crash leaves ~4.3GB (~43s of bandwidth) per survivor.
			ids := collectionIDs(1, scaled(16))
			loadCounters := sim.FleetCounters()
			loadTime := loadAll(t, sim, ids, 1, 2, 8, 400_000, 10*time.Minute)
			loadDelta := delta(sim.FleetCounters(), loadCounters)
			_, ok := waitSettled(sim, 3*time.Minute, time.Second)
			require.True(t, ok)

			victim := qnBase + 1
			lostBytes := sim.Fleet.Snapshot()[victim].UsedMemory
			rec := sim.StartRecorder(100 * time.Millisecond)
			sim.Fleet.Stats.ResetPeaks()
			before := sim.FleetCounters()
			at := rec.Mark("kill node")
			phase := sim.StartPhase()
			sim.KillNode(victim)
			start := time.Now()
			var recovered time.Duration = -1
			if sim.WaitUntil(8*time.Minute, func() bool { s := sim.Observe(); return s.MissingSegments == 0 && s.LeaderlessChannels == 0 }) {
				recovered = time.Since(start)
			}
			lowerBound := float64(lostBytes) / float64(int64(nodes-1)*cfg.LoadBandwidthBytes)
			m := map[string]float64{
				"recovery_seconds":    seconds(recovered),
				"lower_bound_seconds": lowerBound,
				"lost_gb":             float64(lostBytes) / (1 << 30),
				"load_seconds":        loadTime.Seconds(),
				"load_lower_bound_seconds": float64(int64(len(ids))*16*400_000*sim.Broker.BytesPerRow) /
					float64(int64(nodes)*cfg.LoadBandwidthBytes),
				"load_loads_failed":    loadDelta["loads_failed"],
				"load_loads_succeeded": loadDelta["loads_succeeded"],
			}
			if recovered > 0 {
				m["recovery_over_lower_bound"] = recovered.Seconds() / lowerBound
			}
			// Per-survivor share of the reloaded bytes (successful loads only).
			perNode := map[int64]int64{}
			for _, e := range sim.Fleet.Stats.Events(phase.start) {
				if e.Kind == EventLoad {
					perNode[e.Node] += e.Rows * sim.Broker.BytesPerRow
				}
			}
			var maxBytes, total int64
			for node, b := range perNode {
				maxBytes = max(maxBytes, b)
				total += b
				m[fmt.Sprintf("reload_gb_node_%d", node)] = float64(b) / (1 << 30)
			}
			if total > 0 {
				m["reload_max_over_mean"] = float64(maxBytes) / (float64(total) / float64(nodes-1))
			}
			d := delta(sim.FleetCounters(), before)
			merge(m, "delta_", d)
			reportPhase(sim, fmt.Sprintf("S11_recovery_backlog_%s", tc.name), m, phase, rec, at)
		})
	}
}

// S12 Steady-state balance churn on a small cluster with many segments, the
// shape of a production cluster seen doing ~2349 segment moves/hour with
// 5 QueryNodes and ~1119 segments. Nothing changes topology: any sustained
// Move rate is churn. Variants separate heavy-tailed segment sizes and
// ingestion (delegators report growing rows, which the node score counts).
func TestSimMaster_S12_SteadyStateChurn(t *testing.T) {
	requireSim(t)
	for _, tc := range []struct {
		name       string
		sigma      float64
		ingest     bool
		hideGrow   bool // delegators report no growing rows
		neverFlush bool // growing rows accumulate, no new sealed segments
	}{
		{"uniform_idle", 0, false, false, false},
		{"lognormal_idle", 1.2, false, false, false},
		{"uniform_ingest", 0, true, false, false},
		{"lognormal_ingest", 1.2, true, false, false},
		{"uniform_ingest_hidden_growing", 0, true, true, false},
		{"uniform_growing_no_flush", 0, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim := New(t, Options{})
			sim.Broker.RowsSigma = tc.sigma
			sim.Fleet.HideGrowingRows.Store(tc.hideGrow)
			sim.AddNodes(qnBase, 5, CalibratedNodeConfig())
			ids := collectionIDs(1, scaled(10))
			loadAll(t, sim, ids, 1, 2, 56, 20_000, 10*time.Minute)
			sim.Broker.RowsSigma = 0
			// Let the post-load balance finish before measuring.
			_, ok := waitSettled(sim, 5*time.Minute, 5*time.Second)
			require.True(t, ok, "initial load did not settle")

			stop := make(chan struct{})
			done := make(chan struct{})
			if tc.ingest {
				go func() {
					defer close(done)
					tick := time.NewTicker(100 * time.Millisecond)
					defer tick.Stop()
					for i := 1; ; i++ {
						select {
						case <-stop:
							return
						case <-tick.C:
						}
						for _, id := range ids {
							sim.Broker.Ingest(id, 2_000)
							if !tc.neverFlush && i%30 == 0 {
								sim.Broker.FlushGrowing(id)
							}
						}
					}
				}()
			} else {
				close(done)
			}
			rec := sim.StartRecorder(200 * time.Millisecond)
			sim.Fleet.Stats.ResetPeaks()
			before := sim.FleetCounters()
			at := rec.Mark("steady state")
			phase := sim.StartPhase()
			const window = 90 * time.Second
			time.Sleep(window)
			close(stop)
			<-done
			m := map[string]float64{"window_seconds": window.Seconds()}
			mv := phase.Analyze()
			// Simulated time runs ~10x faster than real time.
			m["existing_segment_moves_per_real_hour"] = float64(mv.SegmentLoads) / window.Seconds() * 3600 / 10
			merge(m, "delta_", delta(sim.FleetCounters(), before))
			reportPhase(sim, "S12_steady_churn_"+tc.name, m, phase, rec, at)
		})
	}
}

// S13 One segment whose load alone takes longer than SegmentTaskTimeout.
// The task is killed at the deadline and rebuilt with the same budget every
// checker round, so the collection never finishes loading (production
// a0e8d38c: load p90 317s / p99 486s against the 300s timeout, Loki shows
// the same segment re-opened every ~40s). Control: a timeout that fits.
func TestSimMaster_S13_OversizedSegmentNeverLoads(t *testing.T) {
	requireSim(t)
	for _, tc := range []struct {
		name    string
		timeout string
	}{{"timeout30s", "30000"}, {"timeout120s", "120000"}} {
		t.Run(tc.name, func(t *testing.T) {
			qc := &paramtable.Get().QueryCoordCfg
			sim := New(t, Options{Params: map[string]string{
				qc.SegmentTaskTimeout.Key:      tc.timeout,
				qc.SegmentTaskRetryTimeout.Key: tc.timeout,
			}})
			cfg := CalibratedNodeConfig()
			cfg.LoadJitterSigma = 0
			sim.AddNodes(qnBase, 4, cfg)
			// 20 ordinary 10MB segments and one 2.8GB segment: at 62.5MB/s
			// per load slot the big one needs ~45s, 1.5x the 30s timeout.
			sim.Broker.CreateCollection(1, 1, 20, 20_000)
			sim.Broker.Flush(1, 5_600_000)
			sim.loadExisting(1, 1)
			start := time.Now()
			loaded := sim.WaitUntil(4*time.Minute, func() bool { return sim.Loaded(1) })
			m := map[string]float64{
				"loaded":          boolMetric(loaded),
				"load_seconds":    time.Since(start).Seconds(),
				"loads_failed":    float64(sim.Fleet.Stats.LoadsFailed.Load()),
				"loads_succeeded": float64(sim.Fleet.Stats.LoadsSucceeded.Load()),
				"missing_at_end":  float64(sim.Observe().MissingSegments),
			}
			sim.Report(Result{Scenario: "S13_oversized_segment_" + tc.name, Metrics: m})
		})
	}
}
