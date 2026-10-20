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
)

// Robustness scenarios: each disturbs a two-replica, 8-node cluster while
// balance is active and checks the cluster ends whole (no missing segment,
// no third copy), quiet (settles, little ping-pong) and level enough.

func robustReport(t *testing.T, sim *Sim, name string, phase *Phase, rec *Recorder, at time.Duration, m map[string]float64) {
	ok := sim.WaitUntil(6*time.Minute, func() bool { s := sim.Observe(); return s.MissingSegments == 0 && s.LeaderlessChannels == 0 })
	m["recovered"] = boolMetric(ok)
	quiet, settledOK := waitSettled(sim, 5*time.Minute, 3*time.Second)
	m["settled"] = boolMetric(settledOK)
	m["quiet_seconds"] = seconds(quiet)
	merge(m, "", memSpread(sim))
	reportPhase(sim, name, m, phase, rec, at)
}

// RB1 QueryCoord restarts twice while scale-out moves are in flight: the
// ledger and cooldowns are lost; nothing may be stranded or duplicated.
func TestSimV2_RB1_RestartMidBalance(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(false))
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	at := rec.Mark("scale out + restarts")
	sim.AddNodes(qnBase+8, 2, DefaultNodeConfig())
	time.Sleep(4 * time.Second)
	sim.RestartQueryCoord()
	time.Sleep(6 * time.Second)
	sim.RestartQueryCoord()
	m := map[string]float64{}
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	robustReport(t, sim, "RB1_restart_mid_balance", phase, rec, at, m)
}

// RB2 One load in ten fails while the cluster scales out: failed moves must
// not leave the planner stuck (stale ledger entries) or retry forever.
func TestSimV2_RB2_LoadFailures(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(false))
	sim.Fleet.LoadFailPermille.Store(100)
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	at := rec.Mark("scale out with 10% load failures")
	sim.AddNodes(qnBase+8, 2, DefaultNodeConfig())
	time.Sleep(90 * time.Second)
	sim.Fleet.LoadFailPermille.Store(0)
	m := map[string]float64{}
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	robustReport(t, sim, "RB2_load_failures_10pct", phase, rec, at, m)
}

// RB3 One node flaps (restart three times, 15s apart): each restart empties
// it; recovery and refill must not ping-pong segments between survivors.
func TestSimV2_RB3_NodeFlapping(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(false))
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	at := rec.Mark("node flaps")
	for i := 0; i < 3; i++ {
		sim.RestartNode(qnBase + 2)
		time.Sleep(15 * time.Second)
	}
	m := map[string]float64{}
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	robustReport(t, sim, "RB3_node_flapping", phase, rec, at, m)
}

// RB4 Two nodes drained one after the other (graceful stop, 10s apart): a
// segment drained off the first must not land on the second.
func TestSimV2_RB4_SequentialDrain(t *testing.T) {
	requireSim(t)
	sim, rec := buildCluster(t, defaultTopology(false))
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	at := rec.Mark("sequential drain")
	done := make(chan bool, 2)
	go func() { done <- sim.StopNode(qnBase+1, 3*time.Minute) }()
	time.Sleep(10 * time.Second)
	go func() { done <- sim.StopNode(qnBase+2, 3*time.Minute) }()
	m := map[string]float64{"drain_timeouts": 0}
	for i := 0; i < 2; i++ {
		if !<-done {
			m["drain_timeouts"]++
		}
	}
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	robustReport(t, sim, "RB4_sequential_drain", phase, rec, at, m)
}

// RB5 Near full: small nodes at ~75%, then a big new collection pushes demand
// to ~92%; placement must not oscillate on memory rejections.
func TestSimV2_RB5_NearFull(t *testing.T) {
	requireSim(t)
	sim := New(t, Options{})
	cfg := memConfig()
	sim.AddNodes(qnBase, 4, cfg)
	ids := collectionIDs(1, 8)
	for _, id := range ids {
		sim.Broker.CreateWideCollection(id, 2, 12, 150_000, narrowRow)
		sim.loadExisting(id, 1)
	}
	if !sim.WaitUntil(5*time.Minute, func() bool { return sim.Loaded(ids...) }) {
		t.Fatal("initial load")
	}
	_, _ = waitSettled(sim, 2*time.Minute, 3*time.Second)
	rec := sim.StartRecorder(200 * time.Millisecond)
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	at := rec.Mark("push to ~92%")
	sim.Broker.CreateWideCollection(100, 2, 8, 150_000, narrowRow*2)
	sim.loadExisting(100, 1)
	loaded := sim.WaitUntil(4*time.Minute, func() bool { return sim.Loaded(100) })
	m := map[string]float64{"new_collection_loaded": boolMetric(loaded)}
	memoryMetrics(sim, m)
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	robustReport(t, sim, "RB5_near_full", phase, rec, at, m)
}
