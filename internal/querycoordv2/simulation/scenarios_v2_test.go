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
)

// smallOnlyCluster loads many small collections (2-3 segments each, sizes
// varying) on `nodes` nodes, with no big collection as a base.
func smallOnlyCluster(t *testing.T, nodes, colls int) (*Sim, []int64, NodeConfig) {
	sim := New(t, Options{})
	cfg := CalibratedNodeConfig()
	cfg.LoadJitterSigma = 0
	sim.AddNodes(qnBase, nodes, cfg)
	sim.Broker.MemSigma = 0.6
	ids := collectionIDs(1, colls)
	for i, id := range ids {
		sim.Broker.CreateCollection(id, 1, 2+i%2, 200_000)
		sim.loadExisting(id, 1)
	}
	sim.Broker.MemSigma = 0
	require.True(t, sim.WaitUntil(10*time.Minute, func() bool { return sim.Loaded(ids...) }), "initial load")
	_, ok := waitSettled(sim, 3*time.Minute, 3*time.Second)
	require.True(t, ok, "settle after load")
	return sim, ids, cfg
}

// reloadShare reports how the bytes loaded since phase start split across nodes.
func reloadShare(sim *Sim, since time.Time, m map[string]float64, prefix string) {
	perNode := map[int64]int64{}
	var total, top int64
	for _, e := range sim.Fleet.Stats.Events(since) {
		if e.Kind == EventLoad {
			perNode[e.Node] += e.Rows * sim.Broker.BytesPerRow
		}
	}
	for _, b := range perNode {
		total += b
		top = max(top, b)
	}
	m[prefix+"loading_nodes"] = float64(len(perNode))
	if total > 0 {
		m[prefix+"top_node_share"] = float64(top) / float64(total)
	}
}

// V1 A crash replaced by an empty node in a cluster of only small
// collections: does placement send the whole lost share to the replacement
// (one node's bandwidth), or keep recovery parallel?
func TestSimV2_V1_ReplaceNodeSmallOnly(t *testing.T) {
	requireSim(t)
	const nodes = 6
	sim, _, cfg := smallOnlyCluster(t, nodes, 300)
	victim := qnBase + 1
	lost := sim.Fleet.Snapshot()[victim].UsedMemory
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	phase := sim.StartPhase()
	rec := sim.StartRecorder(200 * time.Millisecond)
	at := rec.Mark("replace node")
	sim.KillNode(victim)
	sim.AddNodes(qnBase+nodes, 1, cfg)
	start := time.Now()
	var recovered time.Duration = -1
	if sim.WaitUntil(8*time.Minute, func() bool { s := sim.Observe(); return s.MissingSegments == 0 && s.LeaderlessChannels == 0 }) {
		recovered = time.Since(start)
	}
	lower := float64(lost) / float64(int64(nodes)*cfg.LoadBandwidthBytes)
	m := map[string]float64{
		"recovery_seconds":          seconds(recovered),
		"lower_bound_seconds":       lower,
		"recovery_over_lower_bound": recovered.Seconds() / lower,
	}
	reloadShare(sim, phase.start, m, "recovery_")
	_, ok := waitSettled(sim, 4*time.Minute, 3*time.Second)
	m["settled"] = boolMetric(ok)
	merge(m, "", memSpread(sim))
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	reportPhase(sim, "V1_replace_node_small_only", m, phase, rec, at)
}

// V2 Scale-out of a cluster of only small collections: no big collection is
// spread onto the new nodes first, so placement and leveling alone fill them.
func TestSimV2_V2_ScaleOutSmallOnly(t *testing.T) {
	requireSim(t)
	const nodes = 4
	sim, ids, cfg := smallOnlyCluster(t, nodes, 300)
	phase := sim.StartPhase()
	rec := sim.StartRecorder(200 * time.Millisecond)
	at := rec.Mark("scale out")
	newNodes := sim.AddNodes(qnBase+nodes, 2, cfg)
	// Keep new segments arriving while the new nodes are empty.
	for i := 0; i < 60; i++ {
		id := ids[i]
		sim.Broker.Flush(id, 200_000)
		time.Sleep(500 * time.Millisecond)
	}
	_, ok := waitSettled(sim, 5*time.Minute, 3*time.Second)
	m := map[string]float64{"settled": boolMetric(ok), "overshoot_new_nodes": overshoot(rec, at, newNodes)}
	reloadShare(sim, phase.start, m, "after_scaleout_")
	merge(m, "", memSpread(sim))
	reportPhase(sim, fmt.Sprintf("V2_scale_out_small_only_%dto%d", nodes, nodes+2), m, phase, rec, at)
}
