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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Case describes a production cluster to replay: topology, data shape,
// ingestion and a timeline of topology events. All times are simulated
// seconds; the simulator runs about 10x faster than real time, so a real
// 7-day trace is compressed (and usually sampled) into a few minutes.
type Case struct {
	Name     string `json:"name"`
	Category string `json:"category"`
	Source   string `json:"source"` // where the numbers came from

	Streaming bool `json:"streaming"`
	QNs       int  `json:"qns"`
	SNs       int  `json:"sns"`
	QNCores   int  `json:"qn_cores"`
	QNMemGB   int  `json:"qn_memory_gb"`

	Load struct {
		Concurrency int   `json:"concurrency"`
		BandwidthMB int64 `json:"bandwidth_mb"`
		// SingleBandwidthMB switches to the fitted model (NodeConfig.LoadSingleBandwidthBytes).
		SingleBandwidthMB int64   `json:"single_bandwidth_mb"`
		BaseMs            int     `json:"base_ms"`
		JitterSigma       float64 `json:"jitter_sigma"`
		// Reference-latency model (see NodeConfig.LoadRefLatency).
		RefLatencyMs float64 `json:"ref_latency_ms"`
		RefBytes     int64   `json:"ref_bytes"`
		SizeExponent float64 `json:"size_exponent"`
	} `json:"load"`
	Search SearchModel `json:"search"`

	// TimeScale > 1 compresses the replay further: event times, ingestion
	// schedules and DurationSeconds (all in simulated seconds, themselves
	// about a tenth of real time) pass that much faster, QueryNodes load
	// that much faster and QueryCoord's intervals, task timeouts and
	// cooldown shrink alike, so a day fits in minutes with the same ratios.
	TimeScale float64 `json:"time_scale"`

	// ResourceGroups beyond the default one, each taking nodes from it.
	ResourceGroups []struct {
		Name  string `json:"name"`
		Nodes int32  `json:"nodes"`
	} `json:"resource_groups"`

	Collections []tableSpec `json:"collections"`

	Ingest struct {
		RowsPerSecPerChannel int64   `json:"rows_per_sec_per_channel"`
		FlushEverySeconds    float64 `json:"flush_every_seconds"`
		CompactEverySeconds  float64 `json:"compact_every_seconds"`
	} `json:"ingest"`

	Events []CaseEvent `json:"events"`

	DurationSeconds float64 `json:"duration_seconds"`
	// MaxSegmentRows caps compaction output (default 2,097,152 rows: 1GB at
	// 512 bytes a row, Milvus's segment max size); -1 lifts the cap.
	MaxSegmentRows int64             `json:"max_segment_rows"`
	Params         map[string]string `json:"params"`
}

// tableSpec describes a group of alike tables.
type tableSpec struct {
	Count              int      `json:"count"`
	Replicas           int32    `json:"replicas"`
	Channels           int      `json:"channels"`
	SegmentsPerChannel int      `json:"segments_per_channel"`
	RowsMedian         int64    `json:"rows_median"`
	RowsSigma          float64  `json:"rows_sigma"`
	RGs                []string `json:"rgs"` // resource groups, default when empty
	// Per-group turnover; zero falls back to the case-level Ingest.
	FlushEverySeconds   float64 `json:"flush_every_seconds"`
	CompactEverySeconds float64 `json:"compact_every_seconds"`
	FlushRows           int64   `json:"flush_rows"`
}

// CaseEvent is one event of the timeline, at a simulated second (before
// the time scale: with TimeScale 6 an event at 600 fires 100s in). Op:
//
//	add_qn          Count nodes join (MemoryGB overrides the case's; RG names
//	                the resource group they join, the default when empty)
//	kill_qn         Node crashes; restart_qn brings it back empty
//	stop_qn         Node drains and leaves; with Count and no Node, the
//	                Count newest nodes leave (scale in)
//	rolling_replace Count oldest nodes are each drained and replaced by a
//	                fresh one, GapSeconds apart (an upgrade)
//	restart_qc      QueryCoord restarts
//	slow_qn         Node loads Slowdown times slower
//	ingest_rate     ingestion runs at Multiplier of the case's rate (0 pauses)
//	load_tables     Tables (a tableSpec) are created and loaded
//	release_tables  the Count newest loaded tables are released
//	reload_tables   the Count most recently released tables are loaded again
//	add_sn, kill_sn streaming nodes
type CaseEvent struct {
	At         float64    `json:"at"`
	Op         string     `json:"op"`
	Node       int64      `json:"node"`
	Count      int        `json:"count"`
	Slowdown   float64    `json:"slowdown"`
	Multiplier float64    `json:"multiplier"`
	MemoryGB   int        `json:"memory_gb"`
	GapSeconds float64    `json:"gap_seconds"`
	Tables     *tableSpec `json:"tables"`
	RG         string     `json:"rg"` // add_qn: the resource group the nodes join
}

func (c *Case) nodeConfig() NodeConfig {
	cfg := CalibratedNodeConfig()
	if c.QNCores > 0 {
		cfg.CPUNum = int64(c.QNCores)
	}
	if c.QNMemGB > 0 {
		cfg.MemoryBytes = int64(c.QNMemGB) << 30
	}
	if c.Load.Concurrency > 0 {
		cfg.LoadConcurrency = c.Load.Concurrency
	}
	if c.Load.BandwidthMB > 0 {
		cfg.LoadBandwidthBytes = c.Load.BandwidthMB << 20
	}
	if c.Load.SingleBandwidthMB > 0 {
		cfg.LoadSingleBandwidthBytes = c.Load.SingleBandwidthMB << 20
	}
	if c.Load.BaseMs > 0 {
		cfg.LoadBaseLatency = time.Duration(c.Load.BaseMs) * time.Millisecond
	}
	if c.Load.JitterSigma > 0 {
		cfg.LoadJitterSigma = c.Load.JitterSigma
	}
	if c.Load.RefLatencyMs > 0 {
		cfg.LoadRefLatency = time.Duration(c.Load.RefLatencyMs * float64(time.Millisecond))
		cfg.LoadRefBytes = c.Load.RefBytes
		cfg.LoadSizeExponent = c.Load.SizeExponent
	}
	return cfg
}

func loadCases(t *testing.T) []Case {
	dir := os.Getenv("MILVUS_SIM_CASES")
	if dir == "" {
		dir = "testdata/cases"
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	require.NoError(t, err)
	filter := os.Getenv("MILVUS_SIM_CASE")
	var cases []Case
	for _, f := range files {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		var c Case
		require.NoError(t, json.Unmarshal(raw, &c), f)
		if c.Name == "" {
			c.Name = strings.TrimSuffix(filepath.Base(f), ".json")
		}
		if filter != "" && !strings.Contains(c.Name, filter) {
			continue
		}
		cases = append(cases, c)
	}
	return cases
}

// TestSimCases replays production-derived cases from testdata/cases (or
// MILVUS_SIM_CASES); MILVUS_SIM_CASE filters by name substring.
func TestSimCases(t *testing.T) {
	requireSim(t)
	for _, c := range loadCases(t) {
		t.Run(c.Name, func(t *testing.T) { runCase(t, c) })
	}
}

// timeScale is the extra compression of the replay (1 = none).
func (c *Case) timeScale() float64 { return max(1, c.TimeScale) }

// scaledParams shrinks QueryCoord's intervals, task timeouts and cooldown
// by the time scale, under the case's own params.
func (c *Case) scaledParams() map[string]string {
	k := c.timeScale()
	p := &paramtable.Get().QueryCoordCfg
	ms := func(base, floor float64) string { return fmt.Sprintf("%.0f", max(floor, base/k)) }
	out := map[string]string{}
	if k > 1 {
		out[p.SegmentCheckInterval.Key] = ms(300, 50)
		out[p.ChannelCheckInterval.Key] = ms(300, 50)
		out[p.BalanceCheckInterval.Key] = ms(100, 30)
		out[p.AutoBalanceInterval.Key] = ms(300, 50)
		out[p.DistPullInterval.Key] = ms(50, 20)
		out[p.CollectionObserverInterval.Key] = ms(50, 20)
		out[p.SegmentTaskTimeout.Key] = ms(6000, 1000)
		out[p.SegmentTaskRetryTimeout.Key] = ms(30000, 2000)
		// Node bandwidth is scaled up by 10k (scaleNodeConfig), so the
		// pacer's starting bandwidth, a production rate, scales the same.
		out[p.LoadBandwidthPerCoreMBps.Key] = fmt.Sprintf("%.0f", 32*harnessSpeedup*k)
		out[p.ChannelTaskTimeout.Key] = ms(6000, 1000)
		out[p.ChannelTaskRetryTimeout.Key] = ms(30000, 2000)
		out["queryCoord.twoLevel.cooldownSeconds"] = fmt.Sprintf("%.1f", 60/k)
	} else {
		// A replay at real time (production cases): the production timeouts
		// and bandwidth as they are, not the harness's 10x-compressed ones.
		out[p.SegmentTaskTimeout.Key] = "60000"
		out[p.SegmentTaskRetryTimeout.Key] = "300000"
		out[p.ChannelTaskTimeout.Key] = "60000"
		out[p.ChannelTaskRetryTimeout.Key] = "300000"
		out[p.LoadBandwidthPerCoreMBps.Key] = "32"
		out["queryCoord.twoLevel.cooldownSeconds"] = "600"
	}
	for key, value := range c.Params {
		out[key] = value
	}
	return out
}

// harnessSpeedup is how much faster than production the harness world runs:
// its intervals and task timeouts are a tenth of production's. A compressed
// case (TimeScale > 1) runs another k times faster, so its load speeds, given
// in production units, are scaled by 10k; a real-time replay (TimeScale 1)
// keeps production timeouts (scaledParams) and production load speeds.
const harnessSpeedup = 10

func (c *Case) scaleNodeConfig(cfg NodeConfig) NodeConfig {
	k := c.timeScale()
	if k <= 1 {
		return cfg
	}
	s := harnessSpeedup * k
	cfg.LoadBandwidthBytes = int64(float64(cfg.LoadBandwidthBytes) * s)
	cfg.LoadSingleBandwidthBytes = int64(float64(cfg.LoadSingleBandwidthBytes) * s)
	cfg.LoadBaseLatency = time.Duration(float64(cfg.LoadBaseLatency) / s)
	cfg.LoadRefLatency = time.Duration(float64(cfg.LoadRefLatency) / s)
	cfg.WatchLatency = time.Duration(float64(cfg.WatchLatency) / k)
	return cfg
}

func runCase(t *testing.T, c Case) {
	opts := Options{Params: c.scaledParams()}
	if c.Streaming {
		opts.Streaming = &StreamingOptions{PChannels: 16, WALReassignDelay: 200 * time.Millisecond}
	}
	sim := New(t, opts)
	sim.Fleet.Search = c.Search
	cfg := c.scaleNodeConfig(c.nodeConfig())
	// Production data was loaded long ago: warm up with fast loads, then
	// switch every node to the cluster's measured load profile for the replay.
	warm := cfg
	warm.LoadBandwidthBytes = 100 << 30
	warm.LoadJitterSigma = 0
	warm.LoadRefLatency = 0
	warm.LoadConcurrency = 16
	if c.Streaming {
		sim.AddStreamingNodes(snBase, max(1, c.SNs), warm, true)
	}
	sim.AddNodes(qnBase, c.QNs, warm)
	for _, rg := range c.ResourceGroups {
		sim.CreateResourceGroup(rg.Name, rg.Nodes)
	}

	type turnover struct {
		flushEvery, compactEvery float64
		flushRows                int64
		replicas                 int32
		rgs                      []string
	}
	// Loaded tables and their turnover, shared with the ingestion loop.
	var (
		tmu      sync.Mutex
		ids      []int64
		turn     []turnover
		released []int64
		relTurn  []turnover
	)
	next := int64(1)
	loadSpec := func(spec tableSpec) {
		sim.Broker.RowsSigma = spec.RowsSigma
		defer func() { sim.Broker.RowsSigma = 0 }()
		switch {
		case c.MaxSegmentRows > 0:
			sim.Broker.MaxSegmentRows = c.MaxSegmentRows
		case c.MaxSegmentRows == 0:
			sim.Broker.MaxSegmentRows = 2097152
		}
		tv := turnover{flushEvery: spec.FlushEverySeconds, compactEvery: spec.CompactEverySeconds, flushRows: spec.FlushRows, replicas: max(1, spec.Replicas)}
		if tv.flushEvery == 0 {
			tv.flushEvery = c.Ingest.FlushEverySeconds
		}
		if tv.compactEvery == 0 {
			tv.compactEvery = c.Ingest.CompactEverySeconds
		}
		if tv.flushRows == 0 {
			tv.flushRows = max(1, spec.RowsMedian/10)
		}
		rgs := spec.RGs
		if len(rgs) == 0 {
			rgs = []string{meta.DefaultResourceGroupName}
		}
		tv.rgs = rgs
		for i := 0; i < spec.Count; i++ {
			sim.LoadCollectionInRGs(next, rgs, tv.replicas, max(1, spec.Channels), spec.SegmentsPerChannel, max(1, spec.RowsMedian))
			tmu.Lock()
			ids = append(ids, next)
			turn = append(turn, tv)
			tmu.Unlock()
			next++
		}
	}
	for _, spec := range c.Collections {
		loadSpec(spec)
	}
	loadStart := time.Now()
	loadRec := sim.StartRecorder(time.Second)
	before0 := sim.FleetCounters()
	if !sim.WaitUntil(10*time.Minute, func() bool { return sim.Loaded(ids...) }) {
		// Loads that outlast SegmentTaskTimeout are killed and rebuilt
		// forever; report how stuck the load is instead of only failing.
		smp := sim.Observe()
		m := map[string]float64{
			"initial_load_stuck":        1,
			"stuck_missing_segments":    float64(smp.MissingSegments),
			"initial_load_wait_seconds": time.Since(loadStart).Seconds(),
		}
		merge(m, "delta_", delta(sim.FleetCounters(), before0))
		merge(m, "", Summary(loadRec.Samples()))
		sim.Report(Result{Scenario: "case_" + c.Name, Metrics: m})
		t.Errorf("initial load did not finish: %d (replica, segment) still missing", smp.MissingSegments)
		return
	}
	loadTime := time.Since(loadStart)
	_, ok := waitSettled(sim, 5*time.Minute, 3*time.Second)
	require.True(t, ok, "initial load did not settle")

	sim.Fleet.SetNodeConfigAll(cfg)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	hasTurnover := c.Ingest.RowsPerSecPerChannel > 0
	for _, tv := range turn {
		hasTurnover = hasTurnover || tv.flushEvery > 0 || tv.compactEvery > 0
	}
	for _, e := range c.Events {
		hasTurnover = hasTurnover || e.Op == "load_tables"
	}
	// rate scales ingestion: rows, seals and compactions all run at this
	// multiple of the case's rate (ingest_rate events; 0 pauses).
	var rate atomic.Value
	rate.Store(1.0)
	k := c.timeScale()
	if hasTurnover {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const tick = 100 * time.Millisecond
			// Collections seal and compact on staggered schedules so the
			// cluster-wide rate is steady, as in production. Schedules are
			// kept in "ingest time", which advances at rate x time scale.
			secs := func(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }
			nextFlush := map[int64]time.Duration{}
			nextCompact := map[int64]time.Duration{}
			var clock time.Duration // ingest time elapsed
			var carry float64
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				r := rate.Load().(float64) * k
				clock += time.Duration(float64(tick) * r)
				// Accumulate fractional rows: low-rate clusters ingest less
				// than one row per tick per channel.
				carry += float64(c.Ingest.RowsPerSecPerChannel) * tick.Seconds() * r
				perTick := int64(carry)
				carry -= float64(perTick)
				tmu.Lock()
				cur := append([]int64(nil), ids...)
				tv := append([]turnover(nil), turn...)
				tmu.Unlock()
				for i, id := range cur {
					if _, ok := nextFlush[id]; !ok {
						frac := float64(i+1) / float64(len(cur))
						nextFlush[id] = clock + secs(tv[i].flushEvery*frac)
						nextCompact[id] = clock + secs(tv[i].compactEvery*frac)
					}
					if perTick > 0 {
						sim.Broker.Ingest(id, perTick)
					}
					if t := tv[i]; t.flushEvery > 0 && clock >= nextFlush[id] {
						// Production sees these as segment_grow; a seal always
						// yields a segment even when few rows arrived.
						if len(sim.Broker.FlushGrowing(id)) == 0 {
							sim.Broker.Flush(id, t.flushRows)
						}
						nextFlush[id] += secs(t.flushEvery)
					}
					if t := tv[i]; t.compactEvery > 0 && clock >= nextCompact[id] {
						sim.Broker.Compact(id, 4)
						nextCompact[id] += secs(t.compactEvery)
					}
				}
			}
		}()
	}

	rec := sim.StartRecorder(200 * time.Millisecond)
	sim.Fleet.Stats.ResetPeaks()
	before := sim.FleetCounters()
	at := rec.Mark("replay")
	phase := sim.StartPhase()
	start := time.Now()

	events := append([]CaseEvent(nil), c.Events...)
	sort.SliceStable(events, func(i, j int) bool { return events[i].At < events[j].At })
	nextQN := qnBase + int64(c.QNs)
	nextSN := snBase + int64(max(1, c.SNs))
	type mark struct {
		name string
		at   time.Duration
	}
	var disruptions []mark
	var (
		drainWG       sync.WaitGroup
		drainMu       sync.Mutex
		maxDrain      time.Duration
		drainTimeouts int
	)
	for _, e := range events {
		if d := time.Duration(e.At/k*float64(time.Second)) - time.Since(start); d > 0 {
			time.Sleep(d)
		}
		label := fmt.Sprintf("%s@%.0fs", e.Op, e.At)
		rec.Mark(label)
		switch e.Op {
		case "add_qn":
			n := max(1, e.Count)
			ncfg := cfg
			if e.MemoryGB > 0 {
				ncfg.MemoryBytes = int64(e.MemoryGB) << 30
			}
			sim.AddNodes(nextQN, n, ncfg)
			nextQN += int64(n)
			if e.RG != "" {
				sim.ResizeResourceGroup(e.RG, n)
			}
		case "rolling_replace":
			// Oldest live nodes first, each replaced before the next.
			live := sim.liveNodeIDs()
			for i := 0; i < min(e.Count, len(live)); i++ {
				t0 := time.Now()
				ok := sim.StopNode(live[i], 2*time.Minute)
				drainMu.Lock()
				if !ok {
					drainTimeouts++
				}
				maxDrain = max(maxDrain, time.Since(t0))
				drainMu.Unlock()
				sim.AddNodes(nextQN, 1, cfg)
				nextQN++
				time.Sleep(time.Duration(e.GapSeconds * float64(time.Second)))
			}
		case "ingest_rate":
			rate.Store(e.Multiplier)
		case "load_tables":
			if e.Tables != nil {
				loadSpec(*e.Tables)
			}
		case "release_tables":
			tmu.Lock()
			n := min(max(1, e.Count), len(ids))
			rel := append([]int64(nil), ids[len(ids)-n:]...)
			relTv := append([]turnover(nil), turn[len(turn)-n:]...)
			ids, turn = ids[:len(ids)-n], turn[:len(turn)-n]
			released, relTurn = append(released, rel...), append(relTurn, relTv...)
			tmu.Unlock()
			for _, id := range rel {
				sim.ReleaseCollection(id)
			}
		case "reload_tables":
			tmu.Lock()
			n := min(max(1, e.Count), len(released))
			back := append([]int64(nil), released[len(released)-n:]...)
			backTv := append([]turnover(nil), relTurn[len(relTurn)-n:]...)
			released, relTurn = released[:len(released)-n], relTurn[:len(relTurn)-n]
			tmu.Unlock()
			for i, id := range back {
				sim.loadInRGs(id, backTv[i].rgs, backTv[i].replicas)
			}
			tmu.Lock()
			ids, turn = append(ids, back...), append(turn, backTv...)
			tmu.Unlock()
		case "kill_qn":
			sim.KillNode(e.Node)
			disruptions = append(disruptions, mark{label, time.Since(start)})
		case "restart_qn":
			sim.RestartNode(e.Node)
		case "stop_qn":
			nodes := []int64{e.Node}
			if e.Node == 0 && e.Count > 0 {
				// Scale in: the newest nodes leave.
				live := sim.liveNodeIDs()
				nodes = live[max(0, len(live)-e.Count):]
			}
			for i, node := range nodes {
				// Scaling a group in: the group shrinks first and the node
				// it releases is the one that stops, as an operator would.
				if rg := sim.NodeResourceGroup(node); rg != "" && rg != meta.DefaultResourceGroupName {
					before := sim.RGNodes(rg)
					sim.ResizeResourceGroup(rg, -1)
					after := lo.SliceToMap(sim.RGNodes(rg), func(id int64) (int64, bool) { return id, true })
					if evicted, ok := lo.Find(before, func(id int64) bool { return !after[id] }); ok {
						node = evicted
						nodes[i] = evicted
					}
				}
				drainWG.Add(1)
				go func(id int64) {
					defer drainWG.Done()
					t0 := time.Now()
					ok := sim.StopNode(id, 2*time.Minute)
					drainMu.Lock()
					defer drainMu.Unlock()
					maxDrain = max(maxDrain, time.Since(t0))
					if !ok {
						drainTimeouts++
					}
				}(node)
			}
			disruptions = append(disruptions, mark{label, time.Since(start)})
		case "add_sn":
			n := max(1, e.Count)
			sim.AddStreamingNodes(nextSN, n, cfg, true)
			nextSN += int64(n)
		case "kill_sn":
			sim.KillStreamingNode(e.Node)
			disruptions = append(disruptions, mark{label, time.Since(start)})
		case "restart_qc":
			sim.RestartQueryCoord()
			disruptions = append(disruptions, mark{label, time.Since(start)})
		case "slow_qn":
			sim.Fleet.SetSlowdown(e.Node, max(1, e.Slowdown))
		default:
			t.Fatalf("unknown event op %q", e.Op)
		}
	}
	if d := time.Duration(c.DurationSeconds/k*float64(time.Second)) - time.Since(start); d > 0 {
		time.Sleep(d)
	}
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)
	// Drains still running count against the replay; wait for their verdict.
	drainWG.Wait()

	m := map[string]float64{
		"drain_timeouts":       float64(drainTimeouts),
		"max_drain_seconds":    maxDrain.Seconds(),
		"initial_load_seconds": loadTime.Seconds(),
		"replay_seconds":       elapsed.Seconds(),
	}
	mv := phase.Analyze()
	m["existing_segment_moves_per_real_hour"] = float64(mv.SegmentLoads) / elapsed.Seconds() * 3600 / 10
	if mv.NewSegmentLoads > 0 {
		m["moves_per_new_segment"] = float64(mv.SegmentLoads) / float64(mv.NewSegmentLoads)
	}
	// Recovery after each disruption: time until every segment and channel
	// is served again.
	samples := rec.Since(at)
	var worst, worstSteady float64
	for i, d := range disruptions {
		r := FirstTime(samples, d.at+200*time.Millisecond, func(s Sample) bool {
			return s.MissingSegments == 0 && s.LeaderlessChannels == 0
		})
		m[fmt.Sprintf("recovery_seconds_%d_%s", i, d.name)] = seconds(r)
		// Under steady ingest there is nearly always a sealed segment that
		// has entered the target and not finished loading, so "nothing
		// missing" can take until the next release of tables (day_01 kill_qn:
		// 494s in two runs, 54s in three). Also measure recovery against the
		// churn seen in the minute before the disruption.
		missBase, leaderBase := steadyMissing(samples, d.at)
		rs := FirstTime(samples, d.at+200*time.Millisecond, func(s Sample) bool {
			return s.MissingSegments <= missBase && s.LeaderlessChannels <= leaderBase
		})
		m[fmt.Sprintf("recovery_steady_seconds_%d_%s", i, d.name)] = seconds(rs)
		if rs < 0 {
			worstSteady = -1
		} else if worstSteady >= 0 {
			worstSteady = max(worstSteady, rs.Seconds())
		}
		if os.Getenv("MILVUS_SIM_DEBUG_RECOVERY") != "" {
			for _, s := range samples {
				if s.At >= d.at && s.At <= d.at+30*time.Second && (s.MissingSegments > 0 || s.LeaderlessChannels > 0) {
					t.Logf("recovery-debug %s t+%.1fs missing=%d leaderless=%d", d.name, (s.At - d.at).Seconds(), s.MissingSegments, s.LeaderlessChannels)
				}
			}
		}
		if r < 0 {
			worst = -1
		} else if worst >= 0 {
			worst = max(worst, r.Seconds())
		}
	}
	if len(disruptions) > 0 {
		m["worst_recovery_seconds"] = worst
		m["worst_recovery_steady_seconds"] = worstSteady
	}
	merge(m, "delta_", delta(sim.FleetCounters(), before))
	reportPhase(sim, "case_"+c.Name, m, phase, rec, at)
	_ = sim.Fleet.Stats.LoadsToStopping.Load()
	var excess []string
	for _, smp := range samples {
		if smp.Excess != "" && len(excess) < 5 {
			excess = append(excess, fmt.Sprintf("t=%.1fs %s", smp.At.Seconds(), smp.Excess))
		}
	}
	require.LessOrEqual(t, Summary(samples)["max_replica_copies"], 2.0, "a segment had more than two live copies in one replica: %v", excess)
}

// steadyMissing is the churn before a disruption: the 90th percentile of
// missing segments and leaderless channels over the minute before at.
func steadyMissing(samples []Sample, at time.Duration) (int, int) {
	var miss, lead []int
	for _, s := range samples {
		if s.At >= at-time.Minute && s.At < at {
			miss = append(miss, s.MissingSegments)
			lead = append(lead, s.LeaderlessChannels)
		}
	}
	p90 := func(xs []int) int {
		if len(xs) == 0 {
			return 0
		}
		sort.Ints(xs)
		return xs[len(xs)*9/10]
	}
	return p90(miss), p90(lead)
}
