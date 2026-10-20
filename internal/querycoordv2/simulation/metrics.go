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
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Sample is one observation of the cluster from QueryNode ground truth.
type Sample struct {
	At time.Duration
	// MissingSegments counts (replica, segment) pairs of loaded collections
	// whose Current-target segment is on no live node of that replica.
	MissingSegments int
	// LeaderlessChannels counts (replica, channel) pairs with no watched
	// delegator on a live node of that replica.
	LeaderlessChannels int
	// MaxReplicaCopies is the largest number of live copies of one segment
	// inside one replica (2 is a Move in progress; more is a violation).
	MaxReplicaCopies int
	// RowCV and SegmentCV are coefficients of variation across live,
	// non-stopping nodes, per resource group averaged by node count.
	RowCV, SegmentCV float64
	SegmentTasks     int
	ChannelTasks     int
	// Excess describes the first (replica, segment) with more than two live
	// copies, for diagnosis.
	Excess string
	// DelegatorsOffWAL counts watched delegators not on the node that hosts
	// their pchannel's WAL (streaming mode only).
	DelegatorsOffWAL int
	// NodeRows is the sealed rows per live node.
	NodeRows map[int64]int64
	// SearchMs is the modeled search latency of every loaded (collection,
	// replica); empty when the search model is off.
	SearchMs []float64
	// Loading is the number of in-flight segment loads cluster-wide.
	Loading int
}

// Recorder samples the simulated cluster on a fixed period.
type Recorder struct {
	sim    *Sim
	start  time.Time
	stop   chan struct{}
	done   chan struct{}
	mu     sync.Mutex
	marks  []Mark
	sample []Sample
}

type Mark struct {
	At    time.Duration
	Label string
}

func (s *Sim) StartRecorder(period time.Duration) *Recorder {
	r := &Recorder{sim: s, start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-ticker.C:
				smp := s.Observe()
				smp.At = time.Since(r.start)
				r.mu.Lock()
				r.sample = append(r.sample, smp)
				r.mu.Unlock()
			}
		}
	}()
	s.t.Cleanup(r.Stop)
	return r
}

func (r *Recorder) Stop() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	<-r.done
}

// Mark records a scenario event; the returned offset can bound later queries.
func (r *Recorder) Mark(label string) time.Duration {
	at := time.Since(r.start)
	r.mu.Lock()
	r.marks = append(r.marks, Mark{At: at, Label: label})
	r.mu.Unlock()
	r.sim.tr.mark(label)
	return at
}

func (r *Recorder) Samples() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Sample(nil), r.sample...)
}

// Since returns the samples taken at or after offset.
func (r *Recorder) Since(offset time.Duration) []Sample {
	var out []Sample
	for _, s := range r.Samples() {
		if s.At >= offset {
			out = append(out, s)
		}
	}
	return out
}

// Observe computes one Sample from QueryNode ground truth and QueryCoord
// replica membership.
func (s *Sim) Observe() Sample {
	qc := s.current()
	views := s.Fleet.Snapshot()
	var smp Sample
	smp.SegmentTasks, smp.ChannelTasks = qc.sched.GetSegmentTaskNum(), qc.sched.GetChannelTaskNum()
	smp.NodeRows = make(map[int64]int64, len(views))
	for node, v := range views {
		if !v.Up {
			continue
		}
		smp.Loading += v.Loading
		var rows int64
		for _, n := range v.Segments {
			rows += n
		}
		smp.NodeRows[node] = rows
	}

	for _, collectionID := range qc.meta.GetAll(qc.ctx) {
		collection := qc.meta.GetCollection(qc.ctx, collectionID)
		loaded := collection != nil && collection.GetStatus().String() == "Loaded"
		target := qc.targetMgr.GetSealedSegmentsByCollection(qc.ctx, collectionID, meta.CurrentTarget)
		channels := qc.targetMgr.GetDmChannelsByCollection(qc.ctx, collectionID, meta.CurrentTarget)
		for _, replica := range qc.meta.GetByCollection(qc.ctx, collectionID) {
			members := typeutil.NewUniqueSet(replica.GetNodes()...)
			members.Insert(replica.GetRONodes()...)
			copies := make(map[int64]int)   // live copies on any member: availability
			rwCopies := make(map[int64]int) // live copies on RW members: the two-copies rule
			holders := make(map[int64][]int64)
			watched := make(map[string]bool)
			// Copies on the replica's RO nodes are leaving (a drain, or a node
			// handed to another replica): they are not counted against the
			// two-copies rule, which is about the RW nodes that will keep them.
			ro := typeutil.NewUniqueSet(replica.GetRONodes()...)
			for node := range members {
				v, ok := views[node]
				if !ok || !v.Up {
					continue
				}
				for id, coll := range v.Collection {
					if coll == collectionID {
						copies[id]++
						if !ro.Contain(node) {
							rwCopies[id]++
						}
						holders[id] = append(holders[id], node)
					}
				}
				for ch, d := range v.Delegators {
					if d.Collection == collectionID && d.Watched {
						watched[ch] = true
					}
				}
			}
			// A delegator is off its WAL when the replica includes the WAL
			// node but the delegator lives elsewhere (only one replica can
			// share the WAL node).
			if s.wal != nil {
				sq := typeutil.NewUniqueSet(replica.GetRWSQNodes()...)
				for node := range members {
					v, ok := views[node]
					if !ok || !v.Up {
						continue
					}
					for ch, d := range v.Delegators {
						if d.Collection != collectionID || !d.Watched {
							continue
						}
						if loc, ok := s.wal.Located(funcutil.ToPhysicalChannel(ch)); ok && loc != node && sq.Contain(loc) {
							smp.DelegatorsOffWAL++
						}
					}
				}
			}
			for id, n := range rwCopies {
				smp.MaxReplicaCopies = max(smp.MaxReplicaCopies, n)
				if n > 2 && smp.Excess == "" {
					smp.Excess = fmt.Sprintf("replica=%d segment=%d nodes=%v rw=%v ro=%v", replica.GetID(), id, holders[id], replica.GetNodes(), replica.GetRONodes())
				}
			}
			if !loaded {
				continue
			}
			if model := s.Fleet.Search; model.enabled() {
				worst := 0.0
				for node := range members {
					v, ok := views[node]
					if !ok || !v.Up {
						continue
					}
					var rows int64
					for id, coll := range v.Collection {
						if coll == collectionID {
							rows += v.Segments[id]
						}
					}
					if rows > 0 {
						worst = max(worst, model.nodeMs(rows, v.Loading, v.CPUNum))
					}
				}
				smp.SearchMs = append(smp.SearchMs, worst)
			}
			for id := range target {
				if copies[id] == 0 {
					smp.MissingSegments++
				}
			}
			for ch := range channels {
				if !watched[ch] {
					smp.LeaderlessChannels++
				}
			}
		}
	}

	// Balance quality: rows/segments across live, non-stopping members of
	// each resource group.
	var rowCV, segCV, weight float64
	for _, rg := range qc.meta.ResourceManager.ListResourceGroups(qc.ctx) {
		nodes, err := qc.meta.ResourceManager.GetNodes(qc.ctx, rg)
		if err != nil {
			continue
		}
		var rows, segs []float64
		for _, node := range nodes {
			v, ok := views[node]
			if !ok || !v.Up || s.Fleet.IsStopping(node) {
				continue
			}
			var r float64
			for _, n := range v.Segments {
				r += float64(n)
			}
			rows = append(rows, r)
			segs = append(segs, float64(len(v.Segments)))
		}
		if len(rows) < 2 {
			continue
		}
		rowCV += cv(rows) * float64(len(rows))
		segCV += cv(segs) * float64(len(segs))
		weight += float64(len(rows))
	}
	if weight > 0 {
		smp.RowCV, smp.SegmentCV = rowCV/weight, segCV/weight
	}
	return smp
}

// RGRowCV is the row coefficient of variation across live, non-stopping
// nodes of one resource group.
func (s *Sim) RGRowCV(rg string) float64 {
	views := s.Fleet.Snapshot()
	var rows []float64
	for _, node := range s.RGNodes(rg) {
		v, ok := views[node]
		if !ok || !v.Up || s.Fleet.IsStopping(node) {
			continue
		}
		var r float64
		for _, n := range v.Segments {
			r += float64(n)
		}
		rows = append(rows, r)
	}
	if len(rows) < 2 {
		return 0
	}
	return cv(rows)
}

func cv(values []float64) float64 {
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	if mean == 0 {
		return 0
	}
	var variance float64
	for _, v := range values {
		variance += (v - mean) * (v - mean)
	}
	return math.Sqrt(variance/float64(len(values))) / mean
}

// Result is the per-scenario report row, comparable across branches.
type Result struct {
	Scenario string             `json:"scenario"`
	Branch   string             `json:"branch,omitempty"`
	Metrics  map[string]float64 `json:"metrics"`
	Notes    []string           `json:"notes,omitempty"`
}

// Report prints the result and, when MILVUS_SIM_REPORT names a file,
// appends it as one JSON line.
func (s *Sim) Report(res Result) {
	if res.Branch == "" {
		res.Branch = os.Getenv("MILVUS_SIM_BRANCH")
	}
	keys := make([]string, 0, len(res.Metrics))
	for k := range res.Metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "simulation %s:", res.Scenario)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%.3f", k, res.Metrics[k])
	}
	s.t.Log(b.String())
	path := os.Getenv("MILVUS_SIM_REPORT")
	if path == "" {
		return
	}
	line, err := json.Marshal(res)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		s.t.Logf("cannot write simulation report: %v", err)
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

// FleetCounters snapshots the QueryNode-side counters.
func (s *Sim) FleetCounters() map[string]float64 {
	st := &s.Fleet.Stats
	return map[string]float64{
		"loads_succeeded":           float64(st.LoadsSucceeded.Load()),
		"loads_failed":              float64(st.LoadsFailed.Load()),
		"loads_rejected_oom":        float64(st.LoadsRejectedOOM.Load()),
		"loads_rejected_threshold":  float64(st.LoadsRejectedThreshold.Load()),
		"loads_without_binlog_size": float64(st.LoadsWithoutBinlogSize.Load()),
		"releases":                  float64(st.Releases.Load()),
		"watches":                   float64(st.Watches.Load()),
		"redundant_watch_rpcs":      float64(st.RedundantWatchRPCs.Load()),
		"unsubs":                    float64(st.Unsubs.Load()),
		"loads_to_stopping":         float64(st.LoadsToStopping.Load()),
		"max_node_loading":          float64(st.MaxNodeLoading.Load()),
		"max_node_queued":           float64(st.MaxNodeQueued.Load()),
		"max_queue_wait_seconds":    float64(st.MaxQueueWait.Load()) / 1e9,
		"queue_wait_seconds_total":  float64(st.QueueWaitNanos.Load()) / 1e9,
		"queued_loads":              float64(st.QueuedLoads.Load()),
		"loads_canceled":            float64(st.LoadsCanceled.Load()),
		"watches_failed":            float64(st.WatchesFailed.Load()),
	}
}

// ProcessCPUSeconds is the user+system CPU time of the whole test process
// (QueryCoord plus the simulated nodes).
func ProcessCPUSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

// Summary aggregates samples into scenario metrics.
func Summary(samples []Sample) map[string]float64 {
	m := map[string]float64{}
	if len(samples) == 0 {
		return m
	}
	var missing, leaderless, copies int
	for _, s := range samples {
		missing += s.MissingSegments
		leaderless += s.LeaderlessChannels
		copies = max(copies, s.MaxReplicaCopies)
	}
	last := samples[len(samples)-1]
	m["missing_segment_samples"] = float64(missing)
	m["leaderless_channel_samples"] = float64(leaderless)
	m["max_replica_copies"] = float64(copies)
	m["final_row_cv"] = last.RowCV
	m["final_segment_cv"] = last.SegmentCV
	m["final_delegators_off_wal"] = float64(last.DelegatorsOffWAL)
	searchSummary(samples, m)
	return m
}

// searchSummary reports modeled search latency over all samples and split
// by whether any segment load was in flight.
func searchSummary(samples []Sample, m map[string]float64) {
	var all, idle, busy []float64
	for _, s := range samples {
		for _, ms := range s.SearchMs {
			all = append(all, ms)
			if s.Loading > 0 {
				busy = append(busy, ms)
			} else {
				idle = append(idle, ms)
			}
		}
	}
	if len(all) == 0 {
		return
	}
	m["search_p50_ms"] = quantile(all, 0.5)
	m["search_p99_ms"] = quantile(all, 0.99)
	if len(idle) > 0 {
		m["search_idle_p99_ms"] = quantile(idle, 0.99)
	}
	if len(busy) > 0 {
		m["search_loading_p99_ms"] = quantile(busy, 0.99)
		m["loading_sample_fraction"] = float64(len(busy)) / float64(len(all))
	}
}

func quantile(xs []float64, q float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(q * float64(len(s)-1))
	return s[i]
}

// FirstTime returns the offset of the first sample satisfying cond (from
// offset on), or -1.
func FirstTime(samples []Sample, offset time.Duration, cond func(Sample) bool) time.Duration {
	for _, s := range samples {
		if s.At >= offset && cond(s) {
			return s.At - offset
		}
	}
	return -1
}
