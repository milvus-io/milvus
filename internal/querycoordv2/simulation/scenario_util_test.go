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
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Scenario tests take tens of seconds each; run them with MILVUS_SIM=1.
// MILVUS_SIM_SCALE multiplies cluster sizes (default 1).
func requireSim(t *testing.T) {
	if os.Getenv("MILVUS_SIM") == "" {
		t.Skip("set MILVUS_SIM=1 to run balance simulations")
	}
}

func scaled(n int) int {
	if v, err := strconv.ParseFloat(os.Getenv("MILVUS_SIM_SCALE"), 64); err == nil && v > 0 {
		return max(1, int(float64(n)*v))
	}
	return n
}

func collectionIDs(first int64, count int) []int64 {
	ids := make([]int64, count)
	for i := range ids {
		ids[i] = first + int64(i)
	}
	return ids
}

// loadAll loads every collection and waits until all are Loaded.
func loadAll(t *testing.T, sim *Sim, ids []int64, replicas int32, channels, segmentsPerChannel int, rows int64, timeout time.Duration) time.Duration {
	start := time.Now()
	for _, id := range ids {
		sim.LoadCollection(id, replicas, channels, segmentsPerChannel, rows)
	}
	require.True(t, sim.WaitUntil(timeout, func() bool { return sim.Loaded(ids...) }), "collections did not load in %s", timeout)
	return time.Since(start)
}

// settled holds when nothing is missing, every channel has a leader and the
// scheduler is idle.
func settled(s Sample) bool {
	return s.MissingSegments == 0 && s.LeaderlessChannels == 0 && s.SegmentTasks == 0 && s.ChannelTasks == 0
}

// waitSettled waits until settled holds for `stable` consecutive time.
func waitSettled(sim *Sim, timeout, stable time.Duration) (time.Duration, bool) {
	start := time.Now()
	var since time.Time
	ok := sim.WaitUntil(timeout, func() bool {
		if !settled(sim.Observe()) {
			since = time.Time{}
			return false
		}
		if since.IsZero() {
			since = time.Now()
		}
		return time.Since(since) >= stable
	})
	return time.Since(start) - stable, ok
}

// waitBalanced waits until the row coefficient of variation is at most cv
// and the cluster is settled.
func waitBalanced(sim *Sim, cvLimit float64, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	ok := sim.WaitUntil(timeout, func() bool {
		s := sim.Observe()
		return settled(s) && s.RowCV <= cvLimit
	})
	return time.Since(start), ok
}

// waitRowBalanced is waitBalanced for ingestion scenarios: new segments keep
// tasks pending, so only require full coverage and a row CV bound.
func waitRowBalanced(sim *Sim, cvLimit float64, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	ok := sim.WaitUntil(timeout, func() bool {
		s := sim.Observe()
		return s.MissingSegments == 0 && s.LeaderlessChannels == 0 && s.RowCV <= cvLimit
	})
	return time.Since(start), ok
}

// flusher keeps flushing (and optionally compacting) collections until stop.
type flusher struct {
	stop chan struct{}
	wg   sync.WaitGroup
}

func startFlusher(sim *Sim, ids []int64, every time.Duration, rows int64, compactEvery int) *flusher {
	f := &flusher{stop: make(chan struct{})}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		round := 0
		for {
			select {
			case <-f.stop:
				return
			case <-ticker.C:
			}
			round++
			for _, id := range ids {
				sim.Broker.Flush(id, rows)
				if compactEvery > 0 && round%compactEvery == 0 {
					sim.Broker.Compact(id, 3)
				}
			}
		}
	}()
	return f
}

func (f *flusher) Stop() {
	close(f.stop)
	f.wg.Wait()
}

func delta(after, before map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(after))
	for k, v := range after {
		if strings.HasPrefix(k, "max_") {
			out[k] = v // peaks are not counters
			continue
		}
		out[k] = v - before[k]
	}
	if out["queued_loads"] > 0 {
		out["mean_queue_wait_seconds"] = out["queue_wait_seconds_total"] / out["queued_loads"]
	}
	return out
}

func seconds(d time.Duration) float64 {
	if d < 0 {
		return -1
	}
	return d.Seconds()
}

// merge copies src into dst with a key prefix.
func merge(dst map[string]float64, prefix string, src map[string]float64) {
	for k, v := range src {
		dst[prefix+k] = v
	}
}
