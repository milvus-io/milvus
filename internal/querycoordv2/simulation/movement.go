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
	"strings"
	"time"
)

// placementKey identifies one replica's copy of an object (segment or channel).
type placementKey struct {
	replica int64
	segment int64
	channel string
}

// Phase captures cluster placement at a point in time so data movement after
// it can be judged: how much moved, how much moved more than once, and how
// much moved back to where it had been.
type Phase struct {
	sim   *Sim
	start time.Time
	// holders is the initial set of nodes per replica copy.
	holders map[placementKey]map[int64]bool
	// membership maps (collection, node) to replica, at start and on Analyze.
	membership map[[2]int64]int64
	// requiredRows is the minimum number of rows that must move for every
	// replica to reach equal rows per segment-hosting node.
	requiredRows int64
}

func (s *Sim) membership(into map[[2]int64]int64) {
	qc := s.current()
	for _, collectionID := range qc.meta.GetAll(qc.ctx) {
		for _, replica := range qc.meta.GetByCollection(qc.ctx, collectionID) {
			for _, node := range replica.GetNodes() {
				into[[2]int64{collectionID, node}] = replica.GetID()
			}
			for _, node := range replica.GetRONodes() {
				into[[2]int64{collectionID, node}] = replica.GetID()
			}
			for _, node := range replica.GetRWSQNodes() {
				into[[2]int64{collectionID, node}] = replica.GetID()
			}
			for _, node := range replica.GetROSQNodes() {
				into[[2]int64{collectionID, node}] = replica.GetID()
			}
		}
	}
}

// StartPhase snapshots placement. Call it after topology changes have been
// reflected in replica membership.
func (s *Sim) StartPhase() *Phase {
	p := &Phase{sim: s, start: time.Now(), holders: map[placementKey]map[int64]bool{}, membership: map[[2]int64]int64{}}
	s.membership(p.membership)
	qc := s.current()
	views := s.Fleet.Snapshot()
	for node, v := range views {
		if !v.Up {
			continue
		}
		for seg, coll := range v.Collection {
			if r, ok := p.membership[[2]int64{coll, node}]; ok {
				k := placementKey{replica: r, segment: seg}
				if p.holders[k] == nil {
					p.holders[k] = map[int64]bool{}
				}
				p.holders[k][node] = true
			}
		}
		for ch, d := range v.Delegators {
			if r, ok := p.membership[[2]int64{d.Collection, node}]; ok {
				k := placementKey{replica: r, channel: ch}
				if p.holders[k] == nil {
					p.holders[k] = map[int64]bool{}
				}
				p.holders[k][node] = true
			}
		}
	}
	// Minimum rows to move per replica: sum of each node's excess over the
	// replica mean, across its segment-hosting (RW, non-streaming) nodes.
	for _, collectionID := range qc.meta.GetAll(qc.ctx) {
		for _, replica := range qc.meta.GetByCollection(qc.ctx, collectionID) {
			var rows []int64
			var total int64
			for _, node := range replica.GetRWNodes() {
				v, ok := views[node]
				if !ok || !v.Up || s.Fleet.IsStopping(node) {
					continue
				}
				var r int64
				for seg, n := range v.Segments {
					if v.Collection[seg] == collectionID {
						r += n
					}
				}
				rows = append(rows, r)
				total += r
			}
			if len(rows) == 0 {
				continue
			}
			mean := total / int64(len(rows))
			for _, r := range rows {
				if r > mean {
					p.requiredRows += r - mean
				}
			}
		}
	}
	return p
}

// Movement summarizes data movement within a phase.
type Movement struct {
	SegmentLoads     int
	MovedRows        int64
	MovedSegments    int // replica copies loaded at least once
	ReMovedSegments  int // replica copies loaded more than once
	PingPongSegments int // replica copies loaded back onto a node that held them in this phase
	ChannelWatches   int
	ReWatchedChans   int
	PingPongChannels int
	Unattributed     int // events on nodes not in any replica of the collection
	// NewSegmentLoads counts first loads of segments that did not exist at
	// phase start (flush/compaction output); they are not data movement.
	NewSegmentLoads int
	NewSegmentRows  int64
}

func (p *Phase) Analyze() Movement {
	p.sim.membership(p.membership)
	var m Movement
	loads := map[placementKey]int{} // moves (loads beyond a new segment's first)
	fresh := map[placementKey]bool{}
	seen := map[placementKey]map[int64]bool{}
	for k, nodes := range p.holders {
		seen[k] = map[int64]bool{}
		for n := range nodes {
			seen[k][n] = true
		}
	}
	for _, e := range p.sim.Fleet.Stats.Events(p.start) {
		if e.Kind != EventLoad && e.Kind != EventWatch {
			continue
		}
		r, ok := p.membership[[2]int64{e.Collection, e.Node}]
		if !ok {
			m.Unattributed++
			continue
		}
		var k placementKey
		if e.Kind == EventLoad {
			k = placementKey{replica: r, segment: e.Segment}
			if _, existed := p.holders[k]; !existed && !fresh[k] {
				m.NewSegmentLoads++
				m.NewSegmentRows += e.Rows
				fresh[k] = true
				if seen[k] == nil {
					seen[k] = map[int64]bool{}
				}
				seen[k][e.Node] = true
				continue
			}
			m.SegmentLoads++
			m.MovedRows += e.Rows
		} else {
			k = placementKey{replica: r, channel: e.Channel}
			m.ChannelWatches++
		}
		loads[k]++
		if seen[k] == nil {
			seen[k] = map[int64]bool{}
		}
		if seen[k][e.Node] {
			if e.Kind == EventLoad {
				m.PingPongSegments++
			} else {
				m.PingPongChannels++
			}
		}
		seen[k][e.Node] = true
	}
	for k, n := range loads {
		if k.channel == "" {
			m.MovedSegments++
			if n > 1 {
				m.ReMovedSegments++
			}
		} else if n > 1 {
			m.ReWatchedChans++
		}
	}
	return m
}

// Metrics renders the movement plus the move amplification over the minimum.
func (p *Phase) Metrics() map[string]float64 {
	m := p.Analyze()
	out := map[string]float64{
		"segment_loads":         float64(m.SegmentLoads),
		"moved_rows":            float64(m.MovedRows),
		"required_rows":         float64(p.requiredRows),
		"moved_segments":        float64(m.MovedSegments),
		"removed_segments":      float64(m.ReMovedSegments),
		"pingpong_segments":     float64(m.PingPongSegments),
		"channel_watches":       float64(m.ChannelWatches),
		"rewatched_channels":    float64(m.ReWatchedChans),
		"pingpong_channels":     float64(m.PingPongChannels),
		"unattributed_events":   float64(m.Unattributed),
		"new_segment_loads":     float64(m.NewSegmentLoads),
		"new_segment_rows":      float64(m.NewSegmentRows),
		"move_amplification":    -1,
		"phase_elapsed_seconds": time.Since(p.start).Seconds(),
	}
	if p.requiredRows > 0 {
		out["move_amplification"] = float64(m.MovedRows) / float64(p.requiredRows)
	}
	return out
}

// ChannelHistories renders the watch/unsub sequence of up to limit channels
// that were watched more than once in this phase, for diagnosing churn.
func (p *Phase) ChannelHistories(limit int) []string {
	return p.histories(limit, EventWatch, EventUnsub)
}

// SegmentHistories renders the load/release sequence of up to limit segments
// that were loaded more than once in this phase.
func (p *Phase) SegmentHistories(limit int) []string {
	return p.histories(limit, EventLoad, EventRelease)
}

func (p *Phase) histories(limit int, grow, reduce EventKind) []string {
	type step struct {
		at   time.Duration
		grow bool
		node int64
	}
	type key struct {
		collection int64
		segment    int64
		channel    string
	}
	hist := map[key][]step{}
	grows := map[key]int{}
	var order []key
	for _, e := range p.sim.Fleet.Stats.Events(p.start) {
		if e.Kind != grow && e.Kind != reduce {
			continue
		}
		k := key{collection: e.Collection, segment: e.Segment}
		if grow == EventWatch {
			k.channel = e.Channel
		}
		if _, ok := hist[k]; !ok {
			order = append(order, k)
		}
		if e.Kind == grow {
			grows[k]++
		}
		hist[k] = append(hist[k], step{at: e.At.Sub(p.start).Round(time.Millisecond), grow: e.Kind == grow, node: e.Node})
	}
	var out []string
	for _, k := range order {
		if grows[k] < 2 {
			continue
		}
		var b strings.Builder
		if grow == EventWatch {
			fmt.Fprintf(&b, "%s:", k.channel)
		} else {
			fmt.Fprintf(&b, "seg %d (coll %d):", k.segment, k.collection)
		}
		for _, s := range hist[k] {
			kind := "-"
			if s.grow {
				kind = "+"
			}
			fmt.Fprintf(&b, " %s%d@%v", kind, s.node, s.at)
		}
		out = append(out, b.String())
		if len(out) == limit {
			break
		}
	}
	return out
}
