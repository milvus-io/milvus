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

// Package simulation runs the real QueryCoord placement machinery (checkers,
// task scheduler/executor, distribution pulls, target and collection
// observers, balancers) against in-memory QueryNodes and an in-memory
// DataCoord. Time is real but scaled: every QueryCoord interval is shortened
// and simulated loads take milliseconds, so scenarios that span minutes of
// production behavior finish in seconds.
package simulation

// Trace records a run for the viewer (tools/simviz): what every node held
// over time, where every table sat at key moments, every task QueryCoord
// planned (with the balancer's reason) and every load and release the
// QueryNodes carried out. On with MILVUS_SIM_TRACE=<file.json>.

import (
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

const (
	traceSamplePeriod   = 500 * time.Millisecond
	traceKeyframePeriod = 10 * time.Second
	traceTopTables      = 40 // tables shown one by one in a keyframe; the rest are "other"
	traceMaxTasks       = 1000000
)

type Trace struct {
	sim   *Sim
	path  string
	start time.Time

	mu           sync.Mutex
	nodes        []traceNode
	marks        []traceMark
	samples      []traceSample
	keyframes    []traceKeyframe
	tasks        []traceTask
	droppedTasks int
	wantKey      bool
	lastKey      time.Time

	stop chan struct{}
	done chan struct{}
}

type traceNode struct {
	ID      int64   `json:"id"`
	Memory  int64   `json:"mem"`
	CPU     int64   `json:"cpu"`
	AddedAt float64 `json:"added_at"`
}

type traceMark struct {
	At    float64 `json:"t"`
	Label string  `json:"label"`
}

// traceSample is one observation: per node [used bytes, loads in flight,
// segments, up].
type traceSample struct {
	At    float64            `json:"t"`
	Nodes map[int64][4]int64 `json:"n"`
}

// traceKeyframe is the table layout at one moment: per table, per node
// [segments, bytes]. Tables beyond the largest traceTopTables are summed
// under table 0.
type traceKeyframe struct {
	At    float64                      `json:"t"`
	Label string                       `json:"label,omitempty"`
	Colls map[int64]map[int64][2]int64 `json:"colls"`
	// RGs maps up nodes to their resource group, present only when a
	// group other than the default one exists.
	RGs map[int64]string `json:"rg,omitempty"`
}

type traceTask struct {
	At      float64 `json:"t"`
	ID      int64   `json:"id"`
	Kind    string  `json:"kind"` // move, grow, reduce, update, watch, unsub
	Source  string  `json:"src"`
	Coll    int64   `json:"coll"`
	Replica int64   `json:"replica"`
	Segment int64   `json:"seg,omitempty"`
	Channel string  `json:"ch,omitempty"`
	From    int64   `json:"from"`
	To      int64   `json:"to"`
	Reason  string  `json:"reason,omitempty"`
}

type traceEvent struct {
	At      float64 `json:"t"`
	Kind    string  `json:"kind"` // load, release, watch, unsub
	Node    int64   `json:"node"`
	Coll    int64   `json:"coll"`
	Segment int64   `json:"seg,omitempty"`
	Channel string  `json:"ch,omitempty"`
	Rows    int64   `json:"rows,omitempty"`
}

type traceFile struct {
	Case         string          `json:"case"`
	Branch       string          `json:"branch"`
	Params       string          `json:"params"`
	PeriodMs     int64           `json:"period_ms"`
	Nodes        []traceNode     `json:"nodes"`
	Marks        []traceMark     `json:"marks"`
	Samples      []traceSample   `json:"samples"`
	Keyframes    []traceKeyframe `json:"keyframes"`
	Tasks        []traceTask     `json:"tasks"`
	DroppedTasks int             `json:"dropped_tasks,omitempty"`
	Events       []traceEvent    `json:"events"`
}

func startTrace(s *Sim) *Trace {
	path := os.Getenv("MILVUS_SIM_TRACE")
	if path == "" {
		return nil
	}
	tr := &Trace{sim: s, path: path, start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	go tr.loop()
	return tr
}

func (tr *Trace) since() float64 { return time.Since(tr.start).Seconds() }

func (tr *Trace) loop() {
	defer close(tr.done)
	ticker := time.NewTicker(traceSamplePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-tr.stop:
			return
		case <-ticker.C:
			tr.sample("")
		}
	}
}

// sample observes the fleet; a keyframe is taken on the keyframe period, or
// right away with a label (a mark just happened).
func (tr *Trace) sample(label string) {
	views := tr.sim.Fleet.Snapshot()
	at := tr.since()
	smp := traceSample{At: at, Nodes: make(map[int64][4]int64, len(views))}
	for id, v := range views {
		up := int64(0)
		if v.Up {
			up = 1
		}
		smp.Nodes[id] = [4]int64{v.UsedMemory, int64(v.Loading), int64(len(v.Segments)), up}
	}
	tr.mu.Lock()
	tr.samples = append(tr.samples, smp)
	key := label != "" || tr.wantKey || time.Since(tr.lastKey) >= traceKeyframePeriod
	tr.wantKey = false
	if key {
		tr.lastKey = time.Now()
	}
	tr.mu.Unlock()
	if !key {
		return
	}
	kf := traceKeyframe{At: at, Label: label, Colls: make(map[int64]map[int64][2]int64)}
	totals := make(map[int64]int64)
	rgs := make(map[int64]string)
	for id, v := range views {
		if !v.Up {
			continue
		}
		if rg := tr.sim.NodeResourceGroup(id); rg != "" {
			rgs[id] = rg
			if rg != meta.DefaultResourceGroupName {
				kf.RGs = rgs
			}
		}
		for seg, rows := range v.Segments {
			coll := v.Collection[seg]
			bytes := rows * tr.sim.Broker.RowBytes(coll)
			if kf.Colls[coll] == nil {
				kf.Colls[coll] = make(map[int64][2]int64)
			}
			e := kf.Colls[coll][id]
			e[0]++
			e[1] += bytes
			kf.Colls[coll][id] = e
			totals[coll] += bytes
		}
	}
	if len(totals) > traceTopTables {
		ids := make([]int64, 0, len(totals))
		for id := range totals {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return totals[ids[i]] > totals[ids[j]] })
		other := make(map[int64][2]int64)
		for _, id := range ids[traceTopTables:] {
			for node, e := range kf.Colls[id] {
				o := other[node]
				o[0] += e[0]
				o[1] += e[1]
				other[node] = o
			}
			delete(kf.Colls, id)
		}
		kf.Colls[0] = other
	}
	tr.mu.Lock()
	tr.keyframes = append(tr.keyframes, kf)
	tr.mu.Unlock()
}

func (tr *Trace) addNode(id int64, cfg NodeConfig) {
	if tr == nil {
		return
	}
	tr.mu.Lock()
	tr.nodes = append(tr.nodes, traceNode{ID: id, Memory: cfg.MemoryBytes, CPU: cfg.CPUNum, AddedAt: tr.since()})
	tr.mu.Unlock()
}

// mark records a scenario event and takes a keyframe.
func (tr *Trace) mark(label string) {
	if tr == nil {
		return
	}
	tr.mu.Lock()
	tr.marks = append(tr.marks, traceMark{At: tr.since(), Label: label})
	tr.mu.Unlock()
	tr.sample(label)
}

func (tr *Trace) addTask(t task.Task) {
	if tr == nil {
		return
	}
	e := traceTask{At: tr.since(), ID: t.ID(), Coll: t.CollectionID(), Replica: t.ReplicaID(), From: -1, To: -1}
	if t.Source() != nil {
		e.Source = t.Source().String()
	}
	switch tt := t.(type) {
	case *task.SegmentTask:
		e.Segment = tt.SegmentID()
		e.Reason = tt.GetReason()
		switch task.GetTaskType(t) {
		case task.TaskTypeMove:
			e.Kind = "move"
		case task.TaskTypeGrow:
			e.Kind = "grow"
		case task.TaskTypeReduce:
			e.Kind = "reduce"
		default:
			e.Kind = "update"
		}
		for _, a := range t.Actions() {
			switch a.Type() {
			case task.ActionTypeGrow:
				e.To = a.Node()
			case task.ActionTypeReduce:
				e.From = a.Node()
			}
		}
	case *task.ChannelTask:
		e.Channel = tt.Channel()
		for _, a := range t.Actions() {
			switch a.Type() {
			case task.ActionTypeGrow:
				e.Kind = "watch"
				e.To = a.Node()
			case task.ActionTypeReduce:
				e.Kind = "unsub"
				e.From = a.Node()
			}
		}
	default:
		return
	}
	tr.mu.Lock()
	// A run that retries a load forever would make the trace unreadable;
	// keep the first traceMaxTasks and count the rest.
	if len(tr.tasks) < traceMaxTasks {
		tr.tasks = append(tr.tasks, e)
	} else {
		tr.droppedTasks++
	}
	tr.mu.Unlock()
}

// finish stops sampling, takes a last keyframe and writes the file.
func (tr *Trace) finish() {
	if tr == nil {
		return
	}
	select {
	case <-tr.stop:
		return
	default:
		close(tr.stop)
	}
	<-tr.done
	tr.sample("end")
	tr.mu.Lock()
	defer tr.mu.Unlock()
	f := traceFile{
		Case:         tr.sim.t.Name(),
		Branch:       os.Getenv("MILVUS_SIM_BRANCH"),
		Params:       os.Getenv("MILVUS_SIM_PARAMS"),
		PeriodMs:     traceSamplePeriod.Milliseconds(),
		Nodes:        tr.nodes,
		Marks:        tr.marks,
		Samples:      tr.samples,
		Keyframes:    tr.keyframes,
		Tasks:        tr.tasks,
		DroppedTasks: tr.droppedTasks,
	}
	if name := os.Getenv("MILVUS_SIM_CASE"); name != "" {
		f.Case = name
	}
	for _, e := range tr.sim.Fleet.Stats.Events(tr.start) {
		te := traceEvent{At: e.At.Sub(tr.start).Seconds(), Node: e.Node, Coll: e.Collection, Segment: e.Segment, Channel: e.Channel, Rows: e.Rows}
		switch e.Kind {
		case EventLoad:
			te.Kind = "load"
		case EventRelease:
			te.Kind = "release"
		case EventWatch:
			te.Kind = "watch"
		case EventUnsub:
			te.Kind = "unsub"
		}
		f.Events = append(f.Events, te)
	}
	raw, err := json.Marshal(f)
	if err != nil {
		tr.sim.t.Logf("trace: %v", err)
		return
	}
	if err := os.WriteFile(tr.path, raw, 0o600); err != nil {
		tr.sim.t.Logf("trace: %v", err)
	}
}

// tracedScheduler records every task added to the scheduler.
type tracedScheduler struct {
	task.Scheduler
	tr *Trace
}

func (s tracedScheduler) Add(t task.Task) error {
	err := s.Scheduler.Add(t)
	if err == nil {
		s.tr.addTask(t)
	}
	return err
}
