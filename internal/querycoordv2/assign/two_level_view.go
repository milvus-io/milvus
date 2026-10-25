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

package assign

// The cluster view shared by placement and balancing.
//
// Every QueryNode has three quantities, in bytes of estimated segment
// memory, of which the view keeps the last two:
//
//	current = segments on the node now (a copy moving away is still held
//	          until the target has loaded it)
//	peak    = current + segments being loaded onto it: the most the node
//	          holds before the loads and releases in flight complete
//	final   = peak - segments leaving it (moving away, or being released):
//	          the node once everything in flight has completed
//
// "Does it fit" uses the peak (the reject line is never crossed on the
// way), "who has more" uses the final value (a decision is made on the
// state the moves already planned will produce, so nothing is planned
// twice). Only sealed segments count; growing data follows the channels.
//
// The view is one pass over the distribution and the segment tasks. The
// ledger records placements and moves QueryCoord has decided but the
// distribution does not show yet, so consecutive decisions see each other.

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TwoLevelConfig holds the tunables, read from queryCoord.twoLevel.* (and
// the load pacer's cap for the node).
type TwoLevelConfig struct {
	// Node balance: the fullest and emptiest node of a resource group may
	// differ by this fraction of the mean fill; HighWaterTolerance once the
	// fullest node is above HighWater.
	NodeTolerance      float64
	HighWater          float64
	HighWaterTolerance float64
	// Table balance: a table on a node may be off its fair share in bytes
	// by this fraction of it, and by at least its largest segment; in
	// segments by TableSegmentCountTolerance of its share, and by at least one.
	TableByteTolerance float64
	// SmallTableSegmentsPerNode: a table with fewer segments per replica node
	// than this is small: placement spreads it, balance leaves it alone.
	SmallTableSegmentsPerNode float64
	// LevelCandidates bounds the segments leveling considers on a node:
	// the largest ones, since moving a large segment levels fastest. 0
	// means all of them.
	LevelCandidates            int
	TableSegmentCountTolerance float64
	// CeilingRatio: table balance and placement add nothing to a node whose
	// fill is above mean x (1 + NodeTolerance x CeilingRatio).
	CeilingRatio float64
	// A segment is not balanced again this soon after it was loaded.
	Cooldown time.Duration

	SwapCandidates int // segments per side tried when pairing a swap
	TablesPerRound int // tables examined per round, round robin
	LedgerTTL      time.Duration
	ViewTTL        time.Duration
}

func GetTwoLevelConfig() TwoLevelConfig {
	cfg := &paramtable.Get().QueryCoordCfg
	return TwoLevelConfig{
		NodeTolerance:              cfg.TwoLevelNodeTolerance.GetAsFloat(),
		HighWater:                  cfg.TwoLevelHighWater.GetAsFloat(),
		HighWaterTolerance:         cfg.TwoLevelHighWaterTolerance.GetAsFloat(),
		TableByteTolerance:         cfg.TwoLevelTableByteTolerance.GetAsFloat(),
		SmallTableSegmentsPerNode:  cfg.TwoLevelSmallTableSegmentsPerNode.GetAsFloat(),
		LevelCandidates:            cfg.TwoLevelLevelCandidates.GetAsInt(),
		TableSegmentCountTolerance: cfg.TwoLevelTableSegmentCountTolerance.GetAsFloat(),
		CeilingRatio:               cfg.TwoLevelCeilingRatio.GetAsFloat(),
		Cooldown:                   time.Duration(cfg.TwoLevelCooldownSeconds.GetAsFloat() * float64(time.Second)),
		SwapCandidates:             cfg.TwoLevelSwapCandidates.GetAsInt(),
		TablesPerRound:             cfg.TwoLevelTablesPerRound.GetAsInt(),
		LedgerTTL:                  time.Duration(cfg.TwoLevelLedgerTTLSeconds.GetAsFloat() * float64(time.Second)),
		ViewTTL:                    time.Duration(cfg.TwoLevelViewTTLMillis.GetAsFloat() * float64(time.Millisecond)),
	}
}

// RejectRatio is the fill at which a QueryNode refuses a load: balance does
// not send a move that would take its target past it.
func RejectRatio() float64 {
	return paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()
}

// defaultBytesPerRow stands in for a segment with no binlog sizes yet.
const defaultBytesPerRow = 512

// SegmentBytes estimates the memory a QueryNode holds for a segment: indexed
// fields count their index size, other fields their in-memory binlog size.
// It is QueryCoord's own estimate from the segment info DataCoord reports;
// the QueryNode's final figure is not known until the segment is loaded.
func SegmentBytes(s *meta.Segment) float64 {
	if s == nil {
		return 0
	}
	var indexed map[int64]int64
	if len(s.IndexInfo) > 0 {
		indexed = make(map[int64]int64, len(s.IndexInfo))
	}
	for _, info := range s.IndexInfo {
		if info.GetIndexSize() > 0 {
			indexed[info.GetFieldID()] += info.GetIndexSize()
		}
	}
	var b int64
	for _, fb := range s.GetBinlogs() {
		if _, ok := indexed[fb.GetFieldID()]; ok {
			continue
		}
		for _, l := range fb.GetBinlogs() {
			if m := l.GetMemorySize(); m > 0 {
				b += m
			} else {
				b += l.GetLogSize()
			}
		}
	}
	for _, sz := range indexed {
		b += sz
	}
	if b == 0 {
		b = s.GetNumOfRows() * defaultBytesPerRow
	}
	return float64(b)
}

// NodeCapacityBytes is the node's memory capacity in bytes, 0 until the node
// has reported it (its first distribution pull).
func NodeCapacityBytes(nodeManager *session.NodeManager, id int64) float64 {
	if info := nodeManager.Get(id); info != nil && info.MemCapacity() > 0 {
		return info.MemCapacity() * 1024 * 1024
	}
	return 0
}

const (
	// unknownCapacity stands in for a node that has not reported its
	// capacity, so fills stay finite.
	unknownCapacity = 64 << 30
	// unknownCapacityWait bounds how long such a node is kept out.
	unknownCapacityWait = 10 * time.Second
	// rejectedGrace: a planned load with no task after this long was
	// rejected by the scheduler.
	rejectedGrace = 3 * time.Second
)

// ViewNode is one QueryNode.
type ViewNode struct {
	ID              int64
	Group           string // resource group
	Capacity        float64
	CPU             int64
	Peak            float64
	Final           float64
	Inflight        int     // segment loads in flight to the node, from any source
	IncomingBytes   float64 // bytes of those loads
	BudgetBytes     float64 // bytes the load pacer lets the node have in flight
	LoadSlots       int     // loads the load pacer lets the node run at once
	Segments        int     // sealed segment copies held or arriving, less those leaving (final terms)
	Stopping        bool
	CapacityUnknown bool // no capacity report yet
	Streaming       bool // a streaming node's embedded QueryNode: holds no sealed segments
}

// Fill is the final fill: what balancing compares.
func (n *ViewNode) Fill() float64 { return n.Final / n.Capacity }

// CanTake reports whether the load pacer would admit a load of sz bytes to
// the node now (the same rule as the pacer: an idle node takes anything;
// otherwise a free load slot and the bytes budget), so placement prefers
// such nodes and balance only sends a move to one: the load starts at once
// instead of queueing behind other loads. Loads for new tables and recovery
// are not limited by it: they queue in the pacer.
func (n *ViewNode) CanTake(sz float64) bool {
	return n.Inflight == 0 || (n.Inflight < n.LoadSlots && n.IncomingBytes+sz <= n.BudgetBytes)
}

// QueueRounds is the node's load queue in rounds of its own budget: how
// many times it must drain what is in flight before a new load starts.
func (n *ViewNode) QueueRounds() float64 {
	if n.BudgetBytes > 0 {
		return n.IncomingBytes / n.BudgetBytes
	}
	return float64(n.Inflight) / float64(max(1, n.LoadSlots))
}

// FitsPeak reports whether loading extra bytes keeps the node's peak under fill.
func (n *ViewNode) FitsPeak(extra, fill float64) bool {
	return n.Peak+extra <= fill*n.Capacity
}

// Usable reports whether sealed segments may be placed on or moved to or
// from the node.
func (n *ViewNode) Usable() bool { return !n.Stopping && !n.CapacityUnknown && !n.Streaming }

// CollNode is one collection on one node, in final terms.
type CollNode struct {
	Bytes    float64
	Count    int
	Segments []*meta.Segment // copies loaded here and staying: move candidates
}

// CollEntry is one collection across nodes.
type CollEntry struct {
	Nodes          []int64
	PerNode        []*CollNode
	LargestSegment float64 // largest segment of the collection, bytes
}

func (e *CollEntry) On(node int64) *CollNode {
	for i, n := range e.Nodes {
		if n == node {
			return e.PerNode[i]
		}
	}
	return nil
}

type segNode struct{ seg, node int64 }

// ClusterView is the state placement and balancing decide on.
type ClusterView struct {
	BuiltAt     time.Time
	Nodes       map[int64]*ViewNode
	Collections map[int64]*CollEntry
	NodeColls   map[int64][]int64 // node -> collections with a part on it
	busy        map[segNode]bool  // copies with a segment task acting on them
	planned     map[segNode]bool  // copies a ledger entry is moving away
	sizes       map[int64]float64 // segment -> bytes the view counts it as
	unsized     map[int64]bool    // segments counted at a guessed size
}

// Bytes is the size the view counts seg as; balance must use the same.
func (v *ClusterView) SizeOf(seg *meta.Segment) float64 { return v.sizes[seg.GetID()] }

// Unsized reports a segment whose size is only guessed (no estimate yet):
// balance leaves it alone.
func (v *ClusterView) Unsized(seg *meta.Segment) bool { return v.unsized[seg.GetID()] }

func (v *ClusterView) collOnOrNew(coll, node int64) *CollNode {
	e := v.Collections[coll]
	if e == nil {
		e = &CollEntry{}
		v.Collections[coll] = e
	}
	if cn := e.On(node); cn != nil {
		return cn
	}
	cn := &CollNode{}
	e.Nodes = append(e.Nodes, node)
	e.PerNode = append(e.PerNode, cn)
	v.NodeColls[node] = append(v.NodeColls[node], coll)
	return cn
}

// CollOn returns coll's part on node, or nil.
func (v *ClusterView) CollOn(coll, node int64) *CollNode {
	if e := v.Collections[coll]; e != nil {
		return e.On(node)
	}
	return nil
}

// Busy reports whether a segment task acts on the copy of seg on node.
func (v *ClusterView) Busy(seg, node int64) bool { return v.busy[segNode{seg, node}] }

// Planned reports whether the copy of seg on node is already being moved.
func (v *ClusterView) Planned(seg, node int64) bool { return v.planned[segNode{seg, node}] }

// incoming applies a load in flight to node.
func (v *ClusterView) incoming(coll, node int64, bytes float64, countInflight bool) {
	n, ok := v.Nodes[node]
	if !ok {
		return
	}
	n.Peak += bytes
	n.Final += bytes
	n.Segments++
	if countInflight {
		n.Inflight++
		n.IncomingBytes += bytes
	}
	cn := v.collOnOrNew(coll, node)
	cn.Bytes += bytes
	cn.Count++
	e := v.Collections[coll]
	e.LargestSegment = max(e.LargestSegment, bytes)
}

// leaving applies a copy that will leave node: still held (in the peak),
// not in the final value.
func (v *ClusterView) leaving(coll, seg, node int64, bytes float64) {
	if v.planned[segNode{seg, node}] {
		return
	}
	v.planned[segNode{seg, node}] = true
	if n, ok := v.Nodes[node]; ok {
		n.Final -= bytes
		n.Segments--
	}
	if cn := v.CollOn(coll, node); cn != nil {
		cn.Bytes -= bytes
		cn.Count--
	}
}

// LedgerEntry is a placement (From == -1) or a move QueryCoord decided.
type LedgerEntry struct {
	Collection int64
	Segment    int64
	From, To   int64
	Bytes      float64
	At         time.Time
}

func (e *LedgerEntry) key() segNode { return segNode{e.Segment, e.To} }

type sizeEntry struct {
	complete bool // computed from the segment's binlogs (not a bare stub)
	indexes  int  // index count, so a newly loaded index refreshes the size
	bytes    float64
}

// TwoLevelState is the process-wide state shared by placement and balance.
type TwoLevelState struct {
	mu        sync.Mutex
	ledger    map[segNode]*LedgerEntry // (segment, target node) -> entry
	pending   map[int64]int            // segment -> ledger entries for it
	view      *ClusterView
	viewVer   int64
	sched     task.Scheduler
	sizes     map[int64]sizeEntry   // segment -> cached size estimate
	firstSeen map[int64]time.Time   // nodes seen without a capacity report
	cursor    int64                 // round-robin position over tables
	moved     map[segNode]time.Time // (segment, node) -> when balance moved the copy there
	metricsOn map[int64]bool        // nodes with per-node gauges published
}

// NodesWithMetrics records the nodes seen this round and returns the ones
// seen before but gone now, whose gauges should be dropped.
func (s *TwoLevelState) NodesWithMetrics(seen map[int64]bool) map[int64]bool {
	gone := make(map[int64]bool)
	for id := range s.metricsOn {
		if !seen[id] {
			gone[id] = true
		}
	}
	s.metricsOn = seen
	return gone
}

var (
	twoLevelOnce  sync.Once
	twoLevelState *TwoLevelState
)

func GetTwoLevelState() *TwoLevelState {
	twoLevelOnce.Do(func() {
		twoLevelState = &TwoLevelState{}
		twoLevelState.reset()
	})
	return twoLevelState
}

func (s *TwoLevelState) reset() {
	s.ledger = make(map[segNode]*LedgerEntry)
	s.pending = make(map[int64]int)
	s.sizes = make(map[int64]sizeEntry)
	s.firstSeen = make(map[int64]time.Time)
	s.view = nil
	s.cursor = 0
	s.moved = make(map[segNode]time.Time)
}

// ResetTwoLevelStateForTest clears all state.
func ResetTwoLevelStateForTest() {
	s := GetTwoLevelState()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()
}

// AgeHistoryForTest moves every recorded time d into the past.
func AgeHistoryForTest(d time.Duration) {
	s := GetTwoLevelState()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.ledger {
		e.At = e.At.Add(-d)
	}
	for k, t := range s.firstSeen {
		s.firstSeen[k] = t.Add(-d)
	}
	for k, t := range s.moved {
		s.moved[k] = t.Add(-d)
	}
	s.view = nil
}

func (s *TwoLevelState) Lock()   { s.mu.Lock() }
func (s *TwoLevelState) Unlock() { s.mu.Unlock() }

// SetScheduler lets the view see segment tasks.
func (s *TwoLevelState) SetScheduler(sched task.Scheduler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sched = sched
}

// CachedBytes avoids re-estimating every segment on every rebuild.
func (s *TwoLevelState) CachedBytes(seg *meta.Segment) float64 {
	// A copy the distribution could not match to a target (QueryCoord just
	// restarted, or the segment left the target) carries a bare stub with
	// no binlogs: keep the size known from before rather than count it as
	// empty. The size belongs to the segment, so every copy gets the same.
	complete := len(seg.GetBinlogs()) > 0 || seg.GetNumOfRows() > 0
	if e, ok := s.sizes[seg.GetID()]; ok && (!complete || (e.complete && len(seg.IndexInfo) <= e.indexes)) {
		return e.bytes
	}
	b := SegmentBytes(seg)
	s.sizes[seg.GetID()] = sizeEntry{complete: complete, indexes: len(seg.IndexInfo), bytes: b}
	return b
}

// Cursor returns and sets the round-robin position over tables.
// CooldownSnapshot copies the move times a planner needs to honor cooldowns
// off the lock.
func (s *TwoLevelState) CooldownSnapshot() map[[2]int64]time.Time {
	out := make(map[[2]int64]time.Time, len(s.moved))
	for k, t := range s.moved {
		out[[2]int64{k.seg, k.node}] = t
	}
	return out
}

// ApplyPlan edits the view as if the move were in flight: bytes arrive on
// the target and leave the source.
func (v *ClusterView) ApplyPlan(coll, seg, from, to int64, bytes float64) {
	v.incoming(coll, to, bytes, true)
	if from != -1 {
		v.leaving(coll, seg, from, bytes)
	}
}

func (s *TwoLevelState) TableCursor() int64     { return s.cursor }
func (s *TwoLevelState) SetTableCursor(c int64) { s.cursor = c }
func (s *TwoLevelState) View() *ClusterView     { return s.view }
func (s *TwoLevelState) LedgerSize() int        { return len(s.ledger) }

// ViewLocked returns the view with the ledger applied. The balancer forces
// a rebuild once per round; placement reuses the view until the
// distribution changed and ViewTTL passed (the ledger keeps it current).
func (s *TwoLevelState) ViewLocked(nodeManager *session.NodeManager, dist *meta.DistributionManager, m *meta.Meta, cfg TwoLevelConfig, force bool) *ClusterView {
	ver := dist.SegmentDistManager.GetVersion()
	if !force && s.view != nil && (ver == s.viewVer || time.Since(s.view.BuiltAt) < cfg.ViewTTL) {
		return s.view
	}
	v := s.buildLocked(nodeManager, dist, m, cfg)
	s.view, s.viewVer = v, ver
	return v
}

// BuildViewLocked builds a fresh view for a planner to own and edit off the
// lock; the cached placement view is left as it is.
func (s *TwoLevelState) BuildViewLocked(nodeManager *session.NodeManager, dist *meta.DistributionManager, m *meta.Meta, cfg TwoLevelConfig) *ClusterView {
	return s.buildLocked(nodeManager, dist, m, cfg)
}

func (s *TwoLevelState) buildLocked(nodeManager *session.NodeManager, dist *meta.DistributionManager, m *meta.Meta, cfg TwoLevelConfig) *ClusterView {
	now := time.Now()
	for id, t := range s.moved {
		if now.Sub(t) >= cfg.Cooldown {
			delete(s.moved, id)
		}
	}
	v := &ClusterView{
		BuiltAt:     now,
		Nodes:       make(map[int64]*ViewNode),
		Collections: make(map[int64]*CollEntry),
		NodeColls:   make(map[int64][]int64),
		busy:        make(map[segNode]bool),
		planned:     make(map[segNode]bool),
		sizes:       make(map[int64]float64),
		unsized:     make(map[int64]bool),
	}
	for _, info := range nodeManager.GetAll() {
		id := info.ID()
		capacity := NodeCapacityBytes(nodeManager, id)
		unknown := false
		if capacity > 0 {
			delete(s.firstSeen, id) // a restart waits afresh
		} else {
			// Wait a bounded time for the first report, then go on with a
			// guess rather than never using the node.
			if _, ok := s.firstSeen[id]; !ok {
				s.firstSeen[id] = now
			}
			unknown = now.Sub(s.firstSeen[id]) < unknownCapacityWait
			capacity = unknownCapacity
		}
		group := ""
		if m != nil && m.ResourceManager != nil {
			group = m.GetResourceGroupByNodeID(id)
		}
		var budgetBytes float64
		slots := 0
		if s.sched != nil {
			budgetBytes, slots = s.sched.LoadBudget(id)
		}
		if slots == 0 {
			slots = max(2, int(info.CPUNum()))
		}
		v.Nodes[id] = &ViewNode{
			ID: id, Group: group, Capacity: capacity, CapacityUnknown: unknown,
			CPU: info.CPUNum(), BudgetBytes: budgetBytes, LoadSlots: slots,
			Stopping: info.IsStoppingState(), Streaming: info.IsEmbeddedQueryNodeInStreamingNode(),
		}
	}

	// Segment tasks: loads in flight (grow actions) and copies leaving
	// (reduce actions: the source of a move, or a release after compaction).
	growing := make(map[segNode]bool)
	leaving := make(map[segNode]bool)
	if s.sched != nil {
		s.sched.GetSegmentTaskNum(func(t task.Task) bool {
			st, ok := t.(*task.SegmentTask)
			if !ok {
				return false
			}
			for _, a := range st.Actions() {
				k := segNode{st.SegmentID(), a.Node()}
				v.busy[k] = true
				switch a.Type() {
				case task.ActionTypeGrow:
					if !growing[k] {
						growing[k] = true
						if n, ok := v.Nodes[a.Node()]; ok {
							n.Inflight++
						}
					}
				case task.ActionTypeReduce:
					leaving[k] = true
				}
			}
			return false
		})
	}

	onNode := make(map[segNode]bool)
	seen := make(map[int64]bool)
	type sized struct {
		seg *meta.Segment
		sz  float64
	}
	var copies []sized
	// A copy with no size known yet (a bare stub right after a restart)
	// counts as the average known segment of its collection.
	known := make(map[int64][2]float64) // collection -> (bytes, count)
	for _, seg := range dist.SegmentDistManager.GetByFilter() {
		if _, ok := v.Nodes[seg.Node]; !ok || seg.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		seen[seg.GetID()] = true
		sz := s.CachedBytes(seg)
		if sz > 0 {
			k := known[seg.GetCollectionID()]
			known[seg.GetCollectionID()] = [2]float64{k[0] + sz, k[1] + 1}
		}
		copies = append(copies, sized{seg, sz})
	}
	for _, c := range copies {
		seg, sz := c.seg, c.sz
		if sz == 0 {
			v.unsized[seg.GetID()] = true
			if k := known[seg.GetCollectionID()]; k[1] > 0 {
				sz = k[0] / k[1]
			}
		}
		v.sizes[seg.GetID()] = sz
		n := v.Nodes[seg.Node]
		k := segNode{seg.GetID(), seg.Node}
		onNode[k] = true
		n.Peak += sz
		cn := v.collOnOrNew(seg.GetCollectionID(), seg.Node)
		if leaving[k] {
			v.planned[k] = true
			continue
		}
		n.Final += sz
		n.Segments++
		cn.Bytes += sz
		cn.Count++
		cn.Segments = append(cn.Segments, seg)
		e := v.Collections[seg.GetCollectionID()]
		e.LargestSegment = max(e.LargestSegment, sz)
	}
	if len(s.sizes) > 1024 && len(s.sizes) > 2*len(seen) {
		for id := range s.sizes {
			if !seen[id] {
				delete(s.sizes, id) // released
			}
		}
	}

	// Ledger: decisions the distribution does not show yet.
	for k, e := range s.ledger {
		landed := onNode[segNode{e.Segment, e.To}]
		srcGone := e.From == -1 || !onNode[segNode{e.Segment, e.From}]
		switch {
		case landed && srcGone, now.Sub(e.At) > cfg.LedgerTTL:
			s.drop(k)
			continue
		case !landed && !growing[segNode{e.Segment, e.To}] && now.Sub(e.At) > rejectedGrace:
			// No task: rejected by the scheduler, failed or canceled.
			s.drop(k)
			continue
		}
		if !landed {
			v.incoming(e.Collection, e.To, e.Bytes, !growing[segNode{e.Segment, e.To}])
		}
		if !srcGone {
			v.leaving(e.Collection, e.Segment, e.From, e.Bytes)
		}
	}
	return v
}

func (s *TwoLevelState) drop(k segNode) {
	if e, ok := s.ledger[k]; ok {
		delete(s.ledger, k)
		if s.pending[e.Segment]--; s.pending[e.Segment] <= 0 {
			delete(s.pending, e.Segment)
		}
	}
}

// Record adds a ledger entry and applies it to the current view.
func (s *TwoLevelState) RecordPlan(e *LedgerEntry) {
	if _, ok := s.ledger[e.key()]; ok {
		s.drop(e.key())
	}
	s.ledger[e.key()] = e
	s.pending[e.Segment]++
	if e.From != -1 {
		s.moved[e.key()] = e.At
	}
	if s.view != nil {
		s.view.incoming(e.Collection, e.To, e.Bytes, true)
		if e.From != -1 {
			s.view.leaving(e.Collection, e.Segment, e.From, e.Bytes)
		}
	}
}

// PendingPlacement returns the node a placement of segmentID to one of
// nodes is already in flight to, if any.
func (s *TwoLevelState) PendingPlacement(segmentID int64, nodes []*ViewNode) (int64, bool) {
	if s.pending[segmentID] == 0 {
		return 0, false
	}
	for _, n := range nodes {
		if e, ok := s.ledger[segNode{segmentID, n.ID}]; ok && e.From == -1 {
			return n.ID, true
		}
	}
	return 0, false
}

// InflightCount counts ledger placements and moves.
func (s *TwoLevelState) InflightCount() (placements, moves int) {
	for _, e := range s.ledger {
		if e.From == -1 {
			placements++
		} else {
			moves++
		}
	}
	return placements, moves
}

// InLedger reports whether segmentID has a ledger entry targeting node.
func (s *TwoLevelState) InLedger(segmentID, node int64) bool {
	_, ok := s.ledger[segNode{segmentID, node}]
	return ok
}

// Cooling reports whether balance moved this copy of seg (to its node)
// within the cooldown, so it must stay put; another replica's copy is free.
// Segments placed by load, recovery or compaction do not cool: a node added
// right after a load may take them at once. The move history is in memory
// and lost on a QueryCoord restart; the new QueryCoord plans from the same
// state the old one left, so it has no reason to undo a landed move, and
// freezing every recent load instead would stall balance for a cooldown
// after each restart.
func (s *TwoLevelState) InCooldown(seg *meta.Segment, now time.Time, cooldown time.Duration) bool {
	k := segNode{seg.GetID(), seg.Node}
	if t, ok := s.moved[k]; ok {
		if now.Sub(t) < cooldown {
			return true
		}
		delete(s.moved, k)
	}
	return false
}
