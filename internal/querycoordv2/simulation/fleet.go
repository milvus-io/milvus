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
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// NodeConfig describes one simulated QueryNode.
type NodeConfig struct {
	MemoryBytes int64
	CPUNum      int64
	// LoadConcurrency bounds concurrent segment loads on the node; further
	// loads queue inside the node (their RPC blocks until they are done).
	LoadConcurrency int
	// LoadBandwidthBytes is the node's total load throughput (download plus
	// deserialize), shared by its concurrent loads. One load takes
	// LoadBaseLatency + size / (LoadBandwidthBytes / LoadConcurrency).
	LoadBandwidthBytes int64
	LoadBaseLatency    time.Duration
	WatchLatency       time.Duration
	// LoadRefLatency, when set, replaces the bandwidth model: a load of
	// LoadRefBytes takes LoadRefLatency, and other sizes scale by
	// (size/LoadRefBytes)^LoadSizeExponent. Production load time grows much
	// slower than segment size (fixed per-segment cost dominates small
	// segments), so the exponent is usually well below 1.
	LoadRefLatency   time.Duration
	LoadRefBytes     int64
	LoadSizeExponent float64
	// LoadJitterSigma multiplies each load's duration by a lognormal factor
	// (production per-segment load time has p99 about 20x p50).
	LoadJitterSigma float64
	// Slowdown multiplies all latencies (stragglers).
	Slowdown float64
}

// CalibratedNodeConfig matches production (7-day fleet metrics, 2026-10)
// at the simulator's tenfold time compression: per-segment load time p50
// ~1.6s / mean ~3.6s / p99 ~33s real (lognormal, sigma ~1.3 overall, split
// between segment size and per-load jitter), busy-node load concurrency
// p90 3-5, and the 5-minute SegmentTaskTimeout scaled to 30s.
func CalibratedNodeConfig() NodeConfig {
	cfg := DefaultNodeConfig()
	cfg.LoadConcurrency = 4
	cfg.LoadBaseLatency = 10 * time.Millisecond
	// A 10MB median segment takes ~0.16s per slot.
	cfg.LoadBandwidthBytes = 250 << 20
	cfg.LoadJitterSigma = 0.8
	return cfg
}

func DefaultNodeConfig() NodeConfig {
	// Scaled about tenfold like QueryCoord intervals: a 10MB segment takes
	// ~0.4s per slot, so a node drains ~100MB/s of queued loads.
	return NodeConfig{
		MemoryBytes:        64 << 30,
		CPUNum:             16,
		LoadConcurrency:    4,
		LoadBandwidthBytes: 100 << 20,
		LoadBaseLatency:    20 * time.Millisecond,
		WatchLatency:       50 * time.Millisecond,
		Slowdown:           1,
	}
}

type loadedSegment struct {
	info    *querypb.SegmentVersionInfo
	rows    int64
	memory  int64
	version int64
}

type delegator struct {
	collection    int64
	channel       string
	version       int64
	targetVersion int64
	watched       bool
	segmentDist   map[int64]*querypb.SegmentDist
}

// QueryNode is an in-memory QueryNode: a worker (sealed segments) and, for the
// channels it watches, a delegator (leader view).
type QueryNode struct {
	ID          int64
	cfg         NodeConfig
	bytesPerRow int64
	// streaming marks a streaming node's embedded QueryNode (delegator host).
	streaming bool

	mu         sync.Mutex
	up         bool
	generation int64 // bumps on every restart; aborts work of a dead process
	segments   map[int64]*loadedSegment
	delegators map[string]*delegator
	usedMemory int64
	loading    int
	queued     int
	rng        *rand.Rand
	// inflight holds segments being loaded, for QueryNode load reports.
	inflight map[int64]*querypb.SegmentLoadInfo
	modifyTs int64
	sem      chan struct{}
}

// decorateDistribution lets a branch add fields its QueryNodes report (for
// example in-flight load reports). It runs with the node lock held.
var decorateDistribution func(n *QueryNode, resp *querypb.GetDataDistributionResponse)

func newQueryNode(id int64, cfg NodeConfig) *QueryNode {
	n := &QueryNode{ID: id, cfg: cfg, rng: rand.New(rand.NewSource(id))}
	n.resetLocked()
	return n
}

func (n *QueryNode) resetLocked() {
	n.up = true
	n.generation++
	n.segments = make(map[int64]*loadedSegment)
	n.delegators = make(map[string]*delegator)
	n.usedMemory = 0
	n.loading = 0
	n.inflight = make(map[int64]*querypb.SegmentLoadInfo)
	n.sem = make(chan struct{}, max(1, n.cfg.LoadConcurrency))
	n.touchLocked()
}

func (n *QueryNode) touchLocked() {
	n.modifyTs = time.Now().UnixNano()
}

func (n *QueryNode) scaled(d time.Duration) time.Duration {
	if n.cfg.Slowdown > 0 {
		return time.Duration(float64(d) * n.cfg.Slowdown)
	}
	return d
}

func (n *QueryNode) alive(gen int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.up && n.generation == gen
}

// FleetStats counts QueryNode-side work, which is ground truth independent of
// what QueryCoord believes.
type FleetStats struct {
	LoadsStarted     atomic.Int64
	LoadsSucceeded   atomic.Int64
	LoadsFailed      atomic.Int64
	LoadsRejectedOOM atomic.Int64
	Releases         atomic.Int64
	Watches          atomic.Int64
	// RedundantWatchRPCs counts WatchDmChannels for an already subscribed
	// channel (QueryCoord resending before distribution caught up).
	RedundantWatchRPCs atomic.Int64
	Unsubs             atomic.Int64
	LoadedRows         atomic.Int64
	// LoadsToStopping counts loads targeting a node that was already stopping.
	LoadsToStopping atomic.Int64
	MaxNodeLoading  atomic.Int64
	// LoadsRejectedThreshold counts the subset of OOM rejections that
	// QueryCoord treats as resource exhaustion (node penalty).
	LoadsRejectedThreshold atomic.Int64
	// LoadsForced counts loads admitted under the force_load threshold.
	LoadsForced atomic.Int64
	// LoadsWithoutBinlogSize counts loads whose request carried no binlog
	// memory sizes (memory falls back to rows x default row width).
	LoadsWithoutBinlogSize atomic.Int64
	// MaxNodeQueued is the deepest queue of loads waiting for a load slot on
	// one node; QueueWaitNanos/QueuedLoads give the mean wait.
	MaxNodeQueued  atomic.Int64
	QueueWaitNanos atomic.Int64
	MaxQueueWait   atomic.Int64
	QueuedLoads    atomic.Int64
	// LoadsCanceled counts loads aborted because QueryCoord's task context
	// ended (task timeout or cancel), whether still queued or loading.
	LoadsCanceled atomic.Int64
	// WatchesFailed counts WatchDmChannels calls aborted before the
	// delegator became serviceable (task timeout, node death).
	WatchesFailed atomic.Int64

	peakMu       sync.Mutex
	peakInflight map[int64]int64 // node -> peak queued+loading since ResetPeaks

	mu          sync.Mutex
	loadsByNode map[int64]int64
	watchLog    []string
	events      []Event
}

// EventKind is a QueryNode-side data movement.
type EventKind int

const (
	EventLoad EventKind = iota
	EventRelease
	EventWatch
	EventUnsub
)

// Event is one ground-truth data movement on a QueryNode.
type Event struct {
	Kind       EventKind
	At         time.Time
	Node       int64
	Collection int64
	Segment    int64
	Channel    string
	Rows       int64
}

// SearchModel estimates search latency: a search fans out to every node of
// the replica that holds the collection's segments and waits for the
// slowest. A node takes BaseMs + MsPerMillionRows * rows/1e6, inflated by
// (1 + LoadInterference * min(loading, cpu)/cpu) while it is loading
// segments (load deserialization and interim index builds compete for CPU
// and memory bandwidth with search).
type SearchModel struct {
	BaseMs           float64
	MsPerMillionRows float64
	LoadInterference float64
}

func (m SearchModel) enabled() bool { return m.BaseMs > 0 || m.MsPerMillionRows > 0 }

func (m SearchModel) nodeMs(rows int64, loading, cpu int) float64 {
	ms := m.BaseMs + m.MsPerMillionRows*float64(rows)/1e6
	if loading > 0 && cpu > 0 {
		ms *= 1 + m.LoadInterference*float64(min(loading, cpu))/float64(cpu)
	}
	return ms
}

// SetNodeConfigAll replaces every node's config; new nodes take theirs from
// AddNode. Used to warm a cluster up with fast loads before a replay.
func (f *Fleet) SetNodeConfigAll(cfg NodeConfig) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, n := range f.nodes {
		n.mu.Lock()
		slowdown := n.cfg.Slowdown
		n.cfg = cfg
		n.cfg.Slowdown = slowdown
		// Called while the cluster is quiet; loads already holding a slot of
		// the old semaphore release into it harmlessly.
		n.sem = make(chan struct{}, max(1, cfg.LoadConcurrency))
		n.mu.Unlock()
	}
}

func (f *Fleet) segmentMemory(info *querypb.SegmentLoadInfo) int64 {
	var memory int64
	for _, fb := range info.GetBinlogPaths() {
		for _, b := range fb.GetBinlogs() {
			memory += b.GetMemorySize()
		}
	}
	if memory == 0 {
		memory = info.GetNumOfRows() * f.broker.BytesPerRow
	}
	return memory
}

// ResetPeaks clears the high-water marks so a phase measures its own peaks.
func (st *FleetStats) ResetPeaks() {
	st.MaxNodeLoading.Store(0)
	st.MaxNodeQueued.Store(0)
	st.MaxQueueWait.Store(0)
	st.peakMu.Lock()
	st.peakInflight = nil
	st.peakMu.Unlock()
}

func (st *FleetStats) notePeakInflight(node, inflight int64) {
	st.peakMu.Lock()
	defer st.peakMu.Unlock()
	if st.peakInflight == nil {
		st.peakInflight = make(map[int64]int64)
	}
	if inflight > st.peakInflight[node] {
		st.peakInflight[node] = inflight
	}
}

// PeakInflightByNode returns, per node, the peak number of loads it held at
// once (queued for a slot plus loading) since the last ResetPeaks.
func (st *FleetStats) PeakInflightByNode() map[int64]int64 {
	st.peakMu.Lock()
	defer st.peakMu.Unlock()
	out := make(map[int64]int64, len(st.peakInflight))
	for k, v := range st.peakInflight {
		out[k] = v
	}
	return out
}

func (st *FleetStats) record(e Event) {
	e.At = time.Now()
	st.mu.Lock()
	st.events = append(st.events, e)
	st.mu.Unlock()
}

// Events returns the events recorded at or after since.
func (st *FleetStats) Events(since time.Time) []Event {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []Event
	for _, e := range st.events {
		if !e.At.Before(since) {
			out = append(out, e)
		}
	}
	return out
}

// WatchLog returns "channel@node" for every watch, in order.
func (st *FleetStats) WatchLog() []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]string(nil), st.watchLog...)
}

func (st *FleetStats) addNodeLoad(node int64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.loadsByNode == nil {
		st.loadsByNode = make(map[int64]int64)
	}
	st.loadsByNode[node]++
}

// LoadsByNode returns successful loads per target node.
func (st *FleetStats) LoadsByNode() map[int64]int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[int64]int64, len(st.loadsByNode))
	for k, v := range st.loadsByNode {
		out[k] = v
	}
	return out
}

// Fleet is the set of simulated QueryNodes. It implements session.Cluster.
type Fleet struct {
	mu       sync.RWMutex
	nodes    map[int64]*QueryNode
	stopping map[int64]bool
	Stats    FleetStats
	// Search models query latency; zero value disables it.
	Search SearchModel
	// HideGrowingRows makes delegators report zero growing rows.
	HideGrowingRows atomic.Bool
	// LoadFailPermille fails that many of every 1000 segment loads with a
	// retriable error (fault injection).
	LoadFailPermille atomic.Int32
	failSeq          atomic.Int64
	// ChargeGrowingMemory makes a delegator's unflushed rows count against
	// its node's memory (QueryNodes hold growing segments in memory), for
	// the overload threshold and for reported usage. Off by default.
	ChargeGrowingMemory atomic.Bool
	broker              *Broker
}

var _ session.Cluster = (*Fleet)(nil)

// growingRows is what a delegator reports as unflushed rows; HideGrowingRows
// turns it off to isolate its effect on the node score.
func (f *Fleet) growingRows(channel string) int64 {
	if f.HideGrowingRows.Load() {
		return 0
	}
	return f.broker.GrowingRows(channel)
}

func NewFleet(broker *Broker) *Fleet {
	return &Fleet{nodes: make(map[int64]*QueryNode), stopping: make(map[int64]bool), broker: broker}
}

func (f *Fleet) AddNode(id int64, cfg NodeConfig) *QueryNode {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := newQueryNode(id, cfg)
	n.bytesPerRow = f.broker.BytesPerRow
	f.nodes[id] = n
	delete(f.stopping, id)
	return n
}

// Kill crashes a node: all in-memory state is lost and in-flight work aborts.
func (f *Fleet) Kill(id int64) {
	if n := f.node(id); n != nil {
		n.mu.Lock()
		n.up = false
		n.generation++
		n.segments = make(map[int64]*loadedSegment)
		n.delegators = make(map[string]*delegator)
		n.inflight = make(map[int64]*querypb.SegmentLoadInfo)
		n.usedMemory = 0
		n.touchLocked()
		n.mu.Unlock()
	}
}

// Restart brings a killed node back with empty state.
func (f *Fleet) Restart(id int64) {
	if n := f.node(id); n != nil {
		n.mu.Lock()
		n.resetLocked()
		n.mu.Unlock()
	}
	f.mu.Lock()
	delete(f.stopping, id)
	f.mu.Unlock()
}

func (f *Fleet) MarkStopping(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopping[id] = true
}

func (f *Fleet) IsStopping(id int64) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.stopping[id]
}

func (f *Fleet) SetSlowdown(id int64, slowdown float64) {
	if n := f.node(id); n != nil {
		n.mu.Lock()
		n.cfg.Slowdown = slowdown
		n.mu.Unlock()
	}
}

func (f *Fleet) node(id int64) *QueryNode {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.nodes[id]
}

func (f *Fleet) liveNode(id int64) (*QueryNode, int64, error) {
	n := f.node(id)
	if n == nil {
		return nil, 0, merr.WrapErrNodeNotFound(id)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.up {
		return nil, 0, merr.WrapErrNodeNotAvailable(id)
	}
	return n, n.generation, nil
}

// NodeIDs returns all nodes, alive or not.
func (f *Fleet) NodeIDs() []int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ids := make([]int64, 0, len(f.nodes))
	for id := range f.nodes {
		ids = append(ids, id)
	}
	return ids
}

// NodeView is a point-in-time copy of one node's state for metrics.
type NodeView struct {
	ID         int64
	Up         bool
	Segments   map[int64]int64 // segmentID -> rows
	Collection map[int64]int64 // segmentID -> collectionID
	Delegators map[string]DelegatorView
	UsedMemory int64
	Loading    int
	CPUNum     int
}

type DelegatorView struct {
	Collection    int64
	TargetVersion int64
	Watched       bool
	SegmentDist   map[int64]int64 // segmentID -> worker node
}

func (f *Fleet) Snapshot() map[int64]NodeView {
	f.mu.RLock()
	nodes := make([]*QueryNode, 0, len(f.nodes))
	for _, n := range f.nodes {
		nodes = append(nodes, n)
	}
	f.mu.RUnlock()
	views := make(map[int64]NodeView, len(nodes))
	for _, n := range nodes {
		n.mu.Lock()
		v := NodeView{ID: n.ID, Up: n.up, Segments: make(map[int64]int64), Collection: make(map[int64]int64),
			Delegators: make(map[string]DelegatorView), UsedMemory: n.usedMemory + f.growingBytesLocked(n), Loading: n.loading, CPUNum: int(n.cfg.CPUNum)}
		for id, s := range n.segments {
			v.Segments[id] = s.rows
			v.Collection[id] = s.info.GetCollection()
		}
		for ch, d := range n.delegators {
			dv := DelegatorView{Collection: d.collection, TargetVersion: d.targetVersion, Watched: d.watched, SegmentDist: make(map[int64]int64)}
			for id, sd := range d.segmentDist {
				dv.SegmentDist[id] = sd.GetNodeID()
			}
			v.Delegators[ch] = dv
		}
		n.mu.Unlock()
		views[n.ID] = v
	}
	return views
}

func (f *Fleet) sleep(ctx context.Context, n *QueryNode, gen int64, d time.Duration) error {
	timer := time.NewTimer(n.scaled(d))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if !n.alive(gen) {
		return merr.WrapErrNodeNotAvailable(n.ID)
	}
	return nil
}

func (f *Fleet) WatchDmChannels(ctx context.Context, nodeID int64, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error) {
	n, gen, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	info := req.GetInfos()[0]
	channel := info.GetChannelName()
	n.mu.Lock()
	if existing := n.delegators[channel]; existing != nil && existing.collection == req.GetCollectionID() {
		// QueryNode treats a repeated watch of a subscribed channel as done.
		n.mu.Unlock()
		f.Stats.RedundantWatchRPCs.Inc()
		return merr.Success(), nil
	}
	f.Stats.Watches.Inc()
	f.Stats.mu.Lock()
	f.Stats.watchLog = append(f.Stats.watchLog, fmt.Sprintf("%s@%d", channel, nodeID))
	f.Stats.mu.Unlock()
	f.Stats.record(Event{Kind: EventWatch, Node: nodeID, Collection: req.GetCollectionID(), Channel: channel})
	n.delegators[channel] = &delegator{
		collection:    req.GetCollectionID(),
		channel:       channel,
		version:       req.GetVersion(),
		targetVersion: req.GetTargetVersion(),
		segmentDist:   make(map[int64]*querypb.SegmentDist),
	}
	n.touchLocked()
	n.mu.Unlock()
	if err := f.sleep(ctx, n, gen, n.cfg.WatchLatency); err != nil {
		f.Stats.WatchesFailed.Inc()
		n.mu.Lock()
		if d := n.delegators[channel]; d != nil && d.version == req.GetVersion() && n.generation == gen {
			delete(n.delegators, channel)
			n.touchLocked()
		}
		n.mu.Unlock()
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if d := n.delegators[channel]; d != nil && d.version == req.GetVersion() {
		d.watched = true
		n.touchLocked()
	}
	return merr.Success(), nil
}

func (f *Fleet) UnsubDmChannel(_ context.Context, nodeID int64, req *querypb.UnsubDmChannelRequest) (*commonpb.Status, error) {
	n, _, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	f.Stats.Unsubs.Inc()
	f.Stats.record(Event{Kind: EventUnsub, Node: nodeID, Collection: req.GetCollectionID(), Channel: req.GetChannelName()})
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.delegators, req.GetChannelName())
	n.touchLocked()
	return merr.Success(), nil
}

func (f *Fleet) LoadSegments(ctx context.Context, nodeID int64, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	if req.GetLoadScope() != querypb.LoadScope_Full {
		return merr.Success(), nil // delta/stats/reopen: no placement effect
	}
	leader, _, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	workerID := req.GetDstNodeID()
	if workerID == 0 {
		workerID = nodeID
	}
	worker, gen, err := f.liveNode(workerID)
	if err != nil {
		return nil, err
	}
	if f.IsStopping(workerID) {
		f.Stats.LoadsToStopping.Inc()
	}
	if p := f.LoadFailPermille.Load(); p > 0 && (f.failSeq.Inc()*7919)%1000 < int64(p) {
		return merr.Status(merr.WrapErrServiceInternal("injected load failure")), nil
	}
	for _, info := range req.GetInfos() {
		if req.GetNeedTransfer() {
			leader.mu.Lock()
			_, ok := leader.delegators[info.GetInsertChannel()]
			leader.mu.Unlock()
			if !ok {
				return merr.Status(merr.WrapErrChannelNotFound(info.GetInsertChannel())), nil
			}
		}
		if err := f.loadOne(ctx, worker, gen, info, req.GetVersion(), req.GetForceLoad()); err != nil {
			return merr.Status(err), nil
		}
		if req.GetNeedTransfer() {
			leader.mu.Lock()
			if d := leader.delegators[info.GetInsertChannel()]; d != nil {
				d.segmentDist[info.GetSegmentID()] = &querypb.SegmentDist{NodeID: workerID, Version: req.GetVersion()}
				leader.touchLocked()
			}
			leader.mu.Unlock()
		}
	}
	return merr.Success(), nil
}

func (f *Fleet) loadOne(ctx context.Context, n *QueryNode, gen int64, info *querypb.SegmentLoadInfo, version int64, force bool) error {
	f.Stats.LoadsStarted.Inc()
	n.mu.Lock()
	n.queued++
	if q := int64(n.queued); q > f.Stats.MaxNodeQueued.Load() {
		f.Stats.MaxNodeQueued.Store(q)
	}
	f.Stats.notePeakInflight(n.ID, int64(n.queued+n.loading))
	n.mu.Unlock()
	queuedAt := time.Now()
	dequeue := func() {
		n.mu.Lock()
		n.queued--
		n.mu.Unlock()
		wait := time.Since(queuedAt).Nanoseconds()
		f.Stats.QueueWaitNanos.Add(wait)
		f.Stats.QueuedLoads.Inc()
		if wait > f.Stats.MaxQueueWait.Load() {
			f.Stats.MaxQueueWait.Store(wait)
		}
	}
	n.mu.Lock()
	sem := n.sem
	n.mu.Unlock()
	select {
	case sem <- struct{}{}:
		dequeue()
	case <-ctx.Done():
		dequeue()
		f.Stats.LoadsFailed.Inc()
		f.Stats.LoadsCanceled.Inc()
		return ctx.Err()
	}
	defer func() { <-sem }()

	rows := info.GetNumOfRows()
	// QueryNode estimates a segment's memory from its binlog sizes.
	var memory int64
	for _, fb := range info.GetBinlogPaths() {
		for _, b := range fb.GetBinlogs() {
			memory += b.GetMemorySize()
		}
	}
	if memory == 0 {
		f.Stats.LoadsWithoutBinlogSize.Inc()
		memory = rows * f.broker.BytesPerRow
	}
	n.mu.Lock()
	if _, exists := n.segments[info.GetSegmentID()]; exists {
		n.mu.Unlock()
		f.Stats.LoadsSucceeded.Inc()
		return nil
	}
	// Two rejections, as in segmentLoader.requestResource: a segment that
	// would push loaded memory past the overload threshold fails with
	// SegmentRequestResourceFailed (QueryCoord then penalizes the node);
	// in-flight reservations overcommitting the node fail with
	// ServiceMemoryLimitExceeded (no penalty).
	var reserved int64
	for _, inf := range n.inflight {
		reserved += f.segmentMemory(inf)
	}
	// force_load: the threshold the QueryNode applies to a load QueryCoord
	// marked after repeated refusals (queryNode.forceLoadMemoryThresholdPercentage).
	threshold := int64(float64(n.cfg.MemoryBytes) * 0.9)
	if force {
		threshold = int64(float64(n.cfg.MemoryBytes) * paramtable.Get().QueryNodeCfg.ForceLoadMemoryThresholdPercentage.GetAsFloat())
		f.Stats.LoadsForced.Inc()
	}
	growing := f.growingBytesLocked(n)
	if n.usedMemory+growing-reserved+memory > threshold {
		mlog.Info(ctx, "sim querynode rejected load: memory threshold", mlog.Int64("node", n.ID), mlog.Int64("segmentID", info.GetSegmentID()),
			mlog.Int64("usedMB", n.usedMemory>>20), mlog.Int64("reservedMB", reserved>>20), mlog.Int64("growingMB", growing>>20),
			mlog.Int64("segmentMB", memory>>20), mlog.Int("loaded", len(n.segments)), mlog.Int("inflight", len(n.inflight)))
		n.mu.Unlock()
		f.Stats.LoadsRejectedOOM.Inc()
		f.Stats.LoadsRejectedThreshold.Inc()
		f.Stats.LoadsFailed.Inc()
		return merr.WrapErrSegmentRequestResourceFailed("Memory")
	}
	if n.usedMemory+growing+memory > n.cfg.MemoryBytes {
		n.mu.Unlock()
		f.Stats.LoadsRejectedOOM.Inc()
		f.Stats.LoadsFailed.Inc()
		return merr.WrapErrServiceMemoryLimitExceeded(float32(n.usedMemory+growing+memory), float32(n.cfg.MemoryBytes))
	}
	n.usedMemory += memory // reserve during load, as QueryNode does
	n.loading++
	n.inflight[info.GetSegmentID()] = info
	n.touchLocked()
	if l := int64(n.loading); l > f.Stats.MaxNodeLoading.Load() {
		f.Stats.MaxNodeLoading.Store(l)
	}
	n.mu.Unlock()

	latency := n.cfg.LoadBaseLatency
	if n.cfg.LoadRefLatency > 0 {
		ratio := float64(memory) / float64(max(1, n.cfg.LoadRefBytes))
		latency += time.Duration(float64(n.cfg.LoadRefLatency) * math.Pow(ratio, n.cfg.LoadSizeExponent))
	} else if n.cfg.LoadBandwidthBytes > 0 {
		perSlot := float64(n.cfg.LoadBandwidthBytes) / float64(max(1, n.cfg.LoadConcurrency))
		latency += time.Duration(float64(memory) / perSlot * float64(time.Second))
	}
	if n.cfg.LoadJitterSigma > 0 {
		n.mu.Lock()
		z := n.rng.NormFloat64()
		n.mu.Unlock()
		latency = time.Duration(float64(latency) * math.Exp(z*n.cfg.LoadJitterSigma))
	}
	err := f.sleep(ctx, n, gen, latency)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.generation != gen {
		f.Stats.LoadsFailed.Inc()
		return merr.WrapErrNodeNotAvailable(n.ID)
	}
	n.loading--
	delete(n.inflight, info.GetSegmentID())
	if err != nil {
		n.usedMemory -= memory
		f.Stats.LoadsFailed.Inc()
		if ctx.Err() != nil {
			f.Stats.LoadsCanceled.Inc()
		}
		return err
	}
	n.segments[info.GetSegmentID()] = &loadedSegment{
		info: &querypb.SegmentVersionInfo{
			ID: info.GetSegmentID(), Collection: info.GetCollectionID(), Partition: info.GetPartitionID(),
			Channel: info.GetInsertChannel(), Version: version, Level: info.GetLevel(), IsSorted: info.GetIsSorted(),
		},
		rows: rows, memory: memory, version: version,
	}
	n.touchLocked()
	f.Stats.LoadsSucceeded.Inc()
	f.Stats.LoadedRows.Add(rows)
	f.Stats.addNodeLoad(n.ID)
	f.Stats.record(Event{Kind: EventLoad, Node: n.ID, Collection: info.GetCollectionID(), Segment: info.GetSegmentID(),
		Channel: info.GetInsertChannel(), Rows: rows})
	return nil
}

func (f *Fleet) ReleaseSegments(_ context.Context, nodeID int64, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	if req.GetScope() == querypb.DataScope_Streaming {
		return merr.Success(), nil
	}
	n, _, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	workerID := req.GetNodeID()
	if workerID == 0 {
		workerID = nodeID
	}
	if req.GetNeedTransfer() {
		n.mu.Lock()
		if d := n.delegators[req.GetShard()]; d != nil {
			for _, id := range req.GetSegmentIDs() {
				if sd := d.segmentDist[id]; sd != nil && sd.GetNodeID() == workerID {
					delete(d.segmentDist, id)
				}
			}
			n.touchLocked()
		}
		n.mu.Unlock()
	}
	worker := f.node(workerID)
	if worker == nil {
		return merr.Success(), nil
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	for _, id := range req.GetSegmentIDs() {
		if s := worker.segments[id]; s != nil {
			worker.usedMemory -= s.memory
			delete(worker.segments, id)
			f.Stats.Releases.Inc()
			f.Stats.record(Event{Kind: EventRelease, Node: workerID, Collection: s.info.GetCollection(), Segment: id, Rows: s.rows})
		}
	}
	worker.touchLocked()
	return merr.Success(), nil
}

func (f *Fleet) LoadPartitions(context.Context, int64, *querypb.LoadPartitionsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (f *Fleet) ReleasePartitions(context.Context, int64, *querypb.ReleasePartitionsRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (f *Fleet) GetDataDistribution(_ context.Context, nodeID int64, _ *querypb.GetDataDistributionRequest) (*querypb.GetDataDistributionResponse, error) {
	n, _, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &querypb.GetDataDistributionResponse{
		Status:          merr.Success(),
		NodeID:          nodeID,
		LastModifyTs:    n.modifyTs,
		MemCapacityInMB: float64(n.cfg.MemoryBytes) / 1024 / 1024,
		CpuNum:          n.cfg.CPUNum,
	}
	for _, s := range n.segments {
		resp.Segments = append(resp.Segments, s.info)
	}
	for ch, d := range n.delegators {
		// QueryNode reports a delegator only once it is serviceable, i.e.
		// after WatchDmChannels finished (querynodev2 GetDataDistribution).
		if !d.watched {
			continue
		}
		resp.Channels = append(resp.Channels, &querypb.ChannelVersionInfo{Channel: ch, Collection: d.collection, Version: d.version})
		view := &querypb.LeaderView{
			Collection:    d.collection,
			Channel:       ch,
			SegmentDist:   make(map[int64]*querypb.SegmentDist, len(d.segmentDist)),
			TargetVersion: d.targetVersion,
			Status:        &querypb.LeaderViewStatus{Serviceable: d.watched},
			// Growing rows are part of the node score QueryCoord balances on.
			NumOfGrowingRows: f.growingRows(ch),
		}
		for id, sd := range d.segmentDist {
			view.SegmentDist[id] = &querypb.SegmentDist{NodeID: sd.GetNodeID(), Version: sd.GetVersion()}
		}
		resp.LeaderViews = append(resp.LeaderViews, view)
	}
	resp.TotalSegmentCount = int64(len(resp.Segments))
	resp.TotalChannelCount = int64(len(resp.Channels))
	if decorateDistribution != nil {
		decorateDistribution(n, resp)
	}
	return resp, nil
}

func (f *Fleet) GetMetrics(context.Context, int64, *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{Status: merr.Success()}, nil
}

func (f *Fleet) SyncDistribution(_ context.Context, nodeID int64, req *querypb.SyncDistributionRequest) (*commonpb.Status, error) {
	n, _, err := f.liveNode(nodeID)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	d := n.delegators[req.GetChannel()]
	if d == nil {
		return merr.Status(merr.WrapErrChannelNotFound(req.GetChannel())), nil
	}
	for _, action := range req.GetActions() {
		switch action.GetType() {
		case querypb.SyncType_Set:
			d.segmentDist[action.GetSegmentID()] = &querypb.SegmentDist{NodeID: action.GetNodeID(), Version: action.GetVersion()}
		case querypb.SyncType_Remove:
			if sd := d.segmentDist[action.GetSegmentID()]; sd != nil && sd.GetNodeID() == action.GetNodeID() {
				delete(d.segmentDist, action.GetSegmentID())
			}
		case querypb.SyncType_UpdateVersion:
			d.targetVersion = action.GetTargetVersion()
			for _, id := range action.GetDroppedInTarget() {
				delete(d.segmentDist, id)
			}
		}
	}
	n.touchLocked()
	return merr.Success(), nil
}

func (f *Fleet) GetComponentStates(_ context.Context, nodeID int64) (*milvuspb.ComponentStates, error) {
	if _, _, err := f.liveNode(nodeID); err != nil {
		return nil, err
	}
	return &milvuspb.ComponentStates{
		State:  &milvuspb.ComponentInfo{NodeID: nodeID, StateCode: commonpb.StateCode_Healthy},
		Status: merr.Success(),
	}, nil
}

func (f *Fleet) RunAnalyzer(context.Context, int64, *querypb.RunAnalyzerRequest) (*milvuspb.RunAnalyzerResponse, error) {
	return nil, unimplemented("RunAnalyzer")
}

func (f *Fleet) ValidateAnalyzer(context.Context, int64, *querypb.ValidateAnalyzerRequest) (*querypb.ValidateAnalyzerResponse, error) {
	return nil, unimplemented("ValidateAnalyzer")
}

func (f *Fleet) SyncFileResource(context.Context, int64, *internalpb.SyncFileResourceRequest) (*commonpb.Status, error) {
	return merr.Success(), nil
}

func (f *Fleet) ClearReadTaskQueue(context.Context, int64, *internalpb.ClearReadTaskQueueRequest) (*internalpb.ClearReadTaskQueueResponse, error) {
	return &internalpb.ClearReadTaskQueueResponse{Status: merr.Success()}, nil
}

func (f *Fleet) ComputePhraseMatchSlop(context.Context, int64, *querypb.ComputePhraseMatchSlopRequest) (*querypb.ComputePhraseMatchSlopResponse, error) {
	return nil, unimplemented("ComputePhraseMatchSlop")
}

func (f *Fleet) Start() {}

func (f *Fleet) Stop() {}

// growingBytesLocked is the memory of the unflushed rows held by the node's
// delegators when ChargeGrowingMemory is on. Called with n.mu held (the
// broker never takes node locks).
func (f *Fleet) growingBytesLocked(n *QueryNode) int64 {
	if !f.ChargeGrowingMemory.Load() || f.HideGrowingRows.Load() {
		return 0
	}
	var bytes int64
	for ch, d := range n.delegators {
		if d.watched {
			bytes += f.broker.GrowingBytes(d.collection, ch)
		}
	}
	return bytes
}

// SetWatchLatency changes how long a WatchDmChannels takes on one node.
func (f *Fleet) SetWatchLatency(id int64, d time.Duration) {
	if n := f.node(id); n != nil {
		n.mu.Lock()
		n.cfg.WatchLatency = d
		n.mu.Unlock()
	}
}

// InflateSegments multiplies the memory of every segment loaded on a node
// by factor, keeping rows unchanged (indexes or lazily loaded fields become
// resident; QueryCoord only sees rows). It returns the bytes added.
func (f *Fleet) InflateSegments(id int64, factor float64) int64 {
	n := f.node(id)
	if n == nil {
		return 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var added int64
	for _, s := range n.segments {
		grown := int64(float64(s.memory) * factor)
		added += grown - s.memory
		s.memory = grown
	}
	n.usedMemory += added
	n.touchLocked()
	return added
}

// MemoryCapacity returns a node's configured memory.
func (f *Fleet) MemoryCapacity(id int64) int64 {
	if n := f.node(id); n != nil {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.cfg.MemoryBytes
	}
	return 0
}

func unimplemented(method string) error {
	return merr.WrapErrServiceUnimplemented(fmt.Errorf("simulated QueryNode does not implement %s", method))
}
