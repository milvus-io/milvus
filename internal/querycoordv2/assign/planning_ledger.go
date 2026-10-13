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

import (
	"context"
	"math"
	"sort"
	"sync"
)

type PlanningLoadKey struct {
	NodeID, CollectionID, SegmentID int64
}

// PlanningLoad contains estimates, not an exact physical-memory contract.
type PlanningLoad struct {
	Key                    PlanningLoadKey
	From, Rows             int64
	MemoryBytes, DiskBytes int64
}

type PlanningNode struct {
	CapacityBytes, MemoryUsageBytes, DiskUsageBytes int64
	ReportedLoadingMemory, ReportedLoadingDisk      int64
	ReclaimableMemory                               bool
	Ready                                           bool
	Rows                                            int64
	CollectionRows                                  map[int64]int64
	PendingRows                                     int64
	PendingCollectionRows                           map[int64]int64
	Channels, PendingChannels                       int64
	CollectionChannels, PendingCollectionChannels   map[int64]int64
}

type planningReservation struct {
	load      PlanningLoad
	committed bool
}

type planningChannelKey struct {
	collection, from, to int64
	channel              string
}

type planningChannelReservation struct {
	rows      int64
	committed bool
}

// PlanningLedger freezes one checker invocation's baseline and shares every
// provisional reservation across source nodes, replicas, and shards. Commit
// keeps the projection until this ledger is discarded; it never rereads newly
// admitted scheduler deltas and therefore cannot count them a second time.
type PlanningLedger struct {
	mu         sync.Mutex
	nodes      map[int64]PlanningNode
	loaded     map[PlanningLoadKey]struct{}
	local      map[PlanningLoadKey]PlanningLoad
	loading    map[PlanningLoadKey]PlanningLoad
	plans      map[PlanningLoadKey]planningReservation
	channels   map[planningChannelKey]planningChannelReservation
	maxPending int
}

type planningLedgerContextKey struct{}

type lazyPlanningLedger struct {
	mu                    sync.Mutex
	factory               func() *PlanningLedger
	ledger                *PlanningLedger
	initialized, released bool
}

// Build a snapshot only when a checker actually plans work. Cached idle checks
// and balance-interval/epoch gates need not scan the complete distribution.
func WithPlanningLedgerFactory(ctx context.Context, factory func() *PlanningLedger) context.Context {
	return context.WithValue(ctx, planningLedgerContextKey{}, &lazyPlanningLedger{factory: factory})
}

// Task contexts outlive a checker invocation. Drop its frozen snapshot once
// planning/admission has finished so slow loads do not retain entire ledgers.
func ReleasePlanningLedger(ctx context.Context) {
	if lazy, ok := ctx.Value(planningLedgerContextKey{}).(*lazyPlanningLedger); ok {
		lazy.mu.Lock()
		defer lazy.mu.Unlock()
		lazy.released = true
		lazy.ledger, lazy.factory = nil, nil
	}
}

func WithPlanningLedger(ctx context.Context, ledger *PlanningLedger) context.Context {
	return context.WithValue(ctx, planningLedgerContextKey{}, ledger)
}

func PlanningLedgerFromContext(ctx context.Context) *PlanningLedger {
	if lazy, ok := ctx.Value(planningLedgerContextKey{}).(*lazyPlanningLedger); ok {
		lazy.mu.Lock()
		defer lazy.mu.Unlock()
		if lazy.released {
			return nil
		}
		if !lazy.initialized {
			lazy.initialized = true
			if lazy.factory != nil {
				lazy.ledger = lazy.factory()
			}
		}
		return lazy.ledger
	}
	ledger, _ := ctx.Value(planningLedgerContextKey{}).(*PlanningLedger)
	return ledger
}

func NewPlanningLedgerState(nodes map[int64]PlanningNode, loaded []PlanningLoadKey, pending, reported []PlanningLoad, maxPending int) *PlanningLedger {
	l := &PlanningLedger{nodes: make(map[int64]PlanningNode), loaded: make(map[PlanningLoadKey]struct{}),
		local: make(map[PlanningLoadKey]PlanningLoad), loading: make(map[PlanningLoadKey]PlanningLoad),
		plans: make(map[PlanningLoadKey]planningReservation), channels: make(map[planningChannelKey]planningChannelReservation), maxPending: max(1, maxPending)}
	for id, node := range nodes {
		node.CollectionRows = copyPlanningMap(node.CollectionRows)
		node.PendingCollectionRows = copyPlanningMap(node.PendingCollectionRows)
		node.CollectionChannels = copyPlanningMap(node.CollectionChannels)
		node.PendingCollectionChannels = copyPlanningMap(node.PendingCollectionChannels)
		l.nodes[id] = node
	}
	for _, key := range loaded {
		l.loaded[key] = struct{}{}
	}
	for _, load := range pending {
		mergePlanningLoad(l.local, load)
		mergePlanningLoad(l.loading, load)
	}
	for _, load := range reported {
		mergePlanningLoad(l.loading, load)
	}
	return l
}

func mergePlanningLoad(loads map[PlanningLoadKey]PlanningLoad, incoming PlanningLoad) {
	previous := loads[incoming.Key]
	incoming.Rows = max(previous.Rows, incoming.Rows)
	incoming.MemoryBytes = max(previous.MemoryBytes, incoming.MemoryBytes)
	incoming.DiskBytes = max(previous.DiskBytes, incoming.DiskBytes)
	loads[incoming.Key] = incoming
}

// Rows adds only QN-reported loading work not already represented by the local
// scheduler delta or actual distribution. Same-node/same-segment metadata is
// merged using its maximum estimate, rather than summing multiple reporters.
func (l *PlanningLedger) Rows(nodeID, collectionID int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rowsLocked(nodeID, collectionID)
}

func (l *PlanningLedger) rowsLocked(nodeID, collectionID int64) int64 {
	node := l.nodes[nodeID]
	rows := node.Rows + node.PendingRows
	if collectionID >= 0 {
		rows = node.CollectionRows[collectionID] + node.PendingCollectionRows[collectionID]
	}
	for key, load := range l.loading {
		if key.NodeID != nodeID || (collectionID >= 0 && key.CollectionID != collectionID) {
			continue
		}
		if _, loaded := l.loaded[key]; loaded {
			continue
		}
		rows += max(int64(0), load.Rows-l.local[key].Rows)
	}
	for _, plan := range l.plans {
		if collectionID >= 0 && plan.load.Key.CollectionID != collectionID {
			continue
		}
		if plan.load.Key.NodeID == nodeID {
			rows += plan.load.Rows
		}
		if plan.load.From == nodeID {
			rows -= plan.load.Rows // Desired final placement only, never physical headroom.
		}
	}
	for key, plan := range l.channels {
		if collectionID >= 0 && key.collection != collectionID {
			continue
		}
		if key.to == nodeID {
			rows += plan.rows
		}
		if key.from == nodeID {
			rows -= plan.rows
		}
	}
	return max(int64(0), rows)
}

func (l *PlanningLedger) ChannelScore(nodeID, collectionID int64, weight float64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	node := l.nodes[nodeID]
	score := float64(node.Channels) + float64(node.CollectionChannels[collectionID])*(math.Max(1, weight)-1)
	score += float64(node.PendingChannels + node.PendingCollectionChannels[collectionID])
	for key := range l.channels {
		delta := float64(1)
		if key.collection == collectionID {
			// A provisional plan must have the same weight as the policy's
			// in-call heap update, so later calls see that exact forecast.
			delta = math.Max(1, weight)
		}
		if key.to == nodeID {
			score += delta
		}
		if key.from == nodeID {
			score -= delta
		}
	}
	return int64(score)
}

func (l *PlanningLedger) Capacity(nodeID int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nodes[nodeID].CapacityBytes
}

// PhysicalUsage is a conservative envelope, not a measured peak. Loading
// reservations may overlap the reported resident usage. A planned Reduce never
// credits either memory or disk before the node confirms physical release.
// Disk capacity is not supplied by the current report, so disk is accounted
// for here but its admission remains QueryNode's responsibility.
func (l *PlanningLedger) PhysicalUsage(nodeID int64) (memory, disk int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.physicalUsageLocked(nodeID)
}

func (l *PlanningLedger) physicalUsageLocked(nodeID int64) (memory, disk int64) {
	node := l.nodes[nodeID]
	var loadingMemory, loadingDisk int64
	for key, load := range l.loading {
		if key.NodeID == nodeID {
			loadingMemory = saturatingPlanningAdd(loadingMemory, load.MemoryBytes)
			loadingDisk = saturatingPlanningAdd(loadingDisk, load.DiskBytes)
		}
	}
	memory = saturatingPlanningAdd(node.MemoryUsageBytes, max(node.ReportedLoadingMemory, loadingMemory))
	disk = saturatingPlanningAdd(node.DiskUsageBytes, max(node.ReportedLoadingDisk, loadingDisk))
	for key, reservation := range l.plans {
		if key.NodeID == nodeID {
			memory = saturatingPlanningAdd(memory, reservation.load.MemoryBytes)
			disk = saturatingPlanningAdd(disk, reservation.load.DiskBytes)
		}
	}
	return memory, disk
}

// PlanRecoveryLoads performs largest-first placement using the projected
// post-placement row/capacity ratio. Every accepted load immediately enters the
// same ledger, including loads from later shards or source nodes. If any node
// has an unknown capacity, compare rows uniformly instead of inventing a quota.
func (l *PlanningLedger) PlanRecoveryLoads(loads []PlanningLoad, candidates []int64, batch int) []PlanningLoad {
	if batch <= 0 {
		return nil
	}
	loads = append([]PlanningLoad(nil), loads...)
	sort.SliceStable(loads, func(i, j int) bool {
		if loads[i].Rows != loads[j].Rows {
			return loads[i].Rows > loads[j].Rows
		}
		if loads[i].Key.CollectionID != loads[j].Key.CollectionID {
			return loads[i].Key.CollectionID < loads[j].Key.CollectionID
		}
		return loads[i].Key.SegmentID < loads[j].Key.SegmentID
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	useCapacity := true
	for _, nodeID := range candidates {
		if l.nodes[nodeID].CapacityBytes <= 0 {
			useCapacity = false
			break
		}
	}
	var result []PlanningLoad
	for _, load := range loads {
		alreadyLoading := false
		for _, nodeID := range candidates {
			key := PlanningLoadKey{nodeID, load.Key.CollectionID, load.Key.SegmentID}
			_, loading := l.loading[key]
			_, planned := l.plans[key]
			if loading || planned {
				alreadyLoading = true
				break
			}
		}
		if alreadyLoading {
			continue
		}
		var selected PlanningLoad
		bestRatio := math.Inf(1)
		found := false
		for _, nodeID := range candidates {
			proposal := load
			proposal.Key.NodeID = nodeID
			if !l.canReserveLocked(proposal) {
				continue
			}
			capacity := int64(1)
			if useCapacity {
				capacity = l.nodes[nodeID].CapacityBytes
			}
			ratio := (float64(l.rowsLocked(nodeID, -1)) + float64(proposal.Rows)) / float64(capacity)
			if !found || ratio < bestRatio || (ratio == bestRatio && nodeID < selected.Key.NodeID) {
				selected, bestRatio, found = proposal, ratio, true
			}
		}
		if found {
			l.plans[selected.Key] = planningReservation{load: selected}
			result = append(result, selected)
			if len(result) >= batch {
				break
			}
		}
	}
	return result
}

func (l *PlanningLedger) HasLoading(collectionID, segmentID int64, candidates []int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, nodeID := range candidates {
		key := PlanningLoadKey{NodeID: nodeID, CollectionID: collectionID, SegmentID: segmentID}
		if _, ok := l.loading[key]; ok {
			return true
		}
		if _, ok := l.plans[key]; ok {
			return true
		}
	}
	return false
}

func (l *PlanningLedger) CanReserve(load PlanningLoad) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.canReserveLocked(load)
}

func (l *PlanningLedger) canReserveLocked(load PlanningLoad) bool {
	node, ok := l.nodes[load.Key.NodeID]
	if !ok || !node.Ready || load.Key.NodeID == load.From || load.Rows < 0 || load.MemoryBytes < 0 || load.DiskBytes < 0 {
		return false
	}
	if _, exists := l.loaded[load.Key]; exists {
		return false
	}
	if _, exists := l.loading[load.Key]; exists {
		return false
	}
	if _, exists := l.plans[load.Key]; exists {
		return false
	}
	count := 0
	for key := range l.loading {
		if key.NodeID == load.Key.NodeID {
			count++
		}
	}
	for key, plan := range l.plans {
		if key.NodeID == load.Key.NodeID {
			count++
		}
		if load.From > 0 && plan.load.From == load.From && key.CollectionID == load.Key.CollectionID && key.SegmentID == load.Key.SegmentID {
			return false // Do not propose a second target for the same source object.
		}
	}
	if count >= l.maxPending {
		return false
	}
	// Reported loading can overlap RSS. This deliberately conservative envelope
	// uses max(aggregate report, unique per-segment estimates), never their sum.
	peak, _ := l.physicalUsageLocked(load.Key.NodeID)
	peak = saturatingPlanningAdd(peak, load.MemoryBytes)
	if node.ReclaimableMemory || node.CapacityBytes <= 0 || peak <= node.CapacityBytes {
		return true
	}
	// Raw-binlog estimates can exceed the real mmap/index residency. Permit
	// one advisory oversize probe on an otherwise idle node with real headroom
	// and let QueryNode enforce native admission. Its reservation remains in
	// the envelope, so no second Grow can use the same headroom in this batch.
	return count == 0 && saturatingPlanningAdd(node.MemoryUsageBytes, node.ReportedLoadingMemory) < node.CapacityBytes
}

func (l *PlanningLedger) Reserve(load PlanningLoad) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.canReserveLocked(load) {
		return false
	}
	l.plans[load.Key] = planningReservation{load: load}
	return true
}

func (l *PlanningLedger) Undo(key PlanningLoadKey) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	plan, ok := l.plans[key]
	if !ok || plan.committed {
		return false
	}
	delete(l.plans, key)
	return true
}

func (l *PlanningLedger) Commit(key PlanningLoadKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	plan, ok := l.plans[key]
	if ok {
		plan.committed = true
		l.plans[key] = plan
	}
}

func copyPlanningMap(source map[int64]int64) map[int64]int64 {
	result := make(map[int64]int64, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func saturatingPlanningAdd(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
