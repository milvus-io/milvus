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

package task

import (
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Score weights. Memory dominates because it is the dimension that refuses
// work; CPU spreads compute-heavy tasks; balance penalizes leaving a worker
// with lots of one dimension and none of the other (Kubernetes'
// BalancedAllocation idea).
const (
	scoreMemoryWeight  = 0.6
	scoreCPUWeight     = 0.25
	scoreBalanceWeight = 0.15
)

// nodePicker places one task per call and charges what it picked, so later
// picks in the same round see the effect.
//
// It has two tiers because a rolling upgrade has both kinds of worker at once
// and there is no honest exchange rate between a slot and a byte:
//   - workers that report cpu/memory are placed on those: memory is a hard
//     filter, cpu only ranks;
//   - workers that do not are placed by the pre-existing slot max-heap.
//
// Nothing gets worse for a task: one that finds no home in the first tier
// falls through to the tier that existed before.
type nodePicker struct {
	nodes []*resourceNode
	heap  typeutil.Heap[*nodeSlotEntry]
}

type resourceNode struct {
	nodeID          int64
	availableSlots  int64
	totalCPU        int64
	availableCPU    int64
	totalMemory     int64
	availableMemory int64
}

func newNodePicker(workerSlots map[int64]*session.WorkerSlots) *nodePicker {
	p := &nodePicker{}
	scalar := make(map[int64]*session.WorkerSlots, len(workerSlots))
	for nodeID, ws := range workerSlots {
		if ws.TotalMemory <= 0 {
			scalar[nodeID] = ws
			continue
		}
		p.nodes = append(p.nodes, &resourceNode{
			nodeID:          nodeID,
			availableSlots:  ws.AvailableSlots,
			totalCPU:        ws.TotalCPU,
			availableCPU:    ws.AvailableCPU,
			totalMemory:     ws.TotalMemory,
			availableMemory: ws.AvailableMemory,
		})
	}
	p.heap = newNodeSlotHeap(scalar)
	return p
}

// Pick returns the node for a task needing taskSlot slots and req resources,
// or NullNodeID when it should wait for the next round.
func (p *nodePicker) Pick(taskSlot int64, req taskcommon.Resource) int64 {
	if nodeID, ok := p.pickByResource(taskSlot, req); ok {
		return nodeID
	}
	if nodeID := pickNodeFromHeap(p.heap, taskSlot); nodeID != NullNodeID {
		return nodeID
	}
	// No worker of either tier can take it as it stands. Only a task that is
	// larger than any resource-reporting worker even when empty is dispatched
	// anyway: for it, waiting never helps.
	return p.pickOversized(taskSlot, req)
}

// pickByResource returns ok=false when no resource-reporting worker can hold
// the task now, so the caller falls through to the scalar tier.
func (p *nodePicker) pickByResource(taskSlot int64, req taskcommon.Resource) (int64, bool) {
	var (
		best      *resourceNode
		bestScore = math.Inf(-1)
	)
	for _, n := range p.nodes {
		if n.availableSlots <= 0 {
			// The worker's queue is full however much memory its ledger has
			// free; the scalar still carries what is merely queued there.
			continue
		}
		if n.availableMemory < req.Memory {
			// Memory gates. It is the only dimension a task is refused a
			// worker for, because exceeding it kills the process rather
			// than slowing it down.
			continue
		}
		if s := n.score(req); s > bestScore {
			best, bestScore = n, s
		}
	}
	if best == nil {
		return NullNodeID, false
	}
	best.charge(taskSlot, req)
	return best.nodeID, true
}

// pickOversized places a task that no resource-reporting worker could hold even
// when empty. Waiting for such a task never helps, so it starts where it has the
// most room and the worker's own limits pace it. A task that merely does not fit
// right now waits instead: NullNodeID.
func (p *nodePicker) pickOversized(taskSlot int64, req taskcommon.Resource) int64 {
	var largest int64
	for _, n := range p.nodes {
		largest = max(largest, n.totalMemory)
	}
	if req.Memory <= largest {
		return NullNodeID
	}
	var emptiest *resourceNode
	for _, n := range p.nodes {
		if n.availableSlots <= 0 {
			continue
		}
		if emptiest == nil || n.availableMemory > emptiest.availableMemory {
			emptiest = n
		}
	}
	if emptiest == nil {
		return NullNodeID
	}
	emptiest.charge(taskSlot, req)
	return emptiest.nodeID
}

func (n *resourceNode) charge(taskSlot int64, req taskcommon.Resource) {
	n.availableCPU -= req.CPU
	n.availableMemory -= req.Memory
	if taskSlot > 0 {
		n.availableSlots = max(n.availableSlots-taskSlot, 0)
	}
}

// score ranks a worker that already fits: how much memory and cpu would be
// left after the task, as fractions of the worker, plus how balanced the
// remainder is. Higher is better; the range is [0, 1].
func (n *resourceNode) score(req taskcommon.Resource) float64 {
	memFrac := remainingFraction(n.availableMemory-req.Memory, n.totalMemory)
	cpuFrac := remainingFraction(n.availableCPU-req.CPU, n.totalCPU)
	balance := 1.0 - math.Abs(memFrac-cpuFrac)
	return scoreMemoryWeight*memFrac + scoreCPUWeight*cpuFrac + scoreBalanceWeight*balance
}

func remainingFraction(remaining, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return math.Min(math.Max(float64(remaining)/float64(total), 0), 1)
}

// exhausted reports that no worker of either tier has a slot left, i.e. nothing
// behind the task that was just refused can be placed either. It is the only
// case in which a NullNodeID should end the scheduling round: a task refused
// because it alone does not fit must give way instead, or one oversized task at
// the head of the queue stalls every smaller task behind it.
//
// It does not mutate the picker: the slot heap is a max-heap on AvailableSlots,
// so peeking its top is enough.
func (p *nodePicker) exhausted() bool {
	for _, n := range p.nodes {
		if n.availableSlots > 0 {
			return false
		}
	}
	return p.heap.Len() == 0 || p.heap.Peek().slots.AvailableSlots <= 0
}
