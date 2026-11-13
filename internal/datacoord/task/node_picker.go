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
// A priced task uses memory admission on a resource-reporting worker. Scalar
// slots only admit unpriced tasks and tasks placed on legacy workers.
//
// Nothing gets worse for a task: one that finds no home in the first tier
// falls through to the tier that existed before.
type nodePicker struct {
	nodes []*resourceNode
	heap  typeutil.Heap[*nodeSlotEntry]
}

type resourceNode struct {
	nodeID          int64
	totalCPU        int64
	availableCPU    int64
	totalMemory     int64
	availableMemory int64
	availableSlots  int64
	// machineMemory is the base of a NodeMemoryBounded share: the worker's
	// whole machine, or totalMemory for a worker that does not report it.
	machineMemory int64
}

func newNodePicker(workerSlots map[int64]*session.WorkerSlots) *nodePicker {
	p := &nodePicker{}
	scalar := make(map[int64]*session.WorkerSlots, len(workerSlots))
	for nodeID, ws := range workerSlots {
		if ws.TotalMemory <= 0 {
			scalar[nodeID] = ws
			continue
		}
		machineMemory := ws.MachineMemory
		if machineMemory <= 0 {
			machineMemory = ws.TotalMemory
		}
		p.nodes = append(p.nodes, &resourceNode{
			nodeID:          nodeID,
			totalCPU:        ws.TotalCPU,
			availableCPU:    ws.AvailableCPU,
			totalMemory:     ws.TotalMemory,
			availableMemory: ws.AvailableMemory,
			availableSlots:  ws.AvailableSlots,
			machineMemory:   machineMemory,
		})
	}
	p.heap = newNodeSlotHeap(scalar)
	return p
}

// NodeMemoryBounded is implemented by a task whose worker never lets it hold
// more than a fixed share of the worker's memory, whatever its estimate says:
// every importv2 reader reserves from one allocator capped at
// dataNode.import.memoryLimitPercentage of the machine. The picker charges such
// a task min(estimate, share * node machine memory) on each node, the same
// bound the worker applies when it books the task. The base is the machine the
// worker reports (machine_memory), not its total_memory: in standalone the
// total carries the standalone discount and the allocator does not, and a
// share of the discounted total would charge a fraction of what the worker
// books. NodeMemoryShare returns the share,
// which may exceed 1 when the bound covers more than the allocator's buffers,
// or 0 (or less) when the task is not bounded.
type NodeMemoryBounded interface {
	NodeMemoryShare() float64
}

// nodeMemoryShare is t's NodeMemoryShare, or 0 for a task without one.
func nodeMemoryShare(t any) float64 {
	if b, ok := t.(NodeMemoryBounded); ok {
		return b.NodeMemoryShare()
	}
	return 0
}

// demand is what one task asks of a worker.
type demand struct {
	slot     int64
	resource taskcommon.Resource
	// memoryShare bounds the memory on each node to this fraction of the
	// node's machine memory; 0 leaves the estimate as it is (see
	// NodeMemoryBounded).
	memoryShare float64
}

// on is the resource the task takes on n: its estimate, with memory bounded
// by memoryShare of n's machine memory when the task has one.
func (d demand) on(n *resourceNode) taskcommon.Resource {
	r := d.resource
	if d.memoryShare > 0 && n.machineMemory > 0 {
		r.Memory = min(r.Memory, int64(float64(n.machineMemory)*d.memoryShare))
	}
	return r
}

// Pick returns the node for a task needing taskSlot slots and req resources,
// or NullNodeID when it should wait for the next round.
func (p *nodePicker) Pick(taskSlot int64, req taskcommon.Resource) int64 {
	return p.pick(demand{slot: taskSlot, resource: req})
}

// PickBounded is Pick for a task whose memory on a node is bounded by
// memoryShare of the node's total (see NodeMemoryBounded).
func (p *nodePicker) PickBounded(taskSlot int64, req taskcommon.Resource, memoryShare float64) int64 {
	return p.pick(demand{slot: taskSlot, resource: req, memoryShare: memoryShare})
}

func (p *nodePicker) pick(d demand) int64 {
	if nodeID, ok := p.pickByResource(d); ok {
		return nodeID
	}
	return pickNodeFromHeap(p.heap, d.slot)
}

// pickByResource returns ok=false when no resource-reporting worker can hold
// the task now, so the caller falls through to the scalar tier.
//
// Priced tasks use memory as the only hard filter and CPU to rank. Unpriced
// tasks retain slot admission and ranking, including on upgraded workers.
func (p *nodePicker) pickByResource(d demand) (int64, bool) {
	var (
		best      *resourceNode
		bestScore = math.Inf(-1)
	)
	for _, n := range p.nodes {
		req := d.on(n)
		var score float64
		if req.IsZero() {
			// An old coordinator or task family has no memory price. Keep its
			// original slot admission and spreading, rather than treating it as free.
			if d.slot > 0 && n.availableSlots <= 0 {
				continue
			}
			score = float64(n.availableSlots)
		} else {
			if n.availableMemory < req.Memory {
				continue
			}
			score = n.score(req)
		}
		if score > bestScore {
			best, bestScore = n, score
		}
	}
	if best == nil {
		return NullNodeID, false
	}
	best.charge(d.slot, d.on(best))
	return best.nodeID, true
}

// nodeRemaining is what one resource-reporting worker has left after the
// placements made so far in the round.
type nodeRemaining struct {
	NodeID          int64 `json:"nodeID"`
	AvailableCPU    int64 `json:"availableCPU"`
	AvailableMemory int64 `json:"availableMemory"`
	AvailableSlots  int64 `json:"availableSlots"`
}

// remaining reports the round's view of every resource-reporting worker, so a
// task that ends the round can be logged next to what it was refused by.
// Workers on the scalar tier are not listed; their slots are in the report
// the round started from.
func (p *nodePicker) remaining() []nodeRemaining {
	res := make([]nodeRemaining, 0, len(p.nodes))
	for _, n := range p.nodes {
		res = append(res, nodeRemaining{
			NodeID:          n.nodeID,
			AvailableCPU:    n.availableCPU,
			AvailableMemory: n.availableMemory,
			AvailableSlots:  n.availableSlots,
		})
	}
	return res
}

// charge books a placement on the worker for the rest of the round. Slots
// drain like the heap's: a task asking for more than is left takes what is left.
func (n *resourceNode) charge(taskSlot int64, req taskcommon.Resource) {
	n.availableCPU -= req.CPU
	if req.IsZero() && taskSlot > 0 {
		// The worker converts an unpriced task's slots to a memory booking.
		// QuerySlot does not report total slots, so we cannot mirror that
		// conversion here. Reserve the remaining memory until the next report
		// instead of offering the same bytes again to priced tasks this round.
		n.availableMemory = 0
	} else {
		n.availableMemory -= req.Memory
	}
	if taskSlot > 0 {
		n.availableSlots -= min(taskSlot, n.availableSlots)
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
