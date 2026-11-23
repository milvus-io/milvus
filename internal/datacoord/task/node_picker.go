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
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
// Every local task has a resource price. Scalar admission is needed only
// for workers that do not report memory capacity.
//
// If neither tier can place the head task, an idle worker can run it alone.
// Otherwise worker dispatch pauses, preserving the waiting task's priority.
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
	exclusive       bool // an oversized task owns this worker for the rest of the round
	idle            bool
	machineMemory   int64
}

func newNodePicker(workerSlots map[int64]*session.WorkerSlots) *nodePicker {
	p := &nodePicker{}
	scalar := make(map[int64]*session.WorkerSlots, len(workerSlots))
	for nodeID, ws := range workerSlots {
		if ws.TotalMemory <= 0 {
			scalar[nodeID] = ws
			continue
		}
		machineMemory := ws.TotalMemory
		if paramtable.GetRole() == typeutil.StandaloneRole {
			machineMemory = int64(hardware.GetMemoryCount())
		}
		p.nodes = append(p.nodes, &resourceNode{
			nodeID:          nodeID,
			totalCPU:        ws.TotalCPU,
			availableCPU:    ws.AvailableCPU,
			totalMemory:     ws.TotalMemory,
			availableMemory: ws.AvailableMemory,
			availableSlots:  ws.AvailableSlots,
			machineMemory:   machineMemory,
			idle:            ws.Idle,
		})
	}
	p.heap = newNodeSlotHeap(scalar)
	return p
}

// NodeMemoryBounded caps a task's estimate to a share of the worker's memory.
// Cluster workers expose their full capacity; standalone shares this process
// and its allocator is sized on the local machine rather than its slot discount.
type NodeMemoryBounded interface {
	NodeMemoryShare() float64
}

// nodeMemoryShare is t's NodeMemoryShare, or 0 for a task without one.
func nodeMemoryShare(t Task) float64 {
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
	if d.memoryShare > 0 {
		r.Memory = min(r.Memory, int64(float64(n.machineMemory)*d.memoryShare))
	}
	return r
}

// PickBounded returns a worker and the price to charge it. A positive
// memoryShare caps the estimate at that share of the worker's machine memory
// (see NodeMemoryBounded).
func (p *nodePicker) PickBounded(taskSlot int64, req taskcommon.Resource, memoryShare float64) (int64, taskcommon.Resource) {
	return p.pick(demand{slot: taskSlot, resource: req, memoryShare: memoryShare})
}

func (p *nodePicker) pick(d demand) (int64, taskcommon.Resource) {
	if nodeID, resource := p.pickByResource(d); nodeID != NullNodeID {
		return nodeID, resource
	}
	if nodeID := pickNodeFromHeap(p.heap, d.slot); nodeID != NullNodeID {
		return nodeID, d.resource
	}
	return p.pickIdle(d)
}

// pickByResource returns NullNodeID when no resource-reporting worker can hold
// the task now, so the caller falls through to the scalar tier.
//
// Tasks use memory as the only hard filter and CPU to rank.
func (p *nodePicker) pickByResource(d demand) (int64, taskcommon.Resource) {
	var (
		best      *resourceNode
		bestScore = math.Inf(-1)
	)
	for _, n := range p.nodes {
		if n.exclusive {
			continue
		}
		req := d.on(n)
		if n.availableMemory < req.Memory {
			continue
		}
		score := n.score(req)
		if score > bestScore {
			best, bestScore = n, score
		}
	}
	if best == nil {
		return NullNodeID, taskcommon.Resource{}
	}
	resource := d.on(best)
	best.charge(d.slot, resource)
	return best.nodeID, resource
}

// pickIdle guarantees progress for the head task when no worker has enough
// unbooked memory. The scheduler waits for an idle worker to run it alone.
func (p *nodePicker) pickIdle(d demand) (int64, taskcommon.Resource) {
	var best *resourceNode
	for _, n := range p.nodes {
		if n.exclusive || !n.idle {
			continue
		}
		if best == nil || n.totalMemory > best.totalMemory || (n.totalMemory == best.totalMemory && n.nodeID < best.nodeID) {
			best = n
		}
	}
	if best == nil {
		return NullNodeID, taskcommon.Resource{}
	}
	resource := d.on(best)
	best.charge(d.slot, resource)
	best.exclusive = true
	return best.nodeID, resource
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
// deferred task can be logged next to what it was refused by.
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
	n.idle = false
	n.availableCPU -= req.CPU
	n.availableMemory -= req.Memory
	if taskSlot > 0 {
		n.availableSlots -= min(taskSlot, n.availableSlots)
	}
}

// score ranks a worker that already fits: how much memory and cpu would be
// left after the task, as fractions of the worker, plus how balanced the
// remainder is. CPU overcommit lowers only the CPU term and can make the score negative.
func (n *resourceNode) score(req taskcommon.Resource) float64 {
	memFrac := remainingFraction(n.availableMemory-req.Memory, n.totalMemory)
	cpuFrac := remainingFraction(n.availableCPU-req.CPU, n.totalCPU)
	balance := 1.0 - math.Abs(memFrac-max(cpuFrac, 0))
	return scoreMemoryWeight*memFrac + scoreCPUWeight*cpuFrac + scoreBalanceWeight*balance
}

func remainingFraction(remaining, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return min(float64(remaining)/float64(total), 1)
}
