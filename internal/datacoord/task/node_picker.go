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
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// ResourceAwareTask is implemented by a task that can state its requirement
// BEFORE it is dispatched, which is what lets the scheduler place it on a node
// that actually has room. A task that cannot is placed on the scalar alone,
// exactly as before.
type ResourceAwareTask interface {
	TaskResources() *datapb.TaskResources
}

// requirementOf reads a task's dimensioned requirement, or reports that it has
// none to state.
func requirementOf(task Task) (taskresource.Requirement, bool) {
	aware, ok := task.(ResourceAwareTask)
	if !ok {
		return taskresource.Requirement{}, false
	}
	return taskresource.RequirementFromProto(aware.TaskResources())
}

// nodePicker places one task per call, decrementing what it picks so later
// picks in the same round see the effect.
//
// It keeps two tiers on purpose, because a rolling upgrade has both kinds of
// worker in the cluster at once and there is no honest common currency between
// them: a scalar slot and a byte are not convertible without reintroducing the
// exchange rate this design removes.
//
//   - Workers that report dimensions are placed on the dimensions. Memory is a
//     hard filter; CPU only ranks. Filtering on CPU would serialize classes
//     that contend for no common thread pool, which is the whole reason CPU is
//     a request rather than a reservation.
//   - Workers that do not are placed by the scalar, unchanged.
//
// As nodes upgrade the first tier grows and placement gets more precise. It
// never gets worse: a task that finds no dimensioned home falls through to the
// tier that existed before.
type nodePicker struct {
	dimensioned []*dimensionedNode
	scalarHeap  scalarHeap
}

type dimensionedNode struct {
	nodeID    int64
	admitting bool
	capacity  taskresource.Capacity
	committed taskresource.Capacity
	// slots mirrors the scalar so a task with no requirement to state can still
	// be placed on a dimensioned worker.
	slots *session.WorkerSlots
}

func (n *dimensionedNode) free() taskresource.Capacity {
	return taskresource.Free(n.capacity, n.committed)
}

// newNodePicker splits the round's workers into the two tiers once, so the
// split is not recomputed per task.
func newNodePicker(workerSlots map[int64]*session.WorkerSlots, scalar scalarHeap) *nodePicker {
	p := &nodePicker{scalarHeap: scalar}
	for nodeID, ws := range workerSlots {
		capacity, committed, ok := taskresource.NodeCapacityFromProto(ws.Resources)
		if !ok {
			continue
		}
		p.dimensioned = append(p.dimensioned, &dimensionedNode{
			nodeID:    nodeID,
			admitting: ws.Resources.GetAdmitting(),
			capacity:  capacity,
			committed: committed,
			slots:     ws,
		})
	}
	return p
}

// scalarHeap is the pre-existing scalar placement, kept as the second tier.
type scalarHeap interface {
	pick(taskSlot int64) int64
}

// Pick places one task and charges the node it picked.
func (p *nodePicker) Pick(req taskresource.Requirement, hasRequirement bool, taskSlot int64) int64 {
	if hasRequirement && len(p.dimensioned) > 0 {
		if nodeID, ok := p.pickDimensioned(req, taskSlot); ok {
			return nodeID
		}
	}
	return p.scalarHeap.pick(taskSlot)
}

// pickDimensioned returns the best dimensioned home for req, or ok=false when
// there is none and the caller should fall through.
func (p *nodePicker) pickDimensioned(req taskresource.Requirement, taskSlot int64) (int64, bool) {
	var (
		best      *dimensionedNode
		bestScore float64
		roomiest  *dimensionedNode
	)
	for _, n := range p.dimensioned {
		if !n.admitting {
			// The worker has stopped taking work for a reason no dimension
			// expresses. Nothing about its numbers makes it a candidate.
			continue
		}
		if roomiest == nil || n.capacity.Memory > roomiest.capacity.Memory {
			roomiest = n
		}
		if n.slots != nil && n.slots.AvailableSlots <= 0 {
			// The dimensions cover tasks the worker has STARTED. Its scalar
			// still carries what is merely queued there -- the executors charge
			// that at enqueue -- so a node whose queue is full is not a
			// candidate however much memory the ledger says is free.
			continue
		}
		free := n.free()
		if free.Memory < req.Memory {
			// Memory gates. This is the only dimension a task can be refused a
			// node for, because exceeding it kills the process rather than
			// slowing it down.
			continue
		}
		if s := score(n, req); best == nil || s > bestScore {
			best, bestScore = n, s
		}
	}

	if best != nil {
		best.charge(req, taskSlot)
		return best.nodeID, true
	}

	// Nothing fits. Two very different reasons, and only one of them is a
	// reason to dispatch anyway.
	if roomiest != nil && req.Memory > roomiest.capacity.Memory {
		// The task is larger than the largest worker, so no amount of waiting
		// helps -- it has to run somewhere, and the emptiest worker is where it
		// will start soonest. This is the case the worker answers by running it
		// alone.
		var emptiest *dimensionedNode
		for _, n := range p.dimensioned {
			if !n.admitting {
				continue
			}
			if emptiest == nil || n.free().Memory > emptiest.free().Memory {
				emptiest = n
			}
		}
		if emptiest != nil {
			emptiest.charge(req, taskSlot)
			return emptiest.nodeID, true
		}
	}

	// The workers are merely busy. Let the task wait for the next round rather
	// than forcing it onto a node that cannot hold it.
	return 0, false
}

// charge decrements what the pick consumed, in both currencies, so later picks
// in the same round see it.
func (n *dimensionedNode) charge(req taskresource.Requirement, taskSlot int64) {
	n.committed.Memory += req.Memory
	n.committed.CPU += req.CPU
	if n.slots != nil && taskSlot > 0 {
		n.slots.AvailableSlots -= taskSlot
		if n.slots.AvailableSlots < 0 {
			n.slots.AvailableSlots = 0
		}
	}
}

// score ranks the candidates that already fit.
//
// Two terms, both as fractions of the node so that heterogeneous workers
// compare on the same footing:
//
//   - How much memory is left after this task. This is water-filling, and it is
//     weighted the highest because memory is the constraint that actually
//     refuses work.
//   - How much CPU is left after this task. This is the only place CPU enters
//     placement at all: it spreads compute-heavy work rather than excluding it.
//
// The balance term penalizes leaving a node lopsided -- lots of one dimension
// free and none of the other -- because such a node can host nothing further.
// It is the same idea as Kubernetes' BalancedAllocation.
func score(n *dimensionedNode, req taskresource.Requirement) float64 {
	const (
		memoryWeight  = 0.6
		cpuWeight     = 0.25
		balanceWeight = 0.15
	)

	free := n.free()
	memFrac := fraction(free.Memory-req.Memory, n.capacity.Memory)
	cpuFrac := fractionF(free.CPU-req.CPU, n.capacity.CPU)

	balance := 1.0 - abs(memFrac-cpuFrac)
	return memoryWeight*memFrac + cpuWeight*cpuFrac + balanceWeight*balance
}

func fraction(remaining, capacity int64) float64 {
	if capacity <= 0 {
		return 0
	}
	return clamp01(float64(remaining) / float64(capacity))
}

func fractionF(remaining, capacity float64) float64 {
	if capacity <= 0 {
		return 0
	}
	return clamp01(remaining / capacity)
}

func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
