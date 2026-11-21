package task

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	pickGiB = int64(1) << 30
)

func resourceWorker(id int64, slots, cpu, mem int64) *session.WorkerSlots {
	return &session.WorkerSlots{
		NodeID: id, AvailableSlots: slots,
		TotalCPU: cpu, AvailableCPU: cpu, TotalMemory: mem, AvailableMemory: mem,
	}
}

func TestNodePicker_MemoryIsAHardFilter(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 4*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	// 6GiB only fits node 2, even though node 1 has the same cpu and slots.
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}))
}

func TestNodePicker_ScoresLeastLoaded(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	// Pre-load node 1: it now has less of both left, so node 2 wins.
	for _, n := range p.nodes {
		if n.nodeID == 1 {
			n.availableMemory -= 8 * pickGiB
			n.availableCPU -= 4
		}
	}
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_ChargesWithinRound(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	req := taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}
	first := p.Pick(1, req)
	second := p.Pick(1, req)
	// Water-filling: two identical workers get one task each.
	assert.NotEqual(t, first, second)
	// Each has 10GiB left; a third 6GiB task fits on either, a fourth on neither.
	assert.NotEqual(t, int64(NullNodeID), p.Pick(1, req))
	assert.NotEqual(t, int64(NullNodeID), p.Pick(1, req))
	assert.Equal(t, int64(NullNodeID), p.Pick(1, req))
}

func TestNodePicker_CPUOnlyRanks(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 2, 64*pickGiB),
	})
	// 8-core request on a 2-core worker is still placed: cpu never refuses.
	assert.Equal(t, int64(1), p.Pick(1, taskcommon.Resource{CPU: 8, Memory: pickGiB}))
	assert.Equal(t, int64(1), p.Pick(1, taskcommon.Resource{CPU: 8, Memory: pickGiB}))
}

func TestNodePicker_MemoryTierIgnoresExhaustedScalarSlots(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 64*pickGiB),
		2: resourceWorker(2, 10, 8, 8*pickGiB),
	})
	req := taskcommon.Resource{CPU: 1, Memory: pickGiB}
	// The memory-rich worker can still execute this task despite having no
	// scalar slots. New workers admit index tasks by memory too.
	byID := make(map[int64]*resourceNode, len(p.nodes))
	for _, n := range p.nodes {
		byID[n.nodeID] = n
	}
	assert.Greater(t, byID[1].score(req), byID[2].score(req))
	assert.Equal(t, int64(1), p.Pick(1, req))
	assert.Equal(t, int64(0), byID[1].availableSlots)
	assert.Equal(t, 63*pickGiB, byID[1].availableMemory)

	// A zero-slot task also follows resource scores; the previous charge now
	// makes node 2 preferable on CPU/balance.
	assert.Equal(t, int64(2), p.Pick(0, req))
}

func TestNodePicker_MemoryBoundsTheRoundAfterSlotsRunOut(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 2, 8, 4*pickGiB),
	})
	req := taskcommon.Resource{CPU: 8, Memory: pickGiB}
	assert.Equal(t, int64(1), p.Pick(128, req))
	assert.Equal(t, int64(1), p.Pick(128, req))
	// Slots are spent; the remaining memory still admits two tasks.
	assert.Equal(t, int64(1), p.Pick(128, req))
	assert.Equal(t, int64(1), p.Pick(128, req))
	assert.Equal(t, int64(NullNodeID), p.Pick(128, req))
}

func TestNodePicker_ScalarSlotsDoNotOverrideResourceScore(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 2, 8, 64*pickGiB),
		2: resourceWorker(2, 8, 8, 8*pickGiB),
	})
	byID := make(map[int64]*resourceNode, len(p.nodes))
	for _, n := range p.nodes {
		byID[n.nodeID] = n
	}
	req := taskcommon.Resource{CPU: 1, Memory: pickGiB}
	// A higher resource score wins even without enough scalar slots.
	assert.Equal(t, int64(1), p.Pick(4, req))
	// The next charge spreads CPU onto node 2, regardless of slot demand.
	assert.Equal(t, int64(2), p.Pick(16, req))
	assert.Equal(t, int64(0), byID[1].availableSlots)
	assert.Equal(t, int64(0), byID[2].availableSlots)
	assert.Equal(t, int64(1), p.Pick(16, req))
}

func TestNodePicker_OversizedWaitsForIdleWorker(t *testing.T) {
	workers := map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 8*pickGiB),
		2: resourceWorker(2, 0, 8, 16*pickGiB),
	}
	workers[1].AvailableMemory--
	workers[2].AvailableMemory--
	req := taskcommon.Resource{CPU: 8, Memory: 32 * pickGiB}
	p := newNodePicker(workers)
	before := p.remaining()
	assert.Equal(t, int64(NullNodeID), p.Pick(128, req))
	assert.Equal(t, before, p.remaining())
	// Idle, with unchanged capacity and no scalar slots: exactly one
	// oversized task gets this worker, including against small followers.
	workers[2].AvailableMemory = workers[2].TotalMemory
	workers[2].Idle = true
	p = newNodePicker(workers)
	assert.Equal(t, int64(2), p.Pick(128, req))
	for _, n := range p.nodes {
		if n.nodeID == 2 {
			assert.Equal(t, -16*pickGiB, n.availableMemory)
			assert.True(t, n.exclusive)
		}
	}
	workers[1].AvailableMemory = 0
	for _, n := range p.nodes {
		if n.nodeID == 1 {
			n.availableMemory = 0
			n.availableSlots = 0
		}
	}
	assert.Equal(t, int64(NullNodeID), p.Pick(1, req))
	assert.Equal(t, int64(NullNodeID), p.Pick(1, taskcommon.Resource{Memory: 1}))
	// Isolate the selected worker: a small task cannot bypass exclusivity.
	for _, n := range p.nodes {
		if n.nodeID == 2 {
			p.nodes = []*resourceNode{n}
			break
		}
	}
	assert.Equal(t, int64(NullNodeID), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 1}))
	// A CPU booking with no memory booking is not idle either.
	workers[2].AvailableCPU--
	workers[2].Idle = false
	assert.Equal(t, int64(NullNodeID), newNodePicker(workers).Pick(1, req))
}

func TestNodePicker_HeadCanRunAloneOnSmallerIdleWorker(t *testing.T) {
	workers := map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 8*pickGiB),
		2: resourceWorker(2, 10, 8, 64*pickGiB),
	}
	workers[2].AvailableMemory = 4 * pickGiB
	req := taskcommon.Resource{CPU: 1, Memory: 32 * pickGiB}
	workers[1].AvailableMemory = pickGiB
	assert.Equal(t, int64(NullNodeID), newNodePicker(workers).Pick(1, req))
	workers[1].AvailableMemory = workers[1].TotalMemory
	workers[1].Idle = true
	id, charged := newNodePicker(workers).PickBounded(1, req, 0)
	assert.Equal(t, int64(1), id)
	assert.Equal(t, req, charged)
}

func TestNodePicker_BusyWaits(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
	})
	p.nodes[0].availableMemory = 2 * pickGiB
	// 8GiB fits an empty node 1 but not now: wait for the next round.
	assert.Equal(t, int64(NullNodeID), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 8 * pickGiB}))
}

func TestNodePicker_ScalarWorkersUseTheHeap(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 80},
	})
	assert.Empty(t, p.nodes)
	assert.Equal(t, int64(2), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_MixedClusterFallsThrough(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 4*pickGiB),
		2: {NodeID: 2, AvailableSlots: 80},
	})
	// Too big for the only resource-reporting worker: the scalar worker takes it.
	assert.Equal(t, int64(2), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}))
	// Fits: the resource-reporting worker is preferred.
	assert.Equal(t, int64(1), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_EmptyPool(t *testing.T) {
	empty := newNodePicker(nil)
	assert.Equal(t, int64(NullNodeID), empty.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_Score(t *testing.T) {
	n := &resourceNode{totalCPU: 8, availableCPU: 8, totalMemory: 16 * pickGiB, availableMemory: 16 * pickGiB}
	empty := n.score(taskcommon.Resource{})
	half := n.score(taskcommon.Resource{CPU: 4, Memory: 8 * pickGiB})
	assert.InDelta(t, 1.0, empty, 1e-9)
	assert.InDelta(t, 0.6*0.5+0.25*0.5+0.15*1.0, half, 1e-9)
	// Lopsided (all cpu gone, memory untouched) scores below balanced.
	lopsided := n.score(taskcommon.Resource{CPU: 8})
	assert.Less(t, lopsided, empty)
	// An extra four cores on an eight-core worker reduce only the CPU term.
	assert.InDelta(t, lopsided-0.25*0.5, n.score(taskcommon.Resource{CPU: 12}), 1e-9)
}

func TestNodePicker_RanksCPUOvercommit(t *testing.T) {
	light := resourceWorker(1, 0, 8, 3*pickGiB)
	heavy := resourceWorker(2, 0, 8, 4*pickGiB)
	light.AvailableCPU = -2
	heavy.AvailableCPU = -16
	p := newNodePicker(map[int64]*session.WorkerSlots{1: light, 2: heavy})
	req := taskcommon.Resource{CPU: 2, Memory: 2 * pickGiB}

	// The more heavily booked worker has more free memory, but both fit.
	first, _ := p.PickBounded(1, req, 0)
	assert.Equal(t, light.NodeID, first)
	// Only the more heavily booked worker now has enough memory.
	second, _ := p.PickBounded(1, req, 0)
	assert.Equal(t, heavy.NodeID, second)
}

func TestNodePicker_ScoreWithoutCPUReport(t *testing.T) {
	// A worker that reports memory but no cpu total is still placed on memory;
	// its cpu fraction contributes nothing instead of dividing by zero.
	n := &resourceNode{totalCPU: 0, availableCPU: 0, totalMemory: 16 * pickGiB, availableMemory: 16 * pickGiB}
	assert.InDelta(t, 0.6*1.0+0.25*0+0.15*0, n.score(taskcommon.Resource{}), 1e-9)
}

func TestNodePicker_RemainingReportsTheRoundsView(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 4, 8, 16*pickGiB),
		2: {NodeID: 2, AvailableSlots: 10}, // scalar tier: not listed
	})
	assert.Equal(t, int64(1), p.Pick(1, taskcommon.Resource{CPU: 2, Memory: 6 * pickGiB}))
	// What a task that ends the round is logged against: the worker's report
	// minus the placements already made in this round.
	assert.Equal(t, []nodeRemaining{
		{NodeID: 1, AvailableCPU: 6, AvailableMemory: 10 * pickGiB, AvailableSlots: 3},
	}, p.remaining())
	assert.Empty(t, newNodePicker(nil).remaining())
}

// TestNodePicker_BoundedMemoryShare: a task whose worker caps it at a share of
// the node's memory is charged that cap on each node, not its estimate. A
// 32GiB estimate bounded at 10% fits a 64GiB node as 6.4GiB and is charged
// 6.4GiB; without the bound it would wait for a node with 32GiB free.
func TestNodePicker_BoundedMemoryShare(t *testing.T) {
	newPicker := func() *nodePicker {
		return newNodePicker(map[int64]*session.WorkerSlots{
			1: resourceWorker(1, 10, 8, 64*pickGiB),
		})
	}
	req := taskcommon.Resource{CPU: 1, Memory: 32 * pickGiB}
	nodeMemory := 64 * pickGiB
	capped := int64(float64(nodeMemory) * 0.1)

	p := newPicker()
	p.nodes[0].availableMemory = 10 * pickGiB
	assert.Equal(t, int64(NullNodeID), p.Pick(1, req), "unbounded: 32GiB does not fit 10GiB free")
	assert.Equal(t, int64(1), pickBoundedID(p, 1, req, 0.1))
	assert.Equal(t, 10*pickGiB-capped, p.nodes[0].availableMemory, "charged the bound")

	// A bound above the estimate leaves the estimate.
	p = newPicker()
	small := taskcommon.Resource{CPU: 1, Memory: pickGiB}
	assert.Equal(t, int64(1), pickBoundedID(p, 1, small, 0.1))
	assert.Equal(t, 63*pickGiB, p.nodes[0].availableMemory)

	// A share of zero or less is no bound.
	for _, share := range []float64{0, -0.5} {
		p = newPicker()
		p.nodes[0].availableMemory = 10 * pickGiB
		assert.Equal(t, int64(NullNodeID), pickBoundedID(p, 1, req, share), share)
	}

	// A bound above the node capacity uses idle-worker exclusive admission.
	p = newPicker()
	p.nodes[0].idle = true
	huge := taskcommon.Resource{CPU: 1, Memory: 200 * pickGiB}
	assert.Equal(t, int64(1), pickBoundedID(p, 1, huge, 1.5))
	assert.Equal(t, -32*pickGiB, p.nodes[0].availableMemory)
	assert.Equal(t, int64(NullNodeID), pickBoundedID(p, 1, huge, 1.5))
}

// A standalone worker reports its discounted capacity (8GiB x 0.25 = 2GiB).
// The co-located coordinator reads the 8GiB machine size locally and bounds
// import's price to its share of that size. The worker books the selected price
// unchanged: one 1.6GiB import fits the 2GiB capacity; the second waits.
func TestNodePicker_BoundedShareOfTheMachine(t *testing.T) {
	standalone := resourceWorker(1, 10, 2, 2*pickGiB)
	paramtable.Init()
	paramtable.SetRole(typeutil.StandaloneRole)
	defer paramtable.SetRole("")
	memory := mockey.Mock(hardware.GetMemoryCount).Return(uint64(8 * pickGiB)).Build()
	defer memory.UnPatch()
	p := newNodePicker(map[int64]*session.WorkerSlots{1: standalone})
	req := taskcommon.Resource{CPU: 1, Memory: 4 * pickGiB}
	share := 0.2 // 10% x importMemoryFactor 2
	booked := int64(float64(8*pickGiB) * share)

	assert.Equal(t, booked, demand{slot: 1, resource: req, memoryShare: share}.on(p.nodes[0]).Memory)
	assert.Equal(t, int64(1), pickBoundedID(p, 1, req, share))
	assert.Equal(t, 2*pickGiB-booked, p.nodes[0].availableMemory)
	assert.Equal(t, int64(NullNodeID), pickBoundedID(p, 1, req, share), "the second does not fit what is left")

	// Cluster workers expose their full capacity.
	paramtable.SetRole("")
	cluster := newNodePicker(map[int64]*session.WorkerSlots{1: resourceWorker(1, 10, 2, 2*pickGiB)})
	assert.Equal(t, int64(float64(2*pickGiB)*share), demand{slot: 1, resource: req, memoryShare: share}.on(cluster.nodes[0]).Memory)
}

// Busy workers cannot take the idle-worker exclusive path. A bounded task
// waits for enough free memory, and its memory bound differs per worker.
func TestNodePicker_BoundedWaitsForBusyWorkers(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 8*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	for _, n := range p.nodes {
		n.availableMemory = 0
	}
	huge := taskcommon.Resource{CPU: 1, Memory: 100 * pickGiB}
	assert.Equal(t, int64(NullNodeID), pickBoundedID(p, 1, huge, 0.5), "waits: both workers are busy and lack free memory")

	for _, n := range p.nodes {
		n.availableMemory = 4 * pickGiB
	}
	// Half of node 1 is 4GiB and fits; half of node 2 is 8GiB and does not.
	assert.Equal(t, int64(1), pickBoundedID(p, 1, huge, 0.5))
}

type boundedTask struct {
	Task
	share float64
}

func (b boundedTask) NodeMemoryShare() float64 { return b.share }

func TestNodeMemoryShare(t *testing.T) {
	assert.Equal(t, 0.1, nodeMemoryShare(boundedTask{share: 0.1}))
	assert.Zero(t, nodeMemoryShare(struct{ Task }{}))
}

func pickBoundedID(p *nodePicker, slots int64, resource taskcommon.Resource, share float64) int64 {
	id, _ := p.PickBounded(slots, resource, share)
	return id
}

func TestNodePicker_ReturnsNodeSpecificCharge(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{1: resourceWorker(1, 16, 16, 10*pickGiB)})
	req := taskcommon.Resource{CPU: 3, Memory: 20 * pickGiB}
	node, charged := p.PickBounded(1, req, 0.2)
	assert.Equal(t, int64(1), node)
	assert.Equal(t, taskcommon.Resource{CPU: 3, Memory: 2 * pickGiB}, charged)
	assert.Equal(t, 8*pickGiB, p.nodes[0].availableMemory)
}

func (p *nodePicker) Pick(slots int64, resource taskcommon.Resource) int64 {
	id, _ := p.PickBounded(slots, resource, 0)
	return id
}
