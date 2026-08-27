package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
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

func TestNodePicker_SlotsExhaustedSkipsWorker(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 64*pickGiB),
		2: resourceWorker(2, 10, 8, 8*pickGiB),
	})
	// Node 1 has the memory but its queue is full (scalar 0): node 2 is picked.
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_OversizedGoesToEmptiest(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 8*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	p.nodes[0].availableMemory, p.nodes[1].availableMemory = 6*pickGiB, 5*pickGiB
	emptiest := p.nodes[0].nodeID
	// 32GiB fits nowhere even empty: dispatch to whoever has most free memory now.
	assert.Equal(t, emptiest, p.Pick(1, taskcommon.Resource{CPU: 8, Memory: 32 * pickGiB}))
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

func TestNodePicker_ZeroRequirement(t *testing.T) {
	onlyResource := newNodePicker(map[int64]*session.WorkerSlots{1: resourceWorker(1, 10, 8, 16*pickGiB)})
	// A task that did not price itself is still placed rather than starved.
	assert.Equal(t, int64(1), onlyResource.Pick(1, taskcommon.Resource{}))

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
	// Over-subscribed cpu clamps at 0 instead of going negative.
	assert.InDelta(t, 0.6*1.0+0.25*0+0.15*0, n.score(taskcommon.Resource{CPU: 100}), 1e-9)
}

func TestNodePicker_OversizedSkipsFullQueues(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 8*pickGiB),
		2: resourceWorker(2, 10, 8, 4*pickGiB),
	})
	// 32GiB fits nowhere even empty, and node 1 has the most free memory, but
	// its queue is full: the oversized task goes to the one that can take it.
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 32 * pickGiB}))
}

func TestNodePicker_ScoreWithoutCPUReport(t *testing.T) {
	// A worker that reports memory but no cpu total is still placed on memory;
	// its cpu fraction contributes nothing instead of dividing by zero.
	n := &resourceNode{totalCPU: 0, availableCPU: 0, totalMemory: 16 * pickGiB, availableMemory: 16 * pickGiB}
	assert.InDelta(t, 0.6*1.0+0.25*0+0.15*0, n.score(taskcommon.Resource{}), 1e-9)
}

func TestNodePicker_Exhausted(t *testing.T) {
	// A worker with slots left but no room for this task: the cluster is not
	// exhausted, so the round must go on to the tasks behind it.
	busy := newNodePicker(map[int64]*session.WorkerSlots{1: resourceWorker(1, 10, 8, 8*pickGiB)})
	busy.nodes[0].availableMemory = 0
	assert.Equal(t, int64(NullNodeID), busy.Pick(1, taskcommon.Resource{Memory: 4 * pickGiB}))
	assert.False(t, busy.exhausted())

	// The dimensioned worker's queue is full but a scalar worker still has slots.
	mixed := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 8*pickGiB),
		2: {NodeID: 2, AvailableSlots: 4},
	})
	assert.False(t, mixed.exhausted())
	// Peeking must leave the heap usable.
	assert.Equal(t, int64(2), mixed.Pick(1, taskcommon.Resource{Memory: pickGiB}))

	// No slot anywhere, of either kind.
	none := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 8*pickGiB),
		2: {NodeID: 2, AvailableSlots: 0},
	})
	assert.True(t, none.exhausted())
	assert.True(t, newNodePicker(nil).exhausted())
}
