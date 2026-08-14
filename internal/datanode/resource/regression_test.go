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

package resource

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The three tests below are issue #52180's incidents, replayed against the
// ledger with the incidents' own numbers and the production estimators. They
// are deliberately not written against hand-picked requirements: what failed on
// those nodes was the *pricing* as much as the accounting, so a fixture that
// invented its own per-task figure would leave half the regression unpinned.
//
// All three ran on the same shape of node. 128 legacy slots is
// min(cores/2, memory/8GiB) x WorkerSlotUnit(16) x BuildParallel(1), so the
// node had 16 cores and 64GiB -- which is also what makes "taskSlot=384" and
// "indexStatsUsed=400" the impossible figures they are.
const (
	gib = int64(1) << 30

	incidentNodeCores  = 16
	incidentNodeMemory = 64 * gib

	// incidentNodeLegacySlots is what the old scheme thought this node was
	// worth. It has no memory dimension at all, which is the root of all three
	// incidents.
	incidentNodeLegacySlots = 128
)

// gibFloat is a variable rather than a constant so the fractional incident
// figures below (36.09GiB and friends) can be written as they were reported.
var gibFloat = float64(gib)

// mockIncidentNode makes taskresource.NodeCapacity and the watermark sampler
// see the node from issue #52180, with usedMemory bytes already resident.
func mockIncidentNode(t *testing.T, usedMemory int64) {
	t.Helper()
	paramtable.Init()

	mkCPU := mockey.Mock(hardware.GetCPUNum).Return(incidentNodeCores).Build()
	t.Cleanup(func() { mkCPU.UnPatch() })
	mkTotal := mockey.Mock(hardware.GetMemoryCount).Return(uint64(incidentNodeMemory)).Build()
	t.Cleanup(func() { mkTotal.UnPatch() })
	mkUsed := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(usedMemory)).Build()
	t.Cleanup(func() { mkUsed.UnPatch() })
}

// Incident 1: one DataNode started eight Vortex (storage v3) mix compactions
// holding 36.09GiB of combined input, charged 32 of its 128 slots, and went
// from 2.5GiB to 32GiB of heap in about two and a half minutes -- reporting
// availableSlots=48 as it was OOM-killed. Those 48 slots are the defect: the
// node was advertising room for two dozen more of the very tasks that were
// killing it, because slots do not measure memory.
//
// What the ledger guarantees is that the node stops. The eight themselves fit
// inside the budget and are admitted; the burst behind them is not.
func TestIncident1MixCompactionBurstStopsAtTheBudget(t *testing.T) {
	mockIncidentNode(t, 0)
	g := NewGuard()

	// One of the incident's eight, priced by the production estimator from the
	// input it actually carries.
	combinedInput := int64(36.09 * gibFloat)
	per := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: combinedInput / 8,
	})
	require.Greater(t, per.Memory, 4*gib,
		"setup: a 4.5GiB share of the input must be priced in GiB, not in a constant")

	// The legacy scheme charged 4 slots for each of these, so it would have run
	// 32 of them concurrently on this node. Offer exactly that many.
	const legacyAdmissions = incidentNodeLegacySlots / 4
	admitted := 0
	for i := int64(1); i <= legacyAdmissions; i++ {
		if ok, _ := g.TryAcquire(i, taskcommon.Compaction, per); ok {
			admitted++
		}
	}

	assert.Less(t, admitted, legacyAdmissions,
		"the node must stop short of what slot accounting allowed")
	assert.GreaterOrEqual(t, admitted, 8,
		"the incident's own eight fit the budget; it is the tail that must be refused")

	snap := g.Snapshot()
	assert.Equal(t, per.Memory*int64(admitted), snap.Reserved.Memory,
		"every admitted task is charged, once")
	assert.LessOrEqual(t, snap.Reserved.Memory, snap.Total.Memory,
		"the ledger never commits more than the budget")
	assert.Greater(t, snap.Reserved.Memory+per.Memory, snap.Total.Memory,
		"and it stops because the budget is full, not because of some unrelated cap")
}

// Incident 2, first half: an HNSW build over 1,163,739 rows at dim 768 was
// priced taskSlot=384 against a node reporting totalSlots=128, after which the
// node reported indexStatsUsed=400 -- an occupancy three times its own size,
// which no admission rule can act on. Priced from the field it reads, the same
// build is an ordinary task that fits, and the node's occupancy stays a figure
// that means something.
func TestIncident2HNSWBuildIsPricedInMemoryNotSlots(t *testing.T) {
	mockIncidentNode(t, 0)

	const (
		rows = int64(1163739)
		dim  = int64(768)
	)
	req := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: rows * dim * 4, // float32: 3.33GiB
		StorageVersion:  3,
	})

	capacity := taskresource.NodeCapacity()
	require.True(t, req.FitsIn(capacity),
		"the build that was priced at three nodes must fit on one")
	assert.LessOrEqual(t, req.CPU, float64(incidentNodeCores)/4,
		"CPU is charged per build, not per row: knowhere's build pool is fixed")

	g := NewGuard()
	ok, _ := g.TryAcquire(1, taskcommon.Index, req)
	require.True(t, ok)

	snap := g.Snapshot()
	assert.Equal(t, req.Memory, snap.Reserved.Memory)
	assert.LessOrEqual(t, snap.Reserved.Memory, snap.Total.Memory,
		"occupancy can no longer exceed the node the way indexStatsUsed=400 did")
	assert.Zero(t, snap.ExclusiveTaskID,
		"a correctly priced build does not take the node hostage")

	// The node is neither falsely full nor infinitely deep: the same estimator
	// decides what may join it, and the ledger stops the queue at the budget.
	joined := 0
	for i := int64(2); i <= int64(incidentNodeLegacySlots); i++ {
		if ok, _ := g.TryAcquire(i, taskcommon.Index, req); ok {
			joined++
		}
	}
	assert.Greater(t, joined, 0, "one build must not fill a 64GiB node")
	snap = g.Snapshot()
	assert.LessOrEqual(t, snap.Reserved.Memory, snap.Total.Memory)
	assert.Greater(t, snap.Reserved.Memory+req.Memory, snap.Total.Memory)
}

// Incident 2, second half: a task that genuinely does not fit the node is no
// longer refused -- a human ruling replaced refusal with exclusive execution --
// so what must hold is that it never runs *alongside* anything, in either
// direction. The oversized task here is priced by the same estimator, from a
// field big enough that no node of this size can hold the build.
func TestIncident2OversizedBuildRunsAloneNeverAlongside(t *testing.T) {
	mockIncidentNode(t, 0)
	g := NewGuard()

	oversized := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: 40 * gib,
		StorageVersion:  3,
	})
	require.False(t, oversized.FitsIn(taskresource.NodeCapacity()),
		"setup: this build must exceed the whole node")

	// Someone else holds part of the node: the oversized build waits.
	ok, _ := g.TryAcquire(1, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 4 * gib})
	require.True(t, ok)

	ok, _ = g.TryAcquire(2, taskcommon.Index, oversized)
	assert.False(t, ok, "an oversized build must not join a node that is already busy")
	assert.Zero(t, g.Snapshot().ExclusiveTaskID)

	// Once the node drains it runs -- and then excludes everyone else.
	g.Release(1)
	ok, _ = g.TryAcquire(2, taskcommon.Index, oversized)
	require.True(t, ok, "a drained node admits it")
	assert.Equal(t, int64(2), g.Snapshot().ExclusiveTaskID)

	ok, _ = g.TryAcquire(3, taskcommon.Compaction, taskresource.Requirement{CPU: 1, Memory: 1})
	assert.False(t, ok, "nothing joins an oversized task, however small")
}

// Incident 3: from a clean 4.18GiB baseline -- the foreground workload had
// finished four hours earlier -- one index task plus ten mix compactions
// carrying 70.22GiB of input, charged as 40 slots, took the node to 60GiB in
// five minutes.
//
// Two things must hold. The 4.18GiB that nothing in the ledger accounts for has
// to come out of the budget before any of it is handed out, and the burst has
// to stop inside what is left. Note also that the index task and the
// compactions are charged against the same budget: on the real node they were
// two separate counters that could not see each other.
func TestIncident3BurstOnAnIdleBaselineIsBounded(t *testing.T) {
	baseline := int64(4.18 * gibFloat)
	mockIncidentNode(t, baseline)
	g := NewGuard()

	// One watermark sample: the resident baseline becomes a non-task
	// reservation and leaves the task budget by that much.
	g.sampleOnce()

	capacity := taskresource.NodeCapacity()
	snap := g.Snapshot()
	require.Equal(t, baseline, snap.NonTask, "the idle baseline is accounted for")
	require.Equal(t, capacity.Memory-baseline, snap.Total.Memory,
		"and taken out of the budget before anything is admitted")
	require.False(t, snap.Frozen, "setup: 4.18GiB of 64GiB is nowhere near the watermark")

	// The index task that was already running. Its size is not in the incident
	// report; the compaction input is, so the index task is priced from the
	// same field as incident 2 and only the compactions carry the incident's
	// own figures.
	indexReq := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: 1163739 * 768 * 4,
		StorageVersion:  3,
	})
	ok, _ := g.TryAcquire(1, taskcommon.Index, indexReq)
	require.True(t, ok, "setup: the index task starts on an empty node")

	compactionInput := int64(70.22 * gibFloat)
	per := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: compactionInput / 10,
	})

	admitted := 0
	for i := int64(2); i <= 11; i++ {
		if ok, _ := g.TryAcquire(i, taskcommon.Compaction, per); ok {
			admitted++
		}
	}

	assert.Less(t, admitted, 10, "the node must refuse part of the burst")
	assert.Greater(t, admitted, 0, "and it must still do work")

	snap = g.Snapshot()
	assert.Equal(t, indexReq.Memory+per.Memory*int64(admitted), snap.Reserved.Memory,
		"the index build and the compactions are charged against one ledger")
	assert.LessOrEqual(t, snap.Reserved.Memory, snap.Total.Memory,
		"the ledger must never exceed the budget, however bursty the arrivals")
	assert.Greater(t, snap.Reserved.Memory+per.Memory, snap.Total.Memory,
		"and it stops at the budget, not before it")
}

// The invariant behind all three, asserted directly against a stream of
// arrivals that never stops offering.
func TestReservedNeverExceedsBudget(t *testing.T) {
	paramtable.Init()
	g := NewGuard()
	g.setCapacityForTest(taskresource.Capacity{CPU: 8, Memory: 1000})

	admitted := 0
	for i := int64(0); i < 500; i++ {
		if ok, _ := g.TryAcquire(i, taskcommon.Index, taskresource.Requirement{CPU: 0.1, Memory: 37}); ok {
			admitted++
		}
		snap := g.Snapshot()
		require.LessOrEqual(t, snap.Reserved.Memory, snap.Total.Memory)
		require.LessOrEqual(t, snap.Reserved.CPU, snap.Total.CPU)
	}
	// 1000/37 = 27 before either dimension is full, well short of 500: if this
	// ever admitted everything offered, the loop above would have proved
	// nothing but that Snapshot returns numbers.
	assert.Equal(t, 27, admitted)
}
