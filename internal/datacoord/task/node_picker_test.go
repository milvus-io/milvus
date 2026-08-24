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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/util/taskresource"
)

const gib = int64(1) << 30

// recordingScalarTier stands in for the pre-existing scalar placement so a test
// can tell "fell through" from "placed on dimensions".
type recordingScalarTier struct {
	calls  int
	nodeID int64
}

func (t *recordingScalarTier) pick(taskSlot int64) int64 {
	t.calls++
	return t.nodeID
}

func dimensionedWorker(nodeID int64, capCPU float64, capMem, usedMem int64, admitting bool) *session.WorkerSlots {
	return &session.WorkerSlots{
		NodeID:         nodeID,
		AvailableSlots: 100,
		Resources: taskresource.NodeResourcesOf(
			taskresource.Capacity{CPU: capCPU, Memory: capMem},
			taskresource.Capacity{Memory: usedMem},
			admitting,
		),
	}
}

func TestPickerPlacesOnTheNodeWithRoom(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 40*gib, true), // 8GiB free
		2: dimensionedWorker(2, 16, 48*gib, 4*gib, true),  // 44GiB free
	}, scalar)

	got := picker.Pick(taskresource.Requirement{Memory: 20 * gib, CPU: 1}, true, 10)
	assert.Equal(t, int64(2), got, "must pick the node that has the room")
	assert.Zero(t, scalar.calls, "a dimensioned placement must not fall through")
}

// Memory gates. A node that cannot hold the task is not a candidate however
// attractive its other dimensions are.
func TestPickerRefusesANodeThatCannotHoldTheTask(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		// Idle CPU, but nowhere near enough memory.
		1: dimensionedWorker(1, 64, 48*gib, 47*gib, true),
	}, scalar)

	got := picker.Pick(taskresource.Requirement{Memory: 20 * gib, CPU: 1}, true, 10)
	assert.Equal(t, int64(-1), got)
	assert.Equal(t, 1, scalar.calls, "a task with no dimensioned home falls through")
}

// CPU must NEVER gate: refusing on it would serialize classes that contend for
// no common thread pool, which is the whole reason it is a request.
func TestPickerNeverRefusesOnCPU(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 4, 48*gib, 0, true),
	}, scalar)

	// Asks for far more CPU than the node has, but the memory fits.
	got := picker.Pick(taskresource.Requirement{Memory: gib, CPU: 64}, true, 10)
	assert.Equal(t, int64(1), got, "CPU pressure must not exclude a node")
	assert.Zero(t, scalar.calls)
}

// A worker that has stopped taking work is not a candidate at all, whatever
// its dimensions say.
func TestPickerSkipsANodeThatIsNotAdmitting(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 0, false), // completely free, frozen
		2: dimensionedWorker(2, 16, 48*gib, 40*gib, true),
	}, scalar)

	got := picker.Pick(taskresource.Requirement{Memory: 4 * gib, CPU: 1}, true, 10)
	assert.Equal(t, int64(2), got)
}

// A task larger than any worker has to run somewhere: it is placed on the
// emptiest node and the worker runs it alone. Leaving it pending forever is
// the alternative, because no node ever grows.
func TestPickerDispatchesATaskLargerThanEveryNode(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 40*gib, true),
		2: dimensionedWorker(2, 16, 48*gib, 4*gib, true),
	}, scalar)

	got := picker.Pick(taskresource.Requirement{Memory: 100 * gib, CPU: 1}, true, 10)
	assert.Equal(t, int64(2), got, "the emptiest node is where it starts soonest")
	assert.Zero(t, scalar.calls)
}

// The distinction that matters: a task that merely does not fit RIGHT NOW must
// wait, not be forced onto a node that cannot hold it.
func TestPickerMakesAMerelyBusyClusterWait(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 47*gib, true),
		2: dimensionedWorker(2, 16, 48*gib, 47*gib, true),
	}, scalar)

	// 20GiB fits in a 48GiB node in principle, so this is congestion, not an
	// oversized task.
	got := picker.Pick(taskresource.Requirement{Memory: 20 * gib, CPU: 1}, true, 10)
	assert.Equal(t, int64(-1), got, "must fall through rather than force a placement")
	assert.Equal(t, 1, scalar.calls)
}

// Charging must be visible to later picks in the same round, or a round of N
// tasks all pile onto whichever node looked emptiest at the start.
func TestPickerChargesWhatItPlaces(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 0, true),
		2: dimensionedWorker(2, 16, 48*gib, 0, true),
	}, scalar)

	req := taskresource.Requirement{Memory: 30 * gib, CPU: 1}
	first := picker.Pick(req, true, 10)
	second := picker.Pick(req, true, 10)

	require.NotZero(t, first)
	require.NotZero(t, second)
	assert.NotEqual(t, first, second,
		"the second task must see the first one's charge and go elsewhere")
}

// A worker that predates the vector reports none. It must still receive work,
// through the tier that existed before.
func TestPickerFallsThroughForWorkersWithoutDimensions(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: 7}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		7: {NodeID: 7, AvailableSlots: 100}, // no Resources
	}, scalar)

	got := picker.Pick(taskresource.Requirement{Memory: gib}, true, 10)
	assert.Equal(t, int64(7), got)
	assert.Equal(t, 1, scalar.calls)
}

// A task that cannot state a requirement is placed by the scalar, unchanged.
func TestPickerFallsThroughForTasksWithoutARequirement(t *testing.T) {
	scalar := &recordingScalarTier{nodeID: 7}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: dimensionedWorker(1, 16, 48*gib, 0, true),
	}, scalar)

	got := picker.Pick(taskresource.Requirement{}, false, 10)
	assert.Equal(t, int64(7), got)
	assert.Equal(t, 1, scalar.calls)
}

// Between two nodes that both fit, the one left less lopsided afterwards wins:
// a node with memory free but no CPU left can host nothing further.
func TestScorePrefersTheNodeLeftLessLopsided(t *testing.T) {
	balanced := &dimensionedNode{
		capacity:  taskresource.Capacity{CPU: 16, Memory: 48 * gib},
		committed: taskresource.Capacity{CPU: 4, Memory: 12 * gib},
	}
	lopsided := &dimensionedNode{
		capacity:  taskresource.Capacity{CPU: 16, Memory: 48 * gib},
		committed: taskresource.Capacity{CPU: 15, Memory: 12 * gib},
	}
	req := taskresource.Requirement{Memory: 4 * gib, CPU: 1}

	assert.Greater(t, score(balanced, req), score(lopsided, req))
}
