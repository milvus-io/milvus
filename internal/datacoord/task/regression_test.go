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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The three OOM kills recorded in issue #52180, as placement decisions.
//
// They live here, and not on the worker, because this is where the decision now
// is. The worker keeps a ledger so that its report is honest; what it does NOT
// do any more is refuse, so a regression test that asserted refusal there would
// be testing a mechanism that no longer exists. What must not regress is that
// the coordinator stops placing before the node is over-committed.
//
// Each case is built from the incident's real figures so that the arithmetic,
// not a hand-picked threshold, is what fails if the model changes.

// The 16-CPU / 64-GiB node from the issue.
func incidentNode(nodeID int64, committedMem int64) *session.WorkerSlots {
	return &session.WorkerSlots{
		NodeID:         nodeID,
		AvailableSlots: 128,
		Resources: taskresource.NodeResourcesOf(
			taskresource.Capacity{CPU: 16, Memory: 48 * gib}, // 64GiB x the 0.75 default
			taskresource.Capacity{Memory: committedMem},
			true,
		),
	}
}

// Incident 1: eight Vortex MixCompactions holding 36.09GiB of combined input,
// every one of them charged a flat 4 slots -- 32 of the node's 128. Heap went
// 2.5 -> 32GiB in about two and a half minutes.
//
// The defect was not the estimate alone: all of them were placed while every
// one was still downloading, so nothing the node had MEASURED had caught up
// yet. Placement has to stop when the ledger of commitments fills.
//
// This is a mitigation, not a closure, and the test says so. The model reasons
// in live heap while the container died on RSS, and nothing in it accounts for
// Go's GC headroom. What it does guarantee is that the concurrency the flat
// constant permitted is no longer reachable.
func TestIncident1V3CompactionConcurrencyIsBoundedByTheBudget(t *testing.T) {
	paramtable.Init()

	// One eighth of the incident's input, priced by the current model.
	perTask := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: 36 * gib / 8,
	})
	require.Greater(t, perTask.Memory, int64(4)<<30,
		"setup: a 4.5GiB v3 compaction must not be priced like a small one")

	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: incidentNode(1, 0),
	}, scalar)

	// The flat constant allowed 128/4 = 32 of these on one node. Offer that many.
	const flatConstantAllowed = 32
	var placed int
	for i := 0; i < flatConstantAllowed; i++ {
		if picker.Pick(perTask, true, 4) != -1 {
			placed++
		}
	}

	assert.LessOrEqual(t, int64(placed)*perTask.Memory, 48*gib,
		"what was placed must fit the budget the node reported")
	assert.Less(t, placed, flatConstantAllowed/2,
		"the concurrency the flat constant permitted must no longer be reachable")
}

// Incident 2: an HNSW build over 1,163,739 rows at dim 768 priced taskSlot=384
// against a node reporting totalSlots=128 -- a task apparently larger than the
// machine -- dispatched anyway, and then run alongside everything else.
//
// Dispatching it was never the defect. It must still be placed, because no node
// ever grows; what must not happen is that the node is left looking like it has
// room for more.
func TestIncident2AnOversizedBuildIsStillPlacedButConsumesTheNode(t *testing.T) {
	paramtable.Init()

	const rows, dim = int64(1_163_739), int64(768)
	build := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: rows * dim * 4,
		StorageVersion:  3,
	})
	require.Greater(t, build.Memory, int64(6)<<30, "setup: 3.33GiB of vectors x the HNSW factor")

	scalar := &recordingScalarTier{nodeID: -1}
	// A node with almost nothing left: the task cannot fit, but it is not
	// bigger than the machine either, so it must WAIT rather than be forced on.
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: incidentNode(1, 46*gib),
	}, scalar)
	assert.Equal(t, int64(-1), picker.Pick(build, true, 384),
		"a node with 2GiB free must not be given a 6GiB build")

	// Genuinely larger than the machine: placed on the emptiest node, because
	// waiting would be refusal by another name.
	huge := taskresource.Requirement{Memory: 200 * gib, CPU: 1}
	scalar2 := &recordingScalarTier{nodeID: -1}
	picker2 := newNodePicker(map[int64]*session.WorkerSlots{
		1: incidentNode(1, 40*gib),
		2: incidentNode(2, 4*gib),
	}, scalar2)
	assert.Equal(t, int64(2), picker2.Pick(huge, true, 384))
}

// Incident 3: from a clean 4.18GiB baseline, one index task plus ten
// MixCompactions -- 70.22GiB of input charged as 40 slots -- reached 60GiB in
// five minutes.
//
// The mixed shape is the point: an index build and a compaction are charged
// against the same budget, so ten compactions cannot be placed just because
// they are a different family from the build.
func TestIncident3MixedFamiliesShareOneBudget(t *testing.T) {
	paramtable.Init()

	perCompaction := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: 70 * gib / 10,
	})
	build := taskresource.EstimateIndexBuild(taskresource.IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: 3 * gib,
		StorageVersion:  3,
	})

	scalar := &recordingScalarTier{nodeID: -1}
	picker := newNodePicker(map[int64]*session.WorkerSlots{
		1: incidentNode(1, 0),
	}, scalar)

	require.NotEqual(t, int64(-1), picker.Pick(build, true, 64), "setup: the build is placed first")

	var placed int
	for i := 0; i < 10; i++ {
		if picker.Pick(perCompaction, true, 4) != -1 {
			placed++
		}
	}

	assert.Less(t, placed, 10, "ten more compactions must not fit alongside the build")
	assert.LessOrEqual(t, build.Memory+int64(placed)*perCompaction.Memory, 48*gib,
		"the build and the compactions are charged against ONE budget")
}
