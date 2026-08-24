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

package compactor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/taskresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const requirementFloor = int64(64) << 20

// planCompactor is the smallest Compactor a requirement can be read from.
type planCompactor struct {
	Compactor
	plan *datapb.CompactionPlan
	slot int64
}

func (c *planCompactor) GetPlan() *datapb.CompactionPlan { return c.plan }
func (c *planCompactor) GetSlotUsage() int64             { return c.slot }
func (c *planCompactor) GetCompactionType() datapb.CompactionType {
	return c.plan.GetType()
}

// A storage-V3 segment loaded after a DataCoord restart ships with EMPTY
// per-FieldBinlog arrays: kv_catalog.AlterSegments skips writing those KVs for
// V3 and the data is described by the manifest instead. Recomputing from the
// plan therefore sees zero bytes and floors the estimate, which switched
// admission control off for exactly the workload issue #52180 is about.
//
// The coordinator's vector is what closes that, so this pins that the vector
// wins and that the result is nowhere near the floor.
func TestV3PlanWithoutBinlogArraysIsPricedFromTheCoordinatorsVector(t *testing.T) {
	paramtable.Init()

	const realInput = int64(36) << 30
	coordSide := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: realInput,
	})
	require.Greater(t, coordSide.Memory, requirementFloor,
		"setup: the coordinator's own estimate must not be the floor")

	plan := &datapb.CompactionPlan{
		PlanID: 1,
		Type:   datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:      100,
			StorageVersion: 3,
			// Exactly what a V3 segment ships after a restart: a manifest and
			// nothing else. No FieldBinlogs, no Deltalogs, no statslogs.
			Manifest: "loon://bucket/manifest@7",
		}},
		TaskResources: coordSide.ToProto(),
	}

	got := taskRequirement(&planCompactor{plan: plan})
	assert.Equal(t, coordSide, got)

	// And the thing that actually went wrong: without the vector this plan
	// prices at the floor, so the assertion above is load-bearing rather than
	// a restatement of the estimator.
	plan.TaskResources = nil
	assert.Equal(t, requirementFloor, taskRequirement(&planCompactor{plan: plan}).Memory,
		"setup: a V3 plan with no vector really does collapse to the floor")
}

// Sort compaction is the family that retains its whole input, so the same
// collapse there is the more dangerous one.
func TestV3SortPlanWithoutBinlogArraysIsPricedFromTheCoordinatorsVector(t *testing.T) {
	paramtable.Init()

	coordSide := taskresource.EstimateCompaction(taskresource.CompactionInput{
		Type:            datapb.CompactionType_SortCompaction,
		StorageVersion:  3,
		TotalMemorySize: 8 << 30,
		TotalRows:       20_000_000,
	})
	require.Greater(t, coordSide.Memory, requirementFloor, "setup: not the floor")

	plan := &datapb.CompactionPlan{
		PlanID:         2,
		Type:           datapb.CompactionType_SortCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 200, StorageVersion: 3, Manifest: "loon://b/m@1"}},
		TaskResources:  coordSide.ToProto(),
	}
	assert.Equal(t, coordSide, taskRequirement(&planCompactor{plan: plan}))
}

// An un-upgraded coordinator sends no vector. The worker must fall back to the
// binlog arrays rather than charging nothing -- on V1/V2 that is still correct.
func TestFallsBackToLocalRecomputeWhenTheCoordinatorSentNoVector(t *testing.T) {
	paramtable.Init()

	plan := &datapb.CompactionPlan{
		PlanID: 3,
		Type:   datapb.CompactionType_MixCompaction,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:      300,
			StorageVersion: 2,
			FieldBinlogs: []*datapb.FieldBinlog{{
				FieldID: 0, ChildFields: []int64{100, 101},
				Binlogs: []*datapb.Binlog{{MemorySize: 4 << 30, EntriesNum: 1_000_000}},
			}},
		}},
	}

	assert.Equal(t, taskresource.RequirementForCompaction(plan), taskRequirement(&planCompactor{plan: plan}))
}

// No plan at all: the legacy scalar, floored so that an absent slot is not
// read as a free task.
func TestFallsBackToTheLegacySlotWhenThereIsNoPlan(t *testing.T) {
	paramtable.Init()

	got := taskRequirement(&planCompactor{plan: nil, slot: 0})
	assert.Equal(t, taskresource.LegacySlotToRequirement(1).Memory, got.Memory)
	assert.Greater(t, got.Memory, int64(0), "an absent slot must never price a task at zero")
}
