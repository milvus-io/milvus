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

package taskresource

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30

	// BytesPerWorkerMemoryUnit is the memory unit CalculateNodeSlots
	// (internal/datanode/index/util.go) counts a worker's memory in: it exposes
	// WorkerSlotsPerMemoryUnit() slots for every 8GiB of raw node memory.
	BytesPerWorkerMemoryUnit = int64(8) << 30
)

// saturatingMul mirrors internal/datacoord's helper of the same name. It is
// repeated here rather than imported because taskresource sits below datacoord
// in the import graph.
func saturatingMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

// WorkerSlotsPerMemoryUnit is how many legacy slots CalculateNodeSlots
// (internal/datanode/index/util.go) exposes per BytesPerWorkerMemoryUnit of
// raw node memory: WorkerSlotUnit x BuildParallel, times StandaloneSlotRatio
// when the worker is embedded in a standalone deployment.
//
// It is the divisor both bytes<->slots conversions invert CalculateNodeSlots
// with -- fmIndexBuildTaskSlots (internal/datacoord/util.go) for FM-index
// builds, LegacyMemoryPerSlot below for the phase-0 scalar slot -- and the two
// must not drift apart, so they share this one definition.
func WorkerSlotsPerMemoryUnit() int64 {
	cfg := &paramtable.Get().DataNodeCfg
	slots := saturatingMul(
		max(cfg.WorkerSlotUnit.GetAsInt64(), 1),
		max(cfg.BuildParallel.GetAsInt64(), 1),
	)
	if paramtable.GetRole() == typeutil.StandaloneRole {
		slots = int64(float64(slots) * cfg.StandaloneSlotRatio.GetAsFloat())
	}
	// Never zero: it is always used as a divisor.
	return max(slots, 1)
}

// LegacyMemoryPerSlot is the exchange rate between the scalar slot the wire
// protocol still carries in phase 0 and the byte budget the DataNode's ledger
// actually reasons in. It is read in both directions -- DataCoord folds a byte
// estimate down to slots with it (memoryToSlots), the DataNode unfolds a slot
// back into a requirement with it (LegacySlotToRequirement) -- so the two sides
// must compute the same number.
//
// It is derived rather than configured, because the node's own reported
// availability already fixes it. legacyAvailableSlots
// (internal/datanode/services.go) reports CalculateNodeSlots x (1 - budget
// utilization), so one reported slot is worth exactly (ledger budget /
// CalculateNodeSlots) bytes. On a memory-bound node the hardware term cancels:
//
//	CalculateNodeSlots = mem/8GiB x WorkerSlotUnit x BuildParallel
//	ledger budget      = mem x memoryRatio          (see NodeCapacity)
//	budget per slot    = 8GiB / (WorkerSlotUnit x BuildParallel) x memoryRatio
//
// which is 512MiB x 0.75 = 384MiB at the defaults. The cancellation is what
// lets a coordinator that has never seen the worker's hardware compute the
// same rate the worker does.
//
// The first factor is exactly the inversion fmIndexBuildTaskSlots
// (internal/datacoord/util.go) performs, and is shared with it. The memoryRatio
// factor is this function's own: fmIndexBuildTaskSlots prices against raw node
// memory, while a slot reported by legacyAvailableSlots is a slice of the
// ledger's budget, which is raw memory already scaled by memoryRatio. Dropping
// that factor would make each slot look 1/memoryRatio times larger than the
// node will charge for it (a third larger at the default), and DataCoord would
// go back to over-dispatching against a budget the node does not have.
//
// On a CPU-bound node CalculateNodeSlots is smaller than the memory-bound
// expression above, so a slot is worth more budget than this returns. The rate
// then understates a slot, DataCoord charges a task more slots than the node
// deducts, and it dispatches less than the node would admit -- the safe
// direction for an estimate the wire cannot carry precisely.
func LegacyMemoryPerSlot() int64 {
	rawPerSlot := max(BytesPerWorkerMemoryUnit/WorkerSlotsPerMemoryUnit(), 1)
	perSlot := int64(float64(rawPerSlot) * paramtable.Get().DataNodeCfg.ResourceMemoryRatio.GetAsFloat())
	// Never zero: it is used as a divisor on the DataCoord side, and as the
	// per-slot charge on the DataNode side where zero would mean "free".
	return max(perSlot, 1)
}
