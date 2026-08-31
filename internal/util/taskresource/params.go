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
// It is the divisor that inverts CalculateNodeSlots, shared by
// fmIndexBuildTaskSlots (internal/datacoord/util.go) and by
// LegacySlotToRequirement, so the two cannot drift apart.
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
