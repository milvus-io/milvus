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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRequirementArithmetic(t *testing.T) {
	a := Requirement{CPU: 1.5, Memory: 100}
	b := Requirement{CPU: 0.5, Memory: 40}

	assert.Equal(t, Requirement{CPU: 2.0, Memory: 140}, a.Add(b))
	assert.Equal(t, Requirement{CPU: 1.0, Memory: 60}, a.Sub(b))
}

func TestRequirementSubClampsAtZero(t *testing.T) {
	// 归还多于预约时不得出现负值，否则账本会被"洗白"从而放行更多任务。
	a := Requirement{CPU: 0.5, Memory: 10}
	b := Requirement{CPU: 1.0, Memory: 40}

	got := a.Sub(b)
	assert.Equal(t, Requirement{CPU: 0, Memory: 0}, got)
}

func TestRequirementFitsIn(t *testing.T) {
	c := Capacity{CPU: 4, Memory: 1000}

	assert.True(t, Requirement{CPU: 4, Memory: 1000}.FitsIn(c))
	assert.False(t, Requirement{CPU: 4.1, Memory: 1000}.FitsIn(c))
	assert.False(t, Requirement{CPU: 4, Memory: 1001}.FitsIn(c))
}

func TestRequirementIsZero(t *testing.T) {
	assert.True(t, Requirement{}.IsZero())
	assert.False(t, Requirement{CPU: 0.1}.IsZero())
	assert.False(t, Requirement{Memory: 1}.IsZero())
}

func TestNodeCapacity(t *testing.T) {
	paramtable.Init()

	mkCPU := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
	defer mkCPU.UnPatch()
	mkMem := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64 << 30)).Build()
	defer mkMem.UnPatch()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceCPURatio.Key, "1.0")
	defer pt.Reset(pt.DataNodeCfg.ResourceCPURatio.Key)
	pt.Save(pt.DataNodeCfg.ResourceMemoryRatio.Key, "0.75")
	defer pt.Reset(pt.DataNodeCfg.ResourceMemoryRatio.Key)

	got := NodeCapacity()
	assert.Equal(t, float64(16), got.CPU)
	assert.Equal(t, int64(48<<30), got.Memory)
}

func TestRequirementString(t *testing.T) {
	r := Requirement{CPU: 2, Memory: 3 << 20}
	assert.Equal(t, "{cpu=2.00 mem=3MiB}", r.String())
}

// The exchange rate is derived from the slot multipliers, not configured, so
// every term of the derivation has to be exercised -- most of all memoryRatio,
// which is the one a later simplification is most likely to drop.
func TestLegacyMemoryPerSlot(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	// Defaults: 8GiB / (16 x 1) = 512MiB of raw memory per slot, of which
	// memoryRatio 0.75 is budget.
	assert.Equal(t, int64(384)<<20, LegacyMemoryPerSlot())

	t.Run("memoryRatio scales the raw rate", func(t *testing.T) {
		pt.Save(pt.DataNodeCfg.ResourceMemoryRatio.Key, "1.0")
		defer pt.Reset(pt.DataNodeCfg.ResourceMemoryRatio.Key)

		assert.Equal(t, int64(512)<<20, LegacyMemoryPerSlot())
	})

	t.Run("both slot multipliers divide the memory unit", func(t *testing.T) {
		pt.Save(pt.DataNodeCfg.WorkerSlotUnit.Key, "8")
		defer pt.Reset(pt.DataNodeCfg.WorkerSlotUnit.Key)
		pt.Save(pt.DataNodeCfg.BuildParallel.Key, "4")
		defer pt.Reset(pt.DataNodeCfg.BuildParallel.Key)

		// 8GiB / (8 x 4) = 256MiB raw, x 0.75.
		assert.EqualValues(t, 32, WorkerSlotsPerMemoryUnit())
		assert.Equal(t, int64(192)<<20, LegacyMemoryPerSlot())
	})

	t.Run("standalone slots are worth proportionally more", func(t *testing.T) {
		// CalculateNodeSlots reports a quarter as many slots in standalone, so
		// each one stands for four times as much of the same budget.
		paramtable.SetRole(typeutil.StandaloneRole)
		defer paramtable.SetRole("")

		assert.EqualValues(t, 4, WorkerSlotsPerMemoryUnit())
		assert.Equal(t, int64(1536)<<20, LegacyMemoryPerSlot())
	})

	t.Run("a non-positive multiplier floors at one instead of dividing by zero", func(t *testing.T) {
		pt.Save(pt.DataNodeCfg.WorkerSlotUnit.Key, "0")
		defer pt.Reset(pt.DataNodeCfg.WorkerSlotUnit.Key)
		pt.Save(pt.DataNodeCfg.BuildParallel.Key, "-3")
		defer pt.Reset(pt.DataNodeCfg.BuildParallel.Key)

		assert.EqualValues(t, 1, WorkerSlotsPerMemoryUnit())
		assert.Equal(t, int64(6)<<30, LegacyMemoryPerSlot())
	})

	t.Run("the rate is never zero, however small the terms", func(t *testing.T) {
		pt.Save(pt.DataNodeCfg.WorkerSlotUnit.Key, "1099511627776") // 1Ti slots per 8GiB
		defer pt.Reset(pt.DataNodeCfg.WorkerSlotUnit.Key)
		pt.Save(pt.DataNodeCfg.ResourceMemoryRatio.Key, "0.0")
		defer pt.Reset(pt.DataNodeCfg.ResourceMemoryRatio.Key)

		assert.EqualValues(t, 1, LegacyMemoryPerSlot(), "a zero rate would price every task as free")
	})
}
