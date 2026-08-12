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

func TestLegacyMemoryPerSlot(t *testing.T) {
	paramtable.Init()

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceLegacyMemoryPerSlot.Key, "134217728")
	defer pt.Reset(pt.DataNodeCfg.ResourceLegacyMemoryPerSlot.Key)

	assert.Equal(t, int64(134217728), legacyMemoryPerSlot())
}
