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

package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProperties_CPUSlot(t *testing.T) {
	tests := []struct {
		name      string
		cpuSlot   float64
		wantValue float64
	}{
		{"integer_value", 4.0, 4.0},
		{"float_value", 2.5, 2.5},
		{"large_value", 32.0, 32.0},
		{"small_value", 0.5, 0.5},
		{"zero", 0.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewProperties(nil)
			p.AppendTaskID(123)
			p.AppendCPUSlot(tt.cpuSlot)

			got, err := p.GetTaskCPUSlot()
			assert.NoError(t, err)
			assert.Equal(t, tt.wantValue, got)
		})
	}
}

func TestProperties_MemorySlot(t *testing.T) {
	tests := []struct {
		name       string
		memorySlot float64
		wantValue  float64
	}{
		{"integer_value", 8.0, 8.0},
		{"float_value", 4.5, 4.5},
		{"large_value", 64.0, 64.0},
		{"small_value", 1.0, 1.0},
		{"zero", 0.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewProperties(nil)
			p.AppendTaskID(123)
			p.AppendMemorySlot(tt.memorySlot)

			got, err := p.GetTaskMemorySlot()
			assert.NoError(t, err)
			assert.Equal(t, tt.wantValue, got)
		})
	}
}

func TestProperties_CPUSlot_Missing(t *testing.T) {
	p := NewProperties(nil)
	p.AppendTaskID(123)

	_, err := p.GetTaskCPUSlot()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cpu_slot")
}

func TestProperties_MemorySlot_Missing(t *testing.T) {
	p := NewProperties(nil)
	p.AppendTaskID(123)

	_, err := p.GetTaskMemorySlot()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "memory_slot")
}

func TestProperties_CPUAndMemorySlot_Independent(t *testing.T) {
	// Test that CPU and Memory slots are independent and don't interfere
	p := NewProperties(nil)
	p.AppendTaskID(456)
	p.AppendCPUSlot(16.0)
	p.AppendMemorySlot(32.0)

	cpuSlot, err := p.GetTaskCPUSlot()
	assert.NoError(t, err)
	assert.Equal(t, 16.0, cpuSlot)

	memorySlot, err := p.GetTaskMemorySlot()
	assert.NoError(t, err)
	assert.Equal(t, 32.0, memorySlot)
}

func TestProperties_BothSlots_SetAndGet(t *testing.T) {
	tests := []struct {
		name       string
		cpuSlot    float64
		memorySlot float64
	}{
		{"vector_index_task", 32.0, 16.0},
		{"scalar_index_task", 2.0, 4.0},
		{"stats_task", 1.0, 4.0},
		{"compaction_task", 1.0, 8.0},
		{"lightweight_task", 0.5, 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewProperties(nil)
			p.AppendTaskID(789)
			p.AppendCPUSlot(tt.cpuSlot)
			p.AppendMemorySlot(tt.memorySlot)

			cpu, err := p.GetTaskCPUSlot()
			assert.NoError(t, err)
			assert.Equal(t, tt.cpuSlot, cpu)

			mem, err := p.GetTaskMemorySlot()
			assert.NoError(t, err)
			assert.Equal(t, tt.memorySlot, mem)
		})
	}
}

func TestProperties_Overwrite(t *testing.T) {
	p := NewProperties(nil)
	p.AppendTaskID(100)
	p.AppendCPUSlot(10.0)
	p.AppendMemorySlot(20.0)

	// Overwrite with new values
	p.AppendCPUSlot(5.0)
	p.AppendMemorySlot(15.0)

	cpu, err := p.GetTaskCPUSlot()
	assert.NoError(t, err)
	assert.Equal(t, 5.0, cpu)

	mem, err := p.GetTaskMemorySlot()
	assert.NoError(t, err)
	assert.Equal(t, 15.0, mem)
}

func TestProperties_TaskSlot_Backward_Compatibility(t *testing.T) {
	// Test that old task_slot still works alongside new cpu/memory slots
	p := NewProperties(nil)
	p.AppendTaskID(200)
	p.AppendTaskSlot(10)
	p.AppendCPUSlot(4.0)
	p.AppendMemorySlot(8.0)

	slot, err := p.GetTaskSlot()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), slot)

	cpu, err := p.GetTaskCPUSlot()
	assert.NoError(t, err)
	assert.Equal(t, 4.0, cpu)

	mem, err := p.GetTaskMemorySlot()
	assert.NoError(t, err)
	assert.Equal(t, 8.0, mem)
}
