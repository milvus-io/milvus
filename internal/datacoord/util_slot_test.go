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

package datacoord

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type SlotCalculationSuite struct {
	suite.Suite
}

func (s *SlotCalculationSuite) SetupSuite() {
	paramtable.Init()
}

func (s *SlotCalculationSuite) TestCalculateIndexTaskSlotV2_ScalarIndex() {
	tests := []struct {
		name         string
		fieldSize    int64
		expectCPU    float64
		expectMemMin float64
		expectMemMax float64
	}{
		{
			name:         "tiny scalar index (<10MB)",
			fieldSize:    5 * 1024 * 1024, // 5MB
			expectCPU:    paramtable.Get().DataCoordCfg.ScalarIndexTaskCPUFactor.GetAsFloat() / 8,
			expectMemMin: 0.01,
			expectMemMax: 0.1,
		},
		{
			name:         "small scalar index (10-100MB)",
			fieldSize:    50 * 1024 * 1024, // 50MB
			expectCPU:    paramtable.Get().DataCoordCfg.ScalarIndexTaskCPUFactor.GetAsFloat() / 4,
			expectMemMin: 0.1,
			expectMemMax: 0.5,
		},
		{
			name:         "medium scalar index (100-512MB)",
			fieldSize:    200 * 1024 * 1024, // 200MB
			expectCPU:    paramtable.Get().DataCoordCfg.ScalarIndexTaskCPUFactor.GetAsFloat() / 2,
			expectMemMin: 0.5,
			expectMemMax: 2.0,
		},
		{
			name:         "large scalar index (>512MB)",
			fieldSize:    1024 * 1024 * 1024, // 1GB
			expectCPU:    paramtable.Get().DataCoordCfg.ScalarIndexTaskCPUFactor.GetAsFloat(),
			expectMemMin: 3.0,
			expectMemMax: 6.0,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cpuSlot, memorySlot := calculateIndexTaskSlotV2(tt.fieldSize, false)
			s.Equal(tt.expectCPU, cpuSlot, "CPU slot mismatch")
			s.GreaterOrEqual(memorySlot, tt.expectMemMin, "Memory slot too low")
			s.LessOrEqual(memorySlot, tt.expectMemMax, "Memory slot too high")
		})
	}
}

func (s *SlotCalculationSuite) TestCalculateIndexTaskSlotV2_VectorIndex() {
	fieldSize := int64(1024 * 1024 * 1024) // 1GB
	cpuSlot, memorySlot := calculateIndexTaskSlotV2(fieldSize, true)

	baseCPU := paramtable.Get().DataCoordCfg.VectorIndexTaskCPUFactor.GetAsFloat()
	baseMemoryFactor := paramtable.Get().DataCoordCfg.VectorIndexTaskMemoryFactor.GetAsFloat()

	// Vector index should use full CPU for large data
	s.Equal(baseCPU, cpuSlot)

	// Memory should be calculated based on field size
	expectedMem := float64(fieldSize) / 1024 / 1024 / 1024 * baseMemoryFactor
	s.InDelta(expectedMem, memorySlot, 0.01)
}

func (s *SlotCalculationSuite) TestCalculateIndexTaskSlotV2_VectorIndex_SmallData() {
	// Test CPU scaling for small vector index tasks
	tests := []struct {
		name      string
		fieldSize int64
		cpuScale  float64
	}{
		{
			name:      "tiny (<10MB)",
			fieldSize: 5 * 1024 * 1024,
			cpuScale:  1.0 / 8,
		},
		{
			name:      "small (10-100MB)",
			fieldSize: 50 * 1024 * 1024,
			cpuScale:  1.0 / 4,
		},
		{
			name:      "medium (100-512MB)",
			fieldSize: 200 * 1024 * 1024,
			cpuScale:  1.0 / 2,
		},
		{
			name:      "large (>512MB)",
			fieldSize: 600 * 1024 * 1024,
			cpuScale:  1.0,
		},
	}

	baseCPU := paramtable.Get().DataCoordCfg.VectorIndexTaskCPUFactor.GetAsFloat()

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cpuSlot, _ := calculateIndexTaskSlotV2(tt.fieldSize, true)
			expectedCPU := baseCPU * tt.cpuScale
			s.Equal(expectedCPU, cpuSlot)
		})
	}
}

func (s *SlotCalculationSuite) TestCalculateStatsTaskSlotV2() {
	baseCPU := paramtable.Get().DataCoordCfg.StatsTaskCPUFactor.GetAsFloat()
	baseMemoryFactor := paramtable.Get().DataCoordCfg.StatsTaskMemoryFactor.GetAsFloat()

	tests := []struct {
		name         string
		segmentSize  int64
		expectCPU    float64
		expectMemMin float64
		expectMemMax float64
	}{
		{
			name:         "small segment (<100MB)",
			segmentSize:  50 * 1024 * 1024,
			expectCPU:    baseCPU, // No CPU scaling for stats tasks
			expectMemMin: 0.01,
			expectMemMax: 0.3,
		},
		{
			name:         "medium segment (100MB-1GB)",
			segmentSize:  500 * 1024 * 1024,
			expectCPU:    baseCPU, // No CPU scaling for stats tasks
			expectMemMin: 1.5,
			expectMemMax: 2.5,
		},
		{
			name:         "large segment (>1GB)",
			segmentSize:  2 * 1024 * 1024 * 1024,
			expectCPU:    baseCPU, // No CPU scaling for stats tasks
			expectMemMin: 7.0,
			expectMemMax: 9.0,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			cpuSlot, memorySlot := calculateStatsTaskSlotV2(tt.segmentSize)
			s.Equal(tt.expectCPU, cpuSlot)

			// Verify memory slot is in expected range
			expectedMem := float64(tt.segmentSize) / 1024 / 1024 / 1024 * baseMemoryFactor
			s.InDelta(expectedMem, memorySlot, 0.01)
			s.GreaterOrEqual(memorySlot, tt.expectMemMin)
			s.LessOrEqual(memorySlot, tt.expectMemMax)
		})
	}
}

func (s *SlotCalculationSuite) TestBackwardCompatibility_CalculateIndexTaskSlot() {
	// Test that old function still works with old logic
	tests := []struct {
		name          string
		fieldSize     int64
		isVectorIndex bool
		expectMin     int64
	}{
		{
			name:          "tiny vector index (<10MB)",
			fieldSize:     5 * 1024 * 1024,
			isVectorIndex: true,
			expectMin:     1,
		},
		{
			name:          "small vector index (10-100MB)",
			fieldSize:     50 * 1024 * 1024,
			isVectorIndex: true,
			expectMin:     1,
		},
		{
			name:          "medium vector index (100-512MB)",
			fieldSize:     200 * 1024 * 1024,
			isVectorIndex: true,
			expectMin:     1,
		},
		{
			name:          "large vector index (>512MB)",
			fieldSize:     600 * 1024 * 1024,
			isVectorIndex: true,
			expectMin:     1,
		},
		{
			name:          "very large vector index (2GB)",
			fieldSize:     2 * 1024 * 1024 * 1024,
			isVectorIndex: true,
			expectMin:     1,
		},
		{
			name:          "tiny scalar index (<10MB)",
			fieldSize:     5 * 1024 * 1024,
			isVectorIndex: false,
			expectMin:     1,
		},
		{
			name:          "small scalar index (10-100MB)",
			fieldSize:     50 * 1024 * 1024,
			isVectorIndex: false,
			expectMin:     1,
		},
		{
			name:          "medium scalar index (100-512MB)",
			fieldSize:     200 * 1024 * 1024,
			isVectorIndex: false,
			expectMin:     1,
		},
		{
			name:          "large scalar index (>512MB)",
			fieldSize:     600 * 1024 * 1024,
			isVectorIndex: false,
			expectMin:     1,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			slotV1 := calculateIndexTaskSlot(tt.fieldSize, tt.isVectorIndex)
			s.GreaterOrEqual(slotV1, tt.expectMin)
			s.IsType(int64(0), slotV1)
		})
	}
}

func (s *SlotCalculationSuite) TestBackwardCompatibility_CalculateStatsTaskSlot() {
	// Test that old function still works with old logic
	tests := []struct {
		name        string
		segmentSize int64
		expectMin   int64
	}{
		{
			name:        "tiny segment (<10MB)",
			segmentSize: 5 * 1024 * 1024,
			expectMin:   1,
		},
		{
			name:        "small segment (10-100MB)",
			segmentSize: 50 * 1024 * 1024,
			expectMin:   1,
		},
		{
			name:        "medium segment (100-512MB)",
			segmentSize: 200 * 1024 * 1024,
			expectMin:   1,
		},
		{
			name:        "large segment (>512MB)",
			segmentSize: 600 * 1024 * 1024,
			expectMin:   1,
		},
		{
			name:        "very large segment (2GB)",
			segmentSize: 2 * 1024 * 1024 * 1024,
			expectMin:   1,
		},
		{
			name:        "extra large segment (10GB)",
			segmentSize: 10 * 1024 * 1024 * 1024,
			expectMin:   1,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			slotV1 := calculateStatsTaskSlot(tt.segmentSize)
			s.GreaterOrEqual(slotV1, tt.expectMin)
			s.IsType(int64(0), slotV1)
		})
	}
}

func TestSlotCalculationSuite(t *testing.T) {
	suite.Run(t, new(SlotCalculationSuite))
}

func TestCalculateIndexTaskSlotV2_EdgeCases(t *testing.T) {
	paramtable.Init()

	// Test zero size
	cpuSlot, memorySlot := calculateIndexTaskSlotV2(0, true)
	assert.Greater(t, cpuSlot, 0.0)
	assert.GreaterOrEqual(t, memorySlot, 0.0)

	// Test very large size
	largeSize := int64(100 * 1024 * 1024 * 1024) // 100GB
	cpuSlot, memorySlot = calculateIndexTaskSlotV2(largeSize, true)
	assert.Greater(t, cpuSlot, 0.0)
	assert.Greater(t, memorySlot, 0.0)
}

func TestCalculateStatsTaskSlotV2_EdgeCases(t *testing.T) {
	paramtable.Init()

	// Test zero size
	cpuSlot, memorySlot := calculateStatsTaskSlotV2(0)
	assert.Greater(t, cpuSlot, 0.0)
	assert.GreaterOrEqual(t, memorySlot, 0.0)

	// Test very large size
	largeSize := int64(100 * 1024 * 1024 * 1024) // 100GB
	cpuSlot, memorySlot = calculateStatsTaskSlotV2(largeSize)
	assert.Greater(t, cpuSlot, 0.0)
	assert.Greater(t, memorySlot, 0.0)

	// Test that CPU slot is consistent across different sizes (no scaling)
	baseCPU := paramtable.Get().DataCoordCfg.StatsTaskCPUFactor.GetAsFloat()
	cpuSlotSmall, _ := calculateStatsTaskSlotV2(99 * 1024 * 1024)
	cpuSlotMedium, _ := calculateStatsTaskSlotV2(100 * 1024 * 1024)
	cpuSlotLarge, _ := calculateStatsTaskSlotV2(1024 * 1024 * 1024)

	// Stats tasks use fixed CPU slot (default is 1)
	assert.Equal(t, baseCPU, cpuSlotSmall)
	assert.Equal(t, baseCPU, cpuSlotMedium)
	assert.Equal(t, baseCPU, cpuSlotLarge)
}

func TestCPUScalingConsistency(t *testing.T) {
	paramtable.Init()

	// Test that CPU scaling is consistent across scalar and vector indexes
	tests := []struct {
		name      string
		fieldSize int64
		cpuScale  float64
	}{
		{"boundary_10MB", 10 * 1024 * 1024, 1.0 / 8},
		{"boundary_100MB", 100 * 1024 * 1024, 1.0 / 4},
		{"boundary_512MB", 512 * 1024 * 1024, 1.0 / 2},
		{"above_512MB", 513 * 1024 * 1024, 1.0},
	}

	scalarBaseCPU := paramtable.Get().DataCoordCfg.ScalarIndexTaskCPUFactor.GetAsFloat()
	vectorBaseCPU := paramtable.Get().DataCoordCfg.VectorIndexTaskCPUFactor.GetAsFloat()

	for _, tt := range tests {
		t.Run(tt.name+"_scalar", func(t *testing.T) {
			cpuSlot, _ := calculateIndexTaskSlotV2(tt.fieldSize, false)
			expectedCPU := scalarBaseCPU * tt.cpuScale
			assert.Equal(t, expectedCPU, cpuSlot)
		})

		t.Run(tt.name+"_vector", func(t *testing.T) {
			cpuSlot, _ := calculateIndexTaskSlotV2(tt.fieldSize, true)
			expectedCPU := vectorBaseCPU * tt.cpuScale
			assert.Equal(t, expectedCPU, cpuSlot)
		})
	}
}

func TestV1V2ConsistencyComparison(t *testing.T) {
	paramtable.Init()

	// Compare V1 and V2 calculations to ensure reasonable relationship
	testCases := []struct {
		fieldSize     int64
		isVectorIndex bool
	}{
		{5 * 1024 * 1024, true},
		{50 * 1024 * 1024, true},
		{200 * 1024 * 1024, true},
		{600 * 1024 * 1024, true},
		{5 * 1024 * 1024, false},
		{50 * 1024 * 1024, false},
		{200 * 1024 * 1024, false},
		{600 * 1024 * 1024, false},
	}

	for _, tc := range testCases {
		t.Run(formatSize(tc.fieldSize)+"_vector="+boolToString(tc.isVectorIndex), func(t *testing.T) {
			slotV1 := calculateIndexTaskSlot(tc.fieldSize, tc.isVectorIndex)
			cpuSlotV2, memorySlotV2 := calculateIndexTaskSlotV2(tc.fieldSize, tc.isVectorIndex)

			// V1 should be positive
			assert.Greater(t, slotV1, int64(0))
			// V2 CPU should be positive
			assert.Greater(t, cpuSlotV2, 0.0)
			// V2 memory should be non-negative
			assert.GreaterOrEqual(t, memorySlotV2, 0.0)
		})
	}
}

func TestMemorySlotNeverNegative(t *testing.T) {
	paramtable.Init()

	// Edge case: ensure memory slot is never negative even with extreme inputs
	testCases := []struct {
		name          string
		fieldSize     int64
		isVectorIndex bool
	}{
		{"zero_size", 0, true},
		{"one_byte", 1, true},
		{"negative_size", -1, true}, // Should handle gracefully
		{"max_int64", 9223372036854775807, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cpuSlot, memorySlot := calculateIndexTaskSlotV2(tc.fieldSize, tc.isVectorIndex)
			assert.GreaterOrEqual(t, memorySlot, 0.0, "Memory slot should never be negative")
			assert.Greater(t, cpuSlot, 0.0, "CPU slot should be positive")
		})
	}
}

func TestStatsTaskSlotBoundaryPrecision(t *testing.T) {
	paramtable.Init()

	baseCPU := paramtable.Get().DataCoordCfg.StatsTaskCPUFactor.GetAsFloat()

	// Stats tasks use fixed CPU slot (no scaling based on size)
	testCases := []struct {
		name        string
		segmentSize int64
		expectedCPU float64
	}{
		{"just_below_100MB", 100*1024*1024 - 1, baseCPU},
		{"exactly_100MB", 100 * 1024 * 1024, baseCPU},
		{"just_above_100MB", 100*1024*1024 + 1, baseCPU},
		{"just_below_1GB", 1024*1024*1024 - 1, baseCPU},
		{"exactly_1GB", 1024 * 1024 * 1024, baseCPU},
		{"just_above_1GB", 1024*1024*1024 + 1, baseCPU},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cpuSlot, _ := calculateStatsTaskSlotV2(tc.segmentSize)
			assert.Equal(t, tc.expectedCPU, cpuSlot)
		})
	}
}

// Helper functions
func formatSize(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	} else if size < 1024*1024 {
		return fmt.Sprintf("%dKB", size/1024)
	} else if size < 1024*1024*1024 {
		return fmt.Sprintf("%dMB", size/1024/1024)
	}
	return fmt.Sprintf("%dGB", size/1024/1024/1024)
}

func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
