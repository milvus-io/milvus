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

package index

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type utilSuite struct {
	suite.Suite
}

func (s *utilSuite) Test_mapToKVPairs() {
	indexParams := map[string]string{
		"index_type": "IVF_FLAT",
		"dim":        "128",
		"nlist":      "1024",
	}

	s.Equal(3, len(mapToKVPairs(indexParams)))
}

func Test_utilSuite(t *testing.T) {
	suite.Run(t, new(utilSuite))
}

func generateFloats(num int) []float32 {
	data := make([]float32, num)
	for i := 0; i < num; i++ {
		data[i] = rand.Float32()
	}
	return data
}

func generateLongs(num int) []int64 {
	data := make([]int64, num)
	for i := 0; i < num; i++ {
		data[i] = rand.Int63()
	}
	return data
}

func TestCalculateNodeSlotsV2(t *testing.T) {
	paramtable.Init()

	cpuCores := hardware.GetCPUNum()
	memoryBytes := hardware.GetMemoryCount()
	memoryGB := float64(memoryBytes) / (1024 * 1024 * 1024)

	t.Run("cluster_mode", func(t *testing.T) {
		origBuildParallel := paramtable.Get().DataNodeCfg.BuildParallel.GetValue()
		defer paramtable.Get().DataNodeCfg.BuildParallel.SwapTempValue(origBuildParallel)

		testCases := []float64{1.0, 2.0, 0.5, 1.5}
		for _, buildParallel := range testCases {
			paramtable.Get().DataNodeCfg.BuildParallel.SwapTempValue(fmt.Sprintf("%f", buildParallel))

			cpuSlot, memorySlot := CalculateNodeSlotsV2()

			expectedCpuSlot := float64(cpuCores) * buildParallel
			expectedMemorySlot := memoryGB * buildParallel

			assert.Equal(t, expectedCpuSlot, cpuSlot,
				"CPU slot should be cpuCores * buildParallel for buildParallel=%v", buildParallel)
			assert.Equal(t, expectedMemorySlot, memorySlot,
				"Memory slot should be memoryGB * buildParallel for buildParallel=%v", buildParallel)
		}
	})

	t.Run("return_values_positive", func(t *testing.T) {
		cpuSlot, memorySlot := CalculateNodeSlotsV2()
		assert.Greater(t, cpuSlot, 0.0, "CPU slot should be positive")
		assert.Greater(t, memorySlot, 0.0, "Memory slot should be positive")
	})

	t.Run("cpu_and_memory_independent", func(t *testing.T) {
		cpuSlot, memorySlot := CalculateNodeSlotsV2()

		expectedRatio := float64(cpuCores) / memoryGB
		actualRatio := cpuSlot / memorySlot

		assert.InDelta(t, expectedRatio, actualRatio, 0.001,
			"CPU/Memory ratio should match hardware ratio")
	})

	t.Run("build_parallel_scaling", func(t *testing.T) {
		origBuildParallel := paramtable.Get().DataNodeCfg.BuildParallel.GetValue()
		defer paramtable.Get().DataNodeCfg.BuildParallel.SwapTempValue(origBuildParallel)

		paramtable.Get().DataNodeCfg.BuildParallel.SwapTempValue("1.0")
		cpu1, mem1 := CalculateNodeSlotsV2()

		paramtable.Get().DataNodeCfg.BuildParallel.SwapTempValue("2.0")
		cpu2, mem2 := CalculateNodeSlotsV2()

		assert.InDelta(t, cpu1*2, cpu2, 0.001, "CPU slots should scale with buildParallel")
		assert.InDelta(t, mem1*2, mem2, 0.001, "Memory slots should scale with buildParallel")
	})

	t.Run("consistency_check", func(t *testing.T) {
		cpuSlot1, memorySlot1 := CalculateNodeSlotsV2()
		cpuSlot2, memorySlot2 := CalculateNodeSlotsV2()

		assert.Equal(t, cpuSlot1, cpuSlot2, "Multiple calls should return consistent CPU slots")
		assert.Equal(t, memorySlot1, memorySlot2, "Multiple calls should return consistent memory slots")
	})
}

func TestGetIndexType(t *testing.T) {
	tests := []struct {
		name        string
		indexParams []*commonpb.KeyValuePair
		expected    string
	}{
		{
			name: "hnsw_index",
			indexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HNSW"},
				{Key: "M", Value: "16"},
			},
			expected: "HNSW",
		},
		{
			name: "ivf_flat_index",
			indexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
				{Key: "nlist", Value: "1024"},
			},
			expected: "IVF_FLAT",
		},
		{
			name: "no_index_type",
			indexParams: []*commonpb.KeyValuePair{
				{Key: "M", Value: "16"},
			},
			expected: "",
		},
		{
			name:        "empty_params",
			indexParams: []*commonpb.KeyValuePair{},
			expected:    "",
		},
		{
			name:        "nil_params",
			indexParams: nil,
			expected:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetIndexType(tt.indexParams)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMapToKVPairs_Comprehensive(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		validate func(*testing.T, []*commonpb.KeyValuePair)
	}{
		{
			name: "normal_map",
			input: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			validate: func(t *testing.T, kvs []*commonpb.KeyValuePair) {
				assert.Len(t, kvs, 2)
				kvMap := make(map[string]string)
				for _, kv := range kvs {
					kvMap[kv.Key] = kv.Value
				}
				assert.Equal(t, "value1", kvMap["key1"])
				assert.Equal(t, "value2", kvMap["key2"])
			},
		},
		{
			name:  "empty_map",
			input: map[string]string{},
			validate: func(t *testing.T, kvs []*commonpb.KeyValuePair) {
				assert.Len(t, kvs, 0)
			},
		},
		{
			name: "single_item",
			input: map[string]string{
				"index_type": "HNSW",
			},
			validate: func(t *testing.T, kvs []*commonpb.KeyValuePair) {
				assert.Len(t, kvs, 1)
				assert.Equal(t, "index_type", kvs[0].Key)
				assert.Equal(t, "HNSW", kvs[0].Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapToKVPairs(tt.input)
			tt.validate(t, result)
		})
	}
}
