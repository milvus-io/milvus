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

package autoindex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigDataIndex_parse(t *testing.T) {
	t.Run("parse normal", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with partial", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with empty", func(t *testing.T) {
		mapString := make(map[string]string)
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with nil", func(t *testing.T) {
		extraParams, err := NewBigDataExtraParamsFromMap(nil)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("new from json normal", func(t *testing.T) {
		jsonStr := `
				{
					"build_ratio": "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}",
					"prepare_ratio": "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}",
					"beamwidth_ratio": "8.0"
				}
			`
		extraParams, err := NewBigDataExtraParamsFromJSON(jsonStr)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)
		assert.Equal(t, 8.0, extraParams.BeamWidthRatio)
	})

	t.Run("new from json partial", func(t *testing.T) {
		jsonStr := `
				{
					"build_ratio": "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
				}
			`
		extraParams, err := NewBigDataExtraParamsFromJSON(jsonStr)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
		assert.Equal(t, 4.0, extraParams.BeamWidthRatio)
	})

	t.Run("new from json empty", func(t *testing.T) {
		jsonStr := `
				{
				}
			`
		extraParams, err := NewBigDataExtraParamsFromJSON(jsonStr)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
		assert.Equal(t, 4.0, extraParams.BeamWidthRatio)
	})

	t.Run("new from json invalid1", func(t *testing.T) {
		jsonStr := `
				{	x
				}
			`
		_, err := NewBigDataExtraParamsFromJSON(jsonStr)
		assert.Error(t, err)
	})

	t.Run("new from json invalid1", func(t *testing.T) {
		jsonStr := `
				""
			`
		_, err := NewBigDataExtraParamsFromJSON(jsonStr)
		assert.Error(t, err)
	})
}
