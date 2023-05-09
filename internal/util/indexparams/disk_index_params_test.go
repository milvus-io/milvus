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

package indexparams

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/autoindex"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func TestDiskIndexParams(t *testing.T) {
	t.Run("fill index params with auto index", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init()
		params.AutoIndexConfig.Enable = true

		mapString := make(map[string]string)
		mapString[autoindex.BuildRatioKey] = "{\"pq_code_budget_gb\": 0.325, \"num_threads\": 2}"
		mapString[autoindex.PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"
		extraParams, err := autoindex.NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		params.AutoIndexConfig.BigDataExtraParams = extraParams
		params.AutoIndexConfig.IndexParams = make(map[string]string)
		params.AutoIndexConfig.IndexParams["max_degree"] = "56"
		params.AutoIndexConfig.IndexParams["search_list_size"] = "100"
		params.AutoIndexConfig.IndexParams["index_type"] = "DISKANN"

		indexParams := make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.325, pqCodeBudgetGBRatio)
		assert.NotEqual(t, params.CommonCfg.PQCodeBudgetGBRatio, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 2.0, buildNumThreadsRatio)
		assert.NotEqual(t, params.CommonCfg.BuildNumThreadsRatio, buildNumThreadsRatio)

		// disable autoindex, use default config
		params.AutoIndexConfig.Enable = false
		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err = strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.NotEqual(t, 0.325, pqCodeBudgetGBRatio)
		assert.Equal(t, params.CommonCfg.PQCodeBudgetGBRatio, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err = strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.NotEqual(t, 2.0, buildNumThreadsRatio)
		assert.Equal(t, params.CommonCfg.BuildNumThreadsRatio, buildNumThreadsRatio)
	})

	t.Run("set disk index build params", func(t *testing.T) {
		indexParams := make(map[string]string)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"

		err := SetDiskIndexBuildParams(indexParams, 100)
		assert.NoError(t, err)

		_, ok := indexParams[PQCodeBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[BuildDramBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[NumBuildThreadKey]
		assert.True(t, ok)
	})

	t.Run("set disk index load params", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init()
		params.AutoIndexConfig.Enable = true

		mapString := make(map[string]string)
		mapString[autoindex.BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[autoindex.PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.15, \"num_threads\": 4}"
		extraParams, err := autoindex.NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		params.AutoIndexConfig.BigDataExtraParams = extraParams
		params.AutoIndexConfig.IndexParams = make(map[string]string)
		params.AutoIndexConfig.IndexParams["max_degree"] = "56"
		params.AutoIndexConfig.IndexParams["search_list_size"] = "100"
		params.AutoIndexConfig.IndexParams["index_type"] = "DISKANN"

		indexParams := make(map[string]string)
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		indexParams["dim"] = "128"
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.NoError(t, err)

		_, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)

		searchCacheBudget, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("%f", float32(GetRowDataSizeOfFloatVector(100, 128))*float32(extraParams.SearchCacheBudgetGBRatio)/(1<<30)), searchCacheBudget)

		numLoadThread, ok := indexParams[NumLoadThreadKey]
		assert.True(t, ok)
		expectedNumLoadThread := int(float32(hardware.GetCPUNum()) * float32(extraParams.LoadNumThreadRatio))
		if expectedNumLoadThread > MaxLoadThread {
			expectedNumLoadThread = MaxLoadThread
		}
		indexParams[NumLoadThreadKey] = strconv.Itoa(expectedNumLoadThread)
		assert.Equal(t, strconv.Itoa(expectedNumLoadThread), numLoadThread)

		beamWidth, ok := indexParams[BeamWidthKey]
		assert.True(t, ok)
		expectedBeamWidth := int(float32(hardware.GetCPUNum()) * float32(extraParams.BeamWidthRatio))
		if expectedBeamWidth > MaxBeamWidth {
			expectedBeamWidth = MaxBeamWidth
		}
		indexParams[BeamWidthKey] = strconv.Itoa(expectedBeamWidth)
		assert.Equal(t, strconv.Itoa(expectedBeamWidth), beamWidth)

		// disable autoindex, use default config
		params.AutoIndexConfig.Enable = false
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.NoError(t, err)

		searchCacheBudget, ok = indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("%f", float32(GetRowDataSizeOfFloatVector(100, 128))*float32(params.CommonCfg.SearchCacheBudgetGBRatio)/(1<<30)), searchCacheBudget)

		numLoadThread, ok = indexParams[NumLoadThreadKey]
		assert.True(t, ok)
		expectedNumLoadThread = int(float32(hardware.GetCPUNum()) * float32(params.CommonCfg.LoadNumThreadRatio))
		if expectedNumLoadThread > MaxLoadThread {
			expectedNumLoadThread = MaxLoadThread
		}
		indexParams[NumLoadThreadKey] = strconv.Itoa(expectedNumLoadThread)
		assert.Equal(t, strconv.Itoa(expectedNumLoadThread), numLoadThread)

		beamWidth, ok = indexParams[BeamWidthKey]
		assert.True(t, ok)
		expectedBeamWidth = int(float32(hardware.GetCPUNum()) * float32(params.CommonCfg.BeamWidthRatio))
		if expectedBeamWidth > MaxBeamWidth {
			expectedBeamWidth = MaxBeamWidth
		}
		indexParams[BeamWidthKey] = strconv.Itoa(expectedBeamWidth)
		assert.Equal(t, strconv.Itoa(expectedBeamWidth), beamWidth)
	})
}
