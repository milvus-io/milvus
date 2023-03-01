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
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/autoindex"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func TestDiskIndexParams(t *testing.T) {
	t.Run("fill index params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init()

		indexParams := make(map[string]string)
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, buildNumThreadsRatio)

		searchCacheBudgetGBRatio, err := strconv.ParseFloat(indexParams[SearchCacheBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.10, searchCacheBudgetGBRatio)

		loadNumThreadRatio, err := strconv.ParseFloat(indexParams[NumLoadThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 8.0, loadNumThreadRatio)

		beamWidthRatio, err := strconv.ParseFloat(indexParams[BeamWidthRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 4.0, beamWidthRatio)
	})

	t.Run("fill index params with auto index", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init()
		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		mapString := make(map[string]string)
		mapString[autoindex.BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[autoindex.PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))
		indexParams := make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams["index_type"] = "DISKANN"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, buildNumThreadsRatio)

		searchCacheBudgetGBRatio, err := strconv.ParseFloat(indexParams[SearchCacheBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.225, searchCacheBudgetGBRatio)

		loadNumThreadRatio, err := strconv.ParseFloat(indexParams[NumLoadThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 4.0, loadNumThreadRatio)

		beamWidthRatio, err := strconv.ParseFloat(indexParams[BeamWidthRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 4.0, beamWidthRatio)
	})

	t.Run("fill index params with wrong auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init()
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		// ExtraParams wrong
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		indexParams := make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams["index_type"] = "DISKANN"
		str, err := json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)

		// IndexParams wrong
		mapString := make(map[string]string)
		mapString[autoindex.BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[autoindex.PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err = json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		indexParams = make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list"] = "100" // should be search_list_size
		indexParams["index_type"] = "DISKANN"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
	})

	t.Run("set disk index build params", func(t *testing.T) {
		indexParams := make(map[string]string)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"

		err := SetDiskIndexBuildParams(indexParams, 100)
		assert.Error(t, err)

		indexParams["dim"] = "128"
		err = SetDiskIndexBuildParams(indexParams, 100)
		assert.NoError(t, err)

		_, ok := indexParams[PQCodeBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[BuildDramBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[NumBuildThreadKey]
		assert.True(t, ok)
	})

	t.Run("set disk index load params", func(t *testing.T) {
		indexParams := make(map[string]string)
		indexParams[SearchCacheBudgetRatioKey] = "0.125"
		indexParams[NumLoadThreadRatioKey] = "8.0"
		indexParams[BeamWidthRatioKey] = "4.0"

		err := SetDiskIndexLoadParams(indexParams, 100)
		assert.Error(t, err)

		indexParams["dim"] = "128"
		err = SetDiskIndexLoadParams(indexParams, 100)
		assert.NoError(t, err)

		_, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[NumLoadThreadKey]
		assert.True(t, ok)
		_, ok = indexParams[BeamWidthKey]
		assert.True(t, ok)
	})
}
