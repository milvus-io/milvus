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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDiskIndexParams(t *testing.T) {
	t.Run("fill index params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		indexParams := make(map[string]string)
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, buildNumThreadsRatio)

		searchCacheBudgetRatio, err := strconv.ParseFloat(indexParams[SearchCacheBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.10, searchCacheBudgetRatio)
	})

	t.Run("fill index params with auto index", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))
		indexParams := make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams[common.IndexTypeKey] = "DISKANN"
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
	})

	t.Run("fill index params with wrong auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		// ExtraParams wrong
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		indexParams := make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams[common.IndexTypeKey] = "DISKANN"
		str, err := json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)

		// IndexParams wrong
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err = json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		indexParams = make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list"] = "100" // should be search_list_size
		indexParams[common.IndexTypeKey] = "DISKANN"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
	})

	t.Run("patch index build params", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		indexParams := make([]*commonpb.KeyValuePair, 0, 3)

		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   PQCodeBudgetRatioKey,
				Value: "0.125",
			})

		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   NumBuildThreadRatioKey,
				Value: "1.0",
			})

		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   BeamWidthRatioKey,
				Value: "4.0",
			})

		indexParams, err := UpdateDiskIndexBuildParams(&params, indexParams)
		assert.NoError(t, err)
		assert.True(t, len(indexParams) == 4)

		val := GetIndexParams(indexParams, SearchCacheBudgetRatioKey)
		cfgVal, cfgErr := strconv.ParseFloat(params.CommonCfg.SearchCacheBudgetGBRatio.GetValue(), 64)
		assert.NoError(t, cfgErr)
		iVal, iErr := strconv.ParseFloat(val, 64)
		assert.NoError(t, iErr)
		assert.Equal(t, cfgVal, iVal)

		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		jsonStr := `
				{
					"build_ratio": "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}",
					"prepare_ratio": "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}",
					"beamwidth_ratio": "8.0"
				}
			`
		params.Save(params.AutoIndexConfig.ExtraParams.Key, jsonStr)

		autoParams := make([]*commonpb.KeyValuePair, 0, 3)

		autoParams = append(autoParams,
			&commonpb.KeyValuePair{
				Key:   PQCodeBudgetRatioKey,
				Value: "0.125",
			})

		autoParams = append(autoParams,
			&commonpb.KeyValuePair{
				Key:   NumBuildThreadRatioKey,
				Value: "1.0",
			})

		autoParams = append(autoParams,
			&commonpb.KeyValuePair{
				Key:   BeamWidthRatioKey,
				Value: "4.0",
			})

		autoParams, err = UpdateDiskIndexBuildParams(&params, autoParams)
		assert.NoError(t, err)
		assert.True(t, len(autoParams) == 4)

		val = GetIndexParams(autoParams, SearchCacheBudgetRatioKey)
		iVal, iErr = strconv.ParseFloat(val, 64)
		assert.NoError(t, iErr)
		assert.Equal(t, 0.225, iVal)

		newJSONStr := `
				{
					"build_ratio": "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}",
					"prepare_ratio": "{\"search_cache_budget_gb\": 0.325, \"num_threads\": 8}",
					"beamwidth_ratio": "8.0"
				}
			`
		params.Save(params.AutoIndexConfig.ExtraParams.Key, newJSONStr)
		autoParams, err = UpdateDiskIndexBuildParams(&params, autoParams)

		assert.NoError(t, err)
		assert.True(t, len(autoParams) == 4)

		val = GetIndexParams(autoParams, SearchCacheBudgetRatioKey)
		iVal, iErr = strconv.ParseFloat(val, 64)
		assert.NoError(t, iErr)
		assert.Equal(t, 0.325, iVal)
	})

	t.Run("set disk index build params", func(t *testing.T) {
		indexParams := make(map[string]string)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"

		indexParams[SearchCacheBudgetRatioKey] = "0.125"
		err := SetDiskIndexBuildParams(indexParams, 100)
		assert.NoError(t, err)

		_, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)

		indexParams[SearchCacheBudgetRatioKey] = "aabb"
		err = SetDiskIndexBuildParams(indexParams, 100)
		assert.Error(t, err)

		delete(indexParams, SearchCacheBudgetRatioKey)
		delete(indexParams, SearchCacheBudgetKey)
		err = SetDiskIndexBuildParams(indexParams, 100)
		assert.NoError(t, err)

		_, ok = indexParams[PQCodeBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[BuildDramBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[NumBuildThreadKey]
		assert.True(t, ok)
		_, ok = indexParams[SearchCacheBudgetKey]
		assert.False(t, ok)
	})

	t.Run("set disk index load params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		indexParams := make(map[string]string)

		err := SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		indexParams[common.DimKey] = "128"
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.NoError(t, err)

		searchCacheBudget, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)
		searchCacheBudgetRatio, err := strconv.ParseFloat(params.CommonCfg.SearchCacheBudgetGBRatio.GetValue(), 64)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%f", float32(getRowDataSizeOfFloatVector(100, 128))*float32(searchCacheBudgetRatio)/(1<<30)), searchCacheBudget)

		numLoadThread, ok := indexParams[NumLoadThreadKey]
		assert.True(t, ok)
		numLoadThreadRatio, err := strconv.ParseFloat(params.CommonCfg.LoadNumThreadRatio.GetValue(), 64)
		assert.NoError(t, err)
		expectedNumLoadThread := int(float32(hardware.GetCPUNum()) * float32(numLoadThreadRatio))
		if expectedNumLoadThread > MaxLoadThread {
			expectedNumLoadThread = MaxLoadThread
		}
		assert.Equal(t, strconv.Itoa(expectedNumLoadThread), numLoadThread)

		beamWidth, ok := indexParams[BeamWidthKey]
		assert.True(t, ok)
		beamWidthRatio, err := strconv.ParseFloat(params.CommonCfg.BeamWidthRatio.GetValue(), 64)
		assert.NoError(t, err)
		expectedBeamWidth := int(float32(hardware.GetCPUNum()) * float32(beamWidthRatio))
		if expectedBeamWidth > MaxBeamWidth {
			expectedBeamWidth = MaxBeamWidth
		}
		assert.Equal(t, strconv.Itoa(expectedBeamWidth), beamWidth)

		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "w1")
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.1")
		params.Save(params.CommonCfg.LoadNumThreadRatio.Key, "w1")
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		params.Save(params.CommonCfg.LoadNumThreadRatio.Key, "8.0")
		params.Save(params.CommonCfg.BeamWidthRatio.Key, "w1")
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)
	})

	t.Run("set disk index load params with auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))
		extraParams, err := NewBigDataExtraParamsFromJSON(params.AutoIndexConfig.ExtraParams.GetValue())
		assert.NoError(t, err)

		indexParams := make(map[string]string)
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		indexParams[common.DimKey] = "128"
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.NoError(t, err)

		searchCacheBudget, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("%f", float32(getRowDataSizeOfFloatVector(100, 128))*float32(extraParams.SearchCacheBudgetGBRatio)/(1<<30)), searchCacheBudget)

		numLoadThread, ok := indexParams[NumLoadThreadKey]
		assert.True(t, ok)
		expectedNumLoadThread := int(float32(hardware.GetCPUNum()) * float32(extraParams.LoadNumThreadRatio))
		if expectedNumLoadThread > MaxLoadThread {
			expectedNumLoadThread = MaxLoadThread
		}
		assert.Equal(t, strconv.Itoa(expectedNumLoadThread), numLoadThread)

		beamWidth, ok := indexParams[BeamWidthKey]
		assert.True(t, ok)
		expectedBeamWidth := int(float32(hardware.GetCPUNum()) * float32(extraParams.BeamWidthRatio))
		if expectedBeamWidth > MaxBeamWidth {
			expectedBeamWidth = MaxBeamWidth
		}
		assert.Equal(t, strconv.Itoa(expectedBeamWidth), beamWidth)
	})

	t.Run("set disk index load params with wrong autoindex param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		// ExtraParams wrong
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		indexParams := make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams[common.IndexTypeKey] = "DISKANN"
		str, err := json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		indexParams[common.DimKey] = "128"

		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		indexParams = make(map[string]string)
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)

		// IndexParams wrong
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"

		str, err = json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		indexParams = make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list"] = "100" // should be search_list_size
		indexParams[common.IndexTypeKey] = "DISKANN"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)
	})
}

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

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1, \"search_cache_budget_gb\": 0.20}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.20, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with build_ratio partial or wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.15}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.15, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"num_threads\": 2}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 2.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"num_threads\": 2"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		_, err = NewBigDataExtraParamsFromMap(mapString)
		assert.Error(t, err)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with prepare_ratio partial or wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.25}"
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.25, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 4}"
		extraParams, err = NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 4.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225"
		_, err = NewBigDataExtraParamsFromMap(mapString)
		assert.Error(t, err)
	})

	t.Run("parse with beamwidth wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BeamWidthRatioKey] = "aa"
		extraParams, err := NewBigDataExtraParamsFromMap(mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
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

func TestAppendPrepareInfo_parse(t *testing.T) {
	t.Run("parse load info", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		mapString := make(map[string]string)
		mapString["key1"] = "value1"
		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.PrepareParams.Key, string(str))

		mapString2 := make(map[string]string)
		mapString2["key2"] = "value2"
		str2, err2 := json.Marshal(mapString2)
		assert.NoError(t, err2)
		params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, string(str2))

		resultMapString := make(map[string]string)
		err = AppendPrepareLoadParams(&params, resultMapString)
		assert.NoError(t, err)
		assert.Equal(t, resultMapString["key1"], "value1")
		assert.Equal(t, resultMapString["key2"], "value2")

		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"GPU_CAGRA.load.adapt_for_cpu", "true")
		indexParams := map[string]string{
			"index_type":       "GPU_CAGRA",
			"nn_descent_niter": "20",
			"build_algo":       "NN_DESCENT",
		}

		err = AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, indexParams["nn_descent_niter"], "20")
		assert.Equal(t, indexParams["build_algo"], "NN_DESCENT")
		assert.Equal(t, indexParams["adapt_for_cpu"], "true")
	})
}

func TestAppendPrepareLoadParams_OverrideIndexType(t *testing.T) {
	t.Run("HNSW overridden to GPU_HNSW", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW.load.override_index_type", "GPU_HNSW")

		indexParams := map[string]string{
			common.IndexTypeKey: "HNSW",
			"M":                 "16",
			"efConstruction":    "200",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])
		assert.Equal(t, "16", indexParams["M"])
		assert.Equal(t, "200", indexParams["efConstruction"])
	})

	t.Run("HNSW_int8 overridden to GPU_HNSW", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW_int8.load.override_index_type", "GPU_HNSW")

		indexParams := map[string]string{
			common.IndexTypeKey: "HNSW_int8",
			"M":                 "16",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])
	})

	t.Run("no override configured leaves index_type unchanged", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")

		indexParams := map[string]string{
			common.IndexTypeKey: "IVF_FLAT",
			"nlist":             "1024",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "IVF_FLAT", indexParams[common.IndexTypeKey])
		assert.Equal(t, "", indexParams[paramtable.OverrideIndexTypeKey])
	})

	t.Run("idempotency: double call produces same result", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW.load.override_index_type", "GPU_HNSW")

		indexParams := map[string]string{
			common.IndexTypeKey: "HNSW",
			"M":                 "16",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])

		// Second call (simulates QueryNode applying override after QueryCoord already did)
		err = AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])
		assert.Equal(t, "16", indexParams["M"])
	})

	t.Run("override merges additional load params for override type", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW.load.override_index_type", "GPU_HNSW")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"GPU_HNSW.load.gpu_cache_size", "4096")

		indexParams := map[string]string{
			common.IndexTypeKey: "HNSW",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "4096", indexParams["gpu_cache_size"])
	})

	t.Run("existing override_index_type in params is not overwritten", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW.load.override_index_type", "GPU_HNSW")

		indexParams := map[string]string{
			common.IndexTypeKey:             "HNSW",
			paramtable.OverrideIndexTypeKey: "GPU_HNSW",
		}

		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])
	})
}

// TestOverrideIndexType_ProtoRoundTrip verifies the override survives the full
// proto serialization cycle used on every load path:
//
//	FieldIndexInfo.IndexParams (proto []*KeyValuePair)
//	  → KeyValuePair2Map (map[string]string)
//	  → AppendPrepareLoadParams (applies override_index_type swap)
//	  → Map2KeyValuePair (back to proto)
//
// This is the exact pattern used in:
//   - segment_loader.go:305 (cold load)
//   - segment_loader.go:2410 (ReopenSegments)
//   - executor.go:907 (getLoadInfo)
//   - services.go:497 (handler-level defense-in-depth)
func TestOverrideIndexType_ProtoRoundTrip(t *testing.T) {
	initParams := func() *paramtable.ComponentParam {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW.load.override_index_type", "GPU_HNSW")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"HNSW_int8.load.override_index_type", "GPU_HNSW")
		return &params
	}

	t.Run("FieldIndexInfo round-trip: HNSW → GPU_HNSW", func(t *testing.T) {
		params := initParams()

		// Simulate proto IndexParams as they come from DataCoord
		protoParams := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: "M", Value: "16"},
			{Key: "efConstruction", Value: "200"},
		}

		// Convert to map, apply override, convert back (the pattern used in all load paths)
		indexParams := funcutil.KeyValuePair2Map(protoParams)
		err := AppendPrepareLoadParams(params, indexParams)
		assert.NoError(t, err)
		result := funcutil.Map2KeyValuePair(indexParams)

		// Verify the proto output has the override
		resultMap := funcutil.KeyValuePair2Map(result)
		assert.Equal(t, "GPU_HNSW", resultMap[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", resultMap[paramtable.OverrideIndexTypeKey])
		assert.Equal(t, "16", resultMap["M"])
		assert.Equal(t, "200", resultMap["efConstruction"])
	})

	t.Run("FieldIndexInfo round-trip: HNSW_int8 → GPU_HNSW", func(t *testing.T) {
		params := initParams()

		protoParams := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW_int8"},
			{Key: "M", Value: "16"},
		}

		indexParams := funcutil.KeyValuePair2Map(protoParams)
		err := AppendPrepareLoadParams(params, indexParams)
		assert.NoError(t, err)
		result := funcutil.Map2KeyValuePair(indexParams)

		resultMap := funcutil.KeyValuePair2Map(result)
		assert.Equal(t, "GPU_HNSW", resultMap[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", resultMap[paramtable.OverrideIndexTypeKey])
	})

	t.Run("SegmentLoadInfo.IndexInfos: all indexes get override applied", func(t *testing.T) {
		params := initParams()

		// Simulate a SegmentLoadInfo with multiple FieldIndexInfo entries
		// (mirrors the loop in segment_loader.go:290 and services.go:501)
		indexInfos := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: "M", Value: "16"},
		}

		indexParams := funcutil.KeyValuePair2Map(indexInfos)
		err := AppendPrepareLoadParams(params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])

		// Write back and re-read to verify proto round-trip
		protoResult := funcutil.Map2KeyValuePair(indexParams)
		verifyMap := funcutil.KeyValuePair2Map(protoResult)
		assert.Equal(t, "GPU_HNSW", verifyMap[common.IndexTypeKey])
	})

	t.Run("double application is safe (QueryCoord + QueryNode both apply)", func(t *testing.T) {
		params := initParams()

		protoParams := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: "M", Value: "16"},
		}

		// First application (QueryCoord executor.go:907)
		indexParams := funcutil.KeyValuePair2Map(protoParams)
		err := AppendPrepareLoadParams(params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])

		// Serialize back to proto (as the QueryCoord would do)
		protoAfterQC := funcutil.Map2KeyValuePair(indexParams)

		// Second application (QueryNode services.go handler or segment_loader.go:305)
		indexParams2 := funcutil.KeyValuePair2Map(protoAfterQC)
		err = AppendPrepareLoadParams(params, indexParams2)
		assert.NoError(t, err)

		// Must be identical to single application
		assert.Equal(t, "GPU_HNSW", indexParams2[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams2[paramtable.OverrideIndexTypeKey])
		assert.Equal(t, "16", indexParams2["M"])
	})

	t.Run("no override config: proto round-trip preserves original type", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.KnowhereConfig.Enable.Key, "true")
		// No override configured for IVF_FLAT

		protoParams := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "IVF_FLAT"},
			{Key: "nlist", Value: "1024"},
		}

		indexParams := funcutil.KeyValuePair2Map(protoParams)
		err := AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		result := funcutil.Map2KeyValuePair(indexParams)

		resultMap := funcutil.KeyValuePair2Map(result)
		assert.Equal(t, "IVF_FLAT", resultMap[common.IndexTypeKey])
		assert.Equal(t, "", resultMap[paramtable.OverrideIndexTypeKey])
		assert.Equal(t, "1024", resultMap["nlist"])
	})

	t.Run("handler-level defense: unpatched request gets override at handler", func(t *testing.T) {
		params := initParams()

		// Simulate a LoadSegmentsRequest arriving at the QueryNode handler WITHOUT
		// the override already applied (the streamingnode fifth-path scenario).
		// The handler-level fix (services.go:497) applies the override before
		// delegating to loader/delegator.
		protoParams := []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW_int8"},
			{Key: "M", Value: "16"},
		}

		// No override_index_type in the incoming params (bug scenario)
		indexParams := funcutil.KeyValuePair2Map(protoParams)
		assert.Equal(t, "", indexParams[paramtable.OverrideIndexTypeKey])

		// Handler applies override
		err := AppendPrepareLoadParams(params, indexParams)
		assert.NoError(t, err)

		// Override is now present
		assert.Equal(t, "GPU_HNSW", indexParams[common.IndexTypeKey])
		assert.Equal(t, "GPU_HNSW", indexParams[paramtable.OverrideIndexTypeKey])
	})
}
