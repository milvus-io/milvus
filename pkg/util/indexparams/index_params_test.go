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

func TestAppendPrepareLoadParamsCagra(t *testing.T) {
	tests := []struct {
		name     string
		build    string
		load     string
		expected string
	}{
		{name: "native format CPU load", build: "false", load: "true", expected: "true"},
		{name: "native format GPU load", build: "false", load: "false", expected: "false"},
		{name: "CPU format CPU load", build: "true", load: "true", expected: "true"},
		{name: "CPU format ignores GPU load", build: "true", load: "false", expected: "true"},
		{name: "omitted build CPU load", load: "true", expected: "true"},
		{name: "omitted build GPU load", load: "false", expected: "false"},
		{name: "native format default load", build: "false", expected: "false"},
		{name: "CPU format default load", build: "true", expected: "true"},
		{name: "omitted build default load"},
		{name: "numeric false", build: "0", load: "true", expected: "true"},
		{name: "numeric true", build: "1", load: "false", expected: "1"},
		{name: "invalid build value is preserved", build: "invalid", load: "true", expected: "invalid"},
	}
	for _, indexType := range []string{"GPU_CAGRA", "GPU_CUVS_CAGRA"} {
		for _, test := range tests {
			t.Run(indexType+"/"+test.name, func(t *testing.T) {
				var params paramtable.ComponentParam
				params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
				params.Save(params.AutoIndexConfig.Enable.Key, "false")
				loadKey := params.KnowhereConfig.IndexParam.KeyPrefix + indexType + ".load.adapt_for_cpu"
				params.Remove(loadKey)
				if test.load != "" {
					params.Save(loadKey, test.load)
				}
				indexParams := map[string]string{
					common.IndexTypeKey: indexType,
					"graph_degree":      "32",
				}
				if test.build != "" {
					indexParams["adapt_for_cpu"] = test.build
				}

				assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
				assert.Equal(t, test.expected, indexParams["adapt_for_cpu"])
				assert.Equal(t, "32", indexParams["graph_degree"])
				// Resource estimation and actual loading may prepare the same params again.
				assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
				assert.Equal(t, test.expected, indexParams["adapt_for_cpu"])
			})
		}
	}

	t.Run("other indexes retain metadata precedence", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "false")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"GPU_IVF_FLAT.load.adapt_for_cpu", "true")
		indexParams := map[string]string{common.IndexTypeKey: "GPU_IVF_FLAT", "adapt_for_cpu": "false"}
		assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
		assert.Equal(t, "false", indexParams["adapt_for_cpu"])
	})

	t.Run("auto index load configuration", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"GPU_CAGRA.load.adapt_for_cpu", "true")
		for _, test := range []struct {
			name     string
			prepare  string
			load     string
			expected string
		}{
			{name: "load overrides prepare and Knowhere", prepare: `{"adapt_for_cpu":"true"}`, load: `{"adapt_for_cpu":"false"}`, expected: "false"},
			{name: "prepare overrides Knowhere", prepare: `{"adapt_for_cpu":"false"}`, load: `{}`, expected: "false"},
			{name: "cloud enables CPU load", prepare: `{}`, load: `{"adapt_for_cpu":"true"}`, expected: "true"},
			{name: "Knowhere fallback", prepare: `{}`, load: `{}`, expected: "true"},
		} {
			t.Run(test.name, func(t *testing.T) {
				params.Save(params.AutoIndexConfig.PrepareParams.Key, test.prepare)
				params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, test.load)
				for _, build := range []string{"false", "true"} {
					indexParams := map[string]string{common.IndexTypeKey: "GPU_CAGRA", "adapt_for_cpu": build}
					expected := test.expected
					if build == "true" {
						expected = "true"
					}
					assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
					assert.Equal(t, expected, indexParams["adapt_for_cpu"])
					assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
					assert.Equal(t, expected, indexParams["adapt_for_cpu"])
				}
			})
		}
	})

	t.Run("other parameters retain merge precedence", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		params.Save(params.AutoIndexConfig.PrepareParams.Key, `{"cloud_key":"prepare"}`)
		params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, `{"cloud_key":"load"}`)
		prefix := params.KnowhereConfig.IndexParam.KeyPrefix + "HNSW.load."
		params.Save(prefix+"cloud_key", "default")
		params.Save(prefix+"explicit_key", "default")
		params.Save(prefix+"missing_key", "default")
		indexParams := map[string]string{
			common.IndexTypeKey: "HNSW",
			"cloud_key":         "index",
			"explicit_key":      "index",
		}
		assert.NoError(t, AppendPrepareLoadParams(&params, indexParams))
		assert.Equal(t, "load", indexParams["cloud_key"])
		assert.Equal(t, "index", indexParams["explicit_key"])
		assert.Equal(t, "default", indexParams["missing_key"])
	})
}
