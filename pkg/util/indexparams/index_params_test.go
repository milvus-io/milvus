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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDiskIndexParams(t *testing.T) {
	t.Run("fill index params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "AISAQ"
		params.Save(params.CommonCfg.AiSAQCfg.InlinePQ.Key, "0")
		params.Save(params.CommonCfg.AiSAQCfg.PQCacheSize.Key, "536870912")
		params.Save(params.CommonCfg.AiSAQCfg.Rearrange.Key, "true")
		params.Save(params.CommonCfg.AiSAQCfg.PQReadPageCacheSize.Key, "512")
		params.Save(params.CommonCfg.AiSAQCfg.NumEntryPoints.Key, "100")
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, buildNumThreadsRatio)

		// SearchCacheBudgetRatioKey should NOT be persisted when user didn't explicitly set it
		// (it comes from global config and should be read live at load time).
		_, searchCacheExists := indexParams[SearchCacheBudgetRatioKey]
		assert.False(t, searchCacheExists,
			"SearchCacheBudgetRatioKey should not be persisted when defaulted from global config")

		pqCacheSize, err := strconv.ParseInt(indexParams[PQCacheSizeKey], 10, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(536870912), pqCacheSize)

		inlinePQ, err := strconv.ParseInt(indexParams[InlinePQKey], 10, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), inlinePQ)

		numEntryPoints, err := strconv.ParseInt(indexParams[NumEntryPointsKey], 10, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), numEntryPoints)

		rearrange, err := strconv.ParseBool(indexParams[RearrangeKey])
		assert.NoError(t, err)
		assert.True(t, rearrange)

		pqReadPageCacheSize, err := strconv.ParseInt(indexParams[PQReadPageCacheSizeKey], 10, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(512), pqReadPageCacheSize)
	})

	t.Run("fill DISKANN index params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)
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
		indexParams[common.IndexTypeKey] = "DISKANN"
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		pqCodeBudgetGBRatio, err := strconv.ParseFloat(indexParams[PQCodeBudgetRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.125, pqCodeBudgetGBRatio)

		buildNumThreadsRatio, err := strconv.ParseFloat(indexParams[NumBuildThreadRatioKey], 64)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, buildNumThreadsRatio)

		indexParams = make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
		indexParams[common.IndexTypeKey] = "AISAQ"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
		indexParams[common.IndexTypeKey] = "AISAQ"
		indexParams["max_degree"] = "56"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
		indexParams[common.IndexTypeKey] = "AISAQ"
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		str2, err2 := json.Marshal(mapString)
		assert.NoError(t, err2)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str2))
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err) // pq_cache_size falls back to CommonCfg.AiSAQCfg.PQCacheSize default
		indexParams["max_degree"] = "56"
		indexParams["search_list_size"] = "100"
		indexParams["pq_cache_size"] = "xxx"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		str2, err2 = json.Marshal(mapString)
		assert.NoError(t, err2)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str2))
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
		indexParams[common.IndexTypeKey] = "DISKANN"
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		str, _ = json.Marshal(indexParams)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))
		err = FillDiskIndexParams(&params, indexParams)
		assert.Error(t, err)
	})

	t.Run("fill AISAQ index params with auto index", func(t *testing.T) {
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
		indexParams[MaxDegreeKey] = "56"
		indexParams[SearchListSizeKey] = "100"
		indexParams[PQCacheSizeKey] = "512"
		indexParams[common.IndexTypeKey] = "AISAQ"
		str, err = json.Marshal(indexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams = make(map[string]string)
		indexParams[common.IndexTypeKey] = "AISAQ"
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)
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
				Key:   common.IndexTypeKey,
				Value: "DISKANN",
			})

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
		assert.True(t, len(indexParams) == 5)

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

		params.Save(params.AutoIndexConfig.Enable.Key, "false")
		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   common.IndexTypeKey,
				Value: "DISKANN",
			})

		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   SearchCacheBudgetRatioKey,
				Value: "0.2",
			})

		indexParams, err = UpdateDiskIndexBuildParams(&params, indexParams)
		assert.NoError(t, err)
		assert.True(t, len(indexParams) == 7)

		indexParams = make([]*commonpb.KeyValuePair, 0, 3)
		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   common.IndexTypeKey,
				Value: "AISAQ",
			})

		indexParams, err = UpdateDiskIndexBuildParams(&params, indexParams)
		assert.NoError(t, err)
		assert.True(t, len(indexParams) == 2)

		indexParams = make([]*commonpb.KeyValuePair, 0, 3)
		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   common.IndexTypeKey,
				Value: "DISKANN",
			})

		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		params.Save(params.AutoIndexConfig.ExtraParams.Key, "")
		indexParams, err = UpdateDiskIndexBuildParams(&params, indexParams)
		assert.Error(t, err)

		newJSONStr = `
				{
					"build_ratio": "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}",
					"prepare_ratio": "{\"search_cache_budget_gb\": 0.325, \"num_threads\": 8}",
					"beamwidth_ratio": "8.0"
				}
			`
		params.Save(params.AutoIndexConfig.Enable.Key, "false")
		params.Save(params.AutoIndexConfig.ExtraParams.Key, newJSONStr)
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "")
		indexParams, err = UpdateDiskIndexBuildParams(&params, indexParams)
		assert.Error(t, err)

		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   SearchCacheBudgetRatioKey,
				Value: "aaa",
			})
		_, err = UpdateDiskIndexBuildParams(&params, indexParams)
		assert.Error(t, err)
	})

	t.Run("set disk index build params", func(t *testing.T) {
		indexParams := make(map[string]string)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"

		indexParams[SearchCacheBudgetRatioKey] = "0.125"
		indexParams[common.IndexTypeKey] = "DISKANN"
		indexParams[common.DimKey] = "128"
		err := SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.NoError(t, err)

		_, ok := indexParams[SearchCacheBudgetKey]
		assert.True(t, ok)

		indexParams[SearchCacheBudgetRatioKey] = "aabb"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)

		delete(indexParams, SearchCacheBudgetRatioKey)
		delete(indexParams, SearchCacheBudgetKey)
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.NoError(t, err)

		_, ok = indexParams[PQCodeBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[BuildDramBudgetKey]
		assert.True(t, ok)
		_, ok = indexParams[NumBuildThreadKey]
		assert.True(t, ok)
		_, ok = indexParams[SearchCacheBudgetKey]
		assert.False(t, ok)

		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_BinaryVector)
		assert.NoError(t, err)
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_BFloat16Vector)
		assert.NoError(t, err)
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_SparseFloatVector)
		assert.Error(t, err)
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_None)
		assert.NoError(t, err)
		indexParams = make(map[string]string)
		indexParams[DiskPQCodeBudgetRatioKey] = "0.2"
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"
		indexParams[common.IndexTypeKey] = "AISAQ"
		indexParams[common.DimKey] = "128"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.NoError(t, err)

		indexParams[PQCodeBudgetRatioKey] = "bbb"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams = make(map[string]string)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams[DiskPQCodeBudgetRatioKey] = "ccc"
		indexParams[common.IndexTypeKey] = "AISAQ"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams[DiskPQCodeBudgetRatioKey] = "0.2"
		indexParams[common.IndexTypeKey] = "AISAQ"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams[common.DimKey] = "ddd"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams = make(map[string]string)
		indexParams[common.DimKey] = "128"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		indexParams = make(map[string]string)
		indexParams[DiskPQCodeBudgetRatioKey] = "0.2"
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "aaa"
		indexParams[common.IndexTypeKey] = "AISAQ"
		indexParams[common.DimKey] = "128"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.Error(t, err)
		IsConfigableIndexParam(PQCodeBudgetRatioKey)
	})

	t.Run("set disk index load params without auto index param", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"

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

		indexParams = make(map[string]string)
		indexParams["max_degree"] = "56"
		indexParams["search_list"] = "100"
		indexParams[common.DimKey] = "128"
		indexParams[common.IndexTypeKey] = "AISAQ"
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
		extraParams, err := NewBigDataExtraParamsFromJSON("DISKANN", params.AutoIndexConfig.ExtraParams.GetValue())
		assert.NoError(t, err)

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"
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

		indexParams = make(map[string]string)
		indexParams[common.DimKey] = "eee"
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)
		indexParams[common.DimKey] = "128"
		err = SetDiskIndexLoadParams(&params, indexParams, 100)
		assert.Error(t, err)
	})

	t.Run("AutoIndex DISKANN omits disk_pq_code_budget_gb_ratio when not configured", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		// ExtraParams without disk_pq_code_budget_gb → DiskPQCodeBudgetGBRatio defaults to 0.25
		// but for DISKANN we should NOT share AISAQ's default.
		// When ExtraParams doesn't specify disk_pq_code_budget_gb, it gets the struct default (0.25).
		// Let's test with an ExtraParams that explicitly has disk_pq_code_budget_gb = 0 to confirm omission.
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"disk_pq_code_budget_gb\": 0, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"
		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		autoIndexParams := make(map[string]string)
		autoIndexParams[MaxDegreeKey] = "56"
		autoIndexParams[SearchListSizeKey] = "100"
		autoIndexParams[common.IndexTypeKey] = "DISKANN"
		str, err = json.Marshal(autoIndexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// DiskPQCodeBudgetRatioKey must NOT be present (or must not be empty string)
		val, exists := indexParams[DiskPQCodeBudgetRatioKey]
		assert.False(t, exists, "DiskPQCodeBudgetRatioKey should not be set when ratio is 0")
		assert.Equal(t, "", val)

		// Now SetDiskIndexBuildParams should succeed without the key
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"
		indexParams[common.DimKey] = "128"
		err = SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.NoError(t, err)
		// disk_pq_dims should be 0 (no disk PQ)
		assert.Equal(t, "0", indexParams[DiskPQDimsKey])
	})

	t.Run("AutoIndex DISKANN writes disk_pq_code_budget_gb_ratio when explicitly configured", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"disk_pq_code_budget_gb\": 0.2, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"
		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		autoIndexParams := make(map[string]string)
		autoIndexParams[MaxDegreeKey] = "56"
		autoIndexParams[SearchListSizeKey] = "100"
		autoIndexParams[common.IndexTypeKey] = "DISKANN"
		str, err = json.Marshal(autoIndexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "DISKANN"
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// DiskPQCodeBudgetRatioKey must be present with the configured value
		val, exists := indexParams[DiskPQCodeBudgetRatioKey]
		assert.True(t, exists, "DiskPQCodeBudgetRatioKey should be set when ratio is non-zero")
		ratio, err := strconv.ParseFloat(val, 64)
		assert.NoError(t, err)
		assert.Equal(t, 0.2, ratio)
	})

	t.Run("AutoIndex AISAQ omits optional params not in AutoIndex config", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")

		// ExtraParams with disk_pq_code_budget_gb = 0 (no disk PQ for this test)
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"disk_pq_code_budget_gb\": 0, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 4}"
		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.ExtraParams.Key, string(str))

		// AutoIndex config only has required params + pq_cache_size (no optional AISAQ params)
		autoIndexParams := make(map[string]string)
		autoIndexParams[MaxDegreeKey] = "56"
		autoIndexParams[SearchListSizeKey] = "100"
		autoIndexParams[PQCacheSizeKey] = "512"
		autoIndexParams[common.IndexTypeKey] = "AISAQ"
		str, err = json.Marshal(autoIndexParams)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.IndexParams.Key, string(str))

		indexParams := make(map[string]string)
		indexParams[common.IndexTypeKey] = "AISAQ"
		err = FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// None of the optional params should be present
		_, exists := indexParams[DiskPQCodeBudgetRatioKey]
		assert.False(t, exists, "DiskPQCodeBudgetRatioKey should not be set when ratio is 0")
		_, exists = indexParams[VectorsBeamWidthKey]
		assert.False(t, exists, "VectorsBeamWidthKey should not be set when not in config")
		_, exists = indexParams[InlinePQKey]
		assert.False(t, exists, "InlinePQKey should not be set when not in config")
		_, exists = indexParams[PQReadPageCacheSizeKey]
		assert.False(t, exists, "PQReadPageCacheSizeKey should not be set when not in config")
		_, exists = indexParams[RearrangeKey]
		assert.False(t, exists, "RearrangeKey should not be set when not in config")
		_, exists = indexParams[NumEntryPointsKey]
		assert.False(t, exists, "NumEntryPointsKey should not be set when not in config")
	})

	t.Run("SetDiskIndexBuildParams handles empty string DiskPQCodeBudgetRatioKey", func(t *testing.T) {
		// Simulates a legacy scenario where the key exists with an empty value
		indexParams := make(map[string]string)
		indexParams[DiskPQCodeBudgetRatioKey] = "" // empty string
		indexParams[PQCodeBudgetRatioKey] = "0.125"
		indexParams[NumBuildThreadRatioKey] = "1.0"
		indexParams[common.IndexTypeKey] = "DISKANN"
		indexParams[common.DimKey] = "128"
		err := SetDiskIndexBuildParams(indexParams, 100, schemapb.DataType_FloatVector)
		assert.NoError(t, err)
		// disk_pq_dims should be 0 (treated as no disk PQ)
		assert.Equal(t, "0", indexParams[DiskPQDimsKey])
	})
}

func TestBigDataIndex_parse(t *testing.T) {
	t.Run("parse normal", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"disk_pq_code_budget_gb\": 0.2, \"pq_code_budget_gb\": 0.125, \"num_threads\": 1, \"search_cache_budget_gb\": 0.20}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.20, extraParams.SearchCacheBudgetGBRatio)
		assert.Equal(t, 0.20, extraParams.DiskPQCodeBudgetGBRatio)
	})

	t.Run("parse with build_ratio partial or wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.15}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.15, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"num_threads\": 2}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 2.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"num_threads\": 2"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		_, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.Error(t, err)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 8}"
		extraParams, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with prepare_ratio partial or wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.25}"
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.25, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"num_threads\": 4}"
		extraParams, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 4.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)

		mapString = make(map[string]string)
		mapString[BuildRatioKey] = "{\"pq_code_budget_gb\": 0.125, \"num_threads\": 1}"
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225"
		_, err = NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.Error(t, err)
	})

	t.Run("parse with beamwidth wrong", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[BeamWidthRatioKey] = "aa"
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with partial", func(t *testing.T) {
		mapString := make(map[string]string)
		mapString[PrepareRatioKey] = "{\"search_cache_budget_gb\": 0.225, \"num_threads\": 8}"
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.225, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with empty", func(t *testing.T) {
		mapString := make(map[string]string)
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", mapString)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, extraParams.BuildNumThreadsRatio)
		assert.Equal(t, 8.0, extraParams.LoadNumThreadRatio)
		assert.Equal(t, 0.125, extraParams.PQCodeBudgetGBRatio)
		assert.Equal(t, 0.10, extraParams.SearchCacheBudgetGBRatio)
	})

	t.Run("parse with nil", func(t *testing.T) {
		extraParams, err := NewBigDataExtraParamsFromMap("DISKANN", nil)
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
		extraParams, err := NewBigDataExtraParamsFromJSON("DISKANN", jsonStr)
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
		extraParams, err := NewBigDataExtraParamsFromJSON("DISKANN", jsonStr)
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
		extraParams, err := NewBigDataExtraParamsFromJSON("DISKANN", jsonStr)
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
		_, err := NewBigDataExtraParamsFromJSON("DISKANN", jsonStr)
		assert.Error(t, err)
	})

	t.Run("new from json invalid1", func(t *testing.T) {
		jsonStr := `
				""
			`
		_, err := NewBigDataExtraParamsFromJSON("DISKANN", jsonStr)
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

// TestDiskIndexParamsPipelineE2E exercises the full parameter pipeline:
//
//	Proxy (FillDiskIndexParams) → DataNode (SetDiskIndexBuildParams) → QueryNode (SetDiskIndexLoadParams)
//
// for two different configurations and asserts that the final build and load
// output maps are actually different — proving that different input configs
// propagate end-to-end rather than being flattened by defaults.
func TestDiskIndexParamsPipelineE2E(t *testing.T) {
	const (
		dim         = int64(128)
		numRows     = int64(100000)
		fieldDataSz = 4 * dim * numRows // float32 vectors
	)

	// runPipeline simulates the full lifecycle of index parameter handling:
	//   1. FillDiskIndexParams (proxy, CreateIndex time)
	//   2. SetDiskIndexBuildParams (DataNode, build time)
	//   3. SetDiskIndexLoadParams (QueryNode, load time)
	// It returns the indexParams map at each stage for assertion.
	type stageResults struct {
		afterFill  map[string]string
		afterBuild map[string]string
		afterLoad  map[string]string
	}
	runPipeline := func(t *testing.T, params *paramtable.ComponentParam, indexType string) stageResults {
		t.Helper()
		indexParams := map[string]string{
			common.IndexTypeKey: indexType,
		}
		// Stage 1: Proxy — FillDiskIndexParams
		err := FillDiskIndexParams(params, indexParams)
		assert.NoError(t, err)
		afterFill := copyMap(indexParams)

		// Stage 2: DataNode — SetDiskIndexBuildParams
		// Add dim (normally added by the proxy before persisting)
		indexParams[common.DimKey] = strconv.FormatInt(dim, 10)
		err = SetDiskIndexBuildParams(indexParams, fieldDataSz, schemapb.DataType_FloatVector)
		assert.NoError(t, err)
		afterBuild := copyMap(indexParams)

		// Stage 3: QueryNode — SetDiskIndexLoadParams
		err = SetDiskIndexLoadParams(params, indexParams, numRows)
		assert.NoError(t, err)
		afterLoad := copyMap(indexParams)

		return stageResults{afterFill: afterFill, afterBuild: afterBuild, afterLoad: afterLoad}
	}

	t.Run("two DISKANN configs produce different build and load params", func(t *testing.T) {
		// Config A: small PQ budget, low thread ratio, low cache
		var paramsA paramtable.ComponentParam
		paramsA.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsA.Save(paramsA.CommonCfg.PQCodeBudgetGBRatio.Key, "0.05")
		paramsA.Save(paramsA.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.05")
		paramsA.Save(paramsA.CommonCfg.BuildNumThreadsRatio.Key, "0.5")
		paramsA.Save(paramsA.CommonCfg.LoadNumThreadRatio.Key, "0.1")
		paramsA.Save(paramsA.CommonCfg.BeamWidthRatio.Key, "0.1")

		// Config B: large PQ budget, high thread ratio, high cache
		var paramsB paramtable.ComponentParam
		paramsB.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsB.Save(paramsB.CommonCfg.PQCodeBudgetGBRatio.Key, "0.25")
		paramsB.Save(paramsB.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.20")
		paramsB.Save(paramsB.CommonCfg.BuildNumThreadsRatio.Key, "2.0")
		paramsB.Save(paramsB.CommonCfg.LoadNumThreadRatio.Key, "0.5")
		paramsB.Save(paramsB.CommonCfg.BeamWidthRatio.Key, "0.5")

		resA := runPipeline(t, &paramsA, "DISKANN")
		resB := runPipeline(t, &paramsB, "DISKANN")

		// Build stage: pq_code_budget_gb and num_build_thread must differ
		assert.NotEqual(t, resA.afterBuild[PQCodeBudgetKey], resB.afterBuild[PQCodeBudgetKey],
			"pq_code_budget_gb should differ between configs")
		assert.NotEqual(t, resA.afterBuild[NumBuildThreadKey], resB.afterBuild[NumBuildThreadKey],
			"num_build_thread should differ between configs")

		// Load stage: search_cache_budget_gb, num_load_thread, beamwidth must differ
		assert.NotEqual(t, resA.afterLoad[SearchCacheBudgetKey], resB.afterLoad[SearchCacheBudgetKey],
			"search_cache_budget_gb should differ between configs")
		assert.NotEqual(t, resA.afterLoad[NumLoadThreadKey], resB.afterLoad[NumLoadThreadKey],
			"num_load_thread should differ between configs")
		assert.NotEqual(t, resA.afterLoad[BeamWidthKey], resB.afterLoad[BeamWidthKey],
			"beamwidth should differ between configs")
	})

	t.Run("two DISKANN AutoIndex configs produce different build and load params", func(t *testing.T) {
		// Config A: low ratios
		var paramsA paramtable.ComponentParam
		paramsA.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsA.Save(paramsA.AutoIndexConfig.Enable.Key, "true")
		extraA := map[string]string{
			BuildRatioKey:     `{"pq_code_budget_gb": 0.05, "num_threads": 0.5}`,
			PrepareRatioKey:   `{"search_cache_budget_gb": 0.05, "num_threads": 0.1}`,
			BeamWidthRatioKey: "0.1",
		}
		strA, _ := json.Marshal(extraA)
		paramsA.Save(paramsA.AutoIndexConfig.ExtraParams.Key, string(strA))
		autoIdxA := map[string]string{MaxDegreeKey: "48", SearchListSizeKey: "64", common.IndexTypeKey: "DISKANN"}
		strAIdx, _ := json.Marshal(autoIdxA)
		paramsA.Save(paramsA.AutoIndexConfig.IndexParams.Key, string(strAIdx))

		// Config B: high ratios
		var paramsB paramtable.ComponentParam
		paramsB.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsB.Save(paramsB.AutoIndexConfig.Enable.Key, "true")
		extraB := map[string]string{
			BuildRatioKey:     `{"pq_code_budget_gb": 0.25, "num_threads": 2.0}`,
			PrepareRatioKey:   `{"search_cache_budget_gb": 0.20, "num_threads": 0.5}`,
			BeamWidthRatioKey: "0.5",
		}
		strB, _ := json.Marshal(extraB)
		paramsB.Save(paramsB.AutoIndexConfig.ExtraParams.Key, string(strB))
		autoIdxB := map[string]string{MaxDegreeKey: "64", SearchListSizeKey: "128", common.IndexTypeKey: "DISKANN"}
		strBIdx, _ := json.Marshal(autoIdxB)
		paramsB.Save(paramsB.AutoIndexConfig.IndexParams.Key, string(strBIdx))

		resA := runPipeline(t, &paramsA, "DISKANN")
		resB := runPipeline(t, &paramsB, "DISKANN")

		// Fill stage: ratios must differ
		assert.NotEqual(t, resA.afterFill[PQCodeBudgetRatioKey], resB.afterFill[PQCodeBudgetRatioKey])
		assert.NotEqual(t, resA.afterFill[NumBuildThreadRatioKey], resB.afterFill[NumBuildThreadRatioKey])
		assert.NotEqual(t, resA.afterFill[SearchCacheBudgetRatioKey], resB.afterFill[SearchCacheBudgetRatioKey])

		// Build stage: absolute values must differ
		assert.NotEqual(t, resA.afterBuild[PQCodeBudgetKey], resB.afterBuild[PQCodeBudgetKey])
		assert.NotEqual(t, resA.afterBuild[NumBuildThreadKey], resB.afterBuild[NumBuildThreadKey])

		// Load stage: absolute values must differ
		assert.NotEqual(t, resA.afterLoad[SearchCacheBudgetKey], resB.afterLoad[SearchCacheBudgetKey])
		assert.NotEqual(t, resA.afterLoad[NumLoadThreadKey], resB.afterLoad[NumLoadThreadKey])
		assert.NotEqual(t, resA.afterLoad[BeamWidthKey], resB.afterLoad[BeamWidthKey])
	})

	t.Run("DISKANN with and without disk PQ produce different disk_pq_dims", func(t *testing.T) {
		// Config A: no disk PQ (ratio = 0)
		var paramsA paramtable.ComponentParam
		paramsA.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsA.Save(paramsA.AutoIndexConfig.Enable.Key, "true")
		extraA := map[string]string{
			BuildRatioKey:   `{"pq_code_budget_gb": 0.125, "disk_pq_code_budget_gb": 0, "num_threads": 1}`,
			PrepareRatioKey: `{"search_cache_budget_gb": 0.10, "num_threads": 4}`,
		}
		strA, _ := json.Marshal(extraA)
		paramsA.Save(paramsA.AutoIndexConfig.ExtraParams.Key, string(strA))
		autoIdx := map[string]string{MaxDegreeKey: "56", SearchListSizeKey: "100", common.IndexTypeKey: "DISKANN"}
		strIdx, _ := json.Marshal(autoIdx)
		paramsA.Save(paramsA.AutoIndexConfig.IndexParams.Key, string(strIdx))

		// Config B: with disk PQ (ratio = 0.25)
		var paramsB paramtable.ComponentParam
		paramsB.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsB.Save(paramsB.AutoIndexConfig.Enable.Key, "true")
		extraB := map[string]string{
			BuildRatioKey:   `{"pq_code_budget_gb": 0.125, "disk_pq_code_budget_gb": 0.25, "num_threads": 1}`,
			PrepareRatioKey: `{"search_cache_budget_gb": 0.10, "num_threads": 4}`,
		}
		strB, _ := json.Marshal(extraB)
		paramsB.Save(paramsB.AutoIndexConfig.ExtraParams.Key, string(strB))
		paramsB.Save(paramsB.AutoIndexConfig.IndexParams.Key, string(strIdx))

		resA := runPipeline(t, &paramsA, "DISKANN")
		resB := runPipeline(t, &paramsB, "DISKANN")

		// Without disk PQ: disk_pq_dims = 0
		assert.Equal(t, "0", resA.afterBuild[DiskPQDimsKey],
			"disk_pq_dims should be 0 when disk PQ is disabled")
		// With disk PQ: disk_pq_dims > 0
		diskPQDimsB, err := strconv.Atoi(resB.afterBuild[DiskPQDimsKey])
		assert.NoError(t, err)
		assert.Greater(t, diskPQDimsB, 0,
			"disk_pq_dims should be > 0 when disk PQ is enabled")
		// They must differ
		assert.NotEqual(t, resA.afterBuild[DiskPQDimsKey], resB.afterBuild[DiskPQDimsKey])
	})

	t.Run("AISAQ AutoIndex with different ExtraParams produces different outputs", func(t *testing.T) {
		// Config A
		var paramsA paramtable.ComponentParam
		paramsA.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsA.Save(paramsA.AutoIndexConfig.Enable.Key, "true")
		extraA := map[string]string{
			BuildRatioKey:     `{"pq_code_budget_gb": 0.05, "disk_pq_code_budget_gb": 0.15, "num_threads": 0.5}`,
			PrepareRatioKey:   `{"search_cache_budget_gb": 0.05, "num_threads": 0.1}`,
			BeamWidthRatioKey: "0.1",
		}
		strA, _ := json.Marshal(extraA)
		paramsA.Save(paramsA.AutoIndexConfig.ExtraParams.Key, string(strA))
		autoIdxA := map[string]string{
			MaxDegreeKey: "48", SearchListSizeKey: "64",
			PQCacheSizeKey: "256", common.IndexTypeKey: "AISAQ",
		}
		strAIdx, _ := json.Marshal(autoIdxA)
		paramsA.Save(paramsA.AutoIndexConfig.IndexParams.Key, string(strAIdx))

		// Config B
		var paramsB paramtable.ComponentParam
		paramsB.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		paramsB.Save(paramsB.AutoIndexConfig.Enable.Key, "true")
		extraB := map[string]string{
			BuildRatioKey:     `{"pq_code_budget_gb": 0.25, "disk_pq_code_budget_gb": 0.30, "num_threads": 2.0}`,
			PrepareRatioKey:   `{"search_cache_budget_gb": 0.20, "num_threads": 0.5}`,
			BeamWidthRatioKey: "0.5",
		}
		strB, _ := json.Marshal(extraB)
		paramsB.Save(paramsB.AutoIndexConfig.ExtraParams.Key, string(strB))
		autoIdxB := map[string]string{
			MaxDegreeKey: "64", SearchListSizeKey: "128",
			PQCacheSizeKey: "512", common.IndexTypeKey: "AISAQ",
		}
		strBIdx, _ := json.Marshal(autoIdxB)
		paramsB.Save(paramsB.AutoIndexConfig.IndexParams.Key, string(strBIdx))

		resA := runPipeline(t, &paramsA, "AISAQ")
		resB := runPipeline(t, &paramsB, "AISAQ")

		// Build stage
		assert.NotEqual(t, resA.afterBuild[PQCodeBudgetKey], resB.afterBuild[PQCodeBudgetKey])
		assert.NotEqual(t, resA.afterBuild[NumBuildThreadKey], resB.afterBuild[NumBuildThreadKey])
		assert.NotEqual(t, resA.afterBuild[DiskPQDimsKey], resB.afterBuild[DiskPQDimsKey])

		// Load stage
		assert.NotEqual(t, resA.afterLoad[SearchCacheBudgetKey], resB.afterLoad[SearchCacheBudgetKey])
		assert.NotEqual(t, resA.afterLoad[NumLoadThreadKey], resB.afterLoad[NumLoadThreadKey])
		assert.NotEqual(t, resA.afterLoad[BeamWidthKey], resB.afterLoad[BeamWidthKey])
	})

	t.Run("persisted ratios are respected at load time over global config", func(t *testing.T) {
		// This tests that SetDiskIndexLoadParams reads persisted values from indexParams
		// rather than always re-reading global config.
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		// Set global config to specific values
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.10")
		params.Save(params.CommonCfg.LoadNumThreadRatio.Key, "4.0")
		params.Save(params.CommonCfg.BeamWidthRatio.Key, "4.0")

		// Simulate indexParams as persisted with DIFFERENT ratio values
		indexParams := map[string]string{
			common.IndexTypeKey:       "DISKANN",
			common.DimKey:             strconv.FormatInt(dim, 10),
			SearchCacheBudgetRatioKey: "0.30",
			NumLoadThreadRatioKey:     "1.0",
			BeamWidthRatioKey:         "1.0",
		}

		err := SetDiskIndexLoadParams(&params, indexParams, numRows)
		assert.NoError(t, err)

		// Verify it used the persisted 0.30, not the global 0.10
		expectedCacheBudget := fmt.Sprintf("%f",
			float32(getRowDataSizeOfFloatVector(numRows, dim))*float32(0.30)/(1<<30))
		assert.Equal(t, expectedCacheBudget, indexParams[SearchCacheBudgetKey],
			"should use persisted search_cache_budget_gb_ratio, not global config")

		// Verify thread/beamwidth used persisted 1.0, not global 4.0
		expectedThreads := int(float32(hardware.GetCPUNum()) * 1.0)
		if expectedThreads > MaxLoadThread {
			expectedThreads = MaxLoadThread
		}
		assert.Equal(t, strconv.Itoa(expectedThreads), indexParams[NumLoadThreadKey],
			"should use persisted num_load_thread_ratio, not global config")

		expectedBeam := int(float32(hardware.GetCPUNum()) * 1.0)
		if expectedBeam > MaxBeamWidth {
			expectedBeam = MaxBeamWidth
		}
		assert.Equal(t, strconv.Itoa(expectedBeam), indexParams[BeamWidthKey],
			"should use persisted beamwidth_ratio, not global config")
	})

	t.Run("config changes propagate to indexes without explicit search_cache_budget_gb_ratio", func(t *testing.T) {
		// This tests the fix for the frozen search_cache_budget_gb_ratio bug:
		// When a DISKANN index is created WITHOUT the user explicitly setting
		// search_cache_budget_gb_ratio, changing the global config and reloading
		// should use the NEW config value (not the old frozen default).
		const (
			testDim     = int64(128)
			testNumRows = int64(100000)
		)

		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		// Step 1: Create index with initial config (user does NOT specify search_cache_budget_gb_ratio)
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.10")
		indexParams := map[string]string{
			common.IndexTypeKey: "DISKANN",
		}
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// Verify: SearchCacheBudgetRatioKey should NOT be persisted
		_, exists := indexParams[SearchCacheBudgetRatioKey]
		assert.False(t, exists,
			"SearchCacheBudgetRatioKey should not be persisted when defaulted from global config")

		// Step 2: Simulate "time passes" — admin changes the config
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.25")

		// Step 3: Load the index (simulates QueryNode loading existing index)
		indexParams[common.DimKey] = strconv.FormatInt(testDim, 10)
		err = SetDiskIndexLoadParams(&params, indexParams, testNumRows)
		assert.NoError(t, err)

		// Step 4: Verify the NEW config value (0.25) is used, not the old one (0.10)
		expectedCacheBudget := fmt.Sprintf("%f",
			float32(getRowDataSizeOfFloatVector(testNumRows, testDim))*float32(0.25)/(1<<30))
		assert.Equal(t, expectedCacheBudget, indexParams[SearchCacheBudgetKey],
			"should use the NEW global config value (0.25), not the old frozen one (0.10)")
	})

	t.Run("user-specified search_cache_budget_gb_ratio is persisted and sticky", func(t *testing.T) {
		// When user explicitly sets search_cache_budget_gb_ratio in CreateIndex,
		// it should be persisted and NOT overridden by subsequent config changes.
		const (
			testDim     = int64(128)
			testNumRows = int64(100000)
		)

		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		// Step 1: Create index with user-specified ratio
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.10")
		indexParams := map[string]string{
			common.IndexTypeKey:       "DISKANN",
			SearchCacheBudgetRatioKey: "0.50", // user explicitly sets this
		}
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// Verify: SearchCacheBudgetRatioKey IS persisted with user's value
		val, exists := indexParams[SearchCacheBudgetRatioKey]
		assert.True(t, exists,
			"SearchCacheBudgetRatioKey should be persisted when user explicitly set it")
		assert.Equal(t, "0.50", val)

		// Step 2: Admin changes config
		params.Save(params.CommonCfg.SearchCacheBudgetGBRatio.Key, "0.25")

		// Step 3: Load the index
		indexParams[common.DimKey] = strconv.FormatInt(testDim, 10)
		err = SetDiskIndexLoadParams(&params, indexParams, testNumRows)
		assert.NoError(t, err)

		// Step 4: Verify the USER's value (0.50) is used, not the new config (0.25)
		expectedCacheBudget := fmt.Sprintf("%f",
			float32(getRowDataSizeOfFloatVector(testNumRows, testDim))*float32(0.50)/(1<<30))
		assert.Equal(t, expectedCacheBudget, indexParams[SearchCacheBudgetKey],
			"should use the user-specified value (0.50), not the changed config (0.25)")
	})

	t.Run("AISAQ config changes propagate to indexes without explicit search_cache_budget_gb_ratio", func(t *testing.T) {
		// Same test as above but for AISAQ index type.
		const (
			testDim     = int64(128)
			testNumRows = int64(100000)
		)

		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))

		// Step 1: Create AISAQ index without user specifying search_cache_budget_gb_ratio
		params.Save(params.CommonCfg.AiSAQCfg.SearchCacheBudgetGBRatio.Key, "0.05")
		params.Save(params.CommonCfg.AiSAQCfg.PQCacheSize.Key, "512")
		params.Save(params.CommonCfg.AiSAQCfg.InlinePQ.Key, "0")
		params.Save(params.CommonCfg.AiSAQCfg.Rearrange.Key, "true")
		params.Save(params.CommonCfg.AiSAQCfg.PQReadPageCacheSize.Key, "512")
		params.Save(params.CommonCfg.AiSAQCfg.NumEntryPoints.Key, "100")
		indexParams := map[string]string{
			common.IndexTypeKey: "AISAQ",
		}
		err := FillDiskIndexParams(&params, indexParams)
		assert.NoError(t, err)

		// Verify: SearchCacheBudgetRatioKey should NOT be persisted
		_, exists := indexParams[SearchCacheBudgetRatioKey]
		assert.False(t, exists,
			"SearchCacheBudgetRatioKey should not be persisted for AISAQ when defaulted from global config")

		// Step 2: Admin changes the config
		params.Save(params.CommonCfg.AiSAQCfg.SearchCacheBudgetGBRatio.Key, "0.20")

		// Step 3: Load the index
		indexParams[common.DimKey] = strconv.FormatInt(testDim, 10)
		err = SetDiskIndexLoadParams(&params, indexParams, testNumRows)
		assert.NoError(t, err)

		// Step 4: Verify the NEW config value (0.20) is used, not the old one (0.05)
		expectedCacheBudget := fmt.Sprintf("%f",
			float32(getRowDataSizeOfFloatVector(testNumRows, testDim))*float32(0.20)/(1<<30))
		assert.Equal(t, expectedCacheBudget, indexParams[SearchCacheBudgetKey],
			"should use the NEW AISAQ config value (0.20), not the old frozen one (0.05)")
	})
}

func copyMap(m map[string]string) map[string]string {
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
