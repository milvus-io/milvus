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
	"unsafe"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	PQCodeBudgetRatioKey      = "pq_code_budget_gb_ratio"
	NumBuildThreadRatioKey    = "num_build_thread_ratio"
	SearchCacheBudgetRatioKey = "search_cache_budget_gb_ratio"
	NumLoadThreadRatioKey     = "num_load_thread_ratio"
	BeamWidthRatioKey         = "beamwidth_ratio"

	MaxDegreeKey         = "max_degree"
	SearchListSizeKey    = "search_list_size"
	PQCodeBudgetKey      = "pq_code_budget_gb"
	BuildDramBudgetKey   = "build_dram_budget_gb"
	NumBuildThreadKey    = "num_build_thread"
	SearchCacheBudgetKey = "search_cache_budget_gb"
	NumLoadThreadKey     = "num_load_thread"
	BeamWidthKey         = "beamwidth"

	MaxLoadThread = 64
	MaxBeamWidth  = 16
)

func getRowDataSizeOfFloatVector(numRows int64, dim int64) int64 {
	var floatValue float32
	/* #nosec G103 */
	return int64(unsafe.Sizeof(floatValue)) * dim * numRows
}

type BigDataIndexExtraParams struct {
	PQCodeBudgetGBRatio      float64
	BuildNumThreadsRatio     float64
	SearchCacheBudgetGBRatio float64
	LoadNumThreadRatio       float64
	BeamWidthRatio           float64
}

const (
	BuildRatioKey                   = "build_ratio"
	PrepareRatioKey                 = "prepare_ratio"
	DefaultPQCodeBudgetGBRatio      = 0.125
	DefaultBuildNumThreadsRatio     = 1.0
	DefaultSearchCacheBudgetGBRatio = 0.10
	DefaultLoadNumThreadRatio       = 8.0
	DefaultBeamWidthRatio           = 4.0
)

func NewBigDataExtraParamsFromJSON(jsonStr string) (*BigDataIndexExtraParams, error) {
	buffer, err := funcutil.JSONToMap(jsonStr)
	if err != nil {
		return nil, err
	}
	return NewBigDataExtraParamsFromMap(buffer)
}

func NewBigDataExtraParamsFromMap(value map[string]string) (*BigDataIndexExtraParams, error) {
	ret := &BigDataIndexExtraParams{}
	ret.SearchCacheBudgetGBRatio = DefaultSearchCacheBudgetGBRatio
	setSearchCache := false
	var err error
	buildRatio, ok := value[BuildRatioKey]
	if !ok {
		ret.PQCodeBudgetGBRatio = DefaultPQCodeBudgetGBRatio
		ret.BuildNumThreadsRatio = DefaultBuildNumThreadsRatio
	} else {
		valueMap1 := make(map[string]float64)
		err = json.Unmarshal([]byte(buildRatio), &valueMap1)
		if err != nil {
			return ret, err
		}
		PQCodeBudgetGBRatio, ok := valueMap1["pq_code_budget_gb"]
		if !ok {
			ret.PQCodeBudgetGBRatio = DefaultPQCodeBudgetGBRatio
		} else {
			ret.PQCodeBudgetGBRatio = PQCodeBudgetGBRatio
		}
		BuildNumThreadsRatio, ok := valueMap1["num_threads"]
		if !ok {
			ret.BuildNumThreadsRatio = DefaultBuildNumThreadsRatio
		} else {
			ret.BuildNumThreadsRatio = BuildNumThreadsRatio
		}
		SearchCacheBudgetGBRatio, ok := valueMap1["search_cache_budget_gb"]
		if ok {
			ret.SearchCacheBudgetGBRatio = SearchCacheBudgetGBRatio
			setSearchCache = true
		}
	}

	prepareRatio, ok := value[PrepareRatioKey]
	if !ok {
		ret.SearchCacheBudgetGBRatio = DefaultSearchCacheBudgetGBRatio
		ret.LoadNumThreadRatio = DefaultLoadNumThreadRatio
	} else {
		valueMap2 := make(map[string]float64)
		err = json.Unmarshal([]byte(prepareRatio), &valueMap2)
		if err != nil {
			return ret, err
		}
		SearchCacheBudgetGBRatio, ok := valueMap2["search_cache_budget_gb"]
		if ok && !setSearchCache {
			ret.SearchCacheBudgetGBRatio = SearchCacheBudgetGBRatio
		}
		LoadNumThreadRatio, ok := valueMap2["num_threads"]
		if !ok {
			ret.LoadNumThreadRatio = DefaultLoadNumThreadRatio
		} else {
			ret.LoadNumThreadRatio = LoadNumThreadRatio
		}
	}
	beamWidthRatioStr, ok := value[BeamWidthRatioKey]
	if !ok {
		ret.BeamWidthRatio = DefaultBeamWidthRatio
	} else {
		beamWidthRatio, err := strconv.ParseFloat(beamWidthRatioStr, 64)
		if err != nil {
			ret.BeamWidthRatio = DefaultBeamWidthRatio
		} else {
			ret.BeamWidthRatio = beamWidthRatio
		}
	}

	return ret, nil
}

// FillDiskIndexParams fill ratio params to index param on proxy node
// Which will be used to calculate build and load params
func FillDiskIndexParams(params *paramtable.ComponentParam, indexParams map[string]string) error {
	maxDegree := params.CommonCfg.MaxDegree.GetValue()
	searchListSize := params.CommonCfg.SearchListSize.GetValue()
	pqCodeBudgetGBRatio := params.CommonCfg.PQCodeBudgetGBRatio.GetValue()
	buildNumThreadsRatio := params.CommonCfg.BuildNumThreadsRatio.GetValue()
	searchCacheBudgetGBRatio := params.CommonCfg.SearchCacheBudgetGBRatio.GetValue()

	if params.AutoIndexConfig.Enable.GetAsBool() {
		indexParams := params.AutoIndexConfig.IndexParams.GetAsJSONMap()
		var ok bool
		maxDegree, ok = indexParams[MaxDegreeKey]
		if !ok {
			return fmt.Errorf("index param max_degree not exist")
		}
		searchListSize, ok = indexParams[SearchListSizeKey]
		if !ok {
			return fmt.Errorf("index param search_list_size not exist")
		}
		extraParams, err := NewBigDataExtraParamsFromJSON(params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		pqCodeBudgetGBRatio = fmt.Sprintf("%f", extraParams.PQCodeBudgetGBRatio)
		buildNumThreadsRatio = fmt.Sprintf("%f", extraParams.BuildNumThreadsRatio)
	}

	indexParams[MaxDegreeKey] = maxDegree
	indexParams[SearchListSizeKey] = searchListSize
	indexParams[PQCodeBudgetRatioKey] = pqCodeBudgetGBRatio
	indexParams[NumBuildThreadRatioKey] = buildNumThreadsRatio
	indexParams[SearchCacheBudgetRatioKey] = searchCacheBudgetGBRatio

	return nil
}

// SetDiskIndexBuildParams set index build params with ratio params on indexNode
// IndexNode cal build param with ratio params and cpu count, memory count...
func SetDiskIndexBuildParams(indexParams map[string]string, fieldDataSize int64) error {
	pqCodeBudgetGBRatioStr, ok := indexParams[PQCodeBudgetRatioKey]
	if !ok {
		return fmt.Errorf("index param pqCodeBudgetGBRatio not exist")
	}
	pqCodeBudgetGBRatio, err := strconv.ParseFloat(pqCodeBudgetGBRatioStr, 64)
	if err != nil {
		return err
	}
	buildNumThreadsRatioStr, ok := indexParams[NumBuildThreadRatioKey]
	if !ok {
		return fmt.Errorf("index param buildNumThreadsRatio not exist")
	}
	buildNumThreadsRatio, err := strconv.ParseFloat(buildNumThreadsRatioStr, 64)
	if err != nil {
		return err
	}

	searchCacheBudgetGBRatioStr, ok := indexParams[SearchCacheBudgetRatioKey]
	if !ok {
		return fmt.Errorf("index param searchCacheBudgetGBRatio not exist")
	}
	SearchCacheBudgetGBRatio, err := strconv.ParseFloat(searchCacheBudgetGBRatioStr, 64)
	if err != nil {
		return err
	}
	indexParams[PQCodeBudgetKey] = fmt.Sprintf("%f", float32(fieldDataSize)*float32(pqCodeBudgetGBRatio)/(1<<30))
	indexParams[NumBuildThreadKey] = strconv.Itoa(int(float32(hardware.GetCPUNum()) * float32(buildNumThreadsRatio)))
	indexParams[BuildDramBudgetKey] = fmt.Sprintf("%f", float32(hardware.GetFreeMemoryCount())/(1<<30))
	indexParams[SearchCacheBudgetKey] = fmt.Sprintf("%f", float32(fieldDataSize)*float32(SearchCacheBudgetGBRatio)/(1<<30))
	return nil
}

// SetDiskIndexLoadParams set disk index load params with ratio params on queryNode
// QueryNode cal load params with ratio params ans cpu count...
func SetDiskIndexLoadParams(params *paramtable.ComponentParam, indexParams map[string]string, numRows int64) error {
	dimStr, ok := indexParams[common.DimKey]
	if !ok {
		// type param dim has been put into index params before build index
		return fmt.Errorf("type param dim not exist")
	}
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return err
	}

	var searchCacheBudgetGBRatio float64
	var loadNumThreadRatio float64
	var beamWidthRatio float64

	if params.AutoIndexConfig.Enable.GetAsBool() {
		extraParams, err := NewBigDataExtraParamsFromJSON(params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		searchCacheBudgetGBRatio = extraParams.SearchCacheBudgetGBRatio
		loadNumThreadRatio = extraParams.LoadNumThreadRatio
		beamWidthRatio = extraParams.BeamWidthRatio
	} else {
		searchCacheBudgetGBRatio, err = strconv.ParseFloat(params.CommonCfg.SearchCacheBudgetGBRatio.GetValue(), 64)
		if err != nil {
			return err
		}
		loadNumThreadRatio, err = strconv.ParseFloat(params.CommonCfg.LoadNumThreadRatio.GetValue(), 64)
		if err != nil {
			return err
		}
		beamWidthRatio, err = strconv.ParseFloat(params.CommonCfg.BeamWidthRatio.GetValue(), 64)
		if err != nil {
			return err
		}

	}

	indexParams[SearchCacheBudgetKey] = fmt.Sprintf("%f",
		float32(getRowDataSizeOfFloatVector(numRows, dim))*float32(searchCacheBudgetGBRatio)/(1<<30))

	numLoadThread := int(float32(hardware.GetCPUNum()) * float32(loadNumThreadRatio))
	if numLoadThread > MaxLoadThread {
		numLoadThread = MaxLoadThread
	}
	indexParams[NumLoadThreadKey] = strconv.Itoa(numLoadThread)

	beamWidth := int(float32(hardware.GetCPUNum()) * float32(beamWidthRatio))
	if beamWidth > MaxBeamWidth {
		beamWidth = MaxBeamWidth
	}
	indexParams[BeamWidthKey] = strconv.Itoa(beamWidth)

	return nil
}
