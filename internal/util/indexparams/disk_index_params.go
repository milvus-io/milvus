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
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/paramtable"
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

// FillDiskIndexParams fill ratio params to index param on proxy node
// Which will be used to calculate build and load params
func FillDiskIndexParams(params *paramtable.ComponentParam, indexParams map[string]string) error {
	maxDegree := strconv.FormatInt(params.CommonCfg.MaxDegree, 10)
	searchListSize := strconv.FormatInt(params.CommonCfg.SearchListSize, 10)
	pgCodeBudgetGBRatio := params.CommonCfg.PGCodeBudgetGBRatio
	buildNumThreadsRatio := params.CommonCfg.BuildNumThreadsRatio

	searchCacheBudgetGBRatio := params.CommonCfg.SearchCacheBudgetGBRatio
	loadNumThreadRatio := params.CommonCfg.LoadNumThreadRatio
	beamWidthRatio := params.CommonCfg.BeamWidthRatio

	if params.AutoIndexConfig.Enable {
		var ok bool
		maxDegree, ok = params.AutoIndexConfig.IndexParams[MaxDegreeKey]
		if !ok {
			return fmt.Errorf("index param max_degree not exist")
		}
		searchListSize, ok = params.AutoIndexConfig.IndexParams[SearchListSizeKey]
		if !ok {
			return fmt.Errorf("index param search_list_size not exist")
		}
		pgCodeBudgetGBRatio = params.AutoIndexConfig.BigDataExtraParams.PGCodeBudgetGBRatio
		buildNumThreadsRatio = params.AutoIndexConfig.BigDataExtraParams.BuildNumThreadsRatio
		searchCacheBudgetGBRatio = params.AutoIndexConfig.BigDataExtraParams.SearchCacheBudgetGBRatio
		loadNumThreadRatio = params.AutoIndexConfig.BigDataExtraParams.LoadNumThreadRatio
		beamWidthRatio = params.AutoIndexConfig.BigDataExtraParams.BeamWidthRatio
	}

	indexParams[MaxDegreeKey] = maxDegree
	indexParams[SearchListSizeKey] = searchListSize
	indexParams[PQCodeBudgetRatioKey] = fmt.Sprintf("%f", pgCodeBudgetGBRatio)
	indexParams[NumBuildThreadRatioKey] = fmt.Sprintf("%f", buildNumThreadsRatio)
	indexParams[SearchCacheBudgetRatioKey] = fmt.Sprintf("%f", searchCacheBudgetGBRatio)
	indexParams[NumLoadThreadRatioKey] = fmt.Sprintf("%f", loadNumThreadRatio)
	indexParams[BeamWidthRatioKey] = fmt.Sprintf("%f", beamWidthRatio)

	return nil
}

// SetDiskIndexBuildParams set index build params with ratio params on indexNode
// IndexNode cal build param with ratio params and cpu count, memory count...
func SetDiskIndexBuildParams(indexParams map[string]string, numRows int64) error {
	dimStr, ok := indexParams["dim"]
	if !ok {
		// type param dim has been put into index params before build index
		return fmt.Errorf("type param dim not exist")
	}
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return err
	}

	pgCodeBudgetGBRatioStr, ok := indexParams[PQCodeBudgetRatioKey]
	if !ok {
		return fmt.Errorf("index param pgCodeBudgetGBRatio not exist")
	}
	pgCodeBudgetGBRatio, err := strconv.ParseFloat(pgCodeBudgetGBRatioStr, 64)
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

	indexParams[PQCodeBudgetKey] = fmt.Sprintf("%f",
		float32(getRowDataSizeOfFloatVector(numRows, dim))*float32(pgCodeBudgetGBRatio)/(1<<30))
	indexParams[NumBuildThreadKey] = strconv.Itoa(int(float32(hardware.GetCPUNum()) * float32(buildNumThreadsRatio)))
	indexParams[BuildDramBudgetKey] = fmt.Sprintf("%f", float32(hardware.GetFreeMemoryCount())/(1<<30))

	return nil
}

// SetDiskIndexLoadParams set disk index load params with ratio params on queryNode
// QueryNode cal load params with ratio params ans cpu count...
func SetDiskIndexLoadParams(indexParams map[string]string, numRows int64) error {
	dimStr, ok := indexParams["dim"]
	if !ok {
		// type param dim has been put into index params before build index
		return fmt.Errorf("type param dim not exist")
	}
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return err
	}

	searchCacheBudgetGBRatioStr, ok := indexParams[SearchCacheBudgetRatioKey]
	if !ok {
		return fmt.Errorf("index param searchCacheBudgetGBRatio not exist")
	}
	searchCacheBudgetGBRatio, err := strconv.ParseFloat(searchCacheBudgetGBRatioStr, 64)
	if err != nil {
		return err
	}
	loadNumThreadRatioStr, ok := indexParams[NumLoadThreadRatioKey]
	if !ok {
		return fmt.Errorf("index param loadNumThreadRatio not exist")
	}
	loadNumThreadRatio, err := strconv.ParseFloat(loadNumThreadRatioStr, 64)
	if err != nil {
		return err
	}
	beamWidthRatioStr, ok := indexParams[BeamWidthRatioKey]
	if !ok {
		return fmt.Errorf("index param beamWidthRatio not exist")
	}
	beamWidthRatio, err := strconv.ParseFloat(beamWidthRatioStr, 64)
	if err != nil {
		return err
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
