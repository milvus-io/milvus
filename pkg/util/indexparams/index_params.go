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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/util/vecindex"
)

const (
	PQCodeBudgetRatioKey      = "pq_code_budget_gb_ratio"
	NumBuildThreadRatioKey    = "num_build_thread_ratio"
	SearchCacheBudgetRatioKey = "search_cache_budget_gb_ratio"
	NumLoadThreadRatioKey     = "num_load_thread_ratio"
	BeamWidthRatioKey         = "beamwidth_ratio"
	DiskPQCodeBudgetRatioKey  = "disk_pq_code_budget_gb_ratio"

	MaxDegreeKey           = "max_degree"
	SearchListSizeKey      = "search_list_size"
	PQCodeBudgetKey        = "pq_code_budget_gb"
	BuildDramBudgetKey     = "build_dram_budget_gb"
	NumBuildThreadKey      = "num_build_thread"
	SearchCacheBudgetKey   = "search_cache_budget_gb"
	NumLoadThreadKey       = "num_load_thread"
	BeamWidthKey           = "beamwidth"
	DiskPQDimsKey          = "disk_pq_dims"
	VectorsBeamWidthKey    = "vectors_beamwidth"
	InlinePQKey            = "inline_pq"
	NumEntryPointsKey      = "num_entry_points"
	PQCacheSizeKey         = "pq_cache_size"
	PQReadPageCacheSizeKey = "pq_read_page_cache_size"
	RearrangeKey           = "rearrange"

	MaxLoadThread = 64
	MaxBeamWidth  = 16
)

var configableIndexParams = typeutil.NewSet[string]()

func init() {
	configableIndexParams.Insert(common.MmapEnabledKey)
	configableIndexParams.Insert(common.IndexOffsetCacheEnabledKey)
	configableIndexParams.Insert(common.WarmupKey)
}

func IsConfigableIndexParam(key string) bool {
	return configableIndexParams.Contain(key)
}

func getDiskPQDims(diskPQCodeBudgetGBRatio float64, dim int64, dataType schemapb.DataType) (int, error) {
	switch dataType {
	case schemapb.DataType_BinaryVector:
		return int((float32(dim) * float32(diskPQCodeBudgetGBRatio)) / 8), nil
	case schemapb.DataType_FloatVector:
		return int(float32(dim) * (float32(diskPQCodeBudgetGBRatio) * 4)), nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return int(float32(dim) * (float32(diskPQCodeBudgetGBRatio) * 2)), nil
	case schemapb.DataType_SparseFloatVector:
		return 0, merr.WrapErrServiceInternalMsg("could not estimate DiskPQDims of SparseFloatVector")
	default:
		return 0, nil
	}
}

func getRowDataSizeOfFloatVector(numRows int64, dim int64) int64 {
	var floatValue float32
	/* #nosec G103 */
	return int64(unsafe.Sizeof(floatValue)) * dim * numRows
}

type BigDataIndexExtraParams struct {
	PQCodeBudgetGBRatio           float64
	DiskPQCodeBudgetGBRatio       float64
	HasDiskPQCodeBudgetGBRatio    bool // true when explicitly configured (vs defaulted)
	BuildNumThreadsRatio          float64
	SearchCacheBudgetGBRatio      float64
	HasSearchCacheBudgetGBRatio   bool // true when explicitly configured (vs defaulted)
	AiSAQSearchCacheBudgetGBRatio float64
	LoadNumThreadRatio            float64
	BeamWidthRatio                float64
}

const (
	DefaultDiskPQCodeBudgetGBRatio       = 0.25
	DefaultAiSAQSearchCacheBudgetGBRatio = 0
	BuildRatioKey                        = "build_ratio"
	PrepareRatioKey                      = "prepare_ratio"
	DefaultPQCodeBudgetGBRatio           = 0.125
	DefaultBuildNumThreadsRatio          = 1.0
	DefaultSearchCacheBudgetGBRatio      = 0.10
	DefaultLoadNumThreadRatio            = 8.0
	DefaultBeamWidthRatio                = 4.0
)

func NewBigDataExtraParamsFromJSON(indexType string, jsonStr string) (*BigDataIndexExtraParams, error) {
	buffer, err := funcutil.JSONToMap(jsonStr)
	if err != nil {
		return nil, err
	}
	return NewBigDataExtraParamsFromMap(indexType, buffer)
}

func NewBigDataExtraParamsFromMap(indexType string, value map[string]string) (*BigDataIndexExtraParams, error) {
	ret := &BigDataIndexExtraParams{}
	if vecindex.IsDiskANN(indexType) {
		ret.SearchCacheBudgetGBRatio = DefaultSearchCacheBudgetGBRatio
	}
	if vecindex.IsAISAQ(indexType) {
		ret.SearchCacheBudgetGBRatio = DefaultAiSAQSearchCacheBudgetGBRatio
	}
	setSearchCache := false
	var err error
	buildRatio, ok := value[BuildRatioKey]
	if !ok {
		ret.PQCodeBudgetGBRatio = DefaultPQCodeBudgetGBRatio
		ret.DiskPQCodeBudgetGBRatio = DefaultDiskPQCodeBudgetGBRatio
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
		DiskPQCodeBudgetGBRatio, ok := valueMap1["disk_pq_code_budget_gb"]
		if !ok {
			ret.DiskPQCodeBudgetGBRatio = DefaultDiskPQCodeBudgetGBRatio
		} else {
			ret.DiskPQCodeBudgetGBRatio = DiskPQCodeBudgetGBRatio
			ret.HasDiskPQCodeBudgetGBRatio = true
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
			ret.HasSearchCacheBudgetGBRatio = true
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
			ret.HasSearchCacheBudgetGBRatio = true
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
	var maxDegree string
	var searchListSize string
	var pqCodeBudgetGBRatio string
	var diskPQCodeBudgetGBRatio string
	var buildNumThreadsRatio string
	var searchCacheBudgetGBRatio string
	// Track whether search_cache_budget_gb_ratio was explicitly set (by user or AutoIndex config)
	// vs defaulted from global config. Only persist when explicitly set so that config changes
	// propagate to existing indexes at load time.
	hasSearchCacheBudgetGBRatio := false

	indexType, ok := indexParams[common.IndexTypeKey]
	if !ok {
		return merr.WrapErrServiceInternalMsg("type param indexType not exist")
	}

	if vecindex.IsAISAQ(indexType) {
		return FillAiSAQIndexParams(params, indexParams)
	}

	if params.AutoIndexConfig.Enable.GetAsBool() {
		indexParams := params.AutoIndexConfig.IndexParams.GetAsJSONMap()
		var ok bool
		maxDegree, ok = indexParams[MaxDegreeKey]
		if !ok {
			return merr.WrapErrParameterInvalidMsg("index param max_degree not exist")
		}
		searchListSize, ok = indexParams[SearchListSizeKey]
		if !ok {
			return merr.WrapErrParameterInvalidMsg("index param search_list_size not exist")
		}
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		pqCodeBudgetGBRatio = fmt.Sprintf("%f", extraParams.PQCodeBudgetGBRatio)
		buildNumThreadsRatio = fmt.Sprintf("%f", extraParams.BuildNumThreadsRatio)
		searchCacheBudgetGBRatio = fmt.Sprintf("%f", extraParams.SearchCacheBudgetGBRatio)
		// AutoIndex ExtraParams are operator-managed; always persist them.
		hasSearchCacheBudgetGBRatio = true
		// Only set diskPQCodeBudgetGBRatio if explicitly configured in ExtraParams.
		// A zero value means "no disk PQ" (Knowhere stores uncompressed vectors on SSD),
		// but we must not write an empty string which would cause ParseFloat("") errors downstream.
		// For DISKANN, do NOT apply the default 0.25 (that's AISAQ's default); only use
		// the value when the operator explicitly configured disk_pq_code_budget_gb.
		if extraParams.HasDiskPQCodeBudgetGBRatio && extraParams.DiskPQCodeBudgetGBRatio != 0 {
			diskPQCodeBudgetGBRatio = fmt.Sprintf("%f", extraParams.DiskPQCodeBudgetGBRatio)
		}
	} else {
		var ok bool
		diskPQCodeBudgetGBRatio, ok = indexParams[DiskPQCodeBudgetRatioKey]
		if !ok {
			diskPQCodeBudgetGBRatio = strconv.Itoa(0)
		}
		maxDegree, ok = indexParams[MaxDegreeKey]
		if !ok {
			maxDegree = params.CommonCfg.MaxDegree.GetValue()
		}
		searchListSize, ok = indexParams[SearchListSizeKey]
		if !ok {
			searchListSize = params.CommonCfg.SearchListSize.GetValue()
		}
		pqCodeBudgetGBRatio, ok = indexParams[PQCodeBudgetRatioKey]
		if !ok {
			pqCodeBudgetGBRatio = params.CommonCfg.PQCodeBudgetGBRatio.GetValue()
		}
		searchCacheBudgetGBRatio, ok = indexParams[SearchCacheBudgetRatioKey]
		if !ok {
			searchCacheBudgetGBRatio = params.CommonCfg.SearchCacheBudgetGBRatio.GetValue()
		} else {
			// User explicitly provided the ratio in CreateIndex request — persist it.
			hasSearchCacheBudgetGBRatio = true
		}
		buildNumThreadsRatio, ok = indexParams[NumBuildThreadRatioKey]
		if !ok {
			buildNumThreadsRatio = params.CommonCfg.BuildNumThreadsRatio.GetValue()
		}
	}

	indexParams[MaxDegreeKey] = maxDegree
	indexParams[SearchListSizeKey] = searchListSize
	indexParams[PQCodeBudgetRatioKey] = pqCodeBudgetGBRatio
	indexParams[NumBuildThreadRatioKey] = buildNumThreadsRatio
	// Only persist SearchCacheBudgetRatioKey when explicitly set by user or AutoIndex config.
	// When it comes from global config defaults, omit it so SetDiskIndexLoadParams reads
	// the live config value — allowing config changes to propagate without index rebuild.
	if hasSearchCacheBudgetGBRatio {
		indexParams[SearchCacheBudgetRatioKey] = searchCacheBudgetGBRatio
	}
	// Only persist DiskPQCodeBudgetRatioKey when explicitly provided.
	// An empty string would cause ParseFloat("") errors in SetDiskIndexBuildParams;
	// omitting the key lets Knowhere use its default (disk_pq_dims=0, no disk PQ).
	if diskPQCodeBudgetGBRatio != "" {
		indexParams[DiskPQCodeBudgetRatioKey] = diskPQCodeBudgetGBRatio
	}

	return nil
}

// FillAiSAQIndexParams fill ratio params to index param on proxy node
// Which will be used to calculate build and load params
func FillAiSAQIndexParams(params *paramtable.ComponentParam, indexParams map[string]string) error {
	var maxDegree string
	var searchListSize string
	var pqCodeBudgetGBRatio string
	var searchCacheBudgetGBRatio string
	var diskPQCodeBudgetGBRatio string
	var buildNumThreadsRatio string
	var aisVectorsBeamWidth string
	var inlinePQ string
	var pqCacheSize string
	var pqReadPageCacheSize string
	var rearrange string
	var numEntryPoints string
	var pqCacheSizeBytes string
	// Track whether search_cache_budget_gb_ratio was explicitly set (by user or AutoIndex config)
	// vs defaulted from global config. Only persist when explicitly set so that config changes
	// propagate to existing indexes at load time.
	hasSearchCacheBudgetGBRatio := false
	indexType, ok := indexParams[common.IndexTypeKey]
	if !ok {
		return merr.WrapErrServiceInternalMsg("type param indexType not exist")
	}

	if params.AutoIndexConfig.Enable.GetAsBool() {
		indexParams := params.AutoIndexConfig.IndexParams.GetAsJSONMap()
		var ok bool
		maxDegree, ok = indexParams[MaxDegreeKey]
		if !ok {
			return merr.WrapErrServiceInternalMsg("index param max_degree not exist")
		}
		searchListSize, ok = indexParams[SearchListSizeKey]
		if !ok {
			return merr.WrapErrServiceInternalMsg("index param search_list_size not exist")
		}
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		pqCodeBudgetGBRatio = fmt.Sprintf("%f", extraParams.PQCodeBudgetGBRatio)
		searchCacheBudgetGBRatio = fmt.Sprintf("%f", extraParams.SearchCacheBudgetGBRatio)
		// AutoIndex ExtraParams are operator-managed; always persist them.
		hasSearchCacheBudgetGBRatio = true
		buildNumThreadsRatio = fmt.Sprintf("%f", extraParams.BuildNumThreadsRatio)
		// For AISAQ, disk PQ is expected (default ratio is 0.25).
		// Only write it when the ratio is non-zero; zero means no disk PQ.
		if extraParams.DiskPQCodeBudgetGBRatio != 0 {
			diskPQCodeBudgetGBRatio = fmt.Sprintf("%f", extraParams.DiskPQCodeBudgetGBRatio)
		}
		pqCacheSize, ok = indexParams[PQCacheSizeKey]
		if !ok {
			pqCacheSize = params.CommonCfg.AiSAQCfg.PQCacheSize.GetValue()
		}
		// Read optional AISAQ-specific params from AutoIndex config if present.
		aisVectorsBeamWidth = indexParams[VectorsBeamWidthKey]
		inlinePQ = indexParams[InlinePQKey]
		pqReadPageCacheSize = indexParams[PQReadPageCacheSizeKey]
		rearrange = indexParams[RearrangeKey]
		numEntryPoints = indexParams[NumEntryPointsKey]
	} else {
		var ok bool
		diskPQCodeBudgetGBRatio, ok = indexParams[DiskPQCodeBudgetRatioKey]
		if !ok {
			diskPQCodeBudgetGBRatio = params.CommonCfg.AiSAQCfg.DiskPQCodeBudgetGBRatio.GetValue()
		}
		maxDegree, ok = indexParams[MaxDegreeKey]
		if !ok {
			maxDegree = params.CommonCfg.AiSAQCfg.MaxDegree.GetValue()
		}
		searchListSize, ok = indexParams[SearchListSizeKey]
		if !ok {
			searchListSize = params.CommonCfg.AiSAQCfg.SearchListSize.GetValue()
		}
		pqCodeBudgetGBRatio, ok = indexParams[PQCodeBudgetRatioKey]
		if !ok {
			pqCodeBudgetGBRatio = params.CommonCfg.AiSAQCfg.PQCodeBudgetGBRatio.GetValue()
		}
		searchCacheBudgetGBRatio, ok = indexParams[SearchCacheBudgetRatioKey]
		if !ok {
			searchCacheBudgetGBRatio = params.CommonCfg.AiSAQCfg.SearchCacheBudgetGBRatio.GetValue()
		} else {
			// User explicitly provided the ratio in CreateIndex request — persist it.
			hasSearchCacheBudgetGBRatio = true
		}
		buildNumThreadsRatio, ok = indexParams[NumBuildThreadRatioKey]
		if !ok {
			buildNumThreadsRatio = params.CommonCfg.BuildNumThreadsRatio.GetValue()
		}
		aisVectorsBeamWidth, ok = indexParams[VectorsBeamWidthKey]
		if !ok {
			aisVectorsBeamWidth = "1"
		}
		inlinePQ, ok = indexParams[InlinePQKey]
		if !ok {
			inlinePQ = params.CommonCfg.AiSAQCfg.InlinePQ.GetValue()
		}
		rearrange, ok = indexParams[RearrangeKey]
		if !ok {
			rearrange = params.CommonCfg.AiSAQCfg.Rearrange.GetValue()
		}
		numEntryPoints, ok = indexParams[NumEntryPointsKey]
		if !ok {
			numEntryPoints = params.CommonCfg.AiSAQCfg.NumEntryPoints.GetValue()
		}
		pqCacheSize, ok = indexParams[PQCacheSizeKey]
		if !ok {
			pqCacheSize = params.CommonCfg.AiSAQCfg.PQCacheSize.GetValue()
		}
		pqReadPageCacheSize, ok = indexParams[PQReadPageCacheSizeKey]
		if !ok {
			pqReadPageCacheSize = params.CommonCfg.AiSAQCfg.PQReadPageCacheSize.GetValue()
		}
	}

	pqCacheSizeInt, err := strconv.Atoi(pqCacheSize)
	if err != nil {
		return merr.WrapErrServiceInternalMsg("Error converting pqCacheSize string to int")
	}
	pqCacheSizeBytes = strconv.Itoa(pqCacheSizeInt)
	indexParams[MaxDegreeKey] = maxDegree
	indexParams[SearchListSizeKey] = searchListSize
	indexParams[PQCodeBudgetRatioKey] = pqCodeBudgetGBRatio
	// Only persist SearchCacheBudgetRatioKey when explicitly set by user or AutoIndex config.
	// When it comes from global config defaults, omit it so SetDiskIndexLoadParams reads
	// the live config value — allowing config changes to propagate without index rebuild.
	if hasSearchCacheBudgetGBRatio {
		indexParams[SearchCacheBudgetRatioKey] = searchCacheBudgetGBRatio
	}
	indexParams[NumBuildThreadRatioKey] = buildNumThreadsRatio
	indexParams[PQCacheSizeKey] = pqCacheSizeBytes
	// Only persist optional params when they have non-empty values.
	// Writing empty strings causes ParseFloat/ParseInt("") errors downstream
	// and prevents Knowhere from applying its built-in defaults.
	if diskPQCodeBudgetGBRatio != "" {
		indexParams[DiskPQCodeBudgetRatioKey] = diskPQCodeBudgetGBRatio
	}
	if aisVectorsBeamWidth != "" {
		indexParams[VectorsBeamWidthKey] = aisVectorsBeamWidth
	}
	if inlinePQ != "" {
		indexParams[InlinePQKey] = inlinePQ
	}
	if pqReadPageCacheSize != "" {
		indexParams[PQReadPageCacheSizeKey] = pqReadPageCacheSize
	}
	if rearrange != "" {
		indexParams[RearrangeKey] = rearrange
	}
	if numEntryPoints != "" {
		indexParams[NumEntryPointsKey] = numEntryPoints
	}

	return nil
}

func GetIndexParams(indexParams []*commonpb.KeyValuePair, key string) string {
	for _, param := range indexParams {
		if param.Key == key {
			return param.Value
		}
	}
	return ""
}

// UpdateDiskIndexBuildParams update index params for `buildIndex` (override search cache size in `CreateIndex`)
func UpdateDiskIndexBuildParams(params *paramtable.ComponentParam, indexParams []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	existedVal := GetIndexParams(indexParams, SearchCacheBudgetRatioKey)
	indexType := GetIndexParams(indexParams, common.IndexTypeKey)

	var searchCacheBudgetGBRatio string
	var configuredSearchCacheBudgetGBRatio string

	if params.AutoIndexConfig.Enable.GetAsBool() {
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			// AutoIndexConfig is server-side configuration, not request input.
			return indexParams, merr.WrapErrServiceInternalMsg("index param search_cache_budget_gb_ratio not exist in AutoIndex Config")
		}
		searchCacheBudgetGBRatio = fmt.Sprintf("%f", extraParams.SearchCacheBudgetGBRatio)
	} else if len(existedVal) == 0 {
		if vecindex.IsDiskANN(indexType) {
			configuredSearchCacheBudgetGBRatio = params.CommonCfg.SearchCacheBudgetGBRatio.GetValue()
		}
		if vecindex.IsAISAQ(indexType) {
			configuredSearchCacheBudgetGBRatio = params.CommonCfg.AiSAQCfg.SearchCacheBudgetGBRatio.GetValue()
		}
		paramVal, err := strconv.ParseFloat(configuredSearchCacheBudgetGBRatio, 64)
		if err != nil {
			return indexParams, merr.WrapErrServiceInternalMsg("index param search_cache_budget_gb_ratio not exist in Config")
		}
		searchCacheBudgetGBRatio = fmt.Sprintf("%f", paramVal)
	} else {
		if vecindex.IsDiskANN(indexType) ||
			vecindex.IsAISAQ(indexType) {
			paramVal, err := strconv.ParseFloat(existedVal, 64)
			if err != nil {
				return indexParams, merr.WrapErrServiceInternalMsg("index param search_cache_budget_gb_ratio not exist in existedVal")
			}
			searchCacheBudgetGBRatio = fmt.Sprintf("%f", paramVal)
		}
	}

	// append when not exist
	if len(existedVal) == 0 {
		indexParams = append(indexParams,
			&commonpb.KeyValuePair{
				Key:   SearchCacheBudgetRatioKey,
				Value: searchCacheBudgetGBRatio,
			})
		return indexParams, nil
	}
	// override when exist
	updatedParams := make([]*commonpb.KeyValuePair, 0, len(indexParams))
	for _, param := range indexParams {
		if param.Key == SearchCacheBudgetRatioKey {
			updatedParams = append(updatedParams,
				&commonpb.KeyValuePair{
					Key:   SearchCacheBudgetRatioKey,
					Value: searchCacheBudgetGBRatio,
				})
		} else {
			updatedParams = append(updatedParams,
				&commonpb.KeyValuePair{
					Key:   param.Key,
					Value: param.Value,
				})
		}
	}
	return updatedParams, nil
}

// SetDiskIndexBuildParams set index build params with ratio params on indexNode
// IndexNode cal build param with ratio params and cpu count, memory count...
func SetDiskIndexBuildParams(indexParams map[string]string, fieldDataSize int64, dataType schemapb.DataType) error {
	pqCodeBudgetGBRatioStr, ok := indexParams[PQCodeBudgetRatioKey]
	if !ok {
		return merr.WrapErrParameterInvalidMsg("index param pqCodeBudgetGBRatio not exist")
	}
	pqCodeBudgetGBRatio, err := strconv.ParseFloat(pqCodeBudgetGBRatioStr, 64)
	if err != nil {
		return err
	}
	buildNumThreadsRatioStr, ok := indexParams[NumBuildThreadRatioKey]
	if !ok {
		return merr.WrapErrParameterInvalidMsg("index param buildNumThreadsRatio not exist")
	}
	buildNumThreadsRatio, err := strconv.ParseFloat(buildNumThreadsRatioStr, 64)
	if err != nil {
		return err
	}

	searchCacheBudgetGBRatioStr, ok := indexParams[SearchCacheBudgetRatioKey]
	// set generate cache size when cache ratio param not set
	if ok {
		SearchCacheBudgetGBRatio, err := strconv.ParseFloat(searchCacheBudgetGBRatioStr, 64)
		if err != nil {
			return err
		}
		indexParams[SearchCacheBudgetKey] = fmt.Sprintf("%f", float32(fieldDataSize)*float32(SearchCacheBudgetGBRatio)/(1<<30))
	}
	indexParams[PQCodeBudgetKey] = fmt.Sprintf("%f", float32(fieldDataSize)*float32(pqCodeBudgetGBRatio)/(1<<30))
	indexParams[NumBuildThreadKey] = strconv.Itoa(int(float32(hardware.GetCPUNum()) * float32(buildNumThreadsRatio)))
	indexParams[BuildDramBudgetKey] = fmt.Sprintf("%f", float32(hardware.GetFreeMemoryCount())/(1<<30))
	indexType, ok := indexParams[common.IndexTypeKey]
	if !ok {
		return merr.WrapErrServiceInternalMsg("type param indexType not exist")
	}

	if vecindex.IsDiskANN(indexType) ||
		vecindex.IsAISAQ(indexType) {
		diskPQCodeBudgetGBRatioStr, ok := indexParams[DiskPQCodeBudgetRatioKey]
		var diskPQCodeBudgetGBRatio float64
		if !ok || diskPQCodeBudgetGBRatioStr == "" {
			// Key absent or empty means "no disk PQ" → disk_pq_dims=0
			// (Knowhere stores uncompressed vectors on SSD).
			diskPQCodeBudgetGBRatio = 0
		} else {
			diskPQCodeBudgetGBRatio, err = strconv.ParseFloat(diskPQCodeBudgetGBRatioStr, 64)
			if err != nil {
				return err
			}
		}
		dimStr, ok := indexParams[common.DimKey]
		if !ok {
			return merr.WrapErrServiceInternalMsg("type param dim not exist")
		}
		dim, err := strconv.ParseInt(dimStr, 10, 64)
		if err != nil {
			return err
		}
		diskPQDims, err := getDiskPQDims(diskPQCodeBudgetGBRatio, dim, dataType)
		if err != nil {
			return err
		}
		indexParams[DiskPQDimsKey] = strconv.Itoa(diskPQDims)
	}
	return nil
}

func SetBitmapIndexLoadParams(params *paramtable.ComponentParam, indexParams map[string]string) {
	_, exist := indexParams[common.IndexOffsetCacheEnabledKey]
	if exist {
		return
	}
	indexParams[common.IndexOffsetCacheEnabledKey] = params.QueryNodeCfg.IndexOffsetCacheEnabled.GetValue()
}

// SetDiskIndexLoadParams set disk index load params with ratio params on queryNode
// QueryNode cal load params with ratio params ans cpu count...
func SetDiskIndexLoadParams(params *paramtable.ComponentParam, indexParams map[string]string, numRows int64) error {
	dimStr, ok := indexParams[common.DimKey]
	if !ok {
		// type param dim has been put into index params before build index
		return merr.WrapErrParameterInvalidMsg("type param dim not exist")
	}
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return err
	}

	indexType, ok := indexParams[common.IndexTypeKey]
	if !ok {
		return merr.WrapErrServiceInternalMsg("type param indexType not exist")
	}
	var searchCacheBudgetGBRatio float64
	var loadNumThreadRatio float64
	var beamWidthRatio float64

	// Prefer values already persisted in indexParams (written during FillDiskIndexParams /
	// FillAiSAQIndexParams / UpdateDiskIndexBuildParams). Fall back to global config only
	// when the key is absent.
	if v, ok := indexParams[SearchCacheBudgetRatioKey]; ok && v != "" {
		searchCacheBudgetGBRatio, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
	} else if params.AutoIndexConfig.Enable.GetAsBool() {
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		searchCacheBudgetGBRatio = extraParams.SearchCacheBudgetGBRatio
	} else {
		var configuredSearchCacheBudgetGBRatio string
		if vecindex.IsDiskANN(indexType) {
			configuredSearchCacheBudgetGBRatio = params.CommonCfg.SearchCacheBudgetGBRatio.GetValue()
		}
		if vecindex.IsAISAQ(indexType) {
			configuredSearchCacheBudgetGBRatio = params.CommonCfg.AiSAQCfg.SearchCacheBudgetGBRatio.GetValue()
		}
		searchCacheBudgetGBRatio, err = strconv.ParseFloat(configuredSearchCacheBudgetGBRatio, 64)
		if err != nil {
			return err
		}
	}

	if v, ok := indexParams[NumLoadThreadRatioKey]; ok && v != "" {
		loadNumThreadRatio, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
	} else if params.AutoIndexConfig.Enable.GetAsBool() {
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		loadNumThreadRatio = extraParams.LoadNumThreadRatio
	} else {
		loadNumThreadRatio, err = strconv.ParseFloat(params.CommonCfg.LoadNumThreadRatio.GetValue(), 64)
		if err != nil {
			return err
		}
	}

	if v, ok := indexParams[BeamWidthRatioKey]; ok && v != "" {
		beamWidthRatio, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return err
		}
	} else if params.AutoIndexConfig.Enable.GetAsBool() {
		extraParams, err := NewBigDataExtraParamsFromJSON(indexType, params.AutoIndexConfig.ExtraParams.GetValue())
		if err != nil {
			return err
		}
		beamWidthRatio = extraParams.BeamWidthRatio
	} else {
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

func AppendPrepareLoadParams(params *paramtable.ComponentParam, indexParams map[string]string) error {
	if params.AutoIndexConfig.Enable.GetAsBool() { // `enable` only for cloud instance.
		// override prepare params by
		for k, v := range params.AutoIndexConfig.PrepareParams.GetAsJSONMap() {
			indexParams[k] = v
		}

		for k, v := range params.AutoIndexConfig.LoadAdaptParams.GetAsJSONMap() {
			indexParams[k] = v
		}
	}

	params.KnowhereConfig.MergeIndexParams(indexParams[common.IndexTypeKey], paramtable.LoadStage, indexParams)

	return nil
}
