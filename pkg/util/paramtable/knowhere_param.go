package paramtable

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
)

type knowhereConfig struct {
	Enable     ParamItem  `refreshable:"true"`
	IndexParam ParamGroup `refreshable:"true"`
}

const (
	BuildStage  = "build"
	LoadStage   = "load"
	SearchStage = "search"
)

const (
	BuildDramBudgetKey = "build_dram_budget_gb"
	VecFieldSizeKey    = "vec_field_size_gb"
)

func (p *knowhereConfig) init(base *BaseTable) {
	p.IndexParam = ParamGroup{
		KeyPrefix: "knowhere.",
		Version:   "2.5.0",
		Export:    true,
		DocFunc: func(key string) string {
			switch key {
			case "DISKANN.build.max_degree":
				return "Maximum degree of the Vamana graph"
			case "DISKANN.build.pq_code_budget_gb_ratio":
				return "Size limit on the PQ code (compared with raw data)"
			case "DISKANN.build.search_cache_budget_gb_ratio":
				return "Ratio of cached node numbers to raw data"
			case "DISKANN.build.search_list_size":
				return "Size of the candidate list during building graph"
			case "DISKANN.search.beam_width_ratio":
				return "Ratio between the maximum number of IO requests per search iteration and CPU number"
			default:
				return ""
			}
		},
	}
	p.IndexParam.Init(base.mgr)

	p.Enable = ParamItem{
		Key:          "knowhere.enable",
		Version:      "2.5.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "When enable this configuration, the index parameters defined following will be automatically populated as index parameters, without requiring user input.",
	}
	p.Enable.Init(base.mgr)
}

func (p *knowhereConfig) getIndexParam(indexType string, stage string) map[string]string {
	matchedParam := make(map[string]string)

	params := p.IndexParam.GetValue()
	prefix := indexType + "." + stage + "."

	for k, v := range params {
		if strings.HasPrefix(k, prefix) {
			matchedParam[strings.TrimPrefix(k, prefix)] = v
		}
	}

	return matchedParam
}

func GetKeyFromSlice(indexParams []*commonpb.KeyValuePair, key string) string {
	for _, param := range indexParams {
		if param.Key == key {
			return param.Value
		}
	}
	return ""
}

func (p *knowhereConfig) GetRuntimeParameter(stage string) (map[string]string, error) {
	params := make(map[string]string)

	if stage == BuildStage {
		params[BuildDramBudgetKey] = fmt.Sprintf("%f", float32(hardware.GetFreeMemoryCount())/(1<<30))
	}

	return params, nil
}

func (p *knowhereConfig) UpdateIndexParams(indexType string, stage string, indexParams []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	defaultParams := p.getIndexParam(indexType, stage)

	for key, val := range defaultParams {
		if GetKeyFromSlice(indexParams, key) == "" {
			indexParams = append(indexParams,
				&commonpb.KeyValuePair{
					Key:   key,
					Value: val,
				})
		}
	}

	return indexParams, nil
}

func (p *knowhereConfig) MergeIndexParams(indexType string, stage string, indexParam map[string]string) (map[string]string, error) {
	defaultParams := p.getIndexParam(indexType, stage)

	for key, val := range defaultParams {
		_, existed := indexParam[key]
		if !existed {
			indexParam[key] = val
		}
	}

	return indexParam, nil
}

func (p *knowhereConfig) MergeResourceParams(vecFieldSize uint64, stage string, indexParam map[string]string) (map[string]string, error) {
	param, _ := p.GetRuntimeParameter(stage)

	for key, val := range param {
		indexParam[key] = val
	}

	indexParam[VecFieldSizeKey] = fmt.Sprintf("%f", float32(vecFieldSize)/(1<<30))

	return indexParam, nil
}
