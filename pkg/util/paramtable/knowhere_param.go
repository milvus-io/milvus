package paramtable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type knowhereConfig struct {
	Enable     ParamItem  `refreshable:"true"`
	IndexParam ParamGroup `refreshable:"true"`
}

const (
	BuildStage  = "build"
	LoadStage   = "load"
	SearchStage = "search"

	OverrideIndexTypeKey = "override_index_type"
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
			case "AISAQ.build.max_degree":
				return "Maximum degree of the Vamana graph"
			case "AISAQ.build.pq_code_budget_gb_ratio":
				return "Size limit on the PQ code (compared with raw data)"
			case "AISAQ.build.search_list_size":
				return "Size of the candidate list during building graph"
			case "AISAQ.build.disk_pq_code_budget_gb_ratio":
				return "Controls the size of the PQ codes of the high precision vectors stored in the index (used for re-ranking), compared to the size of the uncompressed data"
			case "AISAQ.build.inline_pq":
				return "Enable compressed vectors to be stored in-line within the node, the number of in-line vectors is limited by max degree"
			case "AISAQ.build.pq_cache_size":
				return "Compressed vectors cache DRAM size in bytes, default 0"
			case "AISAQ.build.rearrange":
				return "Enable compressed vectors reordering search optimization, default false"
			case "AISAQ.build.num_entry_points":
				return "Number of entry points valid only with aisaq option"
			case "AISAQ.build.search_cache_budget_gb_ratio":
				return "Controls the amount of DRAM to be used for caching frequently accessed index nodes"
			case "AISAQ.search.pq_read_page_cache_size":
				return "Enable compressed vectors read-page cache DRAM size per thread, default 0"
			case "AISAQ.search.beam_width_ratio":
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

// GetIndexParamValue returns the value of a single knowhere index parameter for
// the given index type and stage (e.g. "AISAQ", "build", "max_degree").
// It returns an empty string when the parameter is not configured.
func (p *knowhereConfig) GetIndexParamValue(indexType string, stage string, key string) string {
	return p.getIndexParam(indexType, stage)[key]
}

// GetIndexParamKey returns the fully-qualified configuration key for a single
// knowhere index parameter (e.g. "knowhere.AISAQ.build.pq_cache_size"). Use this
// when you need the config key itself, for example to Save a value via BaseTable.
func (p *knowhereConfig) GetIndexParamKey(indexType string, stage string, key string) string {
	return p.IndexParam.KeyPrefix + indexType + "." + stage + "." + key
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

	overrideIndexType := GetKeyFromSlice(indexParams, OverrideIndexTypeKey)
	if overrideIndexType != "" {
		overrideIndexParams := p.getIndexParam(overrideIndexType, stage)
		mlog.Info(context.TODO(), "override index params", mlog.String("overrideIndexType", overrideIndexType), mlog.Any("overrideIndexParams", overrideIndexParams))
		for key, val := range overrideIndexParams {
			indexParams = append(indexParams,
				&commonpb.KeyValuePair{
					Key:   key,
					Value: val,
				})
		}

		// Replace the original index_type with override_index_type
		for i, param := range indexParams {
			if param.Key == common.IndexTypeKey {
				indexParams[i].Value = overrideIndexType
				break
			}
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

func (p *knowhereConfig) HasIndexParams(indexType, stage string) bool {
	return len(p.getIndexParam(indexType, stage)) > 0
}

func (p *knowhereConfig) MergeIndexParamsJSON(indexType, stage string, params map[string]any) error {
	defaultParams := p.getIndexParam(indexType, stage)
	if len(defaultParams) == 0 {
		return nil
	}

	rawParams := params[common.SearchParamKey].(string)
	if rawParams == "" {
		rawParams = "{}"
	}

	searchParams := make(map[string]json.RawMessage)
	if err := json.Unmarshal([]byte(rawParams), &searchParams); err != nil {
		return err
	}
	if searchParams == nil {
		return merr.WrapErrParameterInvalidMsg("search params must be a JSON object")
	}
	for key, value := range defaultParams {
		if _, exists := searchParams[key]; exists {
			continue
		}
		rawValue := json.RawMessage(value)
		if !json.Valid(rawValue) {
			rawValue, _ = json.Marshal(value)
		}
		searchParams[key] = rawValue
	}

	merged, err := json.Marshal(searchParams)
	if err != nil {
		return err
	}
	params[common.SearchParamKey] = string(merged)
	return nil
}

func (p *knowhereConfig) MergeResourceParams(vecFieldSize uint64, stage string, indexParam map[string]string) (map[string]string, error) {
	param, _ := p.GetRuntimeParameter(stage)

	for key, val := range param {
		indexParam[key] = val
	}

	indexParam[VecFieldSizeKey] = fmt.Sprintf("%f", float32(vecFieldSize)/(1<<30))

	return indexParam, nil
}
