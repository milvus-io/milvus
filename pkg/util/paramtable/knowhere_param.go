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
		// This open namespace contains optional deployment overrides; algorithm
		// defaults belong to Knowhere and are not exported into the shipped YAML.
		Export: false,
	}
	p.IndexParam.Init(base.mgr)

	p.Enable = ParamItem{
		Key:          "knowhere.enable",
		Version:      "2.5.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "Enable optional deployment overrides in the knowhere.<index>.<stage> namespace. Knowhere always owns algorithm defaults; runtime resource facts are supplied independently of this setting.",
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
	explicit := make(map[string]struct{}, len(indexParams))
	positions := make(map[string]int, len(indexParams))
	copied := make([]*commonpb.KeyValuePair, 0, len(indexParams))
	for _, param := range indexParams {
		explicit[param.GetKey()] = struct{}{}
		positions[param.GetKey()] = len(copied)
		copied = append(copied, &commonpb.KeyValuePair{Key: param.GetKey(), Value: param.GetValue()})
	}
	indexParams = copied

	for key, val := range defaultParams {
		if _, present := explicit[key]; !present {
			positions[key] = len(indexParams)
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
			if _, present := explicit[key]; present {
				continue
			}
			if i, present := positions[key]; present {
				indexParams[i].Value = val
				continue
			}
			positions[key] = len(indexParams)
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
