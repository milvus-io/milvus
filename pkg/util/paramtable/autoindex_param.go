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

package paramtable

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// /////////////////////////////////////////////////////////////////////////////
// --- common ---
type AutoIndexConfig struct {
	Enable                 ParamItem `refreshable:"true"`
	EnableOptimize         ParamItem `refreshable:"true"`
	EnableResultLimitCheck ParamItem `refreshable:"true"`

	IndexParams           ParamItem  `refreshable:"true"`
	SparseIndexParams     ParamItem  `refreshable:"true"`
	BinaryIndexParams     ParamItem  `refreshable:"true"`
	PrepareParams         ParamItem  `refreshable:"true"`
	LoadAdaptParams       ParamItem  `refreshable:"true"`
	ExtraParams           ParamItem  `refreshable:"true"`
	IndexType             ParamItem  `refreshable:"true"`
	AutoIndexTypeName     ParamItem  `refreshable:"true"`
	AutoIndexSearchConfig ParamItem  `refreshable:"true"`
	AutoIndexTuningConfig ParamGroup `refreshable:"true"`

	ScalarAutoIndexEnable  ParamItem `refreshable:"true"`
	ScalarAutoIndexParams  ParamItem `refreshable:"true"`
	ScalarNumericIndexType ParamItem `refreshable:"true"`
	ScalarIntIndexType     ParamItem `refreshable:"true"`
	ScalarVarcharIndexType ParamItem `refreshable:"true"`
	ScalarBoolIndexType    ParamItem `refreshable:"true"`
	ScalarFloatIndexType   ParamItem `refreshable:"true"`
	ScalarJSONIndexType    ParamItem `refreshable:"true"`

	BitmapCardinalityLimit ParamItem `refreshable:"true"`
}

const (
	DefaultBitmapCardinalityLimit = 100
)

func (p *AutoIndexConfig) init(base *BaseTable) {
	p.Enable = ParamItem{
		Key:          "autoIndex.enable",
		Version:      "2.2.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.Enable.Init(base.mgr)

	p.EnableOptimize = ParamItem{
		Key:          "autoIndex.optimize",
		Version:      "2.4.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
	}
	p.EnableOptimize.Init(base.mgr)

	p.EnableResultLimitCheck = ParamItem{
		Key:          "autoIndex.resultLimitCheck",
		Version:      "2.5.0",
		DefaultValue: "true",
		PanicIfEmpty: true,
	}
	p.EnableResultLimitCheck.Init(base.mgr)

	p.IndexParams = GetIndexParam()
	p.IndexParams.Init(base.mgr)

	p.SparseIndexParams = ParamItem{
		Key:          "autoIndex.params.sparse.build",
		Version:      "2.4.5",
		DefaultValue: `{"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"}`,
		Formatter:    GetBuildParamFormatter(SparseFloatVectorDefaultMetricType, "autoIndex.params.sparse.build"),
		Export:       true,
	}
	p.SparseIndexParams.Init(base.mgr)

	p.BinaryIndexParams = ParamItem{
		Key:          "autoIndex.params.binary.build",
		Version:      "2.4.5",
		DefaultValue: `{"nlist": 1024, "index_type": "BIN_IVF_FLAT", "metric_type": "HAMMING"}`,
		Formatter:    GetBuildParamFormatter(BinaryVectorDefaultMetricType, "autoIndex.params.sparse.build"),
		Export:       true,
	}
	p.BinaryIndexParams.Init(base.mgr)

	p.PrepareParams = ParamItem{
		Key:     "autoIndex.params.prepare",
		Version: "2.3.2",
	}
	p.PrepareParams.Init(base.mgr)

	p.LoadAdaptParams = ParamItem{
		Key:     "autoIndex.params.load",
		Version: "2.4.5",
	}
	p.LoadAdaptParams.Init(base.mgr)

	p.ExtraParams = ParamItem{
		Key:     "autoIndex.params.extra",
		Version: "2.2.0",
	}
	p.ExtraParams.Init(base.mgr)

	p.IndexType = ParamItem{
		Version: "2.2.0",
		Formatter: func(v string) string {
			m := p.IndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m[common.IndexTypeKey]
		},
	}
	p.IndexType.Init(base.mgr)

	p.AutoIndexTypeName = ParamItem{
		Key:     "autoIndex.type",
		Version: "2.2.0",
	}
	p.AutoIndexTypeName.Init(base.mgr)

	p.AutoIndexSearchConfig = ParamItem{
		Key:     "autoindex.params.search",
		Version: "2.2.0",
	}
	p.AutoIndexSearchConfig.Init(base.mgr)

	p.AutoIndexTuningConfig = ParamGroup{
		KeyPrefix: "autoindex.params.tuning.",
		Version:   "2.3.0",
	}
	p.AutoIndexTuningConfig.Init(base.mgr)

	p.ScalarAutoIndexEnable = ParamItem{
		Key:          "scalarAutoIndex.enable",
		Version:      "2.4.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.ScalarAutoIndexEnable.Init(base.mgr)

	p.ScalarAutoIndexParams = ParamItem{
		Key:          "scalarAutoIndex.params.build",
		Version:      "2.4.0",
		DefaultValue: `{"int": "HYBRID","varchar": "HYBRID","bool": "BITMAP", "float": "INVERTED", "json": "INVERTED"}`,
	}
	p.ScalarAutoIndexParams.Init(base.mgr)

	// Deprecated param
	p.ScalarNumericIndexType = ParamItem{
		Version: "2.4.0",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["numeric"]
		},
	}
	p.ScalarNumericIndexType.Init(base.mgr)

	p.ScalarIntIndexType = ParamItem{
		Version: "2.5.0",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["int"]
		},
	}
	p.ScalarIntIndexType.Init(base.mgr)

	p.ScalarFloatIndexType = ParamItem{
		Version: "2.5.0",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["float"]
		},
	}
	p.ScalarFloatIndexType.Init(base.mgr)

	p.ScalarJSONIndexType = ParamItem{
		Version: "2.5.12",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["json"]
		},
	}
	p.ScalarJSONIndexType.Init(base.mgr)

	p.BitmapCardinalityLimit = ParamItem{
		Key:          "scalarAutoIndex.params.bitmapCardinalityLimit",
		Version:      "2.5.0",
		DefaultValue: strconv.Itoa(DefaultBitmapCardinalityLimit),
		Export:       true,
	}
	p.BitmapCardinalityLimit.Init(base.mgr)

	p.ScalarVarcharIndexType = ParamItem{
		Version: "2.4.0",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["varchar"]
		},
	}
	p.ScalarVarcharIndexType.Init(base.mgr)

	p.ScalarBoolIndexType = ParamItem{
		Version: "2.4.0",
		Formatter: func(v string) string {
			m := p.ScalarAutoIndexParams.GetAsJSONMap()
			if m == nil {
				return ""
			}
			return m["bool"]
		},
	}
	p.ScalarBoolIndexType.Init(base.mgr)
}

func setDefaultIfNotExist(params map[string]string, key string, defaultValue string) {
	_, exist := params[key]
	if !exist {
		params[key] = defaultValue
	}
}

const (
	FloatVectorDefaultMetricType       = metric.COSINE
	SparseFloatVectorDefaultMetricType = metric.IP
	BinaryVectorDefaultMetricType      = metric.HAMMING
	IntVectorDefaultMetricType         = metric.COSINE
)

func SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	if typeutil.IsDenseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, FloatVectorDefaultMetricType)
	} else if typeutil.IsSparseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, SparseFloatVectorDefaultMetricType)
	} else if typeutil.IsBinaryVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, BinaryVectorDefaultMetricType)
	} else if typeutil.IsIntVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, IntVectorDefaultMetricType)
	}
}

func GetBuildParamFormatter(defaultMetricsType metric.MetricType, tag string) func(string) string {
	return func(originValue string) string {
		m, err := funcutil.JSONToMap(originValue)
		if err != nil {
			panic(errors.Wrapf(err, "failed to parse %s config value", tag))
		}
		_, ok := m[common.MetricTypeKey]
		if ok {
			return originValue
		}
		m[common.MetricTypeKey] = defaultMetricsType
		ret, err := funcutil.MapToJSON(m)
		if err != nil {
			panic(errors.Wrapf(err, "failed to convert updated %s map to json", tag))
		}
		return ret
	}
}
