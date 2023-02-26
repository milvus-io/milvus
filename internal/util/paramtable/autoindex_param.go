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
	"github.com/milvus-io/milvus/internal/common"
)

// /////////////////////////////////////////////////////////////////////////////
// --- common ---
type autoIndexConfig struct {
	Enable ParamItem `refreshable:"true"`

	IndexParams         ParamItem `refreshable:"true"`
	ExtraParams         ParamItem `refreshable:"true"`
	SearchParamsYamlStr ParamItem `refreshable:"true"`
	IndexType           ParamItem `refreshable:"true"`
	AutoIndexTypeName   ParamItem `refreshable:"true"`
}

func (p *autoIndexConfig) init(base *BaseTable) {
	p.Enable = ParamItem{
		Key:          "autoIndex.enable",
		Version:      "2.2.0",
		DefaultValue: "false",
		PanicIfEmpty: true,
	}
	p.Enable.Init(base.mgr)

	p.IndexParams = ParamItem{
		Key:     "autoIndex.params.build",
		Version: "2.2.0",
	}
	p.IndexParams.Init(base.mgr)

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

	p.SearchParamsYamlStr = ParamItem{
		Key:     "autoIndex.params.search",
		Version: "2.2.0",
	}
	p.SearchParamsYamlStr.Init(base.mgr)

	p.AutoIndexTypeName = ParamItem{
		Key:     "autoIndex.type",
		Version: "2.2.0",
	}
	p.AutoIndexTypeName.Init(base.mgr)
}
