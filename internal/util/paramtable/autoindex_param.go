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
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/autoindex"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type autoIndexConfig struct {
	Base *BaseTable

	Enable bool

	indexParamsStr string
	IndexParams    map[string]string

	extraParamsStr     string
	BigDataExtraParams *autoindex.BigDataIndexExtraParams

	SearchParamsYamlStr string

	IndexType         string
	AutoIndexTypeName string
	Parser            *autoindex.Parser
}

func (p *autoIndexConfig) init(base *BaseTable) {
	p.Base = base
	p.initEnable() // must call at first
	p.initParams()
}

func (p *autoIndexConfig) initEnable() {
	var err error
	enable := p.Base.LoadWithDefault("autoIndex.enable", "false")
	p.Enable, err = strconv.ParseBool(enable)
	if err != nil {
		panic(err)
	}
}

func (p *autoIndexConfig) initParams() {
	if !p.Enable {
		// init a default ExtraParams
		p.BigDataExtraParams = autoindex.NewBigDataIndexExtraParams()
		return
	}
	p.indexParamsStr = p.Base.LoadWithDefault("autoIndex.params.build", "")
	p.parseBuildParams(p.indexParamsStr)

	p.SearchParamsYamlStr = p.Base.LoadWithDefault("autoIndex.params.search", "")
	p.parseSearchParams(p.SearchParamsYamlStr)
	p.AutoIndexTypeName = p.Base.LoadWithDefault("autoIndex.type", "")
	p.extraParamsStr = p.Base.LoadWithDefault("autoIndex.params.extra", "")
	p.parseExtraParams(p.extraParamsStr)
}

func (p *autoIndexConfig) parseBuildParams(paramsStr string) {
	var err error
	p.IndexParams, err = funcutil.ParseIndexParamsMap(paramsStr)
	if err != nil {
		err2 := fmt.Errorf("parse autoindex build params failed:%w", err)
		panic(err2)
	}
	var ok bool
	p.IndexType, ok = p.IndexParams[common.IndexTypeKey]
	if !ok {
		err2 := fmt.Errorf("parse autoindex %s failed:%w", common.IndexTypeKey, err)
		panic(err2)
	}
}

func (p *autoIndexConfig) parseExtraParams(paramsStr string) {
	var err error
	p.BigDataExtraParams, err = autoindex.NewBigDataExtraParamsFromJSON(paramsStr)
	if err != nil {
		err2 := fmt.Errorf("parse auto index extra params failed:%w", err)
		panic(err2)
	}
}

func (p *autoIndexConfig) parseSearchParams(paramsStr string) {
	p.Parser = autoindex.NewParser()
	err := p.Parser.InitFromJSONStr(paramsStr)
	if err != nil {
		err2 := fmt.Errorf("parse autoindex search params failed:%w", err)
		panic(err2)
	}
}

// GetSearchParamStrCalculator return a method which can calculate searchParams
func (p *autoIndexConfig) GetSearchParamStrCalculator(level int) autoindex.Calculator {
	if !p.Enable {
		return nil
	}
	m, ok := p.Parser.GetMethodByLevel(level)
	if !ok {
		return nil
	}
	return m
}
