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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

///////////////////////////////////////////////////////////////////////////////
// --- common ---
type autoIndexConfig struct {
	Base *BaseTable

	Enable bool

	indexParamsStr string
	IndexParams    map[string]string

	searchParamsStr   string
	LevelSearchParams map[int]string

	IndexType         string
	AutoIndexTypeName string
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
		return
	}
	p.indexParamsStr = p.Base.LoadWithDefault("autoIndex.params.build", "")
	p.parseBuildParams(p.indexParamsStr)

	p.searchParamsStr = p.Base.LoadWithDefault("autoIndex.params.search", "")
	p.parseSearchParams(p.searchParamsStr)
	p.AutoIndexTypeName = p.Base.LoadWithDefault("autoIndex.type", "")
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

func (p *autoIndexConfig) parseSearchParams(paramsStr string) {
	var err error
	buffer := make(map[string]interface{})
	err = json.Unmarshal([]byte(paramsStr), &buffer)
	if err != nil {
		err2 := fmt.Errorf("parse autoindex search params failed:%w", err)
		panic(err2)
	}

	p.LevelSearchParams = make(map[int]string)
	for key, value := range buffer {
		var l int
		l, err = strconv.Atoi(key)
		if err != nil {
			err2 := fmt.Errorf("parse autoindex search params level is not valid:%w", err)
			panic(err2)
		}
		mapValue, ok := value.(map[string]interface{})
		if !ok {
			err2 := fmt.Errorf("parse autoindex search params value is not map[string]interface:%w", err)
			panic(err2)
		}
		data, err2 := json.Marshal(mapValue)
		if err2 != nil {
			err3 := fmt.Errorf("parse autoindex search params value marshal failed:%w", err2)
			panic(err3)
		}
		p.LevelSearchParams[l] = string(data)
	}
	if len(p.LevelSearchParams) == 0 {
		err2 := fmt.Errorf("parse autoindex search params no level is specified")
		panic(err2)
	}
}

// GetSearchParamsByLevel return a json format string
func (p *autoIndexConfig) GetSearchParamsByLevel(level int) string {
	if !p.Enable || len(p.LevelSearchParams) == 0 {
		return "{}"
	}
	var levels []int
	for l := range p.LevelSearchParams {
		levels = append(levels, l)
	}
	sort.Slice(levels, func(i int, j int) bool {
		return levels[i] < levels[j]
	})
	index := 0
	// levels is sure to be non-empty
	for i, l := range levels {
		if l > level {
			break
		}
		index = i
	}
	return p.LevelSearchParams[levels[index]]
}
