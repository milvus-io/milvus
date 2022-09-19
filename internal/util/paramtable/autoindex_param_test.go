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
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/autoindex"
	"github.com/stretchr/testify/assert"
)

const (
	MetricTypeKey = common.MetricTypeKey
	IndexTypeKey  = common.IndexTypeKey
)

func TestAutoIndexParams_build(t *testing.T) {
	var CParams ComponentParam
	CParams.Init()

	t.Run("test parseBuildParams success", func(t *testing.T) {
		//Params := CParams.AutoIndexConfig
		//buildParams := make([string]interface)
		var err error
		map1 := map[string]interface{}{
			IndexTypeKey:     "HNSW",
			"M":              48,
			"efConstruction": 500,
		}
		var jsonStrBytes []byte
		jsonStrBytes, err = json.Marshal(map1)
		assert.NoError(t, err)
		CParams.AutoIndexConfig.parseBuildParams(string(jsonStrBytes))
		assert.Equal(t, "HNSW", CParams.AutoIndexConfig.IndexType)
		assert.Equal(t, strconv.Itoa(map1["M"].(int)), CParams.AutoIndexConfig.IndexParams["M"])
		assert.Equal(t, strconv.Itoa(map1["efConstruction"].(int)), CParams.AutoIndexConfig.IndexParams["efConstruction"])

		map2 := map[string]interface{}{
			IndexTypeKey: "IVF_FLAT",
			"nlist":      1024,
		}
		jsonStrBytes, err = json.Marshal(map2)
		assert.NoError(t, err)
		CParams.AutoIndexConfig.parseBuildParams(string(jsonStrBytes))
		assert.Equal(t, "IVF_FLAT", CParams.AutoIndexConfig.IndexType)
		assert.Equal(t, strconv.Itoa(map2["nlist"].(int)), CParams.AutoIndexConfig.IndexParams["nlist"])
	})

	t.Run("test parseBuildParams miss total", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()
		CParams.AutoIndexConfig.parseBuildParams("")
	})

	t.Run("test parseBuildParams miss index_type", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()
		var err error
		map1 := map[string]interface{}{
			"M":              48,
			"efConstruction": 500,
		}
		var jsonStrBytes []byte
		jsonStrBytes, err = json.Marshal(map1)
		assert.NoError(t, err)
		CParams.AutoIndexConfig.parseBuildParams(string(jsonStrBytes))
	})
}

func TestAutoIndexParams_search1(t *testing.T) {
	var CParams ComponentParam
	CParams.Init()
	CParams.AutoIndexConfig.Enable = true

	var err error
	indexMap := map[string]interface{}{
		IndexTypeKey:     "HNSW",
		"M":              48,
		"efConstruction": 500,
	}
	var jsonStrBytes []byte
	jsonStrBytes, err = json.Marshal(indexMap)
	assert.NoError(t, err)
	CParams.AutoIndexConfig.parseBuildParams(string(jsonStrBytes))

	jsonStr := `
	{
        "1": {
          "methodID": 1,
          "function": "{\"funcID\": 3, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
        },
        "2": {
          "methodID": 2,
          "bp": [10, 200],
          "functions": [
            "{\"funcID\": 1}",
            "{\"funcID\": 2, \"cof1\": 4,\"cof2\": 5}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        },
        "3": {
          "methodID": 2,
          "bp": [10, 300],
          "functions": [
            "{\"funcID\": 2, \"cof1\": 3,\"cof2\": 4}",
            "{\"funcID\": 1}",
            "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
          ]
        }
	}`

	CParams.AutoIndexConfig.Parser = autoindex.NewParser()
	CParams.AutoIndexConfig.Parser.InitFromJSONStr(jsonStr)
	assert.NoError(t, err)

	smallLevels := []int{-1, 0, 1}
	var current autoindex.Calculator
	for _, l := range smallLevels { // for level < 1 , all same to level 1
		m := CParams.AutoIndexConfig.GetSearchParamStrCalculator(l)
		assert.NotNil(t, m)
		if current == nil {
			current = m
		} else {
			assert.Equal(t, m, current)
		}
	}

	largeLevels := []int{3, 4}
	current = nil
	for _, l := range largeLevels { // for level < 1 , all same to level 1
		m := CParams.AutoIndexConfig.GetSearchParamStrCalculator(l)
		assert.NotNil(t, m)
		if current == nil {
			current = m
		} else {
			assert.Equal(t, m, current)
		}
	}
}
