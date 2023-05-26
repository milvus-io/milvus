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
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
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

func Test_autoIndexConfig_panicIfNotInvalid(t *testing.T) {
	t.Run("unsupported index type", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: "unsupported",
			},
		}
		assert.Panics(t, p.panicIfNotInvalid)
	})

	t.Run("efConstruction not found", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: "HNSW",
			},
		}
		assert.Panics(t, p.panicIfNotInvalid)
	})

	t.Run("normal case, hnsw", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: indexparamcheck.IndexHNSW,
				"M":                 "30",
				"efConstruction":    "360",
			},
		}
		assert.NotPanics(t, p.panicIfNotInvalid)
		metricType, exist := p.IndexParams[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, indexparamcheck.FloatVectorDefaultMetricType, metricType)
	})

	t.Run("normal case, ivf flat", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: indexparamcheck.IndexFaissIvfFlat,
				"nlist":             "30",
			},
		}
		assert.NotPanics(t, p.panicIfNotInvalid)
		metricType, exist := p.IndexParams[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, indexparamcheck.FloatVectorDefaultMetricType, metricType)
	})

	t.Run("normal case, diskann", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: indexparamcheck.IndexDISKANN,
			},
		}
		assert.NotPanics(t, p.panicIfNotInvalid)
		metricType, exist := p.IndexParams[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, indexparamcheck.FloatVectorDefaultMetricType, metricType)
	})

	t.Run("normal case, bin flat", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: indexparamcheck.IndexFaissBinIDMap,
			},
		}
		assert.NotPanics(t, p.panicIfNotInvalid)
		metricType, exist := p.IndexParams[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, indexparamcheck.BinaryVectorDefaultMetricType, metricType)
	})

	t.Run("normal case, bin flat", func(t *testing.T) {
		p := &autoIndexConfig{
			Enable: false,
			IndexParams: map[string]string{
				common.IndexTypeKey: indexparamcheck.IndexFaissBinIvfFlat,
				"nlist":             "30",
			},
		}
		assert.NotPanics(t, p.panicIfNotInvalid)
		metricType, exist := p.IndexParams[common.MetricTypeKey]
		assert.True(t, exist)
		assert.Equal(t, indexparamcheck.BinaryVectorDefaultMetricType, metricType)
	})
}
