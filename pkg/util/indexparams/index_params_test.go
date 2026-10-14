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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestAppendPrepareInfo_parse(t *testing.T) {
	t.Run("parse load info", func(t *testing.T) {
		var params paramtable.ComponentParam
		params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
		params.Save(params.AutoIndexConfig.Enable.Key, "true")
		mapString := make(map[string]string)
		mapString["key1"] = "value1"
		str, err := json.Marshal(mapString)
		assert.NoError(t, err)
		params.Save(params.AutoIndexConfig.PrepareParams.Key, string(str))

		mapString2 := make(map[string]string)
		mapString2["key2"] = "value2"
		str2, err2 := json.Marshal(mapString2)
		assert.NoError(t, err2)
		params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, string(str2))

		resultMapString := make(map[string]string)
		err = AppendPrepareLoadParams(&params, resultMapString)
		assert.NoError(t, err)
		assert.Equal(t, resultMapString["key1"], "value1")
		assert.Equal(t, resultMapString["key2"], "value2")

		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(params.KnowhereConfig.IndexParam.KeyPrefix+"GPU_CAGRA.load.adapt_for_cpu", "true")
		indexParams := map[string]string{
			"index_type":       "GPU_CAGRA",
			"nn_descent_niter": "20",
			"build_algo":       "NN_DESCENT",
		}

		err = AppendPrepareLoadParams(&params, indexParams)
		assert.NoError(t, err)
		assert.Equal(t, indexParams["nn_descent_niter"], "20")
		assert.Equal(t, indexParams["build_algo"], "NN_DESCENT")
		assert.Equal(t, indexParams["adapt_for_cpu"], "true")
	})
}

func TestLoadDeploymentParamsPreserveExplicitValues(t *testing.T) {
	var params paramtable.ComponentParam
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	params.Save(params.AutoIndexConfig.Enable.Key, "true")
	params.Save(params.AutoIndexConfig.PrepareParams.Key, `{"zero":1,"disabled":true,"empty":"default","absent":1}`)
	params.Save(params.AutoIndexConfig.LoadAdaptParams.Key, `{"zero":2,"disabled":true,"empty":"adapted","adapt_only":2}`)
	params.Save("knowhere.TEST_INDEX.load.zero", "3")
	params.Save("knowhere.TEST_INDEX.load.knowhere_only", "3")
	params.Save("knowhere.TEST_INDEX.search.search_only", "4")
	input := map[string]string{"index_type": "TEST_INDEX", "zero": "0", "disabled": "false", "empty": ""}
	assert.NoError(t, AppendPrepareLoadParams(&params, input))
	assert.Equal(t, "0", input["zero"])
	assert.Equal(t, "false", input["disabled"])
	assert.Equal(t, "", input["empty"])
	assert.Equal(t, "1", input["absent"])
	assert.Equal(t, "2", input["adapt_only"])
	assert.Equal(t, "3", input["knowhere_only"])
	assert.NotContains(t, input, "search_only")
}

func TestLoadDeploymentParamsDisabled(t *testing.T) {
	var params paramtable.ComponentParam
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true)))
	params.Save(params.KnowhereConfig.Enable.Key, "false")
	params.Save("knowhere.TEST_INDEX.load.load_only", "3")
	input := map[string]string{"index_type": "TEST_INDEX"}
	assert.NoError(t, AppendPrepareLoadParams(&params, input))
	assert.NotContains(t, input, "load_only")
}
