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

package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestReranker(t *testing.T) {
	checkParam := func(params []*commonpb.KeyValuePair, key string, value string) bool {
		for _, kv := range params {
			if kv.Key == key && kv.Value == value {
				return true
			}
		}
		return false
	}

	t.Run("rffReranker", func(t *testing.T) {
		rr := NewRRFReranker()
		params := rr.GetParams()
		assert.True(t, checkParam(params, rerankType, rrfRerankType))
		assert.True(t, checkParam(params, rerankParams, `{"k":60}`), "default k shall be 60")

		rr.WithK(50)
		params = rr.GetParams()
		assert.True(t, checkParam(params, rerankType, rrfRerankType))
		assert.True(t, checkParam(params, rerankParams, `{"k":50}`))
	})

	t.Run("weightedReranker", func(t *testing.T) {
		rr := NewWeightedReranker([]float64{1, 2, 1})
		params := rr.GetParams()
		assert.True(t, checkParam(params, rerankType, weightedRerankType))
		assert.True(t, checkParam(params, rerankParams, `{"weights":[1,2,1]}`))
	})
}
