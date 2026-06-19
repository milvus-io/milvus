/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package rerank

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestGetRerankName(t *testing.T) {
	t.Run("returns reranker name", func(t *testing.T) {
		funcSchema := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Rerank,
			Params: []*commonpb.KeyValuePair{
				{Key: "reranker", Value: "rrf"},
			},
		}
		assert.Equal(t, "rrf", GetRerankName(funcSchema))
	})

	t.Run("case insensitive", func(t *testing.T) {
		funcSchema := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Rerank,
			Params: []*commonpb.KeyValuePair{
				{Key: "RERANKER", Value: "Decay"},
			},
		}
		assert.Equal(t, "decay", GetRerankName(funcSchema))
	})

	t.Run("returns empty when no reranker param", func(t *testing.T) {
		funcSchema := &schemapb.FunctionSchema{
			Type: schemapb.FunctionType_Rerank,
			Params: []*commonpb.KeyValuePair{
				{Key: "weight", Value: "2.0"},
			},
		}
		assert.Equal(t, "", GetRerankName(funcSchema))
	})

	t.Run("constants are correct", func(t *testing.T) {
		assert.Equal(t, "boost", BoostName)
		assert.Equal(t, "filter", FilterKey)
		assert.Equal(t, "weight", WeightKey)
	})
}
