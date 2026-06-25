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

package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestHNSWSQIndex(t *testing.T) {
	t.Run("without_refine", func(t *testing.T) {
		idx := NewHNSWSQIndex(entity.L2, 16, 200, "SQ8")
		params := idx.Params()

		assert.Equal(t, string(HNSWSQ), params[IndexTypeKey])
		assert.Equal(t, string(entity.L2), params[MetricTypeKey])
		assert.Equal(t, "16", params[hnswMKey])
		assert.Equal(t, "200", params[hsnwEfConstruction])
		assert.Equal(t, "SQ8", params[hnswSQTypeKey])

		_, ok := params[hnswRefineKey]
		assert.False(t, ok)
		_, ok = params[hnswRefineTypeKey]
		assert.False(t, ok)
	})

	t.Run("with_refine", func(t *testing.T) {
		idx := NewHNSWSQIndex(entity.L2, 16, 200, "SQ8").WithRefineType("FP32")
		params := idx.Params()

		assert.Equal(t, string(HNSWSQ), params[IndexTypeKey])
		assert.Equal(t, "SQ8", params[hnswSQTypeKey])
		assert.Equal(t, "true", params[hnswRefineKey])
		assert.Equal(t, "FP32", params[hnswRefineTypeKey])
	})
}

func TestHNSWSQAnnParam(t *testing.T) {
	ap := NewHNSWSQAnnParam(64)
	ap.WithRefineK(2.0)
	result := ap.Params()

	ef, ok := result[hnswEfKey]
	assert.True(t, ok)
	assert.Equal(t, 64, ef)

	refineK, ok := result[hnswRefineKKey]
	assert.True(t, ok)
	assert.Equal(t, 2.0, refineK)
}
