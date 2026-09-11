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

	"github.com/milvus-io/milvus/client/v3/entity"
)

func TestHNSWSQ(t *testing.T) {
	idx := NewHNSWSQIndex(entity.COSINE, 16, 200, "SQ8")

	result := idx.Params()
	assert.EqualValues(t, entity.COSINE, result[MetricTypeKey])
	assert.EqualValues(t, HNSWSQ, result[IndexTypeKey])
	assert.Equal(t, "16", result[hnswMKey])
	assert.Equal(t, "200", result[hsnwEfConstruction])
	assert.Equal(t, "SQ8", result[hnswSQTypeKey])
}

func TestHNSWPQ(t *testing.T) {
	idx := NewHNSWPQIndex(entity.L2, 16, 200, 32, 8)

	result := idx.Params()
	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.EqualValues(t, HNSWPQ, result[IndexTypeKey])
	assert.Equal(t, "16", result[hnswMKey])
	assert.Equal(t, "200", result[hsnwEfConstruction])
	// `M` is the graph degree, `m` the number of PQ sub-quantizers: distinct
	// params that differ only in case, so assert both survive the map.
	assert.Equal(t, "32", result[hnswPQMKey])
	assert.NotEqual(t, result[hnswMKey], result[hnswPQMKey])
	assert.Equal(t, "8", result[hnswPQNbitsKey])
}

func TestHNSWPRQ(t *testing.T) {
	idx := NewHNSWPRQIndex(entity.IP, 16, 200, 2, 2, 8)

	result := idx.Params()
	assert.EqualValues(t, entity.IP, result[MetricTypeKey])
	assert.EqualValues(t, HNSWPRQ, result[IndexTypeKey])
	assert.Equal(t, "16", result[hnswMKey])
	assert.Equal(t, "200", result[hsnwEfConstruction])
	assert.Equal(t, "2", result[hnswPQMKey])
	assert.Equal(t, "2", result[hnswPRQNrqKey])
	assert.Equal(t, "8", result[hnswPQNbitsKey])
}

func TestHNSWQuantizedRefine(t *testing.T) {
	// Refine must stay absent unless the caller opts in: an index built without
	// it would otherwise carry an empty refine_type the server rejects.
	for _, result := range []map[string]string{
		NewHNSWSQIndex(entity.L2, 16, 200, "SQ8").Params(),
		NewHNSWPQIndex(entity.L2, 16, 200, 32, 8).Params(),
		NewHNSWPRQIndex(entity.L2, 16, 200, 2, 2, 8).Params(),
	} {
		assert.NotContains(t, result, hnswRefineKey)
		assert.NotContains(t, result, hnswRefineTypeKey)
	}

	result := NewHNSWSQIndex(entity.L2, 16, 200, "SQ8").WithRefineType("FP32").Params()
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "FP32", result[hnswRefineTypeKey])

	result = NewHNSWPQIndex(entity.L2, 16, 200, 32, 8).WithRefineType("SQ8").Params()
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "SQ8", result[hnswRefineTypeKey])

	result = NewHNSWPRQIndex(entity.L2, 16, 200, 2, 2, 8).WithRefineType("BF16").Params()
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "BF16", result[hnswRefineTypeKey])
}

func TestHNSWQuantizedAnnParam(t *testing.T) {
	for _, ap := range []*hnswQuantAnnParam{
		NewHNSWSQAnnParam(64),
		NewHNSWPQAnnParam(64),
		NewHNSWPRQAnnParam(64),
	} {
		result := ap.Params()
		assert.Equal(t, 64, result[hnswEfKey])
		assert.NotContains(t, result, hnswRefineKKey)
		assert.NotContains(t, result, hnswSeedEfKey)

		result = ap.WithRefineK(1.5).WithSeedEf(32).Params()
		// refine_k is knowhere's k_factor (CFG_FLOAT), so fractions must survive.
		assert.Equal(t, 1.5, result[hnswRefineKKey])
		assert.Equal(t, 32, result[hnswSeedEfKey])
	}
}
