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

func TestHNSWIndex(t *testing.T) {
	idx := NewHNSWIndex(entity.L2, 64, 100)

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.EqualValues(t, HNSW, result[IndexTypeKey])
	assert.Equal(t, "64", result[hnswMKey])
	assert.Equal(t, "100", result[hsnwEfConstruction])
}

func TestHNSWSQIndex(t *testing.T) {
	idx := NewHNSWSQIndex(entity.L2, 64, 100, "SQ8")

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.EqualValues(t, HNSWSQ, result[IndexTypeKey])
	assert.Equal(t, "64", result[hnswMKey])
	assert.Equal(t, "100", result[hsnwEfConstruction])
	assert.Equal(t, "SQ8", result[hnswSQTypeKey])

	idx = idx.WithRefine("FP32")

	result = idx.Params()

	assert.NotEmpty(t, result)
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "FP32", result[hnswRefineTypeKey])
}

func TestHNSWPQIndex(t *testing.T) {
	idx := NewHNSWPQIndex(entity.L2, 64, 100, 8, 8)

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.EqualValues(t, HNSWPQ, result[IndexTypeKey])
	assert.Equal(t, "64", result[hnswMKey])
	assert.Equal(t, "100", result[hsnwEfConstruction])
	assert.Equal(t, "8", result[hnswPQMKey])
	assert.Equal(t, "8", result[hnswPQNbitsKey])

	idx = idx.WithRefine("SQ8")

	result = idx.Params()

	assert.NotEmpty(t, result)
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "SQ8", result[hnswRefineTypeKey])
}

func TestHNSWPRQIndex(t *testing.T) {
	idx := NewHNSWPRQIndex(entity.COSINE, 32, 200, 4, 8)

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.COSINE, result[MetricTypeKey])
	assert.EqualValues(t, HNSWPRQ, result[IndexTypeKey])
	assert.Equal(t, "32", result[hnswMKey])
	assert.Equal(t, "200", result[hsnwEfConstruction])
	assert.Equal(t, "4", result[hnswPQMKey])
	assert.Equal(t, "8", result[hnswPQNbitsKey])

	idx = idx.WithRefine("FP16")

	result = idx.Params()

	assert.NotEmpty(t, result)
	assert.Equal(t, "true", result[hnswRefineKey])
	assert.Equal(t, "FP16", result[hnswRefineTypeKey])
}

func TestHNSWQuantAnnParam(t *testing.T) {
	ap := NewHNSWQuantAnnParam(16)
	result := ap.Params()
	assert.Equal(t, 16, result[hnswEfKey])

	ap = ap.WithRefineK(2.0)
	result = ap.Params()
	assert.Equal(t, 2.0, result[hnswRefineKKey])
}
