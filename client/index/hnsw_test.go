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

func TestHNSW(t *testing.T) {
	idx := NewHNSWIndex(entity.COSINE, 30, 360)

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.COSINE, result[MetricTypeKey])
	assert.EqualValues(t, HNSW, result[IndexTypeKey])
	assert.Equal(t, "30", result[hnswMKey])
	assert.Equal(t, "360", result[hsnwEfConstruction])
}

func TestHNSWSQ(t *testing.T) {
	idx := NewHNSWSQIndex(entity.COSINE, 30, 360, "SQ6")

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.COSINE, result[MetricTypeKey])
	assert.EqualValues(t, HNSWSQ, result[IndexTypeKey])
	assert.Equal(t, "30", result[hnswMKey])
	assert.Equal(t, "360", result[hsnwEfConstruction])
	assert.Equal(t, "SQ6", result[hnswSQTypeKey])

	idx = idx.WithRefineType("SQ8")

	result = idx.Params()

	assert.NotEmpty(t, result)
	assert.Equal(t, "SQ8", result[hnswRefineTypeKey])
	assert.Equal(t, "true", result[hnswRefineKey])
}
