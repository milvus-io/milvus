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

func TestGenericIndex(t *testing.T) {
	params := map[string]string{
		IndexTypeKey: string(FMINDEX),
		"custom_key": "custom_value",
	}
	idx := NewGenericIndex("fm_index", params)

	assert.Equal(t, "fm_index", idx.Name())
	assert.EqualValues(t, FMINDEX, idx.IndexType())
	assert.Equal(t, params, idx.Params())

	idxWithoutType := NewGenericIndex("legacy_index", map[string]string{"custom_key": "custom_value"})
	assert.Empty(t, idxWithoutType.IndexType())
}

func TestWithExtraIndexParams(t *testing.T) {
	// A param the typed constructor does not model must reach the wire without
	// giving up the constructor.
	idx := WithExtraIndexParams(NewHNSWIndex(entity.L2, 16, 200), map[string]string{
		"refine":      "true",
		"refine_type": "fp32",
	})

	result := idx.Params()
	assert.EqualValues(t, HNSW, result[IndexTypeKey])
	assert.Equal(t, "16", result[hnswMKey])
	assert.Equal(t, "true", result["refine"])
	assert.Equal(t, "fp32", result["refine_type"])
	assert.EqualValues(t, HNSW, idx.IndexType())

	// Extras win on collision, and the wrapped index is left untouched.
	base := NewHNSWIndex(entity.L2, 16, 200)
	wrapped := WithExtraIndexParams(base, map[string]string{hnswMKey: "32"})
	assert.Equal(t, "32", wrapped.Params()[hnswMKey])
	assert.Equal(t, "16", base.Params()[hnswMKey])
}

func TestWithExtraIndexParamsReservedKeys(t *testing.T) {
	// Overriding index_type would make Params() disagree with IndexType(), so
	// the reserved keys are ignored rather than honored.
	idx := WithExtraIndexParams(NewHNSWIndex(entity.L2, 16, 200), map[string]string{
		IndexTypeKey:  string(IvfFlat),
		MetricTypeKey: string(entity.IP),
		"refine_k":    "64",
	})

	result := idx.Params()
	assert.EqualValues(t, HNSW, result[IndexTypeKey])
	assert.EqualValues(t, HNSW, idx.IndexType())
	assert.EqualValues(t, entity.L2, result[MetricTypeKey])
	assert.Equal(t, "64", result["refine_k"])
}
