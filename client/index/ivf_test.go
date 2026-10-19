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

func TestIvfRabitQ(t *testing.T) {
	idx := NewIvfRabitQIndex(entity.COSINE, 128)

	assert.NotZero(t, idx)

	result := idx.Params()
	assert.NotEmpty(t, result)

	assert.EqualValues(t, entity.COSINE, result[MetricTypeKey])
	assert.EqualValues(t, IvfRabitQ, result[IndexTypeKey])
	assert.Equal(t, "128", result[ivfNlistKey])

	idx = idx.WithRefineType("SQ8")

	result = idx.Params()

	assert.NotEmpty(t, result)
	assert.Equal(t, "SQ8", result[ivfRefineTypeKey])
	assert.Equal(t, "true", result[ivfRefineKey])
}

func TestIvfRabitQAnnParam(t *testing.T) {
	ap := NewIvfRabitQAnnParam(16)
	result := ap.Params()
	assert.Equal(t, 16, result[ivfNprobeKey])

	ap = ap.WithRabitQueryBits(8)
	result = ap.Params()
	assert.Equal(t, 8, result[ivfRbqQueryBitsKey])
	// Pin the wire name: knowhere reads rbq_bits_query, and a transposed key
	// silently leaves the search on the default instead of erroring.
	assert.Equal(t, 8, result["rbq_bits_query"])

	ap = ap.WithRefineK(256)
	result = ap.Params()
	assert.Equal(t, 256, result[ivfRbqRefineKKey])
}

func TestIvfRabitQBits(t *testing.T) {
	idx := NewIvfRabitQIndex(entity.COSINE, 128)
	// Params() has always reported IVF_RABITQ; IndexType() used to disagree.
	assert.EqualValues(t, IvfRabitQ, idx.IndexType())
	assert.EqualValues(t, IvfRabitQ, idx.Params()[IndexTypeKey])

	// Never set: absent, so the server's default applies.
	assert.NotContains(t, idx.Params(), ivfRbqBitsKey)
	assert.Equal(t, "5", idx.WithRbqBits(5).Params()[ivfRbqBitsKey])

	// Explicitly set to an out-of-range value: forwarded so the server rejects
	// it, instead of being mistaken for "unset".
	assert.Equal(t, "0", NewIvfRabitQIndex(entity.COSINE, 128).WithRbqBits(0).Params()[ivfRbqBitsKey])
}
