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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestIsSupportedNullableVectorType(t *testing.T) {
	nullableVectorTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_Int8Vector,
		schemapb.DataType_SparseFloatVector,
	}
	for _, dataType := range nullableVectorTypes {
		assert.True(t, IsSupportedNullableVectorType(dataType), dataType.String())
	}

	assert.False(t, IsSupportedNullableVectorType(schemapb.DataType_ArrayOfVector))
	assert.False(t, IsSupportedNullableVectorType(schemapb.DataType_Int64))
	assert.False(t, IsSupportedNullableVectorType(schemapb.DataType_JSON))
}

func TestBuildNullableVectorDataIndices(t *testing.T) {
	indices, validCount := BuildNullableVectorDataIndices([]bool{true, false, true, false, true})
	assert.Equal(t, []int{0, -1, 1, -1, 2}, indices)
	assert.Equal(t, 3, validCount)

	indices, validCount = BuildNullableVectorDataIndices([]bool{false, false})
	assert.Equal(t, []int{-1, -1}, indices)
	assert.Equal(t, 0, validCount)

	indices, validCount = BuildNullableVectorDataIndices(nil)
	assert.Empty(t, indices)
	assert.Equal(t, 0, validCount)
}
